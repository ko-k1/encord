//! encord-rest: Discord REST client, official-desktop identity.
//! - Base `https://discord.com/api/v9`, desktop UA, `X-Super-Properties`,
//!   `X-Discord-Locale`. No telemetry, 429 honors `retry_after` + jitter.

use encord_core::{Member, Message, Snowflake};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

pub const API_VERSION: u8 = 9;

#[derive(Debug, Error)]
pub enum RestError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("api {status}: {body}")]
    Api { status: u16, body: String },
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct Rest {
    client: reqwest::Client,
    base: String,
    token: String,
    user_agent: String,
    super_b64: String,
}

impl Rest {
    pub fn new(base: impl Into<String>, token: impl Into<String>) -> Result<Self, RestError> {
        Self::with_options(base, token, None)
    }

    /// `ua_override` replaces the stock desktop UA (ini `[http] user_agent`),
    /// keeping `browser_user_agent` in sync inside super-properties.
    pub fn with_options(
        base: impl Into<String>,
        token: impl Into<String>,
        ua_override: Option<String>,
    ) -> Result<Self, RestError> {
        let user_agent = ua_override.unwrap_or_else(encord_identity::desktop_user_agent);
        let super_b64 = encord_identity::super_properties_b64(
            &encord_identity::SuperProperties::desktop_with_ua(&user_agent),
        );
        // reqwest sets its own UA by default; we override per-request below,
        // but keep the client UA consistent anyway.
        let client = reqwest::Client::builder()
            .user_agent(&user_agent)
            .timeout(Duration::from_secs(20))
            .build()?;
        Ok(Self {
            client,
            base: base.into(),
            token: token.into(),
            user_agent,
            super_b64,
        })
    }

    pub fn with_defaults(token: impl Into<String>) -> Result<Self, RestError> {
        Self::new(format!("https://discord.com/api/v{API_VERSION}"), token)
    }

    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            USER_AGENT,
            HeaderValue::from_str(&self.user_agent).expect("valid UA"),
        );
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        h.insert(
            "x-super-properties",
            HeaderValue::from_str(&self.super_b64).expect("valid b64"),
        );
        h.insert("x-discord-locale", HeaderValue::from_static("en-US"));
        if let Ok(v) = HeaderValue::from_str(&self.token) {
            h.insert(AUTHORIZATION, v);
        }
        h
    }

    async fn send_raw(&self, req: reqwest::RequestBuilder) -> Result<reqwest::Response, RestError> {
        // Bounded 429 retries (caller paces; this is the safety net).
        for _ in 0..4 {
            let res = req
                .try_clone()
                .expect("request must be cloneable")
                .headers(self.headers())
                .send()
                .await?;
            let status = res.status().as_u16();
            if status == 429 {
                let body: Value = res.json().await.unwrap_or(Value::Null);
                let wait = body
                    .get("retry_after")
                    .and_then(Value::as_f64)
                    .unwrap_or(1.0)
                    .clamp(0.2, 10.0);
                tokio::time::sleep(Duration::from_millis((wait * 1000.0) as u64 + 100)).await;
                continue;
            }
            if !(200..300).contains(&status) {
                let body = res.text().await.unwrap_or_default();
                return Err(RestError::Api { status, body });
            }
            return Ok(res);
        }
        Err(RestError::Api {
            status: 429,
            body: "rate limited".into(),
        })
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<Value, RestError> {
        Ok(self.send_raw(req).await?.json().await?)
    }

    pub fn messages_url(&self, channel_id: Snowflake, limit: usize) -> String {
        format!(
            "{}/channels/{}/messages?limit={}",
            self.base,
            channel_id,
            limit.clamp(1, 100)
        )
    }

    pub async fn get_messages(
        &self,
        channel_id: Snowflake,
        limit: usize,
    ) -> Result<Vec<Message>, RestError> {
        let v = self
            .send(self.client.get(self.messages_url(channel_id, limit)))
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    pub async fn send_message(
        &self,
        channel_id: Snowflake,
        content: &str,
    ) -> Result<Message, RestError> {
        let v = self
            .send(
                self.client
                    .post(format!("{}/channels/{}/messages", self.base, channel_id))
                    .json(&serde_json::json!({ "content": content })),
            )
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    pub async fn me(&self) -> Result<Value, RestError> {
        self.send(self.client.get(format!("{}/users/@me", self.base)))
            .await
    }

    /// Mark a channel read up to a message (clears unread + mentions).
    /// Failures are non-fatal by contract; callers log and continue.
    pub async fn ack(&self, channel_id: Snowflake, message_id: Snowflake) -> Result<(), RestError> {
        self.send_raw(
            self.client
                .post(format!(
                    "{}/channels/{channel_id}/messages/{message_id}/ack",
                    self.base
                ))
                .json(&serde_json::json!({ "token": null })),
        )
        .await?;
        Ok(())
    }

    pub fn guild_members_url(
        &self,
        guild_id: Snowflake,
        limit: usize,
        after: Option<Snowflake>,
    ) -> String {
        let mut url = format!(
            "{}/guilds/{guild_id}/members?limit={}",
            self.base,
            limit.clamp(1, 1000)
        );
        if let Some(after) = after {
            url.push_str(&format!("&after={after}"));
        }
        url
    }

    /// Guild member list (Abaddon-style backfill for the userlist).
    /// Pages with `after` until a short page or `limit` total members
    /// (clamped 1..=1000): a single first-page call caps big guilds at 100.
    pub async fn get_guild_members(
        &self,
        guild_id: Snowflake,
        limit: usize,
    ) -> Result<Vec<Member>, RestError> {
        let want = limit.clamp(1, 1000);
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page: usize = (want - out.len()).min(1000);
            let v = self
                .send(
                    self.client
                        .get(self.guild_members_url(guild_id, page, after)),
                )
                .await?;
            let mut members: Vec<Member> = serde_json::from_value(v)?;
            let full = members.len() >= page;
            after = members.last().map(|m| m.user.id);
            out.append(&mut members);
            if !full || out.len() >= want {
                break;
            }
        }
        out.truncate(want);
        Ok(out)
    }

    /// Application name for a discord-ipc `client_id` (used to fill the
    /// presence `name` games omit). Failures are routine (unknown app);
    /// callers fall back to the client id.
    pub async fn get_application_name(&self, app_id: &str) -> Result<String, RestError> {
        let v = self
            .send(
                self.client
                    .get(format!("{}/applications/{app_id}/rpc", self.base)),
            )
            .await?;
        v.get("name")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| RestError::Api {
                status: 200,
                body: "applications/rpc missing name".into(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_url_uses_v9_and_clamps() {
        let r = Rest::with_defaults("t").unwrap();
        assert!(r
            .messages_url(Snowflake::new(5), 200)
            .contains("/api/v9/channels/5/messages?limit=100"));
        assert!(r.messages_url(Snowflake::new(5), 0).ends_with("limit=1"));
    }

    #[test]
    fn members_url_pages_with_after() {
        let r = Rest::with_defaults("t").unwrap();
        assert_eq!(
            r.guild_members_url(Snowflake::new(9), 100, None),
            "https://discord.com/api/v9/guilds/9/members?limit=100"
        );
        assert_eq!(
            r.guild_members_url(Snowflake::new(9), 2000, Some(Snowflake::new(42))),
            "https://discord.com/api/v9/guilds/9/members?limit=1000&after=42"
        );
    }
}
