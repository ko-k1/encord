//! encord-gateway: Discord Gateway v9 client, official-desktop identity.
//!
//! Fingerprint parity (see encord-identity): stock desktop UA, full
//! `X-Super-Properties`-style IDENTIFY block, API v9 endpoints, never touch
//! automation-only endpoints or `/science` telemetry.

use encord_core::{Channel, ChannelRead, CoreEvent, Guild, Member, Message, Snowflake, User};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::VecDeque;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

pub const GATEWAY_VERSION: u8 = 9;
pub const ENCODING: &str = "json";

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("websocket: {0}")]
    Ws(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("zlib: {0}")]
    Zlib(#[from] flate2::DecompressError),
    #[error("utf8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("closed: {0}")]
    Closed(String),
    #[error("invalid session (resumable={0})")]
    InvalidSession(bool),
}

impl From<tokio_tungstenite::tungstenite::Error> for GatewayError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Ws(e.to_string())
    }
}

/// Gateway capabilities bitmask for the IDENTIFY payload: the value the
/// official clients ship. Capabilities (not intents) gate user-account
/// behavior; without them the session is degraded (lazy guilds, missing
/// presence/state). `intents` is deliberately NOT sent: it is a bot
/// construct and its use is not recommended for user accounts.
pub const CAPABILITIES: u32 = 16381;

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Full gateway URL, e.g. wss://gateway.discord.gg/?v=9&encoding=json&compress=zlib-stream
    pub url: String,
    pub token: String,
    /// IDENTIFY `os`/`browser`. Empty = official desktop defaults.
    pub os: String,
    pub browser: String,
}

impl GatewayConfig {
    pub fn with_token(url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
            os: encord_identity::DESKTOP_OS.to_string(),
            browser: encord_identity::DESKTOP_BROWSER.to_string(),
        }
    }

    pub fn default_url() -> String {
        format!(
            "wss://gateway.discord.gg/?v={GATEWAY_VERSION}&encoding={ENCODING}&compress=zlib-stream"
        )
    }
}

/// Build the IDENTIFY payload exactly like the official user clients:
/// `capabilities` (16381) + initial presence + `client_state`, no `intents`
/// (bots-only). `large_threshold` 50 keeps GUILD_CREATE member lists small;
/// the member list backfills over REST on guild select.
pub fn identify_payload(cfg: &GatewayConfig) -> Value {
    json!({
        "op": 2,
        "d": {
            "token": cfg.token,
            "capabilities": CAPABILITIES,
            "properties": encord_identity::identify_properties(&cfg.os, &cfg.browser),
            "presence": {
                "activities": [],
                "status": "unknown",
                "since": 0,
                "afk": false,
            },
            "compress": false,
            "large_threshold": 50,
            "client_state": {
                "api_code_version": 0,
                "guild_versions": {},
            },
        }
    })
}

pub fn resume_payload(token: &str, session_id: &str, seq: Option<u64>) -> Value {
    json!({
        "op": 6,
        "d": { "token": token, "session_id": session_id, "seq": seq }
    })
}

pub fn heartbeat_payload(seq: Option<u64>) -> Value {
    json!({ "op": 1, "d": seq })
}

/// Build the WS handshake request exactly the way tungstenite does for a
/// plain URL (Host/Connection/Upgrade/Version/Key), plus browser Origin + UA.
///
/// A bare `http::Request` carrying only custom headers is REJECTED by
/// tungstenite's `generate_request` validation with
/// `Protocol(InvalidHeader(sec-websocket-key))`
/// ("Missing, duplicated or incorrect header …") before any byte is sent —
/// so always start from `into_client_request` and only *add* headers.
fn build_request(url: &str) -> Result<http::Request<()>, GatewayError> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        http::header::ORIGIN,
        http::header::HeaderValue::from_static("https://discord.com"),
    );
    request.headers_mut().insert(
        http::header::USER_AGENT,
        http::header::HeaderValue::from_str(&encord_identity::desktop_user_agent())
            .expect("desktop UA is a valid header value"),
    );
    Ok(request)
}

/// Outbound gateway commands (UI/RPC -> Discord).
#[derive(Clone, Debug)]
pub enum GatewayCmd {
    /// OP 3 presence update (status + Rich Presence activities).
    Presence {
        status: String,
        activities: Vec<Value>,
        afk: bool,
    },
}

/// OP 3 presence payload, same shape the desktop client sends.
pub fn presence_payload(status: &str, activities: &[Value], afk: bool) -> Value {
    json!({
        "op": 3,
        "d": {
            "since": null,
            "activities": activities,
            "status": status,
            "afk": afk,
        }
    })
}

/// Incremental `zlib-stream` transport decompressor (one per connection).
///
/// Discord sends ONE continuous zlib stream; every gateway payload ends at a
/// `Z_SYNC_FLUSH` boundary (`00 00 FF FF`). Decoding MUST reuse a single
/// persistent inflater: later payloads back-reference earlier bytes, so a
/// fresh decoder per message fails with "incomplete deflate stream" (or
/// silently corrupts output).
pub struct ZlibStream {
    decomp: flate2::Decompress,
    pending: Vec<u8>,
}

impl ZlibStream {
    pub fn new() -> Self {
        Self {
            decomp: flate2::Decompress::new(true),
            pending: Vec::new(),
        }
    }

    /// Feed raw bytes; returns every flush-terminated payload completed by
    /// this chunk. Tolerates arbitrary TCP/frame fragmentation AND several
    /// payloads coalesced in one frame (the initial guild burst does this):
    /// the buffer is split at every flush marker and each segment decoded
    /// in order through the shared inflater. Assumes one payload per flush
    /// (Discord's framing). Empty segments are skipped silently.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<String>, GatewayError> {
        self.pending.extend_from_slice(bytes);
        if self.pending.len() < 4 || !self.pending.ends_with(&[0x00, 0x00, 0xff, 0xff]) {
            return Ok(Vec::new());
        }
        // Segment boundaries: end offset (exclusive) of every flush marker.
        let mut ends = Vec::new();
        let mut i = 0;
        while i + 4 <= self.pending.len() {
            if self.pending[i] == 0x00
                && self.pending[i + 1] == 0x00
                && self.pending[i + 2] == 0xff
                && self.pending[i + 3] == 0xff
            {
                ends.push(i + 4);
                i += 4;
            } else {
                i += 1;
            }
        }
        let mut texts = Vec::new();
        let mut prev = 0;
        for end in &ends {
            let segment = &self.pending[prev..*end];
            prev = *end;
            let mut out = Vec::with_capacity(segment.len() * 8 + 64);
            loop {
                match self
                    .decomp
                    .decompress_vec(segment, &mut out, flate2::FlushDecompress::Sync)
                {
                    Ok(flate2::Status::BufError) => out.reserve(65536),
                    Ok(_) => break,
                    Err(e) => return Err(GatewayError::from(e)),
                }
            }
            let text = String::from_utf8(out)?;
            if !text.trim().is_empty() {
                texts.push(text);
            }
        }
        self.pending.drain(..prev);
        Ok(texts)
    }
}

impl Default for ZlibStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Human-readable close frame: Discord sends codes like 4004
/// (authentication failed) or 4014 (disallowed intents) with a reason.
/// These used to be discarded, leaving only "close frame" in the log.
fn close_reason(
    phase: &str,
    frame: Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>,
) -> String {
    match frame {
        Some(f) => format!("{phase} close {} {}", u16::from(f.code), f.reason),
        None => format!("{phase} close (no frame)"),
    }
}

/// If `text` is a Hello envelope (op 10), return its heartbeat interval.
fn hello_ms(text: &str) -> Result<Option<u64>, GatewayError> {
    let v: Value = serde_json::from_str(text)?;
    if v.get("op").and_then(Value::as_u64) == Some(10) {
        Ok(Some(v["d"]["heartbeat_interval"].as_u64().unwrap_or(41250)))
    } else {
        Ok(None)
    }
}

/// Parsed envelope: events, diagnostics, new sequence number.
pub type ParsedEnvelope = (Vec<CoreEvent>, Vec<String>, Option<u64>);
/// Returns (events, warnings, new_seq). Unknown dispatches yield no events
/// (forward-compat). Bulk payloads (READY, GUILD_CREATE) fan out into
/// one event per nested entity. Per-entity skips are reported as warnings
/// instead of silently dropping data.
pub fn parse_envelope(text: &str) -> Result<ParsedEnvelope, GatewayError> {
    let v: Value = serde_json::from_str(text)?;
    let op = v.get("op").and_then(Value::as_u64).unwrap_or(99);
    let seq = v.get("s").and_then(Value::as_u64);
    let t = v.get("t").and_then(Value::as_str).unwrap_or("");

    match op {
        // Dispatch
        0 => {
            let mut warnings = Vec::new();
            let events = parse_dispatch_many(t, v.get("d").unwrap_or(&Value::Null), &mut warnings);
            Ok((events, warnings, seq))
        }
        // Hello / Heartbeat ACK / Reconnect / Invalid session handled by runner
        _ => Ok((Vec::new(), Vec::new(), seq)),
    }
}

fn parse_presence(d: &Value, fallback_guild: Option<Snowflake>) -> Option<CoreEvent> {
    let user_id: Snowflake = serde_json::from_value(d.get("user")?.get("id")?.clone()).ok()?;
    let status = d
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("offline")
        .to_string();
    let guild_id = d
        .get("guild_id")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .or(fallback_guild);
    Some(CoreEvent::PresenceUpdate {
        user_id,
        status,
        guild_id,
    })
}

fn parse_dispatch_many(t: &str, d: &Value, warnings: &mut Vec<String>) -> Vec<CoreEvent> {
    match t {
        "READY" => {
            let mut out = Vec::new();
            match (
                d.get("user")
                    .and_then(|u| serde_json::from_value::<User>(u.clone()).ok()),
                d.get("session_id").and_then(Value::as_str),
            ) {
                (Some(user), Some(session_id)) => {
                    out.push(CoreEvent::Ready {
                        user,
                        session_id: session_id.to_string(),
                        resume_url: d
                            .get("resume_gateway_url")
                            .and_then(Value::as_str)
                            .unwrap_or("wss://gateway.discord.gg")
                            .to_string(),
                    });
                }
                _ => tracing::warn!("dropping READY without user/session_id"),
            }
            // DM channels arrive nested in READY, nowhere else.
            if let Some(channels) = d.get("private_channels").and_then(Value::as_array) {
                for raw in channels {
                    if let Ok(mut c) = serde_json::from_value::<Channel>(raw.clone()) {
                        c.guild_id = None;
                        out.push(CoreEvent::ChannelCreate(c));
                    }
                }
            }
            // Read states drive unread indicators + mention badges.
            if let Some(states) = d.get("read_state").and_then(Value::as_array) {
                let reads: Vec<ChannelRead> = states
                    .iter()
                    .filter_map(|r| {
                        Some(ChannelRead {
                            channel_id: serde_json::from_value(r.get("id")?.clone()).ok()?,
                            last_message_id: r
                                .get("last_message_id")
                                .and_then(|v| serde_json::from_value(v.clone()).ok()),
                            mention_count: r
                                .get("mention_count")
                                .and_then(Value::as_u64)
                                .unwrap_or(0),
                        })
                    })
                    .collect();
                if !reads.is_empty() {
                    out.push(CoreEvent::ReadState(reads));
                }
            }
            out
        }
        "RESUMED" => vec![CoreEvent::Resumed],
        "GUILD_CREATE" => {
            let mut out = Vec::new();
            match serde_json::from_value::<Guild>(d.clone()) {
                Ok(g) => {
                    let gid = g.id;
                    out.push(CoreEvent::GuildCreate(g));
                    // Guild channels are nested here; initial sync sends nothing
                    // else. Fill in guild_id when Discord omits it.
                    if let Some(channels) = d.get("channels").and_then(Value::as_array) {
                        let mut skipped = 0;
                        for raw in channels {
                            match serde_json::from_value::<Channel>(raw.clone()) {
                                Ok(mut c) => {
                                    if c.guild_id.is_none() {
                                        c.guild_id = Some(gid);
                                    }
                                    out.push(CoreEvent::ChannelCreate(c));
                                }
                                Err(_) => skipped += 1,
                            }
                        }
                        if skipped > 0 {
                            warnings.push(format!(
                                "GUILD_CREATE {gid}: skipped {skipped} unparsable channels"
                            ));
                        }
                    }
                    if let Some(members) = d.get("members").and_then(Value::as_array) {
                        let total = members.len();
                        let chunk: Vec<Member> = members
                            .iter()
                            .filter_map(|m| serde_json::from_value(m.clone()).ok())
                            .collect();
                        if chunk.len() < total {
                            warnings.push(format!(
                                "GUILD_CREATE {gid}: skipped {} unparsable members",
                                total - chunk.len()
                            ));
                        }
                        if !chunk.is_empty() {
                            out.push(CoreEvent::GuildMembersChunk {
                                guild_id: gid,
                                members: chunk,
                            });
                        }
                    }
                    if let Some(presences) = d.get("presences").and_then(Value::as_array) {
                        let mut skipped = 0;
                        for raw in presences {
                            match parse_presence(raw, Some(gid)) {
                                Some(ev) => out.push(ev),
                                None => skipped += 1,
                            }
                        }
                        if skipped > 0 {
                            warnings.push(format!(
                                "GUILD_CREATE {gid}: skipped {skipped} unparsable presences"
                            ));
                        }
                    }
                }
                Err(e) => {
                    // Never silent: an unparsable guild blanks the whole rail.
                    tracing::warn!("dropping unparsable GUILD_CREATE: {e}");
                }
            }
            out
        }
        // Member add/update carry the member flattened + guild_id alongside.
        "GUILD_MEMBER_ADD" => {
            match (
                d.get("guild_id")
                    .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok()),
                serde_json::from_value::<Member>(d.clone()).ok(),
            ) {
                (Some(guild_id), Some(member)) => {
                    vec![CoreEvent::GuildMemberAdd { guild_id, member }]
                }
                _ => Vec::new(),
            }
        }
        "GUILD_MEMBER_UPDATE" => {
            match (
                d.get("guild_id")
                    .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok()),
                serde_json::from_value::<Member>(d.clone()).ok(),
            ) {
                (Some(guild_id), Some(member)) => {
                    vec![CoreEvent::GuildMemberUpdate { guild_id, member }]
                }
                _ => Vec::new(),
            }
        }
        "GUILD_MEMBER_REMOVE" => {
            match (
                d.get("guild_id")
                    .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok()),
                d.get("user")
                    .and_then(|u| u.get("id"))
                    .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok()),
            ) {
                (Some(guild_id), Some(user_id)) => {
                    vec![CoreEvent::GuildMemberRemove { guild_id, user_id }]
                }
                _ => Vec::new(),
            }
        }
        "PRESENCE_UPDATE" => parse_presence(d, None)
            .map(|ev| vec![ev])
            .unwrap_or_default(),
        // Op 8 chunks (also used by member search): members carry the
        // guild context in the envelope, not in each member object.
        "GUILD_MEMBERS_CHUNK" => {
            match d
                .get("guild_id")
                .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok())
            {
                Some(guild_id) => {
                    let members: Vec<Member> = d
                        .get("members")
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|m| serde_json::from_value(m.clone()).ok())
                                .collect()
                        })
                        .unwrap_or_default();
                    if members.is_empty() {
                        Vec::new()
                    } else {
                        vec![CoreEvent::GuildMembersChunk { guild_id, members }]
                    }
                }
                None => Vec::new(),
            }
        }
        // Threads arrive as channel objects with guild_id set; active
        // threads bulk-sync the same way.
        "THREAD_CREATE" | "THREAD_UPDATE" => match serde_json::from_value::<Channel>(d.clone()) {
            Ok(c) => vec![CoreEvent::ChannelCreate(c)],
            Err(_) => {
                warnings.push(format!("dropping unparsable {t}"));
                Vec::new()
            }
        },
        "THREAD_DELETE" => {
            match d
                .get("id")
                .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok())
            {
                Some(id) => vec![CoreEvent::ChannelDelete(id)],
                None => {
                    warnings.push(format!("dropping unparsable {t}"));
                    Vec::new()
                }
            }
        }
        "THREAD_LIST_SYNC" => {
            let mut out = Vec::new();
            if let Some(threads) = d.get("threads").and_then(Value::as_array) {
                for raw in threads {
                    if let Ok(c) = serde_json::from_value::<Channel>(raw.clone()) {
                        out.push(CoreEvent::ChannelCreate(c));
                    }
                }
            }
            if let (Some(gid), Some(members)) = (
                d.get("guild_id")
                    .and_then(|v| serde_json::from_value::<Snowflake>(v.clone()).ok()),
                d.get("members").and_then(Value::as_array),
            ) {
                let chunk: Vec<Member> = members
                    .iter()
                    .filter_map(|m| serde_json::from_value(m.clone()).ok())
                    .collect();
                if !chunk.is_empty() {
                    out.push(CoreEvent::GuildMembersChunk {
                        guild_id: gid,
                        members: chunk,
                    });
                }
            }
            out
        }
        _ => match parse_dispatch(t, d) {
            Some(ev) => vec![ev],
            None => {
                // Known state-carrying types must never vanish silently;
                // unknown future types stay silent (forward-compat).
                match t {
                    "MESSAGE_CREATE"
                    | "MESSAGE_UPDATE"
                    | "CHANNEL_CREATE"
                    | "CHANNEL_DELETE"
                    | "CHANNEL_UPDATE"
                    | "GUILD_UPDATE"
                    | "TYPING_START"
                    | "GUILD_DELETE"
                    | "MESSAGE_DELETE_BULK" => {
                        warnings.push(format!("dropping unparsable {t}"));
                    }
                    _ => {}
                }
                Vec::new()
            }
        },
    }
}

fn parse_emoji(d: &Value) -> Option<encord_core::Emoji> {
    let raw = d.get("emoji")?;
    Some(encord_core::Emoji {
        id: raw
            .get("id")
            .and_then(|v| serde_json::from_value(v.clone()).ok()),
        name: raw.get("name").and_then(Value::as_str).map(str::to_string),
    })
}

/// (channel_id, message_id) for message-scoped events. Deletes carry
/// the id as `id`; reactions/acks carry it as `message_id`.
fn parse_message_ref(d: &Value) -> Option<(Snowflake, Snowflake)> {
    Some((
        serde_json::from_value(d.get("channel_id")?.clone()).ok()?,
        serde_json::from_value(d.get("id").or_else(|| d.get("message_id"))?.clone()).ok()?,
    ))
}

fn parse_dispatch(t: &str, d: &Value) -> Option<CoreEvent> {
    match t {
        // READY / GUILD_CREATE fan out in parse_dispatch_many; RESUMED too.
        // `unavailable` set = outage (keep cache); absent = left/removed.
        "GUILD_DELETE" => {
            let id: Snowflake = serde_json::from_value(d.get("id")?.clone()).ok()?;
            let unavailable = d
                .get("unavailable")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(CoreEvent::GuildDelete { id, unavailable })
        }
        "GUILD_UPDATE" => {
            let g: Guild = serde_json::from_value(d.clone()).ok()?;
            Some(CoreEvent::GuildUpdate(g))
        }
        "CHANNEL_CREATE" | "CHANNEL_UPDATE" => {
            let c: Channel = serde_json::from_value(d.clone()).ok()?;
            Some(CoreEvent::ChannelCreate(c))
        }
        "CHANNEL_DELETE" => {
            let c: Channel = serde_json::from_value(d.clone()).ok()?;
            Some(CoreEvent::ChannelDelete(c.id))
        }
        "MESSAGE_CREATE" => {
            let m: Message = serde_json::from_value(d.clone()).ok()?;
            Some(CoreEvent::MessageCreate(m))
        }
        // Full object on content edits, but embed-only updates (link
        // unfurls) carry just ids + changed fields: merge those into the
        // cached row instead of dropping the edit on the floor.
        "MESSAGE_UPDATE" => {
            if let Ok(m) = serde_json::from_value::<Message>(d.clone()) {
                return Some(CoreEvent::MessageUpdate(m));
            }
            let (channel_id, message_id) = parse_message_ref(d)?;
            Some(CoreEvent::MessagePatch {
                channel_id,
                message_id,
                content: d.get("content").and_then(Value::as_str).map(str::to_string),
                edited_timestamp: d
                    .get("edited_timestamp")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        }
        "MESSAGE_DELETE" => {
            let (channel_id, message_id) = parse_message_ref(d)?;
            Some(CoreEvent::MessageDelete {
                channel_id,
                message_id,
            })
        }
        "MESSAGE_DELETE_BULK" => {
            let channel_id: Snowflake =
                serde_json::from_value(d.get("channel_id")?.clone()).ok()?;
            let message_ids: Vec<Snowflake> =
                d.get("ids").and_then(Value::as_array).map(|arr| {
                    arr.iter()
                        .filter_map(|v| serde_json::from_value(v.clone()).ok())
                        .collect()
                })?;
            Some(CoreEvent::MessageDeleteBulk {
                channel_id,
                message_ids,
            })
        }
        "MESSAGE_REACTION_ADD" => {
            let (channel_id, message_id) = parse_message_ref(d)?;
            Some(CoreEvent::ReactionAdd {
                channel_id,
                message_id,
                emoji: parse_emoji(d)?,
            })
        }
        "MESSAGE_REACTION_REMOVE" => {
            let (channel_id, message_id) = parse_message_ref(d)?;
            Some(CoreEvent::ReactionRemove {
                channel_id,
                message_id,
                emoji: parse_emoji(d)?,
            })
        }
        "MESSAGE_REACTION_REMOVE_ALL" => {
            let (channel_id, message_id) = parse_message_ref(d)?;
            Some(CoreEvent::ReactionRemoveAll {
                channel_id,
                message_id,
            })
        }
        "MESSAGE_REACTION_REMOVE_EMOJI" => {
            let (channel_id, message_id) = parse_message_ref(d)?;
            Some(CoreEvent::ReactionRemoveEmoji {
                channel_id,
                message_id,
                emoji: parse_emoji(d)?,
            })
        }
        // Fired by acks from other sessions; keeps badges honest.
        "MESSAGE_ACK" => {
            let channel_id: Snowflake =
                serde_json::from_value(d.get("channel_id")?.clone()).ok()?;
            let message_id: Snowflake = d
                .get("message_id")
                .or_else(|| d.get("id"))
                .and_then(|v| serde_json::from_value(v.clone()).ok())?;
            Some(CoreEvent::MessageAck {
                channel_id,
                message_id,
            })
        }
        "TYPING_START" => Some(CoreEvent::TypingStart {
            channel_id: serde_json::from_value(d.get("channel_id")?.clone()).ok()?,
            user_id: serde_json::from_value(d.get("user_id")?.clone()).ok()?,
        }),
        "VOICE_STATE_UPDATE" => Some(CoreEvent::VoiceStateUpdate {
            guild_id: d
                .get("guild_id")
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            channel_id: d
                .get("channel_id")
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            user_id: serde_json::from_value(d.get("user_id")?.clone()).ok()?,
            session_id: d
                .get("session_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        }),
        _ => None,
    }
}

/// Minimal connection runner: Hello -> Identify/Resume -> heartbeat -> dispatch.
/// Backoff + resume state live here so UI stays dumb.
pub struct Gateway {
    cfg: GatewayConfig,
    events: mpsc::UnboundedSender<CoreEvent>,
    commands: mpsc::UnboundedReceiver<GatewayCmd>,
}

impl Gateway {
    pub fn new(
        cfg: GatewayConfig,
        events: mpsc::UnboundedSender<CoreEvent>,
        commands: mpsc::UnboundedReceiver<GatewayCmd>,
    ) -> Self {
        Self {
            cfg,
            events,
            commands,
        }
    }

    pub async fn run(mut self) -> Result<(), GatewayError> {
        let mut session_id: Option<String> = None;
        let mut seq: Option<u64> = None;
        let mut backoff_secs = 1u64;

        loop {
            // Partial field borrows: config/events stay shared while the
            // command lane is exclusively held by the session.
            let Self {
                ref cfg,
                ref events,
                ref mut commands,
            } = self;
            match Self::connect_once(cfg, events, &mut session_id, &mut seq, commands).await {
                Ok(resumed_or_clean) => {
                    backoff_secs = 1;
                    if resumed_or_clean {
                        continue;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    let _ = events.send(CoreEvent::Disconnected {
                        reason: e.to_string(),
                    });
                    tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(30);
                }
            }
        }
        Ok(())
    }

    async fn connect_once(
        cfg: &GatewayConfig,
        events: &mpsc::UnboundedSender<CoreEvent>,
        session_id: &mut Option<String>,
        seq: &mut Option<u64>,
        commands: &mut mpsc::UnboundedReceiver<GatewayCmd>,
    ) -> Result<bool, GatewayError> {
        use tokio_tungstenite::connect_async;
        let request = build_request(&cfg.url)?;
        let (ws, _) = connect_async(request).await?;
        let (mut write, mut read) = ws.split();
        let mut zlib = ZlibStream::new();
        let mut last_seq = *seq;
        // First payload must be Hello (op 10). Decoded texts queue up: a
        // frame may carry Hello plus later payloads, and leftovers are
        // handed to the main loop instead of dropped.
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut interval = loop {
            let msg = read
                .next()
                .await
                .ok_or_else(|| GatewayError::Closed("eof before hello".into()))??;
            match msg {
                WsMessage::Text(t) => queue.push_back(t.to_string()),
                WsMessage::Binary(b) => queue.extend(zlib.feed(&b)?),
                WsMessage::Ping(p) => {
                    write.send(WsMessage::Pong(p)).await?;
                }
                WsMessage::Pong(_) | WsMessage::Frame(_) => {}
                WsMessage::Close(frame) => {
                    return Err(GatewayError::Closed(close_reason("hello", frame)));
                }
            };
            let mut found_ms = None;
            let mut rest = VecDeque::new();
            for text in std::mem::take(&mut queue) {
                if found_ms.is_some() {
                    rest.push_back(text);
                    continue;
                }
                match hello_ms(&text) {
                    Ok(Some(ms)) => found_ms = Some(ms),
                    Ok(None) => rest.push_back(text),
                    Err(e) => {
                        tracing::warn!("skipping unparsable pre-hello payload: {e}");
                        let _ = events.send(CoreEvent::Warning(format!(
                            "skipped pre-hello garbage: {e}"
                        )));
                    }
                }
            }
            queue = rest;
            if let Some(ms) = found_ms {
                break tokio::time::interval(std::time::Duration::from_millis(ms));
            }
        };

        // Identify or resume.
        if let Some(sid) = session_id.clone() {
            write
                .send(WsMessage::Text(
                    resume_payload(&cfg.token, &sid, last_seq)
                        .to_string()
                        .into(),
                ))
                .await?;
        } else {
            write
                .send(WsMessage::Text(identify_payload(cfg).to_string().into()))
                .await?;
        }

        // First heartbeat right away, then consume the interval's immediate
        // tick so heartbeats stay exactly on cadence (no double-beat).
        write
            .send(WsMessage::Text(
                heartbeat_payload(last_seq).to_string().into(),
            ))
            .await?;
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    write.send(WsMessage::Text(heartbeat_payload(last_seq).to_string().into())).await?;
                }
                Some(cmd) = commands.recv() => {
                    match cmd {
                        GatewayCmd::Presence { status, activities, afk } => {
                            write.send(WsMessage::Text(
                                presence_payload(&status, &activities, afk).to_string().into(),
                            )).await?;
                        }
                    }
                }
                msg = async {
                    // Leftover payloads queued before Hello take priority.
                    if let Some(text) = queue.pop_front() {
                        Some(Ok(WsMessage::Text(text.into())))
                    } else {
                        read.next().await
                    }
                } => {
                    let msg = match msg {
                        Some(m) => m?,
                        None => return Err(GatewayError::Closed("server closed".into())),
                    };
                    // Route by frame type: Binary through the persistent
                    // zlib-stream inflater, Text parsed directly. Never sniff
                    // bytes — mid-stream flush blocks don't start with 0x78,
                    // and misrouted bytes desync the stream.
                    let texts: Vec<String> = match msg {
                        WsMessage::Text(t) => vec![t.to_string()],
                        WsMessage::Binary(b) => zlib.feed(&b)?,
                        WsMessage::Ping(p) => {
                            write.send(WsMessage::Pong(p)).await?;
                            Vec::new()
                        }
                        WsMessage::Pong(_) | WsMessage::Frame(_) => Vec::new(),
                        WsMessage::Close(frame) => {
                            return Err(GatewayError::Closed(close_reason("server", frame)));
                        }
                    };
                    for text in texts {
                        // A single bad payload must never kill the session:
                        // warn with a snippet and move on.
                        let v: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                let snippet: String = text.chars().take(120).collect();
                                tracing::warn!("skipping unparsable payload: {e}");
                                let _ = events.send(CoreEvent::Warning(format!(
                                    "skipped bad payload ({e}): {snippet}"
                                )));
                                continue;
                            }
                        };
                        let op = v.get("op").and_then(Value::as_u64).unwrap_or(99);
                        match op {
                        0 => {
                            if let Some(s) = v.get("s").and_then(Value::as_u64) {
                                last_seq = Some(s);
                                *seq = Some(s);
                            }
                            let (incoming, warnings, _) = parse_envelope(&text)?;
                            for w in warnings {
                                let _ = events.send(CoreEvent::Warning(w));
                            }
                            for ev in incoming {
                                if let CoreEvent::Ready { session_id: sid, .. } = &ev {
                                    *session_id = Some(sid.clone());
                                }
                                let _ = events.send(ev);
                            }
                        }
                        7 => return Err(GatewayError::Closed("server asked reconnect".into())),
                        9 => {
                            let resumable = v.get("d").and_then(Value::as_bool).unwrap_or(false);
                            if !resumable {
                                *session_id = None;
                                *seq = None;
                            }
                            return Err(GatewayError::InvalidSession(resumable));
                        }
                        11 => { /* heartbeat ack */ }
                        _ => {}
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identify_matches_desktop_client() {
        let cfg = GatewayConfig::with_token(GatewayConfig::default_url(), "tok");
        let p = identify_payload(&cfg);
        assert_eq!(p["d"]["properties"]["browser"], "Discord Client");
        assert_eq!(p["d"]["properties"]["os"], "Windows");
        assert_eq!(p["d"]["properties"]["release_channel"], "stable");
        assert_eq!(p["d"]["properties"]["client_version"], "1.0.9249");
        assert_eq!(p["d"]["properties"]["client_build_number"], 582977);
        assert_eq!(p["d"]["compress"], false);
        // User-account shape: capabilities, not bot intents.
        assert_eq!(p["d"]["capabilities"], 16381);
        assert!(p["d"].get("intents").is_none());
        assert_eq!(p["d"]["presence"]["status"], "unknown");
        assert_eq!(p["d"]["presence"]["activities"], serde_json::json!([]));
        assert_eq!(
            p["d"]["client_state"],
            serde_json::json!({"api_code_version": 0, "guild_versions": {}})
        );
    }

    #[test]
    fn presence_payload_shape() {
        let activities = vec![serde_json::json!({"name": "encord", "type": 0})];
        let p = presence_payload("online", &activities, false);
        assert_eq!(p["op"], 3);
        assert_eq!(p["d"]["status"], "online");
        assert_eq!(p["d"]["activities"], serde_json::json!(activities));
        assert_eq!(p["d"]["afk"], false);
    }

    #[test]
    fn parses_ready_and_message_create() {
        let ready = r#"{"op":0,"s":1,"t":"READY","d":{"user":{"id":"3","username":"x"},"session_id":"abc","resume_gateway_url":"wss://gateway.discord.gg","private_channels":[{"id":"7","type":1,"recipients":[{"id":"9","username":"dm-friend"}]}]}}"#;
        let (events, warnings, seq) = parse_envelope(ready).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(seq, Some(1));
        assert!(events.iter().any(|e| matches!(e, CoreEvent::Ready { .. })));
        // DM channels fan out with a displayable name.
        let dm = events
            .iter()
            .find_map(|e| match e {
                CoreEvent::ChannelCreate(c) => Some(c),
                _ => None,
            })
            .expect("dm channel");
        assert_eq!(dm.recipients.len(), 1);

        let msg = r#"{"op":0,"s":2,"t":"MESSAGE_CREATE","d":{"id":"1","channel_id":"2","author":{"id":"3","username":"x"},"content":"hi","timestamp":"2026-01-01T00:00:00Z"}}"#;
        let (events, warnings, _) = parse_envelope(msg).unwrap();
        assert!(warnings.is_empty());
        assert!(events
            .iter()
            .any(|e| matches!(e, CoreEvent::MessageCreate(_))));
    }

    #[test]
    fn unknown_dispatch_is_forward_compat_none() {
        let v = r#"{"op":0,"s":3,"t":"SOME_FUTURE_EVENT","d":{}}"#;
        let (events, warnings, seq) = parse_envelope(v).unwrap();
        assert!(warnings.is_empty());
        assert!(events.is_empty());
        assert_eq!(seq, Some(3));
    }

    #[test]
    fn guild_create_fans_out_channels_members_presences() {
        let payload = serde_json::json!({
            "op": 0, "s": 4, "t": "GUILD_CREATE",
            "d": {
                "id": "100", "name": "g",
                "channels": [
                    {"id": "11", "type": 4, "name": "info", "position": 0},
                    {"id": "12", "type": 0, "name": "general", "position": 1}
                ],
                "members": [
                    {"user": {"id": "21", "username": "alice"}, "nick": "Ali"}
                ],
                "presences": [
                    {"user": {"id": "21"}, "status": "online"}
                ]
            }
        });
        let (events, warnings, _) = parse_envelope(&payload.to_string()).unwrap();
        assert!(warnings.is_empty());
        // 1 guild + 2 channels + 1 member chunk + 1 presence.
        assert_eq!(events.len(), 5);
        let channels: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                CoreEvent::ChannelCreate(c) => Some(c),
                _ => None,
            })
            .collect();
        assert_eq!(channels.len(), 2);
        // Nested channels inherit the guild id.
        assert!(channels
            .iter()
            .all(|c| c.guild_id == Some(Snowflake::new(100))));
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::GuildMembersChunk { members, .. } if members.len() == 1
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::PresenceUpdate { status, .. } if status == "online"
        )));
    }

    #[test]
    fn ready_parses_read_state() {
        let ready = r#"{"op":0,"s":1,"t":"READY","d":{"user":{"id":"3","username":"x"},"session_id":"abc","read_state":[{"id":"11","last_message_id":"100","mention_count":2},{"id":"12"}]}}"#;
        let (events, warnings, _) = parse_envelope(ready).unwrap();
        assert!(warnings.is_empty());
        let reads = events.iter().find_map(|e| match e {
            CoreEvent::ReadState(states) => Some(states),
            _ => None,
        });
        let reads = reads.expect("read state");
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].mention_count, 2);
        assert_eq!(reads[0].last_message_id, Some(Snowflake::new(100)));
        assert_eq!(reads[1].mention_count, 0);
        assert_eq!(reads[1].last_message_id, None);
    }

    #[test]
    fn message_with_mentions_attachments_reactions_parses() {
        let msg = serde_json::json!({
            "op": 0, "s": 2, "t": "MESSAGE_CREATE",
            "d": {
                "id": "1", "channel_id": "2",
                "author": {"id": "3", "username": "x"},
                "content": "hi <@4>",
                "timestamp": "2026-01-01T00:00:00Z",
                "mentions": [{"id": "4", "username": "y"}],
                "referenced_message": {
                    "id": "0", "channel_id": "2",
                    "author": {"id": "5", "username": "z"},
                    "content": "original text here",
                    "timestamp": "2026-01-01T00:00:00Z"
                },
                "attachments": [
                    {"id": "a1", "filename": "pic.png", "size": 42,
                     "url": "https://cdn/x.png", "proxy_url": "https://media/y.png",
                     "content_type": "image/png"}
                ],
                "reactions": [{"count": 2, "me": false, "emoji": {"name": "👍"}}]
            }
        });
        let (events, warnings, _) = parse_envelope(&msg.to_string()).unwrap();
        assert!(warnings.is_empty());
        let m = events.iter().find_map(|e| match e {
            CoreEvent::MessageCreate(m) => Some(m),
            _ => None,
        });
        let m = m.expect("message");
        assert_eq!(m.mentions.len(), 1);
        assert!(m.attachments[0].is_image());
        assert_eq!(m.reactions[0].display_emoji(), "👍");
        assert_eq!(m.referenced_message.as_ref().unwrap().author.username, "z");
    }

    #[test]
    fn guild_delete_distinguishes_outage_from_removal() {
        // Outage: `unavailable` set -> keep cache.
        let v = r#"{"op":0,"s":7,"t":"GUILD_DELETE","d":{"id":"200","unavailable":true}}"#;
        let (events, warnings, _) = parse_envelope(v).unwrap();
        assert!(warnings.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::GuildDelete { id, unavailable: true } if *id == Snowflake::new(200)
        )));
        // Kick/leave: no flag -> evict.
        let v = r#"{"op":0,"s":8,"t":"GUILD_DELETE","d":{"id":"201"}}"#;
        let (events, warnings, _) = parse_envelope(v).unwrap();
        assert!(warnings.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::GuildDelete { id, unavailable: false } if *id == Snowflake::new(201)
        )));
    }

    #[test]
    fn partial_message_update_becomes_patch() {
        // Embed-only unfurl: no author/timestamp -> patch, not a drop.
        let v =
            r#"{"op":0,"s":9,"t":"MESSAGE_UPDATE","d":{"id":"5","channel_id":"6","embeds":[]}}"#;
        let (events, warnings, _) = parse_envelope(v).unwrap();
        assert!(warnings.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::MessagePatch { channel_id, message_id, .. }
            if *channel_id == Snowflake::new(6) && *message_id == Snowflake::new(5)
        )));
        // Content edit with author still parses full.
        let v = r#"{"op":0,"s":10,"t":"MESSAGE_UPDATE","d":{"id":"5","channel_id":"6","author":{"id":"3","username":"x"},"content":"edited","timestamp":"2026-01-01T00:00:00Z"}}"#;
        let (events, warnings, _) = parse_envelope(v).unwrap();
        assert!(warnings.is_empty());
        assert!(events
            .iter()
            .any(|e| matches!(e, CoreEvent::MessageUpdate(_))));
    }

    /// Predicate over one parsed event, keeping the table below readable.
    type EventCheck = fn(&CoreEvent) -> bool;

    #[test]
    fn update_bulk_reaction_voice_thread_events_parse() {
        let cases: &[(&str, EventCheck)] = &[
            (
                r#"{"op":0,"s":1,"t":"CHANNEL_UPDATE","d":{"id":"11","type":0,"name":"renamed"}}"#,
                |e| matches!(e, CoreEvent::ChannelCreate(c) if c.name.as_deref() == Some("renamed")),
            ),
            (
                r#"{"op":0,"s":2,"t":"GUILD_UPDATE","d":{"id":"100","name":"g2"}}"#,
                |e| matches!(e, CoreEvent::GuildUpdate(g) if g.name == "g2"),
            ),
            (
                r#"{"op":0,"s":3,"t":"THREAD_CREATE","d":{"id":"31","type":11,"guild_id":"100","name":"thread"}}"#,
                |e| matches!(e, CoreEvent::ChannelCreate(c) if c.id == Snowflake::new(31)),
            ),
            (
                r#"{"op":0,"s":4,"t":"MESSAGE_DELETE_BULK","d":{"channel_id":"6","ids":["5","7"]}}"#,
                |e| matches!(e, CoreEvent::MessageDeleteBulk { message_ids, .. } if message_ids.len() == 2),
            ),
            (
                r#"{"op":0,"s":5,"t":"MESSAGE_REACTION_ADD","d":{"channel_id":"6","message_id":"5","user_id":"9","emoji":{"name":"👍"}}}"#,
                |e| matches!(e, CoreEvent::ReactionAdd { .. }),
            ),
            (
                r#"{"op":0,"s":6,"t":"MESSAGE_REACTION_REMOVE_ALL","d":{"channel_id":"6","message_id":"5"}}"#,
                |e| matches!(e, CoreEvent::ReactionRemoveAll { .. }),
            ),
            (
                r#"{"op":0,"s":7,"t":"VOICE_STATE_UPDATE","d":{"guild_id":"100","channel_id":"41","user_id":"9","session_id":"sess"}}"#,
                |e| matches!(e, CoreEvent::VoiceStateUpdate { .. }),
            ),
            (
                r#"{"op":0,"s":8,"t":"MESSAGE_ACK","d":{"channel_id":"6","message_id":"50"}}"#,
                |e| matches!(e, CoreEvent::MessageAck { .. }),
            ),
        ];
        for (raw, check) in cases {
            let (events, warnings, _) = parse_envelope(raw).unwrap();
            assert!(warnings.is_empty(), "warnings for {raw}");
            assert_eq!(events.len(), 1, "events for {raw}");
            assert!(check(&events[0]), "shape for {raw}");
        }
    }

    #[test]
    fn unavailable_guild_still_lists() {
        // Lazy/offline guilds arrive as `{id, unavailable: true}` with no
        // name; they must still surface (full data follows later).
        let v = r#"{"op":0,"s":7,"t":"GUILD_CREATE","d":{"id":"200","unavailable":true}}"#;
        let (events, warnings, _) = parse_envelope(v).unwrap();
        assert!(warnings.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::GuildCreate(g) if g.id == Snowflake::new(200)
        )));
    }

    #[test]
    fn member_add_update_remove_parse() {
        let add = r#"{"op":0,"s":5,"t":"GUILD_MEMBER_ADD","d":{"guild_id":"100","user":{"id":"31","username":"bob"},"nick":"Bobby"}}"#;
        let (events, warnings, _) = parse_envelope(add).unwrap();
        assert!(warnings.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::GuildMemberAdd { member, .. } if member.display() == "Bobby"
        )));

        let remove =
            r#"{"op":0,"s":6,"t":"GUILD_MEMBER_REMOVE","d":{"guild_id":"100","user":{"id":"31"}}}"#;
        let (events, warnings, _) = parse_envelope(remove).unwrap();
        assert!(warnings.is_empty());
        assert!(events.iter().any(|e| matches!(
            e,
            CoreEvent::GuildMemberRemove { user_id, .. } if *user_id == Snowflake::new(31)
        )));
    }

    #[test]
    fn heartbeat_payload_shape() {
        assert_eq!(
            heartbeat_payload(Some(5)),
            serde_json::json!({"op":1,"d":5})
        );
    }

    #[test]
    fn handshake_request_carries_ws_and_browser_headers() {
        let req = build_request("wss://gateway.discord.gg/?v=9&encoding=json").unwrap();
        let h = req.headers();
        for required in [
            "host",
            "connection",
            "upgrade",
            "sec-websocket-version",
            "sec-websocket-key",
            "origin",
            "user-agent",
        ] {
            assert!(h.contains_key(required), "missing {required}");
        }
        assert_eq!(h.get("upgrade").unwrap().to_str().unwrap(), "websocket");
    }

    /// Regression test: a bare custom request used to die in tungstenite's
    /// `generate_request` with `InvalidHeader(sec-websocket-key)` before any
    /// I/O. This completes a real handshake against a local server.
    #[tokio::test]
    async fn custom_request_completes_handshake_locally() {
        use futures_util::{SinkExt, StreamExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let msg = ws.next().await.unwrap().unwrap();
            ws.send(msg).await.unwrap();
        });

        let url = format!("ws://{addr}/?v=9&encoding=json");
        let request = build_request(&url).unwrap();
        let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        ws.send(WsMessage::Text("ping".into())).await.unwrap();
        let echo = ws.next().await.unwrap().unwrap();
        assert_eq!(echo.into_text().unwrap(), "ping");
        server.abort();
    }

    /// Regression test for "incomplete deflate stream": encode two payloads
    /// sharing a long token through ONE persistent compressor (exactly what
    /// Discord's zlib-stream looks like — the second message back-references
    /// the first), fragment the wire bytes down to 1 byte per feed, and
    /// require both payloads back byte-identical. A fresh-decoder-per-message
    /// implementation fails this.
    #[test]
    fn zlib_stream_decodes_fragmented_backref_messages() {
        use flate2::{Compress, Compression, FlushCompress};

        let rep = "sess_abcdefghijklmnopqrstuvwxyz0123456789";
        let msg1 = format!(r#"{{"op":10,"d":{{"heartbeat_interval":41250,"token":"{rep}"}}}}"#);
        let msg2 =
            format!(r#"{{"op":0,"s":1,"t":"READY","d":{{"token":"{rep}","extra":"{rep}"}}}}"#);

        let mut enc = Compress::new(Compression::default(), true);
        let mut wire = Vec::new();
        for msg in [&msg1, &msg2] {
            let mut chunk = Vec::with_capacity(msg.len() + 16);
            let status = enc
                .compress_vec(msg.as_bytes(), &mut chunk, FlushCompress::Sync)
                .unwrap();
            assert_eq!(status, flate2::Status::Ok);
            assert!(
                chunk.ends_with(&[0x00, 0x00, 0xff, 0xff]),
                "sync flush must terminate the chunk"
            );
            wire.extend_from_slice(&chunk);
        }

        // Worst case: one byte per feed.
        let mut dec = ZlibStream::new();
        let mut got = Vec::new();
        for byte in wire.chunks(1) {
            got.extend(dec.feed(byte).unwrap());
        }
        assert_eq!(got, [msg1.clone(), msg2.clone()]);

        // Partial, odd-sized fragments: nothing lost, nothing duplicated.
        let mut dec = ZlibStream::new();
        let mut got = Vec::new();
        let half = wire.len() / 2;
        for byte in wire[..half].chunks(3) {
            got.extend(dec.feed(byte).unwrap());
        }
        assert!(got.len() <= 1, "at most msg1 can complete in half the wire");
        for byte in wire[half..].chunks(5) {
            got.extend(dec.feed(byte).unwrap());
        }
        assert_eq!(got, [msg1, msg2]);
    }

    /// Coalesced frames (the initial guild burst): several flush-terminated
    /// payloads in ONE feed must come back as separate texts. The old
    /// whole-buffer decode returned one concatenated blob that killed the
    /// session at JSON parse.
    #[test]
    fn zlib_stream_splits_coalesced_flushes() {
        use flate2::{Compress, Compression, FlushCompress};

        let mut enc = Compress::new(Compression::default(), true);
        let mut wire = Vec::new();
        let mut expected = Vec::new();
        for i in 0..5 {
            let msg = format!(r#"{{"op":0,"s":{i},"t":"GUILD_CREATE","d":{{"id":"{i}"}}}}"#);
            let mut chunk = Vec::with_capacity(msg.len() + 16);
            enc.compress_vec(msg.as_bytes(), &mut chunk, FlushCompress::Sync)
                .unwrap();
            wire.extend_from_slice(&chunk);
            expected.push(msg);
        }

        // All five payloads in a single feed, like one fat frame.
        let mut dec = ZlibStream::new();
        let got = dec.feed(&wire).unwrap();
        assert_eq!(got, expected);

        // Split across two feeds mid-payload still works.
        let mut dec = ZlibStream::new();
        let cut = wire.len() * 2 / 5;
        let mut got = dec.feed(&wire[..cut]).unwrap();
        got.extend(dec.feed(&wire[cut..]).unwrap());
        assert_eq!(got, expected);
    }

    #[test]
    fn unparsable_entities_warn_instead_of_vanishing() {
        // Channel with a garbage id still yields the guild + a warning.
        let payload = serde_json::json!({
            "op": 0, "s": 8, "t": "GUILD_CREATE",
            "d": {
                "id": "100", "name": "g",
                "channels": [{"id": "not-a-snowflake", "type": 0}],
            }
        });
        let (events, warnings, _) = parse_envelope(&payload.to_string()).unwrap();
        assert!(events
            .iter()
            .any(|e| matches!(e, CoreEvent::GuildCreate(_))));
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("skipped 1 unparsable channels"));

        // Known state-carrying types warn; unknown futures stay silent.
        let bad_msg = r#"{"op":0,"s":9,"t":"MESSAGE_CREATE","d":{"nope":true}}"#;
        let (events, warnings, _) = parse_envelope(bad_msg).unwrap();
        assert!(events.is_empty());
        assert_eq!(
            warnings,
            vec!["dropping unparsable MESSAGE_CREATE".to_string()]
        );
        let future = r#"{"op":0,"s":10,"t":"SOME_FUTURE_EVENT","d":{}}"#;
        let (events, warnings, _) = parse_envelope(future).unwrap();
        assert!(events.is_empty() && warnings.is_empty());
    }
}
