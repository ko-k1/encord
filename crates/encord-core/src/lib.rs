//! encord-core: shared types, snowflakes, gateway-agnostic events, errors.
//! Mirrors Abaddon's `discord/` types layer: all crates depend on this, never vice-versa.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Discord snowflake id. Wire format is a string ("123"); we store u64.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Snowflake(pub u64);

impl Serialize for Snowflake {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Snowflake {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct IdVisitor;

        impl serde::de::Visitor<'_> for IdVisitor {
            type Value = Snowflake;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a Discord snowflake as string or integer")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Snowflake, E> {
                v.parse::<u64>()
                    .map(Snowflake)
                    .map_err(|_| E::custom(format!("invalid snowflake: {v}")))
            }

            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Snowflake, E> {
                self.visit_str(&v)
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Snowflake, E> {
                Ok(Snowflake(v))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Snowflake, E> {
                u64::try_from(v)
                    .map(Snowflake)
                    .map_err(|_| E::custom(format!("invalid snowflake: {v}")))
            }
        }

        d.deserialize_any(IdVisitor)
    }
}

impl Snowflake {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Snowflake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Snowflake {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

/// Minimal user object (subset of Discord user).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
    pub id: Snowflake,
    pub username: String,
    #[serde(default)]
    pub discriminator: String,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub bot: bool,
}

/// Minimal guild object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Guild {
    pub id: Snowflake,
    /// Empty for `unavailable` guild stubs (full data arrives later).
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub unavailable: bool,
}

/// Minimal channel object (guild text / DM / voice / thread).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Channel {
    pub id: Snowflake,
    #[serde(rename = "type")]
    pub kind: i32,
    #[serde(default)]
    pub guild_id: Option<Snowflake>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub nsfw: bool,
    /// Category id for guild channels.
    #[serde(default)]
    pub parent_id: Option<Snowflake>,
    /// Sort position within its group.
    #[serde(default)]
    pub position: i32,
    /// DM/group-DM participants (used to name DMs from READY).
    #[serde(default)]
    pub recipients: Vec<User>,
}

impl Channel {
    /// True for channels that host plain chat (selectable in the UI).
    /// Covers guild text/DM/group-DM/announcement plus public, private
    /// and announcement threads (10/11/12).
    pub fn is_text_like(&self) -> bool {
        matches!(self.kind, 0 | 1 | 3 | 5 | 10 | 11 | 12)
    }

    pub fn is_voice_like(&self) -> bool {
        matches!(self.kind, 2 | 13)
    }

    pub fn is_category(&self) -> bool {
        self.kind == 4
    }
}

/// Minimal guild member object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Member {
    pub user: User,
    #[serde(default)]
    pub nick: Option<String>,
    #[serde(default)]
    pub roles: Vec<Snowflake>,
    #[serde(default)]
    pub joined_at: Option<String>,
}

impl Member {
    pub fn display(&self) -> String {
        let base = self
            .nick
            .clone()
            .unwrap_or_else(|| self.user.username.clone());
        if self.user.bot {
            format!("{base} [BOT]")
        } else {
            base
        }
    }
}

/// Minimal message object for cache + UI paint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: Snowflake,
    pub channel_id: Snowflake,
    #[serde(default)]
    pub guild_id: Option<Snowflake>,
    pub author: User,
    pub content: String,
    pub timestamp: String,
    #[serde(default)]
    pub edited_timestamp: Option<String>,
    #[serde(default)]
    pub tts: bool,
    #[serde(default)]
    pub mention_everyone: bool,
    #[serde(default)]
    pub mentions: Vec<User>,
    #[serde(default)]
    pub referenced_message: Option<Box<Message>>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    #[serde(default)]
    pub reactions: Vec<Reaction>,
}

/// Message attachment (images render inline, others as file rows).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attachment {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub proxy_url: String,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub content_type: Option<String>,
}

impl Attachment {
    pub fn is_image(&self) -> bool {
        self.content_type
            .as_deref()
            .map(|c| c.starts_with("image/"))
            .unwrap_or_else(|| {
                let lower = self.filename.to_lowercase();
                lower.ends_with(".png")
                    || lower.ends_with(".jpg")
                    || lower.ends_with(".jpeg")
                    || lower.ends_with(".gif")
                    || lower.ends_with(".webp")
            })
    }

    /// Best URL for download (proxy avoids Cloudflare hotlink quirks).
    pub fn download_url(&self) -> &str {
        if self.proxy_url.is_empty() {
            &self.url
        } else {
            &self.proxy_url
        }
    }
}

/// Partial emoji object inside reactions.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Emoji {
    #[serde(default)]
    pub id: Option<Snowflake>,
    #[serde(default)]
    pub name: Option<String>,
}

impl Emoji {
    /// Short human label: unicode emoji as-is, custom emoji as `:name:`.
    pub fn label(&self) -> String {
        match (&self.id, &self.name) {
            (_, Some(name)) if self.id.is_none() => name.clone(),
            (Some(_), Some(name)) => format!(":{name}:"),
            _ => "?".to_string(),
        }
    }
}

/// Message reaction (display only in M1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reaction {
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub me: bool,
    #[serde(default)]
    pub emoji: Emoji,
}

impl Reaction {
    pub fn display_emoji(&self) -> String {
        self.emoji.label()
    }
}

/// Per-channel read state (unread + mention tracking).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelRead {
    pub channel_id: Snowflake,
    #[serde(default)]
    pub last_message_id: Option<Snowflake>,
    #[serde(default)]
    pub mention_count: u64,
}

/// Core events emitted by gateway crate toward UI / plugins.
/// UI must never talk to gateway directly; it subscribes to these.
#[derive(Clone, Debug)]
pub enum CoreEvent {
    Ready {
        user: User,
        session_id: String,
        resume_url: String,
    },
    GuildCreate(Guild),
    /// `unavailable=true` means outage (keep cache, re-sync follows);
    /// absent means the user left/was removed (evict).
    GuildDelete {
        id: Snowflake,
        unavailable: bool,
    },
    GuildUpdate(Guild),
    ChannelCreate(Channel),
    ChannelDelete(Snowflake),
    MessageCreate(Message),
    MessageUpdate(Message),
    /// Subset update (embed-only edits carry no author): merge into the
    /// cached row instead of replacing it.
    MessagePatch {
        channel_id: Snowflake,
        message_id: Snowflake,
        content: Option<String>,
        edited_timestamp: Option<String>,
    },
    MessageDelete {
        channel_id: Snowflake,
        message_id: Snowflake,
    },
    MessageDeleteBulk {
        channel_id: Snowflake,
        message_ids: Vec<Snowflake>,
    },
    ReactionAdd {
        channel_id: Snowflake,
        message_id: Snowflake,
        emoji: Emoji,
    },
    ReactionRemove {
        channel_id: Snowflake,
        message_id: Snowflake,
        emoji: Emoji,
    },
    ReactionRemoveAll {
        channel_id: Snowflake,
        message_id: Snowflake,
    },
    ReactionRemoveEmoji {
        channel_id: Snowflake,
        message_id: Snowflake,
        emoji: Emoji,
    },
    /// Another session acked this channel; sync read state + badges.
    MessageAck {
        channel_id: Snowflake,
        message_id: Snowflake,
    },
    TypingStart {
        channel_id: Snowflake,
        user_id: Snowflake,
    },
    PresenceUpdate {
        user_id: Snowflake,
        status: String,
        guild_id: Option<Snowflake>,
    },
    /// Bulk members nested in GUILD_CREATE (guild context can't be
    /// inferred from the member objects alone).
    GuildMembersChunk {
        guild_id: Snowflake,
        members: Vec<Member>,
    },
    GuildMemberAdd {
        guild_id: Snowflake,
        member: Member,
    },
    GuildMemberUpdate {
        guild_id: Snowflake,
        member: Member,
    },
    GuildMemberRemove {
        guild_id: Snowflake,
        user_id: Snowflake,
    },
    /// Bulk read states nested in READY.
    ReadState(Vec<ChannelRead>),
    /// Local diagnostic surfaced in the event log (parse skips, bad
    /// payloads). Never comes from Discord; keeps one bad shape from
    /// silently blanking the UI.
    Warning(String),
    VoiceStateUpdate {
        guild_id: Option<Snowflake>,
        channel_id: Option<Snowflake>,
        user_id: Snowflake,
        session_id: String,
    },
    Resumed,
    Disconnected {
        reason: String,
    },
}

/// Commands sent by UI toward core (single writer principle).
#[derive(Clone, Debug)]
pub enum UiCommand {
    SendMessage {
        channel_id: Snowflake,
        content: String,
    },
    EditMessage {
        channel_id: Snowflake,
        message_id: Snowflake,
        content: String,
    },
    DeleteMessage {
        channel_id: Snowflake,
        message_id: Snowflake,
    },
    JoinVoice {
        guild_id: Snowflake,
        channel_id: Snowflake,
    },
    LeaveVoice {
        guild_id: Snowflake,
    },
    SetStatus {
        status: String,
    },
    Shutdown,
}

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid snowflake: {0}")]
    Snowflake(String),
    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_display_parse_roundtrip() {
        let id = Snowflake::new(123456789);
        let s = id.to_string();
        assert_eq!(s, "123456789");
        let back: Snowflake = s.parse().unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn channel_kind_helpers() {
        let mut c = Channel {
            id: Snowflake::new(1),
            kind: 0,
            guild_id: None,
            name: Some("general".into()),
            topic: None,
            nsfw: false,
            parent_id: None,
            position: 0,
            recipients: Vec::new(),
        };
        assert!(c.is_text_like() && !c.is_category());
        c.kind = 4;
        assert!(c.is_category() && !c.is_text_like());
        c.kind = 2;
        assert!(c.is_voice_like());
    }

    #[test]
    fn member_display_prefers_nick() {
        let m = Member {
            user: User {
                id: Snowflake::new(1),
                username: "alice".into(),
                discriminator: "0".into(),
                avatar: None,
                bot: false,
            },
            nick: Some("Ali".into()),
            roles: Vec::new(),
            joined_at: None,
        };
        assert_eq!(m.display(), "Ali");
    }

    #[test]
    fn snowflake_wire_compat_string_and_int() {
        let from_str: Snowflake = serde_json::from_value(serde_json::json!("123456789")).unwrap();
        assert_eq!(from_str, Snowflake::new(123456789));
        let from_int: Snowflake = serde_json::from_value(serde_json::json!(123456789)).unwrap();
        assert_eq!(from_int, Snowflake::new(123456789));
        // Serializes back as string (Discord wire format).
        assert_eq!(
            serde_json::to_value(from_int).unwrap(),
            serde_json::json!("123456789")
        );
    }

    #[test]
    fn attachment_and_reaction_helpers() {
        let img = Attachment {
            id: "1".into(),
            filename: "pic.PNG".into(),
            size: 10,
            url: "https://x/y".into(),
            proxy_url: String::new(),
            width: None,
            height: None,
            content_type: None,
        };
        assert!(img.is_image());
        assert_eq!(img.download_url(), "https://x/y");
        let r = Reaction {
            count: 3,
            me: false,
            emoji: Emoji {
                id: None,
                name: Some("👍".into()),
            },
        };
        assert_eq!(r.display_emoji(), "👍");
    }

    #[test]
    fn message_deserializes_minimal() {
        let v = serde_json::json!({
            "id": "1",
            "channel_id": "2",
            "author": {"id": "3", "username": "x"},
            "content": "hi",
            "timestamp": "2026-01-01T00:00:00Z"
        });
        let msg: Message = serde_json::from_value(v).unwrap();
        assert_eq!(msg.id, Snowflake::new(1));
        assert_eq!(msg.channel_id, Snowflake::new(2));
        assert_eq!(msg.author.id, Snowflake::new(3));
    }
}
