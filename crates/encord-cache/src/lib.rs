//! encord-cache: SQLite (+ optional in-memory) store for guilds/channels/messages.
//! Abaddon parity: `memory_db = true` keeps everything in RAM; otherwise WAL-mode
//! SQLite so the UI can paint instantly (<150ms) before Gateway READY hydrates.

use encord_core::{Channel, ChannelRead, Guild, Member, Message, Snowflake, User};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct Cache {
    conn: Mutex<Connection>,
}

/// Small display helper shared by name lookups: usernames, with a bot tag.
fn user_label(user: &User) -> String {
    if user.bot {
        format!("{} [BOT]", user.username)
    } else {
        user.username.clone()
    }
}

impl Cache {
    /// Lock the SQLite handle, recovering from a poisoned mutex (a prior
    /// panic while holding the lock) instead of cascading panics across
    /// every later cache call.
    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn open(path: &Path) -> Result<Self, CacheError> {
        let conn = Connection::open(path)?;
        let cache = Self {
            conn: Mutex::new(conn),
        };
        cache.init()?;
        Ok(cache)
    }

    pub fn open_in_memory() -> Result<Self, CacheError> {
        let conn = Connection::open_in_memory()?;
        let cache = Self {
            conn: Mutex::new(conn),
        };
        cache.init()?;
        Ok(cache)
    }

    fn init(&self) -> Result<(), CacheError> {
        let conn = self.lock();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS guilds (id TEXT PRIMARY KEY, name TEXT NOT NULL, icon TEXT, json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, guild_id TEXT, name TEXT, kind INTEGER NOT NULL, json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, timestamp TEXT NOT NULL, json TEXT NOT NULL);
             CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id, timestamp);
             CREATE TABLE IF NOT EXISTS members (guild_id TEXT NOT NULL, user_id TEXT NOT NULL, json TEXT NOT NULL, PRIMARY KEY (guild_id, user_id));
             CREATE TABLE IF NOT EXISTS channel_state (channel_id TEXT PRIMARY KEY, last_read_id TEXT, mentions INTEGER NOT NULL DEFAULT 0);",
        )?;
        Ok(())
    }

    pub fn upsert_guild(&self, g: &Guild) -> Result<(), CacheError> {
        let json = serde_json::to_string(g)?;
        let conn = self.lock();
        conn.execute(
            "INSERT INTO guilds (id, name, icon, json) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET name=excluded.name, icon=excluded.icon, json=excluded.json",
            params![g.id.to_string(), g.name, g.icon, json],
        )?;
        Ok(())
    }

    pub fn upsert_channel(&self, c: &Channel) -> Result<(), CacheError> {
        let json = serde_json::to_string(c)?;
        let conn = self.lock();
        conn.execute(
            "INSERT INTO channels (id, guild_id, name, kind, json) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET guild_id=excluded.guild_id, name=excluded.name, kind=excluded.kind, json=excluded.json",
            params![
                c.id.to_string(),
                c.guild_id.map(|g| g.to_string()),
                c.name,
                c.kind,
                json
            ],
        )?;
        Ok(())
    }

    pub fn upsert_message(&self, m: &Message) -> Result<(), CacheError> {
        let json = serde_json::to_string(m)?;
        let conn = self.lock();
        conn.execute(
            "INSERT INTO messages (id, channel_id, timestamp, json) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET channel_id=excluded.channel_id, timestamp=excluded.timestamp, json=excluded.json",
            params![m.id.to_string(), m.channel_id.to_string(), m.timestamp, json],
        )?;
        // Keep only the latest 200 per channel (Abaddon-style trim; cheap bound).
        conn.execute(
            "DELETE FROM messages WHERE channel_id = ?1 AND id NOT IN
             (SELECT id FROM messages WHERE channel_id = ?1 ORDER BY timestamp DESC LIMIT 200)",
            params![m.channel_id.to_string()],
        )?;
        Ok(())
    }

    pub fn last_messages(
        &self,
        channel_id: Snowflake,
        limit: usize,
    ) -> Result<Vec<Message>, CacheError> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT json FROM messages WHERE channel_id = ?1 ORDER BY timestamp DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![channel_id.to_string(), limit as i64], |row| {
            row.get::<_, String>(0)
        })?;
        // Skip (and count) corrupt rows instead of failing the whole list:
        // one bad row must never blank an entire channel.
        let mut out = Vec::new();
        let mut skipped = 0;
        for json in rows {
            let json = json?;
            match serde_json::from_str(&json) {
                Ok(msg) => out.push(msg),
                Err(_) => skipped += 1,
            }
        }
        if skipped > 0 {
            tracing::warn!("skipped {skipped} corrupt cached messages");
        }
        out.reverse();
        Ok(out)
    }

    /// Evict a guild and its scoped rows (channels, members, messages,
    /// read state). Without this, kicked/removed guilds paint forever.
    pub fn remove_guild(&self, guild_id: Snowflake) -> Result<(), CacheError> {
        let conn = self.lock();
        let gid = guild_id.to_string();
        conn.execute("DELETE FROM guilds WHERE id = ?1", params![gid])?;
        conn.execute("DELETE FROM members WHERE guild_id = ?1", params![gid])?;
        // Channels carry guild_id as TEXT; collect ids first for cascades.
        let ids: Vec<String> = conn
            .prepare("SELECT id FROM channels WHERE guild_id = ?1")?
            .query_map(params![gid], |row| row.get(0))?
            .filter_map(Result::ok)
            .collect();
        conn.execute("DELETE FROM channels WHERE guild_id = ?1", params![gid])?;
        for cid in ids {
            conn.execute("DELETE FROM messages WHERE channel_id = ?1", params![cid])?;
            conn.execute(
                "DELETE FROM channel_state WHERE channel_id = ?1",
                params![cid],
            )?;
        }
        Ok(())
    }

    /// Evict one channel and its messages + read state.
    pub fn remove_channel(&self, channel_id: Snowflake) -> Result<(), CacheError> {
        let conn = self.lock();
        let cid = channel_id.to_string();
        conn.execute("DELETE FROM channels WHERE id = ?1", params![cid])?;
        conn.execute("DELETE FROM messages WHERE channel_id = ?1", params![cid])?;
        conn.execute(
            "DELETE FROM channel_state WHERE channel_id = ?1",
            params![cid],
        )?;
        Ok(())
    }

    pub fn delete_message(
        &self,
        channel_id: Snowflake,
        message_id: Snowflake,
    ) -> Result<(), CacheError> {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM messages WHERE id = ?1 AND channel_id = ?2",
            params![message_id.to_string(), channel_id.to_string()],
        )?;
        Ok(())
    }

    pub fn delete_messages(
        &self,
        channel_id: Snowflake,
        message_ids: &[Snowflake],
    ) -> Result<(), CacheError> {
        let conn = self.lock();
        for id in message_ids {
            conn.execute(
                "DELETE FROM messages WHERE id = ?1 AND channel_id = ?2",
                params![id.to_string(), channel_id.to_string()],
            )?;
        }
        Ok(())
    }

    pub fn get_message(
        &self,
        channel_id: Snowflake,
        message_id: Snowflake,
    ) -> Result<Option<Message>, CacheError> {
        let conn = self.lock();
        let json: Option<String> = conn
            .query_row(
                "SELECT json FROM messages WHERE id = ?1 AND channel_id = ?2",
                params![message_id.to_string(), channel_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        match json {
            Some(json) => Ok(serde_json::from_str(&json).ok()),
            None => Ok(None),
        }
    }

    /// Merge a subset MESSAGE_UPDATE into the cached row (embed-only
    /// payloads carry no author). Returns false when nothing was cached.
    pub fn patch_message(
        &self,
        channel_id: Snowflake,
        message_id: Snowflake,
        content: Option<&str>,
        edited_timestamp: Option<&str>,
    ) -> Result<bool, CacheError> {
        let Some(mut msg) = self.get_message(channel_id, message_id)? else {
            return Ok(false);
        };
        if let Some(content) = content {
            msg.content = content.to_string();
        }
        if let Some(ts) = edited_timestamp {
            msg.edited_timestamp = Some(ts.to_string());
        }
        self.upsert_message(&msg)?;
        Ok(true)
    }

    pub fn guild_count(&self) -> Result<usize, CacheError> {
        let conn = self.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM guilds", [], |r| r.get(0))?;
        Ok(n as usize)
    }

    pub fn upsert_member(&self, guild_id: Snowflake, m: &Member) -> Result<(), CacheError> {
        let json = serde_json::to_string(m)?;
        let conn = self.lock();
        conn.execute(
            "INSERT INTO members (guild_id, user_id, json) VALUES (?1, ?2, ?3)
             ON CONFLICT(guild_id, user_id) DO UPDATE SET json=excluded.json",
            params![guild_id.to_string(), m.user.id.to_string(), json],
        )?;
        Ok(())
    }

    pub fn remove_member(&self, guild_id: Snowflake, user_id: Snowflake) -> Result<(), CacheError> {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM members WHERE guild_id = ?1 AND user_id = ?2",
            params![guild_id.to_string(), user_id.to_string()],
        )?;
        Ok(())
    }

    pub fn list_members(&self, guild_id: Snowflake) -> Result<Vec<Member>, CacheError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT json FROM members WHERE guild_id = ?1")?;
        let rows = stmt.query_map(params![guild_id.to_string()], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for json in rows {
            let json = json?;
            if let Ok(item) = serde_json::from_str(&json) {
                out.push(item);
            } else {
                tracing::warn!("skipped corrupt cached row");
            }
        }
        Ok(out)
    }

    /// Display name for any cached member (nick preferred), any guild.
    pub fn find_member_display(&self, user_id: Snowflake) -> Result<Option<String>, CacheError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT json FROM members WHERE user_id = ?1 LIMIT 1")?;
        let json: Option<String> = stmt
            .query_row(params![user_id.to_string()], |row| row.get(0))
            .optional()?
            .flatten();
        match json {
            Some(json) => {
                let m: Member = serde_json::from_str(&json)?;
                Ok(Some(m.display()))
            }
            None => Ok(None),
        }
    }

    /// Display name for a member in one guild (nick preferred). Unlike
    /// [`Self::find_member_display`] this cannot pick a nickname from a
    /// different guild where the same user is also a member.
    pub fn member_display(
        &self,
        guild_id: Snowflake,
        user_id: Snowflake,
    ) -> Result<Option<String>, CacheError> {
        let conn = self.lock();
        let json: Option<String> = conn
            .query_row(
                "SELECT json FROM members WHERE guild_id = ?1 AND user_id = ?2",
                params![guild_id.to_string(), user_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(json
            .and_then(|json| serde_json::from_str::<Member>(&json).ok())
            .map(|m| m.display()))
    }

    /// Human-readable name for a user id. Prefers a guild member row (so
    /// nicknames show), then DM/group-DM recipients, then cached message
    /// authors. This keeps the event log and typing indicator free of raw
    /// snowflakes.
    pub fn find_user_name(&self, user_id: Snowflake) -> Result<Option<String>, CacheError> {
        if let Some(display) = self.find_member_display(user_id)? {
            return Ok(Some(display));
        }
        let conn = self.lock();
        let pattern = format!("%\"id\":\"{user_id}\"%");
        let mut stmt = conn.prepare("SELECT json FROM channels WHERE json LIKE ?1")?;
        let rows = stmt.query_map(params![pattern], |row| row.get::<_, String>(0))?;
        for json in rows {
            let json = json?;
            let Ok(channel) = serde_json::from_str::<Channel>(&json) else {
                continue;
            };
            if let Some(user) = channel.recipients.iter().find(|u| u.id == user_id) {
                return Ok(Some(user_label(user)));
            }
        }
        // DM channels often arrive without recipients, but cached message
        // authors still give us a human name for the log/typing line.
        let mut stmt = conn.prepare(
            "SELECT json FROM messages
             WHERE json_extract(json, '$.author.id') = ?1
             LIMIT 1",
        )?;
        let rows = stmt.query_map(params![user_id.to_string()], |row| row.get::<_, String>(0))?;
        for json in rows {
            let json = json?;
            if let Ok(message) = serde_json::from_str::<Message>(&json) {
                return Ok(Some(user_label(&message.author)));
            }
        }
        Ok(None)
    }

    /// Cached channel object (name + recipients) for readable labels.
    pub fn find_channel(&self, channel_id: Snowflake) -> Result<Option<Channel>, CacheError> {
        let conn = self.lock();
        let json: Option<String> = conn
            .query_row(
                "SELECT json FROM channels WHERE id = ?1",
                params![channel_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(json.and_then(|json| serde_json::from_str::<Channel>(&json).ok()))
    }

    /// Resolve up to two recent non-self message-author labels for each DM
    /// channel in one query. This names DMs whose `recipients` array is
    /// missing without issuing one query per channel.
    pub fn dm_author_labels(
        &self,
        channel_ids: &[Snowflake],
        self_id: Option<Snowflake>,
    ) -> Result<HashMap<Snowflake, String>, CacheError> {
        if channel_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let conn = self.lock();
        let placeholders = vec!["?"; channel_ids.len()].join(",");
        let sql = format!(
            "SELECT channel_id, json FROM (
                 SELECT channel_id, json,
                        ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY timestamp DESC) AS rn
                 FROM messages
                 WHERE channel_id IN ({placeholders})
             )
             WHERE rn <= 50
             ORDER BY channel_id, rn"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(channel_ids.iter().map(|id| id.to_string())),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;

        let mut names: HashMap<Snowflake, Vec<String>> = HashMap::new();
        for row in rows {
            let (channel_id, json) = row?;
            let Ok(channel_id) = channel_id.parse::<u64>() else {
                continue;
            };
            let channel_id = Snowflake::new(channel_id);
            if names
                .get(&channel_id)
                .is_some_and(|labels| labels.len() >= 2)
            {
                continue;
            }
            let Ok(message) = serde_json::from_str::<Message>(&json) else {
                continue;
            };
            if Some(message.author.id) == self_id {
                continue;
            }
            let label = user_label(&message.author);
            if label.is_empty() {
                continue;
            }
            let labels = names.entry(channel_id).or_default();
            if !labels.contains(&label) {
                labels.push(label);
            }
        }
        Ok(names
            .into_iter()
            .map(|(id, labels)| (id, labels.join(", ")))
            .collect())
    }

    /// Cached guild name (empty names report absence so callers can use a
    /// friendly fallback instead of painting a blank label).
    pub fn find_guild_name(&self, guild_id: Snowflake) -> Result<Option<String>, CacheError> {
        let conn = self.lock();
        let name: Option<String> = conn
            .query_row(
                "SELECT name FROM guilds WHERE id = ?1",
                params![guild_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(name.filter(|n| !n.trim().is_empty()))
    }

    pub fn list_guilds(&self) -> Result<Vec<Guild>, CacheError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT json FROM guilds ORDER BY name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for json in rows {
            let json = json?;
            if let Ok(item) = serde_json::from_str(&json) {
                out.push(item);
            } else {
                tracing::warn!("skipped corrupt cached row");
            }
        }
        Ok(out)
    }

    pub fn list_channels(&self) -> Result<Vec<Channel>, CacheError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT json FROM channels ORDER BY kind, name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for json in rows {
            let json = json?;
            if let Ok(item) = serde_json::from_str(&json) {
                out.push(item);
            } else {
                tracing::warn!("skipped corrupt cached row");
            }
        }
        Ok(out)
    }

    pub fn channel_name(&self, channel_id: Snowflake) -> Result<Option<String>, CacheError> {
        let conn = self.lock();
        let name: Option<String> = conn
            .query_row(
                "SELECT name FROM channels WHERE id = ?1",
                params![channel_id.to_string()],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        Ok(name)
    }

    /// Upsert per-channel read state (unread + mention tracking).
    pub fn upsert_read(
        &self,
        channel_id: Snowflake,
        last_read_id: Option<Snowflake>,
        mentions: u64,
    ) -> Result<(), CacheError> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO channel_state (channel_id, last_read_id, mentions) VALUES (?1, ?2, ?3)
             ON CONFLICT(channel_id) DO UPDATE SET last_read_id=excluded.last_read_id, mentions=excluded.mentions",
            params![
                channel_id.to_string(),
                last_read_id.map(|id| id.to_string()),
                mentions as i64
            ],
        )?;
        Ok(())
    }

    pub fn get_read(&self, channel_id: Snowflake) -> Result<ChannelRead, CacheError> {
        let conn = self.lock();
        let row: Option<(Option<String>, i64)> = conn
            .query_row(
                "SELECT last_read_id, mentions FROM channel_state WHERE channel_id = ?1",
                params![channel_id.to_string()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            Some((last, mentions)) => Ok(ChannelRead {
                channel_id,
                last_message_id: last.and_then(|s| s.parse::<u64>().ok()).map(Snowflake::new),
                mention_count: mentions.max(0) as u64,
            }),
            None => Ok(ChannelRead {
                channel_id,
                last_message_id: None,
                mention_count: 0,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use encord_core::User;

    fn msg(id: u64, ch: u64, ts: &str) -> Message {
        Message {
            id: Snowflake::new(id),
            channel_id: Snowflake::new(ch),
            guild_id: None,
            author: User {
                id: Snowflake::new(9),
                username: "u".into(),
                discriminator: "0".into(),
                avatar: None,
                bot: false,
            },
            content: format!("m{id}"),
            timestamp: ts.into(),
            edited_timestamp: None,
            tts: false,
            mention_everyone: false,
            mentions: Vec::new(),
            referenced_message: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
        }
    }

    #[test]
    fn roundtrip_and_trim() {
        let c = Cache::open_in_memory().unwrap();
        c.upsert_guild(&Guild {
            id: Snowflake::new(1),
            name: "g".into(),
            icon: None,
            unavailable: false,
        })
        .unwrap();
        assert_eq!(c.guild_count().unwrap(), 1);
        for i in 0..5 {
            c.upsert_message(&msg(i, 10, &format!("2026-01-01T00:00:0{i}Z")))
                .unwrap();
        }
        let last = c.last_messages(Snowflake::new(10), 50).unwrap();
        assert_eq!(last.len(), 5);
        assert_eq!(last[0].content, "m0");
        c.delete_message(Snowflake::new(10), Snowflake::new(0))
            .unwrap();
        assert_eq!(c.last_messages(Snowflake::new(10), 50).unwrap().len(), 4);
    }

    #[test]
    fn members_upsert_list_remove() {
        use encord_core::{Member, User};

        let member = |id: u64, name: &str, nick: Option<&str>| Member {
            user: User {
                id: Snowflake::new(id),
                username: name.into(),
                discriminator: "0".into(),
                avatar: None,
                bot: false,
            },
            nick: nick.map(str::to_string),
            roles: Vec::new(),
            joined_at: None,
        };

        let c = Cache::open_in_memory().unwrap();
        c.upsert_member(Snowflake::new(1), &member(10, "alice", Some("Ali")))
            .unwrap();
        c.upsert_member(Snowflake::new(1), &member(11, "bob", None))
            .unwrap();
        assert_eq!(c.list_members(Snowflake::new(1)).unwrap().len(), 2);
        assert_eq!(
            c.find_member_display(Snowflake::new(10)).unwrap(),
            Some("Ali".to_string())
        );
        assert_eq!(
            c.find_member_display(Snowflake::new(11)).unwrap(),
            Some("bob".to_string())
        );
        c.remove_member(Snowflake::new(1), Snowflake::new(10))
            .unwrap();
        assert_eq!(c.list_members(Snowflake::new(1)).unwrap().len(), 1);
        assert_eq!(c.find_member_display(Snowflake::new(10)).unwrap(), None);
    }

    #[test]
    fn resolves_names_for_logs_and_messages() {
        use encord_core::{Member, User};

        let c = Cache::open_in_memory().unwrap();
        c.upsert_guild(&Guild {
            id: Snowflake::new(1),
            name: "Guild Name".into(),
            icon: None,
            unavailable: false,
        })
        .unwrap();
        c.upsert_member(
            Snowflake::new(1),
            &Member {
                user: User {
                    id: Snowflake::new(10),
                    username: "alice".into(),
                    discriminator: "0".into(),
                    avatar: None,
                    bot: false,
                },
                nick: Some("Ali".into()),
                roles: Vec::new(),
                joined_at: None,
            },
        )
        .unwrap();
        let dm_user = User {
            id: Snowflake::new(20),
            username: "bob".into(),
            discriminator: "0".into(),
            avatar: None,
            bot: false,
        };
        c.upsert_channel(&Channel {
            id: Snowflake::new(30),
            kind: 1,
            guild_id: None,
            name: None,
            topic: None,
            nsfw: false,
            parent_id: None,
            position: 0,
            recipients: vec![dm_user],
        })
        .unwrap();

        assert_eq!(
            c.find_guild_name(Snowflake::new(1)).unwrap(),
            Some("Guild Name".into())
        );
        assert_eq!(
            c.member_display(Snowflake::new(1), Snowflake::new(10))
                .unwrap(),
            Some("Ali".into())
        );
        assert_eq!(
            c.find_user_name(Snowflake::new(10)).unwrap(),
            Some("Ali".into())
        );
        // Not a guild member: resolved through the DM recipient list.
        assert_eq!(
            c.find_user_name(Snowflake::new(20)).unwrap(),
            Some("bob".into())
        );
        assert_eq!(
            c.find_channel(Snowflake::new(30))
                .unwrap()
                .unwrap()
                .recipients[0]
                .username,
            "bob"
        );
        assert_eq!(c.find_user_name(Snowflake::new(99)).unwrap(), None);
        // Message authors fill the gap when a DM has no recipient rows.
        let mut m = msg(100, 30, "2026-01-01T00:00:00Z");
        m.author.id = Snowflake::new(99);
        m.author.username = "carol".into();
        c.upsert_message(&m).unwrap();
        assert_eq!(
            c.find_user_name(Snowflake::new(99)).unwrap(),
            Some("carol".into())
        );
    }

    #[test]
    fn batches_dm_author_labels() {
        let c = Cache::open_in_memory().unwrap();
        // Oldest to newest: self, bob, carol. Labels are newest-first and
        // exclude the current user.
        for (id, author_id, name, ts) in [
            (1, 9, "me", "2026-01-01T00:00:01Z"),
            (2, 20, "bob", "2026-01-01T00:00:02Z"),
            (3, 21, "carol", "2026-01-01T00:00:03Z"),
        ] {
            let mut m = msg(id, 30, ts);
            m.author.id = Snowflake::new(author_id);
            m.author.username = name.into();
            c.upsert_message(&m).unwrap();
        }

        let labels = c
            .dm_author_labels(&[Snowflake::new(30)], Some(Snowflake::new(9)))
            .unwrap();
        assert_eq!(
            labels.get(&Snowflake::new(30)).map(String::as_str),
            Some("carol, bob")
        );
        assert!(c.dm_author_labels(&[], None).unwrap().is_empty());
    }

    #[test]
    fn remove_guild_and_channel_cascade() {
        let c = Cache::open_in_memory().unwrap();
        c.upsert_guild(&Guild {
            id: Snowflake::new(1),
            name: "g".into(),
            icon: None,
            unavailable: false,
        })
        .unwrap();
        c.upsert_channel(&encord_core::Channel {
            id: Snowflake::new(11),
            kind: 0,
            guild_id: Some(Snowflake::new(1)),
            name: Some("general".into()),
            topic: None,
            nsfw: false,
            parent_id: None,
            position: 0,
            recipients: Vec::new(),
        })
        .unwrap();
        c.upsert_message(&msg(7, 11, "2026-01-01T00:00:00Z"))
            .unwrap();
        c.upsert_read(Snowflake::new(11), Some(Snowflake::new(7)), 1)
            .unwrap();

        // Single-channel eviction clears messages + read state.
        c.remove_channel(Snowflake::new(11)).unwrap();
        assert!(c.last_messages(Snowflake::new(11), 50).unwrap().is_empty());
        assert!(c.list_channels().unwrap().is_empty());

        // Guild eviction clears members and any remaining scoped rows.
        c.upsert_channel(&encord_core::Channel {
            id: Snowflake::new(12),
            kind: 0,
            guild_id: Some(Snowflake::new(1)),
            name: Some("chat".into()),
            topic: None,
            nsfw: false,
            parent_id: None,
            position: 1,
            recipients: Vec::new(),
        })
        .unwrap();
        c.upsert_member(
            Snowflake::new(1),
            &encord_core::Member {
                user: User {
                    id: Snowflake::new(9),
                    username: "u".into(),
                    discriminator: "0".into(),
                    avatar: None,
                    bot: false,
                },
                nick: None,
                roles: Vec::new(),
                joined_at: None,
            },
        )
        .unwrap();
        c.remove_guild(Snowflake::new(1)).unwrap();
        assert!(c.list_guilds().unwrap().is_empty());
        assert!(c.list_channels().unwrap().is_empty());
        assert!(c.list_members(Snowflake::new(1)).unwrap().is_empty());
    }

    #[test]
    fn patch_and_bulk_delete_roundtrip() {
        let c = Cache::open_in_memory().unwrap();
        for i in 0..3 {
            c.upsert_message(&msg(i, 10, &format!("2026-01-01T00:00:0{i}Z")))
                .unwrap();
        }
        // Subset edit merges content, keeps author.
        assert!(c
            .patch_message(Snowflake::new(10), Snowflake::new(1), Some("edited"), None)
            .unwrap());
        let got = c
            .get_message(Snowflake::new(10), Snowflake::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(got.content, "edited");
        assert_eq!(got.author.username, "u");
        // Unknown id reports absence instead of failing.
        assert!(!c
            .patch_message(Snowflake::new(10), Snowflake::new(99), Some("x"), None)
            .unwrap());
        c.delete_messages(Snowflake::new(10), &[Snowflake::new(0), Snowflake::new(2)])
            .unwrap();
        let rest = c.last_messages(Snowflake::new(10), 50).unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].id, Snowflake::new(1));
    }

    #[test]
    fn read_state_upsert_and_default() {
        let c = Cache::open_in_memory().unwrap();
        let ch = Snowflake::new(77);
        let read = c.get_read(ch).unwrap();
        assert_eq!(read.last_message_id, None);
        assert_eq!(read.mention_count, 0);
        c.upsert_read(ch, Some(Snowflake::new(500)), 3).unwrap();
        let read = c.get_read(ch).unwrap();
        assert_eq!(read.last_message_id, Some(Snowflake::new(500)));
        assert_eq!(read.mention_count, 3);
        c.upsert_read(ch, Some(Snowflake::new(600)), 0).unwrap();
        let read = c.get_read(ch).unwrap();
        assert_eq!(read.last_message_id, Some(Snowflake::new(600)));
        assert_eq!(read.mention_count, 0);
    }
}
