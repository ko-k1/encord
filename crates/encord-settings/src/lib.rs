//! encord-settings: abaddon.ini-compatible INI settings.
//! Sections/keys mirror Abaddon so users can migrate easily.
//! `#` starts comments; section and key names are case-sensitive.

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ini parse: {0}")]
    Ini(String),
}

#[derive(Clone, Debug)]
pub struct DiscordSettings {
    /// Override gateway URL (must be json + zlib-stream). Empty = default.
    pub gateway: String,
    /// Override REST base URL. Empty = https://discord.com/api.
    pub api_base: String,
    /// Keep Discord data in memory instead of SQLite.
    pub memory_db: bool,
    /// User token (prefer OS keyring; ini is fallback like Abaddon).
    pub token: String,
    /// Auto-download avatars/attachments on new messages.
    pub prefetch: bool,
    /// Auto-connect on launch.
    pub autoconnect: bool,
    /// Store token in OS keychain when available.
    pub keychain: bool,
    /// IDENTIFY `os` override. Empty = official desktop default.
    pub os: String,
    /// IDENTIFY `browser` override. Empty = official desktop default.
    pub browser: String,
}

impl Default for DiscordSettings {
    fn default() -> Self {
        Self {
            gateway: String::new(),
            api_base: "https://discord.com/api".to_string(),
            memory_db: false,
            token: String::new(),
            prefetch: false,
            autoconnect: false,
            keychain: true,
            os: String::new(),
            browser: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GuiSettings {
    pub stock_emojis: bool,
    pub custom_emojis: bool,
    pub css: String,
    pub theme: String,
    pub animations: bool,
    pub animated_guild_hover_only: bool,
    pub owner_crown: bool,
    pub unreads: bool,
    pub save_state: bool,
    pub hide_to_tray: bool,
    pub show_deleted_indicator: bool,
    pub font_scale: f64,
}

impl Default for GuiSettings {
    fn default() -> Self {
        Self {
            stock_emojis: true,
            custom_emojis: true,
            css: "res/css/encord.css".to_string(),
            theme: "midnight-club".to_string(),
            animations: true,
            animated_guild_hover_only: true,
            owner_crown: true,
            unreads: true,
            save_state: true,
            hide_to_tray: false,
            show_deleted_indicator: true,
            font_scale: 1.0,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct HttpSettings {
    /// REST `User-Agent` override. Empty = stock desktop UA (kept in sync
    /// with super-properties automatically).
    pub user_agent: String,
}

#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub discord: DiscordSettings,
    pub gui: GuiSettings,
    pub http: HttpSettings,
}

impl Settings {
    /// Default config path: cwd `encord.ini`, else `%APPDATA%/encord/encord.ini`
    /// (mirrors Abaddon's `abaddon.ini` lookup).
    pub fn default_path() -> PathBuf {
        let cwd = PathBuf::from("encord.ini");
        if cwd.exists() {
            return cwd;
        }
        if let Ok(appdata) = std::env::var("APPDATA") {
            return PathBuf::from(appdata).join("encord").join("encord.ini");
        }
        cwd
    }

    pub fn load(path: &Path) -> Result<Self, SettingsError> {
        let text = std::fs::read_to_string(path)?;
        Self::parse(&text)
    }

    /// Write back Abaddon-style ini. Only the sections/keys encord manages;
    /// unknown keys in the original file are dropped (documented tradeoff).
    pub fn save(&self, path: &Path) -> Result<(), SettingsError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let text = format!(
            "# encord.ini — managed by encord. Edit while encord is closed.\n\
             # `#` starts comments. Section and key names are case-sensitive.\n\
             \n\
             [discord]\n\
             gateway = {gateway}\n\
             api_base = {api_base}\n\
             memory_db = {memory_db}\n\
             token = {token}\n\
             prefetch = {prefetch}\n\
             autoconnect = {autoconnect}\n\
             keychain = {keychain}\n\
             os = {os}\n\
             browser = {browser}\n\
             \n\
             [gui]\n\
              stock_emojis = {stock_emojis}\n\
              custom_emojis = {custom_emojis}\n\
              css = {css}\n\
              theme = {theme}\n\
              animations = {animations}\n\
              animated_guild_hover_only = {animated_guild_hover_only}\n\
              owner_crown = {owner_crown}\n\
              unreads = {unreads}\n\
              save_state = {save_state}\n\
              hide_to_tray = {hide_to_tray}\n\
              show_deleted_indicator = {show_deleted_indicator}\n\
              font_scale = {font_scale}\n\
              \n\
              [http]\n\
              user_agent = {http_user_agent}\n",
            gateway = self.discord.gateway,
            api_base = self.discord.api_base,
            memory_db = self.discord.memory_db,
            token = self.discord.token,
            prefetch = self.discord.prefetch,
            autoconnect = self.discord.autoconnect,
            keychain = self.discord.keychain,
            os = self.discord.os,
            browser = self.discord.browser,
            stock_emojis = self.gui.stock_emojis,
            custom_emojis = self.gui.custom_emojis,
            css = self.gui.css,
            theme = self.gui.theme,
            animations = self.gui.animations,
            animated_guild_hover_only = self.gui.animated_guild_hover_only,
            owner_crown = self.gui.owner_crown,
            unreads = self.gui.unreads,
            save_state = self.gui.save_state,
            hide_to_tray = self.gui.hide_to_tray,
            show_deleted_indicator = self.gui.show_deleted_indicator,
            font_scale = self.gui.font_scale,
            http_user_agent = self.http.user_agent,
        );
        std::fs::write(path, text)?;
        Ok(())
    }

    pub fn parse(text: &str) -> Result<Self, SettingsError> {
        // Minimal INI parser (no extra dep): `[section]`, `key = value`,
        // `#` full-line comments, case-sensitive names. Matches Abaddon subset.
        use std::collections::BTreeMap;
        let mut sections: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        let mut current = String::new();
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if line.starts_with('[') && line.ends_with(']') {
                current = line[1..line.len() - 1].trim().to_string();
                sections.entry(current.clone()).or_default();
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                let val = v.trim().to_string();
                sections
                    .entry(current.clone())
                    .or_default()
                    .insert(k.trim().to_string(), val);
            }
        }
        let get = |sec: &str, key: &str| -> Option<String> {
            sections.get(sec).and_then(|m| m.get(key)).cloned()
        };

        let mut s = Settings::default();

        if let Some(v) = get("discord", "gateway") {
            s.discord.gateway = v;
        }
        if let Some(v) = get("discord", "api_base") {
            s.discord.api_base = v;
        }
        if let Some(v) = get("discord", "memory_db") {
            s.discord.memory_db = parse_bool(&v);
        }
        if let Some(v) = get("discord", "token") {
            s.discord.token = v;
        }
        if let Some(v) = get("discord", "prefetch") {
            s.discord.prefetch = parse_bool(&v);
        }
        if let Some(v) = get("discord", "autoconnect") {
            s.discord.autoconnect = parse_bool(&v);
        }
        if let Some(v) = get("discord", "keychain") {
            s.discord.keychain = parse_bool(&v);
        }
        if let Some(v) = get("discord", "os") {
            s.discord.os = v;
        }
        if let Some(v) = get("discord", "browser") {
            s.discord.browser = v;
        }

        if let Some(v) = get("gui", "stock_emojis") {
            s.gui.stock_emojis = parse_bool(&v);
        }
        if let Some(v) = get("gui", "custom_emojis") {
            s.gui.custom_emojis = parse_bool(&v);
        }
        if let Some(v) = get("gui", "css") {
            s.gui.css = v;
        }
        if let Some(v) = get("gui", "theme") {
            s.gui.theme = v;
        }
        if let Some(v) = get("gui", "animations") {
            s.gui.animations = parse_bool(&v);
        }
        if let Some(v) = get("gui", "animated_guild_hover_only") {
            s.gui.animated_guild_hover_only = parse_bool(&v);
        }
        if let Some(v) = get("gui", "owner_crown") {
            s.gui.owner_crown = parse_bool(&v);
        }
        if let Some(v) = get("gui", "unreads") {
            s.gui.unreads = parse_bool(&v);
        }
        if let Some(v) = get("gui", "save_state") {
            s.gui.save_state = parse_bool(&v);
        }
        if let Some(v) = get("gui", "hide_to_tray") {
            s.gui.hide_to_tray = parse_bool(&v);
        }
        if let Some(v) = get("gui", "show_deleted_indicator") {
            s.gui.show_deleted_indicator = parse_bool(&v);
        }
        if let Some(v) = get("gui", "font_scale") {
            s.gui.font_scale = v.parse().unwrap_or(1.0);
        }

        if let Some(v) = get("http", "user_agent") {
            s.http.user_agent = v;
        }

        if s.discord.api_base.is_empty() {
            s.discord.api_base = "https://discord.com/api".to_string();
        }
        Ok(s)
    }
}

fn parse_bool(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_abaddon_style_ini() {
        let text = "# comment\n[discord]\ntoken = abc\nmemory_db = true\nautoconnect = false\n\n[gui]\ncss = res/css/encord.css\nfont_scale = 1.25\n";
        let s = Settings::parse(text).unwrap();
        assert_eq!(s.discord.token, "abc");
        assert!(s.discord.memory_db);
        assert!(!s.discord.autoconnect);
        assert_eq!(s.gui.css, "res/css/encord.css");
        assert!((s.gui.font_scale - 1.25).abs() < f64::EPSILON);
    }

    #[test]
    fn defaults_are_sane() {
        let s = Settings::default();
        assert_eq!(s.discord.api_base, "https://discord.com/api");
        assert!(s.discord.keychain);
        assert!(s.gui.animations);
    }

    #[test]
    fn save_roundtrip() {
        let mut s = Settings::default();
        s.discord.token = "tok123".to_string();
        s.discord.autoconnect = true;
        s.gui.font_scale = 1.5;
        // Previously dropped on save; must survive a roundtrip.
        s.gui.animated_guild_hover_only = false;
        s.gui.owner_crown = false;
        s.gui.unreads = false;
        s.gui.save_state = false;
        let dir = std::env::temp_dir().join("encord-settings-test");
        let path = dir.join("encord.ini");
        s.save(&path).unwrap();
        let back = Settings::load(&path).unwrap();
        assert_eq!(back.discord.token, "tok123");
        assert!(back.discord.autoconnect);
        assert!((back.gui.font_scale - 1.5).abs() < f64::EPSILON);
        assert!(!back.gui.animated_guild_hover_only);
        assert!(!back.gui.owner_crown);
        assert!(!back.gui.unreads);
        assert!(!back.gui.save_state);
        std::fs::remove_dir_all(&dir).ok();
    }
}
