//! encord binary: Abaddon-style native client.
//!
//! Default (no flags) opens the GPU-rendered window:
//!   encord [--config encord.ini]
//! Headless helpers:
//!   encord --config encord.ini --check     validate setup, no network
//!   encord --config encord.ini --me        GET /users/@me
//!   encord --config encord.ini --gateway   stream gateway events to stdout log

use anyhow::{bail, Context, Result};
use encord_cache::Cache;
use encord_core::{Channel, CoreEvent};
use encord_gateway::{Gateway, GatewayCmd, GatewayConfig};
use encord_settings::Settings;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let config_path = flag_value(&args, "--config")
        .map(PathBuf::from)
        .unwrap_or_else(Settings::default_path);

    tracing::info!("loading {}", config_path.display());
    let settings = if config_path.exists() {
        Settings::load(&config_path).with_context(|| format!("load {}", config_path.display()))?
    } else {
        tracing::warn!("config not found, using defaults");
        Settings::default()
    };

    let theme_id = validate_theme_and_css(&settings);
    let cache = open_cache(&settings, &config_path)?;

    if args.iter().any(|a| a == "--check") {
        println!("encord check ok (theme={theme_id})");
        return Ok(());
    }

    // Headless paths need async; GUI owns the main thread so each builds
    // its own runtime only when used.
    let rt = || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("build tokio runtime")
    };

    if args.iter().any(|a| a == "--me") {
        require_token(&settings, &config_path)?;
        let rt = rt()?;
        rt.block_on(async {
            let rest = rest_for(&settings)?;
            let me = rest.me().await?;
            println!("{}", serde_json::to_string_pretty(&me)?);
            Ok::<_, anyhow::Error>(())
        })?;
        return Ok(());
    }

    if args.iter().any(|a| a == "--gateway") {
        let token = require_token(&settings, &config_path)?;
        let url = gateway_url(&settings);
        let rt = rt()?;
        rt.block_on(async {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let (_gw_tx, gw_rx) = tokio::sync::mpsc::unbounded_channel();
            let gw = Gateway::new(gateway_config(&settings, url, token), tx, gw_rx);
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    tracing::info!(?ev, "gateway event");
                }
            });
            gw.run().await?;
            Ok::<_, anyhow::Error>(())
        })?;
        return Ok(());
    }

    if args.iter().any(|a| a == "--diag") {
        // Sync diagnostic: connect with a time budget, histogram every
        // event, persist like the GUI does, then print a census. Close the
        // GUI first so both processes don't write the DB at once.
        let secs: u64 = flag_value(&args, "--diag-secs")
            .and_then(|s| s.parse().ok())
            .unwrap_or(40);
        let rt = rt()?;
        rt.block_on(diag(&settings, &config_path, secs))?;
        return Ok(());
    }

    run_gui(settings, config_path, cache, theme_id)
}

/// `--diag` implementation: ground truth for "guilds don't appear".
async fn diag(settings: &Settings, config_path: &Path, secs: u64) -> Result<()> {
    use std::collections::HashMap;

    let token = require_token(settings, config_path)?;
    let url = gateway_url(settings);
    println!("diag: connecting to {url} for {secs}s (close the GUI first)");
    let cache = open_cache(settings, config_path)?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<CoreEvent>();
    let (_gw_tx, gw_rx) = tokio::sync::mpsc::unbounded_channel::<GatewayCmd>();
    let gw = Gateway::new(gateway_config(settings, url, token), tx, gw_rx);
    let handle = tokio::spawn(async move { gw.run().await });

    let deadline = tokio::time::sleep(Duration::from_secs(secs));
    tokio::pin!(deadline);
    let mut hist: HashMap<&'static str, usize> = HashMap::new();
    let mut disconnects: Vec<String> = Vec::new();
    let mut ready_user = String::new();
    loop {
        tokio::select! {
            Some(ev) = rx.recv() => {
                // Persist exactly like the GUI path does.
                match &ev {
                    CoreEvent::Ready { user, .. } => {
                        ready_user = user.username.clone();
                    }
                    CoreEvent::GuildCreate(g) | CoreEvent::GuildUpdate(g) => {
                        if let Err(e) = cache.upsert_guild(g) {
                            println!("diag: upsert_guild failed: {e}");
                        }
                    }
                    CoreEvent::ChannelCreate(c) => {
                        if let Err(e) = cache.upsert_channel(c) {
                            println!("diag: upsert_channel failed: {e}");
                        }
                    }
                    CoreEvent::GuildDelete { id, unavailable } => {
                        // Outage blips must not wipe the cache; only real
                        // removals evict (same rule as the GUI path).
                        if !unavailable {
                            if let Err(e) = cache.remove_guild(*id) {
                                println!("diag: remove_guild failed: {e}");
                            }
                        }
                    }
                    CoreEvent::ChannelDelete(id) => {
                        if let Err(e) = cache.remove_channel(*id) {
                            println!("diag: remove_channel failed: {e}");
                        }
                    }
                    CoreEvent::MessageCreate(m) | CoreEvent::MessageUpdate(m) => {
                        let _ = cache.upsert_message(m);
                    }
                    CoreEvent::MessagePatch {
                        channel_id,
                        message_id,
                        content,
                        edited_timestamp,
                    } => {
                        let _ = cache.patch_message(
                            *channel_id,
                            *message_id,
                            content.as_deref(),
                            edited_timestamp.as_deref(),
                        );
                    }
                    CoreEvent::MessageDelete {
                        channel_id,
                        message_id,
                    } => {
                        let _ = cache.delete_message(*channel_id, *message_id);
                    }
                    CoreEvent::MessageDeleteBulk {
                        channel_id,
                        message_ids,
                    } => {
                        let _ = cache.delete_messages(*channel_id, message_ids);
                    }
                    CoreEvent::ReactionAdd {
                        channel_id,
                        message_id,
                        emoji,
                    } => {
                        apply_reaction(&cache, *channel_id, *message_id, emoji, true);
                    }
                    CoreEvent::ReactionRemove {
                        channel_id,
                        message_id,
                        emoji,
                    } => {
                        apply_reaction(&cache, *channel_id, *message_id, emoji, false);
                    }
                    CoreEvent::ReactionRemoveAll {
                        channel_id,
                        message_id,
                    } => {
                        clear_reactions(&cache, *channel_id, *message_id, None);
                    }
                    CoreEvent::ReactionRemoveEmoji {
                        channel_id,
                        message_id,
                        emoji,
                    } => {
                        clear_reactions(&cache, *channel_id, *message_id, Some(emoji));
                    }
                    CoreEvent::MessageAck {
                        channel_id,
                        message_id,
                    } => {
                        let read = cache.get_read(*channel_id).unwrap_or(
                            encord_core::ChannelRead {
                                channel_id: *channel_id,
                                last_message_id: None,
                                mention_count: 0,
                            },
                        );
                        let _ = cache.upsert_read(
                            *channel_id,
                            Some(*message_id),
                            read.mention_count,
                        );
                    }
                    CoreEvent::GuildMembersChunk { guild_id, members } => {
                        for m in members {
                            let _ = cache.upsert_member(*guild_id, m);
                        }
                    }
                    CoreEvent::GuildMemberAdd { guild_id, member }
                    | CoreEvent::GuildMemberUpdate { guild_id, member } => {
                        let _ = cache.upsert_member(*guild_id, member);
                    }
                    CoreEvent::Disconnected { reason } => {
                        disconnects.push(reason.clone());
                    }
                    _ => {}
                }
                *hist.entry(event_name(&ev)).or_insert(0) += 1;
            }
            _ = &mut deadline => break,
        }
    }
    handle.abort();

    println!("diag: ready_user={ready_user:?}");
    let mut names: Vec<_> = hist.iter().collect();
    names.sort_by_key(|(name, _)| name.to_string());
    println!("diag: event histogram:");
    for (name, count) in names {
        println!("diag:   {name}: {count}");
    }
    if disconnects.is_empty() {
        println!("diag: disconnects: none");
    } else {
        println!("diag: disconnects:");
        for reason in disconnects.iter().take(10) {
            println!("diag:   {reason}");
        }
    }
    let guilds = cache.list_guilds().unwrap_or_default();
    let channels = cache.list_channels().unwrap_or_default().len();
    let mut members = 0;
    for g in &guilds {
        members += cache.list_members(g.id).unwrap_or_default().len();
    }
    println!(
        "diag: cache census: {} guilds, {channels} channels, {members} members",
        guilds.len()
    );
    for g in guilds.iter().take(20) {
        println!("diag:   guild {} {:?}", g.id, g.name);
    }
    Ok(())
}

fn event_name(ev: &CoreEvent) -> &'static str {
    match ev {
        CoreEvent::Ready { .. } => "READY",
        CoreEvent::Resumed => "RESUMED",
        CoreEvent::GuildCreate(_) => "GUILD_CREATE",
        CoreEvent::GuildDelete { .. } => "GUILD_DELETE",
        CoreEvent::GuildUpdate(_) => "GUILD_UPDATE",
        CoreEvent::ChannelCreate(_) => "CHANNEL_CREATE",
        CoreEvent::ChannelDelete(_) => "CHANNEL_DELETE",
        CoreEvent::MessageCreate(_) => "MESSAGE_CREATE",
        CoreEvent::MessageUpdate(_) => "MESSAGE_UPDATE",
        CoreEvent::MessagePatch { .. } => "MESSAGE_UPDATE",
        CoreEvent::MessageDelete { .. } => "MESSAGE_DELETE",
        CoreEvent::MessageDeleteBulk { .. } => "MESSAGE_DELETE_BULK",
        CoreEvent::ReactionAdd { .. } => "MESSAGE_REACTION_ADD",
        CoreEvent::ReactionRemove { .. } => "MESSAGE_REACTION_REMOVE",
        CoreEvent::ReactionRemoveAll { .. } => "MESSAGE_REACTION_REMOVE_ALL",
        CoreEvent::ReactionRemoveEmoji { .. } => "MESSAGE_REACTION_REMOVE_EMOJI",
        CoreEvent::MessageAck { .. } => "MESSAGE_ACK",
        CoreEvent::TypingStart { .. } => "TYPING_START",
        CoreEvent::PresenceUpdate { .. } => "PRESENCE_UPDATE",
        CoreEvent::VoiceStateUpdate { .. } => "VOICE_STATE_UPDATE",
        CoreEvent::GuildMembersChunk { .. } => "GUILD_MEMBERS_CHUNK",
        CoreEvent::GuildMemberAdd { .. } => "GUILD_MEMBER_ADD",
        CoreEvent::GuildMemberUpdate { .. } => "GUILD_MEMBER_UPDATE",
        CoreEvent::GuildMemberRemove { .. } => "GUILD_MEMBER_REMOVE",
        CoreEvent::ReadState(_) => "READ_STATE",
        CoreEvent::Warning(_) => "WARNING",
        CoreEvent::Disconnected { .. } => "DISCONNECTED",
    }
}

/// Bump or drop one reaction on the cached message, then persist.
/// Same helper serves the GUI path and `--diag` so both stay in sync.
fn apply_reaction(
    cache: &Cache,
    channel_id: encord_core::Snowflake,
    message_id: encord_core::Snowflake,
    emoji: &encord_core::Emoji,
    add: bool,
) {
    let Ok(Some(mut msg)) = cache.get_message(channel_id, message_id) else {
        return;
    };
    let same = |r: &encord_core::Reaction| r.emoji.id == emoji.id && r.emoji.name == emoji.name;
    if add {
        match msg.reactions.iter_mut().find(|r| same(r)) {
            Some(r) => r.count += 1,
            None => msg.reactions.push(encord_core::Reaction {
                count: 1,
                me: false,
                emoji: emoji.clone(),
            }),
        }
    } else if let Some(pos) = msg.reactions.iter().position(same) {
        if msg.reactions[pos].count <= 1 {
            msg.reactions.remove(pos);
        } else {
            msg.reactions[pos].count -= 1;
        }
    } else {
        return;
    }
    let _ = cache.upsert_message(&msg);
}

/// Clear reactions on the cached message: all, or all of one emoji.
fn clear_reactions(
    cache: &Cache,
    channel_id: encord_core::Snowflake,
    message_id: encord_core::Snowflake,
    emoji: Option<&encord_core::Emoji>,
) {
    let Ok(Some(mut msg)) = cache.get_message(channel_id, message_id) else {
        return;
    };
    match emoji {
        Some(e) => msg
            .reactions
            .retain(|r| !(r.emoji.id == e.id && r.emoji.name == e.name)),
        None => msg.reactions.clear(),
    }
    let _ = cache.upsert_message(&msg);
}

fn api_base(settings: &Settings) -> String {
    if settings.discord.api_base.is_empty() {
        format!("https://discord.com/api/v{}", encord_rest::API_VERSION)
    } else {
        format!(
            "{}/v{}",
            settings.discord.api_base.trim_end_matches('/'),
            encord_rest::API_VERSION
        )
    }
}

fn gateway_url(settings: &Settings) -> String {
    if settings.discord.gateway.is_empty() {
        GatewayConfig::default_url()
    } else {
        settings.discord.gateway.clone()
    }
}

/// Gateway config with ini overrides (`[discord] os`/`browser`, empty =
/// official desktop defaults from encord-identity).
fn gateway_config(settings: &Settings, url: String, token: String) -> GatewayConfig {
    let mut cfg = GatewayConfig::with_token(url, token);
    if !settings.discord.os.is_empty() {
        cfg.os = settings.discord.os.clone();
    }
    if !settings.discord.browser.is_empty() {
        cfg.browser = settings.discord.browser.clone();
    }
    cfg
}

/// REST client with desktop fingerprint + ini UA override.
fn rest_for(settings: &Settings) -> Result<encord_rest::Rest, anyhow::Error> {
    let ua = if settings.http.user_agent.is_empty() {
        None
    } else {
        Some(settings.http.user_agent.clone())
    };
    Ok(encord_rest::Rest::with_options(
        api_base(settings),
        &settings.discord.token,
        ua,
    )?)
}

fn require_token(settings: &Settings, config_path: &Path) -> Result<String> {
    if settings.discord.token.is_empty() {
        bail!(
            "no token: set token in {} ([discord] token = ...)",
            config_path.display()
        );
    }
    Ok(settings.discord.token.clone())
}

fn open_cache(settings: &Settings, config_path: &Path) -> Result<Cache> {
    if settings.discord.memory_db {
        tracing::info!("cache: in-memory");
        Ok(Cache::open_in_memory()?)
    } else {
        let dir = config_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let db_path = dir.join("encord.db");
        tracing::info!("cache: {}", db_path.display());
        Ok(Cache::open(&db_path)?)
    }
}

/// Loads `themes/midnight-club.toml` + the configured CSS, returns theme id.
/// CSS is validated, never executed (native render, no JS).
fn validate_theme_and_css(settings: &Settings) -> String {
    let theme_path = PathBuf::from("themes/midnight-club.toml");
    let mut theme_id = "built-in".to_string();
    if theme_path.exists() {
        match encord_theme::Theme::load_toml(&theme_path) {
            Ok(theme) => {
                tracing::info!("theme {} ({} tokens)", theme.id, theme.tokens.len());
                theme_id = theme.id.clone();
            }
            Err(e) => tracing::warn!("theme load failed: {e}"),
        }
    }
    let css_path = PathBuf::from(&settings.gui.css);
    if css_path.exists() {
        match std::fs::read_to_string(&css_path) {
            Ok(css) => {
                let unknown = encord_theme::validate_css_selectors(&css);
                if unknown.is_empty() {
                    tracing::info!("css ok: {}", css_path.display());
                } else {
                    tracing::warn!("css unknown selectors: {unknown:?}");
                }
            }
            Err(e) => tracing::warn!("css read failed: {e}"),
        }
    }
    theme_id
}

/// Default path: paint from cache, spawn the network core on a background
/// thread, run the GPU window on the main thread.
fn run_gui(settings: Settings, config_path: PathBuf, cache: Cache, theme_id: String) -> Result<()> {
    let media = encord_media::MediaCache::new(encord_media::default_dir(&config_path))?;
    let _ = MEDIA.set(media);
    let cache = Arc::new(cache);
    let status = if settings.discord.token.is_empty() {
        format!(
            "offline — set token in {} · {theme_id}",
            config_path.display()
        )
    } else if settings.discord.autoconnect {
        format!("connecting… · {theme_id}")
    } else {
        format!("press Connect · {theme_id}")
    };

    let feed = encord_ui::UiFeed {
        status,
        connect_label: if settings.discord.autoconnect && !settings.discord.token.is_empty() {
            "Disconnect".to_string()
        } else {
            "Connect".to_string()
        },
        guild_name: "encord".to_string(),
        token: settings.discord.token.clone(),
        autoconnect: settings.discord.autoconnect,
        themes: list_themes(),
        current_theme: settings.gui.theme.clone(),
        guilds: guild_items(&cache),
        channels: channel_items(&cache, None),
        online: Vec::new(),
        offline: Vec::new(),
        channel_title: String::new(),
        channel_topic: String::new(),
        typing_text: String::new(),
        messages: vec![encord_ui::ChatMsg {
            id: "welcome".to_string(),
            author: "encord".to_string(),
            body: "Set your token in Settings (gear icon), Save, then press Connect.".to_string(),
            time: "now".to_string(),
            reply: String::new(),
            reactions: String::new(),
            attachments: Vec::new(),
        }],
    };

    // Network core lives on its own thread with its own runtime; it pushes
    // typed updates the UI timer drains. Slint keeps the main thread.
    // Buttons send commands through this channel (Connect, select, save…).
    let (ui_tx, ui_rx) = std::sync::mpsc::channel::<encord_ui::UiUpdate>();
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<CoreCmd>();
    let config_clone = config_path.clone();
    std::thread::Builder::new()
        .name("encord-core".into())
        .spawn(move || core_thread(settings, config_clone, cache, ui_tx, cmd_rx))
        .context("spawn core thread")?;

    let cb = encord_ui::UiCallbacks {
        connect: Box::new({
            let t = cmd_tx.clone();
            move || {
                let _ = t.send(CoreCmd::Toggle);
            }
        }),
        select_channel: Box::new({
            let t = cmd_tx.clone();
            move |id: String| {
                let _ = t.send(CoreCmd::SelectChannel(id));
            }
        }),
        select_guild: Box::new({
            let t = cmd_tx.clone();
            move |id: String| {
                let _ = t.send(CoreCmd::SelectGuild(id));
            }
        }),
        send: Box::new({
            let t = cmd_tx.clone();
            move |text: String| {
                let _ = t.send(CoreCmd::Send(text));
            }
        }),
        save: Box::new({
            let t = cmd_tx.clone();
            move |token: String, autoconnect: bool, theme: String| {
                let _ = t.send(CoreCmd::SaveSettings {
                    token,
                    autoconnect,
                    theme,
                });
            }
        }),
        reload_css: Box::new({
            let t = cmd_tx.clone();
            move || {
                let _ = t.send(CoreCmd::ReloadCss);
            }
        }),
    };
    encord_ui::run(feed, ui_rx, cb)
}

/// UI -> core commands.
#[derive(Clone, Debug)]
enum CoreCmd {
    Toggle,
    SelectChannel(String),
    SelectGuild(String),
    Send(String),
    SaveSettings {
        token: String,
        autoconnect: bool,
        theme: String,
    },
    ReloadCss,
}

/// Mutable core state: settings, selection, desired session.
struct CoreState {
    settings: Settings,
    config_path: PathBuf,
    selected_guild: Option<String>,
    selected_channel: Option<String>,
    want: bool,
    /// Our own user id (mention detection), set on READY.
    self_id: Option<String>,
    /// "guild_id:user_id" -> status ("online"/"idle"/"dnd"/"offline").
    presences: HashMap<String, String>,
    /// (channel_id, user_id) -> (display, last TYPING_START).
    typing: HashMap<(String, String), (String, Instant)>,
    last_typing: String,
    /// Resolved discord-ipc `client_id` -> application name cache (fills
    /// the presence `name` games omit; falls back to the client id).
    app_names: HashMap<String, String>,
    /// Latest Rich Presence from the local discord-ipc pipe; applied live
    /// when connected and re-applied on every (re)connect.
    pending_presence: Option<GatewayCmd>,
    session_active: bool,
    /// Live lane into the current gateway session (OP 3 presence, later OP 4
    /// voice). `None` between sessions; recreated per session.
    gw_tx: Option<tokio::sync::mpsc::UnboundedSender<GatewayCmd>>,
}

type UiTx = std::sync::mpsc::Sender<encord_ui::UiUpdate>;

/// Process-wide media cache, initialised once in run_gui before the core
/// starts. Lets list builders resolve cached icon/thumbnail paths (and lets
/// background download tasks run `'static`) without threading handles
/// through every call.
static MEDIA: std::sync::OnceLock<encord_media::MediaCache> = std::sync::OnceLock::new();

fn media() -> &'static encord_media::MediaCache {
    MEDIA.get().expect("media cache initialised in run_gui")
}

fn ui_send(tx: &UiTx, update: encord_ui::UiUpdate) {
    let _ = tx.send(update);
}

fn push_guilds(cache: &Cache, tx: &UiTx) {
    ui_send(tx, encord_ui::UiUpdate::Guilds(guild_items(cache)));
}

/// (unread, mention_count) for one channel from cached read state +
/// latest cached message. Snowflakes are time-ordered, so ids compare.
fn channel_unread(cache: &Cache, channel_id: &str) -> (bool, i32) {
    let id = channel_id
        .parse::<u64>()
        .ok()
        .map(encord_core::Snowflake::new);
    let latest = id.and_then(|i| cache.last_messages(i, 1).unwrap_or_default().pop());
    let read = id
        .map(|i| {
            cache.get_read(i).unwrap_or(encord_core::ChannelRead {
                channel_id: i,
                last_message_id: None,
                mention_count: 0,
            })
        })
        .unwrap_or(encord_core::ChannelRead {
            channel_id: encord_core::Snowflake::new(0),
            last_message_id: None,
            mention_count: 0,
        });
    let unread = match (latest, read.last_message_id) {
        (Some(l), Some(r)) => l.id.0 > r.0,
        (Some(_), None) => true,
        _ => false,
    };
    (unread || read.mention_count > 0, read.mention_count as i32)
}

fn guild_items(cache: &Cache) -> Vec<encord_ui::GuildItem> {
    let mut items = vec![encord_ui::GuildItem {
        id: "home".to_string(),
        label: "🏠".to_string(),
        unread: false,
        icon_path: String::new(),
    }];
    for g in cache.list_guilds().unwrap_or_default() {
        let unread = cache
            .list_channels()
            .unwrap_or_default()
            .iter()
            .filter(|c| c.guild_id == Some(g.id))
            .any(|c| channel_unread(cache, &c.id.to_string()).0);
        let icon_path = g
            .icon
            .as_ref()
            .and_then(|hash| {
                let url = encord_media::MediaCache::guild_icon_url(g.id.0, hash);
                media()
                    .cached("icons", &url, Some("image/png"))
                    .map(|p| p.to_string_lossy().to_string())
            })
            .unwrap_or_default();
        items.push(encord_ui::GuildItem {
            id: g.id.to_string(),
            label: initials(&g.name),
            unread,
            icon_path,
        });
    }
    items
}

/// Display name for a channel: stored name, DM participant list, or fallback.
fn channel_display(c: &Channel) -> String {
    if let Some(name) = &c.name {
        return name.clone();
    }
    if !c.recipients.is_empty() {
        return c
            .recipients
            .iter()
            .map(|u| u.username.clone())
            .collect::<Vec<_>>()
            .join(", ");
    }
    format!("channel {}", c.id)
}

/// Sorted, decorated channel rows: categories as headers, voice with icon.
/// Sort key mirrors Discord: parent category position, categories first,
/// then position.
fn channel_items(cache: &Cache, guild: Option<&str>) -> Vec<encord_ui::ChannelItem> {
    let all = cache.list_channels().unwrap_or_default();
    let parent_pos = |id: &str| -> i32 {
        all.iter()
            .find(|c| c.id.to_string() == id)
            .map(|c| c.position)
            .unwrap_or(0)
    };
    let mut rows: Vec<&Channel> = all
        .iter()
        .filter(|c| match (guild, c.guild_id) {
            // A guild shows its own channels; home (None) shows DMs only.
            (Some(g), Some(id)) => id.to_string() == g,
            (Some(_), None) => false,
            (None, None) => true,
            (None, Some(_)) => false,
        })
        .collect();
    rows.sort_by_key(|c| {
        (
            c.parent_id
                .map(|p| parent_pos(&p.to_string()))
                .unwrap_or(-1),
            if c.is_category() { 0 } else { 1 },
            c.position,
        )
    });
    rows.into_iter()
        .map(|c| {
            let id = c.id.to_string();
            let (unread, mentions) = channel_unread(cache, &id);
            if c.is_category() {
                encord_ui::ChannelItem {
                    id,
                    name: channel_display(c).to_uppercase(),
                    header: true,
                    unread: false,
                    mentions: 0,
                }
            } else if c.is_voice_like() {
                encord_ui::ChannelItem {
                    id,
                    name: format!("🔊 {}", channel_display(c)),
                    header: false,
                    unread: false,
                    mentions: 0,
                }
            } else {
                encord_ui::ChannelItem {
                    id,
                    name: format!("# {}", channel_display(c)),
                    header: false,
                    unread,
                    mentions,
                }
            }
        })
        .collect()
}

fn push_channels(cache: &Cache, tx: &UiTx, guild: Option<&str>) {
    ui_send(
        tx,
        encord_ui::UiUpdate::Channels(channel_items(cache, guild)),
    );
}

fn chat_msg_of(m: &encord_core::Message) -> encord_ui::ChatMsg {
    let mut author = m.author.username.clone();
    if m.author.bot {
        author.push_str(" [BOT]");
    }
    let reply = m
        .referenced_message
        .as_ref()
        .map(|r| format!("↩ {}: {}", r.author.username, truncate(&r.content, 80)))
        .unwrap_or_default();
    let reactions = m
        .reactions
        .iter()
        .map(|r| format!("{} {}", r.display_emoji(), r.count))
        .collect::<Vec<_>>()
        .join("  ");
    let attachments = m
        .attachments
        .iter()
        .map(|a| {
            let (is_image, path) = if a.is_image() {
                let path = media()
                    .cached("attachments", a.download_url(), a.content_type.as_deref())
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                (true, path)
            } else {
                (false, String::new())
            };
            encord_ui::ChatAttachment {
                filename: if a.filename.is_empty() {
                    "attachment".to_string()
                } else {
                    format!("{} ({})", a.filename, human_bytes(a.size))
                },
                is_image,
                path,
            }
        })
        .collect();
    let mut time = short_time(&m.timestamp);
    if m.edited_timestamp.is_some() {
        time.push_str(" ✎");
    }
    encord_ui::ChatMsg {
        id: m.id.to_string(),
        author,
        body: m.content.clone(),
        time,
        reply,
        reactions,
        attachments,
    }
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn push_messages(cache: &Cache, tx: &UiTx, channel: Option<&str>) {
    ui_send(
        tx,
        encord_ui::UiUpdate::Messages(message_items(cache, channel)),
    );
}

fn message_items(cache: &Cache, channel: Option<&str>) -> Vec<encord_ui::ChatMsg> {
    match channel
        .and_then(|id| id.parse::<u64>().ok())
        .map(encord_core::Snowflake::new)
    {
        Some(id) => cache
            .last_messages(id, 50)
            .unwrap_or_default()
            .iter()
            .map(chat_msg_of)
            .collect(),
        None => Vec::new(),
    }
}

/// Online-first member snapshot for the selected guild, presence overlaid.
fn push_members(cache: &Cache, tx: &UiTx, state: &CoreState) {
    let (online, offline) = member_lists(cache, state);
    ui_send(tx, encord_ui::UiUpdate::Members { online, offline });
}

fn member_lists(
    cache: &Cache,
    state: &CoreState,
) -> (Vec<encord_ui::MemberData>, Vec<encord_ui::MemberData>) {
    let empty = (Vec::new(), Vec::new());
    let Some(guild) = state.selected_guild.clone() else {
        return empty;
    };
    let Ok(gid) = guild.parse::<u64>() else {
        return empty;
    };
    let members = cache
        .list_members(encord_core::Snowflake::new(gid))
        .unwrap_or_default();
    let mut online = Vec::new();
    let mut offline = Vec::new();
    for m in members {
        let key = format!("{guild}:{}", m.user.id);
        let status = state
            .presences
            .get(&key)
            .cloned()
            .unwrap_or_else(|| "offline".to_string());
        let item = encord_ui::MemberData {
            user_id: m.user.id.to_string(),
            display: m.display(),
            status: status.clone(),
        };
        if status == "offline" || status == "invisible" {
            offline.push(item);
        } else {
            online.push(item);
        }
    }
    online.sort_by_key(|a| a.display.to_lowercase());
    offline.sort_by_key(|a| a.display.to_lowercase());
    (online, offline)
}

/// Typing line for the open channel; entries older than 10s are dropped
/// (Discord sends no stop-typing event).
fn push_typing(state: &mut CoreState, tx: &UiTx) {
    let now = Instant::now();
    state
        .typing
        .retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(10));
    let mut names: Vec<String> = state
        .typing
        .iter()
        .filter(|((ch, _), _)| Some(ch.as_str()) == state.selected_channel.as_deref())
        .map(|(_, (name, _))| name.clone())
        .collect();
    names.sort();
    names.dedup();
    let text = match names.len() {
        0 => String::new(),
        1 => format!("{} is typing…", names[0]),
        _ => format!("{} are typing…", names.join(", ")),
    };
    if text != state.last_typing {
        state.last_typing = text.clone();
        ui_send(tx, encord_ui::UiUpdate::Typing(text));
    }
}

/// Load `themes/<id>.toml` (fallback: midnight-club) and push live colors.
fn apply_theme(theme_id: &str, tx: &UiTx) {
    let mut path = PathBuf::from("themes").join(format!("{theme_id}.toml"));
    if !path.exists() {
        path = PathBuf::from("themes/midnight-club.toml");
    }
    let theme = encord_theme::Theme::load_toml(&path).unwrap_or_default();
    let get = |key: &str, fallback: &str| {
        theme
            .get(key)
            .map(str::to_string)
            .unwrap_or_else(|| fallback.to_string())
    };
    ui_send(
        tx,
        encord_ui::UiUpdate::Theme(encord_ui::ThemeColors {
            bg0: get("color.bg0", "#0e0f13"),
            bg1: get("color.bg1", "#171923"),
            accent: get("color.accent", "#5865f2"),
            text0: get("color.text0", "#ffffff"),
            text1: get("color.text1", "#b5bac1"),
        }),
    );
}

fn list_themes() -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir("themes") {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    out.push(stem.to_string());
                }
            }
        }
    }
    out.sort();
    if out.is_empty() {
        out.push("midnight-club".to_string());
    }
    out
}

fn short_time(ts: &str) -> String {
    // ASCII timestamps ("2026-01-01T12:34:56Z") -> "12:34". `get` keeps
    // malformed/short values from panicking the core thread.
    ts.get(11..16).unwrap_or(ts).to_string()
}

fn core_thread(
    settings: Settings,
    config_path: PathBuf,
    cache: Arc<Cache>,
    ui_tx: UiTx,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<CoreCmd>,
) {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            ui_send(
                &ui_tx,
                encord_ui::UiUpdate::Status(format!("runtime failed: {e}")),
            );
            return;
        }
    };
    rt.block_on(async {
        let mut state = CoreState {
            want: settings.discord.autoconnect && !settings.discord.token.is_empty(),
            settings,
            config_path,
            selected_guild: None,
            selected_channel: None,
            self_id: None,
            presences: HashMap::new(),
            typing: HashMap::new(),
            last_typing: String::new(),
            app_names: HashMap::new(),
            pending_presence: None,
            session_active: false,
            gw_tx: None,
        };
        apply_theme(&state.settings.gui.theme.clone(), &ui_tx);
        push_guilds(&cache, &ui_tx);
        push_channels(&cache, &ui_tx, None);

        // Gateway outbound lane is per-session (see run_session); the RPC
        // task below only ever sends RpcCmd.
        // Local discord-ipc pipe (games -> Rich Presence). Runs for the whole
        // core lifetime, sessions or not; activities are stashed and applied
        // on the next connect.
        let (rpc_tx, mut rpc_rx) = tokio::sync::mpsc::unbounded_channel::<encord_rpc::RpcCmd>();
        tokio::spawn(async move {
            if let Err(e) = encord_rpc::serve(rpc_tx).await {
                tracing::warn!("rpc server exited: {e}");
            }
        });

        loop {
            // Drain pending UI commands (Send/REST awaits inline: we are async).
            while let Ok(cmd) = cmd_rx.try_recv() {
                match cmd {
                    CoreCmd::Toggle => state.want = !state.want,
                    CoreCmd::SelectChannel(id) => {
                        select_channel(&mut state, &cache, &ui_tx, id).await;
                    }
                    CoreCmd::SelectGuild(id) => {
                        select_guild(&mut state, &cache, &ui_tx, id).await;
                    }
                    CoreCmd::Send(text) => {
                        handle_send(&state, &ui_tx, text).await;
                    }
                    CoreCmd::SaveSettings {
                        token,
                        autoconnect,
                        theme,
                    } => {
                        handle_save(&mut state, &ui_tx, token, autoconnect, theme);
                    }
                    CoreCmd::ReloadCss => {
                        handle_reload_css(&state, &ui_tx);
                    }
                }
            }

            // Drain local RPC (games). Always accepted; applied live when a
            // session is up, otherwise stashed for the next connect.
            while let Ok(cmd) = rpc_rx.try_recv() {
                match cmd {
                    encord_rpc::RpcCmd::Activity {
                        client_id,
                        activity,
                        ..
                    } => {
                        let cleared = activity.is_none();
                        // IPC activities are implicitly for the connected
                        // app: stamp `application_id` and fill the `name`
                        // games omit ( else the OP 3
                        // presence is unattributed/broken).
                        let mut activities = Vec::new();
                        if let Some(a) = activity {
                            activities.push(enrich_activity(&mut state, &client_id, a).await);
                        }
                        let presence = GatewayCmd::Presence {
                            status: "online".to_string(),
                            activities,
                            afk: false,
                        };
                        state.pending_presence = Some(presence.clone());
                        if state.session_active {
                            if let Some(tx) = state.gw_tx.clone() {
                                let _ = tx.send(presence);
                            }
                        }
                        ui_send(
                            &ui_tx,
                            encord_ui::UiUpdate::Log(if cleared {
                                format!("rpc: activity cleared by {client_id}")
                            } else {
                                format!("rpc: activity from {client_id}")
                            }),
                        );
                    }
                    encord_rpc::RpcCmd::Notice(line) => {
                        ui_send(&ui_tx, encord_ui::UiUpdate::Log(line));
                    }
                }
            }

            if state.settings.discord.token.is_empty() {
                ui_send(
                    &ui_tx,
                    encord_ui::UiUpdate::Status(
                        "offline — set token in Settings, Save, Connect".to_string(),
                    ),
                );
                ui_send(
                    &ui_tx,
                    encord_ui::UiUpdate::ConnectLabel("Connect".to_string()),
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            if !state.want {
                ui_send(
                    &ui_tx,
                    encord_ui::UiUpdate::Status("press Connect".to_string()),
                );
                ui_send(
                    &ui_tx,
                    encord_ui::UiUpdate::ConnectLabel("Connect".to_string()),
                );
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                continue;
            }

            run_session(&mut state, &cache, &ui_tx, &mut cmd_rx).await;
        }
    });
}

/// Channel click: text-likes open chat (+title/topic), voice is M3,
/// categories are inert headers.
async fn select_channel(state: &mut CoreState, cache: &Arc<Cache>, tx: &UiTx, id: String) {
    let found = cache
        .list_channels()
        .unwrap_or_default()
        .into_iter()
        .find(|c| c.id.to_string() == id);
    match found {
        Some(c) if c.is_text_like() => {
            state.selected_channel = Some(id.clone());
            ui_send(tx, encord_ui::UiUpdate::ChannelTitle(channel_display(&c)));
            ui_send(
                tx,
                encord_ui::UiUpdate::ChannelTopic(c.topic.clone().unwrap_or_default()),
            );
            // Backfill history when the cache is thin, then paint + mark read.
            if !ensure_history(state, cache, tx, &id).await {
                push_messages(cache, tx, Some(&id));
            }
            mark_read(state, cache, tx, &id);
        }
        Some(c) if c.is_voice_like() => {
            ui_send(
                tx,
                encord_ui::UiUpdate::Log("voice join lands in M3".to_string()),
            );
        }
        _ => {}
    }
}

/// Fetch the last 50 messages when the cache is thin (fresh installs
/// otherwise show an empty channel until live traffic arrives).
/// Returns true when it painted fresh messages itself.
async fn ensure_history(state: &CoreState, cache: &Cache, tx: &UiTx, channel_id: &str) -> bool {
    let Some(cid) = channel_id
        .parse::<u64>()
        .ok()
        .map(encord_core::Snowflake::new)
    else {
        return false;
    };
    let have = cache.last_messages(cid, 50).unwrap_or_default().len();
    if have >= 50 || state.settings.discord.token.is_empty() {
        return false;
    }
    let rest = match rest_for(&state.settings) {
        Ok(r) => r,
        Err(e) => {
            ui_send(
                tx,
                encord_ui::UiUpdate::Log(format!("history fetch failed: {e}")),
            );
            return false;
        }
    };
    match rest.get_messages(cid, 50).await {
        Ok(msgs) => {
            for m in &msgs {
                let _ = cache.upsert_message(m);
            }
            ui_send(
                tx,
                encord_ui::UiUpdate::Log(format!("history: {} messages", msgs.len())),
            );
            push_messages(cache, tx, Some(channel_id));
            true
        }
        Err(e) => {
            ui_send(
                tx,
                encord_ui::UiUpdate::Log(format!("history fetch failed: {e}")),
            );
            false
        }
    }
}
/// Guild click: "home" shows DMs, otherwise filter channels, clear chat,
/// refresh userlist, REST-backfill members (gateway chunks cover small
/// guilds; big ones need this).
async fn select_guild(state: &mut CoreState, cache: &Arc<Cache>, tx: &UiTx, id: String) {
    if id == "home" {
        state.selected_guild = None;
        state.selected_channel = None;
        ui_send(
            tx,
            encord_ui::UiUpdate::GuildName("Direct Messages".to_string()),
        );
        ui_send(tx, encord_ui::UiUpdate::ChannelTitle(String::new()));
        ui_send(tx, encord_ui::UiUpdate::ChannelTopic(String::new()));
        push_channels(cache, tx, None);
        push_messages(cache, tx, None);
        push_members(cache, tx, state);
        return;
    }
    let name = cache
        .list_guilds()
        .unwrap_or_default()
        .into_iter()
        .find(|g| g.id.to_string() == id)
        .map(|g| g.name)
        .unwrap_or_else(|| "encord".to_string());
    state.selected_guild = Some(id.clone());
    state.selected_channel = None;
    ui_send(tx, encord_ui::UiUpdate::GuildName(name));
    ui_send(tx, encord_ui::UiUpdate::ChannelTitle(String::new()));
    ui_send(tx, encord_ui::UiUpdate::ChannelTopic(String::new()));
    push_channels(cache, tx, Some(&id));
    push_messages(cache, tx, None);
    push_members(cache, tx, state);

    if state.settings.discord.token.is_empty() {
        return;
    }
    let Ok(gid) = id.parse::<u64>() else {
        return;
    };
    let rest = match rest_for(&state.settings) {
        Ok(r) => r,
        Err(e) => {
            ui_send(
                tx,
                encord_ui::UiUpdate::Log(format!("members backfill failed: {e}")),
            );
            return;
        }
    };
    match rest
        .get_guild_members(encord_core::Snowflake::new(gid), 100)
        .await
    {
        Ok(members) => {
            for m in &members {
                let _ = cache.upsert_member(encord_core::Snowflake::new(gid), m);
            }
            push_members(cache, tx, state);
            ui_send(
                tx,
                encord_ui::UiUpdate::Log(format!("{} members", members.len())),
            );
        }
        Err(e) => ui_send(
            tx,
            encord_ui::UiUpdate::Log(format!("members backfill failed: {e}")),
        ),
    }
}

fn handle_save(state: &mut CoreState, tx: &UiTx, token: String, autoconnect: bool, theme: String) {
    state.settings.discord.token = token.trim().to_string();
    state.settings.discord.autoconnect = autoconnect;
    let theme = theme.trim();
    state.settings.gui.theme = if theme.is_empty() {
        "midnight-club".to_string()
    } else {
        theme.to_string()
    };
    match state.settings.save(&state.config_path) {
        Ok(()) => {
            ui_send(tx, encord_ui::UiUpdate::Log("settings saved".to_string()));
            apply_theme(&state.settings.gui.theme.clone(), tx);
        }
        Err(e) => ui_send(
            tx,
            encord_ui::UiUpdate::Log(format!("settings save failed: {e}")),
        ),
    }
}

fn handle_reload_css(state: &CoreState, tx: &UiTx) {
    let path = PathBuf::from(&state.settings.gui.css);
    match std::fs::read_to_string(&path) {
        Ok(css) => {
            let unknown = encord_theme::validate_css_selectors(&css);
            if unknown.is_empty() {
                ui_send(
                    tx,
                    encord_ui::UiUpdate::Log(format!("css ok: {}", path.display())),
                );
            } else {
                ui_send(
                    tx,
                    encord_ui::UiUpdate::Log(format!("css unknown selectors: {unknown:?}")),
                );
            }
            // Re-apply token theme so the native UI picks up theme edits.
            apply_theme(&state.settings.gui.theme, tx);
        }
        Err(e) => ui_send(
            tx,
            encord_ui::UiUpdate::Log(format!("css read failed: {e}")),
        ),
    }
}

async fn handle_send(state: &CoreState, tx: &UiTx, text: String) {
    let text = text.trim().to_string();
    if text.is_empty() {
        return;
    }
    let Some(channel) = state.selected_channel.clone() else {
        ui_send(
            tx,
            encord_ui::UiUpdate::Log("select a channel first".to_string()),
        );
        return;
    };
    if state.settings.discord.token.is_empty() {
        ui_send(tx, encord_ui::UiUpdate::Log("set token first".to_string()));
        return;
    }
    let Ok(id) = channel.parse::<u64>() else {
        ui_send(tx, encord_ui::UiUpdate::Log("bad channel id".to_string()));
        return;
    };
    let rest = match rest_for(&state.settings) {
        Ok(r) => r,
        Err(e) => {
            ui_send(tx, encord_ui::UiUpdate::Log(format!("send failed: {e}")));
            return;
        }
    };
    // No optimistic append: the gateway MESSAGE_CREATE echo paints it,
    // which keeps cache + UI in agreement.
    if let Err(e) = rest
        .send_message(encord_core::Snowflake::new(id), &text)
        .await
    {
        ui_send(tx, encord_ui::UiUpdate::Log(format!("send failed: {e}")));
    }
}

/// One connect press: run the gateway until the user toggles off.
/// `Gateway::run` retries drops internally; we only return here on user
/// disconnect or clean close, and the outer loop reconnects while wanted.
async fn run_session(
    state: &mut CoreState,
    cache: &Arc<Cache>,
    tx: &UiTx,
    cmd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<CoreCmd>,
) {
    ui_send(tx, encord_ui::UiUpdate::Status("connecting…".to_string()));
    ui_send(
        tx,
        encord_ui::UiUpdate::ConnectLabel("Disconnect".to_string()),
    );
    ui_send(
        tx,
        encord_ui::UiUpdate::Log("gateway: connecting…".to_string()),
    );

    let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel();
    let (gw_cmd_tx, gw_cmd_rx) = tokio::sync::mpsc::unbounded_channel::<GatewayCmd>();
    let gw = Gateway::new(
        gateway_config(
            &state.settings,
            gateway_url(&state.settings),
            state.settings.discord.token.clone(),
        ),
        ev_tx,
        gw_cmd_rx,
    );
    state.gw_tx = Some(gw_cmd_tx);
    state.session_active = true;
    let mut gw_task = tokio::spawn(async move { gw.run().await });
    let mut typing_tick = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    CoreCmd::Toggle => {
                        state.want = false;
                        gw_task.abort();
                        state.session_active = false;
                        state.gw_tx = None;
                        ui_send(tx, encord_ui::UiUpdate::Status("press Connect".to_string()));
                        ui_send(tx, encord_ui::UiUpdate::ConnectLabel("Connect".to_string()));
                        ui_send(tx, encord_ui::UiUpdate::Log("gateway: disconnected by user".to_string()));
                        return;
                    }
                    // Selection/options work mid-session too.
                    CoreCmd::SelectChannel(id) => {
                        select_channel(state, cache, tx, id).await;
                    }
                    CoreCmd::SelectGuild(id) => {
                        select_guild(state, cache, tx, id).await;
                    }
                    CoreCmd::Send(text) => {
                        handle_send(state, tx, text).await;
                    }
                    CoreCmd::SaveSettings { token, autoconnect, theme } => {
                        handle_save(state, tx, token, autoconnect, theme);
                    }
                    CoreCmd::ReloadCss => {
                        handle_reload_css(state, tx);
                    }
                }
            }
            Some(ev) = ev_rx.recv() => {
                route_event(state, cache, tx, ev).await;
            }
            _ = typing_tick.tick() => {
                push_typing(state, tx);
            }
            res = &mut gw_task => {
                state.session_active = false;
                state.gw_tx = None;
                match res {
                    Ok(Ok(())) => {
                        ui_send(tx, encord_ui::UiUpdate::Status("press Connect".to_string()));
                        ui_send(tx, encord_ui::UiUpdate::Log("gateway: session closed".to_string()));
                    }
                    Ok(Err(e)) => {
                        ui_send(tx, encord_ui::UiUpdate::Status(format!("reconnecting… ({e})")));
                        ui_send(tx, encord_ui::UiUpdate::Log(format!("gateway error: {e}")));
                    }
                    Err(e) => {
                        ui_send(tx, encord_ui::UiUpdate::Status(format!("reconnecting… ({e})")));
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                return;
            }
        }
    }
}

/// Persist read state, clear badges, and ACK to Discord (best-effort).
fn mark_read(state: &CoreState, cache: &Cache, tx: &UiTx, channel_id: &str) {
    let Some(cid) = channel_id
        .parse::<u64>()
        .ok()
        .map(encord_core::Snowflake::new)
    else {
        return;
    };
    let latest = cache
        .last_messages(cid, 1)
        .unwrap_or_default()
        .pop()
        .map(|m| m.id);
    let _ = cache.upsert_read(cid, latest, 0);
    push_channels(cache, tx, state.selected_guild.as_deref());
    push_guilds(cache, tx);
    if let Some(mid) = latest {
        spawn_ack(&state.settings, cid, mid);
    }
}

/// Stamp an IPC activity for the gateway: `application_id` is implicit in
/// the pipe handshake (games never send it), and `name` usually is too.
/// Pure part, unit-tested below.
fn stamp_activity(
    client_id: &str,
    app_name: Option<&str>,
    mut activity: serde_json::Value,
) -> serde_json::Value {
    if let Some(obj) = activity.as_object_mut() {
        if !obj.contains_key("application_id") {
            obj.insert(
                "application_id".to_string(),
                serde_json::Value::String(client_id.to_string()),
            );
        }
        if obj
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(str::is_empty)
            .unwrap_or(true)
        {
            let name = app_name
                .filter(|n| !n.is_empty())
                .unwrap_or(client_id)
                .to_string();
            obj.insert("name".to_string(), serde_json::Value::String(name));
        }
        if !obj.contains_key("type") {
            obj.insert("type".to_string(), serde_json::Value::from(0));
        }
    }
    activity
}

/// Resolve the application name over REST (cached per launch), then stamp.
async fn enrich_activity(
    state: &mut CoreState,
    client_id: &str,
    activity: serde_json::Value,
) -> serde_json::Value {
    let needs_name = activity
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::is_empty)
        .unwrap_or(true);
    let mut app_name: Option<String> = None;
    if needs_name && !state.app_names.contains_key(client_id) {
        if let Ok(rest) = rest_for(&state.settings) {
            match rest.get_application_name(client_id).await {
                Ok(name) => {
                    state.app_names.insert(client_id.to_string(), name.clone());
                    app_name = Some(name);
                }
                Err(e) => {
                    tracing::debug!("rpc app lookup failed for {client_id}: {e}");
                    state.app_names.insert(client_id.to_string(), String::new());
                }
            }
        }
    }
    if app_name.is_none() {
        app_name = state
            .app_names
            .get(client_id)
            .filter(|n| !n.is_empty())
            .cloned();
    }
    stamp_activity(client_id, app_name.as_deref(), activity)
}

fn spawn_ack(
    settings: &Settings,
    channel_id: encord_core::Snowflake,
    message_id: encord_core::Snowflake,
) {
    if settings.discord.token.is_empty() {
        return;
    }
    let settings = settings.clone();
    tokio::spawn(async move {
        match rest_for(&settings) {
            Ok(rest) => {
                if let Err(e) = rest.ack(channel_id, message_id).await {
                    tracing::debug!("ack failed: {e}");
                }
            }
            Err(e) => tracing::debug!("ack client failed: {e}"),
        }
    });
}

/// Download missing guild icons, then refresh the rail once.
async fn ensure_guild_icons(cache: &Cache, tx: &UiTx) {
    let guilds = cache.list_guilds().unwrap_or_default();
    let mut fresh = false;
    for g in &guilds {
        if let Some(hash) = &g.icon {
            let url = encord_media::MediaCache::guild_icon_url(g.id.0, hash);
            if media().cached("icons", &url, Some("image/png")).is_none()
                && media()
                    .fetch("icons", &url, Some("image/png"), 256_000)
                    .await
                    .is_ok()
            {
                fresh = true;
            }
        }
    }
    if fresh {
        push_guilds(cache, tx);
    }
}

/// Download a message's image attachments, then repaint the channel once.
async fn ensure_message_media(cache: &Cache, tx: &UiTx, channel: &str, m: &encord_core::Message) {
    let mut fresh = false;
    for a in &m.attachments {
        if a.is_image()
            && media()
                .cached("attachments", a.download_url(), a.content_type.as_deref())
                .is_none()
            && media()
                .fetch(
                    "attachments",
                    a.download_url(),
                    a.content_type.as_deref(),
                    8_000_000,
                )
                .await
                .is_ok()
        {
            fresh = true;
        }
    }
    if fresh {
        push_messages(cache, tx, Some(channel));
    }
}

/// One-line cache census for the event log (proves sync state at a glance).
fn log_cache_counts(cache: &Cache, tx: &UiTx, prefix: &str) {
    let guilds = cache.list_guilds().unwrap_or_default();
    let channels = cache.list_channels().unwrap_or_default().len();
    let mut members = 0;
    for g in &guilds {
        members += cache.list_members(g.id).unwrap_or_default().len();
    }
    ui_send(
        tx,
        encord_ui::UiUpdate::Log(format!(
            "{prefix}: {} guilds, {channels} channels, {members} members cached",
            guilds.len()
        )),
    );
}

/// Persist one gateway event, refresh the affected UI lists, and log it.
async fn route_event(state: &mut CoreState, cache: &Arc<Cache>, tx: &UiTx, ev: CoreEvent) {
    let selected = state.selected_channel.clone();
    match &ev {
        CoreEvent::Ready { user, .. } => {
            state.self_id = Some(user.id.to_string());
            ui_send(
                tx,
                encord_ui::UiUpdate::Status(format!("connected · @{}", user.username)),
            );
            // Re-apply stashed Rich Presence (local pipe may have set it
            // while offline or mid-reconnect).
            if let Some(presence) = state.pending_presence.clone() {
                if let Some(gw) = state.gw_tx.clone() {
                    let _ = gw.send(presence);
                }
            }
            push_guilds(cache, tx);
            push_channels(cache, tx, state.selected_guild.as_deref());
            log_cache_counts(cache, tx, "sync");
            // First connect with nothing open: pick the first guild (home
            // when guild-less) and its top text channel through the normal
            // select path, so history, members and read state all load.
            if selected.is_none() && state.selected_guild.is_none() {
                let first_guild = cache.list_guilds().unwrap_or_default().into_iter().next();
                match first_guild {
                    Some(g) => select_guild(state, cache, tx, g.id.to_string()).await,
                    None => select_guild(state, cache, tx, "home".to_string()).await,
                }
                let first_text = cache
                    .list_channels()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|c| match (&state.selected_guild, c.guild_id) {
                        (Some(g), Some(id)) => &id.to_string() == g,
                        (Some(_), None) => false,
                        (None, None) => true,
                        (None, Some(_)) => false,
                    })
                    .find(|c| c.is_text_like());
                if let Some(first) = first_text {
                    select_channel(state, cache, tx, first.id.to_string()).await;
                }
            }
            // Guild icons arrive after paint; refresh the rail as they land.
            let c2 = cache.clone();
            let t2 = tx.clone();
            tokio::spawn(async move {
                ensure_guild_icons(&c2, &t2).await;
            });
        }
        CoreEvent::Resumed => {
            ui_send(
                tx,
                encord_ui::UiUpdate::Status("connected · resumed".to_string()),
            );
        }
        CoreEvent::GuildCreate(g) => {
            let _ = cache.upsert_guild(g);
            push_guilds(cache, tx);
            push_channels(cache, tx, state.selected_guild.as_deref());
            let c2 = cache.clone();
            let t2 = tx.clone();
            tokio::spawn(async move {
                ensure_guild_icons(&c2, &t2).await;
            });
        }
        CoreEvent::GuildUpdate(g) => {
            let _ = cache.upsert_guild(g);
            push_guilds(cache, tx);
            let c2 = cache.clone();
            let t2 = tx.clone();
            tokio::spawn(async move {
                ensure_guild_icons(&c2, &t2).await;
            });
        }
        CoreEvent::GuildDelete { id, unavailable } => {
            // Outage blips carry `unavailable=true`: the guild is coming
            // back, so keep the cache painted instead of blanking the rail.
            if *unavailable {
                ui_send(
                    tx,
                    encord_ui::UiUpdate::Log(format!("guild {id} unavailable (outage)")),
                );
                return;
            }
            let _ = cache.remove_guild(*id);
            if state.selected_guild.as_deref() == Some(&id.to_string()) {
                state.selected_guild = None;
                state.selected_channel = None;
                ui_send(tx, encord_ui::UiUpdate::ChannelTitle(String::new()));
                ui_send(tx, encord_ui::UiUpdate::ChannelTopic(String::new()));
                push_messages(cache, tx, None);
                push_members(cache, tx, state);
            }
            push_guilds(cache, tx);
            push_channels(cache, tx, state.selected_guild.as_deref());
        }
        CoreEvent::ChannelCreate(c) => {
            let _ = cache.upsert_channel(c);
            push_channels(cache, tx, state.selected_guild.as_deref());
        }
        CoreEvent::ChannelDelete(id) => {
            let _ = cache.remove_channel(*id);
            if state.selected_channel.as_deref() == Some(&id.to_string()) {
                state.selected_channel = None;
                ui_send(tx, encord_ui::UiUpdate::ChannelTitle(String::new()));
                ui_send(tx, encord_ui::UiUpdate::ChannelTopic(String::new()));
                push_messages(cache, tx, None);
            }
            push_channels(cache, tx, state.selected_guild.as_deref());
        }
        CoreEvent::MessageCreate(m) => {
            let _ = cache.upsert_message(m);
            let channel = m.channel_id.to_string();
            if selected.as_deref() == Some(&channel) {
                ui_send(tx, encord_ui::UiUpdate::MessageAppend(chat_msg_of(m)));
                mark_read(state, cache, tx, &channel);
                // Fetch image attachments, then repaint with thumbnails.
                if m.attachments.iter().any(|a| a.is_image()) {
                    let c2 = cache.clone();
                    let t2 = tx.clone();
                    let owned = m.clone();
                    let ch = channel.clone();
                    tokio::spawn(async move {
                        ensure_message_media(&c2, &t2, &ch, &owned).await;
                    });
                }
            } else {
                // Background traffic: bump mentions, refresh badges.
                let mentioned = state.self_id.as_ref().map(|id| {
                    m.mention_everyone || m.mentions.iter().any(|u| &u.id.to_string() == id)
                });
                if mentioned.unwrap_or(false) {
                    let mut read =
                        cache
                            .get_read(m.channel_id)
                            .unwrap_or(encord_core::ChannelRead {
                                channel_id: m.channel_id,
                                last_message_id: None,
                                mention_count: 0,
                            });
                    read.mention_count += 1;
                    let _ =
                        cache.upsert_read(m.channel_id, read.last_message_id, read.mention_count);
                }
                push_channels(cache, tx, state.selected_guild.as_deref());
                push_guilds(cache, tx);
            }
        }
        CoreEvent::MessageUpdate(m) => {
            let _ = cache.upsert_message(m);
            if selected.as_deref() == Some(&m.channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::MessagePatch {
            channel_id,
            message_id,
            content,
            edited_timestamp,
        } => {
            if cache
                .patch_message(
                    *channel_id,
                    *message_id,
                    content.as_deref(),
                    edited_timestamp.as_deref(),
                )
                .unwrap_or(false)
                && selected.as_deref() == Some(&channel_id.to_string())
            {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::MessageDelete {
            channel_id,
            message_id,
        } => {
            let _ = cache.delete_message(*channel_id, *message_id);
            if selected.as_deref() == Some(&channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::MessageDeleteBulk {
            channel_id,
            message_ids,
        } => {
            let _ = cache.delete_messages(*channel_id, message_ids);
            if selected.as_deref() == Some(&channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::ReactionAdd {
            channel_id,
            message_id,
            emoji,
        } => {
            apply_reaction(cache, *channel_id, *message_id, emoji, true);
            if selected.as_deref() == Some(&channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::ReactionRemove {
            channel_id,
            message_id,
            emoji,
        } => {
            apply_reaction(cache, *channel_id, *message_id, emoji, false);
            if selected.as_deref() == Some(&channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::ReactionRemoveAll {
            channel_id,
            message_id,
        } => {
            clear_reactions(cache, *channel_id, *message_id, None);
            if selected.as_deref() == Some(&channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::ReactionRemoveEmoji {
            channel_id,
            message_id,
            emoji,
        } => {
            clear_reactions(cache, *channel_id, *message_id, Some(emoji));
            if selected.as_deref() == Some(&channel_id.to_string()) {
                push_messages(cache, tx, selected.as_deref());
            }
        }
        CoreEvent::MessageAck {
            channel_id,
            message_id,
        } => {
            // Another session read ahead: adopt its position, keep our
            // local mention count (the ack carries no mention data).
            let read = cache
                .get_read(*channel_id)
                .unwrap_or(encord_core::ChannelRead {
                    channel_id: *channel_id,
                    last_message_id: None,
                    mention_count: 0,
                });
            let _ = cache.upsert_read(*channel_id, Some(*message_id), read.mention_count);
            push_channels(cache, tx, state.selected_guild.as_deref());
            push_guilds(cache, tx);
        }
        CoreEvent::TypingStart {
            channel_id,
            user_id,
        } => {
            let display = cache
                .find_member_display(*user_id)
                .unwrap_or(None)
                .unwrap_or_else(|| {
                    let id = user_id.to_string();
                    // Snowflakes are ASCII digits; `get` keeps this
                    // panic-free even for unexpected id shapes.
                    format!("user {}", id.get(..8.min(id.len())).unwrap_or(&id))
                });
            state.typing.insert(
                (channel_id.to_string(), user_id.to_string()),
                (display, Instant::now()),
            );
            push_typing(state, tx);
        }
        CoreEvent::PresenceUpdate {
            user_id,
            status,
            guild_id,
        } => {
            let key = format!(
                "{}:{user_id}",
                guild_id
                    .map(|g| g.to_string())
                    .unwrap_or_else(|| "dm".to_string())
            );
            state.presences.insert(key, status.clone());
            if let Some(gid) = guild_id {
                if state.selected_guild.as_deref() == Some(&gid.to_string()) {
                    push_members(cache, tx, state);
                }
            }
        }
        CoreEvent::GuildMembersChunk { guild_id, members } => {
            for m in members {
                let _ = cache.upsert_member(*guild_id, m);
            }
            if state.selected_guild.as_deref() == Some(&guild_id.to_string()) {
                push_members(cache, tx, state);
            }
        }
        CoreEvent::GuildMemberAdd { guild_id, member }
        | CoreEvent::GuildMemberUpdate { guild_id, member } => {
            let _ = cache.upsert_member(*guild_id, member);
            if state.selected_guild.as_deref() == Some(&guild_id.to_string()) {
                push_members(cache, tx, state);
            }
        }
        CoreEvent::GuildMemberRemove { guild_id, user_id } => {
            let _ = cache.remove_member(*guild_id, *user_id);
            if state.selected_guild.as_deref() == Some(&guild_id.to_string()) {
                push_members(cache, tx, state);
            }
        }
        CoreEvent::ReadState(states) => {
            for s in states {
                let _ = cache.upsert_read(s.channel_id, s.last_message_id, s.mention_count);
            }
            push_channels(cache, tx, state.selected_guild.as_deref());
            push_guilds(cache, tx);
        }
        CoreEvent::Disconnected { reason } => {
            ui_send(
                tx,
                encord_ui::UiUpdate::Status(format!("reconnecting… ({reason})")),
            );
        }
        _ => {}
    }
    ui_send(tx, encord_ui::UiUpdate::Log(short_event(&ev)));
}

fn short_event(ev: &CoreEvent) -> String {
    match ev {
        CoreEvent::Ready { user, .. } => format!("READY @{}", user.username),
        CoreEvent::Resumed => "RESUMED".to_string(),
        CoreEvent::GuildCreate(g) => format!("+ guild {}", g.name),
        CoreEvent::GuildUpdate(g) => format!("~ guild {}", g.name),
        CoreEvent::GuildDelete { id, unavailable } => {
            if *unavailable {
                format!("guild {id} unavailable (outage)")
            } else {
                format!("- guild {id}")
            }
        }
        CoreEvent::ChannelCreate(c) => {
            format!("+ channel {}", c.name.as_deref().unwrap_or("?"))
        }
        CoreEvent::ChannelDelete(id) => format!("- channel {id}"),
        CoreEvent::MessageCreate(m) => {
            format!("{}: {}", m.author.username, truncate(&m.content, 80))
        }
        CoreEvent::MessageUpdate(m) => format!("edit {}: {}", m.id, truncate(&m.content, 80)),
        CoreEvent::MessagePatch { message_id, .. } => format!("edit {message_id}"),
        CoreEvent::MessageDelete { message_id, .. } => format!("delete {message_id}"),
        CoreEvent::MessageDeleteBulk { message_ids, .. } => {
            format!("bulk delete {} messages", message_ids.len())
        }
        CoreEvent::ReactionAdd { message_id, .. } => format!("+ reaction {message_id}"),
        CoreEvent::ReactionRemove { message_id, .. } => format!("- reaction {message_id}"),
        CoreEvent::ReactionRemoveAll { message_id, .. } => {
            format!("clear reactions {message_id}")
        }
        CoreEvent::ReactionRemoveEmoji { message_id, .. } => {
            format!("clear emoji {message_id}")
        }
        CoreEvent::MessageAck { channel_id, .. } => format!("ack {channel_id}"),
        CoreEvent::TypingStart { user_id, .. } => format!("typing {user_id}"),
        CoreEvent::PresenceUpdate {
            user_id, status, ..
        } => format!("presence {user_id} {status}"),
        CoreEvent::GuildMembersChunk { members, .. } => {
            format!("{} members", members.len())
        }
        CoreEvent::GuildMemberAdd { member, .. } => {
            format!("+ member {}", member.display())
        }
        CoreEvent::GuildMemberUpdate { member, .. } => {
            format!("~ member {}", member.display())
        }
        CoreEvent::GuildMemberRemove { user_id, .. } => format!("- member {user_id}"),
        CoreEvent::ReadState(states) => format!("read state ({} channels)", states.len()),
        CoreEvent::Warning(w) => w.clone(),
        CoreEvent::VoiceStateUpdate { user_id, .. } => format!("voice {user_id}"),
        CoreEvent::Disconnected { reason } => format!("disconnected: {reason}"),
    }
}

fn truncate(s: &str, n: usize) -> String {
    // Char-boundary safe: byte-slicing `&s[..n]` panics on multi-byte
    // UTF-8 (emoji). Count chars so "👍👍…" never crashes the core thread.
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let head: String = s.chars().take(n).collect();
        format!("{head}…")
    }
}

fn initials(name: &str) -> String {
    let mut out = String::new();
    for word in name.split_whitespace() {
        if let Some(ch) = word.chars().next() {
            out.push(ch);
            if out.chars().count() >= 2 {
                break;
            }
        }
    }
    if out.is_empty() {
        "?".to_string()
    } else {
        out.to_uppercase()
    }
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_never_splits_utf8() {
        assert_eq!(truncate("👍👍👍", 2), "👍👍…");
        assert_eq!(truncate("hi", 80), "hi");
    }

    #[test]
    fn stamp_activity_fills_gateway_fields() {
        // Bare game activity gains attribution + defaults.
        let out = stamp_activity(
            "123",
            Some("Cool Game"),
            serde_json::json!({"details": "x"}),
        );
        assert_eq!(out["application_id"], "123");
        assert_eq!(out["name"], "Cool Game");
        assert_eq!(out["type"], 0);
        assert_eq!(out["details"], "x");
        // Explicit fields are never clobbered; unknown app falls back
        // to the client id instead of an empty name.
        let out = stamp_activity(
            "123",
            None,
            serde_json::json!({"name": "N", "type": 2, "application_id": "9"}),
        );
        assert_eq!(out["application_id"], "9");
        assert_eq!(out["name"], "N");
        let out = stamp_activity("123", None, serde_json::json!({}));
        assert_eq!(out["name"], "123");
    }
}
