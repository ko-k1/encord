//! encord-ui: retained-mode GPU window (Slint, FemtoVG over wgpu).
//! wgpu negotiates Vulkan first on Windows/Linux (Metal on macOS, DX12/WGL
//! fallback) — no webview, no Chromium, all raster via the GPU.
//!
//! Threading: the core thread owns a [`std::sync::mpsc`] sender of
//! [`UiUpdate`]; a 150ms UI timer applies them on the UI thread. Slint
//! handles are never touched off-thread. UI actions flow back through
//! [`UiCallbacks`].

use slint::Model;
use std::cell::RefCell;
use std::rc::Rc;

slint::include_modules!();

pub struct GuildItem {
    pub id: String,
    pub label: String,
    pub unread: bool,
    /// Local cached icon path; empty = render initials.
    pub icon_path: String,
}

pub struct ChannelItem {
    pub id: String,
    pub name: String,
    pub header: bool,
    pub unread: bool,
    pub mentions: i32,
}

pub struct ChatAttachment {
    pub filename: String,
    pub is_image: bool,
    /// Local cached path for images; empty = not downloaded yet.
    pub path: String,
}

pub struct ChatMsg {
    pub id: String,
    pub author: String,
    pub body: String,
    pub time: String,
    /// "↩ author: preview" for replies, else empty.
    pub reply: String,
    /// "👍 3  🎉 1" display line, else empty.
    pub reactions: String,
    pub attachments: Vec<ChatAttachment>,
}

pub struct MemberData {
    pub user_id: String,
    pub display: String,
    pub status: String,
}

pub struct ThemeColors {
    pub bg0: String,
    pub bg1: String,
    pub accent: String,
    pub text0: String,
    pub text1: String,
}

/// Core -> UI updates. Lists/messages are full snapshots except
/// `MessageAppend` (live traffic on the open channel).
pub enum UiUpdate {
    Log(String),
    Status(String),
    ConnectLabel(String),
    GuildName(String),
    Guilds(Vec<GuildItem>),
    Channels(Vec<ChannelItem>),
    Messages(Vec<ChatMsg>),
    MessageAppend(ChatMsg),
    Members {
        online: Vec<MemberData>,
        offline: Vec<MemberData>,
    },
    ChannelTitle(String),
    ChannelTopic(String),
    Typing(String),
    Theme(ThemeColors),
}

/// UI -> core actions.
pub struct UiCallbacks {
    pub connect: Box<dyn Fn()>,
    pub select_channel: Box<dyn Fn(String)>,
    pub select_guild: Box<dyn Fn(String)>,
    pub send: Box<dyn Fn(String)>,
    pub save: Box<dyn Fn(String, bool, String)>,
    pub reload_css: Box<dyn Fn()>,
}

pub struct UiFeed {
    pub status: String,
    pub connect_label: String,
    pub guild_name: String,
    pub token: String,
    pub autoconnect: bool,
    pub themes: Vec<String>,
    pub current_theme: String,
    pub guilds: Vec<GuildItem>,
    pub channels: Vec<ChannelItem>,
    pub messages: Vec<ChatMsg>,
    pub online: Vec<MemberData>,
    pub offline: Vec<MemberData>,
    pub channel_title: String,
    pub channel_topic: String,
    pub typing_text: String,
}

const MAX_MESSAGES: usize = 200;
const MAX_LOG: usize = 500;

pub fn run(
    feed: UiFeed,
    rx: std::sync::mpsc::Receiver<UiUpdate>,
    cb: UiCallbacks,
) -> anyhow::Result<()> {
    let ui = AppWindow::new()?;

    ui.set_status_text(feed.status.into());
    ui.set_connect_label(feed.connect_label.into());
    ui.set_guild_name(feed.guild_name.into());
    ui.set_token_text(feed.token.into());
    ui.set_autoconnect(feed.autoconnect);
    ui.set_themes(
        Rc::new(slint::VecModel::from(
            feed.themes
                .into_iter()
                .map(slint::SharedString::from)
                .collect::<Vec<_>>(),
        ))
        .into(),
    );
    ui.set_current_theme(feed.current_theme.into());

    let guilds = Rc::new(slint::VecModel::from(
        feed.guilds.into_iter().map(guild_entry).collect::<Vec<_>>(),
    ));
    ui.set_guilds(guilds.clone().into());

    let channels = Rc::new(slint::VecModel::from(
        feed.channels
            .into_iter()
            .map(|c| ChannelEntry {
                id: c.id.into(),
                name: c.name.into(),
                header: c.header,
                unread: c.unread,
                mentions: c.mentions,
            })
            .collect::<Vec<_>>(),
    ));
    ui.set_channels(channels.clone().into());

    let messages = Rc::new(slint::VecModel::from(
        feed.messages.into_iter().map(chat_msg).collect::<Vec<_>>(),
    ));
    ui.set_messages(messages.clone().into());

    let log = Rc::new(slint::VecModel::<slint::SharedString>::default());
    ui.set_event_log(log.clone().into());

    let online = Rc::new(slint::VecModel::from(
        feed.online.into_iter().map(member_item).collect::<Vec<_>>(),
    ));
    ui.set_online(online.clone().into());
    ui.set_online_count(online.row_count() as i32);
    let offline = Rc::new(slint::VecModel::from(
        feed.offline
            .into_iter()
            .map(member_item)
            .collect::<Vec<_>>(),
    ));
    ui.set_offline(offline.clone().into());
    ui.set_offline_count(offline.row_count() as i32);
    ui.set_channel_title(feed.channel_title.into());
    ui.set_channel_topic(feed.channel_topic.into());
    ui.set_typing_text(feed.typing_text.into());

    ui.on_send_message(move |text| {
        let text = text.trim().to_string();
        if !text.is_empty() {
            (cb.send)(text);
        }
    });
    ui.on_channel_selected(move |id| (cb.select_channel)(id.to_string()));
    ui.on_guild_selected(move |id| (cb.select_guild)(id.to_string()));
    ui.on_connect_pressed(move || (cb.connect)());
    ui.on_save_settings(move |token, autoconnect, theme| {
        (cb.save)(token.to_string(), autoconnect, theme.to_string())
    });
    ui.on_reload_css(move || (cb.reload_css)());

    // Drain core updates on the UI thread.
    let weak = ui.as_weak();
    let rx = Rc::new(RefCell::new(rx));
    let timer = slint::Timer::default();
    timer.start(
        slint::TimerMode::Repeated,
        std::time::Duration::from_millis(150),
        move || {
            let Some(ui) = weak.upgrade() else { return };
            let guard = rx.borrow_mut();
            while let Ok(update) = guard.try_recv() {
                match update {
                    UiUpdate::Log(line) => {
                        log.push(line.into());
                        while log.row_count() > MAX_LOG {
                            log.remove(0);
                        }
                    }
                    UiUpdate::Status(s) => ui.set_status_text(s.into()),
                    UiUpdate::ConnectLabel(l) => ui.set_connect_label(l.into()),
                    UiUpdate::GuildName(n) => ui.set_guild_name(n.into()),
                    UiUpdate::Guilds(items) => {
                        let vec: Vec<GuildEntry> = items.into_iter().map(guild_entry).collect();
                        guilds.set_vec(vec);
                    }
                    UiUpdate::Channels(items) => {
                        let vec: Vec<ChannelEntry> = items
                            .into_iter()
                            .map(|c| ChannelEntry {
                                id: c.id.into(),
                                name: c.name.into(),
                                header: c.header,
                                unread: c.unread,
                                mentions: c.mentions,
                            })
                            .collect();
                        channels.set_vec(vec);
                    }
                    UiUpdate::Messages(items) => {
                        let vec: Vec<ChatMessage> = items.into_iter().map(chat_msg).collect();
                        messages.set_vec(vec);
                    }
                    UiUpdate::MessageAppend(m) => {
                        messages.push(chat_msg(m));
                        while messages.row_count() > MAX_MESSAGES {
                            messages.remove(0);
                        }
                    }
                    UiUpdate::Members {
                        online: on,
                        offline: off,
                    } => {
                        let on_vec: Vec<MemberItem> = on.into_iter().map(member_item).collect();
                        let off_vec: Vec<MemberItem> = off.into_iter().map(member_item).collect();
                        ui.set_online_count(on_vec.len() as i32);
                        ui.set_offline_count(off_vec.len() as i32);
                        online.set_vec(on_vec);
                        offline.set_vec(off_vec);
                    }
                    UiUpdate::ChannelTitle(t) => ui.set_channel_title(t.into()),
                    UiUpdate::ChannelTopic(t) => ui.set_channel_topic(t.into()),
                    UiUpdate::Typing(t) => ui.set_typing_text(t.into()),
                    UiUpdate::Theme(t) => {
                        if let Some(c) = parse_hex(&t.bg0) {
                            ui.set_c_bg0(c);
                        }
                        if let Some(c) = parse_hex(&t.bg1) {
                            ui.set_c_bg1(c);
                        }
                        if let Some(c) = parse_hex(&t.accent) {
                            ui.set_c_accent(c);
                        }
                        if let Some(c) = parse_hex(&t.text0) {
                            ui.set_c_text0(c);
                        }
                        if let Some(c) = parse_hex(&t.text1) {
                            ui.set_c_text1(c);
                        }
                    }
                }
            }
        },
    );

    ui.run()?;
    Ok(())
}

fn member_item(m: MemberData) -> MemberItem {
    MemberItem {
        id: m.user_id.into(),
        display: m.display.into(),
        status: m.status.into(),
    }
}

fn guild_entry(g: GuildItem) -> GuildEntry {
    GuildEntry {
        id: g.id.into(),
        label: g.label.into(),
        unread: g.unread,
        icon: load_image(&g.icon_path),
    }
}

/// Empty path (or missing file) renders as blank so the initials behind it
/// stay visible.
fn load_image(path: &str) -> slint::Image {
    if path.is_empty() {
        return slint::Image::default();
    }
    slint::Image::load_from_path(std::path::Path::new(path)).unwrap_or_default()
}

fn attachment_item(a: ChatAttachment) -> AttachmentItem {
    if a.is_image {
        AttachmentItem {
            is_image: true,
            image: load_image(&a.path),
            label: a.filename.into(),
        }
    } else {
        AttachmentItem {
            is_image: false,
            image: slint::Image::default(),
            label: format!("📎 {}", a.filename).into(),
        }
    }
}

fn chat_msg(m: ChatMsg) -> ChatMessage {
    ChatMessage {
        id: m.id.into(),
        author: m.author.into(),
        body: m.body.into(),
        time: m.time.into(),
        reply: m.reply.into(),
        reactions: m.reactions.into(),
        attachments: Rc::new(slint::VecModel::from(
            m.attachments
                .into_iter()
                .map(attachment_item)
                .collect::<Vec<_>>(),
        ))
        .into(),
    }
}

fn parse_hex(s: &str) -> Option<slint::Color> {
    let s = s.trim().trim_start_matches('#');
    let (r, g, b) = match s.len() {
        6 => (
            u8::from_str_radix(&s[0..2], 16).ok()?,
            u8::from_str_radix(&s[2..4], 16).ok()?,
            u8::from_str_radix(&s[4..6], 16).ok()?,
        ),
        3 => (
            u8::from_str_radix(&s[0..1], 16).ok()? * 17,
            u8::from_str_radix(&s[1..2], 16).ok()? * 17,
            u8::from_str_radix(&s[2..3], 16).ok()? * 17,
        ),
        _ => return None,
    };
    Some(slint::Color::from_rgb_u8(r, g, b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hex_colors() {
        let c = parse_hex("#5865F2").unwrap();
        assert_eq!((c.red(), c.green(), c.blue()), (0x58, 0x65, 0xF2));
        assert!(parse_hex("fff").is_some());
        assert!(parse_hex("nope").is_none());
        assert!(parse_hex("#12345").is_none());
    }
}
