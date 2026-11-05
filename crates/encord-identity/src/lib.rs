//! encord-identity: official desktop client fingerprint (stable channel).
//!
//! Every network surface (REST headers, gateway IDENTIFY, WS handshake UA)
//! presents these values so encord looks like the stock Discord desktop app,
//! the same approach Abaddon uses with the web client. Constants track a
//! known-good stable release; bump them together when Discord ships.
//!
//! Sources: community captures of stable `1.0.9249` (Chromium 138,
//! Electron 37.6, build 582977, native build 86777).

use serde::Serialize;

/// Stable desktop version, e.g. `1.0.9249`.
pub const CLIENT_VERSION: &str = "1.0.9249";
/// Chromium embedded in that release.
pub const CHROME_VERSION: &str = "138.0.7204.251";
/// Electron embedded in that release.
pub const ELECTRON_VERSION: &str = "37.6.0";
/// `client_build_number` for that release.
pub const CLIENT_BUILD_NUMBER: u64 = 582977;
/// `native_build_number` for that release.
pub const NATIVE_BUILD_NUMBER: u64 = 86777;
/// Windows 10 22H2 build the capture came from.
pub const OS_VERSION: &str = "10.0.19045";
pub const OS_SDK_VERSION: &str = "19045";
pub const RELEASE_CHANNEL: &str = "stable";
pub const SYSTEM_LOCALE: &str = "en-US";
pub const DESKTOP_OS: &str = "Windows";
pub const DESKTOP_BROWSER: &str = "Discord Client";
/// Observed static across official desktop captures (same value in every
/// sampled `X-Super-Properties`); acts as a launch marker, not a secret.
pub const LAUNCH_SIGNATURE: &str = "477bea01-90cb-422d-9a38-aaa66ed3e25e";

/// Full browser UA the desktop client sends (also `browser_user_agent`).
pub fn desktop_user_agent() -> String {
    format!(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/{CLIENT_VERSION} Chrome/{CHROME_VERSION} Electron/{ELECTRON_VERSION} Safari/537.36"
    )
}

/// `X-Super-Properties` body. `browser_version` tracks the Electron release
/// (desktop convention), `has_client_mods` is honestly false.
///
/// Field order mirrors official desktop captures. `client_launch_id` is a
/// fresh UUID per process launch and `client_heartbeat_session_id` a fresh
/// UUID per session; without them the header looks automated to anti-abuse
/// (cf. Abaddon #417). `launch_signature` is the static marker observed in
/// every official capture.
#[derive(Clone, Debug, Serialize)]
pub struct SuperProperties {
    pub os: String,
    pub browser: String,
    pub release_channel: String,
    pub client_version: Option<String>,
    pub os_version: String,
    pub os_arch: Option<String>,
    pub app_arch: Option<String>,
    pub system_locale: String,
    pub has_client_mods: bool,
    pub client_launch_id: String,
    pub launch_signature: String,
    pub browser_user_agent: String,
    pub browser_version: String,
    pub os_sdk_version: Option<String>,
    pub client_build_number: u64,
    pub native_build_number: Option<u64>,
    pub client_event_source: Option<serde_json::Value>,
    pub client_heartbeat_session_id: String,
}

impl SuperProperties {
    pub fn desktop() -> Self {
        Self::desktop_with_ua(desktop_user_agent())
    }

    pub fn desktop_with_ua(ua: impl Into<String>) -> Self {
        Self {
            os: DESKTOP_OS.to_string(),
            browser: DESKTOP_BROWSER.to_string(),
            release_channel: RELEASE_CHANNEL.to_string(),
            client_version: Some(CLIENT_VERSION.to_string()),
            os_version: OS_VERSION.to_string(),
            os_arch: Some("x64".to_string()),
            app_arch: Some("x64".to_string()),
            system_locale: SYSTEM_LOCALE.to_string(),
            has_client_mods: false,
            client_launch_id: uuid::Uuid::new_v4().to_string(),
            launch_signature: LAUNCH_SIGNATURE.to_string(),
            browser_user_agent: ua.into(),
            browser_version: ELECTRON_VERSION.to_string(),
            os_sdk_version: Some(OS_SDK_VERSION.to_string()),
            client_build_number: CLIENT_BUILD_NUMBER,
            native_build_number: Some(NATIVE_BUILD_NUMBER),
            client_event_source: None,
            client_heartbeat_session_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

/// Base64 (STANDARD) encoding for the `X-Super-Properties` header.
pub fn super_properties_b64(props: &SuperProperties) -> String {
    use base64::Engine as _;
    let json = serde_json::to_string(props).unwrap_or_else(|_| "{}".to_string());
    base64::engine::general_purpose::STANDARD.encode(json)
}

/// Full IDENTIFY `properties` block of the official desktop client.
/// Empty `os`/`browser` fall back to the desktop defaults.
pub fn identify_properties(os: &str, browser: &str) -> serde_json::Value {
    let os = if os.is_empty() { DESKTOP_OS } else { os };
    let browser = if browser.is_empty() {
        DESKTOP_BROWSER
    } else {
        browser
    };
    serde_json::json!({
        "os": os,
        "browser": browser,
        "release_channel": RELEASE_CHANNEL,
        "client_version": CLIENT_VERSION,
        "os_version": OS_VERSION,
        "os_arch": "x64",
        "system_locale": SYSTEM_LOCALE,
        "browser_user_agent": desktop_user_agent(),
        "browser_version": ELECTRON_VERSION,
        "client_build_number": CLIENT_BUILD_NUMBER,
        "client_event_source": null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ua_tracks_client_release() {
        let ua = desktop_user_agent();
        assert!(ua.contains("discord/1.0.9249"), "{ua}");
        assert!(ua.contains("Electron/37.6.0"), "{ua}");
        assert!(ua.contains("Chrome/138.0.7204.251"), "{ua}");
    }

    #[test]
    fn super_properties_roundtrip() {
        let props = SuperProperties::desktop();
        assert!(!props.has_client_mods);
        let b64 = super_properties_b64(&props);
        let raw = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode(&b64)
                .unwrap()
        };
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["browser"], "Discord Client");
        assert_eq!(v["client_build_number"], 582977);
        assert_eq!(v["release_channel"], "stable");
        // UA and super-props stay in sync (same string both places).
        assert_eq!(v["browser_user_agent"], desktop_user_agent());
        // Anti-abuse parity: launch markers present, signature static,
        // session ids unique per construction.
        assert_eq!(v["launch_signature"], LAUNCH_SIGNATURE);
        let launch = v["client_launch_id"].as_str().unwrap();
        assert_eq!(launch.len(), 36);
        assert!(uuid::Uuid::parse_str(launch).is_ok());
        let beat = v["client_heartbeat_session_id"].as_str().unwrap();
        assert!(uuid::Uuid::parse_str(beat).is_ok());
        let other = SuperProperties::desktop();
        assert_ne!(other.client_launch_id, props.client_launch_id);
    }

    #[test]
    fn identify_properties_match_desktop() {
        let p = identify_properties("", "");
        assert_eq!(p["browser"], "Discord Client");
        assert_eq!(p["release_channel"], "stable");
        assert_eq!(p["client_version"], "1.0.9249");
        assert_eq!(p["client_build_number"], 582977);
        assert!(
            p.get("device").is_none(),
            "desktop identify has no device key"
        );
        let p = identify_properties("Linux", "Chrome");
        assert_eq!(p["os"], "Linux");
        assert_eq!(p["browser"], "Chrome");
    }
}
