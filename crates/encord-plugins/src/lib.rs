//! encord-plugins: M0 stub. M4 = wasmtime WASM sandbox + manifest +
//! capabilities + signing. This stub locks manifest + permission model.

use serde::{Deserialize, Serialize};

/// Plugin API version this host speaks. Bump on breaking host-fn change.
pub const PLUGIN_API_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginManifest {
    pub id: String,
    pub version: String,
    pub name: String,
    #[serde(default = "default_api")]
    pub api: u32,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default)]
    pub signature: Option<String>,
}

fn default_api() -> u32 {
    PLUGIN_API_VERSION
}

impl PluginManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() || !self.id.contains('.') {
            return Err("manifest id must look like author.plugin".into());
        }
        if self.api != PLUGIN_API_VERSION {
            return Err(format!(
                "unsupported plugin api {} (host speaks {})",
                self.api, PLUGIN_API_VERSION
            ));
        }
        for p in &self.permissions {
            if !is_known_permission(p) {
                return Err(format!("unknown permission: {p}"));
            }
        }
        Ok(())
    }
}

pub fn is_known_permission(p: &str) -> bool {
    const KNOWN: &[&str] = &[
        "gateway.subscribe",
        "ui.sidebarPanel",
        "ui.settingsTab",
        "ui.messageContextMenu",
        "messages.decorate",
        "commands.register",
        "storage.kv",
        "theme.hooks",
    ];
    KNOWN.contains(&p) || p.starts_with("net.fetch:allowlist=")
}
