# encord

Abaddon-style native Discord client in Rust. **Not Electron, no webview.**

Direct Gateway v9 + REST with the **official desktop fingerprint** (UA
`discord/1.0.9249`, full IDENTIFY props, `X-Super-Properties`), SQLite
instant-paint cache, `encord.ini` settings, GTK-selector CSS
(`res/css/encord.css`) + token themes (`themes/*.toml`), and a local
`discord-ipc` pipe so games set Rich Presence like with stock Discord.

> **ToS warning (same as Abaddon):** third-party clients violate Discord's ToS.
> The spam filter may force password resets. Avoid joining/leaving servers,
> frequent reconnects, and mass DMs from any third-party client. Use at own risk.

## Quick start (Windows)

```powershell
Copy-Item encord.ini.example encord.ini
# edit encord.ini -> [discord] token = ... + autoconnect = true (optional)
cargo test --workspace
cargo run -p encord                        # opens the GPU window (Vulkan/wgpu)
cargo run -p encord -- --check              # validate setup, no network
cargo run -p encord -- --config encord.ini --me
cargo run -p encord -- --config encord.ini --gateway
```

## Layout

```
crates/encord-core      shared types (Snowflake, Message, CoreEvent/UiCommand)
crates/encord-settings  abaddon.ini-compatible INI
crates/encord-cache     SQLite WAL + memory_db instant paint
crates/encord-gateway   Gateway v9 json+zlib-stream, identify-as-web, resume
crates/encord-rest      REST v9, browser UA, 429 retry_after
crates/encord-theme     token themes + CSS selector validation
crates/encord-identity  official desktop fingerprint (UA, super-props, builds)
crates/encord-rpc       local discord-ipc pipe (Rich Presence from games)
crates/encord-voice     M0 stub (M3: Opus/libsodium/RNNoise/DAVE)
crates/encord-plugins   M0 manifest model (M4: wasmtime sandbox)
crates/encord-ui        GPU window (Slint + FemtoVG over wgpu, Vulkan-first)
apps/encord             binary (default opens the window)
res/css/encord.css      Abaddon-selector stylesheet
themes/                 token themes
```

## Decisions

See [DECISIONS.md](DECISIONS.md).
