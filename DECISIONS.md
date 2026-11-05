# DECISIONS.md

1. **Abaddon, not Equibop fork.** User pointed at Abaddon as the working model:
   native render, direct Gateway/REST, no Electron/webview. No Vencord/BdApi compat
   (new plugin API only).
2. **Rust over C++/GTK.** Abaddon is C++17 + gtkmm3. encord mirrors its behavior
   (web-client IDENTIFY, API v9, zlib-stream, ini, SQLite, CSS selectors) but in
   Rust for memory safety + `wasmtime` plugin isolation later. UI toolkit lands in
   M1 (Slint primary candidate; GTK4-rs kept as option since CSS selectors map).
3. **Gateway v9 + `compress=zlib-stream`.** Matches Abaddon; v10 bot gateway
   deliberately avoided for user-client behavior.
4. **REST base `https://discord.com/api/v9` + browser UA.** No automation-only
   endpoints, no `/science` telemetry, 429 `retry_after` honored.
5. **Cache trims to 200 msgs/channel.** Cheap bound for M0; virtualized UI bench
   in M1 decides final policy.
6. **Theme = tokens + CSS.** Tokens are source of truth; `to_gtk_css()` emits
   `:root` vars + Abaddon selectors so one theme drives both paths.
7. **Voice/plugins stubbed with locked API.** `encord-voice` (join/leave/mute) and
   `encord-plugins` (manifest `author.plugin`, api=1, allowlisted perms) exist so
   M1 UI work can't drift from M3/M4 shape.
8. **Official desktop fingerprint (encord-identity).** UA
   `discord/1.0.9249 Chrome/138 Electron/37.6`, full IDENTIFY properties,
   `X-Super-Properties` + `X-Discord-Locale` on REST. Constants bump together;
   ini overrides (`[discord] os/browser`, `[http] user_agent`) for fallback.
9. **Local Rich Presence (encord-rpc).** `discord-ipc-0..9` named pipes with
   claim detection (never hijack stock Discord's pipes), handshake +
   SET_ACTIVITY/clear forwarded as gateway OP 3, presence re-applied on
   reconnect. Unix socket stubbed, not yet implemented.
8. **GPU render via Slint FemtoVG-over-wgpu (Vulkan-first).** No webview: winit
   window, wgpu negotiates Vulkan first on Windows/Linux (Metal macOS, DX12/WGL
   fallback). Skia-Vulkan (`renderer-winit-skia-vulkan`) rejected for now — it
   needs an LLVM/Skia C++ build; wgpu is pure Rust and reaches the same GPU.
