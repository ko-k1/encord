//! encord-rpc: local discord-ipc server, like the original client.
//!
//! Games talk to stock Discord over a named pipe using opcode plus length
//! prefixed JSON frames. encord serves the same pipe, answers the handshake
//! itself, and forwards set-activity commands to the gateway as OP 3
//! presence. Clearing works too. Anything else gets an explicit unsupported
//! error reply instead of hanging the caller.

use serde_json::Value;
use thiserror::Error;

pub const OPCODE_HANDSHAKE: u32 = 0;
pub const OPCODE_FRAME: u32 = 1;
pub const OPCODE_CLOSE: u32 = 2;
pub const OPCODE_PING: u32 = 3;
pub const OPCODE_PONG: u32 = 4;

/// Rejection cap per frame (official clients enforce a similar bound).
pub const MAX_FRAME: usize = 64 * 1024;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Core -> gateway-bound activity, plus human-readable notices for the log.
#[derive(Clone, Debug)]
pub enum RpcCmd {
    Activity {
        client_id: String,
        pid: u32,
        activity: Option<Value>,
    },
    Notice(String),
}

/// Encode one wire frame.
pub fn encode_frame(opcode: u32, payload: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(payload).unwrap_or_default();
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&opcode.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Split an 8-byte header; validates the length cap.
pub fn decode_header(header: &[u8; 8]) -> Result<(u32, usize), RpcError> {
    let opcode = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
    let len = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes")) as usize;
    if len > MAX_FRAME {
        return Err(RpcError::FrameTooLarge(len));
    }
    Ok((opcode, len))
}

/// Handle the handshake frame (`{"v":1,"client_id":"…"}`).
/// Returns the READY reply plus the client id for logging.
pub fn handle_handshake(payload: &Value) -> Result<(Value, String), RpcError> {
    let client_id = payload
        .get("client_id")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::Protocol("handshake missing client_id".into()))?
        .to_string();
    let reply = serde_json::json!({
        "cmd": "DISPATCH",
        "evt": "READY",
        "nonce": null,
        "data": {
            "v": 1,
            "config": {
                "cdn_host": "cdn.discordapp.com",
                "api_endpoint": "//discord.com/api",
                "environment": "production",
            },
        },
    });
    Ok((reply, client_id))
}

/// Handle one command frame. Returns an optional core-bound command plus the
/// mandatory reply (every command gets exactly one reply carrying `nonce`).
pub fn handle_command(
    client_id: &str,
    payload: &Value,
) -> Result<(Option<RpcCmd>, Value), RpcError> {
    let cmd = payload.get("cmd").and_then(Value::as_str).unwrap_or("");
    let nonce = payload.get("nonce").cloned().unwrap_or(Value::Null);
    let _ = client_id;

    match cmd {
        "SET_ACTIVITY" => {
            let args = payload.get("args").unwrap_or(&Value::Null);
            let activity = args.get("activity").cloned().filter(|a| !a.is_null());
            let pid = args.get("pid").and_then(Value::as_u64).unwrap_or(0) as u32;
            let reply = serde_json::json!({
                "cmd": "SET_ACTIVITY",
                "evt": null,
                "nonce": nonce,
                "data": activity.clone().unwrap_or(Value::Null),
            });
            Ok((
                Some(RpcCmd::Activity {
                    client_id: client_id.to_string(),
                    pid,
                    activity,
                }),
                reply,
            ))
        }
        _ => Ok((
            None,
            serde_json::json!({
                "cmd": if cmd.is_empty() { Value::Null } else { Value::String(cmd.to_string()) },
                "evt": "ERROR",
                "nonce": nonce,
                "data": {
                    "code": 4000,
                    "message": format!("{cmd} is not supported by encord"),
                },
            }),
        )),
    }
}

#[cfg(windows)]
mod pipe {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};

    /// Serve the stock pipe names (`discord-ipc-0..9`).
    pub async fn serve(tx: tokio::sync::mpsc::UnboundedSender<RpcCmd>) -> Result<(), RpcError> {
        serve_with_prefix(tx, "discord-ipc").await
    }

    /// Serve a custom prefix (tests use an isolated name so they never
    /// touch the real client pipes).
    pub async fn serve_with_prefix(
        tx: tokio::sync::mpsc::UnboundedSender<RpcCmd>,
        prefix: &str,
    ) -> Result<(), RpcError> {
        for i in 0..10u8 {
            let tx = tx.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                if let Err(e) = pipe_index(i, &prefix, tx).await {
                    tracing::warn!("rpc pipe {prefix}-{i} exited: {e}");
                }
            });
        }
        // Run forever; per-index tasks own the listeners.
        std::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok(())
    }

    async fn pipe_index(
        i: u8,
        prefix: &str,
        tx: tokio::sync::mpsc::UnboundedSender<RpcCmd>,
    ) -> Result<(), RpcError> {
        let name = format!(r"\\.\pipe\{prefix}-{i}");
        // Claim detection: the first instance must be created with
        // `first_pipe_instance(true)`, which fails while another owner
        // (e.g. stock Discord) holds the name. In that case we back off and
        // retry instead of hijacking its game connections. Once we own the
        // name, later listener instances use `false`.
        let mut first = true;
        loop {
            let server = match ServerOptions::new()
                .first_pipe_instance(first)
                .create(&name)
            {
                Ok(s) => {
                    first = false;
                    s
                }
                Err(e) => {
                    tracing::debug!("rpc pipe {name} claimed by another owner: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
            };
            server.connect().await?;
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(server, tx).await {
                    tracing::debug!("rpc client error: {e}");
                }
            });
        }
    }

    /// One-shot client used by tests (and a template for game integrations).
    pub async fn connect_client(
        prefix: &str,
        i: u8,
    ) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, RpcError> {
        Ok(ClientOptions::new().open(format!(r"\\.\pipe\{prefix}-{i}"))?)
    }

    async fn handle_client<S>(
        stream: S,
        tx: tokio::sync::mpsc::UnboundedSender<RpcCmd>,
    ) -> Result<(), RpcError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let mut stream = stream;
        // First frame must be the handshake.
        let (op, payload) = read_frame(&mut stream).await?;
        if op != OPCODE_HANDSHAKE {
            return Err(RpcError::Protocol("expected handshake first".into()));
        }
        let (reply, client_id) = handle_handshake(&payload)?;
        write_frame(&mut stream, OPCODE_FRAME, &reply).await?;
        let _ = tx.send(RpcCmd::Notice(format!("rpc: {client_id} connected")));

        loop {
            let (op, payload) = read_frame(&mut stream).await?;
            match op {
                OPCODE_FRAME => {
                    let (cmd, reply) = handle_command(&client_id, &payload)?;
                    if let Some(cmd) = cmd {
                        let _ = tx.send(cmd);
                    }
                    write_frame(&mut stream, OPCODE_FRAME, &reply).await?;
                }
                OPCODE_PING => {
                    write_frame(&mut stream, OPCODE_PONG, &payload).await?;
                }
                OPCODE_CLOSE => break,
                _ => {}
            }
        }
        Ok(())
    }

    async fn read_frame<S>(stream: &mut S) -> Result<(u32, Value), RpcError>
    where
        S: AsyncReadExt + Unpin,
    {
        let mut header = [0u8; 8];
        stream.read_exact(&mut header).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                RpcError::Protocol("client disconnected".into())
            } else {
                RpcError::Io(e)
            }
        })?;
        let (opcode, len) = decode_header(&header)?;
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                RpcError::Protocol("client disconnected".into())
            } else {
                RpcError::Io(e)
            }
        })?;
        Ok((opcode, serde_json::from_slice(&body)?))
    }

    async fn write_frame<S>(stream: &mut S, opcode: u32, payload: &Value) -> Result<(), RpcError>
    where
        S: AsyncWriteExt + Unpin,
    {
        stream.write_all(&encode_frame(opcode, payload)).await?;
        Ok(())
    }
}

#[cfg(windows)]
pub use pipe::{connect_client, serve, serve_with_prefix};

/// Non-Windows: pipe server not implemented yet (Unix socket planned).
/// Returns immediately so the core runs everywhere.
#[cfg(not(windows))]
pub async fn serve(_tx: tokio::sync::mpsc::UnboundedSender<RpcCmd>) -> Result<(), RpcError> {
    tracing::warn!("discord-ipc server not implemented on this platform");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_codec_roundtrip() {
        let payload = serde_json::json!({"cmd": "PING", "nonce": "1"});
        let bytes = encode_frame(OPCODE_PING, &payload);
        let (op, len) = decode_header(bytes[0..8].try_into().unwrap()).unwrap();
        assert_eq!(op, OPCODE_PING);
        assert_eq!(len, bytes.len() - 8);
        let back: Value = serde_json::from_slice(&bytes[8..]).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn frame_length_cap() {
        let mut header = [0u8; 8];
        header[4..8].copy_from_slice(&(MAX_FRAME as u32 + 1).to_le_bytes());
        assert!(matches!(
            decode_header(&header),
            Err(RpcError::FrameTooLarge(_))
        ));
    }

    #[test]
    fn handshake_replies_ready() {
        let (reply, id) =
            handle_handshake(&serde_json::json!({"v": 1, "client_id": "123"})).unwrap();
        assert_eq!(id, "123");
        assert_eq!(reply["evt"], "READY");
        assert_eq!(reply["data"]["config"]["environment"], "production");
        assert!(handle_handshake(&serde_json::json!({"v": 1})).is_err());
    }

    #[test]
    fn set_activity_maps_and_echoes_nonce() {
        let activity = serde_json::json!({"details": "x", "type": 0});
        let (cmd, reply) = handle_command(
            "123",
            &serde_json::json!({
                "cmd": "SET_ACTIVITY",
                "nonce": "n1",
                "args": {"pid": 42, "activity": activity},
            }),
        )
        .unwrap();
        match cmd {
            Some(RpcCmd::Activity {
                client_id,
                pid,
                activity: Some(a),
            }) => {
                assert_eq!(client_id, "123");
                assert_eq!(pid, 42);
                assert_eq!(a, activity);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(reply["nonce"], "n1");
        assert_eq!(reply["data"], activity);
    }

    #[test]
    fn clear_activity_is_none() {
        let (cmd, reply) = handle_command(
            "123",
            &serde_json::json!({"cmd": "SET_ACTIVITY", "nonce": "n2", "args": {"activity": null}}),
        )
        .unwrap();
        assert!(matches!(cmd, Some(RpcCmd::Activity { activity: None, .. })));
        assert!(reply["data"].is_null());
    }

    #[test]
    fn unsupported_command_gets_error_reply() {
        let (cmd, reply) = handle_command(
            "123",
            &serde_json::json!({"cmd": "AUTHORIZE", "nonce": "n3", "args": {}}),
        )
        .unwrap();
        assert!(cmd.is_none());
        assert_eq!(reply["evt"], "ERROR");
        assert_eq!(reply["nonce"], "n3");
    }

    /// End-to-end over an isolated pipe name: handshake, activity, clear.
    /// Never touches the real `discord-ipc-*` names (stock Discord may own
    /// them on a dev machine).
    #[cfg(windows)]
    #[tokio::test]
    async fn pipe_roundtrip_on_isolated_name() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let prefix = format!("encord-test-ipc-{}", std::process::id());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let server_prefix = prefix.clone();
        let server = tokio::spawn(async move {
            let _ = serve_with_prefix(tx, &server_prefix).await;
        });
        // Wait for the listener (claim detection retries, so poll).
        let mut client = None;
        for _ in 0..50 {
            match connect_client(&prefix, 0).await {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let mut client = client.expect("test pipe listener came up");

        async fn roundtrip(
            client: &mut tokio::net::windows::named_pipe::NamedPipeClient,
            op: u32,
            payload: serde_json::Value,
        ) -> (u32, serde_json::Value) {
            client.write_all(&encode_frame(op, &payload)).await.unwrap();
            let mut header = [0u8; 8];
            client.read_exact(&mut header).await.unwrap();
            let (rop, len) = decode_header(&header).unwrap();
            let mut body = vec![0u8; len];
            client.read_exact(&mut body).await.unwrap();
            (rop, serde_json::from_slice(&body).unwrap())
        }

        let (op, reply) = roundtrip(
            &mut client,
            OPCODE_HANDSHAKE,
            serde_json::json!({"v": 1, "client_id": "e2e-game"}),
        )
        .await;
        assert_eq!(op, OPCODE_FRAME);
        assert_eq!(reply["evt"], "READY");
        assert!(matches!(rx.recv().await, Some(RpcCmd::Notice(_))));

        let activity = serde_json::json!({"type": 0, "details": "e2e"});
        let (op, reply) = roundtrip(
            &mut client,
            OPCODE_FRAME,
            serde_json::json!({
                "cmd": "SET_ACTIVITY", "nonce": "n1",
                "args": {"pid": 7, "activity": activity},
            }),
        )
        .await;
        assert_eq!(op, OPCODE_FRAME);
        assert_eq!(reply["nonce"], "n1");
        assert_eq!(reply["data"], activity);
        match rx.recv().await {
            Some(RpcCmd::Activity {
                client_id,
                pid,
                activity: Some(a),
            }) => {
                assert_eq!(client_id, "e2e-game");
                assert_eq!(pid, 7);
                assert_eq!(a, activity);
            }
            other => panic!("unexpected {other:?}"),
        }
        server.abort();
    }
}
