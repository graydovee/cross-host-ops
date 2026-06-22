// Reverse proxy client — connects a node xhod (without a public IP) to a
// server xhod (with a public IP) and serves gRPC over the SSH channel.
//
// The node registers itself as a dynamic gateway on the server, allowing
// xho clients on other machines to reach this node through the server.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use russh::ChannelStream;
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::transport::server::Connected;
use tracing::{info, warn};

use crate::config::ReverseProxyClientConfig;
use crate::daemon::gateway::auth::{ClientHandler, normalize_paths, parse_remote_target};
use crate::protocol::rpc as proto_rpc;

use super::{DaemonState, XhoRpcService};

/// Newtype wrapper around an SSH channel stream that adds `Connected` for
/// tonic's server transport. `ChannelStream` already implements tokio's
/// `AsyncRead` + `AsyncWrite`; we just delegate.
struct ReverseProxyServerIo(ChannelStream<client::Msg>);

impl Connected for ReverseProxyServerIo {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl tokio::io::AsyncRead for ReverseProxyServerIo {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for ReverseProxyServerIo {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Run the reverse proxy client loop: connect → serve → reconnect.
///
/// This function blocks until `shutdown_rx` fires. On connection errors,
/// it waits `reconnect_delay` and retries.
pub async fn run_reverse_proxy_client(
    config: ReverseProxyClientConfig,
    state: DaemonState,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let node_name = config.node_name.clone();
    let reconnect_delay = config.reconnect_delay;

    info!(node = %node_name, "reverse proxy client starting");

    loop {
        let connect_result = connect_and_serve(&config, &state).await;

        match &connect_result {
            Ok(()) => info!(node = %node_name, "reverse proxy connection closed"),
            Err(e) => warn!(
                node = %node_name,
                error = %format!("{e:#}"),
                "reverse proxy connection error"
            ),
        }

        // Wait before reconnecting, unless we're shutting down.
        tokio::select! {
            _ = sleep(reconnect_delay) => {}
            _ = &mut shutdown_rx => {
                info!(node = %node_name, "reverse proxy client shutting down");
                return;
            }
        }
    }
}

/// Connect to the server xhod, complete the handshake, and serve gRPC
/// until the connection drops.
async fn connect_and_serve(config: &ReverseProxyClientConfig, state: &DaemonState) -> Result<()> {
    // Parse the server address.
    let target = parse_remote_target(&config.server_address).map_err(|e| {
        anyhow!(
            "failed to parse server_address {:?}: {}",
            config.server_address,
            e
        )
    })?;

    let (identity_file, _known_hosts_path) =
        normalize_paths(Some(&config.identity_file), Some(&config.known_hosts_path))
            .map_err(|e| anyhow!("failed to normalize paths: {}", e))?;

    info!(
        host = %target.host,
        port = %target.port,
        user = %target.user,
        node = %config.node_name,
        "connecting reverse proxy to server xhod"
    );

    // SSH connect.
    let ssh_config = client::Config::default();
    let mut handle = client::connect(
        Arc::new(ssh_config),
        (target.host.as_str(), target.port),
        ClientHandler,
    )
    .await
    .map_err(|e| {
        anyhow!(
            "SSH connection to {}:{} failed: {}",
            target.host,
            target.port,
            e
        )
    })?;

    // Authenticate with public key.
    let key = load_secret_key(&identity_file, None)
        .map_err(|e| anyhow!("failed to load key {}: {}", identity_file, e))?;
    let hash_alg = handle.best_supported_rsa_hash().await?.flatten();
    let auth = handle
        .authenticate_publickey(
            &target.user,
            PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg),
        )
        .await
        .map_err(|e| anyhow!("SSH auth error: {}", e))?;

    if !auth.success() {
        bail!(
            "publickey authentication failed for {} using {}",
            target.user,
            identity_file
        );
    }

    // Open session channel and request xho-reverse subsystem.
    let channel = handle
        .channel_open_session()
        .await
        .map_err(|e| anyhow!("failed to open session channel: {}", e))?;

    channel
        .request_subsystem(true, super::reverse_proxy::REVERSE_SUBSYSTEM_NAME)
        .await
        .map_err(|e| anyhow!("failed to start xho-reverse subsystem: {}", e))?;

    let mut stream = channel.into_stream();

    // --- Registration handshake ---
    // Send: {"name":"node-2"}\n
    let reg = format!(r#"{{"name":"{}"}}"#, config.node_name);
    stream
        .write_all(reg.as_bytes())
        .await
        .map_err(|e| anyhow!("failed to send registration: {}", e))?;
    stream
        .write_all(b"\n")
        .await
        .map_err(|e| anyhow!("failed to send registration newline: {}", e))?;
    stream
        .flush()
        .await
        .map_err(|e| anyhow!("failed to flush registration: {}", e))?;

    // Read ack: {"status":"ok"}\n or {"status":"error","message":"..."}\n
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|e| anyhow!("failed to read ack: {}", e))?;

    let value: Value =
        serde_json::from_str(&line).map_err(|e| anyhow!("failed to parse ack JSON: {}", e))?;
    let status = value
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("error");

    if status != "ok" {
        let message = value
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        bail!("reverse proxy registration rejected: {}", message);
    }

    info!(node = %config.node_name, "reverse proxy registration accepted");

    // Recover the stream from BufReader for gRPC.
    let stream = reader.into_inner();
    let io = ReverseProxyServerIo(stream);

    // Start tonic gRPC server over the single SSH channel.
    // We keep incoming_tx alive so tonic doesn't see the stream end.
    // When the SSH channel breaks, the IO will error, tonic will close
    // the connection, and serve_with_incoming will return.
    let (incoming_tx, incoming_rx) = mpsc::channel::<std::io::Result<ReverseProxyServerIo>>(1);
    let _ = incoming_tx.send(Ok(io)).await;
    // Do NOT drop incoming_tx — it must stay alive to prevent the
    // ReceiverStream from returning None, which would cause tonic to
    // initiate graceful shutdown prematurely.

    let incoming = ReceiverStream::new(incoming_rx);
    let service = XhoRpcService {
        state: state.clone(),
    };

    // Keep the SSH handle alive to maintain the session.
    let _handle = handle;

    Server::builder()
        .add_service(proto_rpc::xho_rpc_server::XhoRpcServer::new(service))
        .serve_with_incoming_shutdown(incoming, std::future::pending::<()>())
        .await
        .map_err(|e| {
            warn!(node = %config.node_name, error = %e, "gRPC server exited with error");
            anyhow!("gRPC server over reverse proxy failed: {}", e)
        })?;

    info!(node = %config.node_name, "gRPC server exited normally");
    Ok(())
}
