// Reverse proxy client — connects a node xhod (without a public IP) to a
// server xhod (with a public IP) and serves gRPC over the SSH channel.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::ReverseProxyClientConfig;
use crate::daemon::gateway::auth::{ClientHandler, normalize_paths, parse_remote_target};
use crate::protocol::rpc as proto_rpc;

use super::{DaemonState, XhoRpcService};

/// Run the reverse proxy client loop: connect → serve → reconnect.
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

        tokio::select! {
            _ = sleep(reconnect_delay) => {}
            _ = &mut shutdown_rx => {
                info!(node = %node_name, "reverse proxy client shutting down");
                return;
            }
        }
    }
}

/// Connect to the server xhod, request reverse proxy subsystem, and serve
/// gRPC until the connection drops.
async fn connect_and_serve(config: &ReverseProxyClientConfig, state: &DaemonState) -> Result<()> {
    let target = parse_remote_target(&config.server_address)
        .map_err(|e| anyhow!("failed to parse server_address: {}", e))?;

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
    .map_err(|e| anyhow!("SSH connection failed: {}", e))?;

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

    // Open session channel and request xho-reverse:<node_name> subsystem.
    let channel = handle
        .channel_open_session()
        .await
        .map_err(|e| anyhow!("failed to open session channel: {}", e))?;

    let subsystem = format!(
        "{}:{}",
        super::reverse_proxy::REVERSE_SUBSYSTEM_NAME,
        config.node_name
    );
    channel
        .request_subsystem(true, &subsystem)
        .await
        .map_err(|e| anyhow!("failed to start xho-reverse subsystem: {}", e))?;

    let mut ssh_stream = channel.into_stream();

    info!(node = %config.node_name, "reverse proxy subsystem accepted");

    // Bridge the SSH channel to a tokio duplex pipe.
    let (mut bridge_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut ssh_for_bridge = ssh_stream;
    let bridge = tokio::spawn(async move {
        let _ = tokio::io::copy_bidirectional(&mut ssh_for_bridge, &mut bridge_io).await;
    });

    // Use mpsc channel (not once) so the stream stays alive while the
    // HTTP/2 handler waits for the client preface. The server-side tonic
    // client connects lazily on first RPC (15s health check), so the
    // preface arrives late. With `once`, tonic returns before that.
    let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<std::io::Result<_>>(1);
    let _ = stream_tx.send(Ok(server_io)).await;
    let incoming = tokio_stream::wrappers::ReceiverStream::new(stream_rx);

    let svc = proto_rpc::xho_rpc_server::XhoRpcServer::new(XhoRpcService {
        state: state.clone(),
    });

    let _handle = handle;

    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await
    });

    // Wait for SSH to break, then end the stream so tonic can exit.
    let _ = bridge.await;
    drop(stream_tx); // stream ends → serve_with_incoming returns

    server_task
        .await
        .map_err(|e| anyhow!("server task panicked: {}", e))?
        .map_err(|e| anyhow!("gRPC server: {}", e))?;

    info!(node = %config.node_name, "gRPC connection closed");
    Ok(())
}
