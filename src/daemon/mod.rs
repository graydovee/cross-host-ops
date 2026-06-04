#[allow(dead_code)]
pub mod connection;
#[allow(dead_code)]
pub mod gateway;
#[allow(dead_code)]
pub mod resolver;
#[allow(dead_code)]
pub mod review;
#[allow(dead_code)]
pub mod rpc;
#[allow(dead_code)]
pub mod ssh_server;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use russh::keys::ssh_key::{self, HashAlg, LineEnding};
use russh::server::{self, Auth, Msg, Server as _};
use russh::{Channel, ChannelId};
use tokio::fs;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{RwLock, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::transport::server::Connected;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{AppConfig, GatewayConfig, ReviewAction, default_config_path, load_server_config, validate_gateways};
use crate::logging::{init_logging, reopen_log_output};
use crate::protocol::{self, ExecRequest, ServerEvent, rpc as proto_rpc};
use self::ssh_server::remote_subsystem_name;
use crate::types::CopySpec;

use self::gateway::Gateway;
use self::gateway::auth::AuthPrompter;
use self::review::CommandReviewer;
use self::resolver::Resolver;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DaemonOrigin {
    CliSpawned,
    External,
}

impl DaemonOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CliSpawned => "cli_spawned",
            Self::External => "external",
        }
    }

    pub fn cli_controllable(self) -> bool {
        matches!(self, Self::CliSpawned)
    }
}

#[derive(Clone, Debug, Default)]
pub struct CliStartOptions {
    pub config_path: Option<String>,
    pub log_level: Option<String>,
}

// ---------------------------------------------------------------------------
// DaemonState — Gateway-based (the only daemon state)
// ---------------------------------------------------------------------------

/// The daemon state backed by the Gateway architecture.
/// Each gateway manages its own connections internally.
#[derive(Clone)]
pub struct DaemonState {
    pub config_path: PathBuf,
    pub config: Arc<RwLock<AppConfig>>,
    /// All gateways, ordered by config declaration. "local" is always first.
    pub gateways: Vec<(String, Arc<dyn Gateway>)>,
    pub reviewer: CommandReviewer,
    pub shutdown_tx: mpsc::Sender<()>,
    pub origin: DaemonOrigin,
    pub cli_start_options: CliStartOptions,
}

impl DaemonState {
    /// Reload the gateways configuration from disk.
    ///
    /// Re-reads the config file, runs `validate_gateways` on the new
    /// `gateways` list, and on success swaps the active config inside
    /// `Arc<RwLock<AppConfig>>`. On failure, logs the error and keeps the
    /// prior configuration unchanged.
    ///
    /// Note: gateways are NOT rebuilt here — a full restart is needed for
    /// gateway topology changes.
    pub async fn reload_config(&self) {
        let new_config = match AppConfig::load(Some(&self.config_path)) {
            Ok(cfg) => cfg,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    config_path = %self.config_path.display(),
                    "failed to read config during reload; keeping prior config"
                );
                return;
            }
        };

        if let Err(error) = validate_gateways(&new_config.gateways) {
            warn!(
                error = %format!("{error}"),
                config_path = %self.config_path.display(),
                "gateways validation failed during reload; keeping prior config"
            );
            return;
        }

        let mut config = self.config.write().await;
        config.gateways = new_config.gateways;
        info!(
            config_path = %self.config_path.display(),
            "gateways reloaded successfully"
        );
    }

    /// Find a gateway by name in the ordered list.
    pub fn find_gateway(&self, name: &str) -> Option<&Arc<dyn Gateway>> {
        self.gateways.iter().find(|(n, _)| n == name).map(|(_, gw)| gw)
    }
}

// ---------------------------------------------------------------------------
// RPC service
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct XhoRpcService {
    state: DaemonState,
}

// ---------------------------------------------------------------------------
// SSH server infrastructure (inline, will be migrated to ssh_server.rs later)
// ---------------------------------------------------------------------------

enum IncomingConn {
    Local(UnixStream),
    Remote(RemoteChannelStream),
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct RemoteConnectInfo {
    peer_addr: Option<SocketAddr>,
    ssh_user: String,
    public_key_fingerprint: String,
}

struct RemoteChannelStream {
    stream: russh::ChannelStream<russh::server::Msg>,
    info: RemoteConnectInfo,
}

#[derive(Clone)]
struct RemoteSshServer {
    state: DaemonState,
    accepted_tx: mpsc::Sender<IncomingConn>,
}

struct RemoteSshHandler {
    state: DaemonState,
    accepted_tx: mpsc::Sender<IncomingConn>,
    peer_addr: Option<SocketAddr>,
    accepted_user: Option<String>,
    accepted_fingerprint: Option<String>,
    channels: HashMap<ChannelId, Channel<Msg>>,
}

impl Connected for IncomingConn {
    type ConnectInfo = Option<RemoteConnectInfo>;

    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            Self::Local(_) => None,
            Self::Remote(stream) => Some(stream.info.clone()),
        }
    }
}

impl tokio::io::AsyncRead for IncomingConn {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            IncomingConn::Remote(stream) => std::pin::Pin::new(&mut stream.stream).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for IncomingConn {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            IncomingConn::Remote(stream) => std::pin::Pin::new(&mut stream.stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            IncomingConn::Remote(stream) => std::pin::Pin::new(&mut stream.stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            IncomingConn::Remote(stream) => std::pin::Pin::new(&mut stream.stream).poll_shutdown(cx),
        }
    }
}

impl server::Server for RemoteSshServer {
    type Handler = RemoteSshHandler;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        RemoteSshHandler {
            state: self.state.clone(),
            accepted_tx: self.accepted_tx.clone(),
            peer_addr,
            accepted_user: None,
            accepted_fingerprint: None,
            channels: HashMap::new(),
        }
    }
}

impl server::Handler for RemoteSshHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        user: &str,
        key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        let config = self.state.config.read().await.clone();
        if user != config.server.remote.user {
            return Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            });
        }
        if !is_authorized_key(Path::new(&config.server.remote.authorized_keys_path), key)? {
            return Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            });
        }
        self.accepted_user = Some(user.to_string());
        self.accepted_fingerprint = Some(key.fingerprint(HashAlg::Sha256).to_string());
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        self.channels.insert(channel.id(), channel);
        Ok(true)
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)?;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        _data: &[u8],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)?;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        if name != remote_subsystem_name() {
            session.channel_failure(channel)?;
            return Ok(());
        }

        let Some(channel_stream) = self.channels.remove(&channel) else {
            session.channel_failure(channel)?;
            return Ok(());
        };

        let info = RemoteConnectInfo {
            peer_addr: self.peer_addr,
            ssh_user: self.accepted_user.clone().unwrap_or_default(),
            public_key_fingerprint: self.accepted_fingerprint.clone().unwrap_or_default(),
        };
        session.channel_success(channel)?;
        self.accepted_tx
            .send(IncomingConn::Remote(RemoteChannelStream {
                stream: channel_stream.into_stream(),
                info,
            }))
            .await
            .map_err(|_| anyhow!("remote incoming queue closed"))?;
        Ok(())
    }

    async fn tcpip_forward(
        &mut self,
        _address: &str,
        _port: &mut u32,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn streamlocal_forward(
        &mut self,
        _socket_path: &str,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Daemon entrypoints
// ---------------------------------------------------------------------------

pub async fn run(config_path: Option<PathBuf>) -> Result<()> {
    run_with_overrides(
        config_path,
        None,
        DaemonOrigin::External,
        CliStartOptions::default(),
    )
    .await
}

pub async fn run_with_overrides(
    config_path: Option<PathBuf>,
    log_level_override: Option<String>,
    origin: DaemonOrigin,
    cli_start_options: CliStartOptions,
) -> Result<()> {
    let config_path = config_path.unwrap_or_else(default_config_path);
    let mut loaded = AppConfig::load(Some(&config_path))?;
    if let Some(level) = log_level_override {
        loaded.server.log_level = level;
    }
    let _log_guard = init_logging(loaded.server.log_path.clone(), &loaded.server.log_level)?;
    info!(config_path = %config_path.display(), "starting xhod");

    let config = Arc::new(RwLock::new(loaded.clone()));
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

    // Build all gateways from the configuration.
    let auth_prompter: Arc<AuthPrompter> = Arc::new(|_req| {
        Box::pin(async { Ok(String::new()) })
    });
    let gateways = gateway::build_gateways(
        config.clone(),
        &loaded.ssh.server_config_path,
        &loaded.gateways,
        auth_prompter,
    );

    let state = DaemonState {
        config_path,
        config: config.clone(),
        gateways,
        reviewer: CommandReviewer::new()?,
        shutdown_tx,
        origin,
        cli_start_options,
    };

    let local_socket_path = if state.config.read().await.server.local.enable {
        let socket_path = PathBuf::from(state.config.read().await.server.local.socket_path.clone());
        ensure_socket_parent(&socket_path).await?;
        if socket_path.exists() {
            let _ = fs::remove_file(&socket_path).await;
        }
        Some(socket_path)
    } else {
        None
    };

    let remote_listener = if state.config.read().await.server.remote.enable {
        let remote_config = state.config.read().await.server.remote.clone();
        ensure_remote_parent(&remote_config).await?;
        ensure_remote_host_key(&remote_config).await?;
        Some(
            TcpListener::bind(&remote_config.listen_addr)
                .await
                .with_context(|| format!("failed to bind {}", remote_config.listen_addr))?,
        )
    } else {
        None
    };

    let (incoming_tx, incoming_rx) = mpsc::channel::<IncomingConn>(32);

    if let Some(socket_path) = local_socket_path.clone() {
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        info!(socket_path = %socket_path.display(), "listening on local socket");
        let local_tx = incoming_tx.clone();
        tokio::spawn(async move {
            let mut incoming = UnixListenerStream::new(listener);
            use tokio_stream::StreamExt;
            while let Some(result) = incoming.next().await {
                match result {
                    Ok(stream) => {
                        if local_tx.send(IncomingConn::Local(stream)).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "failed to accept local socket connection");
                    }
                }
            }
        });
    }

    if let Some(listener) = remote_listener {
        let remote_config = state.config.read().await.server.remote.clone();
        let host_keys = load_host_keys(Path::new(&remote_config.host_key_path))?;
        let mut server = RemoteSshServer {
            state: state.clone(),
            accepted_tx: incoming_tx.clone(),
        };
        let config = Arc::new(server::Config {
            auth_rejection_time: Duration::from_secs(1),
            auth_rejection_time_initial: Some(Duration::from_secs(0)),
            keys: host_keys,
            inactivity_timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        });
        info!(listen_addr = %remote_config.listen_addr, "listening on remote SSH");
        let running = async move {
            let listener = listener;
            server.run_on_socket(config, &listener).await
        };
        tokio::spawn(async move {
            if let Err(error) = running.await {
                error!(error = %error, "remote SSH listener stopped");
            }
        });
    }

    // Spawn idle reaper for all gateways.
    let reaper_state = state.clone();
    tokio::spawn(async move {
        loop {
            let interval = reaper_state.config.read().await.server.reaper_interval;
            sleep(interval).await;
            for (_name, gw) in &reaper_state.gateways {
                gw.prune_idle().await;
            }
            debug!("idle connection reaper tick");
        }
    });

    // Spawn SIGHUP handler for config reload + log reopen.
    let sighup_state = state.clone();
    tokio::spawn(async move {
        let Ok(mut sighup) = signal(SignalKind::hangup()) else {
            return;
        };
        while sighup.recv().await.is_some() {
            match reopen_log_output() {
                Ok(()) => info!("reopened log output after SIGHUP"),
                Err(error) => warn!(error = %format!("{error:#}"), "failed to reopen log output after SIGHUP"),
            }
            sighup_state.reload_config().await;
        }
    });

    let incoming = ReceiverStream::new(receiver_map_incoming(incoming_rx));
    Server::builder()
        .add_service(proto_rpc::xho_rpc_server::XhoRpcServer::new(XhoRpcService {
            state: state.clone(),
        }))
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = shutdown_rx.recv().await;
        })
        .await?;
    if let Some(socket_path) = local_socket_path {
        if socket_path.exists() {
            let _ = fs::remove_file(&socket_path).await;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RPC trait implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl proto_rpc::xho_rpc_server::XhoRpc for XhoRpcService {
    type ExecuteStream = ReceiverStream<Result<proto_rpc::ExecuteResponse, Status>>;
    type CopyStream = ReceiverStream<Result<proto_rpc::CopyResponse, Status>>;

    async fn execute(
        &self,
        request: Request<Streaming<proto_rpc::ExecuteRequest>>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        info!("accepted execute stream");
        let mut inbound = request.into_inner();
        let state = self.state.clone();
        let (sender, receiver) = mpsc::channel(64);

        tokio::spawn(async move {
            let result = async {
                let Some(first) = inbound.message().await? else {
                    bail!("client disconnected before start request");
                };
                let Some(proto_rpc::execute_request::Request::Start(start)) = first.request else {
                    bail!("first execute stream message must be start");
                };
                let exec = ExecRequest {
                    target: start.target,
                    argv: start.argv,
                    pty: start.pty,
                    no_pty: start.no_pty,
                    stdin: start.stdin,
                    timeout_ms: start.timeout_ms,
                    interactive: start.interactive,
                    term_cols: start.term_cols,
                    term_rows: start.term_rows,
                    shell: start.shell,
                    no_shell: start.no_shell,
                };
                process_execute(exec, &state, &mut inbound, &sender).await
            }
            .await;

            if let Err(error) = result {
                error!(error = %format!("{error:#}"), "execute stream failed");
                let _ = sender
                    .send(Ok(protocol::error_response(error.to_string())))
                    .await;
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn copy(
        &self,
        request: Request<Streaming<proto_rpc::CopyRequest>>,
    ) -> Result<Response<Self::CopyStream>, Status> {
        let is_remote = request
            .extensions()
            .get::<Option<RemoteConnectInfo>>()
            .and_then(|info| info.as_ref())
            .is_some();
        let mut inbound = request.into_inner();
        let state = self.state.clone();
        let (sender, receiver) = mpsc::channel(16);

        tokio::spawn(async move {
            let result = async {
                let Some(first) = inbound.message().await? else {
                    bail!("client disconnected before copy start request");
                };
                let Some(proto_rpc::copy_request::Request::Start(start)) = first.request else {
                    bail!("first copy stream message must be start");
                };

                // Defense in depth: reject Copy requests received over the
                // xho-rpc subsystem when local_path is non-empty.
                if is_remote && !start.local_path.is_empty() {
                    bail!("Copy requests received over xho-rpc must not specify local_path");
                }

                let (target_input, mut spec, timeout_ms): (String, CopySpec, u64) = protocol::copy_spec_from_rpc(start)?;
                let config = state.config.read().await.clone();
                let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
                    .unwrap_or_default();
                let resolver = Resolver::new(&config, &server_config, &config.gateways);
                let routes = resolver.resolve(&target_input)?;
                let route = routes
                    .first()
                    .ok_or_else(|| anyhow!("no resolved target candidates"))?;
                info!(
                    target = %route.end_target,
                    gateway = %route.gateway_name,
                    direction = ?spec.direction,
                    local_path = %spec.local_path,
                    remote_path = %spec.remote_path,
                    recursive = spec.recursive,
                    timeout_ms,
                    "copy request"
                );

                let gateway = state
                    .find_gateway(&route.gateway_name)
                    .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;

                // When copy data arrives over an xho-rpc subsystem, the remote
                // daemon materializes it into a temp file before handing off to
                // local SFTP. When the next hop is another xhod gateway, the
                // daemon relays data directly over channels.
                let mut remote_temp_path: Option<PathBuf> = None;
                let mut download_relay_task: Option<tokio::task::JoinHandle<()>> = None;
                if is_remote {
                    use crate::types::CopyDirection;
                    match spec.direction {
                        CopyDirection::Upload => {
                            let temp_path = remote_copy_temp_path("upload");
                            receive_copy_upload_to_temp(&mut inbound, &temp_path).await?;
                            spec.local_path = temp_path.display().to_string();
                            remote_temp_path = Some(temp_path);
                        }
                        CopyDirection::Download => {
                            let temp_path = remote_copy_temp_path("download");
                            spec.local_path = temp_path.display().to_string();
                            remote_temp_path = Some(temp_path);
                        }
                    }
                } else if gateway.kind() == gateway::GatewayKind::Xhod {
                    use crate::types::CopyDirection;
                    match spec.direction {
                        CopyDirection::Upload => {
                            // Upload relay: spawn a task to forward CopyDataChunk messages
                            // from the client gRPC inbound stream into a channel, which
                            // XhodConnection::copy will read instead of a local file.
                            let (upload_tx, upload_rx) = mpsc::channel::<(Vec<u8>, bool)>(16);
                            spec.relay_upload_rx = Some(upload_rx);

                            tokio::spawn(async move {
                                loop {
                                    match inbound.message().await {
                                        Ok(Some(msg)) => {
                                            match msg.request {
                                                Some(proto_rpc::copy_request::Request::DataChunk(chunk)) => {
                                                    let eof = chunk.eof;
                                                    if upload_tx.send((chunk.data, eof)).await.is_err() {
                                                        break;
                                                    }
                                                    if eof {
                                                        break;
                                                    }
                                                }
                                                Some(proto_rpc::copy_request::Request::AuthInput(_)) => {
                                                    // Auth input not handled in relay mode; skip.
                                                }
                                                _ => {}
                                            }
                                        }
                                        Ok(None) | Err(_) => {
                                            // Client disconnected; the channel drop signals EOF.
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        CopyDirection::Download => {
                            // Download relay: XhodConnection::copy sends data chunks to this
                            // channel; we forward them as CopyDataChunk events on the gRPC
                            // response stream back to the client.
                            let (download_tx, mut download_rx) = mpsc::channel::<(Vec<u8>, bool)>(16);
                            spec.relay_download_tx = Some(download_tx);

                            let sender_clone = sender.clone();
                            let relay_task = tokio::spawn(async move {
                                while let Some((data, eof)) = download_rx.recv().await {
                                    let chunk_response = proto_rpc::CopyResponse {
                                        event: Some(proto_rpc::copy_response::Event::DataChunk(
                                            proto_rpc::CopyDataChunk { data, eof },
                                        )),
                                    };
                                    if sender_clone.send(Ok(chunk_response)).await.is_err() {
                                        break;
                                    }
                                    if eof {
                                        break;
                                    }
                                }
                            });
                            download_relay_task = Some(relay_task);
                        }
                    }
                }

                // If timeout is specified, create a deadline future.
                let copy_timeout = if timeout_ms > 0 {
                    Some(tokio::time::sleep(Duration::from_millis(timeout_ms)))
                } else {
                    None
                };
                tokio::pin!(copy_timeout);

                let copy_direction = spec.direction.clone();
                let copy_task = {
                    let gw = gateway.clone();
                    let end_target = route.end_target.clone();
                    tokio::spawn(async move { gw.copy(&end_target, spec).await })
                };
                tokio::pin!(copy_task);

                loop {
                    tokio::select! {
                        // Timeout enforcement: abort copy with exit code 124
                        _ = async {
                            match copy_timeout.as_mut().as_pin_mut() {
                                Some(deadline) => deadline.await,
                                None => std::future::pending().await,
                            }
                        } => {
                            warn!(timeout_ms, "copy timed out");
                            copy_task.abort();
                            if let Some(ref temp_path) = remote_temp_path {
                                let _ = fs::remove_file(temp_path).await;
                            }
                            sender
                                .send(Ok(protocol::copy_error_response("copy timed out (exit code 124)")))
                                .await
                                .map_err(|_| anyhow!("copy client stream closed"))?;
                            break;
                        }
                        result = &mut copy_task => {
                            result?.map_err(|e| {
                                if let Some(ref temp_path) = remote_temp_path {
                                    let _ = std::fs::remove_file(temp_path);
                                }
                                anyhow!("{}", e)
                            })?;
                            if is_remote && copy_direction == crate::types::CopyDirection::Download {
                                if let Some(ref temp_path) = remote_temp_path {
                                    send_copy_download_from_temp(&sender, temp_path).await?;
                                }
                            }
                            if let Some(relay_task) = download_relay_task.take() {
                                let _ = relay_task.await;
                            }
                            sender
                                .send(Ok(protocol::copy_complete_response(String::new())))
                                .await
                                .map_err(|_| anyhow!("copy client stream closed"))?;
                            if let Some(ref temp_path) = remote_temp_path {
                                let _ = fs::remove_file(temp_path).await;
                            }
                            break;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;

            if let Err(error) = result {
                error!(error = %format!("{error:#}"), "copy stream failed");
                let _ = sender
                    .send(Ok(protocol::copy_error_response(error.to_string())))
                    .await;
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn status(
        &self,
        _request: Request<proto_rpc::StatusRequest>,
    ) -> Result<Response<proto_rpc::StatusResponse>, Status> {
        info!("status request");
        let config = self.state.config.read().await.clone();
        let socket_path = config.server.local.socket_path.clone();
        let gateways: Vec<proto_rpc::GatewayStatus> = config
            .gateways
            .iter()
            .map(|entry| {
                let (name, kind, address) = match entry {
                    GatewayConfig::Xhod(c) => (c.name.clone(), "xhod".to_string(), c.address.clone()),
                    GatewayConfig::Jumpserver(c) => (c.name.clone(), "jumpserver".to_string(), format!("{}:{}", c.host, c.port)),
                    GatewayConfig::Direct(c) => (c.name.clone(), "direct".to_string(), format!("{}:{}", c.host, c.port)),
                };
                proto_rpc::GatewayStatus {
                    name,
                    kind,
                    address,
                    sub_status: None,
                }
            })
            .collect();
        let response = proto_rpc::StatusResponse {
            daemon_running: true,
            local_socket_path: socket_path,
            active_executions: 0,
            pools: Vec::new(),
            daemon_origin: self.state.origin.as_str().to_string(),
            cli_controllable: self.state.origin.cli_controllable(),
            cli_start_config_path: self
                .state
                .cli_start_options
                .config_path
                .clone()
                .unwrap_or_default(),
            cli_start_log_level: self
                .state
                .cli_start_options
                .log_level
                .clone()
                .unwrap_or_default(),
            gateways,
            remote_listening: config.server.remote.enable,
            remote_addr: if config.server.remote.enable {
                config.server.remote.listen_addr.clone()
            } else {
                String::new()
            },
            remote_ssh_user: if config.server.remote.enable {
                config.server.remote.user.clone()
            } else {
                String::new()
            },
        };
        Ok(Response::new(response))
    }

    async fn list_servers(
        &self,
        _request: Request<proto_rpc::ServerListRequest>,
    ) -> Result<Response<proto_rpc::ServerListResponse>, Status> {
        let config = self.state.config.read().await.clone();
        let path = PathBuf::from(&config.ssh.server_config_path);

        let (tagged_entries, source_status) = rpc::process_list_servers(&self.state).await;

        // Convert entries to RPC format (flat list without source).
        let servers: Vec<proto_rpc::ServerEntry> = tagged_entries
            .iter()
            .map(|(entry, _source)| protocol::server_entry_to_rpc(entry.clone()))
            .collect();

        // Build merged RPC representation with correct source tags.
        let rows: Vec<crate::protocol::ServerListRow> = tagged_entries
            .into_iter()
            .map(|(entry, source)| crate::protocol::ServerListRow {
                server: entry,
                source,
            })
            .collect();
        let merged = crate::protocol::MergedServerList {
            rows,
            source_status,
        };
        let merged_rpc = protocol::merged_server_list_to_rpc(merged);

        Ok(Response::new(proto_rpc::ServerListResponse {
            server_config_path: path.display().to_string(),
            servers,
            merged: Some(merged_rpc),
        }))
    }

    async fn shutdown(
        &self,
        _request: Request<proto_rpc::ShutdownRequest>,
    ) -> Result<Response<proto_rpc::InfoResponse>, Status> {
        shutdown_daemon(&self.state)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(proto_rpc::InfoResponse {
            message: "daemon shutting down".to_string(),
        }))
    }

    async fn update_config(
        &self,
        request: Request<proto_rpc::UpdateConfigRequest>,
    ) -> Result<Response<proto_rpc::UpdateConfigResponse>, Status> {
        let req = request.into_inner();
        match req.mutation_type.as_str() {
            "add_gateway" => {
                let alias = req.name.trim().to_string();
                if alias.is_empty() {
                    return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                        success: false,
                        message: "name must not be empty".to_string(),
                    }));
                }
                if crate::config::RESERVED_NAMES.contains(&alias.as_str()) {
                    return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                        success: false,
                        message: format!(
                            "name '{}' is reserved (reserved names: {:?})",
                            alias,
                            crate::config::RESERVED_NAMES
                        ),
                    }));
                }
                // Check for collision with existing gateways
                {
                    let config = self.state.config.read().await;
                    if config.gateways.iter().any(|g| g.name() == alias) {
                        return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                            success: false,
                            message: format!(
                                "name '{}' is already used by an existing gateway",
                                alias
                            ),
                        }));
                    }
                }

                let kind_str = req.kind.trim().to_string();
                match kind_str.as_str() {
                    "xhod" => {
                        let new_entry = GatewayConfig::Xhod(crate::config::XhodGatewayConfig {
                            name: alias.clone(),
                            address: req.address.clone(),
                            identity_file: req.identity_file.clone(),
                            known_hosts_path: req.known_hosts_path.clone(),
                        });

                        // Add to in-memory config
                        {
                            let mut config = self.state.config.write().await;
                            config.gateways.push(new_entry);
                        }
                        if let Err(e) = atomic_write_config(&self.state).await {
                            // Rollback the in-memory change
                            let mut config = self.state.config.write().await;
                            config.gateways.retain(|g| g.name() != alias);
                            return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                                success: false,
                                message: format!("failed to write config: {}", e),
                            }));
                        }

                        // Hot-reload to validate
                        self.state.reload_config().await;

                        info!(name = %alias, "added gateway via UpdateConfig");
                        Ok(Response::new(proto_rpc::UpdateConfigResponse {
                            success: true,
                            message: format!("gateway '{}' added successfully", alias),
                        }))
                    }
                    other => {
                        Ok(Response::new(proto_rpc::UpdateConfigResponse {
                            success: false,
                            message: format!(
                                "add_gateway via RPC only supports kind 'xhod', got '{}'",
                                other
                            ),
                        }))
                    }
                }
            }
            "remove_gateway" => {
                let alias = req.name.trim().to_string();
                if alias.is_empty() {
                    return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                        success: false,
                        message: "name must not be empty".to_string(),
                    }));
                }

                // Find and remove the entry
                let removed = {
                    let mut config = self.state.config.write().await;
                    let before_len = config.gateways.len();
                    config.gateways.retain(|g| g.name() != alias);
                    before_len != config.gateways.len()
                };

                if !removed {
                    return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                        success: false,
                        message: format!("gateway '{}' not found", alias),
                    }));
                }

                if let Err(e) = atomic_write_config(&self.state).await {
                    // Reload from disk to restore consistency
                    self.state.reload_config().await;
                    return Ok(Response::new(proto_rpc::UpdateConfigResponse {
                        success: false,
                        message: format!("failed to write config: {}", e),
                    }));
                }

                // Hot-reload to ensure consistency
                self.state.reload_config().await;

                info!(name = %alias, "removed gateway via UpdateConfig");
                Ok(Response::new(proto_rpc::UpdateConfigResponse {
                    success: true,
                    message: format!("gateway '{}' removed successfully", alias),
                }))
            }
            other => Ok(Response::new(proto_rpc::UpdateConfigResponse {
                success: false,
                message: format!("unknown mutation_type: '{}'", other),
            })),
        }
    }

    async fn list_gateways(
        &self,
        _request: Request<proto_rpc::ListGatewaysRequest>,
    ) -> Result<Response<proto_rpc::ListGatewaysResponse>, Status> {
        let config = self.state.config.read().await.clone();
        let gateways: Vec<proto_rpc::GatewayStatus> = config
            .gateways
            .iter()
            .map(|entry| {
                let (name, kind, address) = match entry {
                    GatewayConfig::Xhod(c) => (c.name.clone(), "xhod".to_string(), c.address.clone()),
                    GatewayConfig::Jumpserver(c) => (c.name.clone(), "jumpserver".to_string(), format!("{}:{}", c.host, c.port)),
                    GatewayConfig::Direct(c) => (c.name.clone(), "direct".to_string(), format!("{}:{}", c.host, c.port)),
                };
                proto_rpc::GatewayStatus {
                    name,
                    kind,
                    address,
                    sub_status: None,
                }
            })
            .collect();
        Ok(Response::new(proto_rpc::ListGatewaysResponse { gateways }))
    }
}

// ---------------------------------------------------------------------------
// Execute / Interactive request processing (gateway-based)
// ---------------------------------------------------------------------------

/// Process an execute request using the gateway architecture.
async fn process_execute(
    request: ExecRequest,
    state: &DaemonState,
    inbound: &mut Streaming<proto_rpc::ExecuteRequest>,
    sender: &mpsc::Sender<Result<proto_rpc::ExecuteResponse, Status>>,
) -> Result<()> {
    if request.argv.is_empty() {
        bail!("argv must not be empty");
    }

    // Dispatch to interactive execution path when requested.
    if request.interactive {
        if !request.pty || request.no_pty {
            send_execute_event(
                sender,
                ServerEvent::Error {
                    message: "interactive mode requires pty (--pty) and is incompatible with --no-pty".to_string(),
                },
            )
            .await?;
            return Ok(());
        }
        if request.term_cols == 0 || request.term_rows == 0 {
            send_execute_event(
                sender,
                ServerEvent::Error {
                    message: "interactive mode requires term_cols > 0 and term_rows > 0".to_string(),
                },
            )
            .await?;
            return Ok(());
        }
        return process_interactive_execute(request, state, inbound, sender).await;
    }

    let execution_id = Uuid::new_v4();
    let config = state.config.read().await.clone();
    let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
        .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.gateways);
    let routes = resolver.resolve(&request.target)?;
    let route = routes
        .first()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;

    let review_command = request.argv.join(" ");

    info!(
        execution_id = %execution_id,
        input = %request.target,
        end_target = %route.end_target,
        gateway = %route.gateway_name,
        "resolved target"
    );

    // Review logic
    let decision = match state
        .reviewer
        .review(&config.review, &route.end_target, &request.argv, &review_command)
        .await
    {
        Ok(result) => result,
        Err(error) => {
            warn!(
                execution_id = %execution_id,
                error = %format!("{error:#}"),
                "review failed"
            );
            let action = config.review.failure_action;
            let risk_level = crate::config::RiskLevel::Dangerous;
            send_execute_event(
                sender,
                ServerEvent::ReviewResult {
                    execution_id,
                    risk_level,
                    action,
                    reason: format!("review failed: {error:#}"),
                    matched_whitelist_reason: None,
                },
            )
            .await?;
            match action {
                ReviewAction::Allow | ReviewAction::Warn => None,
                ReviewAction::Confirm => {
                    wait_for_confirmation(execution_id, inbound, sender, "review service failed")
                        .await?;
                    None
                }
                ReviewAction::Deny => {
                    send_execute_event(
                        sender,
                        ServerEvent::Error {
                            message: format!("review failed and policy is deny: {error:#}"),
                        },
                    )
                    .await?;
                    return Ok(());
                }
            }
        }
    };

    if let Some(decision) = decision {
        info!(
            execution_id = %execution_id,
            risk_level = %decision.risk_level,
            action = %decision.action,
            matched_whitelist_reason = decision.matched_whitelist_reason.as_deref().unwrap_or(""),
            "review completed"
        );
        send_execute_event(
            sender,
            ServerEvent::ReviewResult {
                execution_id,
                risk_level: decision.risk_level,
                action: decision.action,
                reason: decision.reason.clone(),
                matched_whitelist_reason: decision.matched_whitelist_reason.clone(),
            },
        )
        .await?;
        match decision.action {
            ReviewAction::Allow | ReviewAction::Warn => {}
            ReviewAction::Confirm => {
                debug!(execution_id = %execution_id, "waiting for confirmation");
                wait_for_confirmation(execution_id, inbound, sender, &decision.reason).await?;
            }
            ReviewAction::Deny => {
                warn!(execution_id = %execution_id, "execution denied by review");
                send_execute_event(
                    sender,
                    ServerEvent::Error {
                        message: format!("command denied: {}", decision.reason),
                    },
                )
                .await?;
                return Ok(());
            }
        }
    }

    // Execute via gateway
    let gateway = state
        .find_gateway(&route.gateway_name)
        .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    // Create stdin forwarding channel when the client requests stdin.
    let (mut stdin_tx, stdin_rx) = if request.stdin {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(32);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let gw_request = gateway::ExecRequest {
        argv: request.argv.clone(),
        sender: event_tx,
        pty: request.pty,
        cols: request.term_cols,
        rows: request.term_rows,
        shell: request.shell.clone(),
        no_shell: request.no_shell,
        timeout_ms: request.timeout_ms,
        stdin: request.stdin,
        stdin_rx: std::sync::Mutex::new(stdin_rx),
    };

    let timeout_ms = request.timeout_ms;
    let stdin_enabled = request.stdin;
    let gw = gateway.clone();
    let end_target = route.end_target.clone();
    let exec_task = tokio::spawn(async move {
        gw.exec(&end_target, &gw_request).await
    });
    tokio::pin!(exec_task);

    // If timeout is specified, create a deadline future.
    let timeout_deadline = if timeout_ms > 0 {
        Some(tokio::time::sleep(Duration::from_millis(timeout_ms)))
    } else {
        None
    };
    tokio::pin!(timeout_deadline);

    // Track whether the client's inbound stream has closed.  Once it has,
    // `inbound.message()` returns `Ok(None)` immediately on every poll, which
    // would otherwise turn the select! loop into a tight busy-loop spamming
    // logs and starving the exec_task from making progress.
    let mut inbound_closed = false;

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                send_execute_event(sender, event).await?;
            }
            // Handle inbound client messages (StdinData forwarding).  Disabled
            // once the stream has closed to avoid a busy-loop on Ok(None).
            msg = inbound.message(), if !inbound_closed => {
                match msg {
                    Ok(Some(message)) => {
                        match message.request {
                            Some(proto_rpc::execute_request::Request::StdinData(stdin_data)) => {
                                if stdin_enabled {
                                    if stdin_data.data.is_empty() {
                                        // Explicit EOF sentinel: drop stdin sender
                                        // to signal EOF to the gateway/connection layer.
                                        // We do NOT close the inbound branch — keep
                                        // the bidirectional stream alive so the remote
                                        // can still send stdout/stderr/ExitStatus.
                                        info!(execution_id = %execution_id, "received explicit stdin EOF sentinel");
                                        stdin_tx.take();
                                    } else if let Some(ref tx) = stdin_tx {
                                        // Forward stdin bytes to gateway; ignore send errors
                                        // (channel may be closed if process exited).
                                        info!(execution_id = %execution_id, bytes = stdin_data.data.len(), "forwarding stdin to gateway");
                                        let _ = tx.send(stdin_data.data).await;
                                    }
                                }
                                // When stdin is not enabled, silently ignore StdinData messages.
                            }
                            _ => {
                                // Ignore other message types in non-interactive mode.
                            }
                        }
                    }
                    Ok(None) => {
                        debug!(execution_id = %execution_id, "client inbound stream closed");
                        // Drop the stdin sender to signal EOF to the gateway.
                        stdin_tx.take();
                        // Disable this select branch — without this guard, the
                        // closed stream would yield Ok(None) immediately on
                        // every poll and the loop would burn CPU and disk I/O.
                        inbound_closed = true;
                    }
                    Err(e) => {
                        debug!(execution_id = %execution_id, error = %e, "inbound stream error");
                        // Treat transport errors the same as a clean close so
                        // we don't spin on a permanently failed stream.
                        stdin_tx.take();
                        inbound_closed = true;
                    }
                }
            }
            // Timeout enforcement: abort execution with exit code 124
            _ = async {
                match timeout_deadline.as_mut().as_pin_mut() {
                    Some(deadline) => deadline.await,
                    None => std::future::pending().await,
                }
            } => {
                warn!(execution_id = %execution_id, timeout_ms, "execution timed out");
                exec_task.abort();
                // Drain any remaining events
                while let Ok(event) = event_rx.try_recv() {
                    send_execute_event(sender, event).await?;
                }
                send_execute_event(sender, ServerEvent::ExitStatus { code: 124 }).await?;
                break;
            }
            result = &mut exec_task => {
                let code = match result? {
                    Ok(c) => c,
                    Err(e) => {
                        send_execute_event(sender, ServerEvent::Error { message: e.to_string() }).await?;
                        return Ok(());
                    }
                };
                while let Ok(event) = event_rx.try_recv() {
                    send_execute_event(sender, event).await?;
                }
                info!(execution_id = %execution_id, code, "execution finished");
                send_execute_event(sender, ServerEvent::ExitStatus { code }).await?;
                break;
            }
        }
    }

    Ok(())
}

/// Process an interactive execute request using the gateway architecture.
async fn process_interactive_execute(
    request: ExecRequest,
    state: &DaemonState,
    inbound: &mut Streaming<proto_rpc::ExecuteRequest>,
    sender: &mpsc::Sender<Result<proto_rpc::ExecuteResponse, Status>>,
) -> Result<()> {
    let execution_id = Uuid::new_v4();
    let config = state.config.read().await.clone();
    let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
        .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.gateways);
    let routes = resolver.resolve(&request.target)?;
    let route = routes
        .first()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;

    let review_command = request.argv.join(" ");

    info!(
        execution_id = %execution_id,
        input = %request.target,
        end_target = %route.end_target,
        gateway = %route.gateway_name,
        interactive = true,
        "resolved target (interactive)"
    );

    // Run review
    let decision = match state
        .reviewer
        .review(&config.review, &route.end_target, &request.argv, &review_command)
        .await
    {
        Ok(result) => result,
        Err(error) => {
            warn!(
                execution_id = %execution_id,
                error = %format!("{error:#}"),
                "review failed"
            );
            let action = config.review.failure_action;
            let risk_level = crate::config::RiskLevel::Dangerous;
            send_execute_event(
                sender,
                ServerEvent::ReviewResult {
                    execution_id,
                    risk_level,
                    action,
                    reason: format!("review failed: {error:#}"),
                    matched_whitelist_reason: None,
                },
            )
            .await?;
            match action {
                ReviewAction::Allow | ReviewAction::Warn => None,
                ReviewAction::Confirm => {
                    wait_for_confirmation(execution_id, inbound, sender, "review service failed")
                        .await?;
                    None
                }
                ReviewAction::Deny => {
                    send_execute_event(
                        sender,
                        ServerEvent::Error {
                            message: format!("review failed and policy is deny: {error:#}"),
                        },
                    )
                    .await?;
                    return Ok(());
                }
            }
        }
    };

    if let Some(decision) = decision {
        info!(
            execution_id = %execution_id,
            risk_level = %decision.risk_level,
            action = %decision.action,
            matched_whitelist_reason = decision.matched_whitelist_reason.as_deref().unwrap_or(""),
            "review completed (interactive)"
        );
        send_execute_event(
            sender,
            ServerEvent::ReviewResult {
                execution_id,
                risk_level: decision.risk_level,
                action: decision.action,
                reason: decision.reason.clone(),
                matched_whitelist_reason: decision.matched_whitelist_reason.clone(),
            },
        )
        .await?;
        match decision.action {
            ReviewAction::Allow | ReviewAction::Warn => {}
            ReviewAction::Confirm => {
                debug!(execution_id = %execution_id, "waiting for confirmation (interactive)");
                wait_for_confirmation(execution_id, inbound, sender, &decision.reason).await?;
            }
            ReviewAction::Deny => {
                warn!(execution_id = %execution_id, "execution denied by review (interactive)");
                send_execute_event(
                    sender,
                    ServerEvent::Error {
                        message: format!("command denied: {}", decision.reason),
                    },
                )
                .await?;
                return Ok(());
            }
        }
    }

    // Open interactive session via gateway
    let gateway = state
        .find_gateway(&route.gateway_name)
        .ok_or_else(|| anyhow!("gateway '{}' not found", route.gateway_name))?;

    let (event_tx, mut _event_rx) = mpsc::unbounded_channel();
    let interactive_request = gateway::InteractiveRequest {
        argv: request.argv.clone(),
        cols: request.term_cols,
        rows: request.term_rows,
        sender: event_tx,
        shell: request.shell.clone(),
    };

    let mut handle = gateway
        .exec_interactive(&route.end_target, &interactive_request)
        .await
        .map_err(|e| anyhow!("{}", e))?;

    info!(execution_id = %execution_id, "interactive session started");

    // Bidirectional forwarding loop
    loop {
        tokio::select! {
            // Remote stdout → client
            data = handle.stdout_rx.recv() => {
                match data {
                    Some(bytes) => {
                        send_execute_event(sender, ServerEvent::Stdout { data: bytes }).await?;
                    }
                    None => {
                        debug!(execution_id = %execution_id, "stdout channel closed");
                        break;
                    }
                }
            }
            // Client messages → remote
            msg = inbound.message() => {
                match msg {
                    Ok(Some(message)) => {
                        match message.request {
                            Some(proto_rpc::execute_request::Request::StdinData(stdin)) => {
                                if handle.stdin_tx.send(stdin.data).await.is_err() {
                                    debug!(execution_id = %execution_id, "stdin_tx closed");
                                    break;
                                }
                            }
                            Some(proto_rpc::execute_request::Request::WindowResize(resize)) => {
                                if handle.resize_tx.send((resize.cols, resize.rows)).await.is_err() {
                                    debug!(execution_id = %execution_id, "resize_tx closed");
                                }
                            }
                            _ => {
                                // Ignore other message types
                            }
                        }
                    }
                    Ok(None) => {
                        debug!(execution_id = %execution_id, "client disconnected (interactive)");
                        break;
                    }
                    Err(e) => {
                        warn!(execution_id = %execution_id, error = %e, "inbound stream error (interactive)");
                        break;
                    }
                }
            }
            // Process exit
            exit_result = &mut handle.exit_rx => {
                let code = exit_result.unwrap_or(0);
                info!(execution_id = %execution_id, code, "interactive session exited");
                // Drain any remaining stdout
                while let Ok(bytes) = handle.stdout_rx.try_recv() {
                    send_execute_event(sender, ServerEvent::Stdout { data: bytes }).await?;
                }
                send_execute_event(sender, ServerEvent::ExitStatus { code }).await?;
                break;
            }
        }
    }

    Ok(())
}

fn remote_copy_temp_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("xho_{}_{}", prefix, Uuid::new_v4()))
}

async fn receive_copy_upload_to_temp(
    inbound: &mut Streaming<proto_rpc::CopyRequest>,
    temp_path: &Path,
) -> Result<()> {
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut file = fs::File::create(temp_path).await?;

    loop {
        let Some(message) = inbound.message().await? else {
            bail!("copy upload stream closed before EOF");
        };
        match message.request {
            Some(proto_rpc::copy_request::Request::DataChunk(chunk)) => {
                if !chunk.data.is_empty() {
                    use tokio::io::AsyncWriteExt as _;
                    file.write_all(&chunk.data).await?;
                }
                if chunk.eof {
                    use tokio::io::AsyncWriteExt as _;
                    file.flush().await?;
                    break;
                }
            }
            Some(proto_rpc::copy_request::Request::AuthInput(_)) => {}
            _ => {}
        }
    }

    Ok(())
}

async fn send_copy_download_from_temp(
    sender: &mpsc::Sender<Result<proto_rpc::CopyResponse, Status>>,
    temp_path: &Path,
) -> Result<()> {
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut file = fs::File::open(temp_path).await?;
    let mut buf = vec![0u8; CHUNK_SIZE];

    loop {
        let n = {
            use tokio::io::AsyncReadExt as _;
            file.read(&mut buf).await?
        };
        if n == 0 {
            break;
        }
        sender
            .send(Ok(proto_rpc::CopyResponse {
                event: Some(proto_rpc::copy_response::Event::DataChunk(
                    proto_rpc::CopyDataChunk {
                        data: buf[..n].to_vec(),
                        eof: false,
                    },
                )),
            }))
            .await
            .map_err(|_| anyhow!("copy client stream closed"))?;
    }

    sender
        .send(Ok(proto_rpc::CopyResponse {
            event: Some(proto_rpc::copy_response::Event::DataChunk(
                proto_rpc::CopyDataChunk {
                    data: Vec::new(),
                    eof: true,
                },
            )),
        }))
        .await
        .map_err(|_| anyhow!("copy client stream closed"))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

async fn wait_for_confirmation(
    execution_id: Uuid,
    inbound: &mut Streaming<proto_rpc::ExecuteRequest>,
    sender: &mpsc::Sender<Result<proto_rpc::ExecuteResponse, Status>>,
    reason: &str,
) -> Result<()> {
    send_execute_event(
        sender,
        ServerEvent::ConfirmRequired {
            execution_id,
            reason: reason.to_string(),
        },
    )
    .await?;

    let Some(message) = inbound.message().await? else {
        bail!("client disconnected before confirmation");
    };

    match message.request {
        Some(proto_rpc::execute_request::Request::Confirm(confirm)) => {
            let response_id = protocol::parse_execution_id(&confirm.execution_id)?;
            if response_id == execution_id && confirm.allow {
                Ok(())
            } else {
                bail!("execution not confirmed");
            }
        }
        _ => bail!("unexpected request while awaiting confirmation"),
    }
}

async fn send_execute_event(
    sender: &mpsc::Sender<Result<proto_rpc::ExecuteResponse, Status>>,
    event: ServerEvent,
) -> Result<()> {
    sender
        .send(Ok(protocol::server_event_to_rpc(event)))
        .await
        .map_err(|_| anyhow!("client receive stream closed"))?;
    Ok(())
}

async fn ensure_socket_parent(socket_path: &Path) -> Result<()> {
    let parent = socket_path
        .parent()
        .ok_or_else(|| anyhow!("invalid socket path {}", socket_path.display()))?;
    fs::create_dir_all(parent).await?;
    Ok(())
}

async fn shutdown_daemon(state: &DaemonState) -> Result<()> {
    if !state.origin.cli_controllable() {
        bail!("daemon is externally managed and cannot be stopped/restarted by CLI");
    }
    let _ = state.shutdown_tx.send(()).await;
    Ok(())
}

fn receiver_map_incoming(
    receiver: mpsc::Receiver<IncomingConn>,
) -> mpsc::Receiver<Result<IncomingConn, io::Error>> {
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        let mut receiver = receiver;
        while let Some(conn) = receiver.recv().await {
            if tx.send(Ok(conn)).await.is_err() {
                break;
            }
        }
    });
    rx
}

async fn ensure_remote_parent(config: &crate::config::RemoteServerConfig) -> Result<()> {
    let host_parent = Path::new(&config.host_key_path)
        .parent()
        .ok_or_else(|| anyhow!("invalid host key path {}", config.host_key_path))?;
    fs::create_dir_all(host_parent).await?;
    let auth_parent = Path::new(&config.authorized_keys_path)
        .parent()
        .ok_or_else(|| anyhow!("invalid authorized_keys path {}", config.authorized_keys_path))?;
    fs::create_dir_all(auth_parent).await?;
    Ok(())
}

async fn ensure_remote_host_key(config: &crate::config::RemoteServerConfig) -> Result<()> {
    let path = Path::new(&config.host_key_path);
    if path.exists() {
        return Ok(());
    }
    let mut rng = rand_core::UnwrapErr(getrandom::SysRng);
    let mut key = ssh_key::PrivateKey::random(&mut rng, ssh_key::Algorithm::Ed25519)
        .context("failed to generate Ed25519 host key")?;
    key.set_comment("xhod host key");
    key.write_openssh_file(path, LineEnding::LF)
        .with_context(|| format!("failed to write host key {}", path.display()))?;
    #[cfg(unix)]
    {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn load_host_keys(path: &Path) -> Result<Vec<ssh_key::PrivateKey>> {
    Ok(vec![
        ssh_key::PrivateKey::read_openssh_file(path)
            .with_context(|| format!("failed to read host key {}", path.display()))?,
    ])
}

fn is_authorized_key(path: &Path, candidate: &ssh_key::PublicKey) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    for (idx, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let first = line
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow!("invalid authorized_keys line {}", idx + 1))?;
        if first.contains('=') || first.contains(',') {
            bail!(
                "authorized_keys options are not supported in {} line {}",
                path.display(),
                idx + 1
            );
        }
        let parsed = ssh_key::PublicKey::from_openssh(line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), idx + 1))?;
        if parsed.key_data() == candidate.key_data() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Atomically writes the current in-memory config to disk using a temp file + rename.
async fn atomic_write_config(state: &DaemonState) -> Result<()> {
    let config = state.config.read().await.clone();
    let toml_str = toml::to_string_pretty(&config)
        .context("failed to serialize config to TOML")?;

    let config_path = &state.config_path;
    let parent = config_path
        .parent()
        .ok_or_else(|| anyhow!("config path has no parent directory"))?;

    // Write to a temp file in the same directory (same filesystem for atomic rename)
    let tmp_path = parent.join(format!(".config.toml.tmp.{}", std::process::id()));
    fs::write(&tmp_path, toml_str.as_bytes())
        .await
        .with_context(|| format!("failed to write temp config {}", tmp_path.display()))?;

    // Atomic rename
    fs::rename(&tmp_path, config_path)
        .await
        .with_context(|| {
            format!(
                "failed to rename {} to {}",
                tmp_path.display(),
                config_path.display()
            )
        })?;

    info!(config_path = %config_path.display(), "config written atomically");
    Ok(())
}

// ---------------------------------------------------------------------------
// Test support
// ---------------------------------------------------------------------------

/// Test support: exposes the ability to create an `XhoRpcServer` service
/// backed by a given `AppConfig` and config path, suitable for serving over
/// an in-process transport (e.g. `tokio::io::duplex`).
pub mod test_support {
    use super::*;

    /// Creates a tonic `XhoRpcServer` service instance backed by the given
    /// config. The returned service can be added to a `tonic::transport::Server`
    /// and served over any async I/O transport.
    pub fn make_test_rpc_service(
        config: AppConfig,
        config_path: PathBuf,
    ) -> proto_rpc::xho_rpc_server::XhoRpcServer<impl proto_rpc::xho_rpc_server::XhoRpc> {
        let config_clone = config.clone();
        let config = Arc::new(RwLock::new(config_clone.clone()));
        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        // Build gateways from config for test.
        let auth_prompter: Arc<AuthPrompter> = Arc::new(|_req| {
            Box::pin(async { Ok(String::new()) })
        });
        let gateways = gateway::build_gateways(
            config.clone(),
            &config_clone.ssh.server_config_path,
            &config_clone.gateways,
            auth_prompter,
        );

        let state = DaemonState {
            config_path,
            config,
            gateways,
            reviewer: CommandReviewer::new().expect("failed to create reviewer"),
            shutdown_tx,
            origin: DaemonOrigin::External,
            cli_start_options: CliStartOptions::default(),
        };
        proto_rpc::xho_rpc_server::XhoRpcServer::new(XhoRpcService { state })
    }
}
