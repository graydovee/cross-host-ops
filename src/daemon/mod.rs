#[allow(dead_code)]
pub mod authorized_keys;
#[allow(dead_code)]
pub mod connection_manager;
#[allow(dead_code)]
pub mod gateway;
#[allow(dead_code)]
pub mod jumpserver_engine;
#[allow(dead_code)]
pub mod proxy_server;
#[allow(dead_code)]
pub mod resolver;
#[allow(dead_code)]
pub mod reverse_client;
#[allow(dead_code)]
pub mod reverse_proxy;
#[allow(dead_code)]
pub mod review;
#[allow(dead_code)]
pub mod rpc;
#[allow(dead_code)]
pub mod session;
#[allow(dead_code)]
pub mod shell;
#[allow(dead_code)]
pub mod ssh_server;
#[allow(dead_code)]
pub mod token_store;

use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use russh::keys::ssh_key::{self, LineEnding};
use russh::server::{self, Server as _};
use tokio::fs;
use tokio::net::{TcpListener, UnixListener};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{RwLock, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use self::ssh_server::{IncomingConn, RemoteSshServer, load_host_keys};
use crate::config::{
    AppConfig, GatewayConfig, ReviewAction, default_config_path, load_server_config,
    validate_gateways,
};
use crate::logging::{init_logging, reopen_log_output};
use crate::protocol::{self, ExecRequest, ServerEvent, rpc as proto_rpc};
use crate::types::{CopyDirection, CopyFrame, CopySpec};

use self::gateway::Gateway;
use self::gateway::Route;
use self::gateway::auth::AuthPrompter;
use self::resolver::{ResolveResult, Resolver};
use self::review::CommandReviewer;

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
    /// Short-lived tokens issued by `xho token gen`, accepted by `auth_password`.
    pub token_store: token_store::TokenStore,
    /// Serializes authorized_keys appends from concurrent bootstrap RPCs.
    pub authorized_keys_lock: Arc<tokio::sync::Mutex<()>>,
    /// Dynamic gateways from reverse proxy connections.
    pub reverse_proxy_registry: Arc<reverse_proxy::ReverseProxyRegistry>,
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
        self.gateways
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, gw)| gw)
    }

    /// Find a gateway by name, checking both static config gateways and
    /// dynamic reverse proxy gateways.
    pub async fn find_gateway_any(&self, name: &str) -> Option<Arc<dyn Gateway>> {
        if let Some((_, gw)) = self.gateways.iter().find(|(n, _)| n == name) {
            return Some(gw.clone());
        }
        self.reverse_proxy_registry.get(name).await
    }

    /// Collect all gateway names: static + dynamic reverse proxy.
    pub async fn all_gateway_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.gateways.iter().map(|(n, _)| n.clone()).collect();
        names.extend(self.reverse_proxy_registry.list_names().await);
        names
    }
}

async fn resolve_target_with_merged_view(
    state: &DaemonState,
    target: &str,
) -> Result<ResolveResult> {
    // Fast path: dynamic gateway names and _self don't need list_servers.
    let dynamic_names = state.reverse_proxy_registry.list_names().await;
    let all_static_names: Vec<String> = state.gateways.iter().map(|(n, _)| n.clone()).collect();
    let is_gateway_name =
        all_static_names.iter().any(|n| n == target) || dynamic_names.iter().any(|n| n == target);

    if is_gateway_name && target != "local" {
        return Ok(ResolveResult {
            routes: vec![Route {
                gateway_name: target.to_string(),
                end_target: "_self".to_string(),
            }],
            warning: None,
        });
    }

    let config = state.config.read().await.clone();
    let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
        .unwrap_or_default();

    // Fast path for explicitly qualified targets (gateway:server):
    // skip the expensive list_servers aggregation that would recurse
    // through reverse proxy connections.
    if target.contains(':') && !target.starts_with('[') {
        let resolver = Resolver::new(&config, &server_config, &config.gateways)
            .with_dynamic_gateways(&dynamic_names);
        return resolver.resolve_with_warning(target);
    }

    // Full path with merged view for bare alias disambiguation.
    let (tagged_entries, source_status) = rpc::process_list_servers(state, false).await;
    let merged_rows = tagged_entries
        .into_iter()
        .map(|(server, source)| protocol::ServerListRow { source, server })
        .collect::<Vec<_>>();
    let dynamic_names = state.reverse_proxy_registry.list_names().await;
    let resolver = Resolver::with_merged_view(
        &config,
        &server_config,
        &config.gateways,
        &merged_rows,
        &source_status,
    )
    .with_dynamic_gateways(&dynamic_names);
    resolver.resolve_with_warning(target)
}

// ---------------------------------------------------------------------------
// RPC service
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct XhoRpcService {
    state: DaemonState,
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
    let auth_prompter: Arc<AuthPrompter> = Arc::new(|_req| Box::pin(async { Ok(String::new()) }));
    let mut gateways = gateway::build_gateways(
        config.clone(),
        &loaded.ssh.server_config_path,
        &loaded.gateways,
        auth_prompter,
    );

    // Register LocalhostGateway (_self) when host access is reachable: via the
    // reverse-proxy allow_host_access flag, or whenever the transparent proxy
    // is enabled (so `ssh _self@<xhod>` resolves).
    if (loaded.reverse_proxy.enable && loaded.reverse_proxy.allow_host_access)
        || loaded.server.proxy.enable
    {
        gateways.push((
            gateway::localhost::SELF_GATEWAY_NAME.to_string(),
            Arc::new(gateway::localhost::LocalhostGateway::new(
                loaded.reverse_proxy.shell.clone(),
                loaded.reverse_proxy.user.clone(),
                loaded.server.proxy.sftp_server_path.clone(),
            )),
        ));
        info!("_self (localhost) gateway registered");
    }

    let state = DaemonState {
        config_path,
        config: config.clone(),
        gateways,
        reviewer: CommandReviewer::new()?,
        shutdown_tx,
        origin,
        cli_start_options,
        token_store: token_store::TokenStore::new(),
        authorized_keys_lock: Arc::new(tokio::sync::Mutex::new(())),
        reverse_proxy_registry: Arc::new(reverse_proxy::ReverseProxyRegistry::new()),
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
                            warn!("failed to hand off local socket connection");
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

    // Transparent SSH proxy listener (human-facing `ssh node@xhod`, default 2222).
    if state.config.read().await.server.proxy.enable {
        let proxy_config = state.config.read().await.server.proxy.clone();
        match TcpListener::bind(&proxy_config.listen_addr).await {
            Ok(listener) => match load_host_keys(Path::new(&proxy_config.host_key_path)) {
                Ok(host_keys) => {
                    let mut server = proxy_server::ProxySshServer {
                        state: state.clone(),
                        authorized_keys_path: proxy_config.authorized_keys_path.clone(),
                    };
                    let config = Arc::new(server::Config {
                        auth_rejection_time: Duration::from_secs(1),
                        auth_rejection_time_initial: Some(Duration::from_secs(0)),
                        keys: host_keys,
                        inactivity_timeout: Some(Duration::from_secs(600)),
                        ..Default::default()
                    });
                    info!(
                        listen_addr = %proxy_config.listen_addr,
                        "listening on transparent proxy SSH"
                    );
                    tokio::spawn(async move {
                        if let Err(error) = server.run_on_socket(config, &listener).await {
                            error!(error = %error, "proxy SSH listener stopped");
                        }
                    });
                }
                Err(error) => warn!(
                    error = %error,
                    "proxy: failed to load host key; proxy listener disabled"
                ),
            },
            Err(error) => warn!(
                error = %error,
                addr = %proxy_config.listen_addr,
                "proxy: failed to bind; proxy listener disabled"
            ),
        }
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

    // Spawn reverse proxy client if enabled.
    // The shutdown sender is held in _rp_shutdown until the tonic server
    // future completes (end of function), then drops to signal shutdown.
    let _rp_shutdown = if loaded.reverse_proxy.enable {
        let rp_config = loaded.reverse_proxy.clone();
        let rp_state = state.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            reverse_client::run_reverse_proxy_client(rp_config, rp_state, rx).await;
        });
        Some(tx)
    } else {
        None
    };

    // Spawn SIGHUP handler for config reload + log reopen.
    let sighup_state = state.clone();
    tokio::spawn(async move {
        let Ok(mut sighup) = signal(SignalKind::hangup()) else {
            return;
        };
        while sighup.recv().await.is_some() {
            match reopen_log_output() {
                Ok(()) => info!("reopened log output after SIGHUP"),
                Err(error) => {
                    warn!(error = %format!("{error:#}"), "failed to reopen log output after SIGHUP")
                }
            }
            sighup_state.reload_config().await;
        }
    });

    let incoming = ReceiverStream::new(receiver_map_incoming(
        incoming_rx,
        state.reverse_proxy_registry.clone(),
    ));
    Server::builder()
        .add_service(proto_rpc::xho_rpc_server::XhoRpcServer::new(
            XhoRpcService {
                state: state.clone(),
            },
        ))
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
    type OpenSessionStream = ReceiverStream<Result<proto_rpc::SessionResponse, Status>>;

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
                    tty: start.tty,
                    tty_intent: proto_rpc::FlagIntent::try_from(start.tty_intent)
                        .unwrap_or(proto_rpc::FlagIntent::Default)
                        .into(),
                    stdin: start.stdin,
                    stdin_intent: proto_rpc::FlagIntent::try_from(start.stdin_intent)
                        .unwrap_or(proto_rpc::FlagIntent::Default)
                        .into(),
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

                let (target_input, mut spec, timeout_ms): (String, CopySpec, u64) = protocol::copy_spec_from_rpc(start)?;
                let resolved = resolve_target_with_merged_view(&state, &target_input).await?;
                let route = resolved.routes
                    .first()
                    .ok_or_else(|| anyhow!("no resolved target candidates"))?;
                if let Some(warning) = resolved.warning {
                    sender
                        .send(Ok(protocol::copy_info_response(warning)))
                        .await
                        .map_err(|_| anyhow!("copy client stream closed"))?;
                }
                info!(
                    target = %route.end_target,
                    gateway = %route.gateway_name,
                    direction = ?spec.direction,
                    remote_path = %spec.remote_path,
                    recursive = spec.recursive,
                    source_name = %spec.source_name,
                    timeout_ms,
                    "copy request"
                );

                let mut download_relay_task: Option<tokio::task::JoinHandle<()>> = None;
                match spec.direction {
                    CopyDirection::Upload => {
                        let (upload_tx, upload_rx) = mpsc::channel::<CopyFrame>(16);
                        spec.upload_rx = Some(upload_rx);

                        tokio::spawn(async move {
                            while let Ok(Some(msg)) = inbound.message().await {
                                match msg.request {
                                    Some(proto_rpc::copy_request::Request::Frame(frame)) => {
                                        let Ok(frame) = protocol::copy_frame_from_rpc(frame) else {
                                            break;
                                        };
                                        let eof = matches!(frame, CopyFrame::EndOfStream);
                                        if upload_tx.send(frame).await.is_err() {
                                            break;
                                        }
                                        if eof {
                                            break;
                                        }
                                    }
                                    Some(proto_rpc::copy_request::Request::AuthInput(_)) => {}
                                    Some(proto_rpc::copy_request::Request::Start(_)) | None => {}
                                }
                            }
                        });
                    }
                    CopyDirection::Download => {
                        let (download_tx, mut download_rx) = mpsc::channel::<CopyFrame>(16);
                        spec.download_tx = Some(download_tx);

                        let sender_clone = sender.clone();
                        let relay_task = tokio::spawn(async move {
                            while let Some(frame) = download_rx.recv().await {
                                let eof = matches!(frame, CopyFrame::EndOfStream);
                                let response = protocol::copy_frame_response(frame);
                                if sender_clone.send(Ok(response)).await.is_err() {
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

                // If timeout is specified, create a deadline future.
                let copy_timeout = if timeout_ms > 0 {
                    Some(tokio::time::sleep(Duration::from_millis(timeout_ms)))
                } else {
                    None
                };
                tokio::pin!(copy_timeout);

                let copy_task: tokio::task::JoinHandle<Result<(), anyhow::Error>> = {
                    let state = state.clone();
                    let route = route.clone();
                    tokio::spawn(async move { session::copy_via_session(&state, &route, spec).await })
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
                            sender
                                .send(Ok(protocol::copy_error_response("copy timed out (exit code 124)")))
                                .await
                                .map_err(|_| anyhow!("copy client stream closed"))?;
                            break;
                        }
                        result = &mut copy_task => {
                            if let Err(e) = result? {
                                sender
                                    .send(Ok(protocol::copy_error_response(e.to_string())))
                                    .await
                                    .map_err(|_| anyhow!("copy client stream closed"))?;
                                break;
                            }
                            if let Some(relay_task) = download_relay_task.take() {
                                let _ = relay_task.await;
                            }
                            sender
                                .send(Ok(protocol::copy_complete_response(String::new())))
                                .await
                                .map_err(|_| anyhow!("copy client stream closed"))?;
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
                    GatewayConfig::Xhod(c) => {
                        (c.name.clone(), "xhod".to_string(), c.address.clone())
                    }
                    GatewayConfig::Jumpserver(c) => (
                        c.name.clone(),
                        "jumpserver".to_string(),
                        format!("{}:{}", c.host, c.port),
                    ),
                    GatewayConfig::Direct(c) => (
                        c.name.clone(),
                        "direct".to_string(),
                        format!("{}:{}", c.host, c.port),
                    ),
                };
                proto_rpc::GatewayStatus {
                    name,
                    kind,
                    address,
                    sub_status: None,
                }
            })
            .collect();
        let rp_nodes = self
            .state
            .reverse_proxy_registry
            .list_nodes()
            .await
            .into_iter()
            .map(|n| proto_rpc::ReverseProxyNodeStatus {
                name: n.name,
                peer_addr: n.peer_addr,
                fingerprint: n.fingerprint,
                connected_at: n.connected_at,
            })
            .collect();

        // Aggregate per-gateway connection pool snapshots (real reuse status).
        let mut pools: Vec<proto_rpc::PoolStatus> = Vec::new();
        for (_name, gw) in &self.state.gateways {
            for snap in gw.pool_status().await {
                pools.push(proto_rpc::PoolStatus {
                    key: snap.key,
                    total: (snap.active + snap.idle) as u64,
                    busy: snap.active as u64,
                    idle: snap.idle as u64,
                    queued: 0,
                });
            }
        }

        let response = proto_rpc::StatusResponse {
            daemon_running: true,
            local_socket_path: socket_path,
            active_executions: 0,
            pools,
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
            reverse_proxy_server_enabled: config.server.remote.reverse_proxy_enable,
            reverse_proxy_nodes: rp_nodes,
            reverse_proxy_client_enabled: config.reverse_proxy.enable,
            reverse_proxy_client_target: if config.reverse_proxy.enable {
                config.reverse_proxy.server_address.clone()
            } else {
                String::new()
            },
            reverse_proxy_client_status: if config.reverse_proxy.enable {
                "active".to_string()
            } else {
                "disabled".to_string()
            },
            proxy_listening: config.server.proxy.enable,
            proxy_addr: if config.server.proxy.enable {
                config.server.proxy.listen_addr.clone()
            } else {
                String::new()
            },
        };
        Ok(Response::new(response))
    }

    async fn list_servers(
        &self,
        request: Request<proto_rpc::ServerListRequest>,
    ) -> Result<Response<proto_rpc::ServerListResponse>, Status> {
        let config = self.state.config.read().await.clone();
        let path = PathBuf::from(&config.ssh.server_config_path);

        // When called from another xhod (forward gateway or reverse proxy),
        // skip reverse proxy gateways to prevent recursive list_servers loops.
        let no_recurse = request.metadata().get("xho-no-recurse").is_some();
        let (tagged_entries, source_status) =
            rpc::process_list_servers(&self.state, no_recurse).await;

        // Convert entries to RPC format (flat list without source).
        // Mark reverse proxy entries with auth_kind = "reverse_proxy".
        let rp_names = self.state.reverse_proxy_registry.list_names().await;
        let servers: Vec<proto_rpc::ServerEntry> = tagged_entries
            .iter()
            .map(|(entry, _source)| {
                let mut rpc_entry = protocol::server_entry_to_rpc(entry.clone());
                if rp_names.contains(&entry.alias) {
                    rpc_entry.auth_kind = "reverse_proxy".to_string();
                }
                rpc_entry
            })
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

    async fn open_session(
        &self,
        request: Request<Streaming<proto_rpc::SessionRequest>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let mut inbound = request.into_inner();
        let state = self.state.clone();
        let (sender, receiver) = mpsc::channel(64);

        tokio::spawn(async move {
            let result = async {
                // First message must be SessionOpen{target}.
                let first = inbound
                    .message()
                    .await?
                    .ok_or_else(|| anyhow!("open_session: missing open request"))?;
                let target = match first.msg {
                    Some(proto_rpc::session_request::Msg::Open(open)) => open.target,
                    _ => bail!("open_session: first message must be SessionOpen"),
                };

                let resolved = resolve_target_with_merged_view(&state, &target).await?;
                let route = resolved
                    .routes
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("no route for target '{target}'"))?;
                let mut sess = session::open_target_session(&state, &route).await?;

                // Acknowledge the open. Exec/shell/subsystem arrive as later
                // requests and drive the session start.
                sender
                    .send(Ok(proto_rpc::SessionResponse {
                        msg: Some(proto_rpc::session_response::Msg::Started(
                            proto_rpc::SessionStarted {},
                        )),
                    }))
                    .await
                    .map_err(|_| anyhow!("open_session: client stream closed"))?;

                loop {
                    tokio::select! {
                        req = inbound.message() => match req {
                            Ok(Some(r)) => match r.msg {
                                Some(proto_rpc::session_request::Msg::Pty(p)) => { let _ = sess.request_pty(&p.term, p.cols, p.rows, &[]).await; }
                                Some(proto_rpc::session_request::Msg::Env(e)) => { let _ = sess.set_env(&e.key, &e.value).await; }
                                Some(proto_rpc::session_request::Msg::Exec(e)) => { if let Err(er) = sess.exec(&e.command).await {
                                    let _ = sender.send(Ok(proto_rpc::SessionResponse { msg: Some(proto_rpc::session_response::Msg::Error(proto_rpc::SessionError { message: er.to_string() })) })).await;
                                }}
                                Some(proto_rpc::session_request::Msg::Shell(_)) => { if let Err(er) = sess.shell().await {
                                    let _ = sender.send(Ok(proto_rpc::SessionResponse { msg: Some(proto_rpc::session_response::Msg::Error(proto_rpc::SessionError { message: er.to_string() })) })).await;
                                }}
                                Some(proto_rpc::session_request::Msg::Subsystem(s)) => { if let Err(er) = sess.subsystem(&s.name).await {
                                    let _ = sender.send(Ok(proto_rpc::SessionResponse { msg: Some(proto_rpc::session_response::Msg::Error(proto_rpc::SessionError { message: er.to_string() })) })).await;
                                }}
                                Some(proto_rpc::session_request::Msg::Resize(r)) => { let _ = sess.window_change(r.cols, r.rows).await; }
                                Some(proto_rpc::session_request::Msg::Signal(s)) => { let _ = sess.signal(&s.signal).await; }
                                Some(proto_rpc::session_request::Msg::Data(d)) => { let _ = sess.write_stdin(&d.data).await; }
                                Some(proto_rpc::session_request::Msg::Eof(_)) => { let _ = sess.eof().await; }
                                Some(proto_rpc::session_request::Msg::Open(_)) | None => break,
                            },
                            Ok(None) => break,
                            Err(_) => break,
                        },
                        ev = sess.next_event() => match ev {
                            Some(session::SessionEvent::Stdout(d)) => {
                                if send_session_msg(&sender, proto_rpc::session_response::Msg::Data(proto_rpc::SessionData { data: d })).await { break; }
                            }
                            Some(session::SessionEvent::Stderr(d)) => {
                                if send_session_msg(&sender, proto_rpc::session_response::Msg::Stderr(proto_rpc::SessionExtendedData { data: d })).await { break; }
                            }
                            Some(session::SessionEvent::ExitStatus(c)) => {
                                let _ = send_session_msg(&sender, proto_rpc::session_response::Msg::ExitStatus(proto_rpc::SessionExitStatus { code: c })).await;
                                let _ = send_session_msg(&sender, proto_rpc::session_response::Msg::Eof(proto_rpc::SessionEofIndication {})).await;
                                break;
                            }
                            Some(session::SessionEvent::ExitSignal(s)) => {
                                let _ = send_session_msg(&sender, proto_rpc::session_response::Msg::ExitSignal(proto_rpc::SessionExitSignal { signal: s })).await;
                                break;
                            }
                            Some(session::SessionEvent::Eof) | None => {
                                let _ = send_session_msg(&sender, proto_rpc::session_response::Msg::Eof(proto_rpc::SessionEofIndication {})).await;
                                break;
                            }
                        },
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                error!(error = %format!("{error:#}"), "open_session stream failed");
                let _ = sender
                    .send(Ok(proto_rpc::SessionResponse {
                        msg: Some(proto_rpc::session_response::Msg::Error(
                            proto_rpc::SessionError {
                                message: error.to_string(),
                            },
                        )),
                    }))
                    .await;
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
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
                    other => Ok(Response::new(proto_rpc::UpdateConfigResponse {
                        success: false,
                        message: format!(
                            "add_gateway via RPC only supports kind 'xhod', got '{}'",
                            other
                        ),
                    })),
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
                    GatewayConfig::Xhod(c) => {
                        (c.name.clone(), "xhod".to_string(), c.address.clone())
                    }
                    GatewayConfig::Jumpserver(c) => (
                        c.name.clone(),
                        "jumpserver".to_string(),
                        format!("{}:{}", c.host, c.port),
                    ),
                    GatewayConfig::Direct(c) => (
                        c.name.clone(),
                        "direct".to_string(),
                        format!("{}:{}", c.host, c.port),
                    ),
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

    async fn token_gen(
        &self,
        request: Request<proto_rpc::TokenGenRequest>,
    ) -> Result<Response<proto_rpc::TokenGenResponse>, Status> {
        let req = request.into_inner();
        let ttl = if req.ttl_secs == 0 {
            Duration::from_secs(300)
        } else {
            Duration::from_secs(req.ttl_secs)
        };
        let label = req
            .label
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string());
        let token = self.state.token_store.generate(ttl, req.once, label).await;
        let expires_at = std::time::SystemTime::now() + ttl;
        let expires_at_str = token_store::format_rfc3339_utc(expires_at);
        info!(
            once = req.once,
            ttl_secs = req.ttl_secs,
            "issued bootstrap token"
        );
        Ok(Response::new(proto_rpc::TokenGenResponse {
            token,
            expires_at: expires_at_str,
            once: req.once,
        }))
    }

    async fn token_list(
        &self,
        _request: Request<proto_rpc::TokenListRequest>,
    ) -> Result<Response<proto_rpc::TokenListResponse>, Status> {
        let entries = self.state.token_store.list().await;
        let tokens = entries
            .into_iter()
            .map(|e| {
                let expires_at = e.expires_at_rfc3339();
                let created_at = e.created_at_rfc3339();
                proto_rpc::TokenInfo {
                    prefix: e.prefix,
                    expires_at,
                    once: e.once,
                    consumed: e.consumed,
                    created_at,
                    label: e.label,
                }
            })
            .collect();
        Ok(Response::new(proto_rpc::TokenListResponse { tokens }))
    }

    async fn token_invalidate(
        &self,
        request: Request<proto_rpc::TokenInvalidateRequest>,
    ) -> Result<Response<proto_rpc::TokenInvalidateResponse>, Status> {
        let req = request.into_inner();
        let invalidated = self
            .state
            .token_store
            .invalidate(&req.token_or_prefix)
            .await;
        if invalidated {
            info!(prefix = req.token_or_prefix, "invalidated bootstrap token");
        }
        Ok(Response::new(proto_rpc::TokenInvalidateResponse {
            invalidated,
        }))
    }

    async fn bootstrap_authorize(
        &self,
        request: Request<proto_rpc::BootstrapAuthorizeRequest>,
    ) -> Result<Response<proto_rpc::BootstrapAuthorizeResponse>, Status> {
        let req = request.into_inner();
        let key = russh::keys::ssh_key::PublicKey::from_openssh(req.public_key.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid public key: {e}")))?;
        let fingerprint = key
            .fingerprint(russh::keys::ssh_key::HashAlg::Sha256)
            .to_string();
        let path_str = {
            let config = self.state.config.read().await;
            config.server.remote.authorized_keys_path.clone()
        };
        let path = PathBuf::from(path_str);
        // Serialize concurrent bootstraps so two RPCs racing for the same key
        // can't both write (the dedup check inside is_authorized_key is
        // non-atomic with the append otherwise).
        let _guard = self.state.authorized_keys_lock.lock().await;
        let (appended, already_present) = authorized_keys::append_authorized_key(&path, &key)
            .await
            .map_err(|e| Status::internal(format!("failed to append authorized_keys: {e}")))?;
        info!(
            fingerprint = %fingerprint,
            appended, already_present,
            "bootstrap_authorize completed"
        );
        Ok(Response::new(proto_rpc::BootstrapAuthorizeResponse {
            appended,
            already_present,
            fingerprint,
        }))
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
        if !request.tty {
            send_execute_event(
                sender,
                ServerEvent::Error {
                    message: "interactive mode requires tty (--tty)".to_string(),
                },
            )
            .await?;
            return Ok(());
        }
        if request.term_cols == 0 || request.term_rows == 0 {
            send_execute_event(
                sender,
                ServerEvent::Error {
                    message: "interactive mode requires term_cols > 0 and term_rows > 0"
                        .to_string(),
                },
            )
            .await?;
            return Ok(());
        }
        return process_interactive_execute(request, state, inbound, sender).await;
    }

    let execution_id = Uuid::new_v4();
    let config = state.config.read().await.clone();
    let resolved = resolve_target_with_merged_view(state, &request.target).await?;
    let route = resolved
        .routes
        .first()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;
    if let Some(warning) = resolved.warning {
        send_execute_event(sender, ServerEvent::Info { message: warning }).await?;
    }

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
        .review(
            &config.review,
            &config.secret_resolver(None),
            &route.end_target,
            &request.argv,
            &review_command,
        )
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

    // Execute via the unified TargetSession abstraction (all gateway kinds,
    // including jumpserver via JumpserverSession).
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    // Create stdin forwarding channel when the client requests stdin.
    let (mut stdin_tx, stdin_rx) = if request.stdin {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(32);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let timeout_ms = request.timeout_ms;
    let stdin_enabled = request.stdin;

    let exec_task: tokio::task::JoinHandle<Result<i32, anyhow::Error>> = {
        let state = state.clone();
        let route = route.clone();
        let argv = request.argv.clone();
        let cli_shell = request.shell.clone();
        let no_shell = request.no_shell;
        let tty = request.tty;
        let cols = request.term_cols;
        let rows = request.term_rows;
        tokio::spawn(async move {
            let (sess, command) =
                session::open_exec_session(&state, &route, &argv, &cli_shell, no_shell).await?;
            session::drive_exec(sess, command, tty, cols, rows, event_tx, stdin_rx).await
        })
    };
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
                if let Err(error) = send_execute_event(sender, event).await {
                    debug!(
                        execution_id = %execution_id,
                        error = %format!("{error:#}"),
                        "client receive stream closed; aborting execution"
                    );
                    stdin_tx.take();
                    exec_task.abort();
                    let _ = (&mut exec_task).await;
                    return Ok(());
                }
            }
            _ = sender.closed() => {
                debug!(execution_id = %execution_id, "client receive stream closed; aborting execution");
                stdin_tx.take();
                exec_task.abort();
                let _ = (&mut exec_task).await;
                return Ok(());
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
                stdin_tx.take();
                exec_task.abort();
                let _ = (&mut exec_task).await;
                // Drain any remaining events
                while let Ok(event) = event_rx.try_recv() {
                    if send_execute_event(sender, event).await.is_err() {
                        return Ok(());
                    }
                }
                let _ = send_execute_event(sender, ServerEvent::ExitStatus { code: 124 }).await;
                break;
            }
            result = &mut exec_task => {
                let code = match result? {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = send_execute_event(sender, ServerEvent::Error { message: e.to_string() }).await;
                        return Ok(());
                    }
                };
                while let Ok(event) = event_rx.try_recv() {
                    if send_execute_event(sender, event).await.is_err() {
                        return Ok(());
                    }
                }
                info!(execution_id = %execution_id, code, "execution finished");
                let _ = send_execute_event(sender, ServerEvent::ExitStatus { code }).await;
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
    let resolved = resolve_target_with_merged_view(state, &request.target).await?;
    let route = resolved
        .routes
        .first()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;
    if let Some(warning) = resolved.warning {
        send_execute_event(sender, ServerEvent::Info { message: warning }).await?;
    }

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
        .review(
            &config.review,
            &config.secret_resolver(None),
            &route.end_target,
            &request.argv,
            &review_command,
        )
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

    // Open interactive session via the unified TargetSession abstraction.
    let mut handle: session::InteractiveHandle = {
        let (sess, command) = session::open_exec_session(
            state,
            route,
            &request.argv,
            &request.shell,
            request.no_shell,
        )
        .await?;
        let exec_command = if request.argv.is_empty() {
            None
        } else {
            Some(command)
        };
        session::drive_interactive(sess, exec_command, request.term_cols, request.term_rows).await?
    };

    info!(execution_id = %execution_id, "interactive session started");

    // Bidirectional forwarding loop
    loop {
        tokio::select! {
            // Remote stdout → client
            data = handle.stdout_rx.recv() => {
                match data {
                    Some(bytes) => {
                        if send_execute_event(sender, ServerEvent::Stdout { data: bytes }).await.is_err() {
                            debug!(execution_id = %execution_id, "client receive stream closed (interactive)");
                            abort_interactive_handles(&handle.abort_handles);
                            break;
                        }
                    }
                    None => {
                        // stdout closed; the exit code follows immediately (the
                        // passthrough task sends it right after closing stdout).
                        // Await it so the client always receives the terminal
                        // ExitStatus rather than a bare stream end — otherwise
                        // races between stdout-close and exit-delivery drop the
                        // exit code (e.g. `xho exec -it -- ls` would lose it).
                        debug!(execution_id = %execution_id, "stdout channel closed; awaiting exit code");
                        let code = (&mut handle.exit_rx).await.unwrap_or(0);
                        info!(execution_id = %execution_id, code, "interactive session exited (after stdout close)");
                        while let Ok(bytes) = handle.stdout_rx.try_recv() {
                            if send_execute_event(sender, ServerEvent::Stdout { data: bytes }).await.is_err() {
                                return Ok(());
                            }
                        }
                        let _ = send_execute_event(sender, ServerEvent::ExitStatus { code }).await;
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
                        abort_interactive_handles(&handle.abort_handles);
                        break;
                    }
                    Err(e) => {
                        warn!(execution_id = %execution_id, error = %e, "inbound stream error (interactive)");
                        abort_interactive_handles(&handle.abort_handles);
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
                    if send_execute_event(sender, ServerEvent::Stdout { data: bytes }).await.is_err() {
                        return Ok(());
                    }
                }
                let _ = send_execute_event(sender, ServerEvent::ExitStatus { code }).await;
                break;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn abort_interactive_handles(handles: &[tokio::task::AbortHandle]) {
    for handle in handles {
        handle.abort();
    }
}

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

/// Send a `SessionResponse` over an OpenSession stream. Returns `true` when the
/// receiver has been dropped (caller should stop driving the session).
async fn send_session_msg(
    sender: &mpsc::Sender<Result<proto_rpc::SessionResponse, Status>>,
    msg: proto_rpc::session_response::Msg,
) -> bool {
    sender
        .send(Ok(proto_rpc::SessionResponse { msg: Some(msg) }))
        .await
        .is_err()
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
    reverse_proxy_registry: Arc<reverse_proxy::ReverseProxyRegistry>,
) -> mpsc::Receiver<Result<IncomingConn, io::Error>> {
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        let mut receiver = receiver;
        while let Some(conn) = receiver.recv().await {
            match conn {
                IncomingConn::ReverseProxy(handshake) => {
                    let registry = reverse_proxy_registry.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            reverse_proxy::process_reverse_handshake(&registry, handshake).await
                        {
                            warn!(error = %format!("{e:#}"), "reverse proxy handshake failed");
                        }
                    });
                }
                other => {
                    if tx.send(Ok(other)).await.is_err() {
                        break;
                    }
                }
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
        .ok_or_else(|| {
            anyhow!(
                "invalid authorized_keys path {}",
                config.authorized_keys_path
            )
        })?;
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

/// Atomically writes the current in-memory config to disk using a temp file + rename.
async fn atomic_write_config(state: &DaemonState) -> Result<()> {
    let config = state.config.read().await.clone();
    let toml_str = toml::to_string_pretty(&config).context("failed to serialize config to TOML")?;

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
    fs::rename(&tmp_path, config_path).await.with_context(|| {
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
        let auth_prompter: Arc<AuthPrompter> =
            Arc::new(|_req| Box::pin(async { Ok(String::new()) }));
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
            token_store: token_store::TokenStore::new(),
            authorized_keys_lock: Arc::new(tokio::sync::Mutex::new(())),
            reverse_proxy_registry: Arc::new(reverse_proxy::ReverseProxyRegistry::new()),
        };
        proto_rpc::xho_rpc_server::XhoRpcServer::new(XhoRpcService { state })
    }
}
