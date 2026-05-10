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
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::sleep;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::transport::server::Connected;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{AppConfig, ReviewAction, default_config_path, list_server_entries};
use crate::connection::CopySpec;
use crate::connection::{AuthPrompter, AuthPromptRequest, build_remote_command, resolve_target};
use crate::logging::{init_logging, reopen_log_output};
use crate::pool::ConnectionPool;
use crate::protocol::{self, AuthPromptMessage, ExecRequest, ServerEvent, rpc};
use crate::remote::remote_subsystem_name;
use crate::review::CommandReviewer;

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

#[derive(Clone)]
struct DaemonState {
    config_path: PathBuf,
    config: Arc<RwLock<AppConfig>>,
    pool: ConnectionPool,
    reviewer: CommandReviewer,
    shutdown_tx: mpsc::Sender<()>,
    origin: DaemonOrigin,
    cli_start_options: CliStartOptions,
}

#[derive(Clone)]
struct RhopRpcService {
    state: DaemonState,
}

struct PendingAuthPrompt {
    message: AuthPromptMessage,
    response_tx: oneshot::Sender<Result<String>>,
}

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
    info!(config_path = %config_path.display(), "starting rhopd");

    let config = Arc::new(RwLock::new(loaded));
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let state = DaemonState {
        config_path,
        config: config.clone(),
        pool: ConnectionPool::new(config),
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

    let reaper_state = state.clone();
    tokio::spawn(async move {
        loop {
            let interval = reaper_state.config.read().await.server.reaper_interval;
            sleep(interval).await;
            reaper_state.pool.prune_idle().await;
            debug!("idle connection reaper tick");
        }
    });

    tokio::spawn(async move {
        let Ok(mut sighup) = signal(SignalKind::hangup()) else {
            return;
        };
        while sighup.recv().await.is_some() {
            match reopen_log_output() {
                Ok(()) => info!("reopened log output after SIGHUP"),
                Err(error) => warn!(error = %format!("{error:#}"), "failed to reopen log output after SIGHUP"),
            }
        }
    });

    let incoming = ReceiverStream::new(receiver_map_incoming(incoming_rx));
    Server::builder()
        .add_service(rpc::rhop_rpc_server::RhopRpcServer::new(RhopRpcService {
            state,
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
    key.set_comment("rhopd host key");
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
        if &parsed == candidate {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn process_execute(
    request: ExecRequest,
    state: &DaemonState,
    inbound: &mut Streaming<rpc::ExecuteRequest>,
    sender: &mpsc::Sender<Result<rpc::ExecuteResponse, Status>>,
) -> Result<()> {
    if request.argv.is_empty() {
        bail!("argv must not be empty");
    }

    let execution_id = Uuid::new_v4();
    let config = state.config.read().await.clone();
    let targets = resolve_target(&request.target, &config)?;
    let target = targets
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;
    let shell_command = build_remote_command(&request.argv);

    info!(
        execution_id = %execution_id,
        input = %target.input,
        ip = %target.ip,
        transport = %target.transport,
        "resolved target"
    );

    let decision = match state
        .reviewer
        .review(&config.review, &target.ip, &request.argv, &shell_command)
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

    let (tx, mut rx) = mpsc::unbounded_channel();
    let pool = state.pool.clone();
    let argv = request.argv.clone();
    let exec_targets = targets.clone();
    let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel();
    let auth_prompter = make_auth_prompter(prompt_tx, target.target_label.clone());
    let exec_task = tokio::spawn(async move {
        pool.execute(exec_targets, argv, tx, auth_prompter).await
    });
    tokio::pin!(exec_task);

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                send_execute_event(sender, event).await?;
            }
            Some(prompt) = prompt_rx.recv() => {
                send_execute_event(
                    sender,
                    ServerEvent::AuthPrompt {
                        prompt_id: prompt.message.prompt_id.clone(),
                        target_label: prompt.message.target_label,
                        kind: prompt.message.kind,
                        secret: prompt.message.secret,
                        message: prompt.message.message,
                    },
                ).await?;
                let reply = wait_for_auth_input_execute(inbound, &prompt.message.prompt_id).await;
                let _ = prompt.response_tx.send(reply);
            }
            result = &mut exec_task => {
                let code = result??;
                while let Ok(event) = rx.try_recv() {
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

fn make_auth_prompter(
    prompt_sender: mpsc::UnboundedSender<PendingAuthPrompt>,
    default_target_label: String,
) -> Arc<AuthPrompter> {
    Arc::new(move |request: AuthPromptRequest| {
        let prompt_sender = prompt_sender.clone();
        let default_target_label = default_target_label.clone();
        Box::pin(async move {
            let prompt_id = Uuid::new_v4().to_string();
            let target_label = if request.target_label.is_empty() {
                default_target_label
            } else {
                request.target_label
            };
            let (response_tx, response_rx) = oneshot::channel();
            prompt_sender
                .send(PendingAuthPrompt {
                    message: AuthPromptMessage {
                        prompt_id,
                        target_label,
                        kind: request.kind,
                        secret: request.secret,
                        message: request.message,
                    },
                    response_tx,
                })
                .map_err(|_| anyhow!("auth prompt channel closed"))?;
            response_rx
                .await
                .map_err(|_| anyhow!("auth prompt response channel closed"))?
        })
    })
}

async fn wait_for_confirmation(
    execution_id: Uuid,
    inbound: &mut Streaming<rpc::ExecuteRequest>,
    sender: &mpsc::Sender<Result<rpc::ExecuteResponse, Status>>,
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
        Some(rpc::execute_request::Request::Confirm(confirm)) => {
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
    sender: &mpsc::Sender<Result<rpc::ExecuteResponse, Status>>,
    event: ServerEvent,
) -> Result<()> {
    sender
        .send(Ok(protocol::server_event_to_rpc(event)))
        .await
        .map_err(|_| anyhow!("client receive stream closed"))?;
    Ok(())
}

async fn wait_for_auth_input_execute(
    inbound: &mut Streaming<rpc::ExecuteRequest>,
    prompt_id: &str,
) -> Result<String> {
    loop {
        let Some(message) = inbound.message().await? else {
            bail!("client disconnected before auth input");
        };
        match message.request {
            Some(rpc::execute_request::Request::AuthInput(input)) if input.prompt_id == prompt_id => {
                return Ok(input.value);
            }
            Some(rpc::execute_request::Request::Confirm(_)) => continue,
            _ => bail!("unexpected request while awaiting auth input"),
        }
    }
}

async fn wait_for_auth_input_copy(
    inbound: &mut Streaming<rpc::CopyRequest>,
    prompt_id: &str,
) -> Result<String> {
    loop {
        let Some(message) = inbound.message().await? else {
            bail!("client disconnected before auth input");
        };
        match message.request {
            Some(rpc::copy_request::Request::AuthInput(input)) if input.prompt_id == prompt_id => {
                return Ok(input.value);
            }
            _ => bail!("unexpected request while awaiting copy auth input"),
        }
    }
}

#[tonic::async_trait]
impl rpc::rhop_rpc_server::RhopRpc for RhopRpcService {
    type ExecuteStream = ReceiverStream<Result<rpc::ExecuteResponse, Status>>;
    type CopyStream = ReceiverStream<Result<rpc::CopyResponse, Status>>;

    async fn execute(
        &self,
        request: Request<Streaming<rpc::ExecuteRequest>>,
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
                let Some(rpc::execute_request::Request::Start(start)) = first.request else {
                    bail!("first execute stream message must be start");
                };
                let exec = ExecRequest {
                    target: start.target,
                    argv: start.argv,
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
        request: Request<Streaming<rpc::CopyRequest>>,
    ) -> Result<Response<Self::CopyStream>, Status> {
        let mut inbound = request.into_inner();
        let state = self.state.clone();
        let (sender, receiver) = mpsc::channel(16);

        tokio::spawn(async move {
            let result = async {
                let Some(first) = inbound.message().await? else {
                    bail!("client disconnected before copy start request");
                };
                let Some(rpc::copy_request::Request::Start(start)) = first.request else {
                    bail!("first copy stream message must be start");
                };
                let (target_input, spec): (String, CopySpec) = protocol::copy_spec_from_rpc(start)?;
                let config = state.config.read().await.clone();
                let targets = resolve_target(&target_input, &config)?;
                let target = targets
                    .first()
                    .cloned()
                    .ok_or_else(|| anyhow!("no resolved target candidates"))?;
                info!(
                    target = %target.key,
                    direction = ?spec.direction,
                    local_path = %spec.local_path,
                    remote_path = %spec.remote_path,
                    recursive = spec.recursive,
                    "copy request"
                );

                let pool = state.pool.clone();
                let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel();
                let auth_prompter = make_auth_prompter(prompt_tx, target.target_label.clone());
                let copy_task = tokio::spawn(async move { pool.copy(targets, spec, auth_prompter).await });
                tokio::pin!(copy_task);

                loop {
                    tokio::select! {
                        Some(prompt) = prompt_rx.recv() => {
                            sender
                                .send(Ok(protocol::copy_auth_prompt_response(prompt.message.clone())))
                                .await
                                .map_err(|_| anyhow!("copy client stream closed"))?;
                            let reply = wait_for_auth_input_copy(&mut inbound, &prompt.message.prompt_id).await;
                            let _ = prompt.response_tx.send(reply);
                        }
                        result = &mut copy_task => {
                            result??;
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
        _request: Request<rpc::StatusRequest>,
    ) -> Result<Response<rpc::StatusResponse>, Status> {
        info!("status request");
        let config = self.state.config.read().await.clone();
        let socket_path = config.server.local.socket_path.clone();
        let pools = self.state.pool.status();
        let active_executions = pools.iter().map(|entry| entry.busy).sum::<usize>() as u64;
        let response = rpc::StatusResponse {
            daemon_running: true,
            local_socket_path: socket_path,
            active_executions,
            pools: pools
                .into_iter()
                .map(protocol::pool_status_to_rpc)
                .collect(),
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
            local_enabled: config.server.local.enable,
            remote_enabled: config.server.remote.enable,
            remote_listen_addr: config.server.remote.listen_addr,
            remote_user: config.server.remote.user,
        };
        Ok(Response::new(response))
    }

    async fn list_config(
        &self,
        _request: Request<rpc::ConfigListRequest>,
    ) -> Result<Response<rpc::ConfigListResponse>, Status> {
        let config = self.state.config.read().await.clone();
        Ok(Response::new(rpc::ConfigListResponse {
            config_path: self.state.config_path.display().to_string(),
            server_config_path: config.ssh.server_config_path,
            ssh_config_path: config.ssh.ssh_config_path,
            local_socket_path: config.server.local.socket_path,
            fallback: config.ssh.fallback.iter().map(ToString::to_string).collect(),
            jumpserver_enabled: config.jumpserver.enabled,
            jumpserver_host: config.jumpserver.host,
            local_enabled: config.server.local.enable,
            remote_enabled: config.server.remote.enable,
            remote_listen_addr: config.server.remote.listen_addr,
            remote_user: config.server.remote.user,
            authorized_keys_path: config.server.remote.authorized_keys_path,
            host_key_path: config.server.remote.host_key_path,
        }))
    }

    async fn list_servers(
        &self,
        _request: Request<rpc::ServerListRequest>,
    ) -> Result<Response<rpc::ServerListResponse>, Status> {
        let config = self.state.config.read().await.clone();
        let path = PathBuf::from(&config.ssh.server_config_path);
        let servers = list_server_entries(&path)
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(|entry| {
                let auth_kind = entry.auth_kind().to_string();
                rpc::ServerEntry {
                    alias: entry.alias,
                    host: entry.host,
                    port: entry.port as u32,
                    user: entry.user,
                    auth_kind,
                }
            })
            .collect();
        Ok(Response::new(rpc::ServerListResponse {
            server_config_path: path.display().to_string(),
            servers,
        }))
    }

    async fn shutdown(
        &self,
        _request: Request<rpc::ShutdownRequest>,
    ) -> Result<Response<rpc::InfoResponse>, Status> {
        shutdown_daemon(&self.state)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(rpc::InfoResponse {
            message: "daemon shutting down".to_string(),
        }))
    }
}
