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

use crate::config::{AppConfig, ReviewAction, default_config_path, load_server_config, validate_jump_hosts};
use crate::connection::CopySpec;
use crate::connection::{AuthPrompter, AuthPromptRequest, build_final_command, Resolver};
use crate::jump::auth::AuthPromptRouter;
use crate::jump::factory::build_jump_host;
use crate::jump::pty::{ExecPtyFlags, effective_pty_decision};
use crate::jump::server_list::ServerListAggregator;
use crate::jump::{JumpHost, ServerListSource};
use crate::logging::{init_logging, reopen_log_output};
use crate::pool::ConnectionPool;
use crate::protocol::{self, AuthPromptMessage, ExecRequest, ServerEvent, ServerListSourceStatus, rpc};
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

impl DaemonState {
    /// Reload the jump-hosts configuration from disk.
    ///
    /// Re-reads the config file, runs `validate_jump_hosts` on the new
    /// `jump_hosts` list, and on success swaps the active config inside
    /// `Arc<RwLock<AppConfig>>`. On failure, logs the error and keeps the
    /// prior configuration unchanged.
    pub async fn reload_jump_hosts(&self) {
        let new_config = match AppConfig::load(Some(&self.config_path)) {
            Ok(cfg) => cfg,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    config_path = %self.config_path.display(),
                    "failed to read config during jump-hosts reload; keeping prior config"
                );
                return;
            }
        };

        if let Err(error) = validate_jump_hosts(&new_config.jump_hosts) {
            warn!(
                error = %format!("{error}"),
                config_path = %self.config_path.display(),
                "jump-hosts validation failed during reload; keeping prior config"
            );
            return;
        }

        let mut config = self.config.write().await;
        config.jump_hosts = new_config.jump_hosts;
        info!(
            config_path = %self.config_path.display(),
            "jump-hosts reloaded successfully"
        );
    }
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
            sighup_state.reload_jump_hosts().await;
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
        if parsed.key_data() == candidate.key_data() {
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

    // Dispatch to interactive execution path when requested.
    if request.interactive {
        // Validate: interactive requires pty == true, term_cols > 0, term_rows > 0
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
    let resolver = Resolver::new(&config, &server_config, &config.jump_hosts);
    let targets = resolver.resolve(&request.target)?;
    let target = targets
        .first()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;
    let shell_command = build_final_command(&request.argv, &request.shell);

    info!(
        execution_id = %execution_id,
        input = %request.target,
        end_target = %target.end_target.alias,
        hops = target.hops.len(),
        "resolved target"
    );

    let decision = match state
        .reviewer
        .review(&config.review, &target.end_target.alias, &request.argv, &shell_command)
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

    // Resolve PTY decision using effective_pty_decision.
    // The daemon has no TTY (stdout_is_tty = false). When both pty and no_pty
    // are false (proto3 defaults from old client), this falls back to
    // config.ssh.pty — preserving backward compatibility.
    let pty_flags = ExecPtyFlags {
        force_pty: request.pty,
        force_no_pty: request.no_pty,
    };
    let pty = effective_pty_decision(&pty_flags, &config.ssh, false);
    let timeout_ms = request.timeout_ms;
    let wants_stdin = request.stdin;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let pool = state.pool.clone();
    let argv = request.argv.clone();
    let exec_targets = targets.clone();
    let (prompt_upstream_tx, mut prompt_upstream_rx) = mpsc::unbounded_channel();
    let router = Arc::new(AuthPromptRouter::new(prompt_upstream_tx));
    let auth_prompter = make_auth_prompter(router.clone(), target.end_target.alias.clone());
    let exec_task = tokio::spawn(async move {
        pool.execute(exec_targets, argv, tx, auth_prompter, pty, request.term_cols, request.term_rows, request.shell).await
    });
    tokio::pin!(exec_task);

    // If timeout is specified, create a deadline future.
    let timeout_deadline = if timeout_ms > 0 {
        Some(tokio::time::sleep(Duration::from_millis(timeout_ms)))
    } else {
        None
    };
    // Pin the optional timeout so we can poll it in select!
    tokio::pin!(timeout_deadline);

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                send_execute_event(sender, event).await?;
            }
            Some(prompt_msg) = prompt_upstream_rx.recv() => {
                let prompt_id = prompt_msg.prompt_id.clone();
                send_execute_event(
                    sender,
                    ServerEvent::AuthPrompt {
                        prompt_id: prompt_msg.prompt_id,
                        target_label: prompt_msg.target_label,
                        kind: prompt_msg.kind,
                        secret: prompt_msg.secret,
                        message: prompt_msg.message,
                    },
                ).await?;
                let reply = wait_for_auth_input_execute(inbound, &prompt_id, wants_stdin).await;
                match reply {
                    Ok(value) => router.deliver_response(&prompt_id, value).await,
                    Err(e) => {
                        // Deliver an empty string so the waiting task unblocks
                        // and can observe the connection-level failure.
                        router.deliver_response(&prompt_id, String::new()).await;
                        warn!(prompt_id = %prompt_id, error = %e, "auth input failed");
                    }
                }
            }
            // Handle inbound messages for stdin forwarding
            msg = inbound.message(), if wants_stdin => {
                match msg {
                    Ok(Some(message)) => {
                        match message.request {
                            Some(rpc::execute_request::Request::AuthInput(input))
                                if input.prompt_id == "__stdin__" =>
                            {
                                // Stdin data forwarding: the client sends stdin
                                // chunks as AuthInputRequest with prompt_id "__stdin__".
                                // NOTE: The current connection architecture does not
                                // expose a direct stdin write channel to the remote
                                // process. This is a known limitation — stdin data is
                                // received but cannot be forwarded to the SSH channel
                                // without additional plumbing in the Connection trait.
                                debug!(
                                    execution_id = %execution_id,
                                    bytes = input.value.len(),
                                    "received stdin data (forwarding not yet supported)"
                                );
                            }
                            _ => {
                                // Other messages (confirm, non-stdin auth_input) are
                                // handled by the auth prompt path; ignore here.
                            }
                        }
                    }
                    Ok(None) => {
                        // Client disconnected
                        debug!(execution_id = %execution_id, "client disconnected (stdin stream ended)");
                    }
                    Err(e) => {
                        warn!(execution_id = %execution_id, error = %e, "inbound stream error during stdin");
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
                while let Ok(event) = rx.try_recv() {
                    send_execute_event(sender, event).await?;
                }
                send_execute_event(sender, ServerEvent::ExitStatus { code: 124 }).await?;
                break;
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

/// Process an interactive execute request.
/// Unlike batch PTY mode, this directly pipes stdin/stdout without sentinels.
/// Bidirectional forwarding: client stdin → remote, remote stdout → client,
/// client resize → remote, remote exit → client.
async fn process_interactive_execute(
    request: ExecRequest,
    state: &DaemonState,
    inbound: &mut Streaming<rpc::ExecuteRequest>,
    sender: &mpsc::Sender<Result<rpc::ExecuteResponse, Status>>,
) -> Result<()> {
    // Step 1: Resolve target and run review (same as batch mode)
    let execution_id = Uuid::new_v4();
    let config = state.config.read().await.clone();
    let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
        .unwrap_or_default();
    let resolver = Resolver::new(&config, &server_config, &config.jump_hosts);
    let targets = resolver.resolve(&request.target)?;
    let target = targets
        .first()
        .ok_or_else(|| anyhow!("no resolved target candidates"))?;
    let shell_command = build_final_command(&request.argv, &request.shell);

    info!(
        execution_id = %execution_id,
        input = %request.target,
        end_target = %target.end_target.alias,
        hops = target.hops.len(),
        interactive = true,
        "resolved target (interactive)"
    );

    // Run review
    let decision = match state
        .reviewer
        .review(&config.review, &target.end_target.alias, &request.argv, &shell_command)
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

    // Step 2: Open interactive session through pool
    let pool = state.pool.clone();
    let (prompt_upstream_tx, mut prompt_upstream_rx) = mpsc::unbounded_channel();
    let router = Arc::new(AuthPromptRouter::new(prompt_upstream_tx));
    let auth_prompter = make_auth_prompter(router.clone(), target.end_target.alias.clone());

    // Create an unbounded sender for the pool (used for status events during connection)
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    let mut handle = pool
        .execute_interactive(
            targets.clone(),
            request.argv.clone(),
            request.term_cols,
            request.term_rows,
            event_tx,
            auth_prompter,
            request.shell.clone(),
        )
        .await?;

    // Drain any connection-phase events (e.g. Info messages)
    while let Ok(event) = event_rx.try_recv() {
        send_execute_event(sender, event).await?;
    }

    info!(execution_id = %execution_id, "interactive session started");

    // Step 3: Bidirectional forwarding loop
    loop {
        tokio::select! {
            // Remote stdout → client
            data = handle.stdout_rx.recv() => {
                match data {
                    Some(bytes) => {
                        send_execute_event(sender, ServerEvent::Stdout { data: bytes }).await?;
                    }
                    None => {
                        // stdout channel closed without exit code; send default
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
                            Some(rpc::execute_request::Request::StdinData(stdin)) => {
                                // Forward stdin bytes to remote
                                if handle.stdin_tx.send(stdin.data).await.is_err() {
                                    debug!(execution_id = %execution_id, "stdin_tx closed");
                                    break;
                                }
                            }
                            Some(rpc::execute_request::Request::WindowResize(resize)) => {
                                // Forward window resize to remote
                                if handle.resize_tx.send((resize.cols, resize.rows)).await.is_err() {
                                    debug!(execution_id = %execution_id, "resize_tx closed");
                                }
                            }
                            Some(rpc::execute_request::Request::AuthInput(input)) => {
                                // Handle auth prompts during interactive session
                                router.deliver_response(&input.prompt_id, input.value).await;
                            }
                            _ => {
                                // Ignore other message types
                            }
                        }
                    }
                    Ok(None) => {
                        // Client disconnected — drop handle to close SSH channel
                        // (remote process gets SIGHUP)
                        debug!(execution_id = %execution_id, "client disconnected (interactive)");
                        break;
                    }
                    Err(e) => {
                        warn!(execution_id = %execution_id, error = %e, "inbound stream error (interactive)");
                        break;
                    }
                }
            }
            // Auth prompts from the connection layer
            Some(prompt_msg) = prompt_upstream_rx.recv() => {
                let prompt_id = prompt_msg.prompt_id.clone();
                send_execute_event(
                    sender,
                    ServerEvent::AuthPrompt {
                        prompt_id: prompt_msg.prompt_id,
                        target_label: prompt_msg.target_label,
                        kind: prompt_msg.kind,
                        secret: prompt_msg.secret,
                        message: prompt_msg.message,
                    },
                ).await?;
                // Auth response will be delivered via the inbound message handler above
                let _ = prompt_id; // prompt_id used for routing in the AuthInput arm
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

/// Creates an `AuthPrompter` closure backed by an `AuthPromptRouter`.
///
/// Each in-flight Execute/Copy request owns one router instance. The prompter
/// closure translates `AuthPromptRequest` values into `AuthPromptMessage` values
/// and delegates to the router's `ask` method, which sends the prompt upstream
/// and blocks until `deliver_response` is called with the matching `prompt_id`.
fn make_auth_prompter(
    router: Arc<AuthPromptRouter>,
    default_target_label: String,
) -> Arc<AuthPrompter> {
    Arc::new(move |request: AuthPromptRequest| {
        let router = router.clone();
        let default_target_label = default_target_label.clone();
        Box::pin(async move {
            let prompt_id = Uuid::new_v4().to_string();
            let target_label = if request.target_label.is_empty() {
                default_target_label
            } else {
                request.target_label
            };
            router
                .ask(AuthPromptMessage {
                    prompt_id,
                    target_label,
                    kind: request.kind,
                    secret: request.secret,
                    message: request.message,
                })
                .await
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
    wants_stdin: bool,
) -> Result<String> {
    loop {
        let Some(message) = inbound.message().await? else {
            bail!("client disconnected before auth input");
        };
        match message.request {
            Some(rpc::execute_request::Request::AuthInput(input)) if input.prompt_id == prompt_id => {
                return Ok(input.value);
            }
            Some(rpc::execute_request::Request::AuthInput(input)) if wants_stdin && input.prompt_id == "__stdin__" => {
                // Skip stdin data messages while waiting for auth input
                continue;
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
                    pty: start.pty,
                    no_pty: start.no_pty,
                    stdin: start.stdin,
                    timeout_ms: start.timeout_ms,
                    interactive: start.interactive,
                    term_cols: start.term_cols,
                    term_rows: start.term_rows,
                    shell: start.shell,
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
                let Some(rpc::copy_request::Request::Start(start)) = first.request else {
                    bail!("first copy stream message must be start");
                };

                // Defense in depth: reject Copy requests received over the
                // rhop-rpc subsystem when local_path is non-empty.
                if is_remote && !start.local_path.is_empty() {
                    bail!("Copy requests received over rhop-rpc must not specify local_path");
                }

                let (target_input, spec, timeout_ms): (String, CopySpec, u64) = protocol::copy_spec_from_rpc(start)?;
                let config = state.config.read().await.clone();
                let server_config = load_server_config(std::path::Path::new(&config.ssh.server_config_path))
                    .unwrap_or_default();
                let resolver = Resolver::new(&config, &server_config, &config.jump_hosts);
                let targets = resolver.resolve(&target_input)?;
                let target = targets
                    .first()
                    .ok_or_else(|| anyhow!("no resolved target candidates"))?;
                info!(
                    target = %target.end_target.alias,
                    direction = ?spec.direction,
                    local_path = %spec.local_path,
                    remote_path = %spec.remote_path,
                    recursive = spec.recursive,
                    timeout_ms,
                    "copy request"
                );

                let pool = state.pool.clone();
                let (prompt_upstream_tx, mut prompt_upstream_rx) = mpsc::unbounded_channel();
                let router = Arc::new(AuthPromptRouter::new(prompt_upstream_tx));
                let auth_prompter = make_auth_prompter(router.clone(), target.end_target.alias.clone());
                let copy_task = tokio::spawn(async move { pool.copy(targets, spec, auth_prompter).await });
                tokio::pin!(copy_task);

                // If timeout is specified, create a deadline future.
                let copy_timeout = if timeout_ms > 0 {
                    Some(tokio::time::sleep(Duration::from_millis(timeout_ms)))
                } else {
                    None
                };
                tokio::pin!(copy_timeout);

                loop {
                    tokio::select! {
                        Some(prompt_msg) = prompt_upstream_rx.recv() => {
                            let prompt_id = prompt_msg.prompt_id.clone();
                            sender
                                .send(Ok(protocol::copy_auth_prompt_response(prompt_msg)))
                                .await
                                .map_err(|_| anyhow!("copy client stream closed"))?;
                            let reply = wait_for_auth_input_copy(&mut inbound, &prompt_id).await;
                            match reply {
                                Ok(value) => router.deliver_response(&prompt_id, value).await,
                                Err(e) => {
                                    router.deliver_response(&prompt_id, String::new()).await;
                                    warn!(prompt_id = %prompt_id, error = %e, "copy auth input failed");
                                }
                            }
                        }
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
        let jump_hosts: Vec<rpc::JumpHostStatus> = config
            .jump_hosts
            .iter()
            .map(|entry| {
                let address = match &entry.fields {
                    crate::config::JumpHostFields::Rhopd(fields) => fields.address.clone(),
                    crate::config::JumpHostFields::Jumpserver(fields) => {
                        format!("{}:{}", fields.host, fields.port)
                    }
                    crate::config::JumpHostFields::Direct(fields) => {
                        format!("{}:{}", fields.host, fields.port)
                    }
                };
                rpc::JumpHostStatus {
                    name: entry.name.clone(),
                    kind: entry.kind.to_string(),
                    address,
                    sub_status: None,
                }
            })
            .collect();
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
            jump_hosts,
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
        _request: Request<rpc::ServerListRequest>,
    ) -> Result<Response<rpc::ServerListResponse>, Status> {
        let config = self.state.config.read().await.clone();
        let path = PathBuf::from(&config.ssh.server_config_path);
        let server_config = load_server_config(&path)
            .map_err(|error| Status::internal(error.to_string()))?;

        // Build jump hosts from the current config. Per-entry construction
        // failures are captured as `(JumpHost(name), Error(msg))` rows so a
        // single misconfigured or unreachable entry does not turn the whole
        // RPC into `Status::Internal`. The `format!("{error}")` text is kept
        // verbatim so callers see the upstream Display message untouched.
        //
        // `list_servers` runs without an interactive client connection, so we
        // wire a router whose upstream channel is dropped immediately. Any
        // keyboard-interactive prompt during jump-host construction will fail
        // with a closed-channel error, which is the correct behavior since
        // `list_servers` cannot solicit input from the caller.
        let (prompt_tx, _prompt_rx) = mpsc::unbounded_channel();
        let router = Arc::new(AuthPromptRouter::new(prompt_tx));
        let auth_prompter = make_auth_prompter(router, String::new());

        let mut jump_hosts: Vec<Box<dyn JumpHost>> = Vec::new();
        let mut prebuilt_status: Vec<(ServerListSource, ServerListSourceStatus)> = Vec::new();
        for entry in &config.jump_hosts {
            match build_jump_host(entry, "", &auth_prompter, &config).await {
                Ok(host) => jump_hosts.push(host),
                Err(error) => {
                    prebuilt_status.push((
                        ServerListSource::JumpHost(entry.name.clone()),
                        ServerListSourceStatus::Error(format!("{error}")),
                    ));
                }
            }
        }

        let mut aggregator = ServerListAggregator {
            local: &server_config,
            jump_hosts: &mut jump_hosts,
            config: &config,
            cache: HashMap::new(),
        };
        let mut merged = aggregator.aggregate(false).await;
        merged.source_status.extend(prebuilt_status);

        // Populate the legacy `servers` field for backward compatibility.
        let servers: Vec<rpc::ServerEntry> = merged
            .rows
            .iter()
            .map(|row| protocol::server_entry_to_rpc(row.server.clone()))
            .collect();

        // Convert the MergedServerList to its RPC representation.
        let merged_rpc = protocol::merged_server_list_to_rpc(merged);

        Ok(Response::new(rpc::ServerListResponse {
            server_config_path: path.display().to_string(),
            servers,
            merged: Some(merged_rpc),
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

    async fn update_config(
        &self,
        request: Request<rpc::UpdateConfigRequest>,
    ) -> Result<Response<rpc::UpdateConfigResponse>, Status> {
        let req = request.into_inner();
        match req.mutation_type.as_str() {
            "add_jump_host" => {
                let alias = req.name.trim().to_string();
                if alias.is_empty() {
                    return Ok(Response::new(rpc::UpdateConfigResponse {
                        success: false,
                        message: "name must not be empty".to_string(),
                    }));
                }
                if crate::config::RESERVED_NAMES.contains(&alias.as_str()) {
                    return Ok(Response::new(rpc::UpdateConfigResponse {
                        success: false,
                        message: format!(
                            "name '{}' is reserved (reserved names: {:?})",
                            alias,
                            crate::config::RESERVED_NAMES
                        ),
                    }));
                }
                // Check for collision with existing jump hosts
                {
                    let config = self.state.config.read().await;
                    if let Some(existing) = config.jump_hosts.iter().find(|e| e.name == alias) {
                        return Ok(Response::new(rpc::UpdateConfigResponse {
                            success: false,
                            message: format!(
                                "name '{}' is already used by a {} jump host",
                                alias, existing.kind
                            ),
                        }));
                    }
                }

                let kind_str = req.kind.trim().to_string();
                let kind = match kind_str.as_str() {
                    "rhopd" => crate::jump::JumpHostKind::Rhopd,
                    "jumpserver" => crate::jump::JumpHostKind::Jumpserver,
                    "direct" => crate::jump::JumpHostKind::Direct,
                    other => {
                        return Ok(Response::new(rpc::UpdateConfigResponse {
                            success: false,
                            message: format!("unknown jump host kind: '{}'", other),
                        }));
                    }
                };

                let new_entry = crate::config::JumpHostConfig {
                    name: alias.clone(),
                    kind,
                    fields: match kind {
                        crate::jump::JumpHostKind::Rhopd => {
                            crate::config::JumpHostFields::Rhopd(crate::config::RhopdJumpHostFields {
                                address: req.address.clone(),
                                identity_file: req.identity_file.clone(),
                                known_hosts_path: req.known_hosts_path.clone(),
                            })
                        }
                        _ => {
                            return Ok(Response::new(rpc::UpdateConfigResponse {
                                success: false,
                                message: format!(
                                    "add_jump_host via RPC only supports kind 'rhopd', got '{}'",
                                    kind_str
                                ),
                            }));
                        }
                    },
                };

                // Add to in-memory config and write atomically
                {
                    let mut config = self.state.config.write().await;
                    config.jump_hosts.push(new_entry);
                }
                if let Err(e) = atomic_write_config(&self.state).await {
                    // Rollback the in-memory change
                    let mut config = self.state.config.write().await;
                    config.jump_hosts.retain(|e| e.name != alias);
                    return Ok(Response::new(rpc::UpdateConfigResponse {
                        success: false,
                        message: format!("failed to write config: {}", e),
                    }));
                }

                // Hot-reload to validate
                self.state.reload_jump_hosts().await;

                info!(name = %alias, "added jump host via UpdateConfig");
                Ok(Response::new(rpc::UpdateConfigResponse {
                    success: true,
                    message: format!("jump host '{}' added successfully", alias),
                }))
            }
            "remove_jump_host" => {
                let alias = req.name.trim().to_string();
                if alias.is_empty() {
                    return Ok(Response::new(rpc::UpdateConfigResponse {
                        success: false,
                        message: "name must not be empty".to_string(),
                    }));
                }

                // Find and remove the entry
                let removed = {
                    let mut config = self.state.config.write().await;
                    let before_len = config.jump_hosts.len();
                    config.jump_hosts.retain(|e| e.name != alias);
                    before_len != config.jump_hosts.len()
                };

                if !removed {
                    return Ok(Response::new(rpc::UpdateConfigResponse {
                        success: false,
                        message: format!("jump host '{}' not found", alias),
                    }));
                }

                if let Err(e) = atomic_write_config(&self.state).await {
                    // Reload from disk to restore consistency
                    self.state.reload_jump_hosts().await;
                    return Ok(Response::new(rpc::UpdateConfigResponse {
                        success: false,
                        message: format!("failed to write config: {}", e),
                    }));
                }

                // Hot-reload to ensure consistency
                self.state.reload_jump_hosts().await;

                info!(name = %alias, "removed jump host via UpdateConfig");
                Ok(Response::new(rpc::UpdateConfigResponse {
                    success: true,
                    message: format!("jump host '{}' removed successfully", alias),
                }))
            }
            other => Ok(Response::new(rpc::UpdateConfigResponse {
                success: false,
                message: format!("unknown mutation_type: '{}'", other),
            })),
        }
    }

    async fn list_jump_hosts(
        &self,
        _request: Request<rpc::ListJumpHostsRequest>,
    ) -> Result<Response<rpc::ListJumpHostsResponse>, Status> {
        let config = self.state.config.read().await.clone();
        let jump_hosts: Vec<rpc::JumpHostStatus> = config
            .jump_hosts
            .iter()
            .map(|entry| {
                let address = match &entry.fields {
                    crate::config::JumpHostFields::Rhopd(fields) => fields.address.clone(),
                    crate::config::JumpHostFields::Jumpserver(fields) => {
                        format!("{}:{}", fields.host, fields.port)
                    }
                    crate::config::JumpHostFields::Direct(fields) => {
                        format!("{}:{}", fields.host, fields.port)
                    }
                };
                rpc::JumpHostStatus {
                    name: entry.name.clone(),
                    kind: entry.kind.to_string(),
                    address,
                    sub_status: None,
                }
            })
            .collect();
        Ok(Response::new(rpc::ListJumpHostsResponse { jump_hosts }))
    }
}

/// Atomically writes the current in-memory config to disk using a temp file + rename.
/// This ensures that a crash during write does not corrupt the config file.
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

/// Test support: exposes the ability to create an `RhopRpcServer` service
/// backed by a given `AppConfig` and config path, suitable for serving over
/// an in-process transport (e.g. `tokio::io::duplex`).
pub mod test_support {
    use super::*;

    /// Creates a tonic `RhopRpcServer` service instance backed by the given
    /// config. The returned service can be added to a `tonic::transport::Server`
    /// and served over any async I/O transport.
    pub fn make_test_rpc_service(
        config: AppConfig,
        config_path: PathBuf,
    ) -> rpc::rhop_rpc_server::RhopRpcServer<impl rpc::rhop_rpc_server::RhopRpc> {
        let config = Arc::new(RwLock::new(config));
        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);
        let state = DaemonState {
            config_path,
            config: config.clone(),
            pool: ConnectionPool::new(config),
            reviewer: CommandReviewer::new().expect("failed to create reviewer"),
            shutdown_tx,
            origin: DaemonOrigin::External,
            cli_start_options: CliStartOptions::default(),
        };
        rpc::rhop_rpc_server::RhopRpcServer::new(RhopRpcService { state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, Write};
    use tempfile::NamedTempFile;

    fn make_test_state(config_path: PathBuf) -> DaemonState {
        let config = Arc::new(RwLock::new(AppConfig::default()));
        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);
        DaemonState {
            config_path,
            config: config.clone(),
            pool: ConnectionPool::new(config),
            reviewer: CommandReviewer::new().unwrap(),
            shutdown_tx,
            origin: DaemonOrigin::External,
            cli_start_options: CliStartOptions::default(),
        }
    }

    #[tokio::test]
    async fn reload_jump_hosts_swaps_on_valid_config() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[[jump_hosts]]
name = "prod"
kind = "rhopd"
address = "admin@prod.example.com:22"
identity_file = "/tmp/id"
known_hosts_path = "/tmp/kh"
"#
        )
        .unwrap();

        let state = make_test_state(file.path().to_path_buf());
        assert!(state.config.read().await.jump_hosts.is_empty());

        state.reload_jump_hosts().await;

        let config = state.config.read().await;
        assert_eq!(config.jump_hosts.len(), 1);
        assert_eq!(config.jump_hosts[0].name, "prod");
    }

    #[tokio::test]
    async fn reload_jump_hosts_keeps_prior_on_validation_failure() {
        // Start with a valid config that has one jump host
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[[jump_hosts]]
name = "existing"
kind = "rhopd"
address = "admin@host.example.com:22"
identity_file = "/tmp/id"
known_hosts_path = "/tmp/kh"
"#
        )
        .unwrap();

        let state = make_test_state(file.path().to_path_buf());
        // Pre-populate with the existing entry
        state.reload_jump_hosts().await;
        assert_eq!(state.config.read().await.jump_hosts.len(), 1);

        // Now write an invalid config (reserved name "local")
        file.as_file_mut().set_len(0).unwrap();
        file.as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
        writeln!(
            file,
            r#"
[[jump_hosts]]
name = "local"
kind = "rhopd"
address = "admin@host.example.com:22"
identity_file = "/tmp/id"
known_hosts_path = "/tmp/kh"
"#
        )
        .unwrap();

        state.reload_jump_hosts().await;

        // Should still have the prior valid config
        let config = state.config.read().await;
        assert_eq!(config.jump_hosts.len(), 1);
        assert_eq!(config.jump_hosts[0].name, "existing");
    }

    #[tokio::test]
    async fn reload_jump_hosts_keeps_prior_on_unreadable_file() {
        // Use a path that exists but is a directory (unreadable as a file)
        let dir = tempfile::tempdir().unwrap();
        let bad_path = dir.path().to_path_buf();
        let state = make_test_state(bad_path.join("subdir_that_is_actually_a_dir"));

        // Create the path as a directory so reading it as a file fails
        std::fs::create_dir_all(state.config_path.clone()).unwrap();

        // Pre-populate with a jump host
        {
            let mut config = state.config.write().await;
            config.jump_hosts.push(crate::config::JumpHostConfig {
                name: "keep-me".to_string(),
                kind: crate::jump::JumpHostKind::Rhopd,
                fields: crate::config::JumpHostFields::Rhopd(
                    crate::config::RhopdJumpHostFields {
                        address: "user@host:22".to_string(),
                        identity_file: String::new(),
                        known_hosts_path: String::new(),
                    },
                ),
            });
        }

        state.reload_jump_hosts().await;

        // Should still have the prior config since reading a directory as a file fails
        let config = state.config.read().await;
        assert_eq!(config.jump_hosts.len(), 1);
        assert_eq!(config.jump_hosts[0].name, "keep-me");
    }
}
