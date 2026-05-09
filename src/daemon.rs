use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use tokio::fs;
use tokio::net::UnixListener;
use tokio::sync::{RwLock, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{AppConfig, ReviewAction, default_config_path};
use crate::connection::CopySpec;
use crate::connection::{build_remote_command, resolve_target};
use crate::logging::init_logging;
use crate::pool::ConnectionPool;
use crate::protocol::{self, ExecRequest, ServerEvent, rpc};
use crate::review::CommandReviewer;

#[derive(Clone)]
struct DaemonState {
    config_path: PathBuf,
    config: Arc<RwLock<AppConfig>>,
    pool: ConnectionPool,
    reviewer: CommandReviewer,
}

#[derive(Clone)]
struct RhopRpcService {
    state: DaemonState,
}

pub async fn run(config_path: Option<PathBuf>) -> Result<()> {
    run_with_overrides(config_path, None).await
}

pub async fn run_with_overrides(
    config_path: Option<PathBuf>,
    log_level_override: Option<String>,
) -> Result<()> {
    let config_path = config_path.unwrap_or_else(default_config_path);
    let mut loaded = AppConfig::load(Some(&config_path))?;
    if let Some(level) = log_level_override {
        loaded.server.log_level = level;
    }
    let _log_guard = init_logging(loaded.server.log_path.clone(), &loaded.server.log_level)?;
    info!(config_path = %config_path.display(), "starting rhopd");

    let config = Arc::new(RwLock::new(loaded));
    let state = DaemonState {
        config_path,
        config: config.clone(),
        pool: ConnectionPool::new(config),
        reviewer: CommandReviewer::new()?,
    };

    ensure_socket_parent(&state).await?;
    let socket_path = PathBuf::from(state.config.read().await.server.socket_path.clone());
    if socket_path.exists() {
        let _ = fs::remove_file(&socket_path).await;
    }
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("failed to bind {}", socket_path.display()))?;
    info!(socket_path = %socket_path.display(), "listening on socket");

    let reaper_state = state.clone();
    tokio::spawn(async move {
        loop {
            let interval = reaper_state.config.read().await.server.reaper_interval;
            sleep(interval).await;
            reaper_state.pool.prune_idle().await;
            debug!("idle connection reaper tick");
        }
    });

    let incoming = UnixListenerStream::new(listener);
    Server::builder()
        .add_service(rpc::rhop_rpc_server::RhopRpcServer::new(RhopRpcService {
            state,
        }))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

async fn ensure_socket_parent(state: &DaemonState) -> Result<()> {
    let socket_path = state.config.read().await.server.socket_path.clone();
    let parent = Path::new(&socket_path)
        .parent()
        .ok_or_else(|| anyhow!("invalid socket path {}", socket_path))?;
    fs::create_dir_all(parent).await?;
    Ok(())
}

async fn reload_config(state: &DaemonState) -> Result<()> {
    let old_log_path = state.config.read().await.server.log_path.clone();
    let config = AppConfig::load(Some(&state.config_path))?;
    if config.server.log_path != old_log_path {
        warn!("log_path changes require daemon restart to take effect");
    }
    *state.config.write().await = config;
    info!(config_path = %state.config_path.display(), "reloaded config");
    Ok(())
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
    let target = resolve_target(&request.target, &config)?;
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
    let exec_target = target.clone();
    let exec_task = tokio::spawn(async move { pool.execute(exec_target, argv, tx).await });
    tokio::pin!(exec_task);

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                send_execute_event(sender, event).await?;
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

#[tonic::async_trait]
impl rpc::rhop_rpc_server::RhopRpc for RhopRpcService {
    type ExecuteStream = ReceiverStream<Result<rpc::ExecuteResponse, Status>>;

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
        request: Request<rpc::CopyRequest>,
    ) -> Result<Response<rpc::InfoResponse>, Status> {
        let (target_input, spec): (String, CopySpec) =
            protocol::copy_spec_from_rpc(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let config = self.state.config.read().await.clone();
        let target = resolve_target(&target_input, &config)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        info!(
            target = %target.key,
            direction = ?spec.direction,
            local_path = %spec.local_path,
            remote_path = %spec.remote_path,
            recursive = spec.recursive,
            "copy request"
        );
        self.state
            .pool
            .copy(target, spec)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(rpc::InfoResponse {
            message: String::new(),
        }))
    }

    async fn status(
        &self,
        _request: Request<rpc::StatusRequest>,
    ) -> Result<Response<rpc::StatusResponse>, Status> {
        info!("status request");
        let socket_path = self.state.config.read().await.server.socket_path.clone();
        let pools = self.state.pool.status();
        let active_executions = pools.iter().map(|entry| entry.busy).sum::<usize>() as u64;
        let response = rpc::StatusResponse {
            daemon_running: true,
            socket_path,
            active_executions,
            pools: pools
                .into_iter()
                .map(protocol::pool_status_to_rpc)
                .collect(),
        };
        Ok(Response::new(response))
    }

    async fn reload_config(
        &self,
        _request: Request<rpc::ReloadConfigRequest>,
    ) -> Result<Response<rpc::InfoResponse>, Status> {
        info!("reload-config request");
        reload_config(&self.state)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(rpc::InfoResponse {
            message: "config reloaded".to_string(),
        }))
    }
}
