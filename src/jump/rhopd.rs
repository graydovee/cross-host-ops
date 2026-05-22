use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::warn;

use crate::config::{AppConfig, ServerEntry};
use crate::connection::CopySpec;
use crate::protocol::{rpc, AuthPromptMessage, ServerEvent};

use super::auth::AuthPromptRouter;
use super::error::UnsupportedCapability;
use super::{JumpHost, JumpHostKind};

/// Placeholder type for the remote address until the `RemoteAddress` parser
/// lands in task 7. For now this is just a string in `user@host:port` form.
pub type RemoteAddress = String;

/// Holds the SSH session and channel that back the gRPC transport to a remote
/// `rhopd`. Dropping this tears down the `rhop-rpc` subsystem.
#[allow(dead_code)]
pub struct RhopdTransport {
    ssh_handle: Option<()>, // placeholder until wired to russh
    ssh_channel: Option<()>, // placeholder until wired to russh
}

impl RhopdTransport {
    /// Create a dummy transport for testing (no real SSH connection).
    #[allow(dead_code)]
    pub fn new_test() -> Self {
        Self {
            ssh_handle: None,
            ssh_channel: None,
        }
    }
}

/// A [`JumpHost`] that reaches end targets through a remote `rhopd` daemon
/// over a gRPC channel multiplexed on the SSH `rhop-rpc` subsystem.
///
/// One gRPC channel is shared per `rhopd` alias; tonic supports concurrent
/// RPCs on a single channel.
#[allow(dead_code)]
pub struct RhopdJumpHost {
    alias: String,
    address: RemoteAddress,
    transport: RhopdTransport,
    client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
    auth_router: Option<Arc<AuthPromptRouter>>,
}

impl RhopdJumpHost {
    /// Stub constructor. In the real implementation this will establish an SSH
    /// connection, open the `rhop-rpc` subsystem, and build a tonic channel
    /// over the resulting byte stream.
    #[allow(dead_code)]
    pub async fn connect(
        _alias: String,
        _address: RemoteAddress,
    ) -> Result<Self> {
        anyhow::bail!("RhopdJumpHost::connect is not yet implemented")
    }

    /// Build from pre-constructed components (useful for testing and for the
    /// pool/factory once the transport layer is wired).
    #[allow(dead_code)]
    pub fn from_parts(
        alias: String,
        address: RemoteAddress,
        transport: RhopdTransport,
        client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
    ) -> Self {
        Self {
            alias,
            address,
            transport,
            client,
            auth_router: None,
        }
    }

    /// Set the auth prompt router used to forward authentication prompts
    /// received from the inner gRPC stream upstream to the CLI.
    #[allow(dead_code)]
    pub fn set_auth_router(&mut self, router: Arc<AuthPromptRouter>) {
        self.auth_router = Some(router);
    }
}

#[async_trait]
impl JumpHost for RhopdJumpHost {
    async fn exec(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        config: &AppConfig,
    ) -> Result<i32> {
        // Build the initial StartRequest and send it as the first message on
        // the Execute streaming RPC. We use an mpsc channel so we can send
        // AuthInputRequest messages back into the stream when auth prompts arrive.
        let start = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: self.alias.clone(),
                argv: argv.to_vec(),
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::ExecuteRequest>(4);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send start request into stream")
        })?;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self
            .client
            .execute(request_stream)
            .await?
            .into_inner();

        let mut exit_code: Option<i32> = None;
        let connect_timeout = config.ssh.connect_timeout;

        // Bridge ExecuteResponse events 1:1 back to the sender.
        while let Some(response) = response_stream.message().await? {
            if let Some(event) = response.event {
                match event {
                    rpc::execute_response::Event::Stdout(chunk) => {
                        let _ = sender.send(ServerEvent::Stdout { data: chunk.data });
                    }
                    rpc::execute_response::Event::Stderr(chunk) => {
                        let _ = sender.send(ServerEvent::Stderr { data: chunk.data });
                    }
                    rpc::execute_response::Event::ExitStatus(status) => {
                        exit_code = Some(status.code);
                    }
                    rpc::execute_response::Event::Error(err) => {
                        let _ = sender.send(ServerEvent::Error {
                            message: err.message,
                        });
                    }
                    rpc::execute_response::Event::ReviewResult(review) => {
                        let risk_level = match review.risk_level.as_str() {
                            "safe" => crate::config::RiskLevel::Safe,
                            "risky" => crate::config::RiskLevel::Risky,
                            _ => crate::config::RiskLevel::Dangerous,
                        };
                        let action = match review.action.as_str() {
                            "allow" => crate::config::ReviewAction::Allow,
                            "warn" => crate::config::ReviewAction::Warn,
                            "confirm" => crate::config::ReviewAction::Confirm,
                            _ => crate::config::ReviewAction::Deny,
                        };
                        let _ = sender.send(ServerEvent::ReviewResult {
                            execution_id: uuid::Uuid::parse_str(&review.execution_id)
                                .unwrap_or_default(),
                            risk_level,
                            action,
                            reason: review.reason,
                            matched_whitelist_reason: if review.matched_whitelist_reason.is_empty()
                            {
                                None
                            } else {
                                Some(review.matched_whitelist_reason)
                            },
                        });
                    }
                    rpc::execute_response::Event::ConfirmRequired(confirm) => {
                        let _ = sender.send(ServerEvent::ConfirmRequired {
                            execution_id: uuid::Uuid::parse_str(&confirm.execution_id)
                                .unwrap_or_default(),
                            reason: confirm.reason,
                        });
                    }
                    rpc::execute_response::Event::Info(_info) => {
                        // Info messages are informational; no action needed.
                    }
                    rpc::execute_response::Event::AuthPrompt(prompt) => {
                        if let Some(router) = &self.auth_router {
                            // Forward the auth prompt upstream via the router and
                            // wait for the response, then send AuthInputRequest back.
                            let prompt_id = prompt.prompt_id.clone();
                            let target_label = prompt.target_label.clone();
                            let msg = AuthPromptMessage {
                                prompt_id: prompt.prompt_id,
                                target_label: prompt.target_label,
                                kind: prompt.kind,
                                secret: prompt.secret,
                                message: prompt.message,
                            };

                            let ask_result = tokio::time::timeout(
                                connect_timeout,
                                router.ask(msg),
                            )
                            .await;

                            match ask_result {
                                Ok(Ok(value)) => {
                                    // Send AuthInputRequest back into the gRPC stream
                                    let auth_input = rpc::ExecuteRequest {
                                        request: Some(
                                            rpc::execute_request::Request::AuthInput(
                                                rpc::AuthInputRequest {
                                                    prompt_id,
                                                    value,
                                                },
                                            ),
                                        ),
                                    };
                                    if req_tx.send(auth_input).await.is_err() {
                                        return Err(anyhow::anyhow!(
                                            "failed to send auth input into gRPC stream"
                                        ));
                                    }
                                }
                                Ok(Err(e)) => {
                                    warn!(
                                        prompt_id = %prompt_id,
                                        error = %e,
                                        "auth prompt ask failed"
                                    );
                                    return Err(e);
                                }
                                Err(_elapsed) => {
                                    return Err(anyhow::anyhow!(
                                        "auth prompt timed out for {}",
                                        target_label
                                    ));
                                }
                            }
                        } else {
                            // No router available; forward as ServerEvent for the
                            // daemon's event loop to handle (legacy path).
                            let _ = sender.send(ServerEvent::AuthPrompt {
                                prompt_id: prompt.prompt_id,
                                target_label: prompt.target_label,
                                kind: prompt.kind,
                                secret: prompt.secret,
                                message: prompt.message,
                            });
                        }
                    }
                }
            }
        }

        exit_code.ok_or_else(|| anyhow::anyhow!("remote daemon closed stream without exit status"))
    }

    async fn copy(&mut self, spec: &CopySpec, config: &AppConfig) -> Result<()> {
        // Build CopyStartRequest with local_path intentionally set to "" for
        // rhopd hops — the remote daemon must not touch local paths.
        let direction = match spec.direction {
            crate::connection::CopyDirection::Upload => rpc::CopyDirection::Upload as i32,
            crate::connection::CopyDirection::Download => rpc::CopyDirection::Download as i32,
        };

        let start = rpc::CopyRequest {
            request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
                target: self.alias.clone(),
                local_path: String::new(), // intentionally empty for rhopd hops
                remote_path: spec.remote_path.clone(),
                recursive: spec.recursive,
                direction,
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::CopyRequest>(4);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send copy start request into stream")
        })?;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self.client.copy(request_stream).await?.into_inner();

        let connect_timeout = config.ssh.connect_timeout;

        // Bridge CopyResponse events 1:1.
        while let Some(response) = response_stream.message().await? {
            if let Some(event) = response.event {
                match event {
                    rpc::copy_response::Event::Complete(_complete) => {
                        return Ok(());
                    }
                    rpc::copy_response::Event::Error(err) => {
                        return Err(anyhow::anyhow!("remote copy error: {}", err.message));
                    }
                    rpc::copy_response::Event::Info(_info) => {
                        // Informational; no action needed.
                    }
                    rpc::copy_response::Event::AuthPrompt(prompt) => {
                        if let Some(router) = &self.auth_router {
                            // Forward the auth prompt upstream via the router and
                            // wait for the response, then send AuthInputRequest back.
                            let prompt_id = prompt.prompt_id.clone();
                            let target_label = prompt.target_label.clone();
                            let msg = AuthPromptMessage {
                                prompt_id: prompt.prompt_id,
                                target_label: prompt.target_label,
                                kind: prompt.kind,
                                secret: prompt.secret,
                                message: prompt.message,
                            };

                            let ask_result = tokio::time::timeout(
                                connect_timeout,
                                router.ask(msg),
                            )
                            .await;

                            match ask_result {
                                Ok(Ok(value)) => {
                                    // Send AuthInputRequest back into the gRPC stream
                                    let auth_input = rpc::CopyRequest {
                                        request: Some(
                                            rpc::copy_request::Request::AuthInput(
                                                rpc::AuthInputRequest {
                                                    prompt_id,
                                                    value,
                                                },
                                            ),
                                        ),
                                    };
                                    if req_tx.send(auth_input).await.is_err() {
                                        return Err(anyhow::anyhow!(
                                            "failed to send auth input into gRPC copy stream"
                                        ));
                                    }
                                }
                                Ok(Err(e)) => {
                                    warn!(
                                        prompt_id = %prompt_id,
                                        error = %e,
                                        "copy auth prompt ask failed"
                                    );
                                    return Err(e);
                                }
                                Err(_elapsed) => {
                                    return Err(anyhow::anyhow!(
                                        "auth prompt timed out for {}",
                                        target_label
                                    ));
                                }
                            }
                        }
                        // If no router, auth prompts are silently ignored (no
                        // way to respond without a router).
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "remote daemon closed copy stream without completion"
        ))
    }

    async fn tui_shell(&mut self, _config: &AppConfig) -> Result<()> {
        // Until interactive shell ships, return UnsupportedCapability per Req 4.5.
        Err(UnsupportedCapability {
            kind: JumpHostKind::Rhopd,
            alias: self.alias.clone(),
            method: "tui_shell",
        }
        .into())
    }

    async fn list_servers(&mut self, _config: &AppConfig) -> Result<Vec<ServerEntry>> {
        // Call the Remote_Daemon's ListServers RPC over the pooled gRPC channel.
        let response = self
            .client
            .list_servers(rpc::ServerListRequest {})
            .await?
            .into_inner();

        let entries = response
            .servers
            .into_iter()
            .map(|s| {
                let auth = if s.auth_kind == "password" {
                    crate::config::DirectAuth::Password {
                        password: String::new(),
                    }
                } else {
                    crate::config::DirectAuth::Key {
                        identity_file: String::new(),
                    }
                };
                ServerEntry {
                    alias: s.alias,
                    host: s.host,
                    port: s.port as u16,
                    user: s.user,
                    auth,
                }
            })
            .collect();

        Ok(entries)
    }

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Rhopd
    }

    fn alias(&self) -> &str {
        &self.alias
    }
}
