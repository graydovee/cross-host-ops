// RhopdConnection implementation.
// Wraps a gRPC client that communicates with a remote rhopd daemon and
// implements the Connection trait for exec/copy/exec_interactive operations.

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::warn;

use crate::types::{CopyDirection, CopySpec};
use crate::protocol::{rpc, ServerEvent};

use super::{Connection, ExecRequest, InteractiveHandle, InteractiveRequest};

// ---------------------------------------------------------------------------
// RhopdConnection
// ---------------------------------------------------------------------------

/// A connection to an end target via a remote rhopd daemon's gRPC interface.
/// The gRPC client is shared with the owning RhopdGateway; this struct holds
/// a clone of the client plus the target label to route requests to.
pub(crate) struct RhopdConnection {
    client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
    /// The end-target server alias sent as the `target` field in RPCs.
    target_label: String,
}

impl RhopdConnection {
    /// Create a new RhopdConnection from a pre-connected gRPC client.
    pub(crate) fn new(
        client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
        target_label: String,
    ) -> Self {
        Self {
            client,
            target_label,
        }
    }
}

#[async_trait::async_trait]
impl Connection for RhopdConnection {
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32> {
        // Build the initial StartRequest and send it as the first message on
        // the Execute streaming RPC.
        let start = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: self.target_label.clone(),
                argv: request.argv.clone(),
                pty: request.pty,
                term_cols: request.cols,
                term_rows: request.rows,
                ..Default::default()
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

        // Bridge ExecuteResponse events back to the sender.
        while let Some(response) = response_stream.message().await? {
            if let Some(event) = response.event {
                match event {
                    rpc::execute_response::Event::Stdout(chunk) => {
                        let _ = request.sender.send(ServerEvent::Stdout { data: chunk.data });
                    }
                    rpc::execute_response::Event::Stderr(chunk) => {
                        let _ = request.sender.send(ServerEvent::Stderr { data: chunk.data });
                    }
                    rpc::execute_response::Event::ExitStatus(status) => {
                        exit_code = Some(status.code);
                    }
                    rpc::execute_response::Event::Error(err) => {
                        let _ = request.sender.send(ServerEvent::Error {
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
                        let _ = request.sender.send(ServerEvent::ReviewResult {
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
                        let _ = request.sender.send(ServerEvent::ConfirmRequired {
                            execution_id: uuid::Uuid::parse_str(&confirm.execution_id)
                                .unwrap_or_default(),
                            reason: confirm.reason,
                        });
                    }
                    rpc::execute_response::Event::Info(_info) => {
                        // Informational; no action needed at the Connection level.
                    }
                    rpc::execute_response::Event::AuthPrompt(prompt) => {
                        // At the Connection level, auth prompts are forwarded as
                        // ServerEvent. The owning Gateway is responsible for
                        // handling the auth flow if needed.
                        let _ = request.sender.send(ServerEvent::AuthPrompt {
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

        exit_code.ok_or_else(|| anyhow::anyhow!("remote daemon closed stream without exit status"))
    }

    async fn copy(&mut self, spec: &CopySpec) -> Result<()> {
        // Build CopyStartRequest with local_path intentionally set to "" for
        // rhopd hops — the remote daemon must not touch local paths.
        let direction = match spec.direction {
            CopyDirection::Upload => rpc::CopyDirection::Upload as i32,
            CopyDirection::Download => rpc::CopyDirection::Download as i32,
        };

        let start = rpc::CopyRequest {
            request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
                target: self.target_label.clone(),
                local_path: String::new(), // intentionally empty for rhopd hops
                remote_path: spec.remote_path.clone(),
                recursive: spec.recursive,
                direction,
                ..Default::default()
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::CopyRequest>(4);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send copy start request into stream")
        })?;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self.client.copy(request_stream).await?.into_inner();

        // Bridge CopyResponse events.
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
                    rpc::copy_response::Event::AuthPrompt(_prompt) => {
                        // Auth prompts at the Connection level cannot be
                        // responded to without a request channel back into the
                        // stream. The owning RhopdGateway handles auth at a
                        // higher level. If we reach here, we silently skip.
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "remote daemon closed copy stream without completion"
        ))
    }

    async fn exec_interactive(
        &mut self,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle> {
        // Send StartRequest with interactive=true to the remote daemon.
        let start = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: self.target_label.clone(),
                argv: request.argv.clone(),
                pty: true,
                interactive: true,
                term_cols: request.cols,
                term_rows: request.rows,
                ..Default::default()
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::ExecuteRequest>(32);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send interactive start request into stream")
        })?;

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self
            .client
            .execute(request_stream)
            .await?
            .into_inner();

        // Set up the InteractiveHandle channels.
        let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(32);
        let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(8);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<i32>();

        // Spawn a task that bridges the gRPC bidirectional stream to the
        // InteractiveHandle channels.
        tokio::spawn(async move {
            let mut exit_code: Option<i32> = None;
            loop {
                tokio::select! {
                    // Forward stdin bytes from the handle to the gRPC stream.
                    Some(data) = stdin_rx.recv() => {
                        let msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::StdinData(
                                rpc::StdinData { data },
                            )),
                        };
                        if req_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    // Forward window resize from the handle to the gRPC stream.
                    Some((cols, rows)) = resize_rx.recv() => {
                        let msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::WindowResize(
                                rpc::WindowResize { cols, rows },
                            )),
                        };
                        if req_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    // Read responses from the remote daemon.
                    response = response_stream.message() => {
                        match response {
                            Ok(Some(msg)) => {
                                if let Some(event) = msg.event {
                                    match event {
                                        rpc::execute_response::Event::Stdout(chunk) => {
                                            if stdout_tx.send(chunk.data).is_err() {
                                                break;
                                            }
                                        }
                                        rpc::execute_response::Event::Stderr(chunk) => {
                                            // In interactive mode, stderr is merged into stdout
                                            // for the terminal output.
                                            if stdout_tx.send(chunk.data).is_err() {
                                                break;
                                            }
                                        }
                                        rpc::execute_response::Event::ExitStatus(status) => {
                                            exit_code = Some(status.code);
                                            break;
                                        }
                                        rpc::execute_response::Event::Error(err) => {
                                            warn!(error = %err.message, "remote interactive error");
                                            break;
                                        }
                                        _ => {
                                            // Info, ReviewResult, ConfirmRequired, AuthPrompt —
                                            // ignored in interactive mode at the Connection level.
                                        }
                                    }
                                }
                            }
                            Ok(None) | Err(_) => {
                                // Stream closed.
                                break;
                            }
                        }
                    }
                }
            }
            let _ = exit_tx.send(exit_code.unwrap_or(255));
        });

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
        })
    }

    fn is_alive(&self) -> bool {
        // A tonic Channel remains usable as long as it hasn't been explicitly
        // dropped. The channel internally handles reconnection for HTTP/2
        // connections. We assume the connection is alive if we still hold the
        // client. The owning RhopdGateway detects transport errors during
        // operations and discards the connection when needed.
        true
    }
}
