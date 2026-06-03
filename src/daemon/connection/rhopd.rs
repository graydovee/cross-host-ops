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
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32> {
        // Take the optional stdin receiver before building the request.
        // This moves stdin_rx out so we can decide whether to spawn a relay task.
        let stdin_rx = request.stdin_rx.take();

        // Build the initial StartRequest and send it as the first message on
        // the Execute streaming RPC.
        let start = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: self.target_label.clone(),
                argv: request.argv.clone(),
                pty: request.pty,
                no_pty: !request.pty,
                stdin: stdin_rx.is_some(),
                timeout_ms: request.timeout_ms,
                interactive: false,
                term_cols: request.cols,
                term_rows: request.rows,
                shell: request.shell.clone(),
                no_shell: request.no_shell,
                ..Default::default()
            })),
        };

        let (req_tx, req_rx) = mpsc::channel::<rpc::ExecuteRequest>(4);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send start request into stream")
        })?;

        // Build the request stream and start the bidirectional RPC FIRST,
        // so that tonic begins consuming the stream and forwarding messages
        // (including any subsequent StdinData chunks the relay task will
        // produce).  Only after the RPC is established do we spawn the
        // stdin relay task — otherwise the relay can finish (and drop req_tx)
        // before tonic has a chance to set up the HTTP/2 stream, which on
        // SSH-tunneled gRPC has been observed to drop buffered messages.
        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self
            .client
            .execute(request_stream)
            .await?
            .into_inner();

        // Hold a clone of req_tx for the duration of the RPC so the request
        // stream stays open even after the relay task finishes.  This is
        // essential because EOF is now signaled via an explicit empty
        // StdinData message rather than by closing the stream — and closing
        // the stream prematurely (over an SSH-tunneled gRPC connection) has
        // been observed to drop in-flight DATA frames on the receiving end.
        let _req_tx_keepalive = req_tx.clone();

        if let Some(mut rx) = stdin_rx {
            // Spawn a stdin relay task that forwards stdin bytes upstream
            // and emits an explicit empty-StdinData EOF sentinel when the
            // local source closes.  After emitting the sentinel, the relay
            // drops its sender — but the keepalive clone above keeps the
            // request stream open so the remote can still send responses.
            tokio::spawn(async move {
                tracing::info!("rhopd-relay: started");
                let mut sent_bytes = 0usize;
                let mut sent_chunks = 0usize;
                while let Some(data) = rx.recv().await {
                    if data.is_empty() {
                        // Upstream EOF sentinel passed through verbatim.
                        let msg = rpc::ExecuteRequest {
                            request: Some(rpc::execute_request::Request::StdinData(
                                rpc::StdinData { data: Vec::new() },
                            )),
                        };
                        match req_tx.send(msg).await {
                            Ok(_) => tracing::info!(sent_chunks, sent_bytes, "rhopd-relay: forwarded explicit EOF sentinel"),
                            Err(_) => tracing::info!(sent_chunks, sent_bytes, "rhopd-relay: failed to forward EOF sentinel"),
                        }
                        break;
                    }
                    sent_bytes += data.len();
                    sent_chunks += 1;
                    let len = data.len();
                    let msg = rpc::ExecuteRequest {
                        request: Some(rpc::execute_request::Request::StdinData(
                            rpc::StdinData { data },
                        )),
                    };
                    match req_tx.send(msg).await {
                        Ok(_) => tracing::info!(len, sent_chunks, "rhopd-relay: forwarded StdinData"),
                        Err(_) => {
                            tracing::info!(sent_chunks, sent_bytes, "rhopd-relay: send failed, stopping");
                            return;
                        }
                    }
                }
                // If we reached here without seeing an explicit empty sentinel
                // (i.e. rx was dropped without sending one), still emit one so
                // the remote daemon can stop waiting for stdin.
                tracing::info!(sent_chunks, sent_bytes, "rhopd-relay: rx exhausted without sentinel, emitting fallback EOF");
                let eof = rpc::ExecuteRequest {
                    request: Some(rpc::execute_request::Request::StdinData(
                        rpc::StdinData { data: Vec::new() },
                    )),
                };
                let _ = req_tx.send(eof).await;
                // req_tx (the relay clone) drops here.  The keepalive clone
                // outside this task keeps the stream open until the RPC ends.
            });
        }
        // Note: we no longer drop req_tx in the no-stdin branch.  The
        // keepalive clone keeps the stream open for the duration of the RPC.

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

    async fn copy(&mut self, mut spec: CopySpec) -> Result<()> {
        // Build CopyStartRequest with local_path intentionally set to "" for
        // rhopd hops — the remote daemon must not touch local paths on this side.
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

        // Use a larger channel buffer for file streaming.
        let (req_tx, req_rx) = mpsc::channel::<rpc::CopyRequest>(16);
        req_tx.send(start).await.map_err(|_| {
            anyhow::anyhow!("failed to send copy start request into stream")
        })?;

        // Take relay channels out of spec (they are not Clone).
        let relay_upload_rx = spec.relay_upload_rx.take();
        let relay_download_tx = spec.relay_download_tx.take();

        if spec.direction == CopyDirection::Upload {
            if let Some(mut upload_rx) = relay_upload_rx {
                // Relay upload path: forward (data, eof) tuples from the relay
                // channel (populated by the daemon's copy RPC handler from the
                // client gRPC stream) as CopyDataChunk messages.
                tokio::spawn(async move {
                    while let Some((data, eof)) = upload_rx.recv().await {
                        let msg = rpc::CopyRequest {
                            request: Some(rpc::copy_request::Request::DataChunk(
                                rpc::CopyDataChunk { data, eof },
                            )),
                        };
                        if req_tx.send(msg).await.is_err() {
                            return;
                        }
                        if eof {
                            // EOF chunk sent — stop reading from relay.
                            break;
                        }
                    }
                    // req_tx drops here, closing the gRPC request stream.
                });
            } else {
                // Local file upload path: read the local file and stream its
                // contents as CopyDataChunk messages.
                let local_path = spec.local_path.clone();
                tokio::spawn(async move {
                    const CHUNK_SIZE: usize = 64 * 1024; // 64 KB chunks
                    match tokio::fs::read(&local_path).await {
                        Ok(file_bytes) => {
                            // Stream file contents in chunks.
                            for chunk in file_bytes.chunks(CHUNK_SIZE) {
                                let msg = rpc::CopyRequest {
                                    request: Some(rpc::copy_request::Request::DataChunk(
                                        rpc::CopyDataChunk {
                                            data: chunk.to_vec(),
                                            eof: false,
                                        },
                                    )),
                                };
                                if req_tx.send(msg).await.is_err() {
                                    // Remote end closed the stream unexpectedly.
                                    return;
                                }
                            }
                            // Send the final EOF chunk to signal end of file.
                            let eof_msg = rpc::CopyRequest {
                                request: Some(rpc::copy_request::Request::DataChunk(
                                    rpc::CopyDataChunk {
                                        data: vec![],
                                        eof: true,
                                    },
                                )),
                            };
                            let _ = req_tx.send(eof_msg).await;
                            // req_tx drops here, closing the gRPC request stream.
                        }
                        Err(e) => {
                            // File read failure — drop req_tx to signal abort to remote.
                            warn!(error = %e, path = %local_path, "upload: failed to read local file");
                        }
                    }
                });
            }
        } else {
            // Download path: no data to send from client side — drop req_tx
            // after the start message to close the upload half of the stream.
            drop(req_tx);
        }

        let request_stream = ReceiverStream::new(req_rx);
        let mut response_stream = self.client.copy(request_stream).await?.into_inner();

        // For local-file download, open the destination file lazily on first DataChunk.
        let mut download_file: Option<tokio::fs::File> = None;
        let local_path = spec.local_path.clone();

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
                    rpc::copy_response::Event::DataChunk(chunk) => {
                        if let Some(ref tx) = relay_download_tx {
                            // Relay download path: forward the chunk to the daemon's
                            // gRPC response sender. If the relay channel is closed,
                            // abort gracefully.
                            let eof = chunk.eof;
                            if tx.send((chunk.data, eof)).await.is_err() {
                                return Err(anyhow::anyhow!(
                                    "download relay channel closed unexpectedly"
                                ));
                            }
                        } else {
                            // Local file download path: write received bytes to file.
                            use tokio::io::AsyncWriteExt as _;

                            if download_file.is_none() {
                                let f = tokio::fs::File::create(&local_path)
                                    .await
                                    .map_err(|e| {
                                        anyhow::anyhow!(
                                            "download: failed to create local file {:?}: {}",
                                            local_path,
                                            e
                                        )
                                    })?;
                                download_file = Some(f);
                            }

                            if !chunk.data.is_empty() {
                                if let Some(f) = download_file.as_mut() {
                                    f.write_all(&chunk.data).await.map_err(|e| {
                                        anyhow::anyhow!(
                                            "download: failed to write to local file {:?}: {}",
                                            local_path,
                                            e
                                        )
                                    })?;
                                }
                            }

                            if chunk.eof {
                                // Flush and close the file; the remote will send
                                // a Complete event next, but we're ready.
                                if let Some(mut f) = download_file.take() {
                                    f.flush().await.map_err(|e| {
                                        anyhow::anyhow!(
                                            "download: failed to flush local file {:?}: {}",
                                            local_path,
                                            e
                                        )
                                    })?;
                                }
                            }
                        }
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tonic::transport::{Channel, Endpoint, Server, Uri};
    use tonic::{Request, Response, Status, Streaming};
    use tower::service_fn;
    use hyper_util::rt::TokioIo;
    use tokio_stream::wrappers::ReceiverStream;
    use proptest::prelude::*;

    use crate::protocol::ServerEvent;

    // -----------------------------------------------------------------------
    // Mock gRPC server that records received messages for validation.
    // -----------------------------------------------------------------------

    /// Recorded messages from one Execute RPC invocation.
    #[derive(Default)]
    struct ExecMessageLog {
        /// All messages received from the RhopdConnection client request stream.
        messages: Vec<crate::protocol::rpc::ExecuteRequest>,
    }

    /// Shared state between the mock server and test.
    type SharedExecLog = Arc<AsyncMutex<ExecMessageLog>>;

    /// A minimal mock gRPC server implementation that logs all received messages.
    struct MockRhopServer {
        exec_log: SharedExecLog,
        copy_messages_tx: tokio::sync::mpsc::UnboundedSender<crate::protocol::rpc::CopyRequest>,
    }

    #[tonic::async_trait]
    impl crate::protocol::rpc::rhop_rpc_server::RhopRpc for MockRhopServer {
        type ExecuteStream = ReceiverStream<Result<crate::protocol::rpc::ExecuteResponse, Status>>;
        type CopyStream = ReceiverStream<Result<crate::protocol::rpc::CopyResponse, Status>>;

        async fn execute(
            &self,
            request: Request<Streaming<crate::protocol::rpc::ExecuteRequest>>,
        ) -> Result<Response<Self::ExecuteStream>, Status> {
            let mut inbound = request.into_inner();
            let log = self.exec_log.clone();

            // Collect messages from the client request stream with a short
            // timeout. We cannot wait for the stream to close because the
            // buggy code keeps req_tx alive without sending more messages
            // (deadlock: mock waits for close, client waits for ExitStatus).
            // Instead: collect for a brief window, then send ExitStatus.
            let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                // Collect messages with a 200ms window to gather any
                // StdinData that might be forwarded (none on unfixed code).
                let collect_deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(200);
                loop {
                    match tokio::time::timeout_at(
                        collect_deadline,
                        inbound.message(),
                    ).await {
                        Ok(Ok(Some(msg))) => {
                            log.lock().await.messages.push(msg);
                        }
                        // Timeout OR stream closed OR error: stop collecting.
                        _ => break,
                    }
                }
                // Send exit status so the RhopdConnection caller can complete.
                let _ = resp_tx.send(Ok(crate::protocol::rpc::ExecuteResponse {
                    event: Some(crate::protocol::rpc::execute_response::Event::ExitStatus(
                        crate::protocol::rpc::ExitStatus { code: 42 },
                    )),
                })).await;
            });

            Ok(Response::new(ReceiverStream::new(resp_rx)))
        }

        async fn copy(
            &self,
            request: Request<Streaming<crate::protocol::rpc::CopyRequest>>,
        ) -> Result<Response<Self::CopyStream>, Status> {
            let mut inbound = request.into_inner();
            let tx = self.copy_messages_tx.clone();
            let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                // Collect messages with a short window — same reason as execute:
                // on buggy code req_tx stays alive without sending more messages.
                let collect_deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(200);
                loop {
                    match tokio::time::timeout_at(
                        collect_deadline,
                        inbound.message(),
                    ).await {
                        Ok(Ok(Some(msg))) => { let _ = tx.send(msg); }
                        _ => break,
                    }
                }
                // Send completion so RhopdConnection copy() returns.
                let _ = resp_tx.send(Ok(crate::protocol::rpc::CopyResponse {
                    event: Some(crate::protocol::rpc::copy_response::Event::Complete(
                        crate::protocol::rpc::CopyComplete { message: String::new() },
                    )),
                })).await;
            });
            Ok(Response::new(ReceiverStream::new(resp_rx)))
        }

        async fn status(
            &self, _: Request<crate::protocol::rpc::StatusRequest>,
        ) -> Result<Response<crate::protocol::rpc::StatusResponse>, Status> {
            Ok(Response::new(Default::default()))
        }
        async fn list_servers(
            &self, _: Request<crate::protocol::rpc::ServerListRequest>,
        ) -> Result<Response<crate::protocol::rpc::ServerListResponse>, Status> {
            Ok(Response::new(Default::default()))
        }
        async fn shutdown(
            &self, _: Request<crate::protocol::rpc::ShutdownRequest>,
        ) -> Result<Response<crate::protocol::rpc::InfoResponse>, Status> {
            Ok(Response::new(Default::default()))
        }
        async fn update_config(
            &self, _: Request<crate::protocol::rpc::UpdateConfigRequest>,
        ) -> Result<Response<crate::protocol::rpc::UpdateConfigResponse>, Status> {
            Ok(Response::new(Default::default()))
        }
        async fn list_gateways(
            &self, _: Request<crate::protocol::rpc::ListGatewaysRequest>,
        ) -> Result<Response<crate::protocol::rpc::ListGatewaysResponse>, Status> {
            Ok(Response::new(Default::default()))
        }
    }

    // -----------------------------------------------------------------------
    // Helper: spin up in-process mock gRPC server and return client + logs.
    // -----------------------------------------------------------------------

    const DUPLEX_BUF: usize = 256 * 1024;

    async fn start_mock_server() -> (
        crate::protocol::rpc::rhop_rpc_client::RhopRpcClient<Channel>,
        SharedExecLog,
        tokio::sync::mpsc::UnboundedReceiver<crate::protocol::rpc::CopyRequest>,
    ) {
        let exec_log: SharedExecLog = Arc::new(AsyncMutex::new(ExecMessageLog::default()));
        let (copy_tx, copy_rx) = tokio::sync::mpsc::unbounded_channel();

        let server = MockRhopServer {
            exec_log: exec_log.clone(),
            copy_messages_tx: copy_tx,
        };

        let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUF);

        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    crate::protocol::rpc::rhop_rpc_server::RhopRpcServer::new(server),
                )
                .serve_with_incoming(tokio_stream::once(
                    Ok::<_, std::io::Error>(server_io),
                ))
                .await
                .ok();
        });

        let slot: std::sync::Mutex<Option<_>> = std::sync::Mutex::new(Some(client_io));
        let channel = Endpoint::from_static("http://[::]:50051")
            .connect_with_connector(service_fn(move |_: Uri| {
                let stream = slot.lock().unwrap().take().unwrap();
                async move { Ok::<_, std::io::Error>(TokioIo::new(stream)) }
            }))
            .await
            .expect("failed to connect test client");

        let client = crate::protocol::rpc::rhop_rpc_client::RhopRpcClient::new(channel);
        (client, exec_log, copy_rx)
    }

    // -----------------------------------------------------------------------
    // Helper: build a minimal ConnExecRequest (no stdin_rx).
    // Used by preservation tests that don't exercise stdin forwarding.
    // -----------------------------------------------------------------------

    fn make_exec_request_no_stdin(argv: Vec<String>) -> ExecRequest {
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<ServerEvent>();
        ExecRequest {
            argv,
            sender,
            pty: false,
            cols: 80,
            rows: 24,
            shell: String::new(),
            no_shell: false,
            timeout_ms: 0,
            stdin: false,
            stdin_rx: None,
        }
    }

    // -----------------------------------------------------------------------
    // Helper: build a ConnExecRequest WITH a pre-loaded stdin channel.
    // Used by the stdin-forwarding bug condition test (post-fix verification).
    // -----------------------------------------------------------------------

    fn make_exec_request_with_stdin(
        argv: Vec<String>,
        stdin_data: Vec<u8>,
    ) -> (ExecRequest, tokio::sync::mpsc::UnboundedReceiver<ServerEvent>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<ServerEvent>();
        // Buffer the entire payload in the channel before exec() is called,
        // then drop the sender so the receiver will return None after reading.
        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
        // Send the data synchronously (channel has capacity); then drop sender.
        let _ = stdin_tx.try_send(stdin_data);
        drop(stdin_tx);

        let req = ExecRequest {
            argv,
            sender: event_tx,
            pty: false,
            cols: 80,
            rows: 24,
            shell: String::new(),
            no_shell: false,
            timeout_ms: 0,
            stdin: true,
            stdin_rx: Some(stdin_rx),
        };
        (req, event_rx)
    }

    // -----------------------------------------------------------------------
    // Property 1 Bug Condition: RhopdConnection::exec must forward stdin
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig { cases: 5, .. ProptestConfig::default() })]

        /// **Validates: Requirements 1.2, 2.2**
        ///
        /// Expected Behavior: RhopdConnection::exec forwards stdin bytes as
        /// StdinData gRPC messages.
        ///
        /// With the fix in place, ExecRequest carries `stdin_rx: Some(rx)`.
        /// RhopdConnection::exec reads from the receiver and sends StdinData
        /// messages on req_tx to the remote daemon.
        ///
        /// This test verifies the fix: the mock server MUST receive at least
        /// one StdinData message whose data matches stdin_payload.
        ///
        /// EXPECTED OUTCOME: PASSES on fixed code (confirms bug is fixed).
        #[test]
        fn prop_bug_rhopd_exec_stdin_never_forwarded(
            stdin_payload in proptest::collection::vec(any::<u8>(), 1..64usize),
            argv in proptest::collection::vec("[a-z]{1,10}", 1..4usize),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (client, exec_log, _copy_rx) = start_mock_server().await;

                let mut conn = RhopdConnection::new(client, "test-target".to_string());

                // Build request WITH stdin_rx carrying the payload.
                // On fixed code, RhopdConnection::exec will spawn a relay task
                // that reads from stdin_rx and sends StdinData messages.
                let (mut request, _events) =
                    make_exec_request_with_stdin(argv.clone(), stdin_payload.clone());

                let exit_code = conn.exec(&mut request).await.expect("exec should complete");
                assert_eq!(exit_code, 42, "mock returns exit code 42");

                let log = exec_log.lock().await;
                let received_msgs = &log.messages;

                // StartRequest must be present.
                let has_start = received_msgs.iter().any(|m| {
                    matches!(
                        &m.request,
                        Some(crate::protocol::rpc::execute_request::Request::Start(_))
                    )
                });
                prop_assert!(has_start, "mock server should receive StartRequest");

                let start_has_stdin = received_msgs.iter().any(|m| {
                    if let Some(crate::protocol::rpc::execute_request::Request::Start(start)) =
                        &m.request
                    {
                        start.stdin
                    } else {
                        false
                    }
                });
                prop_assert!(
                    start_has_stdin,
                    "StartRequest must set stdin=true when stdin_rx is present"
                );

                // StdinData must be present and carry the exact payload.
                let stdin_received = received_msgs.iter().any(|m| {
                    if let Some(crate::protocol::rpc::execute_request::Request::StdinData(d)) =
                        &m.request
                    {
                        d.data == stdin_payload
                    } else {
                        false
                    }
                });

                prop_assert!(
                    stdin_received,
                    "stdin bytes must be forwarded as StdinData on the gRPC stream. \
                     Mock received {} messages: {:?}",
                    received_msgs.len(),
                    received_msgs
                        .iter()
                        .map(|m| match &m.request {
                            Some(crate::protocol::rpc::execute_request::Request::Start(_)) =>
                                "Start",
                            Some(crate::protocol::rpc::execute_request::Request::StdinData(_)) =>
                                "StdinData",
                            _ => "Other",
                        })
                        .collect::<Vec<_>>()
                );

                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 5, .. ProptestConfig::default() })]

        /// **Validates: Requirements 1.3, 1.4**
        ///
        /// Bug Condition: RhopdConnection::copy never streams file data.
        ///
        /// For any upload CopySpec, after calling `RhopdConnection::copy` the
        /// mock server's Copy stream should contain CopyDataChunk messages.
        ///
        /// On unfixed code: only CopyStartRequest is sent, no data chunks.
        /// The proto oneof in CopyRequest has no `data_chunk` variant.
        ///
        /// EXPECTED OUTCOME: Test FAILS on unfixed code because we assert
        /// that data chunks ARE received, but they never arrive (copy hangs
        /// at the remote daemon waiting for data that never comes — in the
        /// mock, we immediately complete, but in production it would hang).
        #[test]
        fn prop_bug_rhopd_copy_upload_no_data_chunks(
            file_data in proptest::collection::vec(any::<u8>(), 1..256usize),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Write file data to a temp file for the copy spec.
                let temp_file = std::env::temp_dir().join(format!(
                    "rhop-test-copy-{}.bin",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .subsec_nanos()
                ));
                tokio::fs::write(&temp_file, &file_data)
                    .await
                    .expect("failed to write temp file");

                let (client, _exec_log, mut copy_rx) = start_mock_server().await;
                let mut conn = RhopdConnection::new(client, "test-target".to_string());

                let spec = crate::types::CopySpec {
                    local_path: temp_file.display().to_string(),
                    remote_path: "/tmp/remote-dest.bin".to_string(),
                    direction: crate::types::CopyDirection::Upload,
                    recursive: false,
                    relay_upload_rx: None,
                    relay_download_tx: None,
                };

                // Perform the copy — on unfixed code this only sends StartRequest.
                let result = conn.copy(spec).await;
                assert!(result.is_ok(), "copy should complete (mock always responds with Complete)");

                // Collect all CopyRequest messages received by mock server.
                let mut copy_messages = Vec::new();
                while let Ok(msg) = copy_rx.try_recv() {
                    copy_messages.push(msg);
                }

                // Verify StartRequest was received.
                let has_start = copy_messages.iter().any(|m| {
                    matches!(
                        &m.request,
                        Some(crate::protocol::rpc::copy_request::Request::Start(_))
                    )
                });
                prop_assert!(has_start, "mock server should receive CopyStartRequest");

                // EXPECTED BEHAVIOR ASSERTION (will FAIL on unfixed code):
                // After the fix, CopyRequest oneof should have a data_chunk variant,
                // and RhopdConnection::copy should stream file data as CopyDataChunk
                // messages before sending the EOF chunk.
                //
                // On UNFIXED code: the proto has no data_chunk variant in CopyRequest,
                // and even if it did, RhopdConnection::copy never reads the local file
                // or streams any data. This assertion FAILS → confirms bug.
                //
                // We check: after StartRequest, at least one additional message was
                // sent (the data chunk). On unfixed code, only StartRequest is sent.
                let total_messages = copy_messages.len();
                let messages_after_start = total_messages.saturating_sub(1);

                prop_assert!(
                    messages_after_start > 0,
                    "BUG CONFIRMED: RhopdConnection::copy only sends CopyStartRequest \
                     and never streams file data. Mock received {} total messages \
                     (expected StartRequest + ≥1 DataChunk), local file had {} bytes.",
                    total_messages,
                    file_data.len()
                );

                // Cleanup
                let _ = tokio::fs::remove_file(&temp_file).await;
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 5, .. ProptestConfig::default() })]

        /// **Validates: Requirements 1.4, 2.4**
        ///
        /// Expected Behavior: RhopdConnection::copy download receives
        /// CopyDataChunk messages and writes the data to the local file.
        ///
        /// With the fix in place, CopyResponse has a DataChunk variant.
        /// The mock sends: DataChunk(data, eof=false) + DataChunk([], eof=true)
        /// + Complete. RhopdConnection::copy must write the data to local_path.
        ///
        /// EXPECTED OUTCOME: PASSES on fixed code (confirms bug is fixed).
        #[test]
        fn prop_bug_rhopd_copy_download_no_data_mechanism(
            expected_content in proptest::collection::vec(any::<u8>(), 1..256usize),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let download_dest = std::env::temp_dir().join(format!(
                    "rhop-test-download-{}.bin",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .subsec_nanos()
                ));

                // Build a dedicated mock server that sends CopyDataChunk responses.
                let content_for_mock = expected_content.clone();

                struct DownloadMock { content: Vec<u8> }

                #[tonic::async_trait]
                impl crate::protocol::rpc::rhop_rpc_server::RhopRpc for DownloadMock {
                    type ExecuteStream = ReceiverStream<Result<crate::protocol::rpc::ExecuteResponse, tonic::Status>>;
                    type CopyStream = ReceiverStream<Result<crate::protocol::rpc::CopyResponse, tonic::Status>>;

                    async fn execute(&self, _: tonic::Request<tonic::Streaming<crate::protocol::rpc::ExecuteRequest>>) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
                        let (t, r) = tokio::sync::mpsc::channel(1);
                        let _ = t.send(Ok(crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::ExitStatus(crate::protocol::rpc::ExitStatus { code: 0 })) })).await;
                        Ok(tonic::Response::new(ReceiverStream::new(r)))
                    }

                    async fn copy(&self, _: tonic::Request<tonic::Streaming<crate::protocol::rpc::CopyRequest>>) -> Result<tonic::Response<Self::CopyStream>, tonic::Status> {
                        let content = self.content.clone();
                        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(4);
                        tokio::spawn(async move {
                            // Send data chunk with the content.
                            let _ = resp_tx.send(Ok(crate::protocol::rpc::CopyResponse {
                                event: Some(crate::protocol::rpc::copy_response::Event::DataChunk(
                                    crate::protocol::rpc::CopyDataChunk { data: content, eof: false },
                                )),
                            })).await;
                            // Send EOF chunk.
                            let _ = resp_tx.send(Ok(crate::protocol::rpc::CopyResponse {
                                event: Some(crate::protocol::rpc::copy_response::Event::DataChunk(
                                    crate::protocol::rpc::CopyDataChunk { data: vec![], eof: true },
                                )),
                            })).await;
                            // Send completion.
                            let _ = resp_tx.send(Ok(crate::protocol::rpc::CopyResponse {
                                event: Some(crate::protocol::rpc::copy_response::Event::Complete(
                                    crate::protocol::rpc::CopyComplete { message: String::new() },
                                )),
                            })).await;
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(resp_rx)))
                    }

                    async fn status(&self, _: tonic::Request<crate::protocol::rpc::StatusRequest>) -> Result<tonic::Response<crate::protocol::rpc::StatusResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn list_servers(&self, _: tonic::Request<crate::protocol::rpc::ServerListRequest>) -> Result<tonic::Response<crate::protocol::rpc::ServerListResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn shutdown(&self, _: tonic::Request<crate::protocol::rpc::ShutdownRequest>) -> Result<tonic::Response<crate::protocol::rpc::InfoResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn update_config(&self, _: tonic::Request<crate::protocol::rpc::UpdateConfigRequest>) -> Result<tonic::Response<crate::protocol::rpc::UpdateConfigResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn list_gateways(&self, _: tonic::Request<crate::protocol::rpc::ListGatewaysRequest>) -> Result<tonic::Response<crate::protocol::rpc::ListGatewaysResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                }

                let server = DownloadMock { content: content_for_mock };
                let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUF);
                tokio::spawn(async move {
                    tonic::transport::Server::builder()
                        .add_service(crate::protocol::rpc::rhop_rpc_server::RhopRpcServer::new(server))
                        .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
                        .await.ok();
                });
                let slot: std::sync::Mutex<Option<_>> = std::sync::Mutex::new(Some(client_io));
                let channel = tonic::transport::Endpoint::from_static("http://[::]:50051")
                    .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                        let s = slot.lock().unwrap().take().unwrap();
                        async move { Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s)) }
                    })).await.expect("connect");
                let client = crate::protocol::rpc::rhop_rpc_client::RhopRpcClient::new(channel);

                let mut conn = RhopdConnection::new(client, "test-target".to_string());

                let spec = crate::types::CopySpec {
                    local_path: download_dest.display().to_string(),
                    remote_path: "/tmp/remote-source.bin".to_string(),
                    direction: crate::types::CopyDirection::Download,
                    recursive: false,
                    relay_upload_rx: None,
                    relay_download_tx: None,
                };

                let result = conn.copy(spec).await;
                assert!(result.is_ok(), "copy should complete: {:?}", result);

                // Verify the downloaded file contains the expected content.
                let written_bytes = tokio::fs::read(&download_dest).await.unwrap_or_default();
                prop_assert_eq!(
                    written_bytes,
                    expected_content,
                    "downloaded file must contain the exact bytes sent by mock server"
                );

                // Cleanup
                let _ = tokio::fs::remove_file(&download_dest).await;
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property 2: Preservation tests
    //
    // These tests verify baseline behavior on UNFIXED code and MUST PASS.
    //
    // **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig { cases: 20, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.1**
        ///
        /// Preservation: exec with no stdin_rx (non-stdin path) correctly
        /// forwards stdout, stderr, and exit code from the mock gRPC server.
        ///
        /// Mock server emits: Stdout chunk -> Stderr chunk -> ExitStatus.
        /// RhopdConnection must forward ALL events to the ServerEvent channel.
        ///
        /// EXPECTED OUTCOME: PASSES on unfixed code.
        #[test]
        fn prop_preservation_non_stdin_exec_forwards_stdout_stderr_exit(
            stdout_data in proptest::collection::vec(any::<u8>(), 1..64usize),
            stderr_data in proptest::collection::vec(any::<u8>(), 1..32usize),
            exit_code in 0i32..=127i32,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Build a mock that emits scripted events.
                let captured_stdout = stdout_data.clone();
                let captured_stderr = stderr_data.clone();
                let captured_exit = exit_code;

                struct StdoutErrMock { stdout: Vec<u8>, stderr: Vec<u8>, exit: i32 }

                #[tonic::async_trait]
                impl crate::protocol::rpc::rhop_rpc_server::RhopRpc for StdoutErrMock {
                    type ExecuteStream = ReceiverStream<Result<crate::protocol::rpc::ExecuteResponse, tonic::Status>>;
                    type CopyStream = ReceiverStream<Result<crate::protocol::rpc::CopyResponse, tonic::Status>>;

                    async fn execute(&self, req: tonic::Request<tonic::Streaming<crate::protocol::rpc::ExecuteRequest>>) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
                        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(8);
                        let stdout = self.stdout.clone();
                        let stderr = self.stderr.clone();
                        let exit = self.exit;
                        tokio::spawn(async move {
                            // Drain incoming with short timeout.
                            let mut inbound = req.into_inner();
                            let dl = tokio::time::Instant::now() + tokio::time::Duration::from_millis(150);
                            loop { match tokio::time::timeout_at(dl, inbound.message()).await { Ok(Ok(Some(_))) => {}, _ => break } }
                            let _ = resp_tx.send(Ok(crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::Stdout(crate::protocol::rpc::OutputChunk { data: stdout })) })).await;
                            let _ = resp_tx.send(Ok(crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::Stderr(crate::protocol::rpc::OutputChunk { data: stderr })) })).await;
                            let _ = resp_tx.send(Ok(crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::ExitStatus(crate::protocol::rpc::ExitStatus { code: exit })) })).await;
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(resp_rx)))
                    }
                    async fn copy(&self, _: tonic::Request<tonic::Streaming<crate::protocol::rpc::CopyRequest>>) -> Result<tonic::Response<Self::CopyStream>, tonic::Status> {
                        let (t, r) = tokio::sync::mpsc::channel(1);
                        let _ = t.send(Ok(crate::protocol::rpc::CopyResponse { event: Some(crate::protocol::rpc::copy_response::Event::Complete(crate::protocol::rpc::CopyComplete { message: String::new() })) })).await;
                        Ok(tonic::Response::new(ReceiverStream::new(r)))
                    }
                    async fn status(&self, _: tonic::Request<crate::protocol::rpc::StatusRequest>) -> Result<tonic::Response<crate::protocol::rpc::StatusResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn list_servers(&self, _: tonic::Request<crate::protocol::rpc::ServerListRequest>) -> Result<tonic::Response<crate::protocol::rpc::ServerListResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn shutdown(&self, _: tonic::Request<crate::protocol::rpc::ShutdownRequest>) -> Result<tonic::Response<crate::protocol::rpc::InfoResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn update_config(&self, _: tonic::Request<crate::protocol::rpc::UpdateConfigRequest>) -> Result<tonic::Response<crate::protocol::rpc::UpdateConfigResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn list_gateways(&self, _: tonic::Request<crate::protocol::rpc::ListGatewaysRequest>) -> Result<tonic::Response<crate::protocol::rpc::ListGatewaysResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                }

                let server = StdoutErrMock { stdout: captured_stdout.clone(), stderr: captured_stderr.clone(), exit: captured_exit };
                let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUF);
                tokio::spawn(async move {
                    tonic::transport::Server::builder()
                        .add_service(crate::protocol::rpc::rhop_rpc_server::RhopRpcServer::new(server))
                        .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
                        .await.ok();
                });
                let slot: std::sync::Mutex<Option<_>> = std::sync::Mutex::new(Some(client_io));
                let channel = tonic::transport::Endpoint::from_static("http://[::]:50051")
                    .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                        let s = slot.lock().unwrap().take().unwrap();
                        async move { Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s)) }
                    })).await.expect("connect");
                let client = crate::protocol::rpc::rhop_rpc_client::RhopRpcClient::new(channel);

                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ServerEvent>();
                let mut request = ExecRequest { sender, ..make_exec_request_no_stdin(vec!["ls".to_string()]) };
                let mut conn = RhopdConnection::new(client, "tgt".to_string());
                let ret_exit = conn.exec(&mut request).await.expect("exec must complete");
                prop_assert_eq!(ret_exit, exit_code, "exit code forwarded correctly");

                // Drain events.
                let mut got_stdout: Option<Vec<u8>> = None;
                let mut got_stderr: Option<Vec<u8>> = None;
                let dl = tokio::time::Instant::now() + tokio::time::Duration::from_millis(200);
                loop {
                    match tokio::time::timeout_at(dl, receiver.recv()).await {
                        Ok(Some(ServerEvent::Stdout { data })) => got_stdout = Some(data),
                        Ok(Some(ServerEvent::Stderr { data })) => got_stderr = Some(data),
                        _ => break,
                    }
                }
                prop_assert_eq!(got_stdout.as_ref(), Some(&stdout_data), "stdout forwarded byte-identical");
                prop_assert_eq!(got_stderr.as_ref(), Some(&stderr_data), "stderr forwarded byte-identical");
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 20, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.6**
        ///
        /// Preservation: ReviewResult, ConfirmRequired, and AuthPrompt events
        /// are forwarded correctly by RhopdConnection::exec.
        ///
        /// Mock sends: ReviewResult -> ConfirmRequired -> AuthPrompt -> ExitStatus.
        /// All must arrive in the ServerEvent channel.
        ///
        /// EXPECTED OUTCOME: PASSES on unfixed code.
        #[test]
        fn prop_preservation_control_events_forwarded(
            exit_code in 0i32..=127i32,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                struct ControlEventsMock { exit: i32 }

                #[tonic::async_trait]
                impl crate::protocol::rpc::rhop_rpc_server::RhopRpc for ControlEventsMock {
                    type ExecuteStream = ReceiverStream<Result<crate::protocol::rpc::ExecuteResponse, tonic::Status>>;
                    type CopyStream = ReceiverStream<Result<crate::protocol::rpc::CopyResponse, tonic::Status>>;

                    async fn execute(&self, _: tonic::Request<tonic::Streaming<crate::protocol::rpc::ExecuteRequest>>) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
                        let exit = self.exit;
                        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(16);
                        tokio::spawn(async move {
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                            let evs: Vec<crate::protocol::rpc::ExecuteResponse> = vec![
                                crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::ReviewResult(crate::protocol::rpc::ReviewResult { execution_id: "00000000-0000-0000-0000-000000000001".to_string(), risk_level: "safe".to_string(), action: "allow".to_string(), reason: "test".to_string(), matched_whitelist_reason: String::new() })) },
                                crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::ConfirmRequired(crate::protocol::rpc::ConfirmRequired { execution_id: "00000000-0000-0000-0000-000000000001".to_string(), reason: "r".to_string() })) },
                                crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::AuthPrompt(crate::protocol::rpc::AuthPrompt { prompt_id: "p1".to_string(), target_label: "t".to_string(), kind: "password".to_string(), secret: true, message: "pw:".to_string() })) },
                                crate::protocol::rpc::ExecuteResponse { event: Some(crate::protocol::rpc::execute_response::Event::ExitStatus(crate::protocol::rpc::ExitStatus { code: exit })) },
                            ];
                            for e in evs { if resp_tx.send(Ok(e)).await.is_err() { break; } }
                        });
                        Ok(tonic::Response::new(ReceiverStream::new(resp_rx)))
                    }
                    async fn copy(&self, _: tonic::Request<tonic::Streaming<crate::protocol::rpc::CopyRequest>>) -> Result<tonic::Response<Self::CopyStream>, tonic::Status> {
                        let (t, r) = tokio::sync::mpsc::channel(1);
                        let _ = t.send(Ok(crate::protocol::rpc::CopyResponse { event: Some(crate::protocol::rpc::copy_response::Event::Complete(crate::protocol::rpc::CopyComplete { message: String::new() })) })).await;
                        Ok(tonic::Response::new(ReceiverStream::new(r)))
                    }
                    async fn status(&self, _: tonic::Request<crate::protocol::rpc::StatusRequest>) -> Result<tonic::Response<crate::protocol::rpc::StatusResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn list_servers(&self, _: tonic::Request<crate::protocol::rpc::ServerListRequest>) -> Result<tonic::Response<crate::protocol::rpc::ServerListResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn shutdown(&self, _: tonic::Request<crate::protocol::rpc::ShutdownRequest>) -> Result<tonic::Response<crate::protocol::rpc::InfoResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn update_config(&self, _: tonic::Request<crate::protocol::rpc::UpdateConfigRequest>) -> Result<tonic::Response<crate::protocol::rpc::UpdateConfigResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                    async fn list_gateways(&self, _: tonic::Request<crate::protocol::rpc::ListGatewaysRequest>) -> Result<tonic::Response<crate::protocol::rpc::ListGatewaysResponse>, tonic::Status> { Ok(tonic::Response::new(Default::default())) }
                }

                let server = ControlEventsMock { exit: exit_code };
                let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUF);
                tokio::spawn(async move {
                    tonic::transport::Server::builder()
                        .add_service(crate::protocol::rpc::rhop_rpc_server::RhopRpcServer::new(server))
                        .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
                        .await.ok();
                });
                let slot: std::sync::Mutex<Option<_>> = std::sync::Mutex::new(Some(client_io));
                let channel = tonic::transport::Endpoint::from_static("http://[::]:50051")
                    .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                        let s = slot.lock().unwrap().take().unwrap();
                        async move { Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s)) }
                    })).await.expect("connect");
                let client = crate::protocol::rpc::rhop_rpc_client::RhopRpcClient::new(channel);

                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ServerEvent>();
                let mut request = ExecRequest { sender, ..make_exec_request_no_stdin(vec!["ls".to_string()]) };
                let mut conn = RhopdConnection::new(client, "tgt".to_string());
                let ret_exit = conn.exec(&mut request).await.expect("exec must complete");
                prop_assert_eq!(ret_exit, exit_code);

                let mut has_review = false;
                let mut has_confirm = false;
                let mut has_auth = false;
                let dl = tokio::time::Instant::now() + tokio::time::Duration::from_millis(200);
                loop {
                    match tokio::time::timeout_at(dl, receiver.recv()).await {
                        Ok(Some(ServerEvent::ReviewResult { .. })) => has_review = true,
                        Ok(Some(ServerEvent::ConfirmRequired { .. })) => has_confirm = true,
                        Ok(Some(ServerEvent::AuthPrompt { .. })) => has_auth = true,
                        _ => break,
                    }
                }
                prop_assert!(has_review, "ReviewResult must be forwarded");
                prop_assert!(has_confirm, "ConfirmRequired must be forwarded");
                prop_assert!(has_auth, "AuthPrompt must be forwarded");
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 10, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.2**
        ///
        /// Preservation: exec_interactive correctly handles stdin/stdout/resize
        /// via InteractiveHandle on unfixed code.
        ///
        /// The existing mock server (start_mock_server) sends ExitStatus(42)
        /// after a 200ms collection window. We verify:
        /// - StartRequest with interactive=true is sent
        /// - StdinData from handle.stdin_tx reaches mock
        /// - WindowResize from handle.resize_tx reaches mock
        /// - The interactive session completes normally
        ///
        /// EXPECTED OUTCOME: PASSES on unfixed code.
        #[test]
        fn prop_preservation_exec_interactive_stdin_resize_forwarding(
            stdin_bytes in proptest::collection::vec(any::<u8>(), 1..32usize),
            cols in 40u32..=220u32,
            rows in 10u32..=60u32,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (client, exec_log, _copy_rx) = start_mock_server().await;

                let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<ServerEvent>();
                let request = super::InteractiveRequest {
                    argv: vec!["bash".to_string()],
                    cols,
                    rows,
                    sender,
                    shell: String::new(),
                };

                let mut conn = RhopdConnection::new(client, "tgt".to_string());
                let handle = conn.exec_interactive(&request).await
                    .expect("exec_interactive must succeed");

                // Forward stdin through the handle.
                handle.stdin_tx.send(stdin_bytes.clone()).await
                    .expect("stdin_tx send must succeed");

                // Forward a resize event.
                handle.resize_tx.send((cols + 1, rows + 1)).await
                    .expect("resize_tx send must succeed");

                // Wait for the session to complete (mock sends ExitStatus(42) after 200ms).
                let result = tokio::time::timeout(
                    tokio::time::Duration::from_millis(700),
                    handle.exit_rx,
                ).await;
                prop_assert!(result.is_ok(), "interactive session must complete in time");

                // Verify mock received interactive StartRequest.
                let log = exec_log.lock().await;
                let has_interactive_start = log.messages.iter().any(|m| {
                    matches!(&m.request, Some(crate::protocol::rpc::execute_request::Request::Start(s)) if s.interactive)
                });
                prop_assert!(has_interactive_start, "StartRequest with interactive=true must be sent");

                // Verify mock received StdinData.
                let has_stdin = log.messages.iter().any(|m| {
                    matches!(&m.request, Some(crate::protocol::rpc::execute_request::Request::StdinData(_)))
                });
                prop_assert!(has_stdin, "StdinData must reach mock from handle.stdin_tx");

                // Verify mock received WindowResize.
                let has_resize = log.messages.iter().any(|m| {
                    matches!(&m.request, Some(crate::protocol::rpc::execute_request::Request::WindowResize(_)))
                });
                prop_assert!(has_resize, "WindowResize must reach mock from handle.resize_tx");

                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 10, .. ProptestConfig::default() })]

        /// **Validates: Requirements 3.3, 3.4**
        ///
        /// Preservation: RhopdConnection::copy sends CopyStartRequest with
        /// correct target/remote_path/direction, and returns Ok when mock
        /// responds with CopyComplete.
        ///
        /// EXPECTED OUTCOME: PASSES on unfixed code.
        #[test]
        fn prop_preservation_copy_start_request_fields_correct(
            remote_path in "[a-z]{3,8}",
            upload in any::<bool>(),
            recursive in any::<bool>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (client, _exec_log, mut copy_rx) = start_mock_server().await;

                let mut conn = RhopdConnection::new(client, "my-target".to_string());
                let direction = if upload {
                    crate::types::CopyDirection::Upload
                } else {
                    crate::types::CopyDirection::Download
                };
                let spec = crate::types::CopySpec {
                    local_path: "/tmp/local.bin".to_string(),
                    remote_path: format!("/remote/{}", remote_path),
                    direction,
                    recursive,
                    relay_upload_rx: None,
                    relay_download_tx: None,
                };

                let result = conn.copy(spec).await;
                prop_assert!(result.is_ok(), "copy must return Ok: {:?}", result.err());

                // Collect copy messages from mock.
                let mut msgs = Vec::new();
                while let Ok(m) = copy_rx.try_recv() { msgs.push(m); }

                // Must have exactly one CopyStartRequest.
                let start_msgs: Vec<_> = msgs.iter().filter_map(|m| {
                    if let Some(crate::protocol::rpc::copy_request::Request::Start(s)) = &m.request {
                        Some(s)
                    } else { None }
                }).collect();

                prop_assert!(!start_msgs.is_empty(), "must send CopyStartRequest");
                let start = start_msgs[0];
                prop_assert_eq!(&start.target, "my-target", "target field correct");
                prop_assert_eq!(&start.remote_path, &format!("/remote/{}", remote_path), "remote_path correct");
                prop_assert_eq!(start.recursive, recursive, "recursive field correct");

                // local_path must be empty in the rhopd copy path.
                prop_assert!(start.local_path.is_empty(), "local_path must be empty for rhopd hops");

                Ok(())
            })?;
        }
    }

} // end #[cfg(test)] mod tests
