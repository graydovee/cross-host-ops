//! Property test: exactly one rhop-rpc subsystem per RhopdJumpHost.
//!
//! Verifies that regardless of how many RPCs (ListServers, Execute, Copy) are
//! issued against a single `RhopdJumpHost` instance, the underlying tonic
//! connector is invoked exactly once — proving that a single subsystem byte
//! stream backs all RPC traffic for the lifetime of the host.

// Feature: rhopd-connect-and-server-list, Property 4: exactly one rhop-rpc subsystem per RhopdJumpHost

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use proptest::prelude::*;
use tokio::sync::mpsc;
use tonic::transport::{Endpoint, Server, Uri};
use tower::service_fn;
use hyper_util::rt::TokioIo;

use rhop::config::AppConfig;
use rhop::connection::{CopyDirection, CopySpec};
use rhop::jump::rhopd::RhopdJumpHost;
use rhop::jump::JumpHost;
use rhop::protocol::rpc;
use rhop::protocol::rpc::rhop_rpc_server::{RhopRpc, RhopRpcServer};

/// Buffer size for the in-process duplex channel (1 MB).
const DUPLEX_BUFFER_SIZE: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// Minimal stub RhopRpc service
// ---------------------------------------------------------------------------

/// A minimal gRPC service that implements ListServers, Execute, and Copy with
/// trivial success responses. No recording is needed — we only care about the
/// connector invocation count.
struct StubRhopRpcService;

#[async_trait::async_trait]
impl RhopRpc for StubRhopRpcService {
    type ExecuteStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<rpc::ExecuteResponse, tonic::Status>> + Send>,
    >;

    async fn execute(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::ExecuteRequest>>,
    ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
        // Read only the first message (StartRequest); do NOT drain the full
        // stream because the client keeps the sender alive while reading
        // responses, which would deadlock.
        let mut stream = request.into_inner();
        let _ = stream.message().await;

        let response_stream = tokio_stream::once(Ok(rpc::ExecuteResponse {
            event: Some(rpc::execute_response::Event::ExitStatus(rpc::ExitStatus {
                code: 0,
            })),
        }));
        Ok(tonic::Response::new(Box::pin(response_stream)))
    }

    type CopyStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<rpc::CopyResponse, tonic::Status>> + Send>,
    >;

    async fn copy(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::CopyRequest>>,
    ) -> Result<tonic::Response<Self::CopyStream>, tonic::Status> {
        // Read only the first message (CopyStartRequest); do NOT drain the
        // full stream to avoid deadlock with the client.
        let mut stream = request.into_inner();
        let _ = stream.message().await;

        let response_stream = tokio_stream::once(Ok(rpc::CopyResponse {
            event: Some(rpc::copy_response::Event::Complete(rpc::CopyComplete {
                message: "done".to_string(),
            })),
        }));
        Ok(tonic::Response::new(Box::pin(response_stream)))
    }

    async fn status(
        &self,
        _request: tonic::Request<rpc::StatusRequest>,
    ) -> Result<tonic::Response<rpc::StatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(rpc::StatusResponse::default()))
    }

    async fn list_servers(
        &self,
        _request: tonic::Request<rpc::ServerListRequest>,
    ) -> Result<tonic::Response<rpc::ServerListResponse>, tonic::Status> {
        Ok(tonic::Response::new(rpc::ServerListResponse {
            server_config_path: String::new(),
            servers: vec![rpc::ServerEntry {
                alias: "stub-target".to_string(),
                host: "127.0.0.1".to_string(),
                port: 22,
                user: "testuser".to_string(),
                auth_kind: "key".to_string(),
            }],
            merged: None,
        }))
    }

    async fn shutdown(
        &self,
        _request: tonic::Request<rpc::ShutdownRequest>,
    ) -> Result<tonic::Response<rpc::InfoResponse>, tonic::Status> {
        Ok(tonic::Response::new(rpc::InfoResponse {
            message: "ok".to_string(),
        }))
    }

    async fn update_config(
        &self,
        _request: tonic::Request<rpc::UpdateConfigRequest>,
    ) -> Result<tonic::Response<rpc::UpdateConfigResponse>, tonic::Status> {
        Ok(tonic::Response::new(rpc::UpdateConfigResponse {
            success: false,
            message: "not implemented".to_string(),
        }))
    }

    async fn list_jump_hosts(
        &self,
        _request: tonic::Request<rpc::ListJumpHostsRequest>,
    ) -> Result<tonic::Response<rpc::ListJumpHostsResponse>, tonic::Status> {
        Ok(tonic::Response::new(rpc::ListJumpHostsResponse {
            jump_hosts: vec![],
        }))
    }
}

// ---------------------------------------------------------------------------
// RPC operation enum for proptest generation
// ---------------------------------------------------------------------------

/// The three RPC operations available on a `RhopdJumpHost`.
#[derive(Debug, Clone)]
enum RpcOp {
    ListServers,
    Execute { argv: Vec<String> },
    Copy { remote_path: String, direction: CopyDirection },
}

/// Strategy producing a random RPC operation.
fn arb_rpc_op() -> impl Strategy<Value = RpcOp> {
    prop_oneof![
        Just(RpcOp::ListServers),
        prop::collection::vec("[a-z]{1,8}", 1..=4)
            .prop_map(|argv| RpcOp::Execute { argv }),
        ("[a-z/]{1,20}", prop_oneof![Just(CopyDirection::Upload), Just(CopyDirection::Download)])
            .prop_map(|(remote_path, direction)| RpcOp::Copy { remote_path, direction }),
    ]
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    // Feature: rhopd-connect-and-server-list, Property 4: exactly one rhop-rpc subsystem per RhopdJumpHost
    /// **Validates: Requirements 3.6, 8.4**
    ///
    /// For any sequence of 1..=32 mixed RPCs (ListServers, Execute, Copy)
    /// issued against a single `RhopdJumpHost` instance, the tonic connector
    /// closure is invoked exactly once. This proves that a single `rhop-rpc`
    /// subsystem byte stream backs all RPC traffic for the lifetime of the
    /// host, and every RPC succeeds.
    #[test]
    fn prop_single_subsystem_per_host(
        ops in prop::collection::vec(arb_rpc_op(), 1..=32),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Set up a duplex pair: one end goes to the tonic server, the
            // other to the client connector.
            let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

            // Spawn the stub gRPC server on the server end of the duplex.
            tokio::spawn(async move {
                Server::builder()
                    .add_service(RhopRpcServer::new(StubRhopRpcService))
                    .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
                    .await
                    .expect("stub gRPC server failed");
            });

            // Build the tonic Channel with a counting connector. The
            // connector closure increments `connect_count` each time tonic
            // asks for a new transport connection.
            let connect_count = Arc::new(AtomicUsize::new(0));
            let counter = connect_count.clone();
            let io_slot = std::sync::Mutex::new(Some(client_io));

            let channel = Endpoint::from_static("http://[::]:50051")
                .connect_with_connector(service_fn(move |_: Uri| {
                    let count = counter.clone();
                    let slot = io_slot.lock().unwrap().take();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        let stream = slot.ok_or_else(|| {
                            std::io::Error::other(
                                "connector invoked more than once — subsystem already consumed",
                            )
                        })?;
                        Ok::<_, std::io::Error>(TokioIo::new(stream))
                    }
                }))
                .await
                .expect("failed to connect gRPC client via duplex");

            // Build the RhopdJumpHost from the pre-connected channel.
            let client = rpc::rhop_rpc_client::RhopRpcClient::new(channel);
            let mut jump_host = RhopdJumpHost::from_parts(
                "prop-test-alias".to_string(),
                "test@localhost:2222".to_string(),
                None,
                client,
            );

            let config = AppConfig::default();

            // Execute all generated RPC operations sequentially.
            for (i, op) in ops.iter().enumerate() {
                match op {
                    RpcOp::ListServers => {
                        let result = jump_host.list_servers(&config).await;
                        prop_assert!(
                            result.is_ok(),
                            "ListServers #{} failed: {:?}",
                            i,
                            result.err()
                        );
                    }
                    RpcOp::Execute { argv } => {
                        let (sender, _rx) = mpsc::unbounded_channel();
                        let result = jump_host.exec(argv, &sender, &config, config.ssh.pty).await;
                        prop_assert!(
                            result.is_ok(),
                            "Execute #{} failed: {:?}",
                            i,
                            result.err()
                        );
                    }
                    RpcOp::Copy { remote_path, direction } => {
                        let spec = CopySpec {
                            direction: direction.clone(),
                            local_path: "/tmp/test".to_string(),
                            remote_path: remote_path.clone(),
                            recursive: false,
                        };
                        let result = jump_host.copy(&spec, &config).await;
                        prop_assert!(
                            result.is_ok(),
                            "Copy #{} failed: {:?}",
                            i,
                            result.err()
                        );
                    }
                }
            }

            // THE critical assertion: the connector was invoked exactly once,
            // proving that a single subsystem byte stream served all RPCs.
            let count = connect_count.load(Ordering::SeqCst);
            prop_assert_eq!(
                count, 1,
                "connector should be invoked exactly once, but was invoked {} times",
                count
            );

            Ok(())
        })?;
    }
}
