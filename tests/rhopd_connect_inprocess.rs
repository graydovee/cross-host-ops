//! Integration tests: end-to-end connection path for `RhopdJumpHost`.
//!
//! - `connect_invalid_address_propagates`: verifies that an invalid address
//!   (whitespace-only) causes `RhopdJumpHost::connect` to return `Err` with
//!   the original address in the error text, and no TCP connection is attempted.
//! - `connect_end_to_end_in_process`: verifies that `list_servers`, `exec`, and
//!   `copy` all succeed when driven through a duplex + tonic server stub via
//!   `RhopdJumpHost::from_parts`.

mod support;

use std::pin::Pin;

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
// Minimal stub RhopRpc service (same pattern as prop_rhopd_single_subsystem)
// ---------------------------------------------------------------------------

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
        let mut stream = request.into_inner();
        let _ = stream.message().await;

        let response_stream = tokio_stream::once(Ok(rpc::ExecuteResponse {
            event: Some(rpc::execute_response::Event::ExitStatus(rpc::ExitStatus {
                code: 42,
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
            server_config_path: "/tmp/server.toml".to_string(),
            servers: vec![
                rpc::ServerEntry {
                    alias: "web01".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 22,
                    user: "deploy".to_string(),
                    auth_kind: "key".to_string(),
                },
                rpc::ServerEntry {
                    alias: "db01".to_string(),
                    host: "10.0.0.2".to_string(),
                    port: 5432,
                    user: "postgres".to_string(),
                    auth_kind: "key".to_string(),
                },
            ],
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
// Test: connect_invalid_address_propagates
// ---------------------------------------------------------------------------

/// Validates: Requirements 1.2, 5.1, 5.2
///
/// Calling `RhopdJumpHost::connect` with a whitespace-only address ("  ") must
/// return `Err`. The error text must contain the original address string "  ".
/// No TCP connection is attempted because `parse_remote_target` fails before
/// any network resource is created (echoes Property 1).
#[tokio::test]
async fn connect_invalid_address_propagates() {
    let result = RhopdJumpHost::connect(
        "alias".into(),
        "  ".into(),
        "".into(),
        "".into(),
        "target".into(),
    )
    .await;

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("connect with whitespace address must fail"),
    };
    let display = format!("{err}");

    // The error text must contain the original address (debug-quoted form
    // includes the whitespace).
    assert!(
        display.contains("  "),
        "error display must contain original address '  ', got: {display}"
    );
}

// ---------------------------------------------------------------------------
// Test: connect_end_to_end_in_process
// ---------------------------------------------------------------------------

/// Validates: Requirements 5.1, 5.2, 8.1, 8.2, 8.3
///
/// Constructs a `RhopdJumpHost` via `from_parts` backed by a duplex + tonic
/// server stub, then verifies that `list_servers`, `exec`, and `copy` all
/// complete successfully through the in-process gRPC channel.
#[tokio::test]
async fn connect_end_to_end_in_process() {
    // Set up a duplex pair: one end goes to the tonic server, the other to
    // the client connector.
    let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

    // Spawn the stub gRPC server on the server end of the duplex.
    tokio::spawn(async move {
        Server::builder()
            .add_service(RhopRpcServer::new(StubRhopRpcService))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
            .expect("stub gRPC server failed");
    });

    // Build the tonic Channel through the duplex stream.
    let io_slot = std::sync::Mutex::new(Some(client_io));
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(service_fn(move |_: Uri| {
            let stream = io_slot
                .lock()
                .unwrap()
                .take()
                .expect("duplex stream already consumed");
            async move { Ok::<_, std::io::Error>(TokioIo::new(stream)) }
        }))
        .await
        .expect("failed to connect gRPC client via duplex");

    // Build the RhopdJumpHost from the pre-connected channel (no real SSH).
    let client = rpc::rhop_rpc_client::RhopRpcClient::new(channel);
    let mut jump_host = RhopdJumpHost::from_parts(
        "test-rhopd".to_string(),
        "testuser@localhost:2222".to_string(),
        None,
        client,
    );

    let config = AppConfig::default();

    // --- list_servers ---
    let servers = jump_host
        .list_servers(&config)
        .await
        .expect("list_servers should succeed");
    assert_eq!(servers.len(), 2, "stub returns 2 server entries");
    assert_eq!(servers[0].alias, "web01");
    assert_eq!(servers[1].alias, "db01");

    // --- exec ---
    let (sender, mut rx) = mpsc::unbounded_channel();
    let exit_code = jump_host
        .exec(&["echo".to_string(), "hello".to_string()], &sender, &config, config.ssh.pty, 80, 24)
        .await
        .expect("exec should succeed");
    assert_eq!(exit_code, 42, "stub returns exit code 42");
    // Drain any events (none expected from stub, but ensure no panic).
    rx.close();

    // --- copy ---
    let spec = CopySpec {
        direction: CopyDirection::Upload,
        local_path: "/tmp/local".to_string(),
        remote_path: "/tmp/remote".to_string(),
        recursive: false,
    };
    jump_host
        .copy(&spec, &config)
        .await
        .expect("copy should succeed");
}
