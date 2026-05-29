//! Property-based test for wire-level identity of RhopdJumpHost::exec and
//! RhopdJumpHost::list_servers.
//!
//! Feature: rhopd-jumpserver-architecture, Property 11: Wire-level identity for RhopdJumpHost::exec and RhopdJumpHost::list_servers
#![allow(clippy::collapsible_if)]

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use proptest::prelude::*;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tower::service_fn;
use hyper_util::rt::TokioIo;

use rhop::config::AppConfig;
use rhop::jump::rhopd::RhopdJumpHost;
use rhop::jump::JumpHost;
use rhop::protocol::rpc;
use rhop::protocol::rpc::rhop_rpc_server::{RhopRpc, RhopRpcServer};

/// Buffer size for the in-process duplex channel.
const DUPLEX_BUFFER_SIZE: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// Mock RhopRpc service
// ---------------------------------------------------------------------------

/// Records the StartRequest received during Execute and returns a pre-configured
/// ListServers response.
struct MockRhopService {
    /// Stores the StartRequest received from the first ExecuteRequest message.
    recorded_start: Arc<Mutex<Option<rpc::StartRequest>>>,
    /// The server entries to return from ListServers.
    server_entries: Vec<rpc::ServerEntry>,
}

#[async_trait::async_trait]
impl RhopRpc for MockRhopService {
    type ExecuteStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<rpc::ExecuteResponse, tonic::Status>> + Send>,
    >;

    async fn execute(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::ExecuteRequest>>,
    ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
        let mut stream = request.into_inner();

        // Read the first message and record the StartRequest
        if let Some(msg) = stream.message().await.map_err(|e| {
            tonic::Status::internal(format!("failed to read execute request: {}", e))
        })? {
            if let Some(rpc::execute_request::Request::Start(start)) = msg.request {
                let mut recorded = self.recorded_start.lock().unwrap();
                *recorded = Some(start);
            }
        }

        // Return ExitStatus(0) immediately
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
        _request: tonic::Request<tonic::Streaming<rpc::CopyRequest>>,
    ) -> Result<tonic::Response<Self::CopyStream>, tonic::Status> {
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
            servers: self.server_entries.clone(),
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
// Helpers
// ---------------------------------------------------------------------------

/// Spawn a mock gRPC server and return a connected client channel.
async fn spawn_mock_server(
    recorded_start: Arc<Mutex<Option<rpc::StartRequest>>>,
    server_entries: Vec<rpc::ServerEntry>,
) -> rpc::rhop_rpc_client::RhopRpcClient<Channel> {
    let service = MockRhopService {
        recorded_start,
        server_entries,
    };

    let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

    tokio::spawn(async move {
        Server::builder()
            .add_service(RhopRpcServer::new(service))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
            .expect("mock gRPC server failed");
    });

    // Connect the client via the duplex stream
    let io = std::sync::Mutex::new(Some(client_io));
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(service_fn(move |_: Uri| {
            let stream = io
                .lock()
                .unwrap()
                .take()
                .expect("duplex stream already consumed");
            async move { Ok::<_, std::io::Error>(TokioIo::new(stream)) }
        }))
        .await
        .expect("failed to connect gRPC client via duplex");

    rpc::rhop_rpc_client::RhopRpcClient::new(channel)
}

/// Build a `RhopdJumpHost` from a pre-connected client.
fn build_rhopd_jump_host(
    alias: String,
    client: rpc::rhop_rpc_client::RhopRpcClient<Channel>,
) -> RhopdJumpHost {
    // In-process tests don't have a real SSH session backing the client, so
    // the optional transport slot is left empty. Production paths always
    // populate it via `RhopdJumpHost::connect`.
    RhopdJumpHost::from_parts(alias, "test@localhost:22".to_string(), None, client)
}

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy for generating valid target alias strings (non-empty, alphanumeric + dashes).
fn arb_target_alias() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_\\-]{0,30}"
}

/// Strategy for generating argv vectors (0 to 10 non-empty strings).
fn arb_argv() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-zA-Z0-9_/\\-\\.]{1,50}", 0..10)
}

/// Strategy for generating a single ServerEntry (rpc type).
fn arb_rpc_server_entry() -> impl Strategy<Value = rpc::ServerEntry> {
    (
        "[a-zA-Z][a-zA-Z0-9_\\-]{0,20}",       // alias
        "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}", // host (IP-like)
        1u32..=65535u32,                         // port
        "[a-zA-Z][a-zA-Z0-9]{0,15}",            // user
        prop_oneof![Just("key".to_string()), Just("password".to_string())], // auth_kind
    )
        .prop_map(|(alias, host, port, user, auth_kind)| rpc::ServerEntry {
            alias,
            host,
            port,
            user,
            auth_kind,
        })
}

/// Strategy for generating a vector of ServerEntry (0 to 10 entries).
fn arb_server_entries() -> impl Strategy<Value = Vec<rpc::ServerEntry>> {
    prop::collection::vec(arb_rpc_server_entry(), 0..10)
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

// Feature: rhopd-jumpserver-architecture, Property 11: Wire-level identity for RhopdJumpHost::exec and RhopdJumpHost::list_servers

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 4.6, 4.8**
    ///
    /// For arbitrary (end_target_alias e, argv v), the first ExecuteRequest the
    /// mocked remote receives equals StartRequest { target: e, argv: v } byte-for-byte.
    #[test]
    fn prop_exec_wire_identity(
        target_alias in arb_target_alias(),
        argv in arb_argv(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let recorded_start = Arc::new(Mutex::new(None));
            let client = spawn_mock_server(recorded_start.clone(), vec![]).await;

            let mut jump_host = build_rhopd_jump_host(target_alias.clone(), client);

            let config = AppConfig::default();
            let (sender, _receiver) = mpsc::unbounded_channel();

            // Call exec with the target alias and argv
            let result = jump_host.exec(&argv, &sender, &config, config.ssh.pty).await;
            prop_assert!(result.is_ok(), "exec should succeed, got: {:?}", result.err());
            prop_assert_eq!(result.unwrap(), 0);

            // Verify the recorded StartRequest matches byte-for-byte
            let recorded = recorded_start.lock().unwrap();
            let start_req = recorded.as_ref().expect("StartRequest should have been recorded");
            prop_assert_eq!(&start_req.target, &target_alias);
            prop_assert_eq!(&start_req.argv, &argv);

            Ok(())
        })?;
    }

    /// **Validates: Requirements 4.6, 4.8**
    ///
    /// For arbitrary Vec<ServerEntry> R returned by the mocked remote's ListServers,
    /// RhopdJumpHost::list_servers returns a vector equal to R.
    #[test]
    fn prop_list_servers_wire_identity(
        entries in arb_server_entries(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let recorded_start = Arc::new(Mutex::new(None));
            let client = spawn_mock_server(recorded_start.clone(), entries.clone()).await;

            let mut jump_host = build_rhopd_jump_host("test-jump".to_string(), client);

            let config = AppConfig::default();

            // Call list_servers
            let result = jump_host.list_servers(&config).await;
            prop_assert!(result.is_ok(), "list_servers should succeed, got: {:?}", result.err());

            let returned_entries = result.unwrap();

            // Verify the returned entries match the mock's entries
            prop_assert_eq!(returned_entries.len(), entries.len());

            for (returned, expected) in returned_entries.iter().zip(entries.iter()) {
                prop_assert_eq!(&returned.alias, &expected.alias);
                prop_assert_eq!(&returned.host, &expected.host);
                prop_assert_eq!(returned.port, expected.port as u16);
                prop_assert_eq!(&returned.user, &expected.user);

                // Verify auth kind mapping
                let expected_auth_kind = if expected.auth_kind == "password" {
                    "password"
                } else {
                    "key"
                };
                prop_assert_eq!(returned.auth_kind(), expected_auth_kind);
            }

            Ok(())
        })?;
    }
}
