//! In-process gRPC test harness.
//!
//! Creates two `XhoRpcService` instances connected via `tokio::io::duplex` so
//! a "local daemon" client and a "remote daemon" server live in one process.
//! Exposes helpers to drive `Execute`, `Copy`, and `ListServers` against a stub
//! end target backed by a tempdir.

use std::path::{Path, PathBuf};

use xho::config::{AppConfig, DirectAuth, ServerEntry};
use xho::daemon::test_support::make_test_rpc_service;
use xho::protocol::rpc;
use xho::protocol::rpc::xho_rpc_client::XhoRpcClient;

use hyper_util::rt::TokioIo;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tower::service_fn;

use std::fs;

/// The buffer size for the in-process duplex channel (1 MB).
const DUPLEX_BUFFER_SIZE: usize = 1024 * 1024;

/// An in-process gRPC test harness that connects a client to a server over
/// `tokio::io::duplex`, backed by a tempdir for server-config files.
#[allow(dead_code)]
pub struct InProcessRpcHarness {
    /// The gRPC client connected to the "remote daemon" server.
    pub client: XhoRpcClient<Channel>,
    /// Path to the tempdir backing the server config (contains `server.toml`).
    pub tempdir: PathBuf,
    /// The config used by the remote daemon service.
    pub config: AppConfig,
}

#[allow(dead_code)]
impl InProcessRpcHarness {
    /// Create a new harness with a single stub server entry in the remote
    /// daemon's server config.
    ///
    /// The stub server entry has alias `stub-target`, host `127.0.0.1`,
    /// port 22, user `testuser`, and key-based auth.
    pub async fn new() -> Self {
        Self::with_servers(vec![ServerEntry {
            alias: "stub-target".to_string(),
            host: "127.0.0.1".to_string(),
            port: 22,
            user: "testuser".to_string(),
            auth: DirectAuth::Key {
                identity_file: "/dev/null".to_string(),
            },
        }])
        .await
    }

    /// Create a new harness with the given server entries available in the
    /// remote daemon's server config.
    pub async fn with_servers(servers: Vec<ServerEntry>) -> Self {
        // Create a tempdir for the server config
        let tempdir = std::env::temp_dir().join(format!("xho-test-{}", uuid()));
        fs::create_dir_all(&tempdir).expect("failed to create tempdir");

        // Write a server.toml with the given entries
        let server_config_path = tempdir.join("server.toml");
        let server_toml = build_server_toml(&servers);
        fs::write(&server_config_path, &server_toml).expect("failed to write server.toml");

        // Write a minimal config.toml
        let config_path = tempdir.join("config.toml");
        let mut config = AppConfig::default();
        config.ssh.server_config_path = server_config_path.display().to_string();
        // Disable local socket and remote SSH listener for tests
        config.server.local.enable = false;
        config.server.remote.enable = false;
        // Disable review for tests
        config.review.enable = false;

        // Create the gRPC service
        let service = make_test_rpc_service(config.clone(), config_path);

        // Create a duplex channel and wire up server + client
        let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

        // Spawn the server on one end of the duplex
        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
                .await
                .expect("gRPC server failed");
        });

        // Connect the client on the other end
        let client = connect_client_via_duplex(client_io).await;

        let harness_config = {
            let mut c = AppConfig::default();
            c.ssh.server_config_path = tempdir.join("server.toml").display().to_string();
            c.server.local.enable = false;
            c.server.remote.enable = false;
            c.review.enable = false;
            c
        };

        Self {
            client,
            tempdir,
            config: harness_config,
        }
    }

    /// Call `ListServers` on the remote daemon and return the server entries.
    pub async fn list_servers(&mut self) -> Vec<rpc::ServerEntry> {
        let response = self
            .client
            .list_servers(rpc::ServerListRequest {})
            .await
            .expect("ListServers RPC failed");
        response.into_inner().servers
    }

    /// Call `Execute` on the remote daemon with the given target and argv.
    /// Returns the collected response events.
    ///
    /// Note: This sends a single `StartRequest` and collects all response
    /// events. It does not handle interactive prompts (confirm/auth).
    pub async fn execute(&mut self, target: &str, argv: &[&str]) -> Vec<rpc::ExecuteResponse> {
        self.execute_with_timeout(target, argv, 0).await
    }

    /// Call `Execute` on the remote daemon with the given target, argv, and
    /// an optional timeout (in milliseconds, 0 = no timeout).
    /// Returns the collected response events.
    pub async fn execute_with_timeout(
        &mut self,
        target: &str,
        argv: &[&str],
        timeout_ms: u64,
    ) -> Vec<rpc::ExecuteResponse> {
        let start_request = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                target: target.to_string(),
                argv: argv.iter().map(|s| s.to_string()).collect(),
                timeout_ms,
                ..Default::default()
            })),
        };

        let response = self
            .client
            .execute(tokio_stream::once(start_request))
            .await
            .expect("Execute RPC failed");

        let mut stream = response.into_inner();
        let mut events = Vec::new();
        while let Some(msg) = stream
            .message()
            .await
            .expect("failed to read Execute response stream")
        {
            events.push(msg);
        }
        events
    }

    /// Call `Copy` on the remote daemon with the given parameters.
    /// Returns the collected response events.
    ///
    /// Note: This sends a single `CopyStartRequest` and collects all response
    /// events. It does not handle interactive auth prompts.
    pub async fn copy(
        &mut self,
        target: &str,
        local_path: &str,
        remote_path: &str,
        direction: rpc::CopyDirection,
        recursive: bool,
    ) -> Vec<rpc::CopyResponse> {
        let start_request = rpc::CopyRequest {
            request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
                target: target.to_string(),
                local_path: local_path.to_string(),
                remote_path: remote_path.to_string(),
                recursive,
                direction: direction as i32,
                ..Default::default()
            })),
        };

        let response = self
            .client
            .copy(tokio_stream::once(start_request))
            .await
            .expect("Copy RPC failed");

        let mut stream = response.into_inner();
        let mut events = Vec::new();
        while let Some(msg) = stream
            .message()
            .await
            .expect("failed to read Copy response stream")
        {
            events.push(msg);
        }
        events
    }

    /// Call `Status` on the remote daemon.
    pub async fn status(&mut self) -> rpc::StatusResponse {
        let response = self
            .client
            .status(rpc::StatusRequest {})
            .await
            .expect("Status RPC failed");
        response.into_inner()
    }

    /// Returns the path to the tempdir backing this harness.
    pub fn tempdir_path(&self) -> &Path {
        &self.tempdir
    }
}

impl Drop for InProcessRpcHarness {
    fn drop(&mut self) {
        // Best-effort cleanup of the tempdir
        let _ = fs::remove_dir_all(&self.tempdir);
    }
}

/// A paired harness with two daemons: a "local" client and a "remote" server,
/// both connected via duplex channels. This models the full local→remote
/// daemon topology in a single process.
#[allow(dead_code)]
pub struct PairedRpcHarness {
    /// Client connected to the "local daemon" service.
    pub local_client: XhoRpcClient<Channel>,
    /// Client connected to the "remote daemon" service (for direct testing).
    pub remote_client: XhoRpcClient<Channel>,
    /// Tempdir for the local daemon's server config.
    pub local_tempdir: PathBuf,
    /// Tempdir for the remote daemon's server config.
    pub remote_tempdir: PathBuf,
}

#[allow(dead_code)]
impl PairedRpcHarness {
    /// Create a paired harness with both local and remote daemons.
    /// The remote daemon has the given server entries; the local daemon
    /// has an empty server config.
    pub async fn new(remote_servers: Vec<ServerEntry>) -> Self {
        // Set up remote daemon
        let remote_tempdir = std::env::temp_dir().join(format!("xho-test-remote-{}", uuid()));
        fs::create_dir_all(&remote_tempdir).expect("failed to create remote tempdir");

        let remote_server_config_path = remote_tempdir.join("server.toml");
        let remote_server_toml = build_server_toml(&remote_servers);
        fs::write(&remote_server_config_path, &remote_server_toml)
            .expect("failed to write remote server.toml");

        let remote_config_path = remote_tempdir.join("config.toml");
        let mut remote_config = AppConfig::default();
        remote_config.ssh.server_config_path = remote_server_config_path.display().to_string();
        remote_config.server.local.enable = false;
        remote_config.server.remote.enable = false;
        remote_config.review.enable = false;

        let remote_service = make_test_rpc_service(remote_config, remote_config_path);

        // Set up local daemon
        let local_tempdir = std::env::temp_dir().join(format!("xho-test-local-{}", uuid()));
        fs::create_dir_all(&local_tempdir).expect("failed to create local tempdir");

        let local_server_config_path = local_tempdir.join("server.toml");
        fs::write(&local_server_config_path, "[defaults]\n")
            .expect("failed to write local server.toml");

        let local_config_path = local_tempdir.join("config.toml");
        let mut local_config = AppConfig::default();
        local_config.ssh.server_config_path = local_server_config_path.display().to_string();
        local_config.server.local.enable = false;
        local_config.server.remote.enable = false;
        local_config.review.enable = false;

        let local_service = make_test_rpc_service(local_config, local_config_path);

        // Wire up remote daemon
        let (remote_client_io, remote_server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);
        tokio::spawn(async move {
            Server::builder()
                .add_service(remote_service)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(
                    remote_server_io,
                )))
                .await
                .expect("remote gRPC server failed");
        });
        let remote_client = connect_client_via_duplex(remote_client_io).await;

        // Wire up local daemon
        let (local_client_io, local_server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);
        tokio::spawn(async move {
            Server::builder()
                .add_service(local_service)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(local_server_io)))
                .await
                .expect("local gRPC server failed");
        });
        let local_client = connect_client_via_duplex(local_client_io).await;

        Self {
            local_client,
            remote_client,
            local_tempdir,
            remote_tempdir,
        }
    }
}

impl Drop for PairedRpcHarness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.local_tempdir);
        let _ = fs::remove_dir_all(&self.remote_tempdir);
    }
}

// --- Private helpers ---

/// Connect a gRPC client through a pre-connected duplex stream.
async fn connect_client_via_duplex(io: tokio::io::DuplexStream) -> XhoRpcClient<Channel> {
    // Wrap the duplex stream in a mutex so the service_fn closure can move it
    let io = std::sync::Mutex::new(Some(io));

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

    XhoRpcClient::new(channel)
}

/// Build a minimal `server.toml` content from a list of server entries.
fn build_server_toml(servers: &[ServerEntry]) -> String {
    let mut toml = String::from("[defaults]\nuser = \"testuser\"\nport = 22\n\n");

    for entry in servers {
        toml.push_str(&format!("[servers.{}]\n", entry.alias));
        toml.push_str(&format!("host = \"{}\"\n", entry.host));
        toml.push_str(&format!("port = {}\n", entry.port));
        toml.push_str(&format!("user = \"{}\"\n", entry.user));
        match &entry.auth {
            DirectAuth::Key { identity_file } => {
                toml.push_str(&format!("identity_file = \"{}\"\n", identity_file));
            }
            DirectAuth::Password { .. } => {
                toml.push_str("auth = \"password\"\n");
            }
        }
        toml.push('\n');
    }

    toml
}

/// Generate a short unique id for tempdir naming.
fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let tid = std::thread::current().id();
    format!("{:x}-{:?}", nanos, tid)
}
