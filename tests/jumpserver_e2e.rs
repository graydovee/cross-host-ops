//! End-to-end tests for `xho exec -it <jump:asset>` against a mock JumpServer
//! bastion (`tests/support/mock_bastion.rs`).
//!
//! These exercise the real JumpserverGateway menu-navigation + interactive
//! sentinel-passthrough path through the in-process daemon, covering:
//! - no `__XHO_E_<uuid>` marker leak + correct exit codes (#1/#3),
//! - session-cache reuse across consecutive calls (#4),
//! - `xterm-256color` PTY term so the remote rc enables colors (#2),
//! - raw ANSI passthrough (no stripping).

mod support;

use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Server};
use tower::service_fn;

use xho::config::{AppConfig, GatewayConfig, JumpserverGatewayConfig};
use xho::daemon::test_support::make_test_rpc_service;
use xho::protocol::rpc;
use xho::protocol::rpc::xho_rpc_client::XhoRpcClient;

use support::mock_bastion::{MOCK_BASTION_KEY, MockBastion};

use hyper_util::rt::TokioIo;
use tonic::transport::Uri;

const DUPLEX_BUFFER_SIZE: usize = 1024 * 1024;
const TARGET_IP: &str = "10.0.0.1";

/// A wired e2e harness: a running mock bastion + an in-process daemon (with a
/// JumpserverGateway pointing at it) reachable via a gRPC client.
struct E2E {
    bastion: MockBastion,
    client: XhoRpcClient<Channel>,
    /// Kept alive for the test's lifetime (holds the key + server.toml).
    _tempdir: TempDir,
}

async fn setup() -> E2E {
    let tempdir = tempfile::tempdir().expect("tempdir");

    // Write the mock bastion key (used as both host key and gateway identity).
    let key_path = tempdir.path().join("id_ed25519");
    std::fs::write(&key_path, MOCK_BASTION_KEY).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }

    // Minimal server.toml (the daemon always registers a `local` direct gateway).
    let server_toml = tempdir.path().join("server.toml");
    std::fs::write(&server_toml, "[defaults]\nuser = \"testuser\"\nport = 22\n").unwrap();

    // Start the mock bastion.
    let bastion = MockBastion::start(TARGET_IP, &key_path)
        .await
        .expect("mock bastion start");

    // Build a config with one Jumpserver gateway pointing at the mock.
    let mut config = AppConfig::default();
    config.ssh.server_config_path = server_toml.to_string_lossy().to_string();
    config.server.local.enable = false;
    config.server.remote.enable = false;
    config.review.enable = false;
    config.gateways = vec![GatewayConfig::Jumpserver(JumpserverGatewayConfig {
        name: "mockjump".to_string(),
        host: "127.0.0.1".to_string(),
        port: bastion.addr.port(),
        user: "mockuser".to_string(),
        identity_file: key_path.to_string_lossy().to_string(),
        pubkey_accepted_algorithms: None,
        totp_secret_base32: String::new(),
        totp_digits: 6,
        totp_period: 30,
        max_cached_sessions: Some(4),
        session_idle_timeout: Duration::from_secs(300),
    })];

    // Serve the daemon over an in-process duplex and connect a gRPC client.
    let service = make_test_rpc_service(config, tempdir.path().join("config.toml"));
    let (client_io, server_io) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await;
    });
    let client = connect_client(client_io).await;

    E2E {
        bastion,
        client,
        _tempdir: tempdir,
    }
}

async fn connect_client(io: tokio::io::DuplexStream) -> XhoRpcClient<Channel> {
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
        .expect("connect gRPC client");
    XhoRpcClient::new(channel)
}

/// Run `xho exec -it mockjump:<ip> -- <argv>` and return (stdout, exit_code).
async fn exec_interactive(
    client: &mut XhoRpcClient<Channel>,
    argv: &[&str],
) -> (Vec<u8>, Option<i32>) {
    let target = format!("mockjump:{TARGET_IP}");
    let (tx, rx) = mpsc::channel::<rpc::ExecuteRequest>(8);
    tx.send(rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target,
            argv: argv.iter().map(|s| s.to_string()).collect(),
            tty: true,
            stdin: true,
            interactive: true,
            term_cols: 80,
            term_rows: 24,
            timeout_ms: 30_000,
            ..Default::default()
        })),
    })
    .await
    .expect("send start");

    let response = client.execute(ReceiverStream::new(rx)).await.expect("execute");
    let mut stream = response.into_inner();
    let mut stdout = Vec::new();
    let mut code = None;
    // Keep `tx` alive until the stream ends so the daemon doesn't see a closed
    // inbound stream (which would abort the interactive session).
    while let Some(msg) = stream.message().await.expect("read response") {
        match msg.event.expect("event") {
            rpc::execute_response::Event::Stdout(chunk) => stdout.extend_from_slice(&chunk.data),
            rpc::execute_response::Event::ExitStatus(status) => {
                code = Some(status.code);
                break;
            }
            rpc::execute_response::Event::Error(e) => {
                panic!("execute error: {}", e.message);
            }
            _ => {}
        }
    }
    drop(tx);
    (stdout, code)
}

#[tokio::test]
async fn no_marker_leak_and_exit_zero() {
    // Bug #1/#3: stdout must contain ls output but NO `__XHO_E` sentinel.
    let mut e2e = setup().await;
    let (stdout, code) = exec_interactive(&mut e2e.client, &["ls"]).await;

    assert_eq!(code, Some(0), "exit code");
    assert!(
        !stdout.windows(b"__XHO_E".len()).any(|w| w == b"__XHO_E"),
        "marker leaked into stdout: {:?}",
        String::from_utf8_lossy(&stdout)
    );
    assert!(
        !stdout.is_empty(),
        "expected ls output, got empty stdout"
    );
}

#[tokio::test]
async fn exit_codes_propagate() {
    let mut e2e = setup().await;
    let (_, code_true) = exec_interactive(&mut e2e.client, &["true"]).await;
    assert_eq!(code_true, Some(0));

    let (_, code_false) = exec_interactive(&mut e2e.client, &["false"]).await;
    assert_eq!(code_false, Some(1), "`false` must exit 1, not 0 (Eof race)");

    let (_, code_42) = exec_interactive(&mut e2e.client, &["sh", "-c", "exit 42"]).await;
    assert_eq!(code_42, Some(42));
}

#[tokio::test]
async fn pty_term_is_xterm_256color() {
    // Bug #2: the gateway must request `xterm-256color` so the remote rc
    // enables its `ls --color=auto` alias.
    let mut e2e = setup().await;
    let (_, _code) = exec_interactive(&mut e2e.client, &["true"]).await;
    let term = e2e.bastion.pty_term().expect("PTY was requested");
    assert_eq!(
        term, "xterm-256color",
        "jumpserver navigation must request xterm-256color, got {term}"
    );
}

#[tokio::test]
async fn raw_ansi_is_passed_through() {
    // The interactive path is raw passthrough — ANSI escapes must survive
    // unstripped (color support assumes no stripping).
    let mut e2e = setup().await;
    let (stdout, _code) = exec_interactive(
        &mut e2e.client,
        &["printf", "\\033[31mred\\033[0m"],
    )
    .await;
    assert!(
        stdout.windows(2).any(|w| w == b"\x1b["),
        "ANSI escape stripped from stdout: {:?}",
        String::from_utf8_lossy(&stdout)
    );
}

#[tokio::test]
async fn consecutive_calls_reuse_cached_session() {
    // Bug #4: a one-shot `-it` command returns its shell to the cache, so the
    // second call does NOT re-navigate the menu.
    let mut e2e = setup().await;

    let (stdout1, code1) = exec_interactive(&mut e2e.client, &["true"]).await;
    assert_eq!(code1, Some(0));
    assert!(!stdout1.contains(&b' ')); // `true` produces no output
    let nav_after_first = e2e.bastion.nav_count();
    assert_eq!(nav_after_first, 1, "first call navigates the menu once");

    let (stdout2, code2) = exec_interactive(&mut e2e.client, &["true"]).await;
    assert_eq!(code2, Some(0));
    let nav_after_second = e2e.bastion.nav_count();
    assert_eq!(
        nav_after_second, 1,
        "second call must hit the session cache, not re-navigate"
    );
    let _ = stdout2;
}
