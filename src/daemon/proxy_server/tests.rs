use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use russh::client;
use russh::keys::PrivateKeyWithHashAlg;
use russh::server::Server as _;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use super::*;
use crate::config::AppConfig;
use crate::daemon::gateway::{Capabilities, Gateway, GatewayError};
use crate::daemon::session::{SessionCommand, TargetSession};
use crate::daemon::{CliStartOptions, DaemonOrigin};
use crate::oversight::Oversight;
use crate::protocol::ServerListRow;

const TEST_DEADLINE: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
enum Mode {
    Echo,
    OpenFailure,
    StartFailure,
    ListHang,
    StartHang,
    EndStream,
}

struct Backend {
    mode: Mode,
    release_open: Semaphore,
    entered_open: Semaphore,
    entered_start: Semaphore,
    open_active: Arc<AtomicUsize>,
    sessions_active: Arc<AtomicUsize>,
    received: Mutex<Vec<u8>>,
    requests: Mutex<Vec<String>>,
}

impl Backend {
    fn new(mode: Mode, block_open: bool) -> Arc<Self> {
        Arc::new(Self {
            mode,
            release_open: Semaphore::new(if block_open { 0 } else { 100 }),
            entered_open: Semaphore::new(0),
            entered_start: Semaphore::new(0),
            open_active: Arc::new(AtomicUsize::new(0)),
            sessions_active: Arc::new(AtomicUsize::new(0)),
            received: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        })
    }

    async fn wait_open(&self) {
        timeout(TEST_DEADLINE, self.entered_open.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }

    fn record(&self, request: String) {
        self.requests.lock().unwrap().push(request);
    }
}

struct ActiveGuard(Arc<AtomicUsize>);

impl ActiveGuard {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct MockGateway(Arc<Backend>);

#[async_trait]
impl Gateway for MockGateway {
    fn name(&self) -> &str {
        "backend"
    }
    fn kind(&self) -> GatewayKind {
        GatewayKind::Direct
    }
    fn capabilities(&self) -> Capabilities {
        let mut caps = Capabilities::PROXY;
        if matches!(self.0.mode, Mode::ListHang) {
            caps |= Capabilities::LIST;
        }
        caps
    }
    async fn open_exec_session(
        &self,
        _: &str,
        _: &[String],
        _: &str,
        _: bool,
    ) -> Result<(Box<dyn TargetSession>, String), GatewayError> {
        unreachable!("proxy must use open_session")
    }
    async fn open_session(&self, _: &str) -> Result<Box<dyn TargetSession>, GatewayError> {
        let _guard = ActiveGuard::new(&self.0.open_active);
        self.0.entered_open.add_permits(1);
        self.0.release_open.acquire().await.unwrap().forget();
        if matches!(self.0.mode, Mode::OpenFailure) {
            return Err(GatewayError::transport(anyhow!("mock connection failed")));
        }
        Ok(Box::new(MockSession(self.0.clone())))
    }
    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError> {
        let _guard = ActiveGuard::new(&self.0.open_active);
        self.0.entered_open.add_permits(1);
        std::future::pending().await
    }
}

struct MockSession(Arc<Backend>);

impl TargetSession for MockSession {
    fn split(self: Box<Self>) -> (SessionWriter, SessionStream) {
        let backend = self.0;
        let guard = ActiveGuard::new(&backend.sessions_active);
        let (tx, mut rx) = mpsc::channel(64);
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let _guard = guard;
            let mut ev_tx = Some(ev_tx);
            while let Some(cmd) = rx.recv().await {
                let start_reply = match cmd {
                    SessionCommand::Pty { term, reply, .. } => {
                        backend.record(format!("pty:{term}"));
                        let _ = reply.send(Ok(()));
                        None
                    }
                    SessionCommand::Env { key, value, reply } => {
                        backend.record(format!("env:{key}={value}"));
                        let _ = reply.send(Ok(()));
                        None
                    }
                    SessionCommand::Exec { command, reply } => {
                        backend.record(format!("exec:{command}"));
                        Some(reply)
                    }
                    SessionCommand::Shell { reply } => {
                        backend.record("shell".to_string());
                        Some(reply)
                    }
                    SessionCommand::Subsystem { name, reply } => {
                        backend.record(format!("subsystem:{name}"));
                        Some(reply)
                    }
                    SessionCommand::Data { bytes } => {
                        backend.received.lock().unwrap().extend_from_slice(&bytes);
                        if let Some(tx) = &ev_tx {
                            let _ = tx.send(SessionEvent::Stdout(bytes));
                        }
                        None
                    }
                    SessionCommand::Resize { cols, rows } => {
                        backend.record(format!("resize:{cols}:{rows}"));
                        None
                    }
                    SessionCommand::Signal { signal } => {
                        backend.record(format!("signal:{signal}"));
                        None
                    }
                    SessionCommand::Eof => {
                        if let Some(tx) = ev_tx.take() {
                            let _ = tx.send(SessionEvent::ExitStatus(7));
                        }
                        None
                    }
                };
                if let Some(mut reply) = start_reply {
                    backend.entered_start.add_permits(1);
                    match backend.mode {
                        Mode::StartFailure => {
                            let _ = reply.send(Err(anyhow!("mock start failed")));
                        }
                        Mode::StartHang => {
                            // Cancellation drops the request's response receiver.
                            reply.closed().await;
                            break;
                        }
                        Mode::EndStream => {
                            let _ = reply.send(Ok(()));
                            ev_tx.take();
                        }
                        _ => {
                            let _ = reply.send(Ok(()));
                        }
                    }
                }
            }
        });
        (SessionWriter { tx }, SessionStream { rx: ev_rx })
    }
}

struct TestClient;

impl client::Handler for TestClient {
    type Error = russh::Error;
    async fn check_server_key(&mut self, _: &ssh_key::PublicKey) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

struct Harness {
    client: client::Handle<TestClient>,
    server: JoinHandle<Result<()>>,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn new(backend: Arc<Backend>, user: &str, setup_timeout: Duration) -> Self {
        timeout(TEST_DEADLINE, async {
            let dir = tempfile::tempdir().unwrap();
            let key = ssh_key::PrivateKey::random(
                &mut rand_core::UnwrapErr(getrandom::SysRng),
                ssh_key::Algorithm::Ed25519,
            )
            .unwrap();
            let authorized_keys = dir.path().join("authorized_keys");
            std::fs::write(&authorized_keys, key.public_key().to_openssh().unwrap()).unwrap();
            let mut config = AppConfig::default();
            config.ssh.connect_timeout = setup_timeout;
            config.ssh.server_config_path =
                dir.path().join("missing-server.toml").display().to_string();
            config.ssh.ssh_config_path =
                dir.path().join("missing-ssh-config").display().to_string();
            config.ssh.fallback.clear();
            let (shutdown_tx, _) = mpsc::channel(1);
            let state = DaemonState {
                config_path: dir.path().join("config.toml"),
                config: Arc::new(tokio::sync::RwLock::new(config)),
                gateways: vec![("backend".to_string(), Arc::new(MockGateway(backend)))],
                oversight: Oversight::disabled(),
                shutdown_tx,
                origin: DaemonOrigin::External,
                cli_start_options: CliStartOptions::default(),
                token_store: super::super::token_store::TokenStore::new(),
                authorized_keys_lock: Arc::new(tokio::sync::Mutex::new(())),
                reverse_proxy_registry: Arc::new(
                    super::super::reverse_proxy::ReverseProxyRegistry::new(),
                ),
            };
            let mut proxy = ProxySshServer {
                state,
                authorized_keys_path: authorized_keys.display().to_string(),
            };
            let handler = proxy.new_client(None);
            let server_config = Arc::new(server::Config {
                keys: vec![key.clone()],
                auth_rejection_time: Duration::ZERO,
                // Exercise the bounded-queue freeze with only a few packets.
                channel_buffer_size: 4,
                ..Default::default()
            });
            let (client_io, server_io) = tokio::io::duplex(1024 * 1024);
            let server = tokio::spawn(async move {
                server::run_stream(server_config, server_io, handler)
                    .await?
                    .await
            });
            let mut client =
                client::connect_stream(Arc::new(client::Config::default()), client_io, TestClient)
                    .await
                    .unwrap();
            assert!(
                client
                    .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), None),)
                    .await
                    .unwrap()
                    .success()
            );
            Self {
                client,
                server,
                _dir: dir,
            }
        })
        .await
        .expect("SSH handshake hung")
    }

    async fn start(&self, pty: bool, start: SessionStart) -> Channel<client::Msg> {
        timeout(TEST_DEADLINE, async {
            let mut channel = self.client.channel_open_session().await.unwrap();
            if pty {
                channel
                    .request_pty(true, "xterm", 80, 24, 0, 0, &[])
                    .await
                    .unwrap();
                expect_success(&mut channel).await;
            }
            channel.set_env(true, "LANG", "C").await.unwrap();
            expect_success(&mut channel).await;
            match start {
                SessionStart::Shell => channel.request_shell(true).await.unwrap(),
                SessionStart::Exec(cmd) => channel.exec(true, cmd).await.unwrap(),
                SessionStart::Subsystem(name) => {
                    channel.request_subsystem(true, name).await.unwrap()
                }
            }
            expect_success(&mut channel).await;
            channel
        })
        .await
        .expect("SSH start request hung")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn expect_success(channel: &mut Channel<client::Msg>) {
    assert!(matches!(channel.wait().await, Some(ChannelMsg::Success)));
}

#[derive(Default, Debug)]
struct Output {
    stdout: Vec<u8>,
    stderr: String,
    status: Option<u32>,
    eof: bool,
    closed: bool,
}

async fn collect(channel: &mut Channel<client::Msg>) -> Output {
    timeout(TEST_DEADLINE, async {
        let mut output = Output::default();
        while let Some(msg) = channel.wait().await {
            match msg {
                ChannelMsg::Data { data } => output.stdout.extend_from_slice(&data),
                ChannelMsg::ExtendedData { ext: 1, data } => {
                    output.stderr.push_str(&String::from_utf8_lossy(&data));
                }
                ChannelMsg::ExitStatus { exit_status } => output.status = Some(exit_status),
                ChannelMsg::Eof => output.eof = true,
                ChannelMsg::Close => {
                    output.closed = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(output.closed, "missing channel close: {output:?}");
        output
    })
    .await
    .expect("SSH channel hung instead of closing")
}

async fn assert_inactive(count: &AtomicUsize) {
    timeout(TEST_DEADLINE, async {
        while count.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backend task leaked after bridge ended");
}

#[tokio::test]
async fn unknown_target_reports_error_and_closes() {
    let backend = Backend::new(Mode::Echo, false);
    let harness = Harness::new(backend, "p22", Duration::from_secs(1)).await;
    let mut channel = harness.start(true, SessionStart::Shell).await;
    let output = collect(&mut channel).await;
    assert_eq!(output.status, Some(255));
    assert!(output.stderr.contains("p22"));
    assert!(output.stderr.contains("failed to resolve target"));
    assert!(output.eof);
}

#[tokio::test]
async fn backend_open_and_start_failures_report_error_and_close() {
    for (mode, message) in [
        (Mode::OpenFailure, "mock connection failed"),
        (Mode::StartFailure, "mock start failed"),
    ] {
        let backend = Backend::new(mode, false);
        let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(1)).await;
        let mut channel = harness.start(true, SessionStart::Shell).await;
        let output = collect(&mut channel).await;
        assert_eq!(output.status, Some(255));
        assert!(output.stderr.contains(message), "{output:?}");
        assert!(output.eof);
        assert_inactive(&backend.sessions_active).await;
    }
}

#[tokio::test]
async fn setup_deadline_bounds_resolution_open_and_start() {
    for (mode, blocked, user) in [
        (Mode::ListHang, false, "unknown"),
        (Mode::Echo, true, "backend"),
        (Mode::StartHang, false, "backend"),
    ] {
        let backend = Backend::new(mode, blocked);
        let harness = Harness::new(backend.clone(), user, Duration::from_millis(80)).await;
        let mut channel = harness.start(false, SessionStart::Shell).await;
        let output = collect(&mut channel).await;
        assert_eq!(output.status, Some(124));
        assert!(output.stderr.contains("timed out"));
        assert!(output.eof);
        assert_inactive(&backend.open_active).await;
        assert_inactive(&backend.sessions_active).await;
    }
}

#[tokio::test]
async fn ctrl_c_and_int_cancel_setup() {
    for signal in [false, true] {
        let backend = Backend::new(Mode::Echo, true);
        let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(30)).await;
        let mut channel = harness.start(!signal, SessionStart::Shell).await;
        backend.wait_open().await;
        if signal {
            channel.signal(Sig::INT).await.unwrap();
        } else {
            channel.data(Cursor::new(vec![3])).await.unwrap();
        }
        let output = collect(&mut channel).await;
        assert_eq!(output.status, Some(130));
        assert!(output.stderr.contains("cancelled"));
        assert_inactive(&backend.open_active).await;
    }
}

#[tokio::test]
async fn int_cancels_pending_start_request() {
    let backend = Backend::new(Mode::StartHang, false);
    let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(30)).await;
    let mut channel = harness.start(true, SessionStart::Shell).await;
    timeout(TEST_DEADLINE, backend.entered_start.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    channel.signal(Sig::INT).await.unwrap();
    assert_eq!(collect(&mut channel).await.status, Some(130));
    assert_inactive(&backend.sessions_active).await;
}

#[tokio::test]
async fn inbound_queue_is_drained_during_setup() {
    let backend = Backend::new(Mode::Echo, true);
    let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(30)).await;
    let mut channel = harness.start(true, SessionStart::Shell).await;
    backend.wait_open().await;
    for _ in 0..32 {
        channel.data(Cursor::new(b"waiting\n")).await.unwrap();
    }
    channel.data(Cursor::new(vec![3])).await.unwrap();
    assert_eq!(collect(&mut channel).await.status, Some(130));
    assert_inactive(&backend.open_active).await;
}

#[tokio::test]
async fn channel_close_cancels_setup_and_connection_can_be_reused() {
    let backend = Backend::new(Mode::Echo, true);
    let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(30)).await;
    let channel = harness.start(false, SessionStart::Shell).await;
    backend.wait_open().await;
    channel.close().await.unwrap();
    assert_inactive(&backend.open_active).await;
    backend.release_open.add_permits(1);
    let mut channel = harness.start(false, SessionStart::Exec("cat".into())).await;
    channel.data(Cursor::new(b"still alive")).await.unwrap();
    channel.eof().await.unwrap();
    let output = collect(&mut channel).await;
    assert_eq!(output.stdout, b"still alive");
    assert_eq!(output.status, Some(7));
}

#[tokio::test]
async fn disconnect_cancels_setup() {
    let backend = Backend::new(Mode::Echo, true);
    let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(30)).await;
    let _channel = harness.start(false, SessionStart::Shell).await;
    backend.wait_open().await;
    harness
        .client
        .disconnect(russh::Disconnect::ByApplication, "done", "")
        .await
        .unwrap();
    assert_inactive(&backend.open_active).await;
}

#[tokio::test]
async fn pending_binary_input_and_eof_keep_their_order() {
    let backend = Backend::new(Mode::Echo, true);
    let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(1)).await;
    // Send data even before shell_request to exercise the synchronous flush.
    let mut channel = harness.client.channel_open_session().await.unwrap();
    channel.data(Cursor::new(b"first\x03")).await.unwrap();
    channel.request_shell(true).await.unwrap();
    expect_success(&mut channel).await;
    backend.wait_open().await;
    channel.data(Cursor::new(b"second\x00")).await.unwrap();
    channel.eof().await.unwrap();
    backend.release_open.add_permits(1);
    let output = collect(&mut channel).await;
    assert_eq!(output.stdout, b"first\x03second\x00");
    assert_eq!(output.status, Some(7));
    assert_eq!(*backend.received.lock().unwrap(), output.stdout);
}

#[tokio::test]
async fn running_sessions_keep_pty_exec_subsystem_and_signal_behavior() {
    for (pty, start, expected) in [
        (true, SessionStart::Shell, "shell"),
        (false, SessionStart::Exec("cat".into()), "exec:cat"),
        (
            false,
            SessionStart::Subsystem("sftp".into()),
            "subsystem:sftp",
        ),
    ] {
        let backend = Backend::new(Mode::Echo, false);
        let harness = Harness::new(backend.clone(), "backend", Duration::from_millis(80)).await;
        let mut channel = harness.start(pty, start).await;
        channel.data(Cursor::new(b"ready")).await.unwrap();
        // Seeing the echo proves setup has completed before sending INT.
        timeout(TEST_DEADLINE, async {
            loop {
                if let Some(ChannelMsg::Data { data }) = channel.wait().await {
                    assert_eq!(&data[..], b"ready");
                    break;
                }
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        channel.signal(Sig::INT).await.unwrap();
        channel.window_change(120, 40, 0, 0).await.unwrap();
        channel.data(Cursor::new(vec![3])).await.unwrap();
        channel.eof().await.unwrap();
        let output = collect(&mut channel).await;
        assert_eq!(output.stdout, vec![3]);
        assert_eq!(output.status, Some(7));
        let requests = backend.requests.lock().unwrap();
        assert!(requests.iter().any(|r| r == expected));
        assert!(requests.iter().any(|r| r == "env:LANG=C"));
        assert!(requests.iter().any(|r| r == "signal:INT"));
        assert!(requests.iter().any(|r| r == "resize:120:40"));
        assert_eq!(requests.iter().any(|r| r == "pty:xterm"), pty);
    }
}

#[tokio::test]
async fn backend_stream_end_closes_client_and_drops_writer() {
    let backend = Backend::new(Mode::EndStream, false);
    let harness = Harness::new(backend.clone(), "backend", Duration::from_secs(1)).await;
    let mut channel = harness.start(false, SessionStart::Shell).await;
    let output = collect(&mut channel).await;
    assert!(output.eof);
    assert_eq!(output.status, None);
    assert_inactive(&backend.sessions_active).await;
}
