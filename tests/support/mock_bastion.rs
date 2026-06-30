//! A minimal mock JumpServer bastion for end-to-end tests.
//!
//! Implements just enough of the menu-driven bastion protocol that
//! `JumpserverGateway::navigate_to_asset` drives against:
//!
//! - **MENU**: on connect emit an `Opt>` prompt; on receiving the target IP
//!   emit an asset table containing it + `Opt>`; on receiving the asset id,
//!   switch to ASSET and emit a shell prompt (`devops@mock:~$ `).
//! - **ASSET**: echo the typed command line (PTY echo), run it through a real
//!   local `sh -c` (so the wrapped `{ cmd; }; status=$?; printf '…'` produces
//!   real output + the `__XHO_E_<uuid>:<code>` sentinel), then re-emit the
//!   prompt.
//!
//! Records the requested PTY `term` and counts asset-entry navigations, so
//! tests can assert on color support (xterm-256color) and session-cache reuse.

use std::io::Cursor;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use russh::server::{self, Auth, Msg, Server as _};
use russh::{Channel, ChannelId, Pty, Sig};
use russh::keys::ssh_key;
use tokio::net::TcpListener;
use tokio::process::Command;

/// Throwaway ed25519 keypair (grants access to nothing) used as BOTH the mock
/// bastion's host key and the gateway's `identity_file`. The mock accepts any
/// offered client key, so the two need not match.
pub const MOCK_BASTION_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\n\
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n\
QyNTUxOQAAACDWkSP1mUrnF8jnnizvtQHhZSiEy8nAuBqAyIVtVE1NXQAAAJiDKHdHgyh3\n\
RwAAAAtzc2gtZWQyNTUxOQAAACDWkSP1mUrnF8jnnizvtQHhZSiEy8nAuBqAyIVtVE1NXQ\n\
AAAEAONeHsr5bnH/CJPJt3bEzGnfNWAD6CiQzGKUSGdaeLodaRI/WZSucXyOeeLO+1AeFl\n\
KITLycC4GoDIhW1UTU1dAAAAFXhoby1tb2NrLWJhc3Rpb24tdGVzdA==\n\
-----END OPENSSH PRIVATE KEY-----\n";

/// Shared, observable state across all connections to the mock bastion.
pub struct MockState {
    pub target_ip: String,
    pub asset_id: String,
    /// PTY `term` requested by the gateway during navigation (e.g.
    /// `xterm-256color`). `Mutex` so the handler can record it from `&mut self`.
    pub pty_term: Mutex<Option<String>>,
    /// Number of times a client navigated from the menu into the asset shell.
    pub nav_count: AtomicUsize,
}

/// A running mock bastion.
pub struct MockBastion {
    pub state: Arc<MockState>,
    pub addr: SocketAddr,
}

impl MockBastion {
    /// Start the mock bastion on a random loopback port. `host_key_path` is a
    /// file containing [`MOCK_BASTION_KEY`] (used as the server host key).
    pub async fn start(target_ip: &str, host_key_path: &Path) -> Result<MockBastion> {
        let host_key = ssh_key::PrivateKey::read_openssh_file(host_key_path)?;
        let config = Arc::new(server::Config {
            auth_rejection_time: Duration::from_secs(1),
            auth_rejection_time_initial: Some(Duration::from_secs(0)),
            keys: vec![host_key],
            inactivity_timeout: Some(Duration::from_secs(600)),
            ..Default::default()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(MockState {
            target_ip: target_ip.to_string(),
            asset_id: "1".to_string(),
            pty_term: Mutex::new(None),
            nav_count: AtomicUsize::new(0),
        });
        let mut server = MockServer {
            state: state.clone(),
        };
        tokio::spawn(async move {
            let _ = server.run_on_socket(config, &listener).await;
        });
        Ok(MockBastion { state, addr })
    }

    pub fn pty_term(&self) -> Option<String> {
        self.state.pty_term.lock().unwrap().clone()
    }

    pub fn nav_count(&self) -> usize {
        self.state.nav_count.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
struct MockServer {
    state: Arc<MockState>,
}

impl server::Server for MockServer {
    type Handler = MockHandler;
    fn new_client(&mut self, _peer: Option<SocketAddr>) -> Self::Handler {
        MockHandler {
            state: self.state.clone(),
            channel: None,
            mode: Mode::Menu,
            buf: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Menu,
    Asset,
}

struct MockHandler {
    state: Arc<MockState>,
    channel: Option<Channel<Msg>>,
    mode: Mode,
    buf: Vec<u8>,
}

impl MockHandler {
    async fn write_bytes(&mut self, bytes: &[u8]) {
        if let Some(ch) = self.channel.as_mut() {
            let _ = ch.data(Cursor::new(bytes.to_vec())).await;
        }
    }

    /// Process every complete line currently buffered.
    async fn drain_lines(&mut self) {
        while let Some(line) = take_line(&mut self.buf) {
            self.handle_line(&line).await;
        }
    }

    async fn handle_line(&mut self, line: &str) {
        let line = line.trim_end_matches(['\r', '\n']);
        match self.mode {
            Mode::Menu => {
                if line == self.state.target_ip {
                    let table = format!(
                        "\r\n  ID | host | IP | note\r\n  {} | mock-asset | {} | \r\n\
                         页码：1，每页行数：9，总页数：1，总数量：1\r\nOpt> ",
                        self.state.asset_id, self.state.target_ip,
                    );
                    self.write_bytes(table.as_bytes()).await;
                } else if line == self.state.asset_id {
                    self.state.nav_count.fetch_add(1, Ordering::SeqCst);
                    self.mode = Mode::Asset;
                    self.write_bytes(b"devops@mock:~$ ").await;
                } else {
                    self.write_bytes(b"Opt> ").await;
                }
            }
            Mode::Asset => {
                // PTY echo of the typed line, then run it, then re-prompt.
                self.write_bytes(format!("{line}\r\n").as_bytes()).await;
                let output = run_shell(line).await;
                if !output.is_empty() {
                    self.write_bytes(&output).await;
                }
                self.write_bytes(b"devops@mock:~$ ").await;
            }
        }
    }
}

/// Run a command line through a real local `sh`, then translate `\n` -> `\r\n`
/// to mimic a real bastion PTY (onlcr). The wrapped sentinel command emits its
/// own `__XHO_E_<uuid>:<code>\r\n` line, which we forward verbatim — this is
/// what forces the scanner to handle the CRLF form that production PTYs emit.
async fn run_shell(line: &str) -> Vec<u8> {
    match Command::new("sh").arg("-c").arg(line).output().await {
        Ok(out) => {
            let mut translated = Vec::with_capacity(out.stdout.len() + 16);
            for &b in &out.stdout {
                if b == b'\n' {
                    translated.push(b'\r');
                }
                translated.push(b);
            }
            translated
        }
        Err(_) => Vec::new(),
    }
}

/// Pop the first line (terminated by `\r` or `\n`) from `buf`. Returns the line
/// without its terminator and removes it (plus a paired `\r\n`) from `buf`.
fn take_line(buf: &mut Vec<u8>) -> Option<String> {
    let idx = buf.iter().position(|&b| b == b'\n' || b == b'\r')?;
    let line = String::from_utf8_lossy(&buf[..idx]).to_string();
    let mut consume = idx + 1;
    if buf.get(idx) == Some(&b'\r') && buf.get(idx + 1) == Some(&b'\n') {
        consume += 1;
    }
    buf.drain(..consume);
    Some(line)
}

impl server::Handler for MockHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        _key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        // Test-only bastion: accept any offered key.
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        self.channel = Some(channel);
        Ok(true)
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(Pty, u32)],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        *self.state.pty_term.lock().unwrap() = Some(term.to_string());
        let _ = session.channel_success(channel);
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        let _ = session.channel_success(channel);
        self.write_bytes(b"\r\nmock jumpserver menu\r\nOpt> ").await;
        Ok(())
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        self.buf.extend_from_slice(data);
        self.drain_lines().await;
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        _channel: ChannelId,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn signal(
        &mut self,
        _channel: ChannelId,
        _signal: Sig,
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
