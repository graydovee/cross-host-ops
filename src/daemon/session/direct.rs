// DirectSshSession — a `TargetSession` backed by a raw russh client channel.
//
// This is the byte-perfect transport: every request (pty/exec/shell/subsystem/
// data/resize/signal) is forwarded verbatim to the outbound SSH channel, and
// every channel message (data/extended-data/exit-status/exit-signal/eof) is
// surfaced as a `SessionEvent`. Because the payload is never interpreted, scp
// (both sftp-mode and legacy `-O`), sftp, exec, and pty all work transparently.
//
// An internal task owns the `Channel` (so `wait()`/`data()` borrows never
// conflict) and is driven through control/stdin channels; trait methods send a
// control message and await the result.

use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use russh::Channel;
use russh::ChannelMsg;
use russh::client::{self};
use russh::keys::ssh_key::HashAlg;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use russh::Pty;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::config::{AppConfig, DirectAuth};

use super::{SessionEvent, TargetSession};

/// Open+authenticate a russh client handle for a direct SSH target.
///
/// Shared by the direct transport so connection pooling can check out a handle
/// and the session opens a channel from it.
pub(crate) async fn connect_authenticated(
    host: &str,
    port: u16,
    user: &str,
    auth: &DirectAuth,
    config: &AppConfig,
) -> Result<client::Handle<ClientHandler>> {
    let mut handle = connect_handle(host, port, config).await?;
    match auth {
        DirectAuth::Key { identity_file } => {
            authenticate_with_key(&mut handle, user, identity_file).await?;
        }
        DirectAuth::Password { password } => {
            authenticate_with_password(&mut handle, user, password).await?;
        }
        DirectAuth::None | DirectAuth::ReverseProxy => {
            anyhow::bail!("direct SSH requires key or password auth");
        }
    }
    Ok(handle)
}

/// russh client handler that accepts any host key (identity is verified via
/// known_hosts at a higher layer, matching existing behaviour).
#[derive(Clone, Default)]
pub(crate) struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

async fn connect_handle(
    host: &str,
    port: u16,
    config: &AppConfig,
) -> Result<client::Handle<ClientHandler>> {
    let client_config = client::Config {
        keepalive_interval: Some(config.ssh.keepalive_interval),
        inactivity_timeout: Some(config.ssh.keepalive_interval * 2),
        ..Default::default()
    };
    let handle = timeout(
        config.ssh.connect_timeout,
        client::connect(Arc::new(client_config), (host, port), ClientHandler),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out opening SSH connection to {host}:{port}"))??;
    Ok(handle)
}

async fn authenticate_with_key(
    handle: &mut client::Handle<ClientHandler>,
    user: &str,
    identity_file: &str,
) -> Result<()> {
    let key = load_secret_key(identity_file, None)
        .map_err(|e| anyhow::anyhow!("failed to load key {identity_file}: {e}"))?;
    let hash_alg = handle
        .best_supported_rsa_hash()
        .await?
        .flatten()
        .or(Some(HashAlg::Sha512));
    let authed = handle
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg))
        .await?;
    if !authed.success() {
        anyhow::bail!("SSH publickey authentication failed for {user}");
    }
    Ok(())
}

async fn authenticate_with_password(
    handle: &mut client::Handle<ClientHandler>,
    user: &str,
    password: &str,
) -> Result<()> {
    let authed = handle.authenticate_password(user, password).await?;
    if !authed.success() {
        anyhow::bail!("SSH password authentication failed for {user}");
    }
    Ok(())
}

// -----------------------------------------------------------------------
// Internal control protocol
// -----------------------------------------------------------------------

#[derive(Debug)]
enum Control {
    Pty {
        term: String,
        cols: u32,
        rows: u32,
        modes: Vec<(Pty, u32)>,
        reply: oneshot::Sender<Result<()>>,
    },
    Env {
        key: String,
        value: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Exec {
        command: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Shell {
        reply: oneshot::Sender<Result<()>>,
    },
    Subsystem {
        name: String,
        reply: oneshot::Sender<Result<()>>,
    },
    WindowChange {
        cols: u32,
        rows: u32,
    },
    Signal {
        signal: String,
    },
    Eof,
}

/// A `TargetSession` backed by a raw outbound russh client channel.
pub(crate) struct DirectSshSession {
    control_tx: mpsc::Sender<Control>,
    stdin_tx: mpsc::Sender<Vec<u8>>,
    events_rx: mpsc::UnboundedReceiver<SessionEvent>,
}

impl DirectSshSession {
    /// Wrap an already-opened session channel from an authenticated handle.
    pub(crate) fn new(channel: Channel<client::Msg>) -> Self {
        let (control_tx, control_rx) = mpsc::channel::<Control>(32);
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();

        tokio::spawn(driver(channel, control_rx, stdin_rx, events_tx));

        Self {
            control_tx,
            stdin_tx,
            events_rx,
        }
    }
}

async fn driver(
    mut channel: Channel<client::Msg>,
    mut control_rx: mpsc::Receiver<Control>,
    mut stdin_rx: mpsc::Receiver<Vec<u8>>,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
) {
    let mut stdin_open = true;
    loop {
        tokio::select! {
            // Forward stdin bytes (or close stdin when the sender drops).
            stdin = stdin_rx.recv(), if stdin_open => match stdin {
                Some(bytes) => {
                    if channel.data(Cursor::new(bytes)).await.is_err() {
                        break;
                    }
                }
                None => {
                    let _ = channel.eof().await;
                    stdin_open = false;
                }
            },
            // Apply a control request.
            ctrl = control_rx.recv() => match ctrl {
                Some(Control::Pty { term, cols, rows, modes, reply }) => {
                    let r = channel
                        .request_pty(true, &term, cols, rows, 0, 0, &modes)
                        .await;
                    let _ = reply.send(r.map_err(Into::into));
                }
                Some(Control::Env { key, value, reply }) => {
                    let r = channel.set_env(true, key, value).await;
                    let _ = reply.send(r.map_err(Into::into));
                }
                Some(Control::Exec { command, reply }) => {
                    let r = channel.exec(true, command).await;
                    let _ = reply.send(r.map_err(Into::into));
                }
                Some(Control::Shell { reply }) => {
                    let r = channel.request_shell(true).await;
                    let _ = reply.send(r.map_err(Into::into));
                }
                Some(Control::Subsystem { name, reply }) => {
                    let r = channel.request_subsystem(true, name).await;
                    let _ = reply.send(r.map_err(Into::into));
                }
                Some(Control::WindowChange { cols, rows }) => {
                    let _ = channel.window_change(cols, rows, 0, 0).await;
                }
                Some(Control::Signal { signal }) => {
                    let _ = channel.signal(parse_sig(&signal)).await;
                }
                Some(Control::Eof) => {
                    let _ = channel.eof().await;
                }
                None => break,
            },
            // Drain channel messages into events.
            msg = channel.wait() => match msg {
                Some(ChannelMsg::Data { data }) => {
                    if events_tx.send(SessionEvent::Stdout(data.to_vec())).is_err() {
                        break;
                    }
                }
                Some(ChannelMsg::ExtendedData { data, .. }) => {
                    if events_tx.send(SessionEvent::Stderr(data.to_vec())).is_err() {
                        break;
                    }
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    let _ = events_tx.send(SessionEvent::ExitStatus(exit_status as i32));
                }
                Some(ChannelMsg::ExitSignal { signal_name, .. }) => {
                    let _ = events_tx.send(SessionEvent::ExitSignal(format!("{signal_name:?}")));
                }
                Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => {
                    let _ = events_tx.send(SessionEvent::Eof);
                    break;
                }
                _ => {}
            },
        }
    }
}

fn parse_sig(name: &str) -> russh::Sig {
    // russh exposes a limited POSIX signal set; anything else is carried as
    // a custom signal name.
    use russh::Sig::*;
    match name.to_ascii_uppercase().as_str() {
        "HUP" => HUP,
        "INT" => INT,
        "QUIT" => QUIT,
        "ILL" => ILL,
        "ABRT" | "IOT" => ABRT,
        "FPE" => FPE,
        "KILL" => KILL,
        "PIPE" => PIPE,
        "ALRM" => ALRM,
        "TERM" => TERM,
        "SEGV" => SEGV,
        "USR1" => USR1,
        other => Custom(other.to_string()),
    }
}

/// Send a control message that carries a reply channel and await its result.
async fn request(
    control_tx: &mpsc::Sender<Control>,
    build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control,
) -> Result<()> {
    let (rtx, rrx) = oneshot::channel();
    control_tx
        .send(build(rtx))
        .await
        .map_err(|_| anyhow::anyhow!("session closed"))?;
    rrx.await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("session closed")))
}

#[async_trait]
impl TargetSession for DirectSshSession {
    async fn request_pty(
        &mut self,
        term: &str,
        cols: u32,
        rows: u32,
        modes: &[(Pty, u32)],
    ) -> Result<()> {
        request(&self.control_tx, |reply| Control::Pty {
            term: term.to_string(),
            cols,
            rows,
            modes: modes.to_vec(),
            reply,
        })
        .await
    }

    async fn set_env(&mut self, key: &str, value: &str) -> Result<()> {
        request(&self.control_tx, |reply| Control::Env {
            key: key.to_string(),
            value: value.to_string(),
            reply,
        })
        .await
    }

    async fn exec(&mut self, command: &str) -> Result<()> {
        request(&self.control_tx, |reply| Control::Exec {
            command: command.to_string(),
            reply,
        })
        .await
    }

    async fn shell(&mut self) -> Result<()> {
        request(&self.control_tx, |reply| Control::Shell { reply }).await
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        request(&self.control_tx, |reply| Control::Subsystem {
            name: name.to_string(),
            reply,
        })
        .await
    }

    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()> {
        let _ = self
            .control_tx
            .send(Control::WindowChange { cols, rows })
            .await;
        Ok(())
    }

    async fn signal(&mut self, signal: &str) -> Result<()> {
        let _ = self
            .control_tx
            .send(Control::Signal {
                signal: signal.to_string(),
            })
            .await;
        Ok(())
    }

    async fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
        self.stdin_tx
            .send(data.to_vec())
            .await
            .map_err(|_| anyhow::anyhow!("session closed"))?;
        Ok(())
    }

    async fn eof(&mut self) -> Result<()> {
        let _ = self.control_tx.send(Control::Eof).await;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<SessionEvent> {
        self.events_rx.recv().await
    }
}
