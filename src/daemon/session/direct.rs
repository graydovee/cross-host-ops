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
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use russh::Channel;
use russh::ChannelId;
use russh::ChannelMsg;
use russh::Sig;
use russh::client::{self};
use russh::keys::ssh_key::HashAlg;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use russh::Pty;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::config::{AppConfig, DirectAuth};

use super::{SessionEvent, TargetSession};

/// Sentinel value meaning "no exit status captured yet".
const NO_EXIT: u32 = u32::MAX;

/// Open+authenticate a russh client handle for a direct SSH target.
///
/// Returns the handle plus a shared exit-code cell that the client
/// [`ClientHandler`] populates via the `exit_status` callback (a reliable
/// fallback when `channel.wait()` drops `ExitStatus` due to buffer pressure).
pub(crate) async fn connect_authenticated(
    host: &str,
    port: u16,
    user: &str,
    auth: &DirectAuth,
    config: &AppConfig,
) -> Result<(client::Handle<ClientHandler>, Arc<AtomicU32>)> {
    let exit_code = Arc::new(AtomicU32::new(NO_EXIT));
    let handler = ClientHandler { exit_code: exit_code.clone() };
    let mut handle = connect_handle(host, port, config, handler).await?;
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
    Ok((handle, exit_code))
}

/// russh client handler that accepts any host key and captures the remote
/// process's exit status in a shared atomic (reliable fallback).
pub(crate) struct ClientHandler {
    exit_code: Arc<AtomicU32>,
}

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn exit_status(
        &mut self,
        _channel: ChannelId,
        exit_status: u32,
        _session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        self.exit_code.store(exit_status, Ordering::Relaxed);
        Ok(())
    }

    async fn exit_signal(
        &mut self,
        _channel: ChannelId,
        _signal_name: Sig,
        _core_dumped: bool,
        _error_message: &str,
        _lang_tag: &str,
        _session: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        self.exit_code.store(255, Ordering::Relaxed);
        Ok(())
    }
}

async fn connect_handle(
    host: &str,
    port: u16,
    config: &AppConfig,
    handler: ClientHandler,
) -> Result<client::Handle<ClientHandler>> {
    let client_config = client::Config {
        keepalive_interval: Some(config.ssh.keepalive_interval),
        inactivity_timeout: Some(config.ssh.keepalive_interval * 2),
        ..Default::default()
    };
    let handle = timeout(
        config.ssh.connect_timeout,
        client::connect(Arc::new(client_config), (host, port), handler),
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
    // best_supported_rsa_hash returns None for non-RSA keys (ed25519, ecdsa)
    // — that is correct: only RSA keys carry a hash algorithm.
    let hash_alg = handle.best_supported_rsa_hash().await?.flatten();
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
    /// `exit_code` is the shared cell populated by the `ClientHandler`'s
    /// `exit_status` callback — used as a fallback when `channel.wait()` drops
    /// the ExitStatus message.
    pub(crate) fn new(channel: Channel<client::Msg>, exit_code: Arc<AtomicU32>) -> Self {
        let (control_tx, control_rx) = mpsc::channel::<Control>(32);
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();

        tokio::spawn(driver(channel, control_rx, stdin_rx, events_tx, exit_code));

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
    exit_code: Arc<AtomicU32>,
) {
    let mut stdin_open = true;
    let mut exit_sent = false;
    loop {
        tokio::select! {
            stdin = stdin_rx.recv(), if stdin_open => match stdin {
                Some(bytes) => {
                    if channel.data(Cursor::new(bytes)).await.is_err() { break; }
                }
                None => {
                    let _ = channel.eof().await;
                    stdin_open = false;
                }
            },
            ctrl = control_rx.recv() => match ctrl {
                Some(Control::Pty { term, cols, rows, modes, reply }) => {
                    let r = channel.request_pty(true, &term, cols, rows, 0, 0, &modes).await;
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
            msg = channel.wait() => match msg {
                Some(ChannelMsg::Data { data }) => {
                    if events_tx.send(SessionEvent::Stdout(data.to_vec())).is_err() { break; }
                }
                Some(ChannelMsg::ExtendedData { data, .. }) => {
                    if events_tx.send(SessionEvent::Stderr(data.to_vec())).is_err() { break; }
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_sent = true;
                    let _ = events_tx.send(SessionEvent::ExitStatus(exit_status as i32));
                }
                Some(ChannelMsg::ExitSignal { signal_name, .. }) => {
                    exit_sent = true;
                    let _ = events_tx.send(SessionEvent::ExitSignal(format!("{signal_name:?}")));
                }
                Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => {
                    // ExitStatus may have been dropped by russh's bounded channel
                    // receiver. Fall back to the Handler callback's captured code.
                    if !exit_sent {
                        let code = exit_code.load(Ordering::Relaxed);
                        if code != NO_EXIT {
                            let _ = events_tx.send(SessionEvent::ExitStatus(code as i32));
                        } else {
                            let _ = events_tx.send(SessionEvent::Eof);
                        }
                    }
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
