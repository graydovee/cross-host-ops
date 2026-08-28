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
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::Result;
use russh::Channel;
use russh::ChannelId;
use russh::ChannelMsg;
use russh::Sig;
use russh::client::{self};
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::config::{AppConfig, DirectAuth};

use super::{SessionCommand, SessionEvent, SessionStream, SessionWriter, TargetSession};

/// Sentinel value meaning "no exit status captured yet".
pub(crate) const NO_EXIT: u32 = u32::MAX;

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
    let handler = ClientHandler {
        exit_code: exit_code.clone(),
    };
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
        inactivity_timeout: config.ssh.inactivity_timeout,
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
// Session: writer/stream halves over a pooled channel
// -----------------------------------------------------------------------

/// A `TargetSession` backed by a raw outbound russh client channel, split into
/// its writer/stream halves.
///
/// The channel is split into read/write halves driven by two independent
/// tasks. This matters for flow control: when the remote stalls reading stdin
/// (e.g. a shell blocked writing output), a parked `data()` send must not
/// stop the reader from consuming output — a single `select!` over both
/// directions re-creates the mutual deadlock the split architecture removes.
pub(crate) struct DirectSshSession {
    writer: Option<SessionWriter>,
    stream: Option<SessionStream>,
}

impl DirectSshSession {
    /// Wrap a channel opened on a *pooled* handle. `exit_code` is reset to
    /// `NO_EXIT` (the handle is reused across execs, so a stale code from a
    /// prior exec must not leak). `on_done` is invoked after both driver
    /// tasks terminate — the gateway uses it to return or discard the handle
    /// lease.
    pub(crate) fn new(
        channel: Channel<client::Msg>,
        exit_code: Arc<AtomicU32>,
        on_done: Box<dyn FnOnce() + Send>,
    ) -> Self {
        exit_code.store(NO_EXIT, Ordering::Relaxed);
        // Commands and stdin payload deliberately share ONE ordered channel:
        // the API contract is "start before data, data before eof". Two
        // separate channels consumed by `select!` reorder messages at random —
        // data or eof can reach the SSH channel before the exec / subsystem
        // request, hanging the session or dropping stdin.
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();

        let (read_half, write_half) = channel.split();
        let writer = tokio::spawn(write_driver(write_half, cmd_rx));
        let reader = tokio::spawn(read_driver(read_half, events_tx, exit_code));
        tokio::spawn(async move {
            let _ = writer.await;
            let _ = reader.await;
            on_done();
        });

        Self {
            writer: Some(SessionWriter { tx: cmd_tx }),
            stream: Some(SessionStream { rx: events_rx }),
        }
    }
}

impl TargetSession for DirectSshSession {
    fn split(mut self: Box<Self>) -> (SessionWriter, SessionStream) {
        (
            self.writer.take().expect("direct session split twice"),
            self.stream.take().expect("direct session split twice"),
        )
    }
}

/// Write half: consume commands in FIFO order and forward them to the SSH
/// channel. Parking here (remote window exhausted because the peer stopped
/// reading stdin) pauses stdin only — the read driver keeps consuming output,
/// which is exactly what lets the peer eventually resume.
async fn write_driver(
    mut write_half: russh::ChannelWriteHalf<client::Msg>,
    mut cmd_rx: mpsc::Receiver<SessionCommand>,
) {
    let mut stdin_open = true;
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            SessionCommand::Pty {
                term,
                cols,
                rows,
                modes,
                reply,
            } => {
                let r = write_half
                    .request_pty(true, &term, cols, rows, 0, 0, &modes)
                    .await;
                let _ = reply.send(r.map_err(Into::into));
            }
            SessionCommand::Env { key, value, reply } => {
                let r = write_half.set_env(true, key, value).await;
                let _ = reply.send(r.map_err(Into::into));
            }
            SessionCommand::Exec { command, reply } => {
                let r = write_half.exec(true, command).await;
                let _ = reply.send(r.map_err(Into::into));
            }
            SessionCommand::Shell { reply } => {
                let r = write_half.request_shell(true).await;
                let _ = reply.send(r.map_err(Into::into));
            }
            SessionCommand::Subsystem { name, reply } => {
                let r = write_half.request_subsystem(true, name).await;
                let _ = reply.send(r.map_err(Into::into));
            }
            SessionCommand::Resize { cols, rows } => {
                let _ = write_half.window_change(cols, rows, 0, 0).await;
            }
            SessionCommand::Signal { signal } => {
                let _ = write_half.signal(parse_sig(&signal)).await;
            }
            SessionCommand::Eof => {
                let _ = write_half.eof().await;
                stdin_open = false;
            }
            SessionCommand::Data { bytes } if stdin_open => {
                if write_half.data(Cursor::new(bytes)).await.is_err() {
                    break;
                }
            }
            SessionCommand::Data { .. } => {}
        }
    }
}

/// Read half: surface channel messages as session events until the channel
/// closes.
async fn read_driver(
    mut read_half: russh::ChannelReadHalf,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
    exit_code: Arc<AtomicU32>,
) {
    let mut exit_sent = false;
    while let Some(msg) = read_half.wait().await {
        match msg {
            ChannelMsg::Data { data } => {
                if events_tx.send(SessionEvent::Stdout(data.to_vec())).is_err() {
                    break;
                }
            }
            ChannelMsg::ExtendedData { data, .. } => {
                if events_tx.send(SessionEvent::Stderr(data.to_vec())).is_err() {
                    break;
                }
            }
            ChannelMsg::ExitStatus { exit_status } => {
                exit_sent = true;
                let _ = events_tx.send(SessionEvent::ExitStatus(exit_status as i32));
            }
            ChannelMsg::ExitSignal { signal_name, .. } => {
                exit_sent = true;
                let _ = events_tx.send(SessionEvent::ExitSignal(format!("{signal_name:?}")));
            }
            ChannelMsg::Eof | ChannelMsg::Close => {
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
        }
    }
    if !exit_sent {
        // wait() returned None (channel closed without Eof/Close) — still
        // resolve the session with the best exit info we have.
        let code = exit_code.load(Ordering::Relaxed);
        if code != NO_EXIT {
            let _ = events_tx.send(SessionEvent::ExitStatus(code as i32));
        } else {
            let _ = events_tx.send(SessionEvent::Eof);
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
