// Transparent SSH proxy server.
//
// Listens on the proxy port (default 2222). A human runs `ssh <node>@<xhod>
// -p 2222`; the SSH username selects the target. After public-key auth against
// the proxy's authorized_keys, each session channel is bridged to a unified
// [`TargetSession`] obtained via [`crate::daemon::session::open_target_session`]:
// inbound SSH requests (pty/exec/shell/subsystem/data/resize/signal) drive the
// session, and session events are written back over the inbound channel. This
// gives transparent scp/sftp/exec/pty compatibility for direct and localhost
// targets.

use std::collections::HashMap;
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use russh::Pty;
use russh::keys::ssh_key::{self, HashAlg};
use russh::server::{self, Auth, Msg};
use russh::{Channel, ChannelId, ChannelMsg, ChannelWriteHalf, Sig};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::DaemonState;
use super::gateway::{GatewayKind, Route};
use super::session::{self, SessionEvent, SessionStream, SessionWriter};

// -----------------------------------------------------------------------
// Server + Handler types
// -----------------------------------------------------------------------

#[derive(Clone)]
pub(super) struct ProxySshServer {
    pub state: DaemonState,
    pub authorized_keys_path: String,
}

struct PtyParams {
    term: String,
    cols: u32,
    rows: u32,
}

/// Messages forwarded from SSH callbacks to a channel's bridge task.
enum ProxyMsg {
    Data(Vec<u8>),
    Resize(u32, u32),
    Signal(String),
    Eof,
}

struct ChannelEntry {
    channel: Channel<Msg>,
    options: SessionOptions,
    /// Messages received before the bridge task exists (data/resize/... that
    /// arrived between channel open and exec/shell/subsystem).
    pending: Vec<ProxyMsg>,
}

struct SessionOptions {
    pty: Option<PtyParams>,
    env: Vec<(String, String)>,
}

/// Dropping a channel or its connection cancels the entire bridge, including
/// setup and both streaming directions. No detached child tasks are needed.
struct Bridge {
    tx: mpsc::UnboundedSender<ProxyMsg>,
    task: JoinHandle<()>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) struct ProxySshHandler {
    state: DaemonState,
    authorized_keys_path: String,
    peer: Option<SocketAddr>,
    user: Option<String>,
    /// SHA-256 fingerprint of the accepted public key (for audit logging).
    accepted_fingerprint: Option<String>,
    channels: HashMap<ChannelId, ChannelEntry>,
    /// Senders to running bridge tasks, keyed by channel id.
    bridges: HashMap<ChannelId, Bridge>,
}

impl server::Server for ProxySshServer {
    type Handler = ProxySshHandler;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        ProxySshHandler {
            state: self.state.clone(),
            authorized_keys_path: self.authorized_keys_path.clone(),
            peer: peer_addr,
            user: None,
            accepted_fingerprint: None,
            channels: HashMap::new(),
            bridges: HashMap::new(),
        }
    }
}

// -----------------------------------------------------------------------
// Bridge: drives a TargetSession and an inbound SSH channel.
// -----------------------------------------------------------------------

async fn forward_or_buffer(
    bridges: &mut HashMap<ChannelId, Bridge>,
    channels: &mut HashMap<ChannelId, ChannelEntry>,
    channel: ChannelId,
    msg: ProxyMsg,
) {
    if let Some(bridge) = bridges.get(&channel) {
        // Unbounded: handler.data() runs INSIDE the russh session loop, so it
        // must never park (a parked data() stops the loop from reading the
        // socket and answering keepalives - the historical terminal freeze).
        // Backpressure is enforced downstream by the pump -> writer -> PTY
        // chain instead.
        let _ = bridge.tx.send(msg);
    } else if let Some(entry) = channels.get_mut(&channel) {
        entry.pending.push(msg);
    }
}

fn spawn_bridge(
    state: &DaemonState,
    user: &str,
    entry: ChannelEntry,
    channel_id: ChannelId,
    start: SessionStart,
    caller: crate::oversight::Caller,
    bridges: &mut HashMap<ChannelId, Bridge>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ProxyMsg>();
    // Queue buffered messages synchronously: a spawned flush can reorder them
    // after new data delivered by the handler.
    for msg in entry.pending {
        let _ = tx.send(msg);
    }
    let state = state.clone();
    let user = user.to_string();
    let task = tokio::spawn(async move {
        let (mut channel_read, channel) = entry.channel.split();
        // Start draining BEFORE setup. russh sends duplicate inbound messages
        // to this bounded queue before calling the handler; leaving it unread
        // blocks the SSH loop and prevents cancellation/keepalive callbacks.
        let drain = async {
            while let Some(msg) = channel_read.wait().await {
                if matches!(msg, ChannelMsg::Close) {
                    break;
                }
            }
        };
        tokio::select! {
            _ = drain => {},
            _ = run_bridge(&state, &user, &channel, &mut rx, entry.options, start, caller) => {},
        }
    });
    bridges.insert(channel_id, Bridge { tx, task });
}

struct PreparedSession {
    writer: SessionWriter,
    events: SessionStream,
    route: Route,
    gateway_kind: Option<GatewayKind>,
}

async fn prepare_session(
    state: &DaemonState,
    user: &str,
    pty: &Option<PtyParams>,
    env: &[(String, String)],
    start: &SessionStart,
) -> Result<PreparedSession> {
    let resolved = super::resolve_target_with_merged_view(state, user)
        .await
        .context("failed to resolve target")?;
    let route = resolved
        .routes
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no route for target"))?;
    let gateway_kind = state
        .find_gateway_any(&route.gateway_name)
        .await
        .map(|g| g.kind());
    let sess = session::open_target_session(state, &route)
        .await
        .context("failed to open session")?;
    let (writer, events) = sess.split();
    if let Some(pty) = pty {
        writer
            .request_pty(&pty.term, pty.cols, pty.rows, &[])
            .await
            .context("failed to request PTY")?;
    }
    for (k, v) in env {
        // Environment requests are optional, as with a regular SSH server.
        let _ = writer.set_env(k, v).await;
    }
    match start {
        SessionStart::Exec(cmd) => writer.exec(cmd).await,
        SessionStart::Shell => writer.shell().await,
        SessionStart::Subsystem(name) => writer.subsystem(name).await,
    }
    .context("failed to start session")?;
    Ok(PreparedSession {
        writer,
        events,
        route,
        gateway_kind,
    })
}

#[derive(Debug)]
enum SetupError {
    Failed(anyhow::Error),
    Timeout,
    Cancelled,
    Disconnected,
}

/// Keep accepting input while setup is pending, but only treat terminal input
/// as an interrupt when a PTY was requested. Binary protocols may contain ETX.
async fn await_setup<T>(
    setup: impl Future<Output = Result<T>>,
    rx: &mut mpsc::UnboundedReceiver<ProxyMsg>,
    has_pty: bool,
    timeout: Duration,
) -> Result<(T, Vec<ProxyMsg>), SetupError> {
    let setup = tokio::time::timeout(timeout, setup);
    tokio::pin!(setup);
    let mut buffered = Vec::new();
    loop {
        tokio::select! {
            result = &mut setup => return match result {
                Ok(Ok(session)) => Ok((session, buffered)),
                Ok(Err(e)) => Err(SetupError::Failed(e)),
                Err(_) => Err(SetupError::Timeout),
            },
            msg = rx.recv() => match msg {
                Some(ProxyMsg::Data(d)) if has_pty && d.contains(&3) => return Err(SetupError::Cancelled),
                Some(ProxyMsg::Signal(s)) if s.eq_ignore_ascii_case("INT") => return Err(SetupError::Cancelled),
                Some(msg) => buffered.push(msg),
                None => return Err(SetupError::Disconnected),
            },
        }
    }
}

async fn close_channel(channel: &ChannelWriteHalf<Msg>) {
    let _ = channel.eof().await;
    let _ = channel.close().await;
}

async fn run_bridge(
    state: &DaemonState,
    user: &str,
    channel: &ChannelWriteHalf<Msg>,
    rx: &mut mpsc::UnboundedReceiver<ProxyMsg>,
    options: SessionOptions,
    start: SessionStart,
    caller: crate::oversight::Caller,
) {
    let timeout = state.config.read().await.ssh.connect_timeout;
    let setup = prepare_session(state, user, &options.pty, &options.env, &start);
    let (prepared, buffered) = match await_setup(setup, rx, options.pty.is_some(), timeout).await {
        Ok(ready) => ready,
        Err(SetupError::Disconnected) => return,
        Err(error) => {
            let (code, reason) = match error {
                SetupError::Failed(e) => (255, format!("{e:#}")),
                SetupError::Timeout => (124, format!("session setup timed out after {timeout:?}")),
                SetupError::Cancelled => (130, "session setup cancelled".to_string()),
                SetupError::Disconnected => unreachable!(),
            };
            warn!(target = %user, error = %reason, "proxy: session setup failed");
            let message = format!("xhod: target '{user}': {reason}\r\n");
            let _ = channel.extended_data(1, Cursor::new(message)).await;
            let _ = channel.exit_status(code).await;
            close_channel(channel).await;
            return;
        }
    };
    let PreparedSession {
        writer,
        mut events,
        route,
        gateway_kind,
    } = prepared;
    let (session_kind, command_or_name): (&str, String) = match &start {
        SessionStart::Exec(cmd) => ("exec", cmd.clone()),
        SessionStart::Shell => ("shell", "(interactive)".to_string()),
        SessionStart::Subsystem(name) => ("subsystem", name.clone()),
    };
    let mut started_event = crate::oversight::AuditEvent::new(
        caller.source,
        crate::oversight::OperationKind::Proxy.as_str(),
        "started",
    );
    started_event.target_input = Some(user.to_string());
    started_event.gateway = Some(route.gateway_name.clone());
    started_event.end_target = Some(route.end_target.clone());
    started_event.gateway_kind = gateway_kind.map(|k| k.to_string());
    started_event.session_kind = Some(session_kind.to_string());
    started_event.command = Some(command_or_name);
    if crate::oversight::audit::include_identity() {
        started_event.caller_source = Some(caller.source.to_string());
        started_event.caller_peer = caller.peer_addr.clone();
        started_event.caller_ssh_user = caller.ssh_user.clone();
        started_event.caller_key_fingerprint = caller.key_fingerprint.clone();
        started_event.caller_via_token = Some(caller.via_token);
    }
    crate::oversight::audit::record(&started_event);

    // Independent futures keep flow control in one direction from blocking
    // the other. Completing either future drops the other and its resources.
    let uplink = async {
        for msg in buffered {
            forward_to_session(&writer, msg).await?;
        }
        while let Some(msg) = rx.recv().await {
            forward_to_session(&writer, msg).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let downlink = async {
        while let Some(ev) = events.next().await {
            match ev {
                SessionEvent::Stdout(d) => channel.data(Cursor::new(d)).await?,
                SessionEvent::Stderr(d) => channel.extended_data(1, Cursor::new(d)).await?,
                SessionEvent::ExitStatus(c) => {
                    let mut ev = crate::oversight::AuditEvent::new(
                        caller.source,
                        crate::oversight::OperationKind::Proxy.as_str(),
                        "completed",
                    );
                    ev.target_input = Some(user.to_string());
                    ev.gateway = Some(route.gateway_name.clone());
                    ev.end_target = Some(route.end_target.clone());
                    ev.gateway_kind = gateway_kind.map(|k| k.to_string());
                    ev.session_kind = Some(session_kind.to_string());
                    ev.exit_code = Some(c);
                    crate::oversight::audit::record(&ev);
                    channel.exit_status(c as u32).await?;
                    break;
                }
                SessionEvent::ExitSignal(_) => {
                    channel.exit_status(255).await?;
                    break;
                }
                SessionEvent::Eof => break,
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::select! {
        result = uplink => result,
        result = downlink => result,
    };
    if let Err(e) = result {
        warn!(target = %user, error = %e, "proxy: session forwarding failed");
    }
    close_channel(channel).await;
}

async fn forward_to_session(writer: &SessionWriter, msg: ProxyMsg) -> Result<()> {
    match msg {
        ProxyMsg::Data(d) => writer.write_stdin(&d).await,
        ProxyMsg::Resize(c, r) => writer.window_change(c, r).await,
        ProxyMsg::Signal(s) => writer.signal(&s).await,
        ProxyMsg::Eof => writer.eof().await,
    }
}

enum SessionStart {
    Exec(String),
    Shell,
    Subsystem(String),
}

// -----------------------------------------------------------------------
// Handler impl
// -----------------------------------------------------------------------

impl server::Handler for ProxySshHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        user: &str,
        key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        // The username selects the target; auth is by proxy authorized_keys.
        let ok =
            super::authorized_keys::is_authorized_key(Path::new(&self.authorized_keys_path), key)
                .unwrap_or(false);
        if ok {
            self.user = Some(user.to_string());
            self.accepted_fingerprint = Some(key.fingerprint(HashAlg::Sha256).to_string());
            info!(peer = ?self.peer, ssh_user = %user, "proxy: accepted publickey");
            Ok(Auth::Accept)
        } else {
            Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            })
        }
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        self.channels.insert(
            channel.id(),
            ChannelEntry {
                channel,
                options: SessionOptions {
                    pty: None,
                    env: Vec::new(),
                },
                pending: Vec::new(),
            },
        );
        Ok(true)
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(Pty, u32)],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        if let Some(entry) = self.channels.get_mut(&channel) {
            entry.options.pty = Some(PtyParams {
                term: term.to_string(),
                cols: col_width,
                rows: row_height,
            });
            let _ = session.channel_success(channel);
        } else {
            let _ = session.channel_failure(channel);
        }
        Ok(())
    }

    async fn env_request(
        &mut self,
        channel: ChannelId,
        variable_name: &str,
        variable_value: &str,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        if let Some(entry) = self.channels.get_mut(&channel) {
            entry
                .options
                .env
                .push((variable_name.to_string(), variable_value.to_string()));
            let _ = session.channel_success(channel);
        } else {
            let _ = session.channel_failure(channel);
        }
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        let ok = self.start_session(channel, SessionStart::Shell);
        reply(session, channel, ok);
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        let command = String::from_utf8_lossy(data).to_string();
        let ok = self.start_session(channel, SessionStart::Exec(command));
        reply(session, channel, ok);
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        let ok = self.start_session(channel, SessionStart::Subsystem(name.to_string()));
        reply(session, channel, ok);
        Ok(())
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        forward_or_buffer(
            &mut self.bridges,
            &mut self.channels,
            channel,
            ProxyMsg::Data(data.to_vec()),
        )
        .await;
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        forward_or_buffer(
            &mut self.bridges,
            &mut self.channels,
            channel,
            ProxyMsg::Resize(col_width, row_height),
        )
        .await;
        Ok(())
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        signal: Sig,
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        let name = format!("{signal:?}");
        forward_or_buffer(
            &mut self.bridges,
            &mut self.channels,
            channel,
            ProxyMsg::Signal(name),
        )
        .await;
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        forward_or_buffer(
            &mut self.bridges,
            &mut self.channels,
            channel,
            ProxyMsg::Eof,
        )
        .await;
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        self.channels.remove(&channel);
        self.bridges.remove(&channel);
        Ok(())
    }
}

impl ProxySshHandler {
    fn start_session(&mut self, channel: ChannelId, start: SessionStart) -> bool {
        let user = match &self.user {
            Some(u) => u.clone(),
            None => return false,
        };
        let caller = crate::oversight::Caller::proxy(
            self.peer.map(|a| a.to_string()),
            user.clone(),
            self.accepted_fingerprint.clone(),
        );
        let Some(entry) = self.channels.remove(&channel) else {
            return false;
        };
        spawn_bridge(
            &self.state,
            &user,
            entry,
            channel,
            start,
            caller,
            &mut self.bridges,
        );
        true
    }
}

fn reply(session: &mut server::Session, channel: ChannelId, ok: bool) {
    let _ = if ok {
        session.channel_success(channel)
    } else {
        session.channel_failure(channel)
    };
}

#[cfg(test)]
mod tests;
