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
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::Result;
use russh::Pty;
use russh::keys::ssh_key;
use russh::server::{self, Auth, Msg};
use russh::{Channel, ChannelId, Sig};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::session::{self, SessionEvent, TargetSession};
use super::DaemonState;

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
    pty: Option<PtyParams>,
    env: Vec<(String, String)>,
    /// Messages received before the bridge task exists (data/resize/... that
    /// arrived between channel open and exec/shell/subsystem).
    pending: Vec<ProxyMsg>,
}

pub(super) struct ProxySshHandler {
    state: DaemonState,
    authorized_keys_path: String,
    peer: Option<SocketAddr>,
    user: Option<String>,
    channels: HashMap<ChannelId, ChannelEntry>,
    /// Senders to running bridge tasks, keyed by channel id.
    bridges: HashMap<ChannelId, mpsc::Sender<ProxyMsg>>,
}

impl server::Server for ProxySshServer {
    type Handler = ProxySshHandler;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        ProxySshHandler {
            state: self.state.clone(),
            authorized_keys_path: self.authorized_keys_path.clone(),
            peer: peer_addr,
            user: None,
            channels: HashMap::new(),
            bridges: HashMap::new(),
        }
    }
}

// -----------------------------------------------------------------------
// Bridge: drives a TargetSession and an inbound SSH channel.
// -----------------------------------------------------------------------

async fn forward_or_buffer(
    bridges: &mut HashMap<ChannelId, mpsc::Sender<ProxyMsg>>,
    channels: &mut HashMap<ChannelId, ChannelEntry>,
    channel: ChannelId,
    msg: ProxyMsg,
) {
    if let Some(tx) = bridges.get(&channel) {
        let _ = tx.send(msg).await;
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
    bridges: &mut HashMap<ChannelId, mpsc::Sender<ProxyMsg>>,
) {
    let (tx, rx) = mpsc::channel::<ProxyMsg>(64);
    // Flush buffered messages in order before the task begins consuming.
    let mut pending = entry.pending;
    let tx_for_flush = tx.clone();
    tokio::spawn(async move {
        for msg in pending.drain(..) {
            if tx_for_flush.send(msg).await.is_err() {
                return;
            }
        }
    });
    let state = state.clone();
    let user = user.to_string();
    tokio::spawn(async move {
        let route = match super::resolve_target_with_merged_view(&state, &user).await {
            Ok(r) => r,
            Err(e) => {
                warn!(target = %user, error = %format!("{e:#}"), "proxy: failed to resolve target");
                return;
            }
        };
        let route = match route.routes.into_iter().next() {
            Some(r) => r,
            None => {
                warn!(target = %user, "proxy: no route for target");
                return;
            }
        };
        let mut sess: Box<dyn TargetSession> = match session::open_target_session(&state, &route).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target = %user, error = %format!("{e:#}"), "proxy: failed to open session");
                return;
            }
        };

        // Apply buffered pty + env.
        if let Some(pty) = &entry.pty {
            let _ = sess
                .request_pty(&pty.term, pty.cols, pty.rows, &[])
                .await;
        }
        for (k, v) in &entry.env {
            let _ = sess.set_env(k, v).await;
        }

        // Start the backend.
        let started = match start {
            SessionStart::Exec(cmd) => sess.exec(&cmd).await,
            SessionStart::Shell => sess.shell().await,
            SessionStart::Subsystem(name) => sess.subsystem(&name).await,
        };
        if let Err(e) = started {
            warn!(target = %user, error = %format!("{e:#}"), "proxy: failed to start session");
            return;
        }

        let channel = entry.channel;
        let mut msg_rx = rx;
        loop {
            tokio::select! {
                msg = msg_rx.recv() => match msg {
                    Some(ProxyMsg::Data(d)) => { let _ = sess.write_stdin(&d).await; }
                    Some(ProxyMsg::Resize(c, r)) => { let _ = sess.window_change(c, r).await; }
                    Some(ProxyMsg::Signal(s)) => { let _ = sess.signal(&s).await; }
                    Some(ProxyMsg::Eof) | None => { let _ = sess.eof().await; }
                },
                ev = sess.next_event() => match ev {
                    Some(SessionEvent::Stdout(d)) => { let _ = channel.data(Cursor::new(d)).await; }
                    Some(SessionEvent::Stderr(d)) => { let _ = channel.extended_data(1, Cursor::new(d)).await; }
                    Some(SessionEvent::ExitStatus(c)) => {
                        let _ = channel.exit_status(c as u32).await;
                        let _ = channel.eof().await;
                        let _ = channel.close().await;
                        return;
                    }
                    Some(SessionEvent::ExitSignal(_)) => {
                        let _ = channel.exit_status(255).await;
                        let _ = channel.close().await;
                        return;
                    }
                    Some(SessionEvent::Eof) | None => {
                        let _ = channel.eof().await;
                        let _ = channel.close().await;
                        return;
                    }
                },
            }
        }
        // `channel_id` kept for future per-channel logging.
        let _ = channel_id;
    });
    bridges.insert(channel_id, tx);
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
        let ok = super::authorized_keys::is_authorized_key(
            Path::new(&self.authorized_keys_path),
            key,
        )
        .unwrap_or(false);
        if ok {
            self.user = Some(user.to_string());
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
                pty: None,
                env: Vec::new(),
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
            entry.pty = Some(PtyParams {
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
        forward_or_buffer(&mut self.bridges, &mut self.channels, channel, ProxyMsg::Signal(name))
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
}

impl ProxySshHandler {
    fn start_session(&mut self, channel: ChannelId, start: SessionStart) -> bool {
        let user = match &self.user {
            Some(u) => u.clone(),
            None => return false,
        };
        let Some(entry) = self.channels.remove(&channel) else {
            return false;
        };
        spawn_bridge(&self.state, &user, entry, channel, start, &mut self.bridges);
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
