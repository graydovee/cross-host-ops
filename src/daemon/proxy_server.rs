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
use russh::keys::ssh_key::{self, HashAlg};
use russh::server::{self, Auth, Msg};
use russh::{Channel, ChannelId, Sig};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::DaemonState;
use super::session::{self, SessionEvent, TargetSession};

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
    /// SHA-256 fingerprint of the accepted public key (for audit logging).
    accepted_fingerprint: Option<String>,
    channels: HashMap<ChannelId, ChannelEntry>,
    /// Senders to running bridge tasks, keyed by channel id.
    bridges: HashMap<ChannelId, tokio::sync::mpsc::UnboundedSender<ProxyMsg>>,
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
    bridges: &mut HashMap<ChannelId, tokio::sync::mpsc::UnboundedSender<ProxyMsg>>,
    channels: &mut HashMap<ChannelId, ChannelEntry>,
    channel: ChannelId,
    msg: ProxyMsg,
) {
    if let Some(tx) = bridges.get(&channel) {
        // Unbounded: handler.data() runs INSIDE the russh session loop, so it
        // must never park (a parked data() stops the loop from reading the
        // socket and answering keepalives - the historical terminal freeze).
        // Backpressure is enforced downstream by the pump -> writer -> PTY
        // chain instead.
        let _ = tx.send(msg);
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
    bridges: &mut HashMap<ChannelId, tokio::sync::mpsc::UnboundedSender<ProxyMsg>>,
) {
    let (tx, rx) = mpsc::unbounded_channel::<ProxyMsg>();
    // Flush buffered messages in order before the task begins consuming.
    let mut pending = entry.pending;
    let tx_for_flush = tx.clone();
    tokio::spawn(async move {
        for msg in pending.drain(..) {
            if tx_for_flush.send(msg).is_err() {
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
        let gateway_kind = state
            .find_gateway_any(&route.gateway_name)
            .await
            .map(|g| g.kind());
        let sess: Box<dyn TargetSession> = match session::open_target_session(&state, &route).await
        {
            Ok(s) => s,
            Err(e) => {
                warn!(target = %user, error = %format!("{e:#}"), "proxy: failed to open session");
                return;
            }
        };
        let (writer, mut events) = sess.split();

        // Apply buffered pty + env.
        if let Some(pty) = &entry.pty {
            let _ = writer.request_pty(&pty.term, pty.cols, pty.rows, &[]).await;
        }
        for (k, v) in &entry.env {
            let _ = writer.set_env(k, v).await;
        }

        // Audit: proxy operation started.
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
        started_event.target_input = Some(user.clone());
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

        // Start the backend.
        let started = match start {
            SessionStart::Exec(cmd) => writer.exec(&cmd).await,
            SessionStart::Shell => writer.shell().await,
            SessionStart::Subsystem(name) => writer.subsystem(&name).await,
        };
        if let Err(e) = started {
            warn!(target = %user, error = %format!("{e:#}"), "proxy: failed to start session");
            return;
        }

        let (channel_read, channel) = entry.channel.split();
        let mut msg_rx = rx;

        // Drain the channel's inbound event queue. russh pushes a copy of
        // every channel message (data/eof/close/...) into a bounded queue
        // consumed only by `Channel::wait()`; with no reader, that queue
        // fills after `channel_buffer_size` messages and the session loop
        // then parks forever inside `chan.send(...)` — freezing keepalive
        // replies and wedging the whole connection. The bridge consumes the
        // same bytes through `handler.data`, so this half is discarded.
        let _drainer = tokio::spawn(async move {
            let mut channel = channel_read;
            while channel.wait().await.is_some() {}
        });

        // Downlink: session events → inbound SSH channel. Its own task so a
        // slow SSH consumer never stalls the stdin direction (see the
        // OpenSession handler for the symmetric rationale).
        let downlink = tokio::spawn(async move {
            while let Some(ev) = events.next().await {
                match ev {
                    SessionEvent::Stdout(d) => {
                        let _ = channel.data(Cursor::new(d)).await;
                    }
                    SessionEvent::Stderr(d) => {
                        let _ = channel.extended_data(1, Cursor::new(d)).await;
                    }
                    SessionEvent::ExitStatus(c) => {
                        let mut ev = crate::oversight::AuditEvent::new(
                            caller.source,
                            crate::oversight::OperationKind::Proxy.as_str(),
                            "completed",
                        );
                        ev.target_input = Some(user.clone());
                        ev.gateway = Some(route.gateway_name.clone());
                        ev.end_target = Some(route.end_target.clone());
                        ev.gateway_kind = gateway_kind.map(|k| k.to_string());
                        ev.session_kind = Some(session_kind.to_string());
                        ev.exit_code = Some(c);
                        crate::oversight::audit::record(&ev);
                        let _ = channel.exit_status(c as u32).await;
                        let _ = channel.eof().await;
                        let _ = channel.close().await;
                        return;
                    }
                    SessionEvent::ExitSignal(_) => {
                        let _ = channel.exit_status(255).await;
                        let _ = channel.close().await;
                        return;
                    }
                    SessionEvent::Eof => {
                        let _ = channel.eof().await;
                        let _ = channel.close().await;
                        return;
                    }
                }
            }
        });

        // Uplink: inbound SSH data/resize/signal → session writer. Ends when
        // the SSH side closes (None), which drops the writer and lets the
        // downlink drain trailing events.
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                ProxyMsg::Data(d) => {
                    let _ = writer.write_stdin(&d).await;
                }
                ProxyMsg::Resize(c, r) => {
                    let _ = writer.window_change(c, r).await;
                }
                ProxyMsg::Signal(s) => {
                    let _ = writer.signal(&s).await;
                }
                ProxyMsg::Eof => {
                    let _ = writer.eof().await;
                }
            }
        }
        drop(writer);
        let _ = downlink.await;
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
