// TunneledSession — a `TargetSession` driven over the control-plane
// `OpenSession` RPC to a remote xhod.
//
// Realises the multi-hop path `ssh → local xhod → control plane 12222 →
// remote xhod → machine`: every request (pty/exec/shell/subsystem/data/
// resize/signal) is forwarded as a `SessionRequest` over the gRPC stream
// opened against the remote daemon's control plane, and every
// `SessionResponse` is surfaced as a `SessionEvent`. The remote xhod services
// `OpenSession` by recursively opening its own `TargetSession`, so
// arbitrary-depth hops are uniform.
//
// The two directions run as separate tasks: one forwards commands onto the
// gRPC request stream (parking on flow control only pauses stdin), the other
// drains responses into the event stream. Neither can starve the other.

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use crate::protocol::rpc as r;
use crate::protocol::rpc::xho_rpc_client::XhoRpcClient;

use super::{SessionCommand, SessionEvent, SessionStream, SessionWriter, TargetSession};

type RpcClient = XhoRpcClient<tonic::transport::Channel>;

pub(crate) struct TunneledSession {
    writer: Option<SessionWriter>,
    stream: Option<SessionStream>,
}

impl TunneledSession {
    pub(crate) fn new(client: RpcClient, target: String) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        tokio::spawn(driver(client, target, cmd_rx, events_tx));
        Self {
            writer: Some(SessionWriter { tx: cmd_tx }),
            stream: Some(SessionStream { rx: events_rx }),
        }
    }
}

impl TargetSession for TunneledSession {
    fn split(mut self: Box<Self>) -> (SessionWriter, SessionStream) {
        (
            self.writer.take().expect("tunnel session split twice"),
            self.stream.take().expect("tunnel session split twice"),
        )
    }
}

async fn driver(
    mut client: RpcClient,
    target: String,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
) {
    let (req_tx, req_rx) = mpsc::channel::<r::SessionRequest>(64);
    let outbound = ReceiverStream::new(req_rx);

    let response = match client.open_session(Request::new(outbound)).await {
        Ok(resp) => resp.into_inner(),
        Err(status) => {
            let _ = events_tx.send(SessionEvent::Stderr(
                format!("open_session: {status}\n").into_bytes(),
            ));
            let _ = events_tx.send(SessionEvent::ExitStatus(255));
            let _ = events_tx.send(SessionEvent::Eof);
            return;
        }
    };
    let mut response = response;

    // Kick off: open the session on the remote end_target.
    if req_tx
        .send(r::SessionRequest {
            msg: Some(r::session_request::Msg::Open(r::SessionOpen { target })),
        })
        .await
        .is_err()
    {
        return;
    }

    // Uplink: forward commands onto the gRPC request stream. Commands and
    // stdin share ONE ordered stream so eof/data cannot overtake the
    // exec/subsystem start on the remote side. When this task ends (session
    // halves dropped or the stream broke), `req_tx` drops and the remote sees
    // end-of-stream.
    let mut uplink = tokio::spawn(async move {
        let mut cmd_rx = cmd_rx;
        while let Some(cmd) = cmd_rx.recv().await {
            // "Start" commands carry a reply: acknowledge once the request is
            // handed to the gRPC stream. Transport failures surface later as
            // an Error event on the response stream, as in the old design.
            let (reply, msg) = match cmd {
                SessionCommand::Pty {
                    term,
                    cols,
                    rows,
                    reply,
                    ..
                } => (
                    Some(reply),
                    r::session_request::Msg::Pty(r::SessionPty { term, cols, rows }),
                ),
                SessionCommand::Env { key, value, reply } => (
                    Some(reply),
                    r::session_request::Msg::Env(r::SessionEnv { key, value }),
                ),
                SessionCommand::Exec { command, reply } => (
                    Some(reply),
                    r::session_request::Msg::Exec(r::SessionExec { command }),
                ),
                SessionCommand::Shell { reply } => (
                    Some(reply),
                    r::session_request::Msg::Shell(r::SessionShell {}),
                ),
                SessionCommand::Subsystem { name, reply } => (
                    Some(reply),
                    r::session_request::Msg::Subsystem(r::SessionSubsystem { name }),
                ),
                SessionCommand::Resize { cols, rows } => (
                    None,
                    r::session_request::Msg::Resize(r::SessionResize { cols, rows }),
                ),
                SessionCommand::Signal { signal } => (
                    None,
                    r::session_request::Msg::Signal(r::SessionSignal { signal }),
                ),
                SessionCommand::Eof => (None, r::session_request::Msg::Eof(r::SessionEof {})),
                SessionCommand::Data { bytes } => (
                    None,
                    r::session_request::Msg::Data(r::SessionData { data: bytes }),
                ),
            };
            let sent = req_tx.send(r::SessionRequest { msg: Some(msg) }).await;
            let sent_ok = sent.is_ok();
            if let Some(reply) = reply {
                let _ = reply.send(sent.map_err(|_| anyhow!("session stream closed")));
            }
            if !sent_ok {
                break;
            }
        }
    });

    // Downlink: drain responses into the event stream until the remote closes.
    // When the uplink task ends (local session halves dropped — stdin is
    // finished), the session must keep draining: the remote still owes output
    // and an exit status. Only the remote closing the stream ends this loop.
    let mut uplink_done = false;
    loop {
        tokio::select! {
            msg = response.message() => match msg {
                Ok(Some(resp)) => match resp.msg {
                    Some(r::session_response::Msg::Started(_)) => {}
                    Some(r::session_response::Msg::Data(d)) => {
                        let _ = events_tx.send(SessionEvent::Stdout(d.data));
                    }
                    Some(r::session_response::Msg::Stderr(d)) => {
                        let _ = events_tx.send(SessionEvent::Stderr(d.data));
                    }
                    Some(r::session_response::Msg::ExitStatus(s)) => {
                        let _ = events_tx.send(SessionEvent::ExitStatus(s.code));
                    }
                    Some(r::session_response::Msg::ExitSignal(s)) => {
                        let _ = events_tx.send(SessionEvent::ExitSignal(s.signal));
                        let _ = events_tx.send(SessionEvent::ExitStatus(255));
                    }
                    Some(r::session_response::Msg::Eof(_)) => {
                        let _ = events_tx.send(SessionEvent::Eof);
                    }
                    Some(r::session_response::Msg::Error(e)) => {
                        let _ = events_tx.send(SessionEvent::Stderr(format!("{}\n", e.message).into_bytes()));
                        let _ = events_tx.send(SessionEvent::ExitStatus(255));
                    }
                    None => {}
                },
                Ok(None) => {
                    let _ = events_tx.send(SessionEvent::Eof);
                    break;
                }
                Err(status) => {
                    let _ = events_tx.send(SessionEvent::Stderr(format!("session stream: {status}\n").into_bytes()));
                    let _ = events_tx.send(SessionEvent::ExitStatus(255));
                    let _ = events_tx.send(SessionEvent::Eof);
                    break;
                }
            },
            _ = &mut uplink, if !uplink_done => {
                // Uplink ended: stdin side is complete. Keep consuming
                // responses below until the remote closes the stream.
                uplink_done = true;
            }
        }
    }
    uplink.abort();
}

fn forward_trailing(resp: r::SessionResponse, events_tx: &mpsc::UnboundedSender<SessionEvent>) {
    match resp.msg {
        Some(r::session_response::Msg::Data(d)) => {
            let _ = events_tx.send(SessionEvent::Stdout(d.data));
        }
        Some(r::session_response::Msg::Stderr(d)) => {
            let _ = events_tx.send(SessionEvent::Stderr(d.data));
        }
        Some(r::session_response::Msg::ExitStatus(s)) => {
            let _ = events_tx.send(SessionEvent::ExitStatus(s.code));
        }
        _ => {}
    }
}
