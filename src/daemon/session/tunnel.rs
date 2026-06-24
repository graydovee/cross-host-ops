// TunneledSession — a `TargetSession` driven over the control-plane
// `OpenSession` RPC to a remote xhod.
//
// Realises the multi-hop path `ssh → 本机xhod → 控制面 12222 → 远程xhod → 机器`:
// every request (pty/exec/shell/subsystem/data/resize/signal) is forwarded as a
// `SessionRequest` over the gRPC stream opened against the remote daemon's
// control plane, and every `SessionResponse` is surfaced as a `SessionEvent`.
// The remote xhod services `OpenSession` by recursively opening its own
// `TargetSession`, so arbitrary-depth hops are uniform.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use russh::Pty;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use crate::protocol::rpc as r;
use crate::protocol::rpc::xho_rpc_client::XhoRpcClient;

use super::{SessionEvent, TargetSession};

type RpcClient = XhoRpcClient<tonic::transport::Channel>;

enum Control {
    Pty { term: String, cols: u32, rows: u32 },
    Env { key: String, value: String },
    Exec { command: String },
    Shell,
    Subsystem { name: String },
    WindowChange { cols: u32, rows: u32 },
    Signal { signal: String },
    Eof,
}

pub(crate) struct TunneledSession {
    control_tx: mpsc::Sender<Control>,
    stdin_tx: mpsc::Sender<Vec<u8>>,
    events_rx: mpsc::UnboundedReceiver<SessionEvent>,
}

impl TunneledSession {
    pub(crate) fn new(client: RpcClient, target: String) -> Self {
        let (control_tx, control_rx) = mpsc::channel::<Control>(32);
        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(64);
        let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        tokio::spawn(driver(client, target, control_rx, stdin_rx, events_tx));
        Self {
            control_tx,
            stdin_tx,
            events_rx,
        }
    }
}

async fn driver(
    mut client: RpcClient,
    target: String,
    mut control_rx: mpsc::Receiver<Control>,
    mut stdin_rx: mpsc::Receiver<Vec<u8>>,
    events_tx: mpsc::UnboundedSender<SessionEvent>,
) {
    let (req_tx, req_rx) = mpsc::channel::<r::SessionRequest>(64);
    let outbound = ReceiverStream::new(req_rx);

    let response = match client.open_session(Request::new(outbound)).await {
        Ok(resp) => resp.into_inner(),
        Err(status) => {
            let _ = events_tx.send(SessionEvent::Stderr(format!("open_session: {status}\n").into_bytes()));
            let _ = events_tx.send(SessionEvent::ExitStatus(255));
            let _ = events_tx.send(SessionEvent::Eof);
            return;
        }
    };
    let mut response = response;

    // Kick off: open the session on the remote end_target.
    if send_req(&req_tx, r::session_request::Msg::Open(r::SessionOpen { target })).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            ctrl = control_rx.recv() => match ctrl {
                Some(Control::Pty { term, cols, rows }) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Pty(r::SessionPty { term, cols, rows })).await;
                }
                Some(Control::Env { key, value }) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Env(r::SessionEnv { key, value })).await;
                }
                Some(Control::Exec { command }) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Exec(r::SessionExec { command })).await;
                }
                Some(Control::Shell) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Shell(r::SessionShell {})).await;
                }
                Some(Control::Subsystem { name }) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Subsystem(r::SessionSubsystem { name })).await;
                }
                Some(Control::WindowChange { cols, rows }) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Resize(r::SessionResize { cols, rows })).await;
                }
                Some(Control::Signal { signal }) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Signal(r::SessionSignal { signal })).await;
                }
                Some(Control::Eof) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Eof(r::SessionEof {})).await;
                }
                None => break,
            },
            stdin = stdin_rx.recv() => match stdin {
                Some(data) => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Data(r::SessionData { data })).await;
                }
                None => {
                    let _ = send_req(&req_tx, r::session_request::Msg::Eof(r::SessionEof {})).await;
                }
            },
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
        }
    }
}

async fn send_req(
    tx: &mpsc::Sender<r::SessionRequest>,
    msg: r::session_request::Msg,
) -> Result<()> {
    tx.send(r::SessionRequest { msg: Some(msg) })
        .await
        .map_err(|_| anyhow!("session stream closed"))
}

#[async_trait]
impl TargetSession for TunneledSession {
    async fn request_pty(
        &mut self,
        term: &str,
        cols: u32,
        rows: u32,
        _modes: &[(Pty, u32)],
    ) -> Result<()> {
        let _ = self
            .control_tx
            .send(Control::Pty {
                term: term.to_string(),
                cols,
                rows,
            })
            .await;
        Ok(())
    }

    async fn set_env(&mut self, key: &str, value: &str) -> Result<()> {
        let _ = self
            .control_tx
            .send(Control::Env {
                key: key.to_string(),
                value: value.to_string(),
            })
            .await;
        Ok(())
    }

    async fn exec(&mut self, command: &str) -> Result<()> {
        self.control_tx
            .send(Control::Exec {
                command: command.to_string(),
            })
            .await
            .map_err(|_| anyhow!("session closed"))?;
        Ok(())
    }

    async fn shell(&mut self) -> Result<()> {
        self.control_tx
            .send(Control::Shell)
            .await
            .map_err(|_| anyhow!("session closed"))?;
        Ok(())
    }

    async fn subsystem(&mut self, name: &str) -> Result<()> {
        self.control_tx
            .send(Control::Subsystem {
                name: name.to_string(),
            })
            .await
            .map_err(|_| anyhow!("session closed"))?;
        Ok(())
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
            .map_err(|_| anyhow!("session closed"))?;
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
