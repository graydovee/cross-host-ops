// SSH server infrastructure for the daemon.
//
// Contains RemoteSshServer, RemoteSshHandler, IncomingConn, RemoteConnectInfo,
// and RemoteChannelStream types with their trait implementations (Connected,
// AsyncRead, AsyncWrite, server::Server, server::Handler).

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Result, anyhow};
use russh::keys::ssh_key::{self, HashAlg};
use russh::server::{self, Auth, Msg};
use russh::{Channel, ChannelId};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tonic::transport::server::Connected;

use super::DaemonState;

// ---------------------------------------------------------------------------
// Subsystem name constant
// ---------------------------------------------------------------------------

const REMOTE_SUBSYSTEM_NAME: &str = "xho-rpc";

/// Returns the SSH subsystem name used for xho gRPC-over-SSH connections.
pub(crate) fn remote_subsystem_name() -> &'static str {
    REMOTE_SUBSYSTEM_NAME
}

// Note: DaemonState is now the gateway-based state struct from super (daemon/mod.rs).

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Represents an incoming connection, either from a local Unix socket or a
/// remote SSH subsystem channel.
pub(super) enum IncomingConn {
    Local(UnixStream),
    Remote(RemoteChannelStream),
}

/// Metadata about a remote SSH connection (peer address, user, key fingerprint).
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(super) struct RemoteConnectInfo {
    pub peer_addr: Option<SocketAddr>,
    pub ssh_user: String,
    pub public_key_fingerprint: String,
}

/// Wraps a russh channel stream with connection metadata.
pub(super) struct RemoteChannelStream {
    pub stream: russh::ChannelStream<russh::server::Msg>,
    pub info: RemoteConnectInfo,
}

/// The SSH server that accepts incoming remote connections and hands them to
/// the daemon's gRPC layer via an `mpsc` channel.
#[derive(Clone)]
pub(super) struct RemoteSshServer {
    pub state: DaemonState,
    pub accepted_tx: mpsc::Sender<IncomingConn>,
}

/// Per-connection handler for the SSH server.
pub(super) struct RemoteSshHandler {
    state: DaemonState,
    accepted_tx: mpsc::Sender<IncomingConn>,
    peer_addr: Option<SocketAddr>,
    accepted_user: Option<String>,
    accepted_fingerprint: Option<String>,
    channels: HashMap<ChannelId, Channel<Msg>>,
}

// ---------------------------------------------------------------------------
// Connected impl (tonic transport integration)
// ---------------------------------------------------------------------------

impl Connected for IncomingConn {
    type ConnectInfo = Option<RemoteConnectInfo>;

    fn connect_info(&self) -> Self::ConnectInfo {
        match self {
            Self::Local(_) => None,
            Self::Remote(stream) => Some(stream.info.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncRead / AsyncWrite for IncomingConn
// ---------------------------------------------------------------------------

impl tokio::io::AsyncRead for IncomingConn {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            IncomingConn::Remote(stream) => {
                std::pin::Pin::new(&mut stream.stream).poll_read(cx, buf)
            }
        }
    }
}

impl tokio::io::AsyncWrite for IncomingConn {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            IncomingConn::Remote(stream) => {
                std::pin::Pin::new(&mut stream.stream).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            IncomingConn::Remote(stream) => std::pin::Pin::new(&mut stream.stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            IncomingConn::Local(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            IncomingConn::Remote(stream) => {
                std::pin::Pin::new(&mut stream.stream).poll_shutdown(cx)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// server::Server impl
// ---------------------------------------------------------------------------

impl server::Server for RemoteSshServer {
    type Handler = RemoteSshHandler;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        RemoteSshHandler {
            state: self.state.clone(),
            accepted_tx: self.accepted_tx.clone(),
            peer_addr,
            accepted_user: None,
            accepted_fingerprint: None,
            channels: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// server::Handler impl
// ---------------------------------------------------------------------------

impl server::Handler for RemoteSshHandler {
    type Error = anyhow::Error;

    async fn auth_publickey(
        &mut self,
        user: &str,
        key: &ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        let config = self.state.config.read().await.clone();
        if user != config.server.remote.user {
            return Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            });
        }
        if !is_authorized_key(Path::new(&config.server.remote.authorized_keys_path), key)? {
            return Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            });
        }
        self.accepted_user = Some(user.to_string());
        self.accepted_fingerprint = Some(key.fingerprint(HashAlg::Sha256).to_string());
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        self.channels.insert(channel.id(), channel);
        Ok(true)
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)?;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        _data: &[u8],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)?;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        if name != remote_subsystem_name() {
            session.channel_failure(channel)?;
            return Ok(());
        }

        let Some(channel_stream) = self.channels.remove(&channel) else {
            session.channel_failure(channel)?;
            return Ok(());
        };

        let info = RemoteConnectInfo {
            peer_addr: self.peer_addr,
            ssh_user: self.accepted_user.clone().unwrap_or_default(),
            public_key_fingerprint: self.accepted_fingerprint.clone().unwrap_or_default(),
        };
        session.channel_success(channel)?;
        self.accepted_tx
            .send(IncomingConn::Remote(RemoteChannelStream {
                stream: channel_stream.into_stream(),
                info,
            }))
            .await
            .map_err(|_| anyhow!("remote incoming queue closed"))?;
        Ok(())
    }

    async fn tcpip_forward(
        &mut self,
        _address: &str,
        _port: &mut u32,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn streamlocal_forward(
        &mut self,
        _socket_path: &str,
        _session: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Check if a candidate public key is present in an authorized_keys file.
fn is_authorized_key(path: &Path, candidate: &ssh_key::PublicKey) -> Result<bool> {
    use anyhow::{Context, bail};

    if !path.exists() {
        return Ok(false);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    for (idx, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let first = line
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow!("invalid authorized_keys line {}", idx + 1))?;
        if first.contains('=') || first.contains(',') {
            bail!(
                "authorized_keys options are not supported in {} line {}",
                path.display(),
                idx + 1
            );
        }
        let parsed = ssh_key::PublicKey::from_openssh(line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), idx + 1))?;
        if parsed.key_data() == candidate.key_data() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Load host keys from the specified file path.
pub(super) fn load_host_keys(path: &Path) -> Result<Vec<ssh_key::PrivateKey>> {
    use anyhow::Context;

    Ok(vec![
        ssh_key::PrivateKey::read_openssh_file(path)
            .with_context(|| format!("failed to read host key {}", path.display()))?,
    ])
}
