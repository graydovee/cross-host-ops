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
use subtle::ConstantTimeEq;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tonic::transport::server::Connected;
use tracing::{info, warn};

use super::DaemonState;
use crate::config::Secret;

// ---------------------------------------------------------------------------
// Subsystem name constant
// ---------------------------------------------------------------------------

const REMOTE_SUBSYSTEM_NAME: &str = "xho-rpc";
const REVERSE_PROXY_SUBSYSTEM_NAME: &str = "xho-reverse";

/// Returns the SSH subsystem name used for xho gRPC-over-SSH connections.
pub(crate) fn remote_subsystem_name() -> &'static str {
    REMOTE_SUBSYSTEM_NAME
}

/// Returns the SSH subsystem name used for reverse proxy connections.
pub(crate) fn reverse_proxy_subsystem_name() -> &'static str {
    REVERSE_PROXY_SUBSYSTEM_NAME
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
    ReverseProxy(ReverseProxyHandshake),
}

/// A reverse proxy handshake: an SSH channel stream + node name (parsed
/// from the subsystem name) + connection metadata.
pub(super) struct ReverseProxyHandshake {
    pub stream: russh::ChannelStream<russh::server::Msg>,
    pub node_name: String,
    pub info: RemoteConnectInfo,
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
    /// True when auth succeeded via a bootstrap token (password auth) rather
    /// than an authorized public key. Used only for audit logging — token-
    /// authed sessions have the same RPC permissions as pubkey-authed ones.
    authed_via_token: bool,
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
            // Reverse proxy connections are intercepted before reaching tonic.
            Self::ReverseProxy(_) => None,
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
            IncomingConn::ReverseProxy(_) => {
                panic!("reverse proxy connection should not reach tonic transport")
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
            IncomingConn::ReverseProxy(_) => {
                panic!("reverse proxy connection should not reach tonic transport")
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
            IncomingConn::ReverseProxy(_) => {
                panic!("reverse proxy connection should not reach tonic transport")
            }
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
            IncomingConn::ReverseProxy(_) => {
                panic!("reverse proxy connection should not reach tonic transport")
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
            authed_via_token: false,
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
        if !super::authorized_keys::is_authorized_key(
            Path::new(&config.server.remote.authorized_keys_path),
            key,
        )? {
            return Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            });
        }
        self.accepted_user = Some(user.to_string());
        self.accepted_fingerprint = Some(key.fingerprint(HashAlg::Sha256).to_string());
        Ok(Auth::Accept)
    }

    /// Token-based bootstrap auth. The client passes a short-lived token
    /// (issued by `xho token gen`) or matches the configured `bootstrap_token`
    /// as the SSH password. Accepting here lets the client open a gRPC channel
    /// and call `BootstrapAuthorize` to register its public key.
    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        let config = self.state.config.read().await.clone();
        if user != config.server.remote.user {
            return Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            });
        }
        // Dynamic tokens issued by `xho token gen` (also sweeps expired ones).
        if self.state.token_store.validate(password).await {
            self.authed_via_token = true;
            self.accepted_user = Some(user.to_string());
            self.accepted_fingerprint = Some("token".to_string());
            info!(peer = ?self.peer_addr, user = %user, "SSH auth via dynamic token");
            return Ok(Auth::Accept);
        }
        // Fixed bootstrap_token from config — supports plaintext + vault:/env:/file:.
        if let Some(ref bt) = config.server.remote.bootstrap_token {
            let resolver = config.secret_resolver(None);
            let secret = Secret::from_reference(bt);
            match secret.resolve(&resolver) {
                Ok(resolved) if !resolved.is_empty() => {
                    let ok: bool = password.as_bytes().ct_eq(resolved.as_bytes()).into();
                    if ok {
                        self.authed_via_token = true;
                        self.accepted_user = Some(user.to_string());
                        self.accepted_fingerprint = Some("bootstrap-token".to_string());
                        info!(peer = ?self.peer_addr, user = %user, "SSH auth via bootstrap_token");
                        return Ok(Auth::Accept);
                    }
                }
                Ok(_) => {} // empty resolved value, treat as no match
                Err(e) => {
                    warn!(peer = ?self.peer_addr, error = %e, "failed to resolve bootstrap_token");
                }
            }
        }
        Ok(Auth::Reject {
            proceed_with_methods: None,
            partial_success: false,
        })
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
        // Determine if this is a reverse proxy subsystem request.
        let reverse_enabled = {
            let config = self.state.config.read().await;
            config.server.remote.reverse_proxy_enable
        };
        // Check for reverse proxy subsystem: "xho-reverse" or "xho-reverse:<node_name>"
        let reverse_prefix = format!("{}:", reverse_proxy_subsystem_name());
        let (is_reverse, rp_node_name) = if name == reverse_proxy_subsystem_name() {
            (true, String::new())
        } else if name.starts_with(&reverse_prefix) {
            (true, name[reverse_prefix.len()..].to_string())
        } else {
            (false, String::new())
        };

        if name != remote_subsystem_name() && !(is_reverse && reverse_enabled) {
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

        if is_reverse {
            self.accepted_tx
                .send(IncomingConn::ReverseProxy(
                    super::ssh_server::ReverseProxyHandshake {
                        stream: channel_stream.into_stream(),
                        node_name: rp_node_name,
                        info,
                    },
                ))
                .await
                .map_err(|_| anyhow!("reverse proxy incoming queue closed"))?;
        } else {
            self.accepted_tx
                .send(IncomingConn::Remote(RemoteChannelStream {
                    stream: channel_stream.into_stream(),
                    info,
                }))
                .await
                .map_err(|_| anyhow!("remote incoming queue closed"))?;
        }
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

/// Load host keys from the specified file path.
pub(super) fn load_host_keys(path: &Path) -> Result<Vec<ssh_key::PrivateKey>> {
    use anyhow::Context;

    Ok(vec![
        ssh_key::PrivateKey::read_openssh_file(path)
            .with_context(|| format!("failed to read host key {}", path.display()))?,
    ])
}
