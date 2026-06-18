//! Client-side bootstrap: SSH into a remote xhod using a token as the SSH
//! password, then call `BootstrapAuthorize` so the daemon appends the local
//! public key to its authorized_keys file. After this succeeds, normal
//! publickey auth works without any out-of-band key distribution.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use hyper_util::rt::TokioIo;
use russh::client;
use russh::keys::ssh_key;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::daemon::gateway::auth::{
    ClientHandler, authenticate_with_password, parse_remote_target,
};
use crate::daemon::ssh_server::remote_subsystem_name;
use crate::protocol::rpc;

/// Connect to `addr` (`user@host[:port]`, port defaults to 22), authenticate
/// with `token` as the SSH password, open the `xho-rpc` subsystem, and call
/// `BootstrapAuthorize` with the OpenSSH public key sidecar of `identity_file`.
///
/// The bootstrap connection is short-lived: a fresh SSH session is opened, the
/// single RPC is issued, and the channel is dropped.
pub(crate) async fn bootstrap_authorize(
    addr: &str,
    token: &str,
    identity_file: &Path,
) -> Result<()> {
    let target = parse_remote_target(addr)
        .with_context(|| format!("failed to parse address {addr}"))?;
    let pubkey = read_pubkey_sidecar(identity_file)?;
    let pubkey_line = pubkey
        .to_openssh()
        .context("failed to serialize public key")?;

    let host_owned = target.host.clone();
    let user_owned = target.user.clone();
    let token_owned = token.to_string();
    let port = target.port;

    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel: Channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let host = host_owned.clone();
            let user = user_owned.clone();
            let token = token_owned.clone();
            async move {
                open_rpc_stream(&host, port, &user, &token)
                    .await
                    .map(TokioIo::new)
                    .map_err(std::io::Error::other)
            }
        }))
        .await
        .context("failed to establish gRPC channel over SSH")?;

    let mut client = rpc::xho_rpc_client::XhoRpcClient::new(channel);
    let response = client
        .bootstrap_authorize(rpc::BootstrapAuthorizeRequest {
            public_key: pubkey_line,
        })
        .await
        .context("BootstrapAuthorize RPC failed")?
        .into_inner();

    if response.appended {
        println!(
            "authorized_keys updated: appended {} (fingerprint {})",
            identity_file.display(),
            response.fingerprint
        );
    } else if response.already_present {
        println!(
            "public key already present in authorized_keys (fingerprint {})",
            response.fingerprint
        );
    } else {
        // Should not happen — appended and already_present are mutually
        // exclusive at the daemon side, but be defensive.
        bail!(
            "daemon reported neither appended nor already_present (fingerprint {})",
            response.fingerprint
        );
    }
    Ok(())
}

/// Open a fresh SSH session to `host:port`, authenticate with `token`, and
/// return the `xho-rpc` subsystem stream wrapped for tonic.
async fn open_rpc_stream(
    host: &str,
    port: u16,
    user: &str,
    token: &str,
) -> Result<russh::ChannelStream<client::Msg>> {
    let client_config = client::Config::default();
    let mut handle = client::connect(
        Arc::new(client_config),
        (host, port),
        ClientHandler,
    )
    .await
    .map_err(|e| anyhow!("SSH connection to {host}:{port} failed: {e}"))?;

    authenticate_with_password(&mut handle, user, token)
        .await
        .map_err(|e| anyhow!("token rejected by {host}:{port}: {e}"))?;

    let channel = handle
        .channel_open_session()
        .await
        .map_err(|e| anyhow!("failed to open session channel: {e}"))?;
    channel
        .request_subsystem(true, remote_subsystem_name())
        .await
        .map_err(|e| anyhow!("failed to start xho-rpc subsystem: {e}"))?;
    Ok(channel.into_stream())
}

/// Read the OpenSSH public key sidecar (`<identity_file>.pub`).
fn read_pubkey_sidecar(identity_file: &Path) -> Result<ssh_key::PublicKey> {
    let mut pubkey_path = identity_file.as_os_str().to_owned();
    pubkey_path.push(".pub");
    let pubkey_path = std::path::PathBuf::from(pubkey_path);
    let content = std::fs::read_to_string(&pubkey_path)
        .with_context(|| format!("failed to read public key {}", pubkey_path.display()))?;
    ssh_key::PublicKey::from_openssh(content.trim())
        .with_context(|| format!("failed to parse public key {}", pubkey_path.display()))
}
