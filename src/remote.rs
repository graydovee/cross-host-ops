use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use hyper_util::rt::TokioIo;
use russh::client;
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, known_hosts, load_secret_key, ssh_key};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::config::{
    ClientConfig, ClientMode, RemoteClientConfig, default_known_hosts_path, expand_tilde,
};
use crate::protocol::rpc;

const REMOTE_SUBSYSTEM_NAME: &str = "rhop-rpc";
const DEFAULT_REMOTE_PORT: u16 = 2222;
const DEFAULT_REMOTE_USER: &str = "rhop";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteTarget {
    pub host: String,
    pub port: u16,
    pub user: String,
}

impl RemoteTarget {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KnownHostState {
    Known,
    Unknown {
        algorithm: String,
        fingerprint: String,
    },
    Changed {
        algorithm: String,
        fingerprint: String,
    },
}

#[derive(Clone, Default)]
struct RemoteClientHandler {
    shared: Arc<Mutex<HostKeyState>>,
}

#[derive(Clone, Default)]
struct HostKeyState {
    expected: Option<ssh_key::PublicKey>,
    seen: Option<ssh_key::PublicKey>,
}

impl client::Handler for RemoteClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        let mut state = self.shared.lock().expect("host key mutex poisoned");
        state.seen = Some(server_public_key.clone());
        Ok(match &state.expected {
            Some(expected) => expected == server_public_key,
            None => true,
        })
    }
}

pub fn remote_subsystem_name() -> &'static str {
    REMOTE_SUBSYSTEM_NAME
}

pub fn load_client_config() -> Result<ClientConfig> {
    ClientConfig::load()
}

pub fn save_client_config(config: &ClientConfig) -> Result<()> {
    config.save()
}

pub fn client_mode(config: &ClientConfig) -> ClientMode {
    config.mode.clone()
}

pub fn parse_remote_target(input: &str) -> Result<RemoteTarget> {
    if input.trim().is_empty() {
        bail!("remote target must not be empty");
    }

    let (user, host_port) = match input.rsplit_once('@') {
        Some((user, host_port)) if !user.is_empty() => (user.to_string(), host_port),
        _ => (DEFAULT_REMOTE_USER.to_string(), input),
    };

    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && !port.is_empty() => {
            let port = port
                .parse::<u16>()
                .with_context(|| format!("invalid remote port in target {}", input))?;
            (host.to_string(), port)
        }
        _ => (host_port.to_string(), DEFAULT_REMOTE_PORT),
    };

    if host.trim().is_empty() {
        bail!("remote host must not be empty");
    }

    Ok(RemoteTarget { host, port, user })
}

pub fn apply_remote_target(config: &mut ClientConfig, target: &RemoteTarget) {
    config.remote.address = target.address();
    config.remote.user = target.user.clone();
}

pub fn enable_remote_mode(config: &mut ClientConfig) -> Result<()> {
    if !config.remote.is_configured() {
        bail!("no remote daemon configured; run `rhop remote connect <host>` first");
    }
    config.mode = ClientMode::Remote;
    Ok(())
}

pub fn disable_remote_mode(config: &mut ClientConfig) {
    config.mode = ClientMode::Local;
}

pub fn normalize_remote_paths(
    identity_file: Option<String>,
    known_hosts_path: Option<String>,
) -> Result<(String, String)> {
    let identity_file = expand_tilde(identity_file.as_deref().unwrap_or("~/.ssh/id_ed25519"))?;
    let known_hosts_default = default_known_hosts_path().display().to_string();
    let known_hosts_path =
        expand_tilde(known_hosts_path.as_deref().unwrap_or(&known_hosts_default))?;
    Ok((identity_file, known_hosts_path))
}

pub fn inspect_known_host(
    target: &RemoteTarget,
    public_key: &ssh_key::PublicKey,
    path: &Path,
) -> KnownHostState {
    match known_hosts::check_known_hosts_path(&target.host, target.port, public_key, path) {
        Ok(true) => KnownHostState::Known,
        Ok(false) => KnownHostState::Unknown {
            algorithm: public_key.algorithm().to_string(),
            fingerprint: public_key.fingerprint(HashAlg::Sha256).to_string(),
        },
        Err(russh::keys::Error::KeyChanged { .. }) => KnownHostState::Changed {
            algorithm: public_key.algorithm().to_string(),
            fingerprint: public_key.fingerprint(HashAlg::Sha256).to_string(),
        },
        Err(_) => KnownHostState::Unknown {
            algorithm: public_key.algorithm().to_string(),
            fingerprint: public_key.fingerprint(HashAlg::Sha256).to_string(),
        },
    }
}

pub fn trust_known_host(target: &RemoteTarget, public_key: &ssh_key::PublicKey, path: &Path) -> Result<()> {
    known_hosts::learn_known_hosts_path(&target.host, target.port, public_key, path)
        .map_err(|error| anyhow!("failed to write known_hosts: {}", error))
}

pub async fn fetch_remote_host_key(target: &RemoteTarget, identity_file: &str) -> Result<ssh_key::PublicKey> {
    let shared = Arc::new(Mutex::new(HostKeyState::default()));
    let handler = RemoteClientHandler {
        shared: shared.clone(),
    };
    let client_config = Arc::new(client::Config::default());
    let mut handle = client::connect(client_config, (target.host.as_str(), target.port), handler)
        .await
        .with_context(|| format!("failed to connect to {}", target.address()))?;
    authenticate_remote_handle(&mut handle, &target.user, identity_file).await?;
    let key = shared
        .lock()
        .expect("host key mutex poisoned")
        .seen
        .clone()
        .ok_or_else(|| anyhow!("server did not present a host key"))?;
    let _ = handle
        .disconnect(russh::Disconnect::ByApplication, "done", "en")
        .await;
    Ok(key)
}

pub async fn connect_remote_client(
    config: &RemoteClientConfig,
) -> Result<rpc::rhop_rpc_client::RhopRpcClient<Channel>> {
    let config = config.clone();
    let endpoint = Endpoint::from_static("http://[::]:50051");
    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let config = config.clone();
            async move { connect_remote_stream(&config).await }
        }))
        .await?;
    Ok(rpc::rhop_rpc_client::RhopRpcClient::new(channel))
}

pub fn remote_target_from_config(config: &RemoteClientConfig) -> Result<RemoteTarget> {
    if config.address.trim().is_empty() {
        bail!("no remote daemon configured; run `rhop remote connect <host>` first");
    }
    let address = if config.user.trim().is_empty() {
        config.address.clone()
    } else {
        format!("{}@{}", config.user, config.address)
    };
    parse_remote_target(&address)
}

async fn connect_remote_stream(
    config: &RemoteClientConfig,
) -> Result<TokioIo<russh::ChannelStream<russh::client::Msg>>> {
    let target = remote_target_from_config(config)?;
    let expected_host_key =
        load_known_host_key(&target, Path::new(&config.known_hosts_path))?;
    let shared = Arc::new(Mutex::new(HostKeyState {
        expected: Some(expected_host_key),
        seen: None,
    }));
    let handler = RemoteClientHandler {
        shared: shared.clone(),
    };
    let client_config = Arc::new(client::Config::default());
    let mut handle = client::connect(client_config, (target.host.as_str(), target.port), handler)
        .await
        .with_context(|| format!("failed to connect to {}", target.address()))?;
    authenticate_remote_handle(&mut handle, &target.user, &config.identity_file).await?;
    let channel = handle
        .channel_open_session()
        .await
        .context("failed to open SSH session channel")?;
    channel
        .request_subsystem(true, REMOTE_SUBSYSTEM_NAME)
        .await
        .context("failed to request rhop-rpc subsystem")?;
    let _ = handle; // Keep SSH session alive until stream closes.
    Ok(TokioIo::new(channel.into_stream()))
}

async fn authenticate_remote_handle(
    handle: &mut client::Handle<RemoteClientHandler>,
    user: &str,
    identity_file: &str,
) -> Result<()> {
    let key = load_secret_key(identity_file, None)
        .with_context(|| format!("failed to load key {}", identity_file))?;
    let hash_alg = handle.best_supported_rsa_hash().await?.flatten();
    let auth = handle
        .authenticate_publickey(
            user,
            PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg),
        )
        .await?;
    if auth.success() {
        return Ok(());
    }
    bail!("SSH publickey authentication failed for {}", user)
}

fn load_known_host_key(target: &RemoteTarget, path: &Path) -> Result<ssh_key::PublicKey> {
    let entries = known_hosts::known_host_keys_path(&target.host, target.port, path)
        .map_err(|error| anyhow!("failed to read known_hosts: {}", error))?;
    if entries.is_empty() {
        bail!(
            "unknown host {}; run `rhop remote connect {}` first",
            target.address(),
            target.address()
        );
    }
    Ok(entries[0].1.clone())
}

pub fn known_hosts_path(config: &ClientConfig) -> PathBuf {
    PathBuf::from(&config.remote.known_hosts_path)
}

pub fn identity_file(config: &ClientConfig) -> &str {
    &config.remote.identity_file
}

#[cfg(test)]
mod tests {
    use super::{RemoteTarget, parse_remote_target};

    #[test]
    fn parses_host_only_target() {
        let target = parse_remote_target("example.com").unwrap();
        assert_eq!(
            target,
            RemoteTarget {
                host: "example.com".to_string(),
                port: 2222,
                user: "rhop".to_string(),
            }
        );
    }

    #[test]
    fn parses_host_and_port_target() {
        let target = parse_remote_target("example.com:2200").unwrap();
        assert_eq!(target.host, "example.com");
        assert_eq!(target.port, 2200);
        assert_eq!(target.user, "rhop");
    }

    #[test]
    fn parses_user_host_port_target() {
        let target = parse_remote_target("ops@example.com:2200").unwrap();
        assert_eq!(target.host, "example.com");
        assert_eq!(target.port, 2200);
        assert_eq!(target.user, "ops");
    }
}
