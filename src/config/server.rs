use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::duration::{deserialize_duration, serialize_duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    pub log_path: Option<String>,
    pub log_level: String,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub reaper_interval: Duration,
    pub local: LocalServerConfig,
    pub remote: RemoteServerConfig,
    pub proxy: ProxyServerConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            log_path: None,
            log_level: "info".to_string(),
            reaper_interval: Duration::from_secs(30),
            local: LocalServerConfig::default(),
            remote: RemoteServerConfig::default(),
            proxy: ProxyServerConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.local.enable && !self.remote.enable && !self.proxy.enable {
            bail!(
                "at least one of server.local.enable, server.remote.enable, or server.proxy.enable must be true"
            );
        }
        if self.local.enable && self.local.socket_path.trim().is_empty() {
            bail!("server.local.socket_path must not be empty");
        }
        if self.remote.enable {
            if self.remote.user.trim().is_empty() {
                bail!("server.remote.user must not be empty");
            }
            if self.remote.listen_addr.parse::<SocketAddr>().is_err() {
                bail!(
                    "server.remote.listen_addr is invalid: {}",
                    self.remote.listen_addr
                );
            }
            if self.remote.host_key_path.trim().is_empty() {
                bail!("server.remote.host_key_path must not be empty");
            }
            if self.remote.authorized_keys_path.trim().is_empty() {
                bail!("server.remote.authorized_keys_path must not be empty");
            }
        }
        if self.proxy.enable {
            if self.proxy.listen_addr.parse::<SocketAddr>().is_err() {
                bail!(
                    "server.proxy.listen_addr is invalid: {}",
                    self.proxy.listen_addr
                );
            }
            if self.proxy.host_key_path.trim().is_empty() {
                bail!("server.proxy.host_key_path must not be empty");
            }
            if self.proxy.authorized_keys_path.trim().is_empty() {
                bail!("server.proxy.authorized_keys_path must not be empty");
            }
            if self.remote.enable && self.proxy.listen_addr == self.remote.listen_addr {
                bail!("server.proxy.listen_addr must differ from server.remote.listen_addr");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LocalServerConfig {
    pub enable: bool,
    pub socket_path: String,
}

impl Default for LocalServerConfig {
    fn default() -> Self {
        Self {
            enable: true,
            socket_path: crate::config::path::default_socket_path(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct RemoteServerConfig {
    pub enable: bool,
    pub listen_addr: String,
    pub user: String,
    pub host_key_path: String,
    pub authorized_keys_path: String,
    /// Optional long-lived token accepted by `auth_password` as a fallback
    /// when no dynamic token matches. Accepts plaintext or any reference
    /// supported by the secret resolver (`vault:NAME`, `env:VAR`, `file:PATH`).
    /// Storing plaintext here is a security risk — prefer `vault:` references.
    pub bootstrap_token: Option<String>,
    /// When `true`, accept reverse proxy connections (the `xho-reverse` SSH
    /// subsystem) from nodes without public IPs. Nodes authenticate via the
    /// same `authorized_keys_path` and register as dynamic gateways.
    /// Requires `enable = true`.
    pub reverse_proxy_enable: bool,
}

impl Default for RemoteServerConfig {
    fn default() -> Self {
        Self {
            enable: false,
            listen_addr: "0.0.0.0:12222".to_string(),
            user: "xho".to_string(),
            host_key_path: "~/.xho/host_key".to_string(),
            authorized_keys_path: "~/.xho/authorized_keys".to_string(),
            bootstrap_token: None,
            reverse_proxy_enable: false,
        }
    }
}

/// Transparent SSH proxy listener (human-facing `ssh node@xhod`).
///
/// Distinct from the control plane (`remote`): the proxy authenticates humans by
/// public key with `username = target node`, while the control plane
/// authenticates machines by `authorized_keys` under a single `user`. Keeping
/// them on separate ports with separate key stores prevents a human key from
/// granting machine-to-machine control-plane access.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ProxyServerConfig {
    pub enable: bool,
    pub listen_addr: String,
    pub host_key_path: String,
    pub authorized_keys_path: String,
    /// Optional explicit path to the `sftp-server` binary used to serve the
    /// sftp subsystem for localhost targets. When `None`, common locations and
    /// `PATH` are probed at runtime.
    pub sftp_server_path: Option<String>,
}

impl Default for ProxyServerConfig {
    fn default() -> Self {
        Self {
            enable: true,
            listen_addr: "0.0.0.0:2222".to_string(),
            host_key_path: "~/.xho/host_key".to_string(),
            authorized_keys_path: "~/.xho/proxy_authorized_keys".to_string(),
            sftp_server_path: None,
        }
    }
}
