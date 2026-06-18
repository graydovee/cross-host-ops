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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            log_path: None,
            log_level: "info".to_string(),
            reaper_interval: Duration::from_secs(30),
            local: LocalServerConfig::default(),
            remote: RemoteServerConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.local.enable && !self.remote.enable {
            bail!("at least one of server.local.enable or server.remote.enable must be true");
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
}

impl Default for RemoteServerConfig {
    fn default() -> Self {
        Self {
            enable: false,
            listen_addr: "0.0.0.0:2222".to_string(),
            user: "xho".to_string(),
            host_key_path: "~/.xho/host_key".to_string(),
            authorized_keys_path: "~/.xho/authorized_keys".to_string(),
            bootstrap_token: None,
        }
    }
}
