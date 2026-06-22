use std::time::Duration;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::duration::{deserialize_duration, serialize_duration};

/// Client-side reverse proxy configuration.
///
/// When enabled, this xhod connects (as an SSH client) to a server xhod
/// with a public IP and registers itself as a dynamic gateway. This allows
/// xho clients on other machines to reach this node through the server.
///
/// The connection reuses the server xhod's existing SSH port (e.g. 2222)
/// and authenticates via the same `authorized_keys` mechanism.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReverseProxyClientConfig {
    /// Master switch. When `false` (default) no reverse proxy client is started.
    pub enable: bool,

    /// Target server xhod address: `[user@]host[:port]`.
    /// Defaults: user = "xho", port = 2222.
    pub server_address: String,

    /// SSH private key for authenticating to the server xhod.
    pub identity_file: String,

    /// SSH known_hosts file for verifying the server xhod's host key.
    pub known_hosts_path: String,

    /// Name under which this node registers on the server.
    /// Becomes a gateway name usable in target strings (e.g. `node-1:web01`).
    /// Must be unique on the server; a conflict causes rejection.
    pub node_name: String,

    /// Whether upstream clients may operate this machine directly.
    ///
    /// When `true`, `xho exec node-1:node-2 <cmd>` executes on node-2's host.
    /// When `false`, only deeper targets (e.g. `node-1:node-2:node-3`) are
    /// reachable; direct host access returns an error.
    pub allow_host_access: bool,

    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub reconnect_delay: Duration,

    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub keepalive_interval: Duration,

    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub max_idle_time: Duration,
}

impl Default for ReverseProxyClientConfig {
    fn default() -> Self {
        Self {
            enable: false,
            server_address: String::new(),
            identity_file: "~/.ssh/id_ed25519".to_string(),
            known_hosts_path: "~/.xho/known_hosts".to_string(),
            node_name: String::new(),
            allow_host_access: false,
            reconnect_delay: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(30),
            max_idle_time: Duration::from_secs(600),
        }
    }
}

impl ReverseProxyClientConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enable {
            return Ok(());
        }
        if self.server_address.trim().is_empty() {
            bail!("reverse_proxy.server_address must not be empty when reverse_proxy is enabled");
        }
        if self.node_name.trim().is_empty() {
            bail!("reverse_proxy.node_name must not be empty when reverse_proxy is enabled");
        }
        if self.identity_file.trim().is_empty() {
            bail!("reverse_proxy.identity_file must not be empty when reverse_proxy is enabled");
        }
        if self.known_hosts_path.trim().is_empty() {
            bail!("reverse_proxy.known_hosts_path must not be empty when reverse_proxy is enabled");
        }
        Ok(())
    }
}
