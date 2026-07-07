use std::path::PathBuf;

use anyhow::{Result, anyhow};
use home::home_dir;
use serde::{Deserialize, Serialize};

pub fn default_config_path() -> PathBuf {
    home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".xho/config.toml")
}

pub fn default_client_config_path() -> PathBuf {
    default_root_dir().join("client.toml")
}

pub fn default_root_dir() -> PathBuf {
    home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".xho")
}

pub fn default_known_hosts_path() -> PathBuf {
    default_root_dir().join("known_hosts")
}

/// Path to the local encrypted secret vault when the config directory is
/// unknown (`~/.xho/secrets`). In practice the vault follows the config file
/// (`<config_dir>/secrets`); this is only the fallback for zero-config runs.
pub fn default_vault_path() -> PathBuf {
    default_root_dir().join("secrets")
}

/// Smart default for the local daemon control-socket path.
///
/// Follows the Docker / systemd convention for root daemons (`/var/run/<name>`),
/// while keeping non-root (local dev) usage under `~/.xho` where the user has
/// write access.
///
/// This value is only consulted when the local control transport is `unix`.
/// On Windows the default transport is `tcp` (see [`default_local_transport`]),
/// so this path is unused there unless the user explicitly selects `unix`.
#[cfg(unix)]
pub fn default_socket_path() -> String {
    if unsafe { libc::geteuid() } == 0 {
        "/var/run/xho/xhod.sock".to_string()
    } else {
        "~/.xho/xhod.sock".to_string()
    }
}

/// Windows has no Unix-domain-socket path semantics; return a placeholder.
/// The actual control endpoint on Windows is the TCP loopback listener whose
/// address is published in a lock file (see `default_tcp_lock_file`).
#[cfg(not(unix))]
pub fn default_socket_path() -> String {
    String::new()
}

/// Control-channel transport for the local daemon.
///
/// `Unix` (default on Unix) binds a Unix-domain socket at `socket_path`.
/// `Tcp` (default on Windows) binds a loopback TCP listener and advertises the
/// actual address via a lock file, since Windows socket-path semantics differ.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LocalTransport {
    Unix,
    Tcp,
}

impl Default for LocalTransport {
    fn default() -> Self {
        default_local_transport()
    }
}

/// Default control-channel transport for the local daemon.
///
/// Unix keeps the traditional Unix-domain socket; Windows defaults to a
/// TCP loopback listener (OS-assigned port advertised via a lock file) since
/// Windows socket-path semantics differ and named-pipe support in tonic is
/// less ergonomic.
pub fn default_local_transport() -> LocalTransport {
    if cfg!(unix) {
        LocalTransport::Unix
    } else {
        LocalTransport::Tcp
    }
}

/// Default lock file recording the daemon's actual TCP loopback address.
///
/// Only relevant when `transport = "tcp"`. The daemon writes
/// `127.0.0.1:<port>` (plus the daemon PID for staleness checks) here on
/// startup and removes it on shutdown; the CLI reads it to discover the port.
pub fn default_tcp_lock_file() -> String {
    "~/.xho/xhod.tcp".to_string()
}

pub fn expand_tilde(value: &str) -> Result<String> {
    if value == "~" {
        return Ok(home_dir()
            .ok_or_else(|| anyhow!("home directory not found"))?
            .display()
            .to_string());
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return Ok(home_dir()
            .ok_or_else(|| anyhow!("home directory not found"))?
            .join(rest)
            .display()
            .to_string());
    }
    Ok(value.to_string())
}
