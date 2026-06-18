use std::path::PathBuf;

use anyhow::{Result, anyhow};
use home::home_dir;

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

/// Smart default for the local daemon control-socket path.
///
/// Follows the Docker / systemd convention for root daemons (`/var/run/<name>`),
/// while keeping non-root (local dev) usage under `~/.xho` where the user has
/// write access.
pub fn default_socket_path() -> String {
    if unsafe { libc::geteuid() } == 0 {
        "/var/run/xho/xhod.sock".to_string()
    } else {
        "~/.xho/xhod.sock".to_string()
    }
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
