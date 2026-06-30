use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::duration::{
    deserialize_duration, deserialize_optional_duration, serialize_duration,
    serialize_optional_duration,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SshConfig {
    pub ssh_config_path: String,
    pub server_config_path: String,
    pub fallback: Vec<FallbackEntry>,
    /// When true, allocate PTY by default unless --no-tty overrides.
    pub tty: bool,
    /// When true, forward stdin by default unless --no-stdin overrides.
    pub stdin: bool,
    /// When true, auto-detect TTY based on stdout. If stdout is not a TTY, disable TTY.
    pub auto_tty_detect: bool,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub connect_timeout: Duration,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub keepalive_interval: Duration,
    /// SSH inactivity timeout. If no data is received from the server for this
    /// duration, the connection is closed. `None` = never timeout (rely on
    /// keepalive for liveness detection). Default: `None`.
    #[serde(
        deserialize_with = "deserialize_optional_duration",
        serialize_with = "serialize_optional_duration",
        default
    )]
    pub inactivity_timeout: Option<Duration>,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub max_idle_time: Duration,
    pub max_connections_per_ip: usize,
}

impl Default for SshConfig {
    fn default() -> Self {
        Self {
            ssh_config_path: "~/.ssh/config".to_string(),
            server_config_path: "~/.xho/server.toml".to_string(),
            fallback: vec![FallbackEntry::Local],
            tty: true,
            stdin: false,
            auto_tty_detect: true,
            connect_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(30),
            inactivity_timeout: None,
            max_idle_time: Duration::from_secs(600),
            max_connections_per_ip: 10,
        }
    }
}

/// A single entry in the `ssh.fallback` list.
///
/// - `"local"` deserializes to `FallbackEntry::Local` (resolve via ~/.ssh/config)
/// - Any other string deserializes to `FallbackEntry::Gateway(name)` (route through named gateway)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FallbackEntry {
    /// Resolve via local ~/.ssh/config
    Local,
    /// Route through the named gateway
    Gateway(String),
}

impl<'de> Deserialize<'de> for FallbackEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "local" {
            Ok(FallbackEntry::Local)
        } else {
            Ok(FallbackEntry::Gateway(s))
        }
    }
}

impl Serialize for FallbackEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            FallbackEntry::Local => serializer.serialize_str("local"),
            FallbackEntry::Gateway(name) => serializer.serialize_str(name),
        }
    }
}

impl fmt::Display for FallbackEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FallbackEntry::Local => write!(f, "local"),
            FallbackEntry::Gateway(name) => write!(f, "{}", name),
        }
    }
}
