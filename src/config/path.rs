//! Re-export layer over [`crate::filepath`].
//!
//! The path/default helpers consolidated in `filepath`; this module keeps the
//! historical `crate::config::path::…` import paths working. New code should
//! import from [`crate::filepath`] directly.

use serde::{Deserialize, Serialize};

pub use crate::filepath::local::{
    default_client_config_path, default_config_path, default_known_hosts_path, default_root_dir,
    default_socket_path, default_tcp_lock_file, default_vault_path, expand_tilde,
};

/// Default control-channel transport for the local daemon.
///
/// Unix keeps the traditional Unix-domain socket; Windows defaults to a
/// TCP loopback listener (OS-assigned port advertised via a lock file) since
/// Windows socket-path semantics differ and named-pipe support in tonic is
/// less ergonomic.
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

pub fn default_local_transport() -> LocalTransport {
    if cfg!(unix) {
        LocalTransport::Unix
    } else {
        LocalTransport::Tcp
    }
}

/// Default audit-log path. Root daemons write to `/var/log/xho/audit.jsonl`
/// (standard syslog area); non-root daemons write to `~/.xho/audit.jsonl`.
pub fn default_audit_log_path() -> String {
    #[cfg(unix)]
    {
        if unsafe { libc::geteuid() } == 0 {
            return "/var/log/xho/audit.jsonl".to_string();
        }
    }
    "~/.xho/audit.jsonl".to_string()
}
