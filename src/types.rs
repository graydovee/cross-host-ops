// Shared types used across the crate.
// Migrated from connection/types.rs and jump/types.rs to provide a
// stable, module-independent home for types that outlive the legacy modules.

/// Direction of a file copy operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyDirection {
    Upload,
    Download,
}

/// Specification for a single file copy operation.
#[derive(Clone, Debug)]
pub struct CopySpec {
    pub direction: CopyDirection,
    pub local_path: String,
    pub remote_path: String,
    pub recursive: bool,
}

/// Identifies the source of server-list entries.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ServerListSource {
    /// Entries from the local daemon's own server.toml.
    Local,
    /// Entries from a configured jump host.
    JumpHost(String), // the jump host alias
}

// --- Address parsing (migrated from jump/address.rs) ---

use anyhow::{bail, Result};

/// Defaults applied when the input string omits user or port.
#[derive(Clone, Debug)]
pub struct AddressDefaults {
    pub user: String,
    pub port: u16,
}

/// A structured SSH-style remote address with explicit user, host, and port.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RemoteAddress {
    pub user: String,
    pub host: String,
    pub port: u16,
}

impl RemoteAddress {
    /// Parse `[user@]host[:port]`.
    ///
    /// - Empty input is rejected.
    /// - Empty host (e.g. `user@` or `user@:22`) is rejected.
    /// - If `user` is missing, fills `defaults.user`.
    /// - If `port` is missing, fills `defaults.port`.
    pub fn parse(input: &str, defaults: &AddressDefaults) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            bail!("address input is empty: {:?}", input);
        }

        let (user, host_port) = if let Some(at_pos) = trimmed.find('@') {
            let user_part = &trimmed[..at_pos];
            let rest = &trimmed[at_pos + 1..];
            (user_part.to_string(), rest)
        } else {
            (String::new(), trimmed)
        };

        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let host_part = &host_port[..colon_pos];
            let port_str = &host_port[colon_pos + 1..];
            let port: u16 = port_str
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port in address {:?}", input))?;
            (host_part.to_string(), Some(port))
        } else {
            (host_port.to_string(), None)
        };

        if host.is_empty() {
            bail!("empty host in address {:?}", input);
        }

        let effective_user = if user.is_empty() {
            defaults.user.clone()
        } else {
            user
        };

        let effective_port = port.unwrap_or(defaults.port);

        Ok(RemoteAddress {
            user: effective_user,
            host,
            port: effective_port,
        })
    }

    /// Produces the canonical `user@host:port` form.
    ///
    /// Round-trips with `parse` when `user` is non-empty.
    pub fn format(&self) -> String {
        format!("{}@{}:{}", self.user, self.host, self.port)
    }
}

// --- PTY decision logic (migrated from jump/pty.rs) ---

use crate::config::SshConfig;

/// Flags derived from the CLI's `--pty` / `--no-pty` arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecPtyFlags {
    /// `--pty` was passed.
    pub force_pty: bool,
    /// `--no-pty` was passed.
    pub force_no_pty: bool,
}

/// Compute the effective PTY decision.
///
/// Priority (each step short-circuits):
/// 1. `--no-pty` → false
/// 2. `--pty` → true
/// 3. `auto_pty_detect && !stdout_is_tty` → false
/// 4. Otherwise → `ssh.pty`
///
/// Note: `(force_pty=true, force_no_pty=true)` is unreachable because clap's
/// `conflicts_with` rejects it at parse time. If somehow both are true,
/// `force_no_pty` wins (it is checked first).
pub fn effective_pty_decision(
    flags: &ExecPtyFlags,
    ssh_config: &SshConfig,
    stdout_is_tty: bool,
) -> bool {
    if flags.force_no_pty {
        return false;
    }
    if flags.force_pty {
        return true;
    }
    if ssh_config.auto_pty_detect && !stdout_is_tty {
        return false;
    }
    ssh_config.pty
}
