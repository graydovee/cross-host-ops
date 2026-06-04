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
///
/// The `relay_upload_rx` and `relay_download_tx` fields are used when the
/// daemon acts as a relay for xhod-routed copies. In this mode, file data
/// does not live on the daemon's local disk — it travels on gRPC streams.
///
/// - `relay_upload_rx`: When `Some`, the connection reads upload chunks from
///   this channel instead of reading from `local_path`.
/// - `relay_download_tx`: When `Some`, the connection sends download chunks
///   to this channel instead of writing to `local_path`.
///
/// Both fields are `None` for direct (non-relay) copy operations, preserving
/// the original SFTP file-based behavior.
pub struct CopySpec {
    pub direction: CopyDirection,
    pub local_path: String,
    pub remote_path: String,
    pub recursive: bool,
    /// Upload relay: receive (data, eof) tuples from the daemon's gRPC inbound stream.
    /// When Some, replaces local file reading for upload operations.
    pub relay_upload_rx: Option<tokio::sync::mpsc::Receiver<(Vec<u8>, bool)>>,
    /// Download relay: send (data, eof) tuples to the daemon's gRPC response sender.
    /// When Some, replaces local file writing for download operations.
    pub relay_download_tx: Option<tokio::sync::mpsc::Sender<(Vec<u8>, bool)>>,
}

impl Clone for CopySpec {
    fn clone(&self) -> Self {
        // relay channels are not clonable; cloning drops them (used only in retry paths
        // before channels are populated, so this is safe in practice).
        Self {
            direction: self.direction.clone(),
            local_path: self.local_path.clone(),
            remote_path: self.remote_path.clone(),
            recursive: self.recursive,
            relay_upload_rx: None,
            relay_download_tx: None,
        }
    }
}

impl std::fmt::Debug for CopySpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopySpec")
            .field("direction", &self.direction)
            .field("local_path", &self.local_path)
            .field("remote_path", &self.remote_path)
            .field("recursive", &self.recursive)
            .field("relay_upload_rx", &self.relay_upload_rx.is_some())
            .field("relay_download_tx", &self.relay_download_tx.is_some())
            .finish()
    }
}

/// Identifies the source of server-list entries.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ServerListSource {
    /// Entries from the local daemon's own server.toml.
    Local,
    /// Entries from a configured gateway.
    Gateway(String), // the gateway alias
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

// --- TTY / stdin decision logic ---

use crate::config::SshConfig;

/// Flags derived from the CLI's --tty / --no-tty arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecTtyFlags {
    /// --tty or -t was passed.
    pub force_tty: bool,
    /// --no-tty was passed.
    pub force_no_tty: bool,
}

/// Flags derived from the CLI's --stdin / --no-stdin arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecStdinFlags {
    /// --stdin or -i was passed.
    pub force_stdin: bool,
    /// --no-stdin was passed.
    pub force_no_stdin: bool,
}

/// Compute the effective TTY decision.
///
/// Priority (each step short-circuits):
/// 1. --no-tty → false
/// 2. --tty → true
/// 3. auto_tty_detect && !stdout_is_tty → false
/// 4. Otherwise → ssh_config.tty
///
/// Note: (force_tty=true, force_no_tty=true) is unreachable because clap's
/// `conflicts_with` rejects it at parse time. If somehow both are true,
/// force_no_tty wins (it is checked first).
pub fn effective_tty_decision(
    flags: &ExecTtyFlags,
    ssh_config: &SshConfig,
    stdout_is_tty: bool,
) -> bool {
    if flags.force_no_tty {
        return false;
    }
    if flags.force_tty {
        return true;
    }
    if ssh_config.auto_tty_detect && !stdout_is_tty {
        return false;
    }
    ssh_config.tty
}

/// Compute the effective stdin decision.
///
/// Priority (each step short-circuits):
/// 1. --no-stdin → false
/// 2. --stdin → true
/// 3. Otherwise → ssh_config.stdin
pub fn effective_stdin_decision(
    flags: &ExecStdinFlags,
    ssh_config: &SshConfig,
) -> bool {
    if flags.force_no_stdin {
        return false;
    }
    if flags.force_stdin {
        return true;
    }
    ssh_config.stdin
}

/// Determine if the current execution should enter full interactive mode.
///
/// Interactive mode requires ALL of:
/// - tty allocation is enabled (resolved_tty = true)
/// - stdin forwarding is enabled (resolved_stdin = true)
/// - stdin is a TTY device
/// - stdout is a TTY device
pub fn should_use_interactive_mode(
    resolved_tty: bool,
    resolved_stdin: bool,
    stdin_is_tty: bool,
    stdout_is_tty: bool,
) -> bool {
    resolved_tty && resolved_stdin && stdin_is_tty && stdout_is_tty
}
