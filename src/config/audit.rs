use serde::{Deserialize, Serialize};

/// Audit logging configuration. The audit log records every machine operation
/// (exec, copy, session tunnel, transparent proxy) as a JSON-Lines record with
/// detailed source-identity metadata (peer address, SSH user, key fingerprint).
///
/// Enabled by default; disable via `[audit] enabled = false`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct AuditConfig {
    pub enabled: bool,
    /// File path for the audit log. When `None`, falls back to
    /// [`crate::config::path::default_audit_log_path`] (root →
    /// `/var/log/xho/audit.jsonl`, non-root → `~/.xho/audit.jsonl`).
    pub path: Option<String>,
    /// When `true`, record caller identity fields (peer/user/fingerprint).
    pub include_identity: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: None,
            include_identity: true,
        }
    }
}
