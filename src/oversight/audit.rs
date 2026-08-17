//! Audit logging: a JSON-Lines record of every machine operation the daemon
//! performs (exec, copy, session tunnel, transparent proxy), including source
//! identity (peer address, SSH user, key fingerprint) and result.
//!
//! The audit sink is an independent non-blocking file appender — it is fully
//! decoupled from the `tracing` debug log. Like the debug log it follows the
//! SIGHUP-driven rotation convention via [`reopen_audit_output`].

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use std::io::Write;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder};
use uuid::Uuid;

use crate::config::AuditConfig;

/// One audit record. Serialized as a single JSON line. All optional fields use
/// `skip_serializing_if` so records stay compact.
#[derive(Serialize)]
pub struct AuditEvent {
    pub ts_epoch_ms: i64,
    pub ts: String,
    pub event_id: String,
    pub source: &'static str,
    pub op: &'static str,
    pub status: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_kind: Option<String>,

    // exec
    #[serde(skip_serializing_if = "Option::is_none")]
    pub argv: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interactive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shell: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_shell: Option<bool>,

    // copy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,

    // session / proxy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_kind: Option<String>,

    // result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    // caller identity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_ssh_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_key_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_via_token: Option<bool>,

    // review outcome (when reviewed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_risk: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_reason: Option<String>,
}

impl AuditEvent {
    /// Build a fresh event with timestamp + id filled. Other fields default to
    /// `None` via the builder-style helper methods below.
    pub fn new(source: &'static str, op: &'static str, status: &'static str) -> Self {
        let now = SystemTime::now();
        let ts_epoch_ms = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let ts = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_default();
        Self {
            ts_epoch_ms,
            ts,
            event_id: Uuid::new_v4().to_string(),
            source,
            op,
            status,
            execution_id: None,
            target_input: None,
            gateway: None,
            end_target: None,
            gateway_kind: None,
            argv: None,
            command: None,
            interactive: None,
            tty: None,
            shell: None,
            no_shell: None,
            direction: None,
            remote_path: None,
            recursive: None,
            source_name: None,
            session_kind: None,
            exit_code: None,
            duration_ms: None,
            timeout_ms: None,
            error: None,
            caller_source: None,
            caller_peer: None,
            caller_ssh_user: None,
            caller_key_fingerprint: None,
            caller_via_token: None,
            review_risk: None,
            review_action: None,
            review_reason: None,
        }
    }
}

// ---------------------------------------------------------------------------
// AuditSink — cheaply-cloned handle wrapping the global appender state.
// ---------------------------------------------------------------------------

struct AuditState {
    writer: NonBlocking,
    guard: WorkerGuard,
    enabled: bool,
    include_identity: bool,
    path: PathBuf,
}

static AUDIT_STATE: OnceLock<Mutex<AuditState>> = OnceLock::new();
static AUDIT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Initialize the audit sink from configuration. Called once at daemon start.
/// Returns nothing — the sink is accessed globally via [`record`].
pub fn init_audit(config: &AuditConfig) -> Result<()> {
    let (resolved_path, writer, guard) = build_appender(config)?;
    AUDIT_ENABLED.store(config.enabled, Ordering::Relaxed);
    let state = AuditState {
        writer,
        guard,
        enabled: config.enabled,
        include_identity: config.include_identity,
        path: resolved_path.clone(),
    };
    // If already initialized (e.g. tests), the first wins; that's fine.
    let _ = AUDIT_STATE.set(Mutex::new(state));
    tracing::info!(
        enabled = config.enabled,
        path = %resolved_path.display(),
        "audit sink initialized"
    );
    Ok(())
}

/// Re-open the audit file at the stored path (SIGHUP rotation hook).
pub fn reopen_audit_output() -> Result<()> {
    let state = AUDIT_STATE
        .get()
        .ok_or_else(|| anyhow!("audit is not initialized"))?;
    let mut state = state
        .lock()
        .map_err(|_| anyhow!("audit state mutex is poisoned"))?;
    let (path, writer, guard) =
        build_appender_at(&state.path, state.enabled, state.include_identity)?;
    state.writer = writer;
    state.guard = guard;
    state.path = path;
    Ok(())
}

/// Update the enabled flag from a hot-reloaded config.
pub fn apply_config(config: &AuditConfig) {
    AUDIT_ENABLED.store(config.enabled, Ordering::Relaxed);
    if let Some(state) = AUDIT_STATE.get() {
        if let Ok(mut state) = state.lock() {
            state.enabled = config.enabled;
            state.include_identity = config.include_identity;
        }
    }
}

/// Whether audit recording is currently active. Checked by [`record`] and by
/// call sites deciding whether to build an event at all.
pub fn is_enabled() -> bool {
    AUDIT_ENABLED.load(Ordering::Relaxed)
}

/// Record one audit event as a JSON line. No-op when audit is disabled.
pub fn record(event: &AuditEvent) {
    if !is_enabled() {
        return;
    }
    let Some(state) = AUDIT_STATE.get() else {
        return;
    };
    let Ok(mut line) = serde_json::to_string(event) else {
        return;
    };
    line.push('\n');
    // Clone the channel-backed writer out of the mutex so we hold the lock
    // only briefly. NonBlocking is lossy-by-default, so write_all never blocks.
    let mut writer = {
        let state = state.lock();
        let Ok(state) = state else {
            return;
        };
        state.writer.clone()
    };
    let _ = writer.write_all(line.as_bytes());
}

/// Globally accessible helper for callers that want the configured identity
/// flag without holding a config reference.
pub fn include_identity() -> bool {
    AUDIT_STATE
        .get()
        .and_then(|s| s.lock().ok())
        .map(|s| s.include_identity)
        .unwrap_or(true)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn build_appender(config: &AuditConfig) -> Result<(PathBuf, NonBlocking, WorkerGuard)> {
    let raw = config
        .path
        .clone()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(crate::config::default_audit_log_path);
    let path = PathBuf::from(raw);
    build_appender_at(&path, config.enabled, config.include_identity)
}

fn build_appender_at(
    path: &PathBuf,
    _enabled: bool,
    _include_identity: bool,
) -> Result<(PathBuf, NonBlocking, WorkerGuard)> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("invalid audit log path {}", path.display()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("invalid audit log path {}", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create audit log directory {}", parent.display()))?;
    let appender = tracing_appender::rolling::never(parent, file_name);
    // A non-blocking, lossy writer: never blocks the audit caller. Dropped
    // lines under extreme backpressure are acceptable for an audit log.
    let (writer, guard) = NonBlockingBuilder::default()
        .thread_name("xho-audit")
        .finish(appender);
    Ok((path.clone(), writer, guard))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuditConfig;

    #[test]
    fn event_serializes_to_json_and_skips_none() {
        let mut ev = AuditEvent::new("control-plane", "exec", "started");
        ev.execution_id = Some("abc".to_string());
        ev.command = Some("ls -l".to_string());
        let json = serde_json::to_string(&ev).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["op"], "exec");
        assert_eq!(v["status"], "started");
        assert_eq!(v["execution_id"], "abc");
        assert_eq!(v["command"], "ls -l");
        // optional unset fields are skipped
        assert!(v.get("exit_code").is_none());
        assert!(v.get("direction").is_none());
        // core fields always present
        assert!(v["ts"].is_string());
        assert!(v["event_id"].is_string());
    }

    #[test]
    fn record_is_noop_when_disabled() {
        // Don't call init_audit (would clobber global state in other tests);
        // just ensure record() on an uninitialized/disabled sink doesn't panic.
        AUDIT_ENABLED.store(false, Ordering::Relaxed);
        let ev = AuditEvent::new("control-plane", "exec", "completed");
        record(&ev); // must not panic
    }

    #[test]
    fn audit_config_defaults() {
        let c = AuditConfig::default();
        assert!(c.enabled);
        assert!(c.include_identity);
        assert!(c.path.is_none());
    }

    #[tokio::test]
    async fn init_and_record_writes_jsonl() {
        use std::time::Duration;
        // Skip if audit was already initialized by a prior test (OnceLock).
        if AUDIT_STATE.get().is_some() {
            eprintln!("skipping init_and_record_writes_jsonl: audit already initialized");
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("audit.jsonl");
        let mut cfg = AuditConfig::default();
        cfg.path = Some(path.display().to_string());
        cfg.enabled = true;
        init_audit(&cfg).expect("init");

        let mut ev = AuditEvent::new("control-plane", "exec", "completed");
        ev.execution_id = Some("test-123".to_string());
        ev.exit_code = Some(0);
        ev.caller_peer = Some("127.0.0.1:12345".to_string());
        ev.caller_ssh_user = Some("alice".to_string());
        record(&ev);

        // The non-blocking writer flushes on a background thread; poll the file.
        let mut attempts = 0;
        let content = loop {
            attempts += 1;
            if let Ok(content) = std::fs::read_to_string(&path) {
                if !content.is_empty() {
                    break content;
                }
            }
            if attempts > 50 {
                panic!("audit file was not written after 50 attempts");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let line = content.lines().next().expect("at least one line");
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
        assert_eq!(v["op"], "exec");
        assert_eq!(v["status"], "completed");
        assert_eq!(v["execution_id"], "test-123");
        assert_eq!(v["exit_code"], 0);
        assert_eq!(v["caller_peer"], "127.0.0.1:12345");
        assert_eq!(v["caller_ssh_user"], "alice");
    }
}
