//! Oversight: a unified layer that combines AI command review and audit logging
//! for every machine operation the daemon performs.
//!
//! Two concerns, one entry point:
//! - [`audit`] records a JSON-Lines audit trail (always on by default).
//! - [`review`] classifies an operation via an LLM (opt-in per operation kind)
//!   and returns a [`ReviewOutcome`] the caller enforces.
//!
//! All four operation surfaces — `exec`, `copy`, `open_session` tunnel, and the
//! transparent 2222 proxy — route through this layer via the [`Oversight`]
//! facade, sharing a single [`Operation`] description and [`Caller`] identity.

pub mod audit;
pub mod review;

use anyhow::Result;
use tonic::Request;

use crate::config::AppConfig;
use crate::daemon::gateway::{GatewayKind, Route};
use crate::daemon::ssh_server::RemoteConnectInfo;
use crate::types::CopyDirection;

use review::CommandReviewer;

// Re-export the main review types so callers depend on `oversight::` only.
pub use audit::{AuditEvent, is_enabled as audit_enabled, record as record_audit};
pub use review::ReviewDecision;

/// Which operation surface a request came through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Exec,
    ExecInteractive,
    Copy,
    /// `open_session` multi-hop tunnel (control-plane).
    Session,
    /// Transparent SSH proxy on port 2222.
    Proxy,
}

impl OperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OperationKind::Exec => "exec",
            OperationKind::ExecInteractive => "exec-interactive",
            OperationKind::Copy => "copy",
            OperationKind::Session => "session",
            OperationKind::Proxy => "proxy",
        }
    }
}

/// Operation-kind-specific payload for [`Operation`]. All variants hold only
/// borrowed data, so this is `Copy`.
#[derive(Clone, Copy, Debug)]
pub enum OperationDetail<'a> {
    Exec {
        argv: &'a [String],
        command: &'a str,
        interactive: bool,
        tty: bool,
        shell: bool,
        no_shell: bool,
    },
    Copy {
        direction: CopyDirection,
        remote_path: &'a str,
        recursive: bool,
        source_name: &'a str,
    },
    /// A session/proxy inner operation (exec/shell/subsystem).
    SessionOp {
        session_kind: SessionKind,
        command_or_name: &'a str,
    },
}

/// Sub-class of a session/proxy operation.
#[derive(Clone, Copy, Debug)]
pub enum SessionKind {
    Exec,
    Shell,
    Subsystem,
}

impl SessionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionKind::Exec => "exec",
            SessionKind::Shell => "shell",
            SessionKind::Subsystem => "subsystem",
        }
    }
}

/// A fully-resolved, auditable/reviewable description of one machine operation.
pub struct Operation<'a> {
    pub kind: OperationKind,
    pub route: &'a Route,
    pub gateway_kind: Option<GatewayKind>,
    pub caller: &'a Caller,
    pub detail: OperationDetail<'a>,
    pub execution_id: Option<&'a str>,
    pub timeout_ms: Option<u64>,
}

/// Identity of the caller that initiated an operation. Populated from the SSH
/// transport layer for remote connections; empty for local Unix-socket callers.
#[derive(Clone, Debug, Default)]
pub struct Caller {
    /// `"control-plane"` (gRPC over local socket or remote SSH subsystem) or
    /// `"proxy-ssh"` (transparent 2222 proxy).
    pub source: &'static str,
    pub peer_addr: Option<String>,
    pub ssh_user: Option<String>,
    pub key_fingerprint: Option<String>,
    pub via_token: bool,
}

impl Caller {
    /// A local Unix-socket caller: no transport-level identity.
    pub fn local() -> Self {
        Self {
            source: "control-plane",
            ..Default::default()
        }
    }

    /// A transparent-proxy caller: identified by SSH username (= target).
    pub fn proxy(peer: Option<String>, ssh_user: String, key_fingerprint: Option<String>) -> Self {
        Self {
            source: "proxy-ssh",
            peer_addr: peer,
            ssh_user: Some(ssh_user),
            key_fingerprint,
            via_token: false,
        }
    }
}

/// The outcome of reviewing an operation.
#[derive(Debug)]
pub enum ReviewOutcome {
    /// Review is not applicable (session/proxy, or the operation kind's review
    /// is disabled). The caller should proceed without enforcement.
    Skipped,
    /// The LLM (or a list short-circuit) produced a decision the caller must
    /// enforce according to its [`ReviewAction`](crate::config::ReviewAction).
    Decision(ReviewDecision),
    /// The review service itself failed. The caller applies `failure_action`.
    Failed(anyhow::Error),
}

/// The unified oversight facade held by [`DaemonState`].
#[derive(Clone)]
pub struct Oversight {
    reviewer: CommandReviewer,
}

impl Oversight {
    pub fn new() -> Result<Self> {
        Ok(Self {
            reviewer: CommandReviewer::new()?,
        })
    }

    /// A no-op oversight (reviewer without an LLM call). Useful for tests and
    /// paths that only need the audit sink.
    pub fn disabled() -> Self {
        Self {
            reviewer: CommandReviewer::new_disabled(),
        }
    }

    /// Review an operation. Session/proxy always return [`ReviewOutcome::Skipped`].
    pub async fn review(&self, config: &AppConfig, op: &Operation<'_>) -> ReviewOutcome {
        self.reviewer.review(&config.review, config, op).await
    }

    /// Initialize the audit sink from config. Must be called once at startup.
    pub fn init_audit(&self, config: &AppConfig) -> Result<()> {
        audit::init_audit(&config.audit)
    }

    /// Apply a hot-reloaded config (audit enabled flag / identity flag).
    pub fn apply_config(&self, config: &AppConfig) {
        audit::apply_config(&config.audit);
    }
}

/// Extract caller identity from a tonic request's connection-info extension.
///
/// For remote SSH-subsystem connections this yields peer/user/fingerprint; for
/// local Unix-socket connections (and reverse-proxy connections that never reach
/// tonic) it yields an empty [`Caller::local`].
pub fn extract_caller<T>(request: &Request<T>) -> Caller {
    // tonic stores the `ConnectInfo` in the request extensions.
    let info = request
        .extensions()
        .get::<Option<RemoteConnectInfo>>()
        .and_then(|opt| opt.as_ref());
    match info {
        Some(info) => Caller {
            source: "control-plane",
            peer_addr: info.peer_addr.map(|a| a.to_string()),
            ssh_user: Some(info.ssh_user.clone()),
            key_fingerprint: Some(info.public_key_fingerprint.clone()),
            via_token: info.via_token,
        },
        None => Caller::local(),
    }
}
