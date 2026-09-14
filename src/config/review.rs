use std::collections::HashMap;
use std::env;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::duration::{deserialize_duration, serialize_duration};

/// AI review configuration. The LLM connection fields (endpoint/model/api_key/
/// timeout/headers/failure_action) are shared between all reviewed operation
/// kinds; each operation kind has its own sub-config for enable flag, prompts,
/// policy and allow/blocklists.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewConfig {
    // --- shared LLM connection ---
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub timeout: Duration,
    pub headers: HashMap<String, String>,
    /// Fallback action when the LLM service itself errors (network failure,
    /// bad response, parse error). Applied by the oversight layer.
    pub failure_action: ReviewAction,

    // --- per-operation review config ---
    pub exec: ReviewExecConfig,
    pub copy: ReviewCopyConfig,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            endpoint: default_review_endpoint(),
            model: default_review_model(),
            api_key: default_review_api_key(),
            timeout: Duration::from_secs(10),
            headers: HashMap::new(),
            failure_action: ReviewAction::Deny,
            exec: ReviewExecConfig::default(),
            copy: ReviewCopyConfig::default(),
        }
    }
}

/// AI review settings for `exec` operations.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewExecConfig {
    pub enable: bool,
    pub prompts: ReviewPrompts,
    pub policy: ReviewPolicy,
    pub fast_allowlist: FastAllowlistConfig,
    pub semantic_whitelist: Vec<SemanticWhitelistEntry>,
}

impl Default for ReviewExecConfig {
    fn default() -> Self {
        Self {
            enable: false,
            prompts: ReviewPrompts::default(),
            policy: ReviewPolicy::default(),
            fast_allowlist: FastAllowlistConfig::default(),
            semantic_whitelist: default_semantic_whitelist(),
        }
    }
}

/// AI review settings for `cp` (file copy) operations. Copies matching the
/// blocklist are denied immediately; copies matching the allowlist are allowed
/// immediately; everything else is classified by the LLM.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewCopyConfig {
    pub enable: bool,
    pub prompts: ReviewCopyPrompts,
    pub policy: ReviewPolicy,
    /// Glob patterns matched against `remote_path` and `source_name`.
    /// A match short-circuits to an allow decision (e.g. `/var/log/*`).
    pub allowlist: Vec<String>,
    /// Glob patterns matched against `remote_path` and `source_name`.
    /// A match short-circuits to a deny decision (e.g. `~/.ssh/*`).
    pub blocklist: Vec<String>,
}

impl Default for ReviewCopyConfig {
    fn default() -> Self {
        Self {
            enable: false,
            prompts: ReviewCopyPrompts::default(),
            policy: ReviewPolicy::default(),
            allowlist: Vec::new(),
            blocklist: default_copy_blocklist(),
        }
    }
}

/// System + user template for copy-review LLM prompts.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewCopyPrompts {
    pub system: String,
    pub template: String,
}

impl Default for ReviewCopyPrompts {
    fn default() -> Self {
        Self {
            system: default_copy_review_system_prompt(),
            template: default_copy_review_template(),
        }
    }
}

/// Default copy blocklist: credential and config directories that should not be
/// silently copied. Operators can override via `[review.copy] blocklist`.
pub fn default_copy_blocklist() -> Vec<String> {
    vec![
        ".ssh".to_string(),
        ".aws".to_string(),
        ".gnupg".to_string(),
        ".kube".to_string(),
        "/etc/shadow".to_string(),
        "/etc/ssh".to_string(),
    ]
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewPrompts {
    pub system: String,
    pub template: String,
}

impl Default for ReviewPrompts {
    fn default() -> Self {
        Self {
            system: default_review_system_prompt(),
            template: default_review_template(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewPolicy {
    pub safe: ReviewAction,
    pub risky: ReviewAction,
    pub dangerous: ReviewAction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct FastAllowlistConfig {
    pub enable: bool,
    pub commands: Vec<String>,
}

impl Default for FastAllowlistConfig {
    fn default() -> Self {
        Self {
            enable: true,
            commands: Vec::new(),
        }
    }
}

impl Default for ReviewPolicy {
    fn default() -> Self {
        Self {
            safe: ReviewAction::Allow,
            risky: ReviewAction::Confirm,
            dangerous: ReviewAction::Deny,
        }
    }
}

impl ReviewPolicy {
    pub fn action_for(&self, risk: RiskLevel) -> ReviewAction {
        match risk {
            RiskLevel::Safe => self.safe,
            RiskLevel::Risky => self.risky,
            RiskLevel::Dangerous => self.dangerous,
        }
    }
}

pub fn default_review_endpoint() -> String {
    "https://api.openai.com/v1/chat/completions".to_string()
}

pub fn default_review_model() -> String {
    "gpt-4.1-mini".to_string()
}

pub fn default_review_api_key() -> Option<String> {
    env::var("XHO_REVIEW_API_KEY")
        .ok()
        .or_else(|| env::var("OPENAI_API_KEY").ok())
}

pub fn default_review_system_prompt() -> String {
    [
        "You are a Unix command safety reviewer for a remote execution tool.",
        "Your job is to classify whether a command is operationally safe before execution.",
        "Be conservative and judge the overall effect of the full command, not isolated substrings.",
        "Return JSON only, with no markdown and no extra text.",
    ]
    .join(" ")
}

pub fn default_review_template() -> String {
    [
        "Classify the command into exactly one risk level: safe, risky, or dangerous.",
        "safe: read-only inspection or diagnostics that should not modify files, system state, services, users, packages, network policy, or remote data.",
        "risky: bounded changes to application or working state with moderate blast radius, where human confirmation is reasonable.",
        "dangerous: deletion, overwrite, download-and-execute, package install, service restart/stop, privilege escalation, system configuration change, cluster mutation, credential access, data exfiltration, or any broad/uncertain shell script.",
        "Judge the whole command after shell operators, pipes, redirects, subshells, and scripts are considered together.",
        "Semantic whitelist entries are only hints. They can justify safe only when the entire command is actually read-only or otherwise clearly within the allowed intent.",
        "If a command mixes a benign subcommand with any mutating or unclear behavior, do not whitelist it.",
        "Return compact JSON with keys: risk_level, reason, matched_whitelist_reason.",
        "matched_whitelist_reason must be null when no whitelist intent applies.",
    ]
    .join("\n")
}

pub fn default_copy_review_system_prompt() -> String {
    [
        "You are a file-transfer safety reviewer for a remote operations tool.",
        "Your job is to classify whether copying a file or directory to/from a target host is safe.",
        "Be conservative: credential files, secrets, private keys, and broad system directories are sensitive.",
        "Return JSON only, with no markdown and no extra text.",
    ]
    .join(" ")
}

pub fn default_copy_review_template() -> String {
    [
        "Classify the copy operation into exactly one risk level: safe, risky, or dangerous.",
        "safe: ordinary application data, logs, build artifacts, or public files with no secret material.",
        "risky: sensitive but non-secret paths where exfiltration or overwrite could cause harm.",
        "dangerous: credentials, private keys, shadow files, or any path likely to contain secrets.",
        "Consider the direction: downloading secrets from a host is exfiltration; overwriting system files is destructive.",
        "Return compact JSON with keys: risk_level, reason, matched_whitelist_reason.",
        "matched_whitelist_reason must be null when no special reason applies.",
    ]
    .join("\n")
}

pub fn default_semantic_whitelist() -> Vec<SemanticWhitelistEntry> {
    vec![
        SemanticWhitelistEntry {
            name: "read-only inspection".to_string(),
            description: "Read-only inspection of files, logs, process state, sockets, environment, or system metadata.".to_string(),
            examples: vec![
                "cat /etc/hosts".to_string(),
                "journalctl -u nginx".to_string(),
                "ps aux | grep kubelet".to_string(),
            ],
        },
        SemanticWhitelistEntry {
            name: "source and git inspection".to_string(),
            description: "Read-only inspection of source code or git history/status without checkout, reset, clean, apply, or commit.".to_string(),
            examples: vec![
                "grep -R TODO src".to_string(),
                "git status --short".to_string(),
                "git log --oneline -20".to_string(),
            ],
        },
        SemanticWhitelistEntry {
            name: "kubernetes read-only inspection".to_string(),
            description: "Cluster inspection commands that only get, describe, or view logs and do not patch, edit, apply, delete, scale, or exec.".to_string(),
            examples: vec![
                "kubectl get pods -A".to_string(),
                "kubectl describe pod my-pod -n prod".to_string(),
                "kubectl logs deploy/api -n prod --since=10m".to_string(),
            ],
        },
    ]
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct SemanticWhitelistEntry {
    pub name: String,
    pub description: String,
    pub examples: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAction {
    Allow,
    Warn,
    Confirm,
    Deny,
}

impl fmt::Display for ReviewAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReviewAction::Allow => write!(f, "allow"),
            ReviewAction::Warn => write!(f, "warn"),
            ReviewAction::Confirm => write!(f, "confirm"),
            ReviewAction::Deny => write!(f, "deny"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Safe,
    Risky,
    Dangerous,
}

impl fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RiskLevel::Safe => write!(f, "safe"),
            RiskLevel::Risky => write!(f, "risky"),
            RiskLevel::Dangerous => write!(f, "dangerous"),
        }
    }
}
