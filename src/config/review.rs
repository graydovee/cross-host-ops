use std::collections::HashMap;
use std::env;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::duration::{deserialize_duration, serialize_duration};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct MfaConfig {
    pub totp_secret_base32: String,
    pub digits: u32,
    pub period: u64,
    pub digest: String,
}

impl Default for MfaConfig {
    fn default() -> Self {
        Self {
            totp_secret_base32: String::new(),
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReviewConfig {
    pub enable: bool,
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    pub timeout: Duration,
    pub failure_action: ReviewAction,
    pub headers: HashMap<String, String>,
    pub prompts: ReviewPrompts,
    pub policy: ReviewPolicy,
    pub fast_allowlist: FastAllowlistConfig,
    pub semantic_whitelist: Vec<SemanticWhitelistEntry>,
}

impl Default for ReviewConfig {
    fn default() -> Self {
        Self {
            enable: false,
            endpoint: default_review_endpoint(),
            model: default_review_model(),
            api_key: default_review_api_key(),
            timeout: Duration::from_secs(10),
            failure_action: ReviewAction::Deny,
            headers: HashMap::new(),
            prompts: ReviewPrompts::default(),
            policy: ReviewPolicy::default(),
            fast_allowlist: FastAllowlistConfig::default(),
            semantic_whitelist: default_semantic_whitelist(),
        }
    }
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
