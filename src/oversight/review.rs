//! AI command/file review via an OpenAI-compatible chat-completions endpoint.
//!
//! The [`CommandReviewer`] holds a reusable HTTP client. Its [`review`]
//! method dispatches on [`OperationKind`](super::OperationKind): `exec` and
//! `copy` go through the LLM (with allow/blocklist short-circuits), while
//! `session` and `proxy` are not reviewed.
//!
//! Shared LLM connection settings (endpoint/model/api_key/timeout/headers) live
//! at the top of [`ReviewConfig`](crate::config::ReviewConfig); per-operation
//! settings (enable/prompts/policy/lists) live in `ReviewConfig.exec` and
//! `ReviewConfig.copy`.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::config::{ReviewAction, ReviewConfig, RiskLevel, Secret, SecretResolver};

use super::{Operation, OperationKind, ReviewOutcome};

/// Holds a reusable HTTP client for LLM calls.
#[derive(Clone)]
pub struct CommandReviewer {
    #[allow(dead_code)]
    client: Option<reqwest::Client>,
}

impl CommandReviewer {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder().build()?;
        Ok(Self {
            client: Some(client),
        })
    }

    /// A reviewer with no HTTP client — every review returns [`ReviewOutcome::Skipped`].
    /// Used by tests and by daemons that only need the audit sink.
    pub fn new_disabled() -> Self {
        Self { client: None }
    }

    /// Review an operation. See module docs for per-kind behavior.
    pub async fn review(
        &self,
        config: &ReviewConfig,
        app_config: &crate::config::AppConfig,
        op: &Operation<'_>,
    ) -> ReviewOutcome {
        match op.kind {
            OperationKind::Exec | OperationKind::ExecInteractive => {
                self.review_exec(config, app_config, op).await
            }
            OperationKind::Copy => self.review_copy(config, app_config, op).await,
            // session/proxy are not reviewed — audit-only.
            OperationKind::Session | OperationKind::Proxy => ReviewOutcome::Skipped,
        }
    }

    async fn review_exec(
        &self,
        config: &ReviewConfig,
        app_config: &crate::config::AppConfig,
        op: &Operation<'_>,
    ) -> ReviewOutcome {
        let exec_cfg = &config.exec;
        if !exec_cfg.enable {
            return ReviewOutcome::Skipped;
        }
        let super::OperationDetail::Exec { argv, command, .. } = &op.detail else {
            return ReviewOutcome::Skipped;
        };
        // Fast local allowlist short-circuits the LLM.
        if let Some(decision) = fast_allow(exec_cfg, argv) {
            return ReviewOutcome::Decision(decision);
        }
        self.call_llm_exec(config, app_config, &op.route.end_target, argv, command)
            .await
            .map_or_else(ReviewOutcome::Failed, ReviewOutcome::Decision)
    }

    async fn review_copy(
        &self,
        config: &ReviewConfig,
        app_config: &crate::config::AppConfig,
        op: &Operation<'_>,
    ) -> ReviewOutcome {
        let copy_cfg = &config.copy;
        if !copy_cfg.enable {
            return ReviewOutcome::Skipped;
        }
        let super::OperationDetail::Copy {
            direction,
            remote_path,
            source_name,
            ..
        } = &op.detail
        else {
            return ReviewOutcome::Skipped;
        };
        let target = &op.route.end_target;

        // blocklist → deny short-circuit.
        if matches_any(&copy_cfg.blocklist, remote_path, source_name) {
            return ReviewOutcome::Decision(ReviewDecision {
                risk_level: RiskLevel::Dangerous,
                action: ReviewAction::Deny,
                reason: "copy path matched blocklist pattern".to_string(),
                matched_whitelist_reason: Some("blocklist".to_string()),
            });
        }
        // allowlist → allow short-circuit.
        if matches_any(&copy_cfg.allowlist, remote_path, source_name) {
            return ReviewOutcome::Decision(ReviewDecision {
                risk_level: RiskLevel::Safe,
                action: copy_cfg.policy.action_for(RiskLevel::Safe),
                reason: "copy path matched allowlist pattern".to_string(),
                matched_whitelist_reason: Some("allowlist".to_string()),
            });
        }

        self.call_llm_copy(
            config,
            app_config,
            target,
            *direction,
            remote_path,
            source_name,
        )
        .await
        .map_or_else(ReviewOutcome::Failed, ReviewOutcome::Decision)
    }

    async fn call_llm_exec(
        &self,
        config: &ReviewConfig,
        app_config: &crate::config::AppConfig,
        target: &str,
        argv: &[String],
        shell_command: &str,
    ) -> Result<ReviewDecision> {
        let exec_cfg = &config.exec;
        if config.endpoint.is_empty() || config.model.is_empty() {
            bail!("review is enabled but endpoint/model is missing");
        }
        let resolver = app_config.secret_resolver(None);
        let whitelist = render_semantic_whitelist(&exec_cfg.semantic_whitelist);
        let user_prompt = format!(
            "{}\n\nTarget host: {}\nArgv JSON: {}\nShell command: {}\n\nSemantic whitelist intents:\n{}\n\nReturn JSON only.",
            exec_cfg.prompts.template,
            target,
            serde_json::to_string(argv)?,
            shell_command,
            whitelist,
        );
        let result = self
            .call_llm(config, &resolver, &exec_cfg.prompts.system, &user_prompt)
            .await?;
        Ok(ReviewDecision {
            action: exec_cfg.policy.action_for(result.risk_level),
            risk_level: result.risk_level,
            reason: result.reason,
            matched_whitelist_reason: result.matched_whitelist_reason,
        })
    }

    async fn call_llm_copy(
        &self,
        config: &ReviewConfig,
        app_config: &crate::config::AppConfig,
        target: &str,
        direction: crate::types::CopyDirection,
        remote_path: &str,
        source_name: &str,
    ) -> Result<ReviewDecision> {
        let copy_cfg = &config.copy;
        if config.endpoint.is_empty() || config.model.is_empty() {
            bail!("review is enabled but endpoint/model is missing");
        }
        let resolver = app_config.secret_resolver(None);
        let dir_str = match direction {
            crate::types::CopyDirection::Upload => "upload (local → target)",
            crate::types::CopyDirection::Download => "download (target → local)",
        };
        let user_prompt = format!(
            "{}\n\nTarget host: {}\nDirection: {}\nRemote path: {}\nSource name: {}\n\nReturn JSON only.",
            copy_cfg.prompts.template, target, dir_str, remote_path, source_name,
        );
        let result = self
            .call_llm(config, &resolver, &copy_cfg.prompts.system, &user_prompt)
            .await?;
        Ok(ReviewDecision {
            action: copy_cfg.policy.action_for(result.risk_level),
            risk_level: result.risk_level,
            reason: result.reason,
            matched_whitelist_reason: result.matched_whitelist_reason,
        })
    }

    /// Shared LLM HTTP call for both exec and copy review paths.
    async fn call_llm(
        &self,
        config: &ReviewConfig,
        resolver: &SecretResolver,
        system: &str,
        user_prompt: &str,
    ) -> Result<ReviewModelResult> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("reviewer is disabled (no HTTP client)"))?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(api_key) = &config.api_key {
            let api_key = Secret::from_reference(api_key)
                .resolve(resolver)
                .context("failed to resolve review api key")?;
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", *api_key))
                    .context("invalid review api key header")?,
            );
        }
        apply_extra_headers(&mut headers, resolver, &config.headers)?;

        let request = ChatCompletionsRequest {
            model: config.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: system.to_string(),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: user_prompt.to_string(),
                },
            ],
            temperature: 0.0,
        };
        let response = client
            .post(&config.endpoint)
            .headers(headers)
            .timeout(config.timeout)
            .json(&request)
            .send()
            .await
            .context("review request failed")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("review request failed with status {}: {}", status, body);
        }
        let payload: ChatCompletionsResponse = response.json().await?;
        let content = payload
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("review response has no choices"))?
            .message
            .content;
        let normalized = normalize_json_content(&content);
        let result: ReviewModelResult =
            serde_json::from_str(&normalized).context("failed to parse review result JSON")?;
        Ok(result)
    }
}

/// The result of a review: classification + resolved action + explanation.
#[derive(Clone, Debug)]
pub struct ReviewDecision {
    pub risk_level: RiskLevel,
    pub action: ReviewAction,
    pub reason: String,
    pub matched_whitelist_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// exec fast-allowlist helpers
// ---------------------------------------------------------------------------

fn fast_allow(
    exec_cfg: &crate::config::ReviewExecConfig,
    argv: &[String],
) -> Option<ReviewDecision> {
    if !exec_cfg.fast_allowlist.enable || argv.is_empty() {
        return None;
    }
    if is_complex_script(argv) {
        return None;
    }
    let raw_command = argv.join(" ");
    if matches_fast_allowlist(&exec_cfg.fast_allowlist.commands, argv, &raw_command) {
        return Some(ReviewDecision {
            risk_level: RiskLevel::Safe,
            action: exec_cfg.policy.action_for(RiskLevel::Safe),
            reason: "Matched local fast allowlist for a simple command.".to_string(),
            matched_whitelist_reason: Some("fast_allowlist".to_string()),
        });
    }
    None
}

fn is_complex_script(argv: &[String]) -> bool {
    if argv.is_empty() {
        return false;
    }
    let first = argv[0].as_str();
    if matches!(
        first,
        "bash" | "sh" | "zsh" | "python" | "python3" | "perl" | "ruby"
    ) {
        return true;
    }
    argv.iter().any(|arg| {
        arg.contains("&&")
            || arg.contains("||")
            || arg.contains(";")
            || arg.contains("$(")
            || arg.contains('`')
            || arg.contains('\n')
    })
}

fn matches_fast_allowlist(patterns: &[String], argv: &[String], raw_command: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| matches_fast_pattern(pattern, argv, raw_command))
}

fn matches_fast_pattern(pattern: &str, argv: &[String], raw_command: &str) -> bool {
    if pattern.contains('*') {
        return glob_match(pattern, raw_command);
    }
    if argv.len() == 1 {
        return argv[0] == pattern;
    }
    raw_command == pattern
}

/// Match a copy path against glob patterns. A path matches if it (or any
/// path component) hits the pattern. Patterns may be bare directory names
/// (e.g. `.ssh`, matching `/home/x/.ssh/id_rsa`), absolute globs (`/etc/*`),
/// or relative globs (`~/.ssh/*`).
pub fn matches_any(patterns: &[String], remote_path: &str, source_name: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| path_matches(pattern, remote_path) || path_matches(pattern, source_name))
}

/// One pattern against one path: exact glob match, segment match, or prefix
/// match. Handles bare names (`.ssh`), absolute dirs (`/etc/ssh`), and globs
/// (`/var/log/*`, `*.pem`).
fn path_matches(pattern: &str, path: &str) -> bool {
    if glob_match(pattern, path) {
        return true;
    }
    // Prefix match: `/etc/ssh` matches `/etc/ssh/sshd_config`.
    if let Some(rest) = path.strip_prefix(pattern) {
        if rest.is_empty() || rest.starts_with('/') {
            return true;
        }
    }
    // Segment match: `.ssh` matches `/home/u/.ssh` and `~/.ssh/id_rsa`.
    let segments: Vec<&str> = path.split('/').collect();
    if segments.iter().any(|seg| *seg == pattern) {
        return true;
    }
    // Glob-as-segment: `*.pem` matches `id_rsa.pem` inside any dir.
    segments
        .iter()
        .any(|seg| pattern.contains('*') && glob_match(pattern, seg))
}

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_inner(
        &pattern.chars().collect::<Vec<_>>(),
        &text.chars().collect::<Vec<_>>(),
        0,
        0,
    )
}

fn glob_match_inner(pattern: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    if pi == pattern.len() {
        return ti == text.len();
    }
    match pattern[pi] {
        '*' => {
            for next_ti in ti..=text.len() {
                if glob_match_inner(pattern, text, pi + 1, next_ti) {
                    return true;
                }
            }
            false
        }
        ch => ti < text.len() && ch == text[ti] && glob_match_inner(pattern, text, pi + 1, ti + 1),
    }
}

fn apply_extra_headers(
    headers: &mut HeaderMap,
    resolver: &SecretResolver,
    extras: &HashMap<String, String>,
) -> Result<()> {
    for (key, value) in extras {
        let value = Secret::from_reference(value)
            .resolve(resolver)
            .with_context(|| format!("failed to resolve review header value for {}", key))?;
        headers.insert(
            HeaderName::from_bytes(key.as_bytes())
                .with_context(|| format!("invalid review header {}", key))?,
            HeaderValue::from_str(&value)
                .with_context(|| format!("invalid review header value for {}", key))?,
        );
    }
    Ok(())
}

fn render_semantic_whitelist(entries: &[crate::config::SemanticWhitelistEntry]) -> String {
    if entries.is_empty() {
        return "None.".to_string();
    }
    entries
        .iter()
        .map(|entry| {
            let examples = if entry.examples.is_empty() {
                String::new()
            } else {
                format!("; examples: {}", entry.examples.join(" | "))
            };
            format!("- {}: {}{}", entry.name, entry.description, examples)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn normalize_json_content(content: &str) -> String {
    let trimmed = content.trim();
    if let Some(body) = trimmed
        .strip_prefix("```json")
        .and_then(|inner| inner.strip_suffix("```"))
    {
        return body.trim().to_string();
    }
    if let Some(body) = trimmed
        .strip_prefix("```")
        .and_then(|inner| inner.strip_suffix("```"))
    {
        return body.trim().to_string();
    }
    trimmed.to_string()
}

// ---------------------------------------------------------------------------
// Wire types (OpenAI-compatible chat completions)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ChatCompletionsRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionsResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct ReviewModelResult {
    risk_level: RiskLevel,
    reason: String,
    #[serde(default)]
    matched_whitelist_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_allowlist_matches_simple_command() {
        let mut cfg = crate::config::ReviewExecConfig::default();
        cfg.fast_allowlist.enable = true;
        cfg.fast_allowlist.commands = vec!["ls".to_string()];
        let argv = vec!["ls".to_string()];
        let decision = fast_allow(&cfg, &argv).expect("should match");
        assert_eq!(decision.action, crate::config::ReviewAction::Allow);
        assert_eq!(
            decision.matched_whitelist_reason.as_deref(),
            Some("fast_allowlist")
        );
    }

    #[test]
    fn fast_allowlist_skips_complex_scripts() {
        let mut cfg = crate::config::ReviewExecConfig::default();
        cfg.fast_allowlist.enable = true;
        cfg.fast_allowlist.commands = vec!["rm".to_string()];
        // complex (contains ;)
        let argv = vec!["rm".to_string(), "x;rm".to_string()];
        assert!(fast_allow(&cfg, &argv).is_none());
    }

    #[test]
    fn copy_blocklist_matches_ssh_dir() {
        let patterns = crate::config::default_copy_blocklist();
        // bare `.ssh` matches a deep path containing that segment
        assert!(matches_any(&patterns, "/home/alice/.ssh/id_rsa", "id_rsa"));
        assert!(matches_any(&patterns, "~/.ssh/config", "config"));
        assert!(matches_any(
            &patterns,
            "/etc/ssh/sshd_config",
            "sshd_config"
        ));
    }

    #[test]
    fn copy_allowlist_glob_matches() {
        let patterns = vec!["/var/log/*".to_string()];
        assert!(matches_any(&patterns, "/var/log/nginx.log", "nginx.log"));
        assert!(!matches_any(&patterns, "/etc/passwd", "passwd"));
    }

    #[test]
    fn copy_blocklist_does_not_match_benign() {
        let patterns = crate::config::default_copy_blocklist();
        assert!(!matches_any(
            &patterns,
            "/srv/app/build.tar.gz",
            "build.tar.gz"
        ));
        assert!(!matches_any(&patterns, "/tmp/notes.txt", "notes.txt"));
    }

    #[test]
    fn glob_match_basic() {
        assert!(glob_match("*.pem", "key.pem"));
        assert!(!glob_match("*.pem", "key.txt"));
        assert!(glob_match("/etc/*", "/etc/hosts"));
    }
}
