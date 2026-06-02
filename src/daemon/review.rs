use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::config::{ReviewAction, ReviewConfig, RiskLevel};

#[derive(Clone)]
pub struct CommandReviewer {
    client: reqwest::Client,
}

impl CommandReviewer {
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder().build()?;
        Ok(Self { client })
    }

    pub async fn review(
        &self,
        config: &ReviewConfig,
        target: &str,
        argv: &[String],
        shell_command: &str,
    ) -> Result<Option<ReviewDecision>> {
        if !config.enable {
            return Ok(None);
        }
        if let Some(decision) = fast_allow(config, argv) {
            return Ok(Some(decision));
        }
        if config.endpoint.is_empty() || config.model.is_empty() {
            bail!("review is enabled but endpoint/model is missing");
        }
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(api_key) = &config.api_key {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", api_key))
                    .context("invalid review api key header")?,
            );
        }
        apply_extra_headers(&mut headers, &config.headers)?;

        let whitelist = if config.semantic_whitelist.is_empty() {
            "None.".to_string()
        } else {
            config
                .semantic_whitelist
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
        };

        let user_prompt = format!(
            "{}\n\nTarget host: {}\nArgv JSON: {}\nShell command: {}\n\nSemantic whitelist intents:\n{}\n\nReturn JSON only.",
            config.prompts.template,
            target,
            serde_json::to_string(argv)?,
            shell_command,
            whitelist
        );

        let request = ChatCompletionsRequest {
            model: config.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: config.prompts.system.clone(),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: user_prompt,
                },
            ],
            temperature: 0.0,
        };

        let response = self
            .client
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
        let action = config.policy.action_for(result.risk_level);
        Ok(Some(ReviewDecision {
            risk_level: result.risk_level,
            action,
            reason: result.reason,
            matched_whitelist_reason: result.matched_whitelist_reason,
        }))
    }
}

fn fast_allow(config: &ReviewConfig, argv: &[String]) -> Option<ReviewDecision> {
    if !config.fast_allowlist.enable || argv.is_empty() {
        return None;
    }
    if is_complex_script(argv) {
        return None;
    }
    let raw_command = argv.join(" ");
    if matches_fast_allowlist(&config.fast_allowlist.commands, argv, &raw_command) {
        return Some(ReviewDecision {
            risk_level: RiskLevel::Safe,
            action: config.policy.action_for(RiskLevel::Safe),
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

fn apply_extra_headers(headers: &mut HeaderMap, extras: &HashMap<String, String>) -> Result<()> {
    for (key, value) in extras {
        headers.insert(
            HeaderName::from_bytes(key.as_bytes())
                .with_context(|| format!("invalid review header {}", key))?,
            HeaderValue::from_str(value)
                .with_context(|| format!("invalid review header value for {}", key))?,
        );
    }
    Ok(())
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

#[derive(Clone, Debug)]
pub struct ReviewDecision {
    pub risk_level: RiskLevel,
    pub action: ReviewAction,
    pub reason: String,
    pub matched_whitelist_reason: Option<String>,
}

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
