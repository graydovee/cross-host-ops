//! Non-interactive auth resolution logic.
//!
//! Provides environment-variable credential bypass and three-way dispatch:
//! env var hit → Answer; non-interactive + no env → Fail(exit 126); else → PromptStdin.

use crate::protocol::AuthPromptMessage;

/// Credentials sourced from environment variables, used to bypass interactive prompts.
#[derive(Clone, Debug, Default)]
pub struct EnvCredentials {
    pub password: Option<String>,
    pub totp_secret: Option<String>,
}

impl EnvCredentials {
    /// Read credentials from the environment.
    ///
    /// - `RHOP_PASSWORD` → `password`
    /// - `RHOP_TOTP_SECRET` → `totp_secret`
    pub fn from_env() -> Self {
        Self {
            password: std::env::var("RHOP_PASSWORD").ok(),
            totp_secret: std::env::var("RHOP_TOTP_SECRET").ok(),
        }
    }

    /// Look up the credential value for a given auth prompt kind.
    ///
    /// Returns `Some(&str)` if the matching env var was set, `None` otherwise.
    pub fn for_kind(&self, kind: &str) -> Option<&str> {
        match kind {
            "password" => self.password.as_deref(),
            "jump_mfa" | "mfa" | "totp" => self.totp_secret.as_deref(),
            _ => None,
        }
    }
}

/// The result of resolving how to respond to an auth prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthResolution {
    /// The prompt can be answered immediately with this value (from env var).
    Answer(String),
    /// The prompt cannot be answered: non-interactive mode with no env var.
    /// The string describes the failure reason (exit 126).
    Fail(String),
    /// The prompt should be presented to the user on stdin.
    PromptStdin,
}

/// Resolve how to respond to an auth prompt given the environment credentials
/// and the non-interactive flag.
///
/// Priority:
/// 1. If the matching env var is set, return `Answer` (even in non-interactive mode).
/// 2. If `non_interactive` is true and no env var matches, return `Fail`.
/// 3. Otherwise, return `PromptStdin`.
pub fn resolve_auth_response(
    prompt: &AuthPromptMessage,
    env: &EnvCredentials,
    non_interactive: bool,
) -> AuthResolution {
    if let Some(value) = env.for_kind(&prompt.kind) {
        return AuthResolution::Answer(value.to_string());
    }
    if non_interactive {
        return AuthResolution::Fail(format!(
            "non-interactive mode: cannot prompt for {} (target {})",
            prompt.kind, prompt.target_label
        ));
    }
    AuthResolution::PromptStdin
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_prompt(kind: &str) -> AuthPromptMessage {
        AuthPromptMessage {
            prompt_id: "test-id".to_string(),
            target_label: "test-target".to_string(),
            kind: kind.to_string(),
            secret: true,
            message: format!("Enter {}:", kind),
        }
    }

    #[test]
    fn env_var_hit_returns_answer() {
        let env = EnvCredentials {
            password: Some("secret123".to_string()),
            totp_secret: None,
        };
        let result = resolve_auth_response(&make_prompt("password"), &env, false);
        assert_eq!(result, AuthResolution::Answer("secret123".to_string()));
    }

    #[test]
    fn env_var_hit_in_non_interactive_still_returns_answer() {
        let env = EnvCredentials {
            password: Some("secret123".to_string()),
            totp_secret: None,
        };
        let result = resolve_auth_response(&make_prompt("password"), &env, true);
        assert_eq!(result, AuthResolution::Answer("secret123".to_string()));
    }

    #[test]
    fn totp_secret_matches_jump_mfa() {
        let env = EnvCredentials {
            password: None,
            totp_secret: Some("JBSWY3DPEHPK3PXP".to_string()),
        };
        let result = resolve_auth_response(&make_prompt("jump_mfa"), &env, false);
        assert_eq!(
            result,
            AuthResolution::Answer("JBSWY3DPEHPK3PXP".to_string())
        );
    }

    #[test]
    fn non_interactive_no_env_returns_fail() {
        let env = EnvCredentials::default();
        let result = resolve_auth_response(&make_prompt("password"), &env, true);
        match result {
            AuthResolution::Fail(msg) => {
                assert!(msg.contains("non-interactive"));
                assert!(msg.contains("password"));
                assert!(msg.contains("test-target"));
            }
            other => panic!("expected Fail, got: {:?}", other),
        }
    }

    #[test]
    fn interactive_no_env_returns_prompt_stdin() {
        let env = EnvCredentials::default();
        let result = resolve_auth_response(&make_prompt("password"), &env, false);
        assert_eq!(result, AuthResolution::PromptStdin);
    }

    #[test]
    fn unknown_kind_no_env_non_interactive_returns_fail() {
        let env = EnvCredentials {
            password: Some("pw".to_string()),
            totp_secret: Some("totp".to_string()),
        };
        let result = resolve_auth_response(&make_prompt("host_key_trust"), &env, true);
        match result {
            AuthResolution::Fail(msg) => {
                assert!(msg.contains("host_key_trust"));
            }
            other => panic!("expected Fail, got: {:?}", other),
        }
    }

    #[test]
    fn unknown_kind_interactive_returns_prompt_stdin() {
        let env = EnvCredentials {
            password: Some("pw".to_string()),
            totp_secret: Some("totp".to_string()),
        };
        let result = resolve_auth_response(&make_prompt("host_key_trust"), &env, false);
        assert_eq!(result, AuthResolution::PromptStdin);
    }

    #[test]
    fn from_env_with_no_vars_returns_defaults() {
        // This test just verifies the struct can be constructed with defaults
        let env = EnvCredentials::default();
        assert!(env.password.is_none());
        assert!(env.totp_secret.is_none());
    }
}
