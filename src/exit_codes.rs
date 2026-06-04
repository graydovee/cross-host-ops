//! Exit-code taxonomy for the `xho` CLI.
//!
//! This module defines a deterministic mapping from error conditions to process
//! exit codes, ensuring callers can distinguish remote-command failure from
//! xho-internal failure without parsing prose.
//!
//! Exit-code bands:
//! - `0`       — success
//! - `1..=123` — remote command exit code (transparently forwarded, capped)
//! - `124`     — `--timeout` deadline expired
//! - `125`     — xho/daemon internal error (config, transport, resolver, missing operand)
//! - `126`     — cannot execute (auth failure, host-key rejection, review deny, non-interactive prompt)
//! - `127`     — target not found / unknown alias / unsupported capability

/// Remote command succeeded.
pub const EXIT_SUCCESS: i32 = 0;

/// General error (fallback for unclassified failures).
pub const EXIT_GENERAL_ERROR: i32 = 1;

/// Usage error (bad arguments, missing operand).
pub const EXIT_USAGE_ERROR: i32 = 2;

/// Operation aborted because `--timeout` deadline expired.
pub const EXIT_TIMEOUT: i32 = 124;

/// Xho/daemon internal error (config, transport, resolver, daemon unreachable).
pub const EXIT_INTERNAL: i32 = 125;

/// Cannot execute: authentication failure, host-key rejection, review deny,
/// or non-interactive mode blocked a required prompt.
pub const EXIT_CANNOT_EXECUTE: i32 = 126;

/// Target not found, unknown alias, or unsupported capability.
pub const EXIT_TARGET_NOT_FOUND: i32 = 127;

/// Cap a remote command's exit code so it never collides with xho's reserved
/// exit-code bands (124–127).
///
/// - If `c` is in `0..=123`, return `c` unchanged.
/// - If `c >= 124`, return `123` (the maximum transparent remote exit code).
/// - If `c < 0`, return `125` (treated as an internal/unexpected error).
pub fn cap_remote_exit_code(c: i32) -> i32 {
    if c < 0 {
        EXIT_INTERNAL
    } else if c >= 124 {
        123
    } else {
        c
    }
}

/// Categorized error kinds that map to specific exit codes.
///
/// Each variant represents a class of failure that the CLI can encounter.
/// The `exit_code()` method provides the deterministic mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XhoError {
    /// Operation timed out (`--timeout` deadline expired).
    Timeout,

    /// Internal/daemon error: config error, daemon unreachable, resolver failure,
    /// transport error, missing operand.
    Internal(String),

    /// Usage error: bad arguments or missing operand.
    UsageError(String),

    /// Authentication failure, host-key rejection, review deny, or
    /// non-interactive mode blocked a required prompt.
    CannotExecute(String),

    /// Target not found, unknown alias, or unsupported capability.
    TargetNotFound(String),

    /// General/unclassified error.
    General(String),
}

impl XhoError {
    /// Map this error to its documented exit code.
    pub fn exit_code(&self) -> i32 {
        match self {
            XhoError::Timeout => EXIT_TIMEOUT,
            XhoError::Internal(_) => EXIT_INTERNAL,
            XhoError::UsageError(_) => EXIT_USAGE_ERROR,
            XhoError::CannotExecute(_) => EXIT_CANNOT_EXECUTE,
            XhoError::TargetNotFound(_) => EXIT_TARGET_NOT_FOUND,
            XhoError::General(_) => EXIT_GENERAL_ERROR,
        }
    }

    /// Get the human-readable message for this error.
    pub fn message(&self) -> &str {
        match self {
            XhoError::Timeout => "operation timed out",
            XhoError::Internal(msg) => msg,
            XhoError::UsageError(msg) => msg,
            XhoError::CannotExecute(msg) => msg,
            XhoError::TargetNotFound(msg) => msg,
            XhoError::General(msg) => msg,
        }
    }
}

impl std::fmt::Display for XhoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for XhoError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_remote_exit_code_passthrough() {
        assert_eq!(cap_remote_exit_code(0), 0);
        assert_eq!(cap_remote_exit_code(1), 1);
        assert_eq!(cap_remote_exit_code(42), 42);
        assert_eq!(cap_remote_exit_code(123), 123);
    }

    #[test]
    fn test_cap_remote_exit_code_caps_high() {
        assert_eq!(cap_remote_exit_code(124), 123);
        assert_eq!(cap_remote_exit_code(125), 123);
        assert_eq!(cap_remote_exit_code(126), 123);
        assert_eq!(cap_remote_exit_code(127), 123);
        assert_eq!(cap_remote_exit_code(255), 123);
        assert_eq!(cap_remote_exit_code(1000), 123);
    }

    #[test]
    fn test_cap_remote_exit_code_negative() {
        assert_eq!(cap_remote_exit_code(-1), EXIT_INTERNAL);
        assert_eq!(cap_remote_exit_code(-128), EXIT_INTERNAL);
        assert_eq!(cap_remote_exit_code(i32::MIN), EXIT_INTERNAL);
    }

    #[test]
    fn test_xho_error_exit_codes() {
        assert_eq!(XhoError::Timeout.exit_code(), 124);
        assert_eq!(
            XhoError::Internal("daemon unreachable".into()).exit_code(),
            125
        );
        assert_eq!(
            XhoError::UsageError("missing operand".into()).exit_code(),
            2
        );
        assert_eq!(
            XhoError::CannotExecute("auth failed".into()).exit_code(),
            126
        );
        assert_eq!(
            XhoError::TargetNotFound("no such target".into()).exit_code(),
            127
        );
        assert_eq!(XhoError::General("something broke".into()).exit_code(), 1);
    }

    // Feature: xhod-jumpserver-architecture, Property 15: Exit-code semantics consistency
    mod prop_exit_codes {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

            /// For any i32 code, `cap_remote_exit_code(code)` is in [0, 123] ∪ {125}
            /// (never in the 124–127 range that xho reserves for its own semantics,
            /// except 125 which is used for negative/unexpected codes).
            ///
            /// **Validates: Requirements 17.10, 17.11, 17.12**
            #[test]
            fn prop_cap_remote_exit_code_never_in_reserved_band(code in proptest::num::i32::ANY) {
                let result = cap_remote_exit_code(code);
                // Result must be in [0, 123] or exactly 125 (for negative inputs)
                let valid = (result >= 0 && result <= 123) || result == EXIT_INTERNAL;
                prop_assert!(
                    valid,
                    "cap_remote_exit_code({}) = {} is outside [0,123] ∪ {{125}}",
                    code,
                    result
                );
                // Specifically must never be 124, 126, or 127
                prop_assert_ne!(result, EXIT_TIMEOUT, "must not return 124 (timeout)");
                prop_assert_ne!(result, EXIT_CANNOT_EXECUTE, "must not return 126 (cannot execute)");
                prop_assert_ne!(result, EXIT_TARGET_NOT_FOUND, "must not return 127 (target not found)");
            }
        }
    }
}
