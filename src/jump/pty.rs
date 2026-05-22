//! Effective PTY decision logic.
//!
//! Centralizes the PTY allocation decision as a pure function of its inputs,
//! making it testable and deterministic (Property 17).

use crate::config::SshConfig;

/// Flags derived from the CLI's `--pty` / `--no-pty` arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecPtyFlags {
    /// `--pty` was passed.
    pub force_pty: bool,
    /// `--no-pty` was passed.
    pub force_no_pty: bool,
}

/// Compute the effective PTY decision.
///
/// Priority (each step short-circuits):
/// 1. `--no-pty` → false
/// 2. `--pty` → true
/// 3. `auto_pty_detect && !stdout_is_tty` → false
/// 4. Otherwise → `ssh.pty`
///
/// Note: `(force_pty=true, force_no_pty=true)` is unreachable because clap's
/// `conflicts_with` rejects it at parse time. If somehow both are true,
/// `force_no_pty` wins (it is checked first).
pub fn effective_pty_decision(
    flags: &ExecPtyFlags,
    ssh_config: &SshConfig,
    stdout_is_tty: bool,
) -> bool {
    if flags.force_no_pty {
        return false;
    }
    if flags.force_pty {
        return true;
    }
    if ssh_config.auto_pty_detect && !stdout_is_tty {
        return false;
    }
    ssh_config.pty
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn no_pty_flag_overrides_everything() {
        let flags = ExecPtyFlags {
            force_pty: false,
            force_no_pty: true,
        };
        let mut config = SshConfig::default();
        config.pty = true;
        config.auto_pty_detect = true;
        assert!(!effective_pty_decision(&flags, &config, true));
        assert!(!effective_pty_decision(&flags, &config, false));
    }

    #[test]
    fn pty_flag_overrides_auto_detect() {
        let flags = ExecPtyFlags {
            force_pty: true,
            force_no_pty: false,
        };
        let mut config = SshConfig::default();
        config.pty = false;
        config.auto_pty_detect = true;
        // Even when stdout is not a TTY, --pty wins
        assert!(effective_pty_decision(&flags, &config, false));
    }

    #[test]
    fn auto_detect_suppresses_pty_when_not_tty() {
        let flags = ExecPtyFlags::default();
        let mut config = SshConfig::default();
        config.pty = true;
        config.auto_pty_detect = true;
        // stdout is not a TTY → no PTY
        assert!(!effective_pty_decision(&flags, &config, false));
    }

    #[test]
    fn auto_detect_disabled_falls_through_to_ssh_pty() {
        let flags = ExecPtyFlags::default();
        let mut config = SshConfig::default();
        config.pty = true;
        config.auto_pty_detect = false;
        // Even though stdout is not a TTY, auto_pty_detect is off → use ssh.pty
        assert!(effective_pty_decision(&flags, &config, false));
    }

    #[test]
    fn fallback_to_ssh_pty_false() {
        let flags = ExecPtyFlags::default();
        let mut config = SshConfig::default();
        config.pty = false;
        config.auto_pty_detect = false;
        assert!(!effective_pty_decision(&flags, &config, true));
    }

    // Feature: rhopd-jumpserver-architecture, Property 17: PTY decision determinism
    proptest! {
        #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

        /// **Validates: Requirements 17.19, 17.20, 17.21, 17.22, 17.23**
        ///
        /// For any combination of (force_no_pty, force_pty, auto_pty_detect, ssh_pty,
        /// stdout_is_tty) where force_no_pty and force_pty are not both true (clap
        /// rejects that), `effective_pty_decision` is total, returns the value
        /// dictated by the priority rule, and equals itself across two calls with
        /// the same inputs.
        #[test]
        fn prop_pty_decision_determinism(
            force_no_pty in any::<bool>(),
            force_pty in any::<bool>(),
            auto_pty_detect in any::<bool>(),
            ssh_pty in any::<bool>(),
            stdout_is_tty in any::<bool>(),
        ) {
            // Skip the unreachable combination (clap rejects it)
            prop_assume!(!(force_no_pty && force_pty));

            let flags = ExecPtyFlags { force_pty, force_no_pty };
            let mut config = SshConfig::default();
            config.pty = ssh_pty;
            config.auto_pty_detect = auto_pty_detect;

            let result1 = effective_pty_decision(&flags, &config, stdout_is_tty);
            let result2 = effective_pty_decision(&flags, &config, stdout_is_tty);

            // Determinism: same inputs → same output
            prop_assert_eq!(result1, result2);

            // Verify priority rule
            let expected = if force_no_pty {
                false
            } else if force_pty {
                true
            } else if auto_pty_detect && !stdout_is_tty {
                false
            } else {
                ssh_pty
            };

            prop_assert_eq!(
                result1, expected,
                "effective_pty_decision({:?}, auto_pty_detect={}, pty={}, stdout_is_tty={}) = {}, expected {}",
                flags, auto_pty_detect, ssh_pty, stdout_is_tty, result1, expected
            );
        }
    }
}
