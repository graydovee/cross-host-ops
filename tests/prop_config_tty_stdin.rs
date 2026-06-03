//! Property-based tests for SshConfig tty/stdin deserialization.
//!
//! Feature: exec-stdin-tty-refactor, Property 8: Config field tty/stdin deserialization
//! Validates: Requirements 5.2, 5.1

use proptest::prelude::*;

use rhop::config::SshConfig;

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

// Feature: exec-stdin-tty-refactor, Property 8: Config field tty/stdin deserialization
// Validates: Requirements 5.2, 5.1
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 5.2, 5.1**
    ///
    /// For any (tty: bool, stdin: bool): a TOML string containing
    /// `tty = <tty_val>\nstdin = <stdin_val>` SHALL deserialize to
    /// SshConfig { tty: tty_val, stdin: stdin_val, .. }.
    #[test]
    fn prop_config_tty_stdin_deserialization(
        tty_val in any::<bool>(),
        stdin_val in any::<bool>(),
    ) {
        let toml_str = format!("tty = {}\nstdin = {}", tty_val, stdin_val);
        let config: SshConfig = toml::from_str(&toml_str).unwrap();

        prop_assert_eq!(
            config.tty, tty_val,
            "SshConfig.tty should be {} after deserializing '{}'",
            tty_val, toml_str,
        );
        prop_assert_eq!(
            config.stdin, stdin_val,
            "SshConfig.stdin should be {} after deserializing '{}'",
            stdin_val, toml_str,
        );
    }
}
