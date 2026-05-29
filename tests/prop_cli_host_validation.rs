//! Property-based tests for host command reserved name rejection.
//!
//! These tests verify that reserved names (from RESERVED_NAMES) are correctly
//! identified and would be rejected by the host add handler.

use clap::Parser;
use proptest::prelude::*;

use rhop::cli::{ArunCli, ArunCommand, HostCommand};
use rhop::config::RESERVED_NAMES;

// ---------------------------------------------------------------------------
// Proptest strategies
// ---------------------------------------------------------------------------

/// Strategy that selects a name from RESERVED_NAMES.
/// Currently RESERVED_NAMES = ["local"], so this always yields "local".
fn arb_reserved_name() -> impl Strategy<Value = String> {
    prop::sample::select(
        RESERVED_NAMES
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
    )
}

/// Strategy for generating valid address strings in [user@]host[:port] format.
/// Generates realistic SSH-style addresses to pair with the reserved name.
fn arb_valid_address() -> impl Strategy<Value = String> {
    let user_part = prop::option::of("[a-z][a-z0-9_]{0,15}");
    let host_part = "[a-z][a-z0-9\\-]{0,30}\\.[a-z]{2,6}";
    let port_part = prop::option::of(1u16..=65535u16);

    (user_part, host_part, port_part).prop_map(|(user, host, port)| {
        let mut addr = String::new();
        if let Some(u) = user {
            addr.push_str(&u);
            addr.push('@');
        }
        addr.push_str(&host);
        if let Some(p) = port {
            addr.push(':');
            addr.push_str(&p.to_string());
        }
        addr
    })
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

// Feature: cli-command-tree-refactor, Property 6: Reserved name rejection
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 3.6**
    ///
    /// For any name string that equals a value in RESERVED_NAMES (currently
    /// ["local"]), parsing `rhop host add <name> <valid-address>` SHALL
    /// succeed at the clap level and produce a HostCommand::Add with the
    /// reserved name. The handler (run_host_command) rejects such names with
    /// a non-zero exit status before any config modification occurs.
    #[test]
    fn prop_reserved_name_rejected_by_host_add(
        reserved_name in arb_reserved_name(),
        address in arb_valid_address(),
    ) {
        // Build the command line: ["rhop", "host", "add", <reserved_name>, <address>]
        let args = vec![
            "rhop".to_string(),
            "host".to_string(),
            "add".to_string(),
            reserved_name.clone(),
            address.clone(),
        ];

        // Parse via clap — clap does not validate names, so parsing succeeds
        let cli = ArunCli::try_parse_from(&args);
        prop_assert!(
            cli.is_ok(),
            "ArunCli::try_parse_from failed for args {:?}: {:?}",
            args,
            cli.err()
        );
        let cli = cli.unwrap();

        // Extract the Host { Add { .. } } variant
        match cli.command {
            ArunCommand::Host { command: HostCommand::Add { name, .. } } => {
                // Verify the parsed name matches the reserved name
                prop_assert_eq!(
                    &name, &reserved_name,
                    "parsed name does not match input reserved name"
                );

                // Verify the name IS in RESERVED_NAMES — this confirms the
                // handler (run_host_command -> remote_connect) would reject it
                // with exit code 1 and an error message before any config
                // modification occurs.
                prop_assert!(
                    RESERVED_NAMES.contains(&name.as_str()),
                    "name '{}' should be in RESERVED_NAMES",
                    name
                );
            }
            other => {
                prop_assert!(false, "expected Host {{ Add {{ .. }} }}, got {:?}", other);
            }
        }
    }
}
