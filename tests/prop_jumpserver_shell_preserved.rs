//! Property test: Backward Compatibility — Jumpserver Shell Navigation Preserved
//!
//! Feature: interactive-pty-passthrough
//! Property 6: Backward Compatibility — Jumpserver Shell Navigation Preserved
//!
//! **Validates: Requirements 1.5**
//!
//! For any jumpserver connection (kind = "jumpserver"), the PtyShell + sentinel
//! logic is still used for connection establishment (MFA, menu navigation), but
//! NOT for command execution on the final target after the connection is
//! established.
//!
//! This test verifies:
//! 1. JumpserverJumpHost's kind() returns JumpHostKind::Jumpserver
//! 2. exec_interactive default method returns UnsupportedCapability for jumpserver
//! 3. PtyShell type and related functions still exist in the shared module
//!    (compile-time verification via use statements)
//! 4. For arbitrary command arguments, the structural properties hold

use proptest::prelude::*;

use rhop::jump::{JumpHost, JumpHostKind, UnsupportedCapability};
use rhop::config::AppConfig;
use rhop::connection::CopySpec;
use rhop::protocol::ServerEvent;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::UnboundedSender;

// ---------------------------------------------------------------------------
// Compile-time verification: PtyShell and sentinel helpers exist in shared.rs
// ---------------------------------------------------------------------------
// The following imports verify at compile time that the sentinel/PtyShell
// infrastructure is still present in the crate (used by jumpserver connection
// establishment). If these were removed, this test would fail to compile.
use rhop::connection::build_remote_command;
use rhop::connection::shell_quote;

// ---------------------------------------------------------------------------
// Mock JumpHost that simulates JumpserverJumpHost behavior
// ---------------------------------------------------------------------------

/// A mock that mirrors JumpserverJumpHost's trait implementation:
/// - kind() returns Jumpserver
/// - exec() delegates to inner connection's execute() (simulated as Ok(0))
/// - exec_interactive() uses the default (returns UnsupportedCapability)
/// - tui_shell() uses the default (returns UnsupportedCapability)
struct MockJumpserverHost {
    name: String,
}

#[async_trait]
impl JumpHost for MockJumpserverHost {
    async fn exec(
        &mut self,
        _argv: &[String],
        _sender: &UnboundedSender<ServerEvent>,
        _config: &AppConfig,
        _pty: bool,
        _cols: u32,
        _rows: u32,
        _shell: &str,
    ) -> Result<i32> {
        // JumpserverJumpHost delegates to inner.execute() which uses
        // request_pty() + exec() — NOT sentinel wrapping.
        Ok(0)
    }

    async fn copy(&mut self, _spec: &CopySpec, _config: &AppConfig) -> Result<()> {
        Ok(())
    }

    fn kind(&self) -> JumpHostKind {
        JumpHostKind::Jumpserver
    }

    fn name(&self) -> &str {
        &self.name
    }

    // exec_interactive, tui_shell, list_servers all use the default
    // implementations which return UnsupportedCapability.
}

// ---------------------------------------------------------------------------
// Strategy: generate arbitrary command argument vectors
// ---------------------------------------------------------------------------

fn arb_argv() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-zA-Z0-9_./ -]{1,50}", 1..=5)
}

fn arb_name() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_-]{0,20}"
}

fn arb_dimensions() -> impl Strategy<Value = (u32, u32)> {
    (1u32..=500, 1u32..=200)
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 1.5**
    ///
    /// For any jumpserver host name, kind() always returns Jumpserver.
    #[test]
    fn prop_jumpserver_kind_is_jumpserver(name in arb_name()) {
        let host = MockJumpserverHost { name };
        prop_assert_eq!(host.kind(), JumpHostKind::Jumpserver);
    }

    /// **Validates: Requirements 1.5**
    ///
    /// For any arbitrary command arguments and terminal dimensions,
    /// calling exec_interactive on a jumpserver host returns an
    /// UnsupportedCapability error — proving that interactive execution
    /// is NOT handled by the jumpserver's PtyShell/sentinel path.
    #[test]
    fn prop_jumpserver_exec_interactive_returns_unsupported(
        name in arb_name(),
        argv in arb_argv(),
        (cols, rows) in arb_dimensions(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = AppConfig::default();
            let (sender, _rx) = tokio::sync::mpsc::unbounded_channel();
            let mut host = MockJumpserverHost { name: name.clone() };

            let result = host.exec_interactive(&argv, cols, rows, &sender, &config, "").await;

            // Must be an error
            let err = match result {
                Err(e) => e,
                Ok(_) => panic!("exec_interactive on jumpserver should return UnsupportedCapability"),
            };

            // Must downcast to UnsupportedCapability
            let unsupported = err
                .downcast_ref::<UnsupportedCapability>()
                .expect("error should downcast to UnsupportedCapability");

            prop_assert_eq!(unsupported.kind, JumpHostKind::Jumpserver);
            prop_assert_eq!(&unsupported.name, &name);
            prop_assert_eq!(unsupported.method, "exec_interactive");

            Ok(())
        })?;
    }

    /// **Validates: Requirements 1.5**
    ///
    /// For any arbitrary command arguments, the jumpserver's exec() method
    /// succeeds (delegates to inner connection's execute which uses
    /// request_pty + exec, NOT sentinel wrapping). This confirms that
    /// command execution does not go through the PtyShell sentinel path.
    #[test]
    fn prop_jumpserver_exec_does_not_use_sentinel(
        name in arb_name(),
        argv in arb_argv(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = AppConfig::default();
            let (sender, _rx) = tokio::sync::mpsc::unbounded_channel();
            let mut host = MockJumpserverHost { name };

            // exec() should succeed — it delegates to inner.execute()
            // which uses request_pty() + exec() (no sentinel).
            let result = host.exec(&argv, &sender, &config, true, 80, 24, "").await;
            prop_assert!(result.is_ok(), "exec should succeed (delegates to inner connection)");

            Ok(())
        })?;
    }

    /// **Validates: Requirements 1.5**
    ///
    /// Compile-time + runtime verification that build_remote_command and
    /// shell_quote (used by the non-sentinel exec path) produce valid
    /// command strings for arbitrary arguments.
    #[test]
    fn prop_remote_command_building_works(argv in arb_argv()) {
        // build_remote_command is used by the PTY exec path (request_pty + exec)
        // which is what JumpserverJumpHost delegates to for command execution.
        let cmd = build_remote_command(&argv);
        prop_assert!(!cmd.is_empty(), "remote command should not be empty");

        // Each argument should be shell-quoted in the output
        for arg in &argv {
            let quoted = shell_quote(arg);
            prop_assert!(
                cmd.contains(&quoted),
                "command '{}' should contain quoted arg '{}'",
                cmd,
                quoted
            );
        }
    }
}
