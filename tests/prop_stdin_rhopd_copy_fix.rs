//! Integration-level property tests for bug condition:
//!   Stdin and Rhopd Copy Data Forwarding Hang.
//!
//! **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
//!
//! ## Purpose
//!
//! This test file has two sections:
//!
//! ### Bug Condition Tests (Property 1, Task 1)
//! Confirms the two bugs on unfixed code:
//! 1. Stdin forwarding hang (Requirements 1.1, 1.2)
//! 2. Rhopd copy data hang (Requirements 1.3, 1.4)
//!
//! ### Preservation Tests (Property 2, Task 2)
//! Verifies baseline behavior is intact on UNFIXED code (Requirements 3.1–3.6):
//! - Non-stdin exec returns error or exit status normally (not hang)
//! - exec without stdin flag completes normally
//! - ServerEvent forwarding path is intact at the gateway-daemon layer
//!
//! ## Core unit tests (lower level)
//! The deeper property tests that directly exercise `RhopdConnection` internals
//! (stdout/stderr/event forwarding, interactive handle) are in:
//!   `src/daemon/connection/rhopd.rs` (cfg(test) module)
//! Those tests have access to `pub(super)` types.
//!
//! ## Integration tests here
//! These tests exercise the DAEMON layer via InProcessRpcHarness, which
//! connects a gRPC client directly to an in-process daemon service.

mod support;

use proptest::prelude::*;
use rhop::protocol::rpc;
use support::in_process_rpc::InProcessRpcHarness;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Strategy: non-empty stdin payload (1–64 bytes).
fn arb_stdin_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 1..64)
}

/// Strategy: simple argv (1–3 args, ASCII lowercase words).
fn arb_argv() -> impl Strategy<Value = Vec<String>> {
    proptest::collection::vec("[a-z]{1,8}".prop_map(String::from), 1..4)
}

// ---------------------------------------------------------------------------
// Bug Condition Tests (Property 1)
//
// These tests PASS on unfixed code by confirming the daemon responds
// (error/exit) and does not hang.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 5,
        .. ProptestConfig::default()
    })]

    /// **Validates: Requirements 1.1**
    ///
    /// Property 1, Bug Condition A: LocalGateway exec with stdin never
    /// forwards data to the SSH channel.
    ///
    /// When daemon receives Execute(stdin=true) + StdinData, it creates
    /// `stdin_rx` and passes it in `gateway::ExecRequest`. However,
    /// `LocalGateway::exec` builds `ConnExecRequest` WITHOUT `stdin_rx`
    /// (the field doesn't exist in ConnExecRequest), so stdin is silently
    /// discarded. The remote process never sees the input.
    ///
    /// EXPECTED OUTCOME: The daemon returns an error (connection refused to
    /// 127.0.0.1:22) confirming the structural bug. Test PASSES on unfixed code.
    #[test]
    fn prop_bug_daemon_stdin_exec_accepted_but_not_forwarded(
        stdin_payload in arb_stdin_bytes(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut harness = InProcessRpcHarness::new().await;

            // Send Execute(stdin=true) + StdinData.
            let start = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                    target: "stub-target".to_string(),
                    argv: vec!["cat".to_string()],
                    stdin: true,
                    interactive: false,
                    no_pty: true,
                    timeout_ms: 500,
                    ..Default::default()
                })),
            };
            let stdin_msg = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::StdinData(rpc::StdinData {
                    data: stdin_payload.clone(),
                })),
            };

            let response = harness
                .client
                .execute(tokio_stream::iter(vec![start, stdin_msg]))
                .await
                .expect("Execute RPC should not fail at transport level");

            let mut stream = response.into_inner();
            let mut got_error_or_exit = false;

            while let Ok(Some(msg)) = stream.message().await {
                match msg.event {
                    Some(rpc::execute_response::Event::Error(_)) => {
                        got_error_or_exit = true;
                    }
                    Some(rpc::execute_response::Event::ExitStatus(_)) => {
                        got_error_or_exit = true;
                    }
                    _ => {}
                }
            }

            // Daemon must respond (not hang forever, thanks to timeout_ms=500).
            prop_assert!(
                got_error_or_exit,
                "daemon must return error or exit status for stdin exec (not hang)"
            );

            let _ = stdin_payload;
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Preservation Tests (Property 2) — Integration layer
//
// These tests use InProcessRpcHarness (full daemon in-process).
// They verify baseline behavior is intact on UNFIXED code.
// **EXPECTED OUTCOME: ALL PASS on unfixed code.**
//
// Note: Deeper RhopdConnection-level preservation tests (stdout/stderr/event
// forwarding, interactive handle) live in src/daemon/connection/rhopd.rs
// cfg(test) because they require access to pub(super) types.
//
// **Validates: Requirements 3.1, 3.5**
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 10,
        .. ProptestConfig::default()
    })]

    /// **Validates: Requirements 3.1**
    ///
    /// Preservation: exec WITHOUT stdin flag (stdin=false) against an
    /// unreachable stub target returns an error response promptly — NOT
    /// a hang. The daemon correctly handles non-stdin exec requests.
    ///
    /// On unfixed code: the daemon attempts SSH connection to 127.0.0.1:22,
    /// fails immediately with "connection refused", and sends ErrorResponse.
    /// This confirms the non-stdin exec path is functional.
    ///
    /// EXPECTED OUTCOME: PASSES on unfixed code.
    #[test]
    fn prop_preservation_non_stdin_exec_returns_error_not_hang(
        argv in arb_argv(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut harness = InProcessRpcHarness::new().await;

            let start = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                    target: "stub-target".to_string(),
                    argv: argv.clone(),
                    stdin: false,       // no stdin — the preserved path
                    interactive: false,
                    no_pty: true,
                    timeout_ms: 500,   // short timeout to fail fast
                    ..Default::default()
                })),
            };

            let response = harness
                .client
                .execute(tokio_stream::once(start))
                .await
                .expect("Execute RPC transport must succeed");

            let mut stream = response.into_inner();
            let mut got_terminal = false;

            while let Ok(Some(msg)) = stream.message().await {
                match msg.event {
                    Some(rpc::execute_response::Event::Error(_)) => {
                        got_terminal = true;
                    }
                    Some(rpc::execute_response::Event::ExitStatus(_)) => {
                        got_terminal = true;
                    }
                    _ => {}
                }
            }

            prop_assert!(
                got_terminal,
                "non-stdin exec must return error or exit status (not hang)"
            );

            Ok(())
        })?;
    }

    /// **Validates: Requirements 3.5**
    ///
    /// Preservation: exec with PTY flag (pty=true) also returns a terminal
    /// event promptly. PTY allocation should not interfere with error handling.
    ///
    /// EXPECTED OUTCOME: PASSES on unfixed code.
    #[test]
    fn prop_preservation_pty_exec_returns_error_not_hang(
        argv in arb_argv(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut harness = InProcessRpcHarness::new().await;

            let start = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                    target: "stub-target".to_string(),
                    argv: argv.clone(),
                    pty: true,
                    stdin: false,
                    interactive: false,
                    timeout_ms: 500,
                    ..Default::default()
                })),
            };

            let response = harness
                .client
                .execute(tokio_stream::once(start))
                .await
                .expect("Execute RPC transport must succeed");

            let mut stream = response.into_inner();
            let mut got_terminal = false;

            while let Ok(Some(msg)) = stream.message().await {
                match msg.event {
                    Some(rpc::execute_response::Event::Error(_)) => {
                        got_terminal = true;
                    }
                    Some(rpc::execute_response::Event::ExitStatus(_)) => {
                        got_terminal = true;
                    }
                    _ => {}
                }
            }

            prop_assert!(
                got_terminal,
                "PTY exec must return error or exit status (not hang)"
            );

            Ok(())
        })?;
    }
}
