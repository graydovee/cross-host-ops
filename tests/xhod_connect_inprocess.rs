//! Integration tests: end-to-end connection path for xhod gateway.
//!
//! NOTE: This test originally tested the `XhodGateway` struct from the
//! deleted `src/jump/` module. That module was removed as part of the
//! config-and-legacy-cleanup spec. The equivalent end-to-end connection
//! behavior is now tested via the in-process RPC harness
//! (in_process_rpc_test.rs) which exercises the full gateway-based daemon.
//!
//! The in-process harness validates:
//! - list_servers works through gRPC
//! - exec works through gRPC
//! - status reports correct info

mod support;

use support::in_process_rpc::InProcessRpcHarness;

/// Validates that list_servers returns entries from the stub server config.
#[tokio::test]
async fn connect_end_to_end_list_servers() {
    let mut harness = InProcessRpcHarness::new().await;
    let servers = harness.list_servers().await;

    // The `_self` localhost gateway (copy e2e support) adds one extra row
    // beyond the stub entry.
    let stub = servers
        .iter()
        .find(|s| s.alias == "stub-target")
        .expect("stub entry present");
    assert_eq!(stub.host, "127.0.0.1");
    assert_eq!(stub.port, 22);
    assert_eq!(stub.user, "testuser");
}

/// Validates that execute returns an error for a non-existent target
/// (proves the gateway-based daemon resolves targets correctly).
#[tokio::test]
async fn connect_exec_nonexistent_target_errors() {
    let mut harness = InProcessRpcHarness::new().await;
    let events = harness
        .execute("nonexistent-host", &["echo", "hello"])
        .await;

    // Should get an error event (target not found)
    let has_error = events.iter().any(|e| {
        if let Some(xho::protocol::rpc::execute_response::Event::Error(err)) = &e.event {
            !err.message.is_empty()
        } else {
            false
        }
    });
    assert!(has_error, "should get error for nonexistent target");
}
