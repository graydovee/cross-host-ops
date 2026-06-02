//! Integration tests: end-to-end connection path for rhopd gateway.
//!
//! NOTE: This test originally tested the `RhopdGateway` struct from the
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

    assert_eq!(servers.len(), 1, "stub returns 1 server entry");
    assert_eq!(servers[0].alias, "stub-target");
    assert_eq!(servers[0].host, "127.0.0.1");
    assert_eq!(servers[0].port, 22);
    assert_eq!(servers[0].user, "testuser");
}

/// Validates that execute returns an error for a non-existent target
/// (proves the gateway-based daemon resolves targets correctly).
#[tokio::test]
async fn connect_exec_nonexistent_target_errors() {
    let mut harness = InProcessRpcHarness::new().await;
    let events = harness.execute("nonexistent-host", &["echo", "hello"]).await;

    // Should get an error event (target not found)
    let has_error = events.iter().any(|e| {
        if let Some(rhop::protocol::rpc::execute_response::Event::Error(err)) = &e.event {
            !err.message.is_empty()
        } else {
            false
        }
    });
    assert!(has_error, "should get error for nonexistent target");
}
