//! Integration test verifying the in-process gRPC harness works.

mod support;

use support::in_process_rpc::InProcessRpcHarness;

#[tokio::test]
async fn harness_list_servers_returns_stub_entries() {
    let mut harness = InProcessRpcHarness::new().await;
    let servers = harness.list_servers().await;

    // The harness registers the `_self` localhost gateway (used by the copy
    // e2e tests), which contributes one extra row beyond the stub entry.
    let stub = servers
        .iter()
        .find(|s| s.alias == "stub-target")
        .expect("stub-target entry present");
    assert_eq!(stub.host, "127.0.0.1");
    assert_eq!(stub.port, 22);
    assert_eq!(stub.user, "testuser");
    assert_eq!(stub.auth_kind, "key");
}

#[tokio::test]
async fn harness_status_reports_daemon_running() {
    let mut harness = InProcessRpcHarness::new().await;
    let status = harness.status().await;

    assert!(status.daemon_running);
}

#[tokio::test]
async fn harness_execute_returns_error_for_nonexistent_target() {
    let mut harness = InProcessRpcHarness::new().await;
    let events = harness.execute("nonexistent", &["echo", "hello"]).await;

    // The daemon should return an error event since the target can't be resolved
    assert!(!events.is_empty());
    // Check that at least one event is an error
    let has_error = events.iter().any(|e| {
        matches!(
            &e.event,
            Some(xho::protocol::rpc::execute_response::Event::Error(_))
        )
    });
    assert!(has_error, "expected an error event for nonexistent target");
}
