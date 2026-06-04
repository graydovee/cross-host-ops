//! Integration test verifying the in-process gRPC harness works.

mod support;

use support::in_process_rpc::InProcessRpcHarness;

#[tokio::test]
async fn harness_list_servers_returns_stub_entries() {
    let mut harness = InProcessRpcHarness::new().await;
    let servers = harness.list_servers().await;

    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].alias, "stub-target");
    assert_eq!(servers[0].host, "127.0.0.1");
    assert_eq!(servers[0].port, 22);
    assert_eq!(servers[0].user, "testuser");
    assert_eq!(servers[0].auth_kind, "key");
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
