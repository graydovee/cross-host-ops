//! Preservation property tests for exec PTY/stdin/timeout passthrough bugfix.
//!
//! Feature: exec-pty-passthrough-fix
//! Property 2: Preservation - Default Behavior Without Flags
//!
//! These tests capture the CURRENT baseline behavior that must be preserved
//! after the fix is implemented. They run on UNFIXED code and must PASS.
//!
//! Observations:
//! - `StartRequest { target, argv }` (only target+argv) executes correctly
//! - `CopyStartRequest { target, local_path, remote_path, recursive, direction }`
//!   (no timeout) copies correctly
//! - When no PTY flags are set, daemon uses `config.ssh.pty` for PTY decision
//! - Proto3 default values map to "no override" semantics
//!
//! **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**

mod support;

use proptest::prelude::*;

use rhop::config::SshConfig;
use rhop::types::{ExecPtyFlags, effective_pty_decision};
use rhop::protocol::rpc;

use support::in_process_rpc::InProcessRpcHarness;

// ---------------------------------------------------------------------------
// Property 2.1: Plain exec without flags works correctly
// ---------------------------------------------------------------------------

/// **Validates: Requirements 3.1, 3.5**
///
/// For any valid (target, argv) pair, sending a StartRequest with ONLY
/// target and argv fields (the current proto schema) results in the daemon
/// accepting the request and producing a response stream. The daemon does
/// not crash, hang, or reject the request.
///
/// On current unfixed code, this is the normal execution path and must work.
/// After the fix, proto3 defaults (false/0 for new fields) must produce
/// identical behavior.
#[tokio::test]
async fn preservation_plain_exec_accepted() {
    let mut harness = InProcessRpcHarness::new().await;

    // Send a plain StartRequest with only target + argv (current schema)
    let start_request = rpc::ExecuteRequest {
        request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
            target: "stub-target".to_string(),
            argv: vec!["echo".to_string(), "hello".to_string()],
            ..Default::default()
        })),
    };

    let response = harness
        .client
        .execute(tokio_stream::once(start_request))
        .await
        .expect("Execute RPC should succeed for plain StartRequest");

    let mut stream = response.into_inner();
    let mut got_response = false;

    while let Some(msg) = stream
        .message()
        .await
        .expect("failed to read Execute response stream")
    {
        got_response = true;
        // We expect the daemon to process the request (may get an error
        // because stub-target can't actually SSH, but the RPC layer works)
        if let Some(event) = msg.event {
            match event {
                rpc::execute_response::Event::ExitStatus(_) => break,
                rpc::execute_response::Event::Error(_) => break,
                _ => {}
            }
        }
    }

    assert!(
        got_response,
        "daemon should produce at least one response event for plain exec"
    );
}

// ---------------------------------------------------------------------------
// Property 2.2: Plain copy without timeout works correctly
// ---------------------------------------------------------------------------

/// **Validates: Requirements 3.3, 3.6**
///
/// For a valid CopyStartRequest with only the current fields (target,
/// local_path, remote_path, recursive, direction — no timeout_ms), the
/// daemon accepts the request and produces a response stream.
///
/// On current unfixed code, this is the normal copy path and must work.
/// After the fix, proto3 default (timeout_ms=0) must produce identical
/// behavior (no timeout enforcement).
#[tokio::test]
async fn preservation_plain_copy_accepted() {
    let mut harness = InProcessRpcHarness::new().await;

    // Send a plain CopyStartRequest with only current fields
    let start_request = rpc::CopyRequest {
        request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
            target: "stub-target".to_string(),
            local_path: "/tmp/local_file".to_string(),
            remote_path: "/tmp/remote_file".to_string(),
            recursive: false,
            direction: rpc::CopyDirection::Upload as i32,
            ..Default::default()
        })),
    };

    let response = harness
        .client
        .copy(tokio_stream::once(start_request))
        .await
        .expect("Copy RPC should succeed for plain CopyStartRequest");

    let mut stream = response.into_inner();
    let mut got_response = false;

    while let Some(msg) = stream
        .message()
        .await
        .expect("failed to read Copy response stream")
    {
        got_response = true;
        if let Some(event) = msg.event {
            match event {
                rpc::copy_response::Event::Complete(_) => break,
                rpc::copy_response::Event::Error(_) => break,
                _ => {}
            }
        }
    }

    assert!(
        got_response,
        "daemon should produce at least one response event for plain copy"
    );
}

// ---------------------------------------------------------------------------
// Property 2.3: PTY decision falls back to config.ssh.pty when no flags set
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.5**
//
// When no PTY flags are set (force_pty=false, force_no_pty=false), the
// effective_pty_decision function falls back to config.ssh.pty. This is
// the current daemon behavior for all exec requests (since the proto has
// no PTY fields). After the fix, proto3 defaults (pty=false, no_pty=false)
// must produce the same fallback behavior.
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 3.5**
    ///
    /// For any SshConfig with arbitrary pty and auto_pty_detect values,
    /// when no PTY flags are set (the "no override" case that proto3 defaults
    /// produce), effective_pty_decision returns the config-driven result.
    /// This captures the current daemon behavior that must be preserved.
    #[test]
    fn prop_no_flags_falls_back_to_config(
        ssh_pty in any::<bool>(),
        auto_pty_detect in any::<bool>(),
        stdout_is_tty in any::<bool>(),
    ) {
        // No flags set — this is what proto3 defaults (false/false) produce
        let flags = ExecPtyFlags {
            force_pty: false,
            force_no_pty: false,
        };
        let config = SshConfig {
            pty: ssh_pty,
            auto_pty_detect,
            ..Default::default()
        };

        let result = effective_pty_decision(&flags, &config, stdout_is_tty);

        // Expected behavior: auto_pty_detect + !stdout_is_tty → false,
        // otherwise → ssh_pty
        let expected = if auto_pty_detect && !stdout_is_tty {
            false
        } else {
            ssh_pty
        };

        prop_assert_eq!(
            result, expected,
            "no-flags PTY decision should match config fallback: \
             ssh_pty={}, auto_pty_detect={}, stdout_is_tty={} → expected {}, got {}",
            ssh_pty, auto_pty_detect, stdout_is_tty, expected, result
        );
    }
}

// ---------------------------------------------------------------------------
// Property 2.4: StartRequest wire identity (current fields only)
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.1, 3.5**
//
// For any (target, argv) pair, constructing a StartRequest and reading
// back its fields produces the same values. This verifies the current
// proto schema's round-trip identity for the existing fields.
//
// After the fix adds new fields, this test ensures the existing fields
// are still correctly preserved.
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    #[test]
    fn prop_start_request_current_fields_identity(
        target in "[a-zA-Z][a-zA-Z0-9_\\-]{0,30}",
        argv in prop::collection::vec("[a-zA-Z0-9_/\\-\\.]{1,30}", 0..10),
    ) {
        let req = rpc::StartRequest {
            target: target.clone(),
            argv: argv.clone(),
            ..Default::default()
        };

        // Verify fields are preserved
        prop_assert_eq!(&req.target, &target);
        prop_assert_eq!(&req.argv, &argv);
    }
}

// ---------------------------------------------------------------------------
// Property 2.5: CopyStartRequest wire identity (current fields only)
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.3, 3.6**
//
// For any valid CopyStartRequest field combination, constructing the
// message and reading back its fields produces the same values.
//
// After the fix adds timeout_ms, this test ensures existing fields
// are still correctly preserved.
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    #[test]
    fn prop_copy_start_request_current_fields_identity(
        target in "[a-zA-Z][a-zA-Z0-9_\\-]{0,30}",
        local_path in "/[a-zA-Z0-9_/]{1,30}",
        remote_path in "/[a-zA-Z0-9_/]{1,30}",
        recursive in any::<bool>(),
        direction in prop_oneof![
            Just(rpc::CopyDirection::Upload as i32),
            Just(rpc::CopyDirection::Download as i32),
        ],
    ) {
        let req = rpc::CopyStartRequest {
            target: target.clone(),
            local_path: local_path.clone(),
            remote_path: remote_path.clone(),
            recursive,
            direction,
            ..Default::default()
        };

        prop_assert_eq!(&req.target, &target);
        prop_assert_eq!(&req.local_path, &local_path);
        prop_assert_eq!(&req.remote_path, &remote_path);
        prop_assert_eq!(req.recursive, recursive);
        prop_assert_eq!(req.direction, direction);
    }
}

// ---------------------------------------------------------------------------
// Property 2.6: Serialize/Deserialize round-trip for StartRequest
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.1, 3.5**
//
// For any (target, argv) pair, serializing a StartRequest via prost and
// deserializing it back produces an identical message. This verifies
// wire-level identity for the current proto schema.
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    #[test]
    fn prop_start_request_serialize_roundtrip(
        target in "[a-zA-Z][a-zA-Z0-9_\\-]{0,30}",
        argv in prop::collection::vec("[a-zA-Z0-9_/\\-\\.]{1,30}", 0..10),
    ) {
        use prost::Message;

        let original = rpc::StartRequest {
            target: target.clone(),
            argv: argv.clone(),
            ..Default::default()
        };

        // Serialize
        let mut buf = Vec::new();
        original.encode(&mut buf).expect("encode should succeed");

        // Deserialize
        let decoded = rpc::StartRequest::decode(buf.as_slice())
            .expect("decode should succeed");

        prop_assert_eq!(&decoded.target, &original.target);
        prop_assert_eq!(&decoded.argv, &original.argv);
    }
}

// ---------------------------------------------------------------------------
// Property 2.7: Serialize/Deserialize round-trip for CopyStartRequest
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.3, 3.6**
//
// For any valid CopyStartRequest, serializing via prost and deserializing
// back produces an identical message.
proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    #[test]
    fn prop_copy_start_request_serialize_roundtrip(
        target in "[a-zA-Z][a-zA-Z0-9_\\-]{0,30}",
        local_path in "/[a-zA-Z0-9_/]{1,30}",
        remote_path in "/[a-zA-Z0-9_/]{1,30}",
        recursive in any::<bool>(),
        direction in prop_oneof![
            Just(rpc::CopyDirection::Upload as i32),
            Just(rpc::CopyDirection::Download as i32),
        ],
    ) {
        use prost::Message;

        let original = rpc::CopyStartRequest {
            target: target.clone(),
            local_path: local_path.clone(),
            remote_path: remote_path.clone(),
            recursive,
            direction,
            ..Default::default()
        };

        let mut buf = Vec::new();
        original.encode(&mut buf).expect("encode should succeed");

        let decoded = rpc::CopyStartRequest::decode(buf.as_slice())
            .expect("decode should succeed");

        prop_assert_eq!(&decoded.target, &original.target);
        prop_assert_eq!(&decoded.local_path, &original.local_path);
        prop_assert_eq!(&decoded.remote_path, &original.remote_path);
        prop_assert_eq!(decoded.recursive, original.recursive);
        prop_assert_eq!(decoded.direction, original.direction);
    }
}

// ---------------------------------------------------------------------------
// Property 2.8: Plain exec via harness produces response
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.1, 3.4, 3.5**
//
// Property-based test: for any valid target and non-empty argv, the daemon
// accepts the StartRequest and produces at least one response event.
// This exercises the full RPC path on unfixed code.
proptest! {
    #![proptest_config(ProptestConfig { cases: 20, .. ProptestConfig::default() })]

    #[test]
    fn prop_plain_exec_produces_response(
        argv in prop::collection::vec("[a-zA-Z0-9_]{1,10}", 1..4),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut harness = InProcessRpcHarness::new().await;

            let start_request = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::Start(rpc::StartRequest {
                    target: "stub-target".to_string(),
                    argv: argv.clone(),
                    ..Default::default()
                })),
            };

            let response = harness
                .client
                .execute(tokio_stream::once(start_request))
                .await
                .expect("Execute RPC should succeed");

            let mut stream = response.into_inner();
            let mut event_count = 0;

            while let Some(msg) = stream
                .message()
                .await
                .expect("failed to read response stream")
            {
                event_count += 1;
                if let Some(event) = msg.event {
                    match event {
                        rpc::execute_response::Event::ExitStatus(_) => break,
                        rpc::execute_response::Event::Error(_) => break,
                        _ => {}
                    }
                }
            }

            prop_assert!(
                event_count > 0,
                "daemon should produce at least one response event for argv={:?}",
                argv
            );

            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Property 2.9: Plain copy via harness produces response
// ---------------------------------------------------------------------------

// **Validates: Requirements 3.3, 3.6**
//
// Property-based test: for any valid copy parameters, the daemon accepts
// the CopyStartRequest and produces at least one response event.
proptest! {
    #![proptest_config(ProptestConfig { cases: 20, .. ProptestConfig::default() })]

    #[test]
    fn prop_plain_copy_produces_response(
        local_path in "/tmp/[a-zA-Z0-9_]{1,10}",
        remote_path in "/tmp/[a-zA-Z0-9_]{1,10}",
        recursive in any::<bool>(),
        direction in prop_oneof![
            Just(rpc::CopyDirection::Upload as i32),
            Just(rpc::CopyDirection::Download as i32),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut harness = InProcessRpcHarness::new().await;

            let start_request = rpc::CopyRequest {
                request: Some(rpc::copy_request::Request::Start(rpc::CopyStartRequest {
                    target: "stub-target".to_string(),
                    local_path: local_path.clone(),
                    remote_path: remote_path.clone(),
                    recursive,
                    direction,
                    ..Default::default()
                })),
            };

            let response = harness
                .client
                .copy(tokio_stream::once(start_request))
                .await
                .expect("Copy RPC should succeed");

            let mut stream = response.into_inner();
            let mut event_count = 0;

            while let Some(msg) = stream
                .message()
                .await
                .expect("failed to read response stream")
            {
                event_count += 1;
                if let Some(event) = msg.event {
                    match event {
                        rpc::copy_response::Event::Complete(_) => break,
                        rpc::copy_response::Event::Error(_) => break,
                        _ => {}
                    }
                }
            }

            prop_assert!(
                event_count > 0,
                "daemon should produce at least one response event for copy"
            );

            Ok(())
        })?;
    }
}
