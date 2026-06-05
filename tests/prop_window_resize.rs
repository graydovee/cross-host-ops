//! Property-based test: Window Resize Propagation
//!
//! Feature: interactive-pty-passthrough
//! Property 4: Window Resize Propagation
//!
//! For any valid terminal size (cols > 0, rows > 0), a SIGWINCH event results
//! in a WindowResize message being sent with the correct dimensions, and the
//! daemon forwards those exact dimensions to the remote SSH channel via
//! `window_change`.
//!
//! Since we cannot easily trigger real SIGWINCH in tests, we test the data
//! path: WindowResize dimensions are preserved exactly through the gRPC
//! protocol layer (serialization/deserialization round-trip).
//!
//! **Validates: Requirements 6.1, 6.2**

use proptest::prelude::*;

use prost::Message;
use xho::protocol::rpc;

// ---------------------------------------------------------------------------
// Property 4: Window Resize Propagation — arbitrary valid sizes
// ---------------------------------------------------------------------------

/// **Validates: Requirements 6.1, 6.2**
///
/// For any valid terminal size (cols in 1..=500, rows in 1..=200), a
/// WindowResize message round-trips through prost serialization/deserialization
/// preserving the exact dimensions.
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    #[test]
    fn prop_window_resize_roundtrip_preserves_dimensions(
        cols in 1u32..=500,
        rows in 1u32..=200,
    ) {
        // Create WindowResize message
        let resize_msg = rpc::WindowResize { cols, rows };

        // Serialize via prost
        let mut buf = Vec::new();
        resize_msg.encode(&mut buf).expect("encode WindowResize should succeed");

        // Deserialize
        let decoded = rpc::WindowResize::decode(buf.as_slice())
            .expect("decode WindowResize should succeed");

        // Verify exact dimension preservation
        prop_assert_eq!(decoded.cols, cols, "cols must be preserved exactly");
        prop_assert_eq!(decoded.rows, rows, "rows must be preserved exactly");
    }

    /// Verify that WindowResize wrapped inside an ExecuteRequest oneof also
    /// preserves dimensions through serialization round-trip.
    #[test]
    fn prop_execute_request_window_resize_roundtrip(
        cols in 1u32..=500,
        rows in 1u32..=200,
    ) {
        // Wrap in full ExecuteRequest message
        let exec_req = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::WindowResize(
                rpc::WindowResize { cols, rows },
            )),
        };

        // Serialize via prost
        let mut buf = Vec::new();
        exec_req.encode(&mut buf).expect("encode ExecuteRequest should succeed");

        // Deserialize
        let decoded = rpc::ExecuteRequest::decode(buf.as_slice())
            .expect("decode ExecuteRequest should succeed");

        // Extract WindowResize and verify dimensions
        match decoded.request {
            Some(rpc::execute_request::Request::WindowResize(resize)) => {
                prop_assert_eq!(
                    resize.cols, cols,
                    "ExecuteRequest WindowResize cols must be preserved"
                );
                prop_assert_eq!(
                    resize.rows, rows,
                    "ExecuteRequest WindowResize rows must be preserved"
                );
            }
            other => {
                prop_assert!(
                    false,
                    "expected WindowResize variant, got {:?}", other
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property 4: Window Resize Propagation — edge case: minimum size (1, 1)
// ---------------------------------------------------------------------------

/// **Validates: Requirements 6.1, 6.2**
///
/// The minimum valid terminal size (1, 1) is preserved through round-trip.
#[test]
fn window_resize_minimum_size() {
    let resize_msg = rpc::WindowResize { cols: 1, rows: 1 };

    let mut buf = Vec::new();
    resize_msg.encode(&mut buf).expect("encode should succeed");

    let decoded = rpc::WindowResize::decode(buf.as_slice()).expect("decode should succeed");

    assert_eq!(decoded.cols, 1, "minimum cols must be preserved");
    assert_eq!(decoded.rows, 1, "minimum rows must be preserved");
}

// ---------------------------------------------------------------------------
// Property 4: Window Resize Propagation — edge case: maximum reasonable sizes
// ---------------------------------------------------------------------------

/// **Validates: Requirements 6.1, 6.2**
///
/// Maximum reasonable terminal sizes are preserved through round-trip.
#[test]
fn window_resize_maximum_reasonable_sizes() {
    // Large but reasonable terminal sizes
    let test_cases: Vec<(u32, u32)> = vec![
        (500, 200), // max from our generator range
        (320, 100), // 4K ultra-wide
        (240, 67),  // typical large terminal
        (80, 24),   // classic VT100
        (132, 43),  // classic VT132
    ];

    for (cols, rows) in test_cases {
        let resize_msg = rpc::WindowResize { cols, rows };

        let mut buf = Vec::new();
        resize_msg.encode(&mut buf).expect("encode should succeed");

        let decoded = rpc::WindowResize::decode(buf.as_slice()).expect("decode should succeed");

        assert_eq!(decoded.cols, cols, "cols {} must be preserved", cols);
        assert_eq!(decoded.rows, rows, "rows {} must be preserved", rows);
    }
}

// ---------------------------------------------------------------------------
// Property 4: Window Resize Propagation — StartRequest initial dimensions
// ---------------------------------------------------------------------------

/// **Validates: Requirements 6.1, 6.2**
///
/// Initial terminal dimensions in StartRequest (term_cols, term_rows) are
/// preserved through serialization round-trip, ensuring the initial PTY
/// allocation uses the correct client dimensions.
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    #[test]
    fn prop_start_request_initial_dimensions_preserved(
        cols in 1u32..=500,
        rows in 1u32..=200,
    ) {
        let start_req = rpc::StartRequest {
            target: "test-host".to_string(),
            argv: vec!["vim".to_string()],
            pty: true,
            no_pty: false,
            stdin: false,
            timeout_ms: 0,
            interactive: true,
            term_cols: cols,
            term_rows: rows,
            shell: String::new(),
            no_shell: false,
        };

        // Wrap in ExecuteRequest
        let exec_req = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::Start(start_req)),
        };

        // Serialize
        let mut buf = Vec::new();
        exec_req.encode(&mut buf).expect("encode should succeed");

        // Deserialize
        let decoded = rpc::ExecuteRequest::decode(buf.as_slice())
            .expect("decode should succeed");

        // Extract StartRequest and verify dimensions
        match decoded.request {
            Some(rpc::execute_request::Request::Start(start)) => {
                prop_assert_eq!(
                    start.term_cols, cols,
                    "StartRequest term_cols must be preserved"
                );
                prop_assert_eq!(
                    start.term_rows, rows,
                    "StartRequest term_rows must be preserved"
                );
                prop_assert!(start.interactive, "interactive flag must be preserved");
                prop_assert!(start.pty, "pty flag must be preserved");
            }
            other => {
                prop_assert!(
                    false,
                    "expected Start variant, got {:?}", other
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property 4: Window Resize Propagation — multiple resize events ordering
// ---------------------------------------------------------------------------

/// **Validates: Requirements 6.1, 6.2**
///
/// When multiple WindowResize messages are sent in sequence (simulating rapid
/// SIGWINCH events), each message independently preserves its dimensions.
proptest! {
    #![proptest_config(ProptestConfig { cases: 50, .. ProptestConfig::default() })]

    #[test]
    fn prop_window_resize_multiple_events_preserved(
        sizes in prop::collection::vec(
            (1u32..=500, 1u32..=200),
            1..20
        ),
    ) {
        // Simulate sending multiple WindowResize messages in order
        let mut serialized: Vec<Vec<u8>> = Vec::new();

        for &(cols, rows) in &sizes {
            let exec_req = rpc::ExecuteRequest {
                request: Some(rpc::execute_request::Request::WindowResize(
                    rpc::WindowResize { cols, rows },
                )),
            };
            let mut buf = Vec::new();
            exec_req.encode(&mut buf).expect("encode should succeed");
            serialized.push(buf);
        }

        // Deserialize each and verify dimensions preserved in order
        for (i, ((expected_cols, expected_rows), encoded)) in
            sizes.iter().zip(serialized.iter()).enumerate()
        {
            let decoded = rpc::ExecuteRequest::decode(encoded.as_slice())
                .expect("decode should succeed");

            match decoded.request {
                Some(rpc::execute_request::Request::WindowResize(resize)) => {
                    prop_assert_eq!(
                        resize.cols, *expected_cols,
                        "resize event {} cols must be preserved", i
                    );
                    prop_assert_eq!(
                        resize.rows, *expected_rows,
                        "resize event {} rows must be preserved", i
                    );
                }
                other => {
                    prop_assert!(
                        false,
                        "resize event {}: expected WindowResize, got {:?}", i, other
                    );
                }
            }
        }
    }
}
