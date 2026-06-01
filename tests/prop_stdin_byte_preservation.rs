//! Property-based test: Stdin Byte Preservation
//!
//! Feature: interactive-pty-passthrough
//! Property 2: Stdin Byte Preservation
//!
//! For any sequence of raw bytes (including non-UTF8, null bytes, control
//! characters, and all 256 possible byte values) written to stdin, the bytes
//! forwarded through the StdinData gRPC message to the remote SSH channel are
//! identical in content and order — no encoding transformation, no loss, no
//! reordering.
//!
//! **Validates: Requirements 5.1, 5.2, 7.2**

use proptest::prelude::*;

use prost::Message;
use rhop::protocol::rpc;

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — arbitrary byte sequences
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 5.2, 7.2**
///
/// For any arbitrary byte sequence, wrapping it in a StdinData message and
/// round-tripping through prost serialization/deserialization preserves the
/// exact bytes in content and order.
proptest! {
    #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

    #[test]
    fn prop_stdin_data_roundtrip_preserves_bytes(
        data in prop::collection::vec(any::<u8>(), 0..4096),
    ) {
        // Wrap in StdinData message
        let stdin_msg = rpc::StdinData {
            data: data.clone(),
        };

        // Serialize via prost
        let mut buf = Vec::new();
        stdin_msg.encode(&mut buf).expect("encode StdinData should succeed");

        // Deserialize
        let decoded = rpc::StdinData::decode(buf.as_slice())
            .expect("decode StdinData should succeed");

        // Verify exact byte preservation
        prop_assert_eq!(
            &decoded.data, &data,
            "StdinData round-trip must preserve bytes exactly"
        );
    }

    /// Verify that StdinData wrapped inside an ExecuteRequest oneof also
    /// preserves bytes through serialization round-trip.
    #[test]
    fn prop_execute_request_stdin_data_roundtrip(
        data in prop::collection::vec(any::<u8>(), 0..4096),
    ) {
        // Wrap in full ExecuteRequest message
        let exec_req = rpc::ExecuteRequest {
            request: Some(rpc::execute_request::Request::StdinData(rpc::StdinData {
                data: data.clone(),
            })),
        };

        // Serialize via prost
        let mut buf = Vec::new();
        exec_req.encode(&mut buf).expect("encode ExecuteRequest should succeed");

        // Deserialize
        let decoded = rpc::ExecuteRequest::decode(buf.as_slice())
            .expect("decode ExecuteRequest should succeed");

        // Extract StdinData and verify bytes
        match decoded.request {
            Some(rpc::execute_request::Request::StdinData(stdin)) => {
                prop_assert_eq!(
                    &stdin.data, &data,
                    "ExecuteRequest StdinData round-trip must preserve bytes exactly"
                );
            }
            other => {
                prop_assert!(
                    false,
                    "expected StdinData variant, got {:?}", other
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — all 256 byte values
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 5.2, 7.2**
///
/// A sequence containing all 256 possible byte values (0x00..=0xFF) is
/// preserved exactly through StdinData serialization round-trip.
#[test]
fn stdin_data_all_256_byte_values() {
    let all_bytes: Vec<u8> = (0u8..=255).collect();

    let stdin_msg = rpc::StdinData {
        data: all_bytes.clone(),
    };

    let mut buf = Vec::new();
    stdin_msg.encode(&mut buf).expect("encode should succeed");

    let decoded = rpc::StdinData::decode(buf.as_slice())
        .expect("decode should succeed");

    assert_eq!(decoded.data, all_bytes, "all 256 byte values must be preserved");
}

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — null bytes
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 7.2**
///
/// Sequences consisting entirely of null bytes are preserved.
#[test]
fn stdin_data_null_bytes() {
    let null_bytes = vec![0u8; 1024];

    let stdin_msg = rpc::StdinData {
        data: null_bytes.clone(),
    };

    let mut buf = Vec::new();
    stdin_msg.encode(&mut buf).expect("encode should succeed");

    let decoded = rpc::StdinData::decode(buf.as_slice())
        .expect("decode should succeed");

    assert_eq!(decoded.data, null_bytes, "null byte sequence must be preserved");
}

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — non-UTF8 sequences
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 7.2**
///
/// Non-UTF8 byte sequences (invalid UTF-8 continuations) are preserved.
#[test]
fn stdin_data_non_utf8_sequences() {
    // Invalid UTF-8: high bytes without proper continuation
    let non_utf8: Vec<u8> = vec![0xFF, 0xFE, 0x80, 0xC0, 0xC1, 0xF5, 0xF8, 0xFC];

    let stdin_msg = rpc::StdinData {
        data: non_utf8.clone(),
    };

    let mut buf = Vec::new();
    stdin_msg.encode(&mut buf).expect("encode should succeed");

    let decoded = rpc::StdinData::decode(buf.as_slice())
        .expect("decode should succeed");

    assert_eq!(decoded.data, non_utf8, "non-UTF8 bytes must be preserved");
}

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — empty bytes
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 7.2**
///
/// An empty byte sequence is preserved (no phantom bytes introduced).
#[test]
fn stdin_data_empty_bytes() {
    let empty: Vec<u8> = vec![];

    let stdin_msg = rpc::StdinData {
        data: empty.clone(),
    };

    let mut buf = Vec::new();
    stdin_msg.encode(&mut buf).expect("encode should succeed");

    let decoded = rpc::StdinData::decode(buf.as_slice())
        .expect("decode should succeed");

    assert_eq!(decoded.data, empty, "empty byte sequence must be preserved");
}

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — single byte values
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 7.2**
///
/// Each individual byte value (0x00..=0xFF) is preserved when sent alone.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn prop_stdin_data_single_byte(byte_val in any::<u8>()) {
        let data = vec![byte_val];

        let stdin_msg = rpc::StdinData {
            data: data.clone(),
        };

        let mut buf = Vec::new();
        stdin_msg.encode(&mut buf).expect("encode should succeed");

        let decoded = rpc::StdinData::decode(buf.as_slice())
            .expect("decode should succeed");

        prop_assert_eq!(
            &decoded.data, &data,
            "single byte 0x{:02X} must be preserved", byte_val
        );
    }
}

// ---------------------------------------------------------------------------
// Property 2: Stdin Byte Preservation — ordering preserved across chunks
// ---------------------------------------------------------------------------

/// **Validates: Requirements 5.1, 5.2, 7.2**
///
/// When multiple StdinData messages are sent in sequence, each message
/// independently preserves its bytes, ensuring overall ordering is maintained.
proptest! {
    #![proptest_config(ProptestConfig { cases: 50, .. ProptestConfig::default() })]

    #[test]
    fn prop_stdin_data_multiple_chunks_ordering(
        chunks in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..512),
            1..10
        ),
    ) {
        // Simulate sending multiple StdinData messages in order
        let mut serialized_chunks: Vec<Vec<u8>> = Vec::new();

        for chunk in &chunks {
            let stdin_msg = rpc::StdinData {
                data: chunk.clone(),
            };
            let mut buf = Vec::new();
            stdin_msg.encode(&mut buf).expect("encode should succeed");
            serialized_chunks.push(buf);
        }

        // Deserialize each and verify order + content
        for (i, (original, encoded)) in chunks.iter().zip(serialized_chunks.iter()).enumerate() {
            let decoded = rpc::StdinData::decode(encoded.as_slice())
                .expect("decode should succeed");
            prop_assert_eq!(
                &decoded.data, original,
                "chunk {} must preserve bytes exactly", i
            );
        }
    }
}
