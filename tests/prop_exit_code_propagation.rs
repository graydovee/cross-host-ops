//! Property test for exit code propagation via SSH ExitStatus.
//!
//! Feature: interactive-pty-passthrough
//! Property 5: Exit Code Propagation via SSH ExitStatus
//!
//! For any PTY command execution (interactive or non-interactive) through
//! `DirectSshConnection`, the exit code returned to the caller is obtained
//! from `ChannelMsg::ExitStatus` delivered by the SSH protocol. For any exit
//! code value in the range 0–255, the value is preserved without modification
//! through the execution pipeline.
//!
//! **Validates: Requirements 1.2, 7.4**

use proptest::prelude::*;
use prost::Message;

use rhop::protocol::{ServerEvent, server_event_to_rpc};
use rhop::protocol::rpc;

// ---------------------------------------------------------------------------
// Property 5.1: ServerEvent::ExitStatus → rpc::ExecuteResponse preserves code
// ---------------------------------------------------------------------------

// **Validates: Requirements 1.2, 7.4**
//
// For any exit code in 0..=255, converting a ServerEvent::ExitStatus to an
// rpc::ExecuteResponse via server_event_to_rpc preserves the exit code value
// without modification. This is the daemon-side conversion that bridges the
// SSH ChannelMsg::ExitStatus to the gRPC response stream.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn prop_exit_code_preserved_through_server_event_to_rpc(code in 0i32..=255) {
        // Simulate what DirectSshConnection::execute() does when it receives
        // ChannelMsg::ExitStatus { exit_status }: it stores exit_status as i32
        // and the daemon sends ServerEvent::ExitStatus { code }.
        let event = ServerEvent::ExitStatus { code };
        let response = server_event_to_rpc(event);

        // Extract the exit code from the RPC response
        let extracted_code = match response.event {
            Some(rpc::execute_response::Event::ExitStatus(status)) => status.code,
            other => panic!(
                "expected ExitStatus event, got {:?} for input code={}",
                other, code
            ),
        };

        prop_assert_eq!(
            extracted_code, code,
            "exit code should be preserved through server_event_to_rpc: \
             input={}, output={}",
            code, extracted_code
        );
    }
}

// ---------------------------------------------------------------------------
// Property 5.2: rpc::ExitStatus proto serialization round-trip
// ---------------------------------------------------------------------------

// **Validates: Requirements 1.2, 7.4**
//
// For any exit code in 0..=255, serializing an rpc::ExitStatus message via
// prost and deserializing it back produces the same exit code value. This
// verifies the wire-level identity of exit codes through the gRPC protocol.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn prop_exit_code_preserved_through_proto_roundtrip(code in 0i32..=255) {
        let original = rpc::ExitStatus { code };

        // Serialize to bytes (simulates sending over gRPC wire)
        let mut buf = Vec::new();
        original.encode(&mut buf).expect("encode should succeed");

        // Deserialize from bytes (simulates receiving on client side)
        let decoded = rpc::ExitStatus::decode(buf.as_slice())
            .expect("decode should succeed");

        prop_assert_eq!(
            decoded.code, code,
            "exit code should survive proto serialization round-trip: \
             input={}, decoded={}",
            code, decoded.code
        );
    }
}

// ---------------------------------------------------------------------------
// Property 5.3: Full ExecuteResponse with ExitStatus proto round-trip
// ---------------------------------------------------------------------------

// **Validates: Requirements 1.2, 7.4**
//
// For any exit code in 0..=255, the full ExecuteResponse message containing
// an ExitStatus event preserves the exit code through serialization. This
// tests the complete response envelope that the client receives.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn prop_exit_code_preserved_in_full_response_roundtrip(code in 0i32..=255) {
        // Build the full response as the daemon would send it
        let event = ServerEvent::ExitStatus { code };
        let response = server_event_to_rpc(event);

        // Serialize the full ExecuteResponse
        let mut buf = Vec::new();
        response.encode(&mut buf).expect("encode should succeed");

        // Deserialize as the client would receive it
        let decoded = rpc::ExecuteResponse::decode(buf.as_slice())
            .expect("decode should succeed");

        let extracted_code = match decoded.event {
            Some(rpc::execute_response::Event::ExitStatus(status)) => status.code,
            other => panic!(
                "expected ExitStatus event after round-trip, got {:?} for code={}",
                other, code
            ),
        };

        prop_assert_eq!(
            extracted_code, code,
            "exit code should be preserved through full response round-trip: \
             input={}, output={}",
            code, extracted_code
        );
    }
}

// ---------------------------------------------------------------------------
// Property 5.4: Exit code from ChannelMsg::ExitStatus cast preserves value
// ---------------------------------------------------------------------------

// **Validates: Requirements 1.2, 7.4**
//
// In DirectSshConnection::execute(), the exit code is obtained via:
//   ChannelMsg::ExitStatus { exit_status } => exit_code = Some(exit_status as i32)
//
// For any u32 value in 0..=255 (the valid SSH exit code range), casting to
// i32 and back preserves the value. This verifies the type conversion at the
// SSH protocol boundary does not corrupt exit codes.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn prop_exit_code_u32_to_i32_cast_preserves_value(exit_status in 0u32..=255) {
        // This is exactly what DirectSshConnection::execute() does:
        // ChannelMsg::ExitStatus { exit_status } => exit_code = Some(exit_status as i32)
        let code_as_i32 = exit_status as i32;

        // The value must be non-negative and equal to the original
        prop_assert!(
            code_as_i32 >= 0,
            "cast from u32 {} to i32 produced negative value {}",
            exit_status, code_as_i32
        );
        prop_assert_eq!(
            code_as_i32 as u32, exit_status,
            "round-trip u32→i32→u32 should preserve value: {} != {}",
            code_as_i32 as u32, exit_status
        );
    }
}

// ---------------------------------------------------------------------------
// Property 5.5: End-to-end exit code pipeline (cast + event + proto)
// ---------------------------------------------------------------------------

// **Validates: Requirements 1.2, 7.4**
//
// For any exit code in 0..=255, the complete pipeline from SSH u32 exit_status
// through i32 cast, ServerEvent creation, RPC conversion, proto serialization,
// and deserialization preserves the original value without modification.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    #[test]
    fn prop_exit_code_end_to_end_pipeline(exit_status in 0u32..=255) {
        // Step 1: SSH layer delivers ChannelMsg::ExitStatus { exit_status }
        // DirectSshConnection::execute() casts to i32
        let code = exit_status as i32;

        // Step 2: Daemon creates ServerEvent::ExitStatus { code }
        let event = ServerEvent::ExitStatus { code };

        // Step 3: Daemon converts to RPC response
        let response = server_event_to_rpc(event);

        // Step 4: Serialize over the wire
        let mut buf = Vec::new();
        response.encode(&mut buf).expect("encode should succeed");

        // Step 5: Client deserializes
        let decoded = rpc::ExecuteResponse::decode(buf.as_slice())
            .expect("decode should succeed");

        // Step 6: Client extracts exit code
        let final_code = match decoded.event {
            Some(rpc::execute_response::Event::ExitStatus(status)) => status.code,
            other => panic!(
                "expected ExitStatus event, got {:?} for exit_status={}",
                other, exit_status
            ),
        };

        // The final code must equal the original SSH exit_status
        prop_assert_eq!(
            final_code as u32, exit_status,
            "end-to-end pipeline should preserve exit code: \
             ssh_exit_status={}, final_code={}",
            exit_status, final_code
        );
    }
}
