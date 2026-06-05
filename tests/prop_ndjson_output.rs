//! Property-based test for NDJSON output validity.
//!
//! Feature: xhod-jumpserver-architecture, Property 14: NDJSON output is a valid stream
//!
//! For any arbitrary CliEvent value, JsonSink produces valid JSON that:
//! 1. Parses as a single JSON object
//! 2. Contains an "event" discriminator field
//! 3. The schema matches the documented CliEvent enum
//!
//! Validates: Requirements 17.6, 17.8, 17.9

use proptest::prelude::*;
use serde_json::Value;

use xho::output::CliEvent;

/// Strategy to generate arbitrary CliEvent values.
fn arb_cli_event() -> impl Strategy<Value = CliEvent> {
    prop_oneof![
        // Stdout with arbitrary bytes (base64-encoded internally)
        prop::collection::vec(any::<u8>(), 0..256).prop_map(|data| CliEvent::stdout(&data)),
        // Stderr with arbitrary bytes
        prop::collection::vec(any::<u8>(), 0..256).prop_map(|data| CliEvent::stderr(&data)),
        // ExitStatus with arbitrary code
        any::<i32>().prop_map(CliEvent::exit_status),
        // Error with arbitrary message and kind
        ("[^\x00]{0,100}", "[a-z_]{1,20}").prop_map(|(msg, kind)| CliEvent::error(msg, kind)),
        // Info with arbitrary message
        "[^\x00]{0,100}".prop_map(CliEvent::info),
        // AuthPrompt with arbitrary fields
        (
            "[a-f0-9]{8,32}",
            "[^\x00]{0,50}",
            "[a-z_]{1,20}",
            any::<bool>(),
            "[^\x00]{0,100}",
        )
            .prop_map(|(prompt_id, target_label, kind, secret, message)| {
                CliEvent::AuthPrompt {
                    prompt_id,
                    target_label,
                    kind,
                    secret,
                    message,
                }
            }),
        // ReviewResult
        ("[a-z]{3,10}", "[^\x00]{0,50}")
            .prop_map(|(action, reason)| CliEvent::ReviewResult { action, reason }),
        // ConfirmRequired
        ("[a-f0-9]{8,32}", "[^\x00]{0,100}").prop_map(|(execution_id, reason)| {
            CliEvent::ConfirmRequired {
                execution_id,
                reason,
            }
        }),
        // CopyComplete
        "[^\x00]{0,100}".prop_map(CliEvent::copy_complete),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// Property 14: For any arbitrary CliEvent, serializing it to JSON produces
    /// a valid JSON object with an "event" discriminator field.
    // Feature: xhod-jumpserver-architecture, Property 14: NDJSON output is a valid stream
    #[test]
    fn ndjson_output_is_valid_json(event in arb_cli_event()) {
        // Serialize the event to a JSON string (same as JsonSink does internally)
        let json_str = serde_json::to_string(&event).unwrap();

        // 1. Each line must parse as valid JSON
        let parsed: Value = serde_json::from_str(&json_str)
            .expect("CliEvent serialization must produce valid JSON");

        // 2. Must be a JSON object (not array, string, etc.)
        assert!(parsed.is_object(), "CliEvent must serialize to a JSON object, got: {}", json_str);

        // 3. Must have an "event" discriminator field
        let event_field = parsed.get("event")
            .expect("CliEvent JSON must have an 'event' field");

        // 4. The "event" field must be a non-empty string
        let event_tag = event_field.as_str()
            .expect("'event' field must be a string");
        assert!(!event_tag.is_empty(), "'event' field must not be empty");

        // 5. The event tag must be one of the documented event kinds
        let valid_tags = [
            "stdout", "stderr", "exit_status", "error", "info",
            "auth_prompt", "review_result", "confirm_required", "copy_complete",
        ];
        assert!(
            valid_tags.contains(&event_tag),
            "unexpected event tag '{}', expected one of {:?}",
            event_tag,
            valid_tags
        );

        // 6. Byte payload events must have a valid base64 data_b64 field
        if event_tag == "stdout" || event_tag == "stderr" {
            let data_b64 = parsed.get("data_b64")
                .expect("stdout/stderr events must have data_b64 field")
                .as_str()
                .expect("data_b64 must be a string");
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.decode(data_b64)
                .expect("data_b64 must be valid base64");
        }

        // 7. The JSON string must not contain embedded newlines (NDJSON requirement)
        assert!(
            !json_str.contains('\n'),
            "NDJSON line must not contain embedded newlines"
        );
    }
}
