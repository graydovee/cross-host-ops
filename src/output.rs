//! CLI output module providing structured NDJSON and human-readable text sinks.
//!
//! All CLI events flow through the `OutputSink` trait, which has two implementations:
//! - `TextSink`: writes human-readable output (stdout/stderr separation)
//! - `JsonSink`: writes NDJSON (one JSON object per line) to stdout

use std::io::{self, Write};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;

/// All events the CLI can emit, tagged for NDJSON serialization.
///
/// In JSON mode, each event is serialized as a single JSON object with an `event`
/// discriminator field. Byte payloads are base64-encoded in the `data_b64` field.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CliEvent {
    Stdout {
        data_b64: String,
    },
    Stderr {
        data_b64: String,
    },
    ExitStatus {
        code: i32,
    },
    Error {
        message: String,
        kind: String,
    },
    Info {
        message: String,
    },
    AuthPrompt {
        prompt_id: String,
        target_label: String,
        kind: String,
        secret: bool,
        message: String,
    },
    ReviewResult {
        action: String,
        reason: String,
    },
    ConfirmRequired {
        execution_id: String,
        reason: String,
    },
    CopyComplete {
        message: String,
    },
}

impl CliEvent {
    /// Create a Stdout event from raw bytes (base64-encoded).
    pub fn stdout(data: &[u8]) -> Self {
        CliEvent::Stdout {
            data_b64: BASE64.encode(data),
        }
    }

    /// Create a Stderr event from raw bytes (base64-encoded).
    pub fn stderr(data: &[u8]) -> Self {
        CliEvent::Stderr {
            data_b64: BASE64.encode(data),
        }
    }

    /// Create an Info event.
    pub fn info(message: impl Into<String>) -> Self {
        CliEvent::Info {
            message: message.into(),
        }
    }

    /// Create an Error event.
    pub fn error(message: impl Into<String>, kind: impl Into<String>) -> Self {
        CliEvent::Error {
            message: message.into(),
            kind: kind.into(),
        }
    }

    /// Create an ExitStatus event.
    pub fn exit_status(code: i32) -> Self {
        CliEvent::ExitStatus { code }
    }

    /// Create a CopyComplete event.
    pub fn copy_complete(message: impl Into<String>) -> Self {
        CliEvent::CopyComplete {
            message: message.into(),
        }
    }
}

/// Trait for emitting CLI events in different output formats.
pub trait OutputSink: Send {
    /// Emit a single event.
    fn emit(&self, event: &CliEvent);

    /// Signal that no more events will be emitted. Implementations may flush buffers.
    fn finish(&self);
}

/// Text-mode sink: writes human-readable output.
///
/// - Stdout events → CLI stdout (raw bytes)
/// - Stderr events → CLI stderr (raw bytes)
/// - Info/Error events → CLI stderr
/// - ExitStatus → no output (used for exit code)
/// - Other events → CLI stderr as formatted text
pub struct TextSink;

impl TextSink {
    pub fn new() -> Self {
        TextSink
    }
}

impl OutputSink for TextSink {
    fn emit(&self, event: &CliEvent) {
        match event {
            CliEvent::Stdout { data_b64 } => {
                if let Ok(bytes) = BASE64.decode(data_b64) {
                    let _ = io::stdout().write_all(&bytes);
                    let _ = io::stdout().flush();
                }
            }
            CliEvent::Stderr { data_b64 } => {
                if let Ok(bytes) = BASE64.decode(data_b64) {
                    let _ = io::stderr().write_all(&bytes);
                    let _ = io::stderr().flush();
                }
            }
            CliEvent::Info { message } => {
                let _ = writeln!(io::stderr(), "{}", message);
            }
            CliEvent::Error { message, .. } => {
                let _ = writeln!(io::stderr(), "error: {}", message);
            }
            CliEvent::ExitStatus { .. } => {
                // Exit status is used for the process exit code, not printed.
            }
            CliEvent::AuthPrompt { message, .. } => {
                let _ = write!(io::stderr(), "{}", message);
                let _ = io::stderr().flush();
            }
            CliEvent::ReviewResult { action, reason } => {
                let _ = writeln!(io::stderr(), "review: {} ({})", action, reason);
            }
            CliEvent::ConfirmRequired { reason, .. } => {
                let _ = write!(io::stderr(), "{}", reason);
                let _ = io::stderr().flush();
            }
            CliEvent::CopyComplete { message } => {
                if !message.is_empty() {
                    let _ = writeln!(io::stdout(), "{}", message);
                    let _ = io::stdout().flush();
                }
            }
        }
    }

    fn finish(&self) {
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
    }
}

/// JSON-mode sink: writes NDJSON (one JSON object per line) to stdout.
///
/// Every event is serialized as a single JSON object terminated by `\n`.
/// The CLI's stderr is reserved for genuine bugs (panics, unhandled errors).
pub struct JsonSink;

impl JsonSink {
    pub fn new() -> Self {
        JsonSink
    }
}

impl OutputSink for JsonSink {
    fn emit(&self, event: &CliEvent) {
        let mut stdout = io::stdout().lock();
        // serde_json::to_writer writes compact JSON without trailing newline
        let _ = serde_json::to_writer(&mut stdout, event);
        let _ = stdout.write_all(b"\n");
        let _ = stdout.flush();
    }

    fn finish(&self) {
        let _ = io::stdout().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_event_stdout_base64() {
        let event = CliEvent::stdout(b"hello world");
        match &event {
            CliEvent::Stdout { data_b64 } => {
                assert_eq!(BASE64.decode(data_b64).unwrap(), b"hello world");
            }
            _ => panic!("expected Stdout variant"),
        }
    }

    #[test]
    fn test_cli_event_serialization_has_event_tag() {
        let event = CliEvent::info("test message");
        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event"], "info");
        assert_eq!(parsed["message"], "test message");
    }

    #[test]
    fn test_cli_event_exit_status_serialization() {
        let event = CliEvent::exit_status(42);
        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event"], "exit_status");
        assert_eq!(parsed["code"], 42);
    }

    #[test]
    fn test_cli_event_error_serialization() {
        let event = CliEvent::error("something failed", "transport");
        let json = serde_json::to_string(&event).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event"], "error");
        assert_eq!(parsed["message"], "something failed");
        assert_eq!(parsed["kind"], "transport");
    }

    #[test]
    fn test_json_sink_produces_valid_ndjson_line() {
        // We can't easily capture stdout in a unit test, but we can verify
        // the serialization logic produces valid JSON.
        let event = CliEvent::stdout(b"\x00\x01\x02\xff");
        let json = serde_json::to_string(&event).unwrap();
        // Verify it parses back
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["event"], "stdout");
        // Verify the base64 round-trips
        let decoded = BASE64.decode(parsed["data_b64"].as_str().unwrap()).unwrap();
        assert_eq!(decoded, b"\x00\x01\x02\xff");
    }

    #[test]
    fn test_all_event_variants_serialize_with_event_tag() {
        let events = vec![
            CliEvent::stdout(b"data"),
            CliEvent::stderr(b"err"),
            CliEvent::exit_status(0),
            CliEvent::error("msg", "kind"),
            CliEvent::info("info"),
            CliEvent::AuthPrompt {
                prompt_id: "id".into(),
                target_label: "target".into(),
                kind: "password".into(),
                secret: true,
                message: "enter password".into(),
            },
            CliEvent::ReviewResult {
                action: "allow".into(),
                reason: "safe".into(),
            },
            CliEvent::ConfirmRequired {
                execution_id: "exec-1".into(),
                reason: "dangerous command".into(),
            },
            CliEvent::copy_complete("done"),
        ];

        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
            // Every variant must have an "event" field
            assert!(
                parsed.get("event").is_some(),
                "missing 'event' tag in: {}",
                json
            );
            // The event field must be a non-empty string
            let event_tag = parsed["event"].as_str().unwrap();
            assert!(!event_tag.is_empty());
        }
    }
}
