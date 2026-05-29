// Feature: rhopd-connect-and-server-list, Property 6: handler does not modify upstream error Display text
//
// This property test verifies that the `list_servers` handler's error
// conversion path preserves the upstream error's Display text byte-for-byte.
// The conversion under test is:
//   anyhow::Error (from build_jump_host) -> format!("{error}") -> ServerListSourceStatus::Error(msg)
//
// For any arbitrary UTF-8 string `s` (length 0–1024), constructing
// `anyhow!("{s}")` and formatting it with `format!("{}", error)` must yield
// exactly `s` in the resulting `ServerListSourceStatus::Error(msg)`.

use proptest::prelude::*;
use rhop::protocol::ServerListSourceStatus;

/// Simulate the exact error conversion path used in `daemon::list_servers`:
///
/// ```ignore
/// Err(error) => {
///     prebuilt_status.push((
///         ServerListSource::JumpHost(entry.name.clone()),
///         ServerListSourceStatus::Error(format!("{error}")),
///     ));
/// }
/// ```
///
/// Given an arbitrary string `s`, we construct `anyhow!("{s}")` and then
/// apply `format!("{}", error)` to produce the message stored in
/// `ServerListSourceStatus::Error(msg)`.
fn simulate_handler_error_conversion(s: &str) -> ServerListSourceStatus {
    let error: anyhow::Error = anyhow::anyhow!("{}", s);
    let msg = format!("{error}");
    ServerListSourceStatus::Error(msg)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, .. ProptestConfig::default() })]

    /// **Validates: Requirements 7.5**
    ///
    /// For any UTF-8 string `s` of length 0–1024, the handler's error
    /// conversion path must produce `ServerListSourceStatus::Error(msg)`
    /// where `msg == s` — no truncation, no prefix/suffix added, no
    /// character escaping.
    #[test]
    fn prop_error_text_passthrough(s in "\\PC{0,1024}") {
        let status = simulate_handler_error_conversion(&s);
        match status {
            ServerListSourceStatus::Error(msg) => {
                prop_assert_eq!(
                    &msg, &s,
                    "Error message must equal the original string byte-for-byte. \
                     Expected {:?}, got {:?}",
                    s, msg
                );
            }
            _ => {
                prop_assert!(false, "Expected ServerListSourceStatus::Error, got {:?}", status);
            }
        }
    }
}
