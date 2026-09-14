use serde::{Deserialize, Serialize};

/// TOTP-based multi-factor authentication settings, used by the Jumpserver
/// gateway and the keyboard-interactive auth flow to generate one-time codes.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct MfaConfig {
    pub totp_secret_base32: String,
    pub digits: u32,
    pub period: u64,
    pub digest: String,
}

impl Default for MfaConfig {
    fn default() -> Self {
        Self {
            totp_secret_base32: String::new(),
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        }
    }
}
