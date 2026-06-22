//! Short-lived in-memory authorization tokens used by the bootstrap flow.
//!
//! A token is a 32-byte random value base64url-encoded (~43 chars). The raw
//! token is shown to the caller exactly once at generation time; the store
//! keeps only its SHA-256 hash (for O(1) lookup) and an 8-char prefix (for
//! display and prefix-based invalidation). Tokens are entirely in-memory and
//! are lost when the daemon restarts — `[server.remote] bootstrap_token` is
//! the recovery path.
//!
//! Concurrency: `once` tokens are consumed atomically. `validate` holds the
//! write lock for the whole "check TTL + mark consumed" path so concurrent
//! callers cannot both win.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

const TOKEN_BYTES: usize = 32;
const PREFIX_LEN: usize = 8;

/// Metadata for one issued token. The raw token string is NEVER stored.
#[derive(Clone, Debug)]
pub struct TokenEntry {
    pub prefix: String,
    pub hash: String,
    pub expires_at: SystemTime,
    pub once: bool,
    pub consumed: bool,
    pub created_at: SystemTime,
    pub label: Option<String>,
}

impl TokenEntry {
    pub fn is_expired(&self) -> bool {
        SystemTime::now() > self.expires_at
    }

    pub fn expires_at_rfc3339(&self) -> String {
        format_rfc3339_utc(self.expires_at)
    }

    pub fn created_at_rfc3339(&self) -> String {
        format_rfc3339_utc(self.created_at)
    }
}

/// Shared, cloneable token store. Cheap to clone (single `Arc`).
#[derive(Clone, Default)]
pub struct TokenStore(Arc<RwLock<HashMap<String, TokenEntry>>>);

impl TokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Generate a fresh token, store its hash + metadata, and return the raw
    /// token exactly once.
    pub async fn generate(&self, ttl: Duration, once: bool, label: Option<String>) -> String {
        let mut bytes = [0u8; TOKEN_BYTES];
        rand::thread_rng().fill_bytes(&mut bytes);
        let token = URL_SAFE_NO_PAD.encode(bytes);
        let prefix = token.chars().take(PREFIX_LEN).collect();
        let hash = hex_sha256(token.as_bytes());
        let now = SystemTime::now();
        let entry = TokenEntry {
            prefix,
            hash: hash.clone(),
            expires_at: now + ttl,
            once,
            consumed: false,
            created_at: now,
            label,
        };
        self.0.write().await.insert(hash, entry);
        token
    }

    /// Validate a raw token. For `once` tokens, the first caller wins and the
    /// token is marked consumed; subsequent callers return false. Expired
    /// tokens are swept on access.
    pub async fn validate(&self, raw: &str) -> bool {
        let hash = hex_sha256(raw.as_bytes());
        let mut map = self.0.write().await;
        let Some(entry) = map.get_mut(&hash) else {
            return false;
        };
        if entry.is_expired() {
            let hash = hash.clone();
            map.remove(&hash);
            return false;
        }
        if entry.once && entry.consumed {
            return false;
        }
        if entry.once {
            entry.consumed = true;
        }
        true
    }

    /// Invalidate by raw token, by full hash, or by 8-char prefix. Returns
    /// true if anything was removed.
    pub async fn invalidate(&self, token_or_prefix: &str) -> bool {
        let mut map = self.0.write().await;
        let needle = token_or_prefix.trim();
        // Try as raw token first (hash lookup).
        let hash = hex_sha256(needle.as_bytes());
        if map.remove(&hash).is_some() {
            return true;
        }
        // Fall back to prefix match.
        let to_remove: Vec<String> = map
            .iter()
            .filter_map(|(k, v)| {
                if v.prefix == needle {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();
        let removed = !to_remove.is_empty();
        for k in to_remove {
            map.remove(&k);
        }
        removed
    }

    /// List all non-expired entries, sweeping expired ones as a side effect.
    pub async fn list(&self) -> Vec<TokenEntry> {
        let mut map = self.0.write().await;
        let expired: Vec<String> = map
            .iter()
            .filter_map(|(k, v)| {
                if v.is_expired() {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();
        for k in expired {
            map.remove(&k);
        }
        let mut entries: Vec<_> = map.values().cloned().collect();
        entries.sort_by(|a, b| a.expires_at.cmp(&b.expires_at));
        entries
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    data_encoding::HEXLOWER.encode(&hasher.finalize())
}

/// Format a `SystemTime` as an RFC3339 UTC string (`YYYY-MM-DDTHH:MM:SSZ`).
/// Inlined to avoid pulling in chrono/time just for display.
pub(crate) fn format_rfc3339_utc(t: SystemTime) -> String {
    let Ok(dur) = t.duration_since(UNIX_EPOCH) else {
        return "1970-01-01T00:00:00Z".to_string();
    };
    let secs = dur.as_secs();
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month,
        day,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's civil-from-days algorithm.
/// https://howardhinnant.github.io/date_algorithms.html
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m_raw = if mp < 10 { mp + 3 } else { mp - 9 };
    let y_final = if m_raw <= 2 { y + 1 } else { y };
    (y_final, m_raw as u32, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn generate_returns_nonempty_token_and_lists() {
        let store = TokenStore::new();
        let t = store
            .generate(Duration::from_secs(300), true, Some("test".into()))
            .await;
        assert!(t.len() >= 40);
        let list = store.list().await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].prefix, t.chars().take(8).collect::<String>());
        assert!(list[0].once);
        assert!(!list[0].consumed);
        assert_eq!(list[0].label.as_deref(), Some("test"));
    }

    #[tokio::test]
    async fn once_token_consumed_exactly_once() {
        let store = TokenStore::new();
        let t = store.generate(Duration::from_secs(300), true, None).await;
        assert!(store.validate(&t).await);
        assert!(!store.validate(&t).await);
        assert!(!store.validate(&t).await);
    }

    #[tokio::test]
    async fn reusable_token_validates_repeatedly() {
        let store = TokenStore::new();
        let t = store.generate(Duration::from_secs(300), false, None).await;
        assert!(store.validate(&t).await);
        assert!(store.validate(&t).await);
        assert!(store.validate(&t).await);
    }

    #[tokio::test]
    async fn unknown_token_rejected() {
        let store = TokenStore::new();
        assert!(!store.validate("not-a-real-token").await);
    }

    #[tokio::test]
    async fn expired_token_rejected_and_swept() {
        let store = TokenStore::new();
        let t = store.generate(Duration::from_millis(10), true, None).await;
        sleep(Duration::from_millis(50)).await;
        assert!(!store.validate(&t).await);
        assert!(store.list().await.is_empty());
    }

    #[tokio::test]
    async fn invalidate_by_prefix_then_by_full_token() {
        let store = TokenStore::new();
        let t = store.generate(Duration::from_secs(300), false, None).await;
        let prefix = t.chars().take(8).collect::<String>();
        assert!(store.invalidate(&prefix).await);
        assert!(!store.validate(&t).await);

        let t2 = store.generate(Duration::from_secs(300), false, None).await;
        assert!(store.invalidate(&t2).await);
        assert!(!store.validate(&t2).await);
    }

    #[tokio::test]
    async fn invalidate_unknown_returns_false() {
        let store = TokenStore::new();
        assert!(!store.invalidate("deadbeef").await);
    }

    #[tokio::test]
    async fn concurrent_once_token_has_single_winner() {
        let store = TokenStore::new();
        let t = store.generate(Duration::from_secs(300), true, None).await;
        let s1 = store.clone();
        let s2 = store.clone();
        let t_clone = t.clone();
        let t1 = tokio::spawn(async move { s1.validate(&t).await });
        let t2 = tokio::spawn(async move { s2.validate(&t_clone).await });
        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();
        assert!(r1 ^ r2, "exactly one concurrent caller should win");
    }

    #[test]
    fn rfc3339_formatter_matches_known_epoch() {
        // 2023-01-01T00:00:00Z == 1672531200
        let t = UNIX_EPOCH + Duration::from_secs(1_672_531_200);
        assert_eq!(format_rfc3339_utc(t), "2023-01-01T00:00:00Z");
    }
}
