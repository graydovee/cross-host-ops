//! Secret value indirection for configuration files.
//!
//! Instead of storing plaintext credentials in `config.toml` / `server.toml`,
//! a [`Secret`] stores a *reference* to where the plaintext lives:
//!
//! - `env:NAME`   — read from environment variable `NAME`
//! - `file:/path` — read the trimmed contents of a file
//! - `vault:name` — decrypt entry `name` from the local encrypted vault
//!   (`~/.xho/secrets`), whose key is derived from an SSH identity file
//! - anything else — treated as a literal plaintext value (back-compat),
//!   resolving it logs a one-time warning encouraging migration
//!
//! Resolution is deferred to the moment a credential is actually needed
//! (e.g. when opening an SSH connection), so listing servers never triggers
//! environment / file / vault access.

use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::Once;

use anyhow::{Context, Result, anyhow};
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

/// The `[secret]` configuration section.
///
/// Controls where the encrypted vault lives and which identity file derives
/// its key. Both are optional: `vault_path` defaults to `~/.xho/secrets`, and
/// `key_source` falls back to `server.toml`'s `[defaults].identity_file`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SecretConfig {
    /// Path to the encrypted vault file. Defaults to `~/.xho/secrets`.
    pub vault_path: Option<String>,
    /// Identity file whose key material derives the vault encryption key.
    pub key_source: Option<String>,
}

/// Prefix markers for the supported secret backends.
const ENV_PREFIX: &str = "env:";
const FILE_PREFIX: &str = "file:";
const VAULT_PREFIX: &str = "vault:";

/// Where the plaintext of a secret comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretSource {
    /// A literal plaintext value embedded directly in the config (legacy).
    Literal(String),
    /// Read from an environment variable.
    Env(String),
    /// Read from a file (trailing whitespace/newline trimmed).
    File(PathBuf),
    /// Decrypt a named entry from the local vault.
    Vault(String),
}

/// A credential stored in configuration as an indirection reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Secret(SecretSource);

impl Secret {
    /// Build a secret from a raw config string, classifying it by prefix.
    pub fn from_reference(raw: &str) -> Self {
        if let Some(name) = raw.strip_prefix(ENV_PREFIX) {
            Secret(SecretSource::Env(name.to_string()))
        } else if let Some(path) = raw.strip_prefix(FILE_PREFIX) {
            Secret(SecretSource::File(PathBuf::from(path)))
        } else if let Some(name) = raw.strip_prefix(VAULT_PREFIX) {
            Secret(SecretSource::Vault(name.to_string()))
        } else {
            Secret(SecretSource::Literal(raw.to_string()))
        }
    }

    /// Construct a `vault:` reference for the given entry name.
    pub fn vault(name: impl Into<String>) -> Self {
        Secret(SecretSource::Vault(name.into()))
    }

    /// The backend source backing this secret.
    pub fn source(&self) -> &SecretSource {
        &self.0
    }

    /// Whether this secret is an inline plaintext literal (i.e. not yet
    /// migrated to an indirection backend). Used by `xho secret encrypt`.
    pub fn is_plaintext(&self) -> bool {
        matches!(self.0, SecretSource::Literal(_))
    }

    /// The plaintext value if this is a literal, otherwise `None`.
    pub fn literal_value(&self) -> Option<&str> {
        match &self.0 {
            SecretSource::Literal(value) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Render this secret back to its config string form (the reference,
    /// or the literal value for legacy entries).
    pub fn to_reference(&self) -> String {
        match &self.0 {
            SecretSource::Literal(value) => value.clone(),
            SecretSource::Env(name) => format!("{ENV_PREFIX}{name}"),
            SecretSource::File(path) => format!("{FILE_PREFIX}{}", path.display()),
            SecretSource::Vault(name) => format!("{VAULT_PREFIX}{name}"),
        }
    }

    /// Resolve to plaintext. `resolver` supplies vault location and the
    /// identity file used to derive the vault key; it is only consulted for
    /// `vault:` secrets.
    pub fn resolve(&self, resolver: &SecretResolver) -> Result<Zeroizing<String>> {
        match &self.0 {
            SecretSource::Literal(value) => {
                warn_plaintext_once();
                Ok(Zeroizing::new(value.clone()))
            }
            SecretSource::Env(name) => {
                let value = std::env::var(name).with_context(|| {
                    format!("secret references env var `{name}`, which is not set")
                })?;
                Ok(Zeroizing::new(value))
            }
            SecretSource::File(path) => {
                let raw = fs::read_to_string(path).with_context(|| {
                    format!("failed to read secret file {}", path.display())
                })?;
                Ok(Zeroizing::new(raw.trim_end_matches(['\n', '\r']).to_string()))
            }
            SecretSource::Vault(name) => resolver.resolve_vault(name),
        }
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the literal plaintext; show the backend kind instead.
        match &self.0 {
            SecretSource::Literal(_) => f.write_str("<plaintext secret>"),
            other => f.write_str(&Secret(other.clone()).to_reference()),
        }
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_reference())
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer).map_err(de::Error::custom)?;
        Ok(Secret::from_reference(&raw))
    }
}

/// Context needed to resolve `vault:` secrets.
///
/// `env:` / `file:` / literal secrets do not consult this; only the vault
/// path and key source matter, and they are resolved lazily.
#[derive(Clone, Debug, Default)]
pub struct SecretResolver {
    /// Path to the encrypted vault file (`~/.xho/secrets`).
    vault_path: Option<PathBuf>,
    /// Identity file whose key material derives the vault encryption key.
    key_source: Option<String>,
}

impl SecretResolver {
    pub fn new(vault_path: Option<PathBuf>, key_source: Option<String>) -> Self {
        Self {
            vault_path,
            key_source,
        }
    }

    fn resolve_vault(&self, name: &str) -> Result<Zeroizing<String>> {
        let vault_path = self
            .vault_path
            .clone()
            .unwrap_or_else(crate::config::path::default_vault_path);
        let key_source = self.key_source.as_deref().ok_or_else(|| {
            anyhow!(
                "secret `vault:{name}` requires an identity file to derive the vault key; \
                 set [secret].key_source or [defaults].identity_file"
            )
        })?;
        let vault = crate::secret::Vault::open(&vault_path)
            .with_context(|| format!("failed to open vault {}", vault_path.display()))?;
        vault.get(name, key_source)
    }
}

/// Emit a single warning the first time a legacy plaintext secret is resolved.
fn warn_plaintext_once() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        tracing::warn!(
            "configuration contains a plaintext secret; \
             run `xho secret encrypt` to migrate it to the encrypted vault"
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn classifies_prefixes() {
        assert_eq!(
            Secret::from_reference("env:FOO").source(),
            &SecretSource::Env("FOO".to_string())
        );
        assert_eq!(
            Secret::from_reference("file:/run/secrets/db").source(),
            &SecretSource::File(PathBuf::from("/run/secrets/db"))
        );
        assert_eq!(
            Secret::from_reference("vault:db").source(),
            &SecretSource::Vault("db".to_string())
        );
        assert_eq!(
            Secret::from_reference("hunter2").source(),
            &SecretSource::Literal("hunter2".to_string())
        );
    }

    #[test]
    fn literal_helpers() {
        let s = Secret::from_reference("hunter2");
        assert!(s.is_plaintext());
        assert_eq!(s.literal_value(), Some("hunter2"));
        let v = Secret::from_reference("vault:db");
        assert!(!v.is_plaintext());
        assert_eq!(v.literal_value(), None);
    }

    #[test]
    fn resolves_env() {
        // SAFETY: single-threaded test.
        unsafe { std::env::set_var("XHO_TEST_SECRET_ENV", "from-env") };
        let resolver = SecretResolver::default();
        let s = Secret::from_reference("env:XHO_TEST_SECRET_ENV");
        assert_eq!(&*s.resolve(&resolver).unwrap(), "from-env");
    }

    #[test]
    fn resolves_file_trimming_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pw");
        std::fs::write(&path, "from-file\n").unwrap();
        let resolver = SecretResolver::default();
        let s = Secret::from_reference(&format!("file:{}", path.display()));
        assert_eq!(&*s.resolve(&resolver).unwrap(), "from-file");
    }

    #[test]
    fn vault_without_key_source_errors() {
        let resolver = SecretResolver::new(None, None);
        let s = Secret::from_reference("vault:db");
        assert!(s.resolve(&resolver).is_err());
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 200, .. ProptestConfig::default() })]

        /// Any reference string round-trips through classification and back.
        #[test]
        fn prop_reference_round_trip(
            kind in 0u8..4,
            body in "[a-zA-Z0-9_./-]{0,24}",
        ) {
            let raw = match kind {
                0 => format!("env:{body}"),
                1 => format!("file:{body}"),
                2 => format!("vault:{body}"),
                // Literal body must not accidentally start with a known prefix.
                _ => format!("lit-{body}"),
            };
            let secret = Secret::from_reference(&raw);
            prop_assert_eq!(secret.to_reference(), raw);
        }
    }
}
