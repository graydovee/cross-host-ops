//! Encrypted vault file format and crypto operations.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zeroize::Zeroizing;

use russh::keys::ssh_encoding::Encode;
use russh::keys::{HashAlg, load_secret_key};

use crate::config::expand_tilde;

const VERSION: u32 = 1;
const KDF: &str = "hkdf-sha256";
const AEAD: &str = "xchacha20poly1305";
const HKDF_INFO: &[u8] = b"xho-vault:xchacha20poly1305:v1";
const NONCE_LEN: usize = 24;
const SALT_LEN: usize = 32;
const KEY_LEN: usize = 32;

/// On-disk representation of the vault.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VaultFile {
    header: Header,
    #[serde(default)]
    entries: BTreeMap<String, Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Header {
    version: u32,
    kdf: String,
    aead: String,
    /// base64 HKDF salt.
    salt: String,
    /// SHA256 fingerprint of the identity used to derive the key, e.g.
    /// `SHA256:abc...`. Used to detect a mismatched identity file.
    key_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    /// base64 XChaCha20-Poly1305 nonce (24 bytes).
    nonce: String,
    /// base64 ciphertext (includes the Poly1305 tag).
    ciphertext: String,
}

/// An open vault, backed by a file on disk.
pub struct Vault {
    path: PathBuf,
    file: VaultFile,
}

impl Vault {
    /// Open an existing vault. Errors if the file does not exist.
    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            bail!(
                "vault {} does not exist; create entries with `xho secret set <name>`",
                path.display()
            );
        }
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read vault {}", path.display()))?;
        let file: VaultFile = toml::from_str(&raw)
            .with_context(|| format!("failed to parse vault {}", path.display()))?;
        if file.header.version != VERSION {
            bail!(
                "unsupported vault version {} (expected {})",
                file.header.version,
                VERSION
            );
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    /// Open an existing vault, or build an empty in-memory one whose header is
    /// initialized from `key_source` (a salt is freshly generated). The empty
    /// vault is not written until [`Vault::save`] is called.
    pub fn open_or_init(path: &Path, key_source: &str) -> Result<Self> {
        if path.exists() {
            return Self::open(path);
        }
        let mut salt = [0u8; SALT_LEN];
        rand::thread_rng().fill_bytes(&mut salt);
        let fingerprint = identity_fingerprint(key_source)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: VaultFile {
                header: Header {
                    version: VERSION,
                    kdf: KDF.to_string(),
                    aead: AEAD.to_string(),
                    salt: BASE64.encode(salt),
                    key_fingerprint: fingerprint,
                },
                entries: BTreeMap::new(),
            },
        })
    }

    /// List entry names in sorted order.
    pub fn list(&self) -> Vec<String> {
        self.file.entries.keys().cloned().collect()
    }

    /// Whether an entry with the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.file.entries.contains_key(name)
    }

    /// Decrypt and return the plaintext for `name`, deriving the key from
    /// `key_source`.
    pub fn get(&self, name: &str, key_source: &str) -> Result<Zeroizing<String>> {
        let entry = self
            .file
            .entries
            .get(name)
            .ok_or_else(|| anyhow!("vault has no entry `{name}`"))?;
        let key = self.derive_key(key_source)?;
        let cipher = XChaCha20Poly1305::new((&*key).into());
        let nonce_bytes = BASE64
            .decode(&entry.nonce)
            .context("vault entry nonce is not valid base64")?;
        if nonce_bytes.len() != NONCE_LEN {
            bail!("vault entry `{name}` has an invalid nonce length");
        }
        let ciphertext = BASE64
            .decode(&entry.ciphertext)
            .context("vault entry ciphertext is not valid base64")?;
        let plaintext = cipher
            .decrypt(XNonce::from_slice(&nonce_bytes), ciphertext.as_ref())
            .map_err(|_| {
                anyhow!(
                    "failed to decrypt vault entry `{name}`; \
                     the identity file may not match the one used to encrypt it"
                )
            })?;
        let text = String::from_utf8(plaintext)
            .map_err(|_| anyhow!("vault entry `{name}` did not decrypt to valid UTF-8"))?;
        Ok(Zeroizing::new(text))
    }

    /// Encrypt `plaintext` and store it under `name`, replacing any existing
    /// entry. Does not write to disk; call [`Vault::save`] afterwards.
    pub fn set(&mut self, name: &str, plaintext: &str, key_source: &str) -> Result<()> {
        let key = self.derive_key(key_source)?;
        let cipher = XChaCha20Poly1305::new((&*key).into());
        let mut nonce = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(XNonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| anyhow!("failed to encrypt vault entry `{name}`"))?;
        self.file.entries.insert(
            name.to_string(),
            Entry {
                nonce: BASE64.encode(nonce),
                ciphertext: BASE64.encode(ciphertext),
            },
        );
        Ok(())
    }

    /// Re-encrypt every entry under a new identity file, updating the header
    /// salt and fingerprint. Decrypts with `old_key_source`, re-encrypts with
    /// `new_key_source`. Does not write to disk; call [`Vault::save`].
    pub fn rekey(&mut self, old_key_source: &str, new_key_source: &str) -> Result<()> {
        // Decrypt everything with the old key first.
        let mut plaintexts: BTreeMap<String, Zeroizing<String>> = BTreeMap::new();
        let names: Vec<String> = self.file.entries.keys().cloned().collect();
        for name in &names {
            plaintexts.insert(name.clone(), self.get(name, old_key_source)?);
        }
        // Rotate salt + fingerprint to the new identity.
        let mut salt = [0u8; SALT_LEN];
        rand::thread_rng().fill_bytes(&mut salt);
        self.file.header.salt = BASE64.encode(salt);
        self.file.header.key_fingerprint = identity_fingerprint(new_key_source)?;
        self.file.entries.clear();
        for (name, plaintext) in &plaintexts {
            self.set(name, plaintext, new_key_source)?;
        }
        Ok(())
    }

    /// Atomically write the vault to disk with 0600 permissions.
    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let serialized = toml::to_string_pretty(&self.file).context("failed to serialize vault")?;
        write_file_secure(&self.path, serialized.as_bytes())
    }

    /// Derive the 32-byte symmetric key from the identity file and stored salt,
    /// after verifying the identity matches the vault's fingerprint.
    fn derive_key(&self, key_source: &str) -> Result<Zeroizing<[u8; KEY_LEN]>> {
        let fingerprint = identity_fingerprint(key_source)?;
        if fingerprint != self.file.header.key_fingerprint {
            bail!(
                "identity file {} (fingerprint {}) does not match the one used for this vault \
                 (fingerprint {}); use `xho secret rekey` to re-encrypt under a new identity",
                key_source,
                fingerprint,
                self.file.header.key_fingerprint
            );
        }
        let salt = BASE64
            .decode(&self.file.header.salt)
            .context("vault salt is not valid base64")?;
        let ikm = identity_ikm(key_source)?;
        let hk = Hkdf::<Sha256>::new(Some(&salt), &ikm);
        let mut okm = Zeroizing::new([0u8; KEY_LEN]);
        hk.expand(HKDF_INFO, okm.as_mut())
            .map_err(|_| anyhow!("HKDF expand failed"))?;
        Ok(okm)
    }
}

/// Load a private key, returning its parsed form. Surfaces a clear error if the
/// key is passphrase-protected (the vault requires an unencrypted identity).
fn load_identity(key_source: &str) -> Result<russh::keys::PrivateKey> {
    let path = expand_tilde(key_source)?;
    load_secret_key(&path, None).with_context(|| {
        format!(
            "failed to load identity {path} for vault key derivation \
             (passphrase-protected keys are not supported)"
        )
    })
}

/// SHA256 public-key fingerprint of the identity, e.g. `SHA256:...`.
fn identity_fingerprint(key_source: &str) -> Result<String> {
    let key = load_identity(key_source)?;
    Ok(key.fingerprint(HashAlg::Sha256).to_string())
}

/// Stable, secret input keying material derived from the parsed private key.
///
/// Uses the SSH wire encoding of `KeypairData`, which contains the algorithm
/// and private scalar(s) but *not* the comment or the per-save checkint, so it
/// stays constant across re-saves of the same key.
fn identity_ikm(key_source: &str) -> Result<Zeroizing<Vec<u8>>> {
    let key = load_identity(key_source)?;
    let mut ikm: Vec<u8> = Vec::new();
    key.key_data()
        .encode(&mut ikm)
        .map_err(|e| anyhow!("failed to encode key material: {e}"))?;
    Ok(Zeroizing::new(ikm))
}

/// Write `bytes` to `path` atomically (temp file + rename) with 0600 perms.
fn write_file_secure(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("vault path has no parent directory"))?;
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("secrets")
    ));
    fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    set_owner_only(&tmp)?;
    fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to set permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_owner_only(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use russh::keys::PrivateKey;
    use russh::keys::ssh_key::LineEnding;

    /// Generate a fresh unencrypted Ed25519 private key for tests.
    fn random_key() -> PrivateKey {
        let mut rng = rand_core::UnwrapErr(getrandom::SysRng);
        PrivateKey::random(&mut rng, russh::keys::Algorithm::Ed25519).expect("generate key")
    }

    /// Write a fresh unencrypted Ed25519 key to disk, returning its path.
    fn write_test_key(dir: &Path, name: &str) -> String {
        let key = random_key();
        let path = dir.join(name);
        key.write_openssh_file(&path, LineEnding::LF)
            .expect("write key");
        path.display().to_string()
    }

    #[test]
    fn set_get_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let key = write_test_key(dir.path(), "id_ed25519");
        let vault_path = dir.path().join("secrets");

        let mut vault = Vault::open_or_init(&vault_path, &key).unwrap();
        vault.set("db", "s3cr3t", &key).unwrap();
        vault.save().unwrap();

        let reopened = Vault::open(&vault_path).unwrap();
        assert_eq!(&*reopened.get("db", &key).unwrap(), "s3cr3t");
        assert_eq!(reopened.list(), vec!["db".to_string()]);
    }

    #[test]
    fn wrong_identity_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let key = write_test_key(dir.path(), "id_a");
        let other = write_test_key(dir.path(), "id_b");
        let vault_path = dir.path().join("secrets");

        let mut vault = Vault::open_or_init(&vault_path, &key).unwrap();
        vault.set("db", "s3cr3t", &key).unwrap();
        vault.save().unwrap();

        let reopened = Vault::open(&vault_path).unwrap();
        assert!(reopened.get("db", &other).is_err());
    }

    #[test]
    fn rekey_migrates_entries() {
        let dir = tempfile::tempdir().unwrap();
        let old = write_test_key(dir.path(), "id_old");
        let new = write_test_key(dir.path(), "id_new");
        let vault_path = dir.path().join("secrets");

        let mut vault = Vault::open_or_init(&vault_path, &old).unwrap();
        vault.set("db", "s3cr3t", &old).unwrap();
        vault.rekey(&old, &new).unwrap();
        vault.save().unwrap();

        let reopened = Vault::open(&vault_path).unwrap();
        assert!(reopened.get("db", &old).is_err());
        assert_eq!(&*reopened.get("db", &new).unwrap(), "s3cr3t");
    }

    #[test]
    fn ikm_is_stable_across_resaves() {
        let dir = tempfile::tempdir().unwrap();
        let key = random_key();
        let p1 = dir.path().join("k1");
        let p2 = dir.path().join("k2");
        key.write_openssh_file(&p1, LineEnding::LF).unwrap();
        // Re-save the same key with a different comment; IKM must not change.
        let mut key2 = key.clone();
        key2.set_comment("different@comment");
        key2.write_openssh_file(&p2, LineEnding::LF).unwrap();

        let ikm1 = identity_ikm(&p1.display().to_string()).unwrap();
        let ikm2 = identity_ikm(&p2.display().to_string()).unwrap();
        assert_eq!(&*ikm1, &*ikm2);
    }
}
