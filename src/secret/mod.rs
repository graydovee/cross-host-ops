//! Local encrypted secret vault.
//!
//! The vault stores credentials encrypted at rest in a TOML file
//! (`~/.xho/secrets`, mode 0600). The symmetric key is *derived* from an SSH
//! identity file rather than stored separately:
//!
//! 1. The private key is parsed (not the raw file bytes — that would include
//!    the per-save random checkint and the public comment, both unstable).
//! 2. Its `KeypairData` is SSH-encoded to obtain stable, secret input keying
//!    material (IKM). The comment is *not* part of this encoding.
//! 3. HKDF-SHA256(salt, IKM, info) derives a 32-byte XChaCha20-Poly1305 key.
//!
//! The salt and the identity's public-key fingerprint are stored in the vault
//! header. The fingerprint lets us detect "wrong identity file" up front and
//! emit a clear error instead of an opaque AEAD failure.
//!
//! Security boundary: the derived key and the ciphertext live on the same
//! host. Anyone who can read the identity file can decrypt the vault — but
//! that same person can already use the key to authenticate directly, so this
//! does not widen the attack surface. It protects against accidental git
//! commits, backups, and shoulder-surfing, not against a compromised host.

mod vault;

pub use vault::Vault;
