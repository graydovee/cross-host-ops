//! End-to-end secret indirection tests exercising the public library API:
//! store a secret in the vault, reference it from a server entry, and confirm
//! the connection-path resolver decrypts it back to the original plaintext.

use std::path::Path;

use xho::config::{
    Secret, SecretResolver, ServerDefaults, ServerHostConfig, resolve_server_entry,
};
use xho::secret::Vault;

/// Write a fresh unencrypted Ed25519 identity to `dir/name`, returning its path.
fn write_identity(dir: &Path, name: &str) -> String {
    use russh::keys::PrivateKey;
    use russh::keys::ssh_key::LineEnding;
    let mut rng = rand_core::UnwrapErr(getrandom::SysRng);
    let key = PrivateKey::random(&mut rng, russh::keys::Algorithm::Ed25519).expect("generate key");
    let path = dir.join(name);
    key.write_openssh_file(&path, LineEnding::LF)
        .expect("write key");
    path.display().to_string()
}

#[test]
fn vault_secret_resolves_on_connection_path() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_identity(dir.path(), "id_ed25519");
    let vault_path = dir.path().join("secrets");

    // Store a password in the vault.
    let mut vault = Vault::open_or_init(&vault_path, &key).unwrap();
    vault.set("server.db.password", "s3cr3t-pw", &key).unwrap();
    vault.save().unwrap();

    // A server entry that references it.
    let server = ServerHostConfig {
        host: "192.0.2.20".to_string(),
        port: Some(22),
        user: "dba".to_string(),
        identity_file: None,
        password: Some(Secret::vault("server.db.password")),
        shell: None,
    };
    let defaults = ServerDefaults::default();

    // Without a resolver (listing/validation): auth kind preserved, no decryption.
    let listed = resolve_server_entry("db", &server, &defaults, None).unwrap();
    assert_eq!(listed.auth_kind(), "password");

    // With a resolver (connection path): password decrypts to plaintext.
    let resolver = SecretResolver::new(Some(vault_path.clone()), Some(key.clone()));
    let connected = resolve_server_entry("db", &server, &defaults, Some(&resolver)).unwrap();
    match connected.auth {
        xho::config::DirectAuth::Password { password } => assert_eq!(password, "s3cr3t-pw"),
        other => panic!("expected password auth, got {other:?}"),
    }
}

#[test]
fn env_secret_resolves_without_vault() {
    // SAFETY: the variable name is unique to this test.
    unsafe { std::env::set_var("XHO_IT_SECRET_PW", "env-pw") };
    let resolver = SecretResolver::default();
    let secret = Secret::from_reference("env:XHO_IT_SECRET_PW");
    assert_eq!(&*secret.resolve(&resolver).unwrap(), "env-pw");
}

#[test]
fn file_secret_resolves_and_trims() {
    let dir = tempfile::tempdir().unwrap();
    let pw_file = dir.path().join("pw");
    std::fs::write(&pw_file, "file-pw\n").unwrap();
    let resolver = SecretResolver::default();
    let secret = Secret::from_reference(&format!("file:{}", pw_file.display()));
    assert_eq!(&*secret.resolve(&resolver).unwrap(), "file-pw");
}

#[test]
fn wrong_identity_fails_to_resolve() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_identity(dir.path(), "id_real");
    let other = write_identity(dir.path(), "id_other");
    let vault_path = dir.path().join("secrets");

    let mut vault = Vault::open_or_init(&vault_path, &key).unwrap();
    vault.set("db", "s3cr3t", &key).unwrap();
    vault.save().unwrap();

    let resolver = SecretResolver::new(Some(vault_path), Some(other));
    let secret = Secret::vault("db");
    assert!(secret.resolve(&resolver).is_err());
}
