// Gateway authentication module.
//
// Defines the AuthPrompter callback type, AuthPrompt payload struct,
// and shared SSH authentication helpers (key auth, password auth,
// known_hosts verification, TOTP generation) used by all Gateway
// implementations.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, Mac};
use russh::client::{self, AuthResult, Handle, KeyboardInteractiveAuthResponse};
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, known_hosts, load_secret_key, ssh_key};
use russh::MethodKind;
use sha1::Sha1;
use tokio::time::timeout;
use tracing::info;

use crate::config::{AppConfig, MfaConfig, default_known_hosts_path, expand_tilde};

type HmacSha1 = Hmac<Sha1>;

// ---------------------------------------------------------------------------
// AuthPrompter type alias and AuthPrompt struct
// ---------------------------------------------------------------------------

/// Callback for interactive authentication prompts.
/// Injected into Gateway at construction time.
pub type AuthPrompter = dyn Fn(AuthPrompt) -> Pin<Box<dyn Future<Output = Result<String>> + Send>>
    + Send
    + Sync;

/// Authentication prompt payload.
#[derive(Clone, Debug)]
pub struct AuthPrompt {
    /// Which gateway is requesting authentication.
    pub gateway_name: String,
    /// Human-readable prompt message.
    pub message: String,
    /// Whether the input should be hidden (password, MFA code).
    pub secret: bool,
}

// ---------------------------------------------------------------------------
// Known-hosts inspection and trust
// ---------------------------------------------------------------------------

/// The result of checking the known_hosts file for a server's host key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KnownHostState {
    /// The host key matches a previously-recorded entry.
    Known,
    /// The host key has never been seen before.
    Unknown {
        algorithm: String,
        fingerprint: String,
    },
    /// The host key differs from the previously-recorded entry (MITM warning).
    Changed {
        algorithm: String,
        fingerprint: String,
    },
}

/// Inspect the known_hosts file for a given host+port and public key.
pub fn inspect_known_host(
    host: &str,
    port: u16,
    public_key: &ssh_key::PublicKey,
    path: &Path,
) -> KnownHostState {
    match known_hosts::check_known_hosts_path(host, port, public_key, path) {
        Ok(true) => KnownHostState::Known,
        Ok(false) => KnownHostState::Unknown {
            algorithm: public_key.algorithm().to_string(),
            fingerprint: public_key.fingerprint(HashAlg::Sha256).to_string(),
        },
        Err(russh::keys::Error::KeyChanged { .. }) => KnownHostState::Changed {
            algorithm: public_key.algorithm().to_string(),
            fingerprint: public_key.fingerprint(HashAlg::Sha256).to_string(),
        },
        Err(_) => KnownHostState::Unknown {
            algorithm: public_key.algorithm().to_string(),
            fingerprint: public_key.fingerprint(HashAlg::Sha256).to_string(),
        },
    }
}

/// Record a host key in the known_hosts file so future connections are trusted.
pub fn trust_known_host(
    host: &str,
    port: u16,
    public_key: &ssh_key::PublicKey,
    path: &Path,
) -> Result<()> {
    known_hosts::learn_known_hosts_path(host, port, public_key, path)
        .map_err(|error| anyhow!("failed to write known_hosts: {}", error))
}

/// Normalize identity_file and known_hosts_path, expanding ~ to the user's home.
/// Falls back to `~/.ssh/id_ed25519` and the default rhop known_hosts path.
pub fn normalize_paths(
    identity_file: Option<&str>,
    known_hosts_path: Option<&str>,
) -> Result<(String, String)> {
    let identity_file = expand_tilde(identity_file.unwrap_or("~/.ssh/id_ed25519"))?;
    let known_hosts_default = default_known_hosts_path().display().to_string();
    let known_hosts_path = expand_tilde(known_hosts_path.unwrap_or(&known_hosts_default))?;
    Ok((identity_file, known_hosts_path))
}

// ---------------------------------------------------------------------------
// SSH client handler (accepts all host keys — verification done externally)
// ---------------------------------------------------------------------------

/// Minimal SSH client handler that accepts all host keys.
/// Known-hosts verification is done separately before or after the handshake.
pub(crate) struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// SSH connection establishment
// ---------------------------------------------------------------------------

/// Open an SSH connection to host:port using the given AppConfig for timeout
/// and keepalive settings. Returns an opaque client handle.
pub(crate) async fn connect_handle(
    host: &str,
    port: u16,
    config: &AppConfig,
) -> Result<Handle<ClientHandler>> {
    let client_config = client::Config {
        inactivity_timeout: Some(config.ssh.keepalive_interval * 2),
        ..Default::default()
    };
    let handle = timeout(
        config.ssh.connect_timeout,
        client::connect(Arc::new(client_config), (host, port), ClientHandler),
    )
    .await
    .context("timed out opening SSH connection")??;
    Ok(handle)
}

// ---------------------------------------------------------------------------
// SSH authentication helpers
// ---------------------------------------------------------------------------

/// Authenticate using a public key. If partial success with
/// keyboard-interactive remaining (common for MFA), continue with
/// keyboard-interactive flow using the MFA config or AuthPrompter.
pub(crate) async fn authenticate_with_key(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    identity_file: &str,
    gateway_name: &str,
    mfa: Option<&MfaConfig>,
    pubkey_accepted_algorithms: Option<&str>,
    auth_prompter: Option<&AuthPrompter>,
) -> Result<()> {
    let key = load_secret_key(identity_file, None)
        .with_context(|| format!("failed to load key {}", identity_file))?;
    let hash_alg = preferred_rsa_hash(pubkey_accepted_algorithms, handle).await?;
    let auth = handle
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg))
        .await?;
    if auth.success() {
        return Ok(());
    }
    match auth {
        AuthResult::Failure {
            remaining_methods,
            partial_success,
        } if partial_success && remaining_methods.contains(&MethodKind::KeyboardInteractive) => {
            authenticate_keyboard_interactive(handle, user, gateway_name, mfa, auth_prompter)
                .await?;
            info!(user = %user, "SSH keyboard-interactive MFA succeeded");
            Ok(())
        }
        _ => bail!("SSH publickey authentication failed for {}", user),
    }
}

/// Authenticate using a plaintext password.
pub(crate) async fn authenticate_with_password(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    password: &str,
) -> Result<()> {
    let auth = handle.authenticate_password(user, password).await?;
    if auth.success() {
        return Ok(());
    }
    bail!("SSH password authentication failed for {}", user)
}

// ---------------------------------------------------------------------------
// Keyboard-interactive (MFA) flow
// ---------------------------------------------------------------------------

async fn authenticate_keyboard_interactive(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    gateway_name: &str,
    mfa: Option<&MfaConfig>,
    auth_prompter: Option<&AuthPrompter>,
) -> Result<()> {
    let mut reply = handle
        .authenticate_keyboard_interactive_start(user, Option::<String>::None)
        .await?;
    loop {
        match reply {
            KeyboardInteractiveAuthResponse::Success => return Ok(()),
            KeyboardInteractiveAuthResponse::Failure { .. } => {
                bail!(
                    "SSH keyboard-interactive authentication failed for {}",
                    user
                )
            }
            KeyboardInteractiveAuthResponse::InfoRequest { prompts, .. } => {
                let mut responses = Vec::with_capacity(prompts.len());
                for prompt in prompts {
                    let response = if let Some(mfa) = mfa {
                        if !mfa.totp_secret_base32.is_empty() {
                            generate_totp(mfa)?
                        } else if let Some(auth_prompter) = auth_prompter {
                            auth_prompter(AuthPrompt {
                                gateway_name: gateway_name.to_string(),
                                message: prompt.prompt.to_string(),
                                secret: !prompt.echo,
                            })
                            .await?
                        } else {
                            bail!("keyboard-interactive MFA requires an auth prompt handler")
                        }
                    } else if let Some(auth_prompter) = auth_prompter {
                        auth_prompter(AuthPrompt {
                            gateway_name: gateway_name.to_string(),
                            message: prompt.prompt.to_string(),
                            secret: !prompt.echo,
                        })
                        .await?
                    } else {
                        bail!("keyboard-interactive MFA requires an auth prompt handler")
                    };
                    responses.push(response);
                }
                reply = handle
                    .authenticate_keyboard_interactive_respond(responses)
                    .await?;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TOTP generation
// ---------------------------------------------------------------------------

/// Generate a TOTP code from the given MFA configuration.
/// Only SHA-1 digest is supported (standard TOTP per RFC 6238).
pub fn generate_totp(config: &MfaConfig) -> Result<String> {
    if config.digest.to_ascii_lowercase() != "sha1" {
        bail!("only sha1 TOTP is supported");
    }
    let secret = BASE32_NOPAD
        .decode(config.totp_secret_base32.as_bytes())
        .context("invalid base32 TOTP secret")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?;
    let counter = now.as_secs() / config.period;
    let mut message = [0u8; 8];
    message.copy_from_slice(&counter.to_be_bytes());
    let mut mac = HmacSha1::new_from_slice(&secret)?;
    mac.update(&message);
    let digest = mac.finalize().into_bytes();
    let offset = (digest[digest.len() - 1] & 0x0f) as usize;
    let value = ((u32::from(digest[offset]) & 0x7f) << 24)
        | (u32::from(digest[offset + 1]) << 16)
        | (u32::from(digest[offset + 2]) << 8)
        | u32::from(digest[offset + 3]);
    let modulo = 10_u32.pow(config.digits);
    Ok(format!(
        "{:0width$}",
        value % modulo,
        width = config.digits as usize
    ))
}

// ---------------------------------------------------------------------------
// RSA hash negotiation helper
// ---------------------------------------------------------------------------

async fn preferred_rsa_hash(
    pubkey_accepted_algorithms: Option<&str>,
    handle: &Handle<ClientHandler>,
) -> Result<Option<HashAlg>> {
    if wants_legacy_ssh_rsa(pubkey_accepted_algorithms) {
        return Ok(None);
    }
    Ok(handle.best_supported_rsa_hash().await?.flatten())
}

fn wants_legacy_ssh_rsa(pubkey_accepted_algorithms: Option<&str>) -> bool {
    let Some(value) = pubkey_accepted_algorithms else {
        return false;
    };
    value
        .split(',')
        .map(str::trim)
        .any(|item| item == "ssh-rsa" || item == "+ssh-rsa")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_valid_totp_code() {
        let config = MfaConfig {
            totp_secret_base32: "JBSWY3DPEHPK3PXP".to_string(),
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        };
        let code = generate_totp(&config).unwrap();
        assert_eq!(code.len(), 6);
        assert!(code.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn totp_rejects_non_sha1_digest() {
        let config = MfaConfig {
            totp_secret_base32: "JBSWY3DPEHPK3PXP".to_string(),
            digits: 6,
            period: 30,
            digest: "sha256".to_string(),
        };
        let result = generate_totp(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("sha1"));
    }

    #[test]
    fn totp_rejects_invalid_base32() {
        let config = MfaConfig {
            totp_secret_base32: "!!!invalid!!!".to_string(),
            digits: 6,
            period: 30,
            digest: "sha1".to_string(),
        };
        let result = generate_totp(&config);
        assert!(result.is_err());
    }

    #[test]
    fn normalize_paths_defaults() {
        let (id_file, kh_path) = normalize_paths(None, None).unwrap();
        assert!(id_file.contains(".ssh/id_ed25519"));
        assert!(!kh_path.is_empty());
    }

    #[test]
    fn normalize_paths_with_custom_values() {
        let (id_file, kh_path) =
            normalize_paths(Some("/tmp/my_key"), Some("/tmp/known_hosts")).unwrap();
        assert_eq!(id_file, "/tmp/my_key");
        assert_eq!(kh_path, "/tmp/known_hosts");
    }
}
