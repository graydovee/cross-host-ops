use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, Mac};
use russh::MethodKind;
use russh::client::KeyboardInteractiveAuthResponse;
use russh::client::{self, AuthResult, Handle};
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, load_secret_key};
use sha1::Sha1;
use tokio::time::timeout;
use tracing::info;

use crate::config::{AppConfig, MfaConfig};

use super::{AuthPromptRequest, AuthPrompter};

type HmacSha1 = Hmac<Sha1>;

pub(super) struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

pub(super) async fn connect_handle(
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

pub(super) async fn authenticate_with_key(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    identity_file: &str,
    target_label: &str,
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
            authenticate_keyboard_interactive(handle, user, target_label, mfa, auth_prompter)
                .await?;
            info!(user = %user, "SSH keyboard-interactive MFA succeeded");
            Ok(())
        }
        _ => bail!("SSH publickey authentication failed for {}", user),
    }
}

pub(super) async fn authenticate_with_password(
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

pub fn build_remote_command(argv: &[String]) -> String {
    let mut result = String::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            result.push(' ');
        }
        result.push_str(&shell_quote(arg));
    }
    result
}

pub fn shell_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

pub(super) fn looks_like_prompt(buffer: &[u8], suffixes: &[String]) -> bool {
    let text = String::from_utf8_lossy(buffer);
    let tail = text
        .rsplit('\n')
        .next()
        .unwrap_or(text.as_ref())
        .trim_end_matches('\r');
    suffixes.iter().any(|suffix| tail.ends_with(suffix))
}

pub(super) fn extract_sentinel<'a>(
    buffer: &'a [u8],
    prefix: &[u8],
) -> Option<(i32, &'a [u8], &'a [u8])> {
    let start = find_subslice(buffer, prefix)?;
    let before = &buffer[..start];
    let after_prefix = &buffer[start + prefix.len()..];
    let status_start = after_prefix.strip_prefix(b":")?;
    let line_end = status_start
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(status_start.len());
    let line = &status_start[..line_end];
    let line = strip_trailing_cr(line);
    let status = std::str::from_utf8(line).ok()?.trim().parse::<i32>().ok()?;
    let remainder = if line_end < status_start.len() {
        &status_start[line_end + 1..]
    } else {
        &status_start[line_end..]
    };
    Some((status, before, remainder))
}

fn strip_trailing_cr(bytes: &[u8]) -> &[u8] {
    if let Some(stripped) = bytes.strip_suffix(b"\r") {
        return stripped;
    }
    bytes
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

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

async fn authenticate_keyboard_interactive(
    handle: &mut Handle<ClientHandler>,
    user: &str,
    target_label: &str,
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
                            auth_prompter(AuthPromptRequest {
                                target_label: target_label.to_string(),
                                kind: "jump_mfa".to_string(),
                                message: prompt.prompt.to_string(),
                                secret: !prompt.echo,
                            })
                            .await?
                        } else {
                            bail!("keyboard-interactive MFA requires an auth prompt handler")
                        }
                    } else if let Some(auth_prompter) = auth_prompter {
                        auth_prompter(AuthPromptRequest {
                            target_label: target_label.to_string(),
                            kind: "jump_mfa".to_string(),
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

fn generate_totp(config: &MfaConfig) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use super::{build_remote_command, extract_sentinel, shell_quote};

    #[test]
    fn shell_quotes_arguments() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn builds_remote_command() {
        let argv = vec!["echo".to_string(), "hello world".to_string()];
        assert_eq!(build_remote_command(&argv), "'echo' 'hello world'");
    }

    #[test]
    fn extracts_sentinel() {
        let input = b"hello\n__ARUN_EXIT__abc__:17\nprompt$ ";
        let (status, before, after) = extract_sentinel(input, b"__ARUN_EXIT__abc__").unwrap();
        assert_eq!(status, 17);
        assert_eq!(before, b"hello\n");
        assert_eq!(after, b"prompt$ ");
    }
}
