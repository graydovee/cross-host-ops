use std::io::Cursor;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, Mac};
use russh::MethodKind;
use russh::ChannelMsg;
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

pub fn build_interactive_shell_command(argv: &[String]) -> String {
    let mut result = String::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            result.push(' ');
        }
        if index == 0 && is_safe_shell_command_word(arg) {
            result.push_str(arg);
        } else {
            result.push_str(&shell_quote(arg));
        }
    }
    result
}

fn is_safe_shell_command_word(arg: &str) -> bool {
    !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | '+'))
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

pub(super) const DEFAULT_PTY_TERM: &str = "xterm";
pub(super) const DEFAULT_PTY_COLS: u32 = 80;
pub(super) const DEFAULT_PTY_ROWS: u32 = 24;

pub(super) async fn request_default_pty(
    channel: &russh::Channel<russh::client::Msg>,
) -> Result<()> {
    channel
        .request_pty(
            true,
            DEFAULT_PTY_TERM,
            DEFAULT_PTY_COLS,
            DEFAULT_PTY_ROWS,
            0,
            0,
            &[],
        )
        .await?;
    Ok(())
}

pub(super) struct PtyShell {
    channel: russh::Channel<russh::client::Msg>,
    pending: Vec<u8>,
    prompt_suffixes: Vec<String>,
    shell_timeout: std::time::Duration,
}

impl PtyShell {
    pub(super) fn new(
        channel: russh::Channel<russh::client::Msg>,
        prompt_suffixes: Vec<String>,
        shell_timeout: std::time::Duration,
    ) -> Self {
        Self {
            channel,
            pending: Vec::new(),
            prompt_suffixes,
            shell_timeout,
        }
    }

    pub(super) async fn request_shell(&self) -> Result<()> {
        self.channel.request_shell(true).await?;
        Ok(())
    }

    pub(super) async fn wait_for_prompt(&mut self) -> Result<()> {
        while !looks_like_prompt(&self.pending, &self.prompt_suffixes) {
            let chunk = self.read_chunk().await?;
            self.pending.extend_from_slice(&chunk);
        }
        Ok(())
    }

    pub(super) fn pending_text(&self) -> String {
        String::from_utf8_lossy(&self.pending).to_string()
    }

    pub(super) fn pending_has_prompt(&self) -> bool {
        looks_like_prompt(&self.pending, &self.prompt_suffixes)
    }

    pub(super) fn clear_pending(&mut self) {
        self.pending.clear();
    }

    pub(super) fn extend_pending(&mut self, chunk: &[u8]) {
        self.pending.extend_from_slice(chunk);
    }

    pub(super) fn clear_prompt_remainder(&mut self) {
        if looks_like_prompt(&self.pending, &self.prompt_suffixes) {
            self.pending.clear();
        }
    }

    pub(super) async fn finish_roundtrip(&mut self) -> Result<()> {
        self.wait_for_prompt().await?;
        self.pending.clear();
        Ok(())
    }

    pub(super) async fn write_line(&mut self, line: &str) -> Result<()> {
        let payload = format!("{line}\r").into_bytes();
        self.channel.data(Cursor::new(payload)).await?;
        Ok(())
    }

    pub(super) async fn write_raw(&mut self, payload: &[u8]) -> Result<()> {
        self.channel.data(Cursor::new(payload.to_vec())).await?;
        Ok(())
    }

    pub(super) async fn read_chunk(&mut self) -> Result<Vec<u8>> {
        let message = timeout(self.shell_timeout, self.channel.wait())
            .await
            .context("timed out waiting for shell output")?;
        let Some(message) = message else {
            bail!("shell closed unexpectedly");
        };
        match message {
            ChannelMsg::Data { data } => Ok(data.to_vec()),
            ChannelMsg::ExtendedData { data, .. } => Ok(data.to_vec()),
            ChannelMsg::Close | ChannelMsg::Eof => bail!("shell closed unexpectedly"),
            _ => Ok(Vec::new()),
        }
    }

    pub(super) fn make_marker(&self, prefix: &str) -> String {
        format!("{}{}__", prefix, uuid::Uuid::new_v4().simple())
    }

    pub(super) fn wrap_shell_command(&self, command: &str, marker: &str) -> String {
        format!("{{ {command}; }}; status=$?; printf '{marker}:%s\\n' \"$status\"")
    }

    pub(super) async fn read_until_sentinel(
        &mut self,
        marker: &str,
        sender: Option<&tokio::sync::mpsc::UnboundedSender<crate::protocol::ServerEvent>>,
    ) -> Result<(i32, Vec<u8>)> {
        let prefix = marker.as_bytes();
        let mut payload = Vec::new();
        let mut first_output = true;

        loop {
            let chunk = self.read_chunk().await?;
            self.pending.extend_from_slice(&chunk);
            if let Some((code, before, after)) = extract_sentinel(&self.pending, prefix) {
                let before = if first_output {
                    strip_leading_shell_noise(before)
                } else {
                    before
                };
                if !before.is_empty() {
                    if let Some(sender) = sender {
                        let _ = sender.send(crate::protocol::ServerEvent::Stdout {
                            data: before.to_vec(),
                        });
                    } else {
                        payload.extend_from_slice(before);
                    }
                }
                self.pending = after.to_vec();
                return Ok((code, payload));
            }

            let keep = prefix.len() + 32;
            if self.pending.len() > keep {
                let safe_len = self.pending.len() - keep;
                let chunk = if first_output {
                    first_output = false;
                    strip_leading_shell_noise(&self.pending[..safe_len]).to_vec()
                } else {
                    self.pending[..safe_len].to_vec()
                };
                self.pending.drain(..safe_len);
                if !chunk.is_empty() {
                    if let Some(sender) = sender {
                        let _ = sender.send(crate::protocol::ServerEvent::Stdout { data: chunk });
                    } else {
                        payload.extend_from_slice(&chunk);
                    }
                }
            }
        }
    }
}

fn strip_leading_shell_noise(bytes: &[u8]) -> &[u8] {
    let mut index = 0;
    loop {
        while index < bytes.len() && matches!(bytes[index], b'\r' | b'\n') {
            index += 1;
        }
        if let Some(next) = skip_leading_ansi_escape(bytes, index) {
            index = next;
            continue;
        }
        break;
    }
    &bytes[index..]
}

fn skip_leading_ansi_escape(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&0x1b) {
        return None;
    }
    match bytes.get(start + 1) {
        Some(b'[') => {
            let mut index = start + 2;
            while let Some(byte) = bytes.get(index) {
                if (0x40..=0x7e).contains(byte) {
                    return Some(index + 1);
                }
                index += 1;
            }
            None
        }
        Some(b']') => {
            let mut index = start + 2;
            while let Some(byte) = bytes.get(index) {
                if *byte == 0x07 {
                    return Some(index + 1);
                }
                if *byte == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
                    return Some(index + 2);
                }
                index += 1;
            }
            None
        }
        _ => None,
    }
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
