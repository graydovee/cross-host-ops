use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use russh::{Channel, ChannelMsg, client};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task;
use tokio::time::{Duration, timeout};
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::{
    AppConfig, JumpserverConfig, MfaConfig, SshHostEntry, parse_ssh_config, resolve_ssh_host,
};
use crate::protocol::ServerEvent;

use super::Connection;
use super::shared::{
    ClientHandler, authenticate_with_key, build_remote_command, connect_handle, extract_sentinel,
    looks_like_prompt,
};
use super::types::{CopyDirection, CopySpec, ResolvedTarget};

const EXEC_SENTINEL_PREFIX: &str = "__ARUN_EXEC__";
const COPY_SENTINEL_PREFIX: &str = "__ARUN_COPY__";
const COPY_HEREDOC_PREFIX: &str = "ARUN_COPY";

struct ShellCommandOutcome {
    exit_code: i32,
    payload: Vec<u8>,
}

pub struct JumpSshConnection {
    _handle: client::Handle<ClientHandler>,
    channel: Channel<russh::client::Msg>,
    pending: Vec<u8>,
    prompt_suffixes: Vec<String>,
    shell_timeout: Duration,
}

impl JumpSshConnection {
    pub async fn connect(target: &ResolvedTarget, config: &AppConfig) -> Result<Self> {
        let jump = merge_jumpserver_target(config)?;
        let mut handle = connect_handle(&jump.host, jump.port, config).await?;
        authenticate_with_key(
            &mut handle,
            &jump.user,
            jump.identity_file
                .as_deref()
                .ok_or_else(|| anyhow!("jumpserver identity_file is required"))?,
            Some(&jump.mfa),
            jump.pubkey_accepted_algorithms.as_deref(),
        )
        .await?;
        let channel = handle.channel_open_session().await?;
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await?;
        channel.request_shell(true).await?;

        let mut connection = Self {
            _handle: handle,
            channel,
            pending: Vec::new(),
            prompt_suffixes: jump.shell_prompt_suffixes.clone(),
            shell_timeout: config.ssh.connect_timeout,
        };
        connection
            .establish_jump_shell(target, &jump, &config.jumpserver.mfa)
            .await?;
        Ok(connection)
    }

    async fn establish_jump_shell(
        &mut self,
        target: &ResolvedTarget,
        jump: &JumpserverConfig,
        mfa: &MfaConfig,
    ) -> Result<()> {
        debug!(target_ip = %target.ip, "waiting for jumpserver shell output");
        let mut selected = false;
        let mut mfa_sent = false;
        loop {
            let chunk = self.read_shell_chunk().await?;
            self.pending.extend_from_slice(&chunk);
            let text = String::from_utf8_lossy(&self.pending);
            if !mfa_sent
                && !mfa.totp_secret_base32.is_empty()
                && text.contains(&jump.mfa_prompt_contains)
            {
                debug!(target_ip = %target.ip, "jumpserver requested MFA");
                let code = generate_totp(mfa)?;
                self.write_shell_line(&code).await?;
                self.pending.clear();
                mfa_sent = true;
                info!(target_ip = %target.ip, "jumpserver MFA completed");
                continue;
            }
            if !selected && text.contains(&jump.menu_prompt_contains) {
                debug!(target_ip = %target.ip, "jumpserver menu detected, selecting target");
                self.write_shell_line(&target.ip).await?;
                self.pending.clear();
                selected = true;
                continue;
            }
            if selected && looks_like_prompt(&self.pending, &self.prompt_suffixes) {
                debug!(target_ip = %target.ip, "remote shell prompt detected");
                break;
            }
        }
        self.pending.clear();
        self.write_shell_line("stty -echo").await?;
        self.wait_for_prompt().await?;
        self.pending.clear();
        Ok(())
    }

    async fn wait_for_prompt(&mut self) -> Result<()> {
        while !looks_like_prompt(&self.pending, &self.prompt_suffixes) {
            let chunk = self.read_shell_chunk().await?;
            self.pending.extend_from_slice(&chunk);
        }
        Ok(())
    }

    async fn write_shell_line(&mut self, line: &str) -> Result<()> {
        let payload = format!("{line}\r").into_bytes();
        self.channel.data(Cursor::new(payload)).await?;
        Ok(())
    }

    async fn read_shell_chunk(&mut self) -> Result<Vec<u8>> {
        let message = timeout(self.shell_timeout, self.channel.wait())
            .await
            .context("timed out waiting for jumpserver shell output")?;
        let Some(message) = message else {
            bail!("jumpserver shell closed unexpectedly");
        };
        match message {
            ChannelMsg::Data { data } => Ok(data.to_vec()),
            ChannelMsg::ExtendedData { data, .. } => Ok(data.to_vec()),
            ChannelMsg::Close | ChannelMsg::Eof => bail!("jumpserver shell closed unexpectedly"),
            _ => Ok(Vec::new()),
        }
    }

    fn clear_prompt_remainder(&mut self) {
        if looks_like_prompt(&self.pending, &self.prompt_suffixes) {
            self.pending.clear();
        }
    }

    async fn finish_shell_roundtrip(&mut self) -> Result<()> {
        self.wait_for_prompt().await?;
        self.pending.clear();
        Ok(())
    }

    fn make_marker(&self, prefix: &str) -> String {
        format!("{}{}__", prefix, Uuid::new_v4().simple())
    }

    fn wrap_shell_command(&self, command: &str, marker: &str) -> String {
        format!("{{ {command}; }}; status=$?; printf '\\n{marker}:%s\\n' \"$status\"")
    }

    async fn read_until_sentinel(
        &mut self,
        marker: &str,
        sender: Option<&UnboundedSender<ServerEvent>>,
    ) -> Result<ShellCommandOutcome> {
        let prefix = marker.as_bytes();
        let mut payload = Vec::new();

        loop {
            let chunk = self.read_shell_chunk().await?;
            self.pending.extend_from_slice(&chunk);
            if let Some((code, before, after)) = extract_sentinel(&self.pending, prefix) {
                if !before.is_empty() {
                    if let Some(sender) = sender {
                        let _ = sender.send(ServerEvent::Stdout {
                            data: before.to_vec(),
                        });
                    } else {
                        payload.extend_from_slice(before);
                    }
                }
                self.pending = after.to_vec();
                return Ok(ShellCommandOutcome {
                    exit_code: code,
                    payload,
                });
            }

            let keep = prefix.len() + 32;
            if self.pending.len() > keep {
                let safe_len = self.pending.len() - keep;
                let chunk = self.pending[..safe_len].to_vec();
                self.pending.drain(..safe_len);
                if let Some(sender) = sender {
                    let _ = sender.send(ServerEvent::Stdout { data: chunk });
                } else {
                    payload.extend_from_slice(&chunk);
                }
            }
        }
    }

    async fn run_shell_command_stream(
        &mut self,
        command: &str,
        sender: &UnboundedSender<ServerEvent>,
        marker_prefix: &str,
    ) -> Result<i32> {
        self.clear_prompt_remainder();
        let marker = self.make_marker(marker_prefix);
        let wrapped = self.wrap_shell_command(command, &marker);
        self.write_shell_line(&wrapped).await?;
        let outcome = self.read_until_sentinel(&marker, Some(sender)).await?;
        self.finish_shell_roundtrip().await?;
        Ok(outcome.exit_code)
    }

    async fn run_shell_command_capture(
        &mut self,
        command: &str,
        marker_prefix: &str,
    ) -> Result<ShellCommandOutcome> {
        self.clear_prompt_remainder();
        let marker = self.make_marker(marker_prefix);
        let wrapped = self.wrap_shell_command(command, &marker);
        self.write_shell_line(&wrapped).await?;
        let outcome = self.read_until_sentinel(&marker, None).await?;
        self.finish_shell_roundtrip().await?;
        Ok(outcome)
    }

    async fn run_shell_heredoc_upload(
        &mut self,
        command: &str,
        payload: &[u8],
        marker_prefix: &str,
    ) -> Result<()> {
        self.clear_prompt_remainder();
        let marker = self.make_marker(marker_prefix);
        let command = format!("{}\r", command.replace("{}", &marker));
        self.write_shell_raw(command.as_bytes()).await?;
        self.stream_shell_payload(payload).await?;
        self.write_shell_line(&marker).await?;
        self.finish_shell_roundtrip().await
    }

    async fn write_shell_raw(&mut self, payload: &[u8]) -> Result<()> {
        self.channel.data(Cursor::new(payload.to_vec())).await?;
        Ok(())
    }
}

fn validate_copy_spec(spec: &CopySpec) -> Result<()> {
    if spec.local_path.is_empty() || spec.remote_path.is_empty() {
        bail!("local_path and remote_path must not be empty");
    }
    if !spec.recursive {
        let path = Path::new(&spec.local_path);
        if matches!(spec.direction, CopyDirection::Upload) && path.is_dir() {
            bail!("copying a directory requires -r");
        }
    }
    Ok(())
}

fn upload_here_doc_command(spec: &CopySpec, marker: &str) -> String {
    if spec.recursive {
        format!(
            "base64 -d <<'{}' | tar xf - -C {}",
            marker,
            shell_single_quote(&spec.remote_path)
        )
    } else {
        format!(
            "base64 -d <<'{}' > {}",
            marker,
            shell_single_quote(&spec.remote_path)
        )
    }
}

fn download_command(spec: &CopySpec) -> Result<String> {
    if spec.recursive {
        let remote = Path::new(&spec.remote_path);
        let name = remote
            .file_name()
            .ok_or_else(|| {
                anyhow!(
                    "invalid remote path for recursive copy: {}",
                    spec.remote_path
                )
            })?
            .to_string_lossy()
            .to_string();
        let parent = remote
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_string_lossy()
            .to_string();
        Ok(format!(
            "cd {} && tar cf - {} | base64 -w 0; printf '\\n'",
            shell_single_quote(&parent),
            shell_single_quote(&name)
        ))
    } else {
        Ok(format!(
            "base64 -w 0 {} ; printf '\\n'",
            shell_single_quote(&spec.remote_path)
        ))
    }
}

async fn build_upload_payload(spec: &CopySpec) -> Result<Vec<u8>> {
    let spec = spec.clone();
    task::spawn_blocking(move || build_upload_payload_blocking(&spec))
        .await
        .map_err(|error| anyhow!("upload payload task failed: {}", error))?
}

async fn consume_download_payload(spec: &CopySpec, payload: Vec<u8>) -> Result<()> {
    let spec = spec.clone();
    task::spawn_blocking(move || consume_download_payload_blocking(&spec, payload))
        .await
        .map_err(|error| anyhow!("download payload task failed: {}", error))?
}

fn build_upload_payload_blocking(spec: &CopySpec) -> Result<Vec<u8>> {
    if spec.recursive {
        let mut child = Command::new("tar")
            .arg("cf")
            .arg("-")
            .arg("-C")
            .arg(&spec.local_path)
            .arg(".")
            .stdout(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn tar for {}", spec.local_path))?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture tar stdout"))?;
        let mut tar_bytes = Vec::new();
        stdout.read_to_end(&mut tar_bytes)?;
        let status = child.wait()?;
        if !status.success() {
            bail!("tar command failed for {}", spec.local_path);
        }
        let mut encoded = BASE64_STANDARD.encode(tar_bytes).into_bytes();
        encoded.push(b'\n');
        Ok(encoded)
    } else {
        let data = std::fs::read(&spec.local_path)
            .with_context(|| format!("failed to read {}", spec.local_path))?;
        let mut encoded = BASE64_STANDARD.encode(data).into_bytes();
        encoded.push(b'\n');
        Ok(encoded)
    }
}

fn consume_download_payload_blocking(spec: &CopySpec, payload: Vec<u8>) -> Result<()> {
    let data = BASE64_STANDARD
        .decode(payload)
        .context("failed to decode base64 download payload")?;
    if spec.recursive {
        std::fs::create_dir_all(&spec.local_path)
            .with_context(|| format!("failed to create {}", spec.local_path))?;
        let mut child = Command::new("tar")
            .arg("xf")
            .arg("-")
            .arg("-C")
            .arg(&spec.local_path)
            .stdin(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn tar extract for {}", spec.local_path))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open tar stdin"))?;
        stdin.write_all(&data)?;
        drop(stdin);
        let status = child.wait()?;
        if !status.success() {
            bail!("tar extract failed for {}", spec.local_path);
        }
        Ok(())
    } else {
        if let Some(parent) = Path::new(&spec.local_path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(&spec.local_path, data)
            .with_context(|| format!("failed to write {}", spec.local_path))?;
        Ok(())
    }
}

fn strip_trailing_newlines(mut bytes: Vec<u8>) -> Vec<u8> {
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    bytes
}

fn shell_single_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

#[tonic::async_trait]
impl Connection for JumpSshConnection {
    async fn execute(
        &mut self,
        argv: &[String],
        sender: &UnboundedSender<ServerEvent>,
        _config: &AppConfig,
    ) -> Result<i32> {
        let command = build_remote_command(argv);
        self.run_shell_command_stream(&command, sender, EXEC_SENTINEL_PREFIX)
            .await
    }

    async fn copy(&mut self, spec: &CopySpec, _config: &AppConfig) -> Result<()> {
        validate_copy_spec(spec)?;
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(spec).await,
            CopyDirection::Download => self.copy_download(spec).await,
        }
    }
}

impl JumpSshConnection {
    async fn copy_upload(&mut self, spec: &CopySpec) -> Result<()> {
        let payload = build_upload_payload(spec).await?;
        let command = upload_here_doc_command(spec, "{}");
        self.run_shell_heredoc_upload(&command, &payload, COPY_HEREDOC_PREFIX)
            .await
    }

    async fn copy_download(&mut self, spec: &CopySpec) -> Result<()> {
        let command = download_command(spec)?;
        let outcome = self
            .run_shell_command_capture(&command, COPY_SENTINEL_PREFIX)
            .await?;
        if outcome.exit_code != 0 {
            bail!(
                "remote copy command exited with status {}",
                outcome.exit_code
            );
        }
        consume_download_payload(spec, strip_trailing_newlines(outcome.payload)).await
    }

    async fn stream_shell_payload(&mut self, payload: &[u8]) -> Result<()> {
        for chunk in payload.chunks(32 * 1024) {
            self.channel.data(Cursor::new(chunk.to_vec())).await?;
        }
        Ok(())
    }
}

fn merge_jumpserver_target(config: &AppConfig) -> Result<JumpserverConfig> {
    let mut jump = config.jumpserver.clone();
    if jump.host.is_empty() {
        bail!("jumpserver is enabled but jumpserver.host is missing");
    }
    let entries = parse_ssh_config(Path::new(&config.ssh.ssh_config_path))?;
    if let Some(host_entry) = resolve_ssh_host(&entries, &jump.host) {
        apply_jumpserver_host_defaults(&mut jump, &host_entry);
    }
    if jump.identity_file.is_none() {
        bail!("jumpserver identity_file is missing");
    }
    if jump.user.is_empty() {
        bail!("jumpserver user is missing");
    }
    Ok(jump)
}

fn apply_jumpserver_host_defaults(jump: &mut JumpserverConfig, host: &SshHostEntry) {
    if jump.port == 22 {
        if let Some(port) = host.port {
            jump.port = port;
        }
    }
    if jump.user.is_empty() {
        if let Some(user) = &host.user {
            jump.user = user.clone();
        }
    }
    if jump.identity_file.is_none() {
        jump.identity_file = host.identity_file.clone();
    }
    if jump.pubkey_accepted_algorithms.is_none() {
        jump.pubkey_accepted_algorithms = host.pubkey_accepted_algorithms.clone();
    }
}

fn generate_totp(config: &MfaConfig) -> Result<String> {
    use data_encoding::BASE32_NOPAD;
    use hmac::{Hmac, Mac};
    use sha1::Sha1;
    use std::time::{SystemTime, UNIX_EPOCH};

    type HmacSha1 = Hmac<Sha1>;

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
