use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use russh::client;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task;
use tracing::{debug, info};

use crate::config::{
    AppConfig, JumpserverConfig, MfaConfig, SshHostEntry, parse_ssh_config, resolve_ssh_host,
};
use crate::protocol::ServerEvent;

use super::{AuthPromptRequest, AuthPrompter, Connection};
use super::shared::{
    ClientHandler, PtyShell, authenticate_with_key, build_interactive_shell_command,
    connect_handle, join_remote_path, maybe_local_download_target,
    remote_path_needs_expansion, request_default_pty, split_tilde_path,
    upload_destination_for_directory,
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
    shell: PtyShell,
}

impl JumpSshConnection {
    pub async fn connect(
        target: &ResolvedTarget,
        config: &AppConfig,
        auth_prompter: &AuthPrompter,
    ) -> Result<Self> {
        let jump = merge_jumpserver_target(config)?;
        let mut handle = connect_handle(&jump.host, jump.port, config).await?;
        authenticate_with_key(
            &mut handle,
            &jump.user,
            jump.identity_file
                .as_deref()
                .ok_or_else(|| anyhow!("jumpserver identity_file is required"))?,
            &target.target_label,
            Some(&jump.mfa),
            jump.pubkey_accepted_algorithms.as_deref(),
            Some(auth_prompter),
        )
        .await?;
        let channel = handle.channel_open_session().await?;
        request_default_pty(&channel).await?;
        let shell = PtyShell::new(
            channel,
            jump.shell_prompt_suffixes.clone(),
            config.ssh.connect_timeout,
        );
        let mut connection = Self { _handle: handle, shell };
        connection.shell.request_shell().await?;
        connection
            .establish_jump_shell(target, &jump, &config.jumpserver.mfa, auth_prompter)
            .await?;
        Ok(connection)
    }

    async fn establish_jump_shell(
        &mut self,
        target: &ResolvedTarget,
        jump: &JumpserverConfig,
        mfa: &MfaConfig,
        auth_prompter: &AuthPrompter,
    ) -> Result<()> {
        debug!(target_ip = %target.ip, "waiting for jumpserver shell output");
        let mut selected = false;
        let mut mfa_sent = false;
        loop {
            let chunk = self.shell.read_chunk().await?;
            self.shell.extend_pending(&chunk);
            let text = self.shell.pending_text();
            if !mfa_sent
                && text.contains(&jump.mfa_prompt_contains)
            {
                debug!(target_ip = %target.ip, "jumpserver requested MFA");
                let code = if !mfa.totp_secret_base32.is_empty() {
                    generate_totp(mfa)?
                } else {
                    auth_prompter(AuthPromptRequest {
                        target_label: target.target_label.clone(),
                        kind: "jump_mfa".to_string(),
                        message: format!(
                            "jumpserver requested MFA for {}",
                            target.target_label
                        ),
                        secret: true,
                    })
                    .await?
                };
                self.shell.write_line(&code).await?;
                self.shell.clear_pending();
                mfa_sent = true;
                info!(target_ip = %target.ip, "jumpserver MFA completed");
                continue;
            }
            if !selected && text.contains(&jump.menu_prompt_contains) {
                debug!(target_ip = %target.ip, "jumpserver menu detected, selecting target");
                self.shell.write_line(&target.ip).await?;
                self.shell.clear_pending();
                selected = true;
                continue;
            }
            if selected && self.shell.pending_has_prompt() {
                debug!(target_ip = %target.ip, "remote shell prompt detected");
                break;
            }
        }
        self.shell.clear_pending();
        self.shell.write_line("stty -echo").await?;
        self.shell.wait_for_prompt().await?;
        self.shell.clear_pending();
        Ok(())
    }

    async fn run_shell_command_stream(
        &mut self,
        command: &str,
        sender: &UnboundedSender<ServerEvent>,
        marker_prefix: &str,
    ) -> Result<i32> {
        self.shell.clear_prompt_remainder();
        let marker = self.shell.make_marker(marker_prefix);
        let wrapped = self.shell.wrap_shell_command(command, &marker);
        self.shell.write_line(&wrapped).await?;
        let (status, _) = self.shell.read_until_sentinel(&marker, Some(sender)).await?;
        self.shell.finish_roundtrip().await?;
        Ok(status)
    }

    async fn run_shell_command_capture(
        &mut self,
        command: &str,
        marker_prefix: &str,
    ) -> Result<ShellCommandOutcome> {
        self.shell.clear_prompt_remainder();
        let marker = self.shell.make_marker(marker_prefix);
        let wrapped = self.shell.wrap_shell_command(command, &marker);
        self.shell.write_line(&wrapped).await?;
        let (exit_code, payload) = self.shell.read_until_sentinel(&marker, None).await?;
        self.shell.finish_roundtrip().await?;
        Ok(ShellCommandOutcome { exit_code, payload })
    }

    async fn run_shell_heredoc_upload(
        &mut self,
        command: &str,
        payload: &[u8],
        marker_prefix: &str,
    ) -> Result<()> {
        self.shell.clear_prompt_remainder();
        let marker = self.shell.make_marker(marker_prefix);
        let command = format!("{}\r", command.replace("{}", &marker));
        self.shell.write_raw(command.as_bytes()).await?;
        self.stream_shell_payload(payload).await?;
        self.shell.write_line(&marker).await?;
        self.shell.finish_roundtrip().await
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
        let command = build_interactive_shell_command(argv);
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
        let local = Path::new(&spec.local_path);
        let remote_path = self
            .normalize_remote_upload_path(spec, local)
            .await?;
        let mut spec = spec.clone();
        spec.remote_path = remote_path;
        let payload = build_upload_payload(&spec).await?;
        let command = upload_here_doc_command(&spec, "{}");
        self.run_shell_heredoc_upload(&command, &payload, COPY_HEREDOC_PREFIX)
            .await
    }

    async fn copy_download(&mut self, spec: &CopySpec) -> Result<()> {
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        let local_path = maybe_local_download_target(Path::new(&spec.local_path), &remote_path)?;
        let mut spec = spec.clone();
        spec.remote_path = remote_path;
        spec.local_path = local_path;
        let command = download_command(&spec)?;
        let outcome = self
            .run_shell_command_capture(&command, COPY_SENTINEL_PREFIX)
            .await?;
        if outcome.exit_code != 0 {
            bail!(
                "remote copy command exited with status {}",
                outcome.exit_code
            );
        }
        consume_download_payload(&spec, strip_trailing_newlines(outcome.payload)).await
    }

    async fn stream_shell_payload(&mut self, payload: &[u8]) -> Result<()> {
        for chunk in payload.chunks(32 * 1024) {
            self.shell.write_raw(chunk).await?;
        }
        Ok(())
    }

    async fn expand_remote_copy_path(&mut self, remote_path: &str) -> Result<String> {
        if !remote_path_needs_expansion(remote_path) {
            return Ok(remote_path.to_string());
        }
        let (user, suffix) = split_tilde_path(remote_path)
            .ok_or_else(|| anyhow!("invalid remote path {}", remote_path))?;
        let home = match user {
            Some(user) => self.remote_home_for_user(user).await?,
            None => self.remote_home_for_current_user().await?,
        };
        Ok(join_remote_path(&home, suffix))
    }

    async fn normalize_remote_upload_path(
        &mut self,
        spec: &CopySpec,
        local_path: &Path,
    ) -> Result<String> {
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        if spec.recursive {
            return Ok(remote_path);
        }
        if self.remote_path_is_dir(&remote_path).await? {
            return upload_destination_for_directory(local_path, &remote_path);
        }
        Ok(remote_path)
    }

    async fn remote_path_is_dir(&mut self, remote_path: &str) -> Result<bool> {
        let command = format!(
            "test -d {}",
            shell_single_quote(remote_path)
        );
        let outcome = self
            .run_shell_command_capture(&command, COPY_SENTINEL_PREFIX)
            .await?;
        Ok(outcome.exit_code == 0)
    }

    async fn remote_home_for_current_user(&mut self) -> Result<String> {
        let home = self.capture_simple_stdout("printf %s \"$HOME\"").await?;
        if !home.is_empty() && home.starts_with('/') {
            return Ok(home);
        }
        self.capture_simple_stdout("getent passwd \"$(id -un)\" | cut -d: -f6")
            .await
    }

    async fn remote_home_for_user(&mut self, user: &str) -> Result<String> {
        self.capture_simple_stdout(&format!(
            "getent passwd {} | cut -d: -f6",
            shell_single_quote(user)
        ))
        .await
    }

    async fn capture_simple_stdout(&mut self, command: &str) -> Result<String> {
        let outcome = self
            .run_shell_command_capture(command, COPY_SENTINEL_PREFIX)
            .await?;
        let output = String::from_utf8_lossy(&strip_trailing_newlines(outcome.payload))
            .trim()
            .to_string();
        if outcome.exit_code != 0 || output.is_empty() || !output.starts_with('/') {
            bail!("failed to resolve remote path via `{}`", command);
        }
        Ok(output)
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
