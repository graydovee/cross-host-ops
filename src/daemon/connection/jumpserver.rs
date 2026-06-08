// JumpserverConnection implementation.
// Wraps a PtyShell to execute commands through an interactive jumpserver shell.
// Implements the Connection trait using sentinel-based exit code extraction.

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::protocol::ServerEvent;
use crate::types::{CopyDirection, CopyFrame, CopySpec};

use super::shared::{PtyShell, build_interactive_shell_command};
use super::{Connection, ExecRequest, InteractiveHandle, InteractiveRequest};

const EXEC_SENTINEL_PREFIX: &str = "__ARUN_EXEC__";
const COPY_SENTINEL_PREFIX: &str = "__ARUN_COPY__";
const COPY_HEREDOC_PREFIX: &str = "ARUN_COPY";

/// Outcome of a captured shell command (exit code + raw output bytes).
struct ShellCommandOutcome {
    exit_code: i32,
    payload: Vec<u8>,
}

/// A connection to an end target through an interactive jumpserver PTY shell.
/// Commands are executed by writing to the shell and parsing sentinel markers
/// to extract exit codes and output.
pub(crate) struct JumpserverConnection {
    shell: Option<PtyShell>,
}

/// A borrowed variant of JumpserverConnection that operates on a PtyShell
/// owned by the JumpserverGateway. This avoids moving the shell out of the
/// gateway's state while still allowing command execution.
pub(crate) struct BorrowedJumpserverConnection<'a> {
    shell: &'a mut PtyShell,
}

impl JumpserverConnection {
    /// Create a new JumpserverConnection from an already-navigated PtyShell.
    /// The shell should have completed menu navigation and be at a command prompt
    /// on the target host.
    pub(crate) fn new(shell: PtyShell) -> Self {
        Self { shell: Some(shell) }
    }

    /// Create a borrowed variant that operates on a PtyShell reference.
    pub(crate) fn new_borrowed(shell: &mut PtyShell) -> BorrowedJumpserverConnection<'_> {
        BorrowedJumpserverConnection { shell }
    }

    /// Execute a command, streaming stdout to the sender, and return the exit code.
    async fn run_shell_command_stream(
        &mut self,
        command: &str,
        sender: &mpsc::UnboundedSender<ServerEvent>,
        marker_prefix: &str,
    ) -> Result<i32> {
        let shell = self.shell_mut()?;
        shell.clear_prompt_remainder();
        let marker = shell.make_marker(marker_prefix);
        let wrapped = shell.wrap_shell_command(command, &marker);
        shell.write_line(&wrapped).await?;
        let (status, _) = shell.read_until_sentinel(&marker, Some(sender)).await?;
        shell.finish_roundtrip().await?;
        Ok(status)
    }

    /// Execute a command, capturing stdout into a buffer, and return exit code + output.
    async fn run_shell_command_capture(
        &mut self,
        command: &str,
        marker_prefix: &str,
    ) -> Result<ShellCommandOutcome> {
        let shell = self.shell_mut()?;
        shell.clear_prompt_remainder();
        let marker = shell.make_marker(marker_prefix);
        let wrapped = shell.wrap_shell_command(command, &marker);
        shell.write_line(&wrapped).await?;
        let (exit_code, payload) = shell.read_until_sentinel(&marker, None).await?;
        shell.finish_roundtrip().await?;
        Ok(ShellCommandOutcome { exit_code, payload })
    }

    /// Upload data via a here-document command through the PTY shell.
    async fn run_shell_heredoc_upload(
        &mut self,
        command: &str,
        payload: &[u8],
        marker_prefix: &str,
    ) -> Result<()> {
        let shell = self.shell_mut()?;
        shell.clear_prompt_remainder();
        let marker = shell.make_marker(marker_prefix);
        let command = format!("{}\r", command.replace("{}", &marker));
        shell.write_raw(command.as_bytes()).await?;
        self.stream_shell_payload(payload).await?;
        let shell = self.shell_mut()?;
        shell.write_line(&marker).await?;
        shell.finish_roundtrip().await
    }

    /// Stream a payload to the PTY shell in chunks.
    async fn stream_shell_payload(&mut self, payload: &[u8]) -> Result<()> {
        let shell = self.shell_mut()?;
        for chunk in payload.chunks(32 * 1024) {
            shell.write_raw(chunk).await?;
        }
        Ok(())
    }

    /// Upload files via base64-encoded heredoc through the PTY shell.
    async fn copy_upload(&mut self, spec: &mut CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let remote_path = self.normalize_remote_upload_path(spec).await?;
        spec.remote_path = remote_path;
        let payload = build_upload_payload_from_frames(spec).await?;
        let command = upload_here_doc_command(spec, "{}");
        self.run_shell_heredoc_upload(&command, &payload, COPY_HEREDOC_PREFIX)
            .await
    }

    /// Download files via base64-encoded output through the PTY shell.
    async fn copy_download(&mut self, spec: &mut CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        spec.remote_path = remote_path;
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
        let payload = strip_trailing_newlines(outcome.payload);
        send_download_payload_as_frames(spec, payload).await
    }

    /// Expand ~ paths on the remote by executing shell commands.
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

    /// Normalize upload path, checking if remote path is a directory.
    async fn normalize_remote_upload_path(&mut self, spec: &CopySpec) -> Result<String> {
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        if spec.recursive {
            return Ok(remote_path);
        }
        if self.remote_path_is_dir(&remote_path).await? {
            return Ok(format!(
                "{}/{}",
                remote_path.trim_end_matches('/'),
                shell_path_basename_or(&spec.source_name, "copy")
            ));
        }
        Ok(remote_path)
    }

    /// Check if a remote path is a directory.
    async fn remote_path_is_dir(&mut self, remote_path: &str) -> Result<bool> {
        let command = format!("test -d {}", shell_single_quote(remote_path));
        let outcome = self
            .run_shell_command_capture(&command, COPY_SENTINEL_PREFIX)
            .await?;
        Ok(outcome.exit_code == 0)
    }

    /// Get the home directory for the current user on the remote.
    async fn remote_home_for_current_user(&mut self) -> Result<String> {
        let home = self.capture_simple_stdout("printf %s \"$HOME\"").await?;
        if !home.is_empty() && home.starts_with('/') {
            return Ok(home);
        }
        self.capture_simple_stdout("getent passwd \"$(id -un)\" | cut -d: -f6")
            .await
    }

    /// Get the home directory for a specific user on the remote.
    async fn remote_home_for_user(&mut self, user: &str) -> Result<String> {
        self.capture_simple_stdout(&format!(
            "getent passwd {} | cut -d: -f6",
            shell_single_quote(user)
        ))
        .await
    }

    /// Run a command and capture its stdout as a string.
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

    fn shell_mut(&mut self) -> Result<&mut PtyShell> {
        self.shell
            .as_mut()
            .ok_or_else(|| anyhow!("jumpserver shell has been moved"))
    }
}

#[async_trait::async_trait]
impl Connection for JumpserverConnection {
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32> {
        // Jumpserver connections always operate through an interactive PTY shell.
        // Build the command using interactive shell formatting (first word unquoted
        // for alias expansion).
        let command = build_interactive_shell_command(&request.argv);
        self.run_shell_command_stream(&command, &request.sender, EXEC_SENTINEL_PREFIX)
            .await
    }

    async fn copy(&mut self, mut spec: CopySpec) -> Result<()> {
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(&mut spec).await,
            CopyDirection::Download => self.copy_download(&mut spec).await,
        }
    }

    async fn exec_interactive(
        &mut self,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle> {
        let command = build_interactive_shell_command(&request.argv);
        let shell = self
            .shell
            .take()
            .ok_or_else(|| anyhow!("jumpserver shell has been moved"))?;
        shell
            .into_interactive_command(command, EXEC_SENTINEL_PREFIX)
            .await
    }

    fn is_alive(&self) -> bool {
        self.shell
            .as_ref()
            .map(|shell| shell.is_channel_open())
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// BorrowedJumpserverConnection — operates on a &mut PtyShell
// ---------------------------------------------------------------------------

impl<'a> BorrowedJumpserverConnection<'a> {
    /// Execute a command, streaming stdout to the sender, and return the exit code.
    async fn run_shell_command_stream(
        &mut self,
        command: &str,
        sender: &mpsc::UnboundedSender<ServerEvent>,
        marker_prefix: &str,
    ) -> Result<i32> {
        self.shell.clear_prompt_remainder();
        let marker = self.shell.make_marker(marker_prefix);
        let wrapped = self.shell.wrap_shell_command(command, &marker);
        self.shell.write_line(&wrapped).await?;
        let (status, _) = self
            .shell
            .read_until_sentinel(&marker, Some(sender))
            .await?;
        self.shell.finish_roundtrip().await?;
        Ok(status)
    }

    /// Execute a command, capturing stdout into a buffer, and return exit code + output.
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

    /// Upload data via a here-document command through the PTY shell.
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

    /// Stream a payload to the PTY shell in chunks.
    async fn stream_shell_payload(&mut self, payload: &[u8]) -> Result<()> {
        for chunk in payload.chunks(32 * 1024) {
            self.shell.write_raw(chunk).await?;
        }
        Ok(())
    }

    /// Upload files via base64-encoded heredoc through the PTY shell.
    async fn copy_upload(&mut self, spec: &mut CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let remote_path = self.normalize_remote_upload_path(spec).await?;
        spec.remote_path = remote_path;
        let payload = build_upload_payload_from_frames(spec).await?;
        let command = upload_here_doc_command(spec, "{}");
        self.run_shell_heredoc_upload(&command, &payload, COPY_HEREDOC_PREFIX)
            .await
    }

    /// Download files via base64-encoded output through the PTY shell.
    async fn copy_download(&mut self, spec: &mut CopySpec) -> Result<()> {
        validate_copy_spec(spec)?;
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        spec.remote_path = remote_path;
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
        let payload = strip_trailing_newlines(outcome.payload);
        send_download_payload_as_frames(spec, payload).await
    }

    /// Expand ~ paths on the remote by executing shell commands.
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

    /// Normalize upload path, checking if remote path is a directory.
    async fn normalize_remote_upload_path(&mut self, spec: &CopySpec) -> Result<String> {
        let remote_path = self.expand_remote_copy_path(&spec.remote_path).await?;
        if spec.recursive {
            return Ok(remote_path);
        }
        if self.remote_path_is_dir(&remote_path).await? {
            return Ok(format!(
                "{}/{}",
                remote_path.trim_end_matches('/'),
                shell_path_basename_or(&spec.source_name, "copy")
            ));
        }
        Ok(remote_path)
    }

    /// Check if a remote path is a directory.
    async fn remote_path_is_dir(&mut self, remote_path: &str) -> Result<bool> {
        let command = format!("test -d {}", shell_single_quote(remote_path));
        let outcome = self
            .run_shell_command_capture(&command, COPY_SENTINEL_PREFIX)
            .await?;
        Ok(outcome.exit_code == 0)
    }

    /// Get the home directory for the current user on the remote.
    async fn remote_home_for_current_user(&mut self) -> Result<String> {
        let home = self.capture_simple_stdout("printf %s \"$HOME\"").await?;
        if !home.is_empty() && home.starts_with('/') {
            return Ok(home);
        }
        self.capture_simple_stdout("getent passwd \"$(id -un)\" | cut -d: -f6")
            .await
    }

    /// Get the home directory for a specific user on the remote.
    async fn remote_home_for_user(&mut self, user: &str) -> Result<String> {
        self.capture_simple_stdout(&format!(
            "getent passwd {} | cut -d: -f6",
            shell_single_quote(user)
        ))
        .await
    }

    /// Run a command and capture its stdout as a string.
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

#[async_trait::async_trait]
impl Connection for BorrowedJumpserverConnection<'_> {
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32> {
        let command = build_interactive_shell_command(&request.argv);
        self.run_shell_command_stream(&command, &request.sender, EXEC_SENTINEL_PREFIX)
            .await
    }

    async fn copy(&mut self, mut spec: CopySpec) -> Result<()> {
        match spec.direction {
            CopyDirection::Upload => self.copy_upload(&mut spec).await,
            CopyDirection::Download => self.copy_download(&mut spec).await,
        }
    }

    async fn exec_interactive(
        &mut self,
        request: &InteractiveRequest,
    ) -> Result<InteractiveHandle> {
        let command = build_interactive_shell_command(&request.argv);
        self.shell.clear_prompt_remainder();
        self.shell.write_line(&command).await?;

        let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(32);
        let (resize_tx, _resize_rx) = mpsc::channel::<(u32, u32)>(8);
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = oneshot::channel::<i32>();

        let sender = request.sender.clone();
        let shell_timeout = self.shell.shell_timeout();

        tokio::spawn(async move {
            let _ = (stdin_rx, stdout_tx, exit_tx, sender, shell_timeout);
        });

        Ok(InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
        })
    }

    fn is_alive(&self) -> bool {
        self.shell.is_channel_open()
    }
}

// ---------------------------------------------------------------------------
// Path and copy helpers (ported from src/connection/jump.rs)
// ---------------------------------------------------------------------------

fn validate_copy_spec(spec: &CopySpec) -> Result<()> {
    if spec.remote_path.is_empty() {
        bail!("remote_path must not be empty");
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

fn shell_single_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

fn remote_path_needs_expansion(path: &str) -> bool {
    path == "~" || path.starts_with("~/") || is_tilde_user_path(path)
}

fn is_tilde_user_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix('~') else {
        return false;
    };
    !rest.is_empty() && !rest.starts_with('/') && rest.contains('/')
}

fn split_tilde_path(path: &str) -> Option<(Option<&str>, &str)> {
    let rest = path.strip_prefix('~')?;
    if rest.is_empty() {
        return Some((None, ""));
    }
    if let Some(stripped) = rest.strip_prefix('/') {
        return Some((None, stripped));
    }
    let (user, suffix) = rest.split_once('/')?;
    Some((Some(user), suffix))
}

fn join_remote_path(home: &str, suffix: &str) -> String {
    if suffix.is_empty() {
        home.to_string()
    } else {
        format!("{}/{}", home.trim_end_matches('/'), suffix)
    }
}

fn strip_trailing_newlines(mut bytes: Vec<u8>) -> Vec<u8> {
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    bytes
}

/// Build an upload payload from standard copy frames. The base64/tar transport
/// is a jumpserver-only detail and never leaves this module.
async fn build_upload_payload_from_frames(spec: &mut CopySpec) -> Result<Vec<u8>> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use std::io::Read;
    use std::process::{Command, Stdio};

    let mut upload_rx = spec
        .upload_rx
        .take()
        .ok_or_else(|| anyhow!("upload copy frame stream missing"))?;
    if spec.recursive {
        let temp_dir = InternalTempDir::new("xho_jump_upload")?;
        materialize_frames_to_dir(temp_dir.path(), &mut upload_rx).await?;
        let temp_root = temp_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut child = Command::new("tar")
                .arg("cf")
                .arg("-")
                .arg("-C")
                .arg(&temp_root)
                .arg(".")
                .stdout(Stdio::piped())
                .spawn()
                .context("failed to spawn tar for jumpserver upload frames")?;
            let mut stdout = child
                .stdout
                .take()
                .ok_or_else(|| anyhow!("failed to capture tar stdout"))?;
            let mut tar_bytes = Vec::new();
            stdout.read_to_end(&mut tar_bytes)?;
            let status = child.wait()?;
            if !status.success() {
                bail!("tar command failed for jumpserver upload frames");
            }
            let mut encoded = BASE64_STANDARD.encode(tar_bytes).into_bytes();
            encoded.push(b'\n');
            Ok(encoded)
        })
        .await
        .map_err(|error| anyhow!("upload payload task failed: {}", error))?
    } else {
        let data = collect_single_file_upload(&mut upload_rx).await?;
        let mut encoded = BASE64_STANDARD.encode(data).into_bytes();
        encoded.push(b'\n');
        Ok(encoded)
    }
}

/// Decode a jumpserver download payload and emit standard copy frames.
async fn send_download_payload_as_frames(spec: &mut CopySpec, payload: Vec<u8>) -> Result<()> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use std::io::Write;
    use std::process::{Command, Stdio};

    let data = BASE64_STANDARD
        .decode(payload)
        .context("failed to decode base64 download payload")?;
    let tx = spec
        .download_tx
        .take()
        .ok_or_else(|| anyhow!("download copy frame stream missing"))?;

    if spec.recursive {
        let temp_dir = InternalTempDir::new("xho_jump_download")?;
        let temp_root = temp_dir.path().to_path_buf();
        let data_for_extract = data;
        tokio::task::spawn_blocking(move || {
            let mut child = Command::new("tar")
                .arg("xf")
                .arg("-")
                .arg("-C")
                .arg(&temp_root)
                .stdin(Stdio::piped())
                .spawn()
                .context("failed to spawn tar extract for jumpserver download payload")?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| anyhow!("failed to open tar stdin"))?;
            stdin.write_all(&data_for_extract)?;
            drop(stdin);
            let status = child.wait()?;
            if !status.success() {
                bail!("tar extract failed for jumpserver download payload");
            }
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("download payload task failed: {}", error))??;

        let source_root = extracted_tar_root(temp_dir.path(), &spec.source_name)?;
        emit_local_path_frames(&source_root, Path::new(""), true, &tx).await?;
    } else {
        let name = shell_path_basename_or(&spec.source_name, "download");
        tx.send(CopyFrame::BeginFile {
            relative_path: name,
            mode: 0,
            size: data.len() as u64,
            mtime: 0,
        })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
        for chunk in data.chunks(64 * 1024) {
            tx.send(CopyFrame::FileData {
                data: chunk.to_vec(),
            })
            .await
            .map_err(|_| anyhow!("download copy frame stream closed"))?;
        }
        tx.send(CopyFrame::EndFile)
            .await
            .map_err(|_| anyhow!("download copy frame stream closed"))?;
    }

    tx.send(CopyFrame::EndOfStream)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

struct InternalTempDir {
    path: PathBuf,
}

impl InternalTempDir {
    fn new(prefix: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!("{}_{}", prefix, Uuid::new_v4()));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create temp dir {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for InternalTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

async fn materialize_frames_to_dir(
    root: &Path,
    upload_rx: &mut mpsc::Receiver<CopyFrame>,
) -> Result<()> {
    let mut current_file: Option<tokio::fs::File> = None;
    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginDirectory {
                relative_path,
                mode,
                ..
            } => {
                let path = join_relative_path(root, &relative_path)?;
                tokio::fs::create_dir_all(&path)
                    .await
                    .with_context(|| format!("failed to create {}", path.display()))?;
                if mode != 0 {
                    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                        .await
                        .with_context(|| format!("failed to chmod {}", path.display()))?;
                }
            }
            CopyFrame::BeginFile {
                relative_path,
                mode,
                ..
            } => {
                if current_file.is_some() {
                    bail!("copy stream began a new file before ending the previous file");
                }
                let path = join_relative_path(root, &relative_path)?;
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }
                let file = tokio::fs::File::create(&path)
                    .await
                    .with_context(|| format!("failed to create {}", path.display()))?;
                if mode != 0 {
                    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                        .await
                        .with_context(|| format!("failed to chmod {}", path.display()))?;
                }
                current_file = Some(file);
            }
            CopyFrame::FileData { data } => {
                let file = current_file
                    .as_mut()
                    .ok_or_else(|| anyhow!("copy stream sent file data before BeginFile"))?;
                file.write_all(&data).await?;
            }
            CopyFrame::EndFile => {
                let mut file = current_file
                    .take()
                    .ok_or_else(|| anyhow!("copy stream sent EndFile before BeginFile"))?;
                file.flush().await?;
            }
            CopyFrame::Symlink {
                relative_path,
                target,
            } => {
                let path = join_relative_path(root, &relative_path)?;
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                }
                let _ = tokio::fs::remove_file(&path).await;
                std::os::unix::fs::symlink(target, &path)
                    .with_context(|| format!("failed to create symlink {}", path.display()))?;
            }
            CopyFrame::EndOfStream => break,
        }
    }
    if current_file.is_some() {
        bail!("copy stream ended before EndFile");
    }
    Ok(())
}

async fn collect_single_file_upload(upload_rx: &mut mpsc::Receiver<CopyFrame>) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    let mut in_file = false;
    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginFile { .. } if !in_file => {
                in_file = true;
            }
            CopyFrame::FileData { data: chunk } if in_file => {
                data.extend_from_slice(&chunk);
            }
            CopyFrame::EndFile if in_file => {
                in_file = false;
            }
            CopyFrame::EndOfStream => break,
            CopyFrame::BeginDirectory { .. } => {
                bail!("non-recursive jumpserver upload received a directory frame");
            }
            CopyFrame::Symlink { .. } => {
                bail!("non-recursive jumpserver symlink upload is not supported");
            }
            other => bail!("unexpected copy frame in single-file upload: {:?}", other),
        }
    }
    if in_file {
        bail!("copy stream ended before EndFile");
    }
    Ok(data)
}

async fn emit_local_path_frames(
    root: &Path,
    relative_root: &Path,
    include_root: bool,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(root)
        .await
        .with_context(|| format!("failed to inspect {}", root.display()))?;
    if include_root {
        send_local_entry_frame(root, relative_root, &metadata, tx).await?;
    }
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        let mut stack = vec![(root.to_path_buf(), relative_root.to_path_buf())];
        while let Some((dir, rel_dir)) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .with_context(|| format!("failed to read {}", dir.display()))?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let rel = rel_dir.join(entry.file_name());
                let metadata = tokio::fs::symlink_metadata(&path)
                    .await
                    .with_context(|| format!("failed to inspect {}", path.display()))?;
                send_local_entry_frame(&path, &rel, &metadata, tx).await?;
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    stack.push((path, rel));
                }
            }
        }
    }
    Ok(())
}

async fn send_local_entry_frame(
    path: &Path,
    relative_path: &Path,
    metadata: &std::fs::Metadata,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    validate_relative_path(relative_path)?;
    let relative_path = relative_path_to_string(relative_path)?;
    if metadata.file_type().is_symlink() {
        let target = tokio::fs::read_link(path)
            .await
            .with_context(|| format!("failed to read symlink {}", path.display()))?;
        tx.send(CopyFrame::Symlink {
            relative_path,
            target: target.to_string_lossy().to_string(),
        })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
        return Ok(());
    }
    if metadata.is_dir() {
        tx.send(CopyFrame::BeginDirectory {
            relative_path,
            mode: metadata.permissions().mode(),
            mtime: metadata.mtime(),
        })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
        return Ok(());
    }
    if !metadata.is_file() {
        bail!(
            "unsupported file type for jumpserver copy: {}",
            path.display()
        );
    }
    tx.send(CopyFrame::BeginFile {
        relative_path,
        mode: metadata.permissions().mode(),
        size: metadata.len(),
        mtime: metadata.mtime(),
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        tx.send(CopyFrame::FileData {
            data: buf[..n].to_vec(),
        })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    }
    tx.send(CopyFrame::EndFile)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

fn extracted_tar_root(temp_dir: &Path, source_name: &str) -> Result<PathBuf> {
    if let Some(name) = shell_path_basename(source_name) {
        let preferred = temp_dir.join(name);
        if preferred.exists() {
            return Ok(preferred);
        }
    }
    let mut entries = std::fs::read_dir(temp_dir)
        .with_context(|| format!("failed to read extracted tar dir {}", temp_dir.display()))?
        .filter_map(Result::ok)
        .collect::<Vec<_>>();
    if entries.len() == 1 {
        return Ok(entries.remove(0).path());
    }
    Ok(temp_dir.to_path_buf())
}

fn shell_path_basename(value: &str) -> Option<String> {
    Path::new(value.trim_end_matches('/'))
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
}

fn shell_path_basename_or(value: &str, fallback: &str) -> String {
    shell_path_basename(value).unwrap_or_else(|| fallback.to_string())
}

fn relative_path_to_string(path: &Path) -> Result<String> {
    validate_relative_path(path)?;
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", path.display()))
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute() {
        bail!(
            "copy frame relative path must not be absolute: {}",
            path.display()
        );
    }
    for component in path.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!(
                    "copy frame relative path contains invalid component: {}",
                    path.display()
                );
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }
    Ok(())
}

fn join_relative_path(root: &Path, relative_path: &str) -> Result<PathBuf> {
    if relative_path.is_empty() {
        return Ok(root.to_path_buf());
    }
    let relative = Path::new(relative_path);
    validate_relative_path(relative)?;
    Ok(root.join(relative))
}
