// Shared connection utilities.
// Contains shell_quote, build_command, wrap_in_shell, PtyShell, and related helpers.

use std::io::Cursor;

use anyhow::{Context, Result, bail};
use russh::ChannelMsg;
use tokio::time::timeout;

/// Shell-quote a single argument using single-quote wrapping.
/// Empty strings produce `''`. Internal single-quotes are escaped via `'\''`.
pub fn shell_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{}'", escaped)
}

/// Build a remote command string by shell-quoting every argument.
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

/// Build an interactive shell command where the first word (if "safe") is
/// left unquoted so that shell aliases can expand.
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

/// Determine the flags for a given shell name.
/// bash, zsh: use -ic (interactive)
/// sh, fish, others: use -c only
fn shell_flags(shell_name: &str) -> &'static str {
    match shell_name {
        "bash" | "zsh" => "-ic",
        _ => "-c",
    }
}

/// Wrap a command string in the specified shell invocation.
/// Single quotes in the input are escaped using the `'\''` technique.
pub fn wrap_in_shell(inner_cmd: &str, shell_name: &str) -> String {
    let escaped = inner_cmd.replace('\'', "'\\''");
    let flags = shell_flags(shell_name);
    format!("{} {} '{}'", shell_name, flags, escaped)
}

/// Build the final remote command, optionally wrapping in a shell.
/// If shell is empty, no wrapping is applied.
pub fn build_final_command(argv: &[String], shell: &str) -> String {
    let inner = build_remote_command(argv);
    if shell.is_empty() {
        inner
    } else {
        // When wrapping in a shell, we join argv with spaces but leave the
        // first word (command name) unquoted so that shell aliases can expand.
        // Subsequent arguments are still shell-quoted to preserve semantics.
        let shell_inner = build_shell_inner_command(argv);
        wrap_in_shell(&shell_inner, shell)
    }
}

/// Build a command string for use inside a shell wrapper.
/// The first argument (command name) is left unquoted so aliases expand.
/// Remaining arguments are shell-quoted to preserve word boundaries.
fn build_shell_inner_command(argv: &[String]) -> String {
    let mut result = String::new();
    for (index, arg) in argv.iter().enumerate() {
        if index > 0 {
            result.push(' ');
        }
        if index == 0 {
            // Leave command name unquoted for alias expansion
            result.push_str(arg);
        } else {
            result.push_str(&shell_quote(arg));
        }
    }
    result
}

/// Resolve the effective shell-wrapping decision.
///
/// Priority (highest to lowest):
/// 1. --no-shell or --shell=false → None (disable)
/// 2. --shell <name> → Some(name) (CLI override)
/// 3. server_shell (per-server from server.toml) → Some(name) or None if empty
/// 4. defaults_shell (from server.toml [defaults]) → Some(name) or None if empty
pub fn resolve_shell(
    cli_shell: Option<&str>,
    no_shell: bool,
    server_shell: Option<&str>,
    defaults_shell: &str,
) -> Option<String> {
    if no_shell {
        return None;
    }
    if let Some(name) = cli_shell {
        if name == "false" {
            return None;
        }
        return Some(name.to_string());
    }
    if let Some(shell) = server_shell {
        if shell.is_empty() {
            return None;
        }
        return Some(shell.to_string());
    }
    if defaults_shell.is_empty() {
        None
    } else {
        Some(defaults_shell.to_string())
    }
}

/// Check whether a byte buffer ends with something that looks like a shell prompt.
pub(crate) fn looks_like_prompt(buffer: &[u8], suffixes: &[String]) -> bool {
    let text = String::from_utf8_lossy(buffer);
    let tail = text
        .rsplit('\n')
        .next()
        .unwrap_or(text.as_ref())
        .trim_end_matches('\r');
    suffixes.iter().any(|suffix| tail.ends_with(suffix))
}

/// If `buffer` ends with a shell prompt, return the index where the prompt
/// begins — i.e. the command output is `buffer[..split]` and the prompt is
/// `buffer[split..]`. Returns `None` when no prompt is present yet.
pub(crate) fn prompt_output_split(buffer: &[u8], suffixes: &[String]) -> Option<usize> {
    if !looks_like_prompt(buffer, suffixes) {
        return None;
    }
    // The prompt is the last line; output is everything up to it.
    let last_nl = buffer.iter().rposition(|&b| b == b'\n');
    Some(last_nl.map(|p| p + 1).unwrap_or(0))
}

/// Extract a sentinel marker from a byte buffer.
/// Returns (exit_status, bytes_before_sentinel, bytes_after_sentinel).
pub(crate) fn extract_sentinel<'a>(
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

pub(crate) const DEFAULT_PTY_TERM: &str = "xterm";
pub(crate) const DEFAULT_PTY_COLS: u32 = 80;
pub(crate) const DEFAULT_PTY_ROWS: u32 = 24;

pub(crate) async fn request_default_pty(
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

/// A PTY shell session that wraps command execution with sentinel-based
/// exit code extraction. Used by JumpserverGateway and similar interactive
/// connection types.
pub(crate) struct PtyShell {
    channel: russh::Channel<russh::client::Msg>,
    pending: Vec<u8>,
    prompt_suffixes: Vec<String>,
    shell_timeout: std::time::Duration,
}

impl PtyShell {
    pub(crate) fn new(
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

    /// Get the configured shell timeout duration.
    pub(crate) fn shell_timeout(&self) -> std::time::Duration {
        self.shell_timeout
    }

    /// Check whether the underlying SSH channel is still open.
    /// Note: russh Channel does not expose an is_closed method directly.
    /// This returns true optimistically; actual closure is detected when
    /// read_chunk() or write operations fail.
    pub(crate) fn is_channel_open(&self) -> bool {
        // Channel liveness is determined by I/O failure in practice.
        // The gateway layer discards the shell on transport errors.
        true
    }

    pub(crate) async fn request_shell(&self) -> Result<()> {
        self.channel.request_shell(true).await?;
        Ok(())
    }

    pub(crate) async fn wait_for_prompt(&mut self) -> Result<()> {
        while !looks_like_prompt(&self.pending, &self.prompt_suffixes) {
            let chunk = self.read_chunk().await?;
            self.pending.extend_from_slice(&chunk);
        }
        Ok(())
    }

    pub(crate) fn pending_text(&self) -> String {
        String::from_utf8_lossy(&self.pending).to_string()
    }

    pub(crate) fn pending_has_prompt(&self) -> bool {
        looks_like_prompt(&self.pending, &self.prompt_suffixes)
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending.clear();
    }

    pub(crate) fn extend_pending(&mut self, chunk: &[u8]) {
        self.pending.extend_from_slice(chunk);
    }

    pub(crate) fn clear_prompt_remainder(&mut self) {
        if looks_like_prompt(&self.pending, &self.prompt_suffixes) {
            self.pending.clear();
        }
    }

    pub(crate) async fn finish_roundtrip(&mut self) -> Result<()> {
        self.wait_for_prompt().await?;
        self.pending.clear();
        Ok(())
    }

    pub(crate) async fn write_line(&mut self, line: &str) -> Result<()> {
        let payload = format!("{line}\r").into_bytes();
        self.channel.data(Cursor::new(payload)).await?;
        Ok(())
    }

    pub(crate) async fn write_raw(&mut self, payload: &[u8]) -> Result<()> {
        self.channel.data(Cursor::new(payload.to_vec())).await?;
        Ok(())
    }

    pub(crate) async fn read_chunk(&mut self) -> Result<Vec<u8>> {
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

    pub(crate) fn make_marker(&self, prefix: &str) -> String {
        format!("{}{}__", prefix, uuid::Uuid::new_v4().simple())
    }

    pub(crate) fn wrap_shell_command(&self, command: &str, marker: &str) -> String {
        format!("{{ {command}; }}; status=$?; printf '{marker}:%s\\n' \"$status\"")
    }

    pub(crate) async fn read_until_sentinel(
        &mut self,
        marker: &str,
        sender: Option<&tokio::sync::mpsc::UnboundedSender<crate::protocol::ServerEvent>>,
    ) -> Result<(i32, Vec<u8>)> {
        self.read_until_sentinel_with_stdin(marker, sender, None)
            .await
    }

    pub(crate) async fn read_until_sentinel_with_stdin(
        &mut self,
        marker: &str,
        sender: Option<&tokio::sync::mpsc::UnboundedSender<crate::protocol::ServerEvent>>,
        mut stdin_rx: Option<&mut tokio::sync::mpsc::Receiver<Vec<u8>>>,
    ) -> Result<(i32, Vec<u8>)> {
        let prefix = marker.as_bytes();
        let mut payload = Vec::new();
        let mut first_output = true;

        loop {
            let chunk = if let Some(rx) = stdin_rx.as_mut() {
                tokio::select! {
                    data = rx.recv() => {
                        match data {
                            Some(data) => {
                                self.write_raw(&data).await?;
                                continue;
                            }
                            None => {
                                self.write_raw(b"\x04").await?;
                                stdin_rx = None;
                                continue;
                            }
                        }
                    }
                    chunk = self.read_chunk() => chunk?,
                }
            } else {
                self.read_chunk().await?
            };
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

    /// Sentinel-free command execution: write the command, then stream its
    /// stdout to `sender` until the shell prompt reappears (command finished),
    /// stripping the prompt itself. No exit code is captured — this replaces the
    /// `echo $?`+marker sentinel. Returns `Ok(())` when the command is done.
    pub(crate) async fn run_command_plain(
        &mut self,
        command: &str,
        sender: &tokio::sync::mpsc::UnboundedSender<crate::protocol::ServerEvent>,
    ) -> Result<()> {
        self.clear_prompt_remainder();
        self.write_line(command).await?;
        let mut first_output = true;
        loop {
            let chunk = self.read_chunk().await?;
            self.pending.extend_from_slice(&chunk);
            // Prompt reappeared → command finished. Stream everything before it
            // and KEEP the prompt in `pending` so the caller's `finish_roundtrip`
            // (wait_for_prompt + clear) resolves immediately instead of blocking.
            if let Some(split) = prompt_output_split(&self.pending, &self.prompt_suffixes) {
                let out = if first_output {
                    strip_leading_shell_noise(&self.pending[..split])
                } else {
                    &self.pending[..split]
                };
                if !out.is_empty() {
                    let _ = sender.send(crate::protocol::ServerEvent::Stdout { data: out.to_vec() });
                }
                self.pending.drain(..split);
                return Ok(());
            }
            // Otherwise stream the safe prefix, retaining a tail for matching.
            let keep = 64;
            if self.pending.len() > keep {
                let safe_len = self.pending.len() - keep;
                let data = if first_output {
                    first_output = false;
                    strip_leading_shell_noise(&self.pending[..safe_len]).to_vec()
                } else {
                    self.pending[..safe_len].to_vec()
                };
                self.pending.drain(..safe_len);
                if !data.is_empty() {
                    let _ = sender.send(crate::protocol::ServerEvent::Stdout { data });
                }
            }
        }
    }

    pub(super) async fn into_interactive_command(
        mut self,
        command: String,
        marker_prefix: &str,
    ) -> Result<crate::daemon::connection::InteractiveHandle> {
        self.clear_prompt_remainder();
        let marker = self.make_marker(marker_prefix);
        let wrapped = self.wrap_shell_command(&command, &marker);
        self.write_line(&wrapped).await?;

        let PtyShell {
            mut channel,
            mut pending,
            ..
        } = self;
        let prefix = marker.into_bytes();

        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
        let (resize_tx, mut resize_rx) = tokio::sync::mpsc::channel::<(u32, u32)>(8);
        let (stdout_tx, stdout_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<i32>();

        let task = tokio::spawn(async move {
            let mut first_output = true;
            let mut exit_code = 255;

            loop {
                tokio::select! {
                    Some(data) = stdin_rx.recv() => {
                        if channel.data(Cursor::new(data)).await.is_err() {
                            break;
                        }
                    }
                    Some((cols, rows)) = resize_rx.recv() => {
                        let _ = channel.window_change(cols, rows, 0, 0).await;
                    }
                    // No read timeout here: an interactive session may sit idle
                    // for long stretches while the user is not typing. Transport
                    // liveness is handled by SSH keepalives at the client config
                    // level; the channel closing yields None and breaks the loop.
                    message = channel.wait() => {
                        let message = match message {
                            Some(message) => message,
                            None => break,
                        };

                        let chunk = match message {
                            ChannelMsg::Data { data } => data.to_vec(),
                            ChannelMsg::ExtendedData { data, .. } => data.to_vec(),
                            ChannelMsg::ExitStatus { exit_status } => {
                                exit_code = exit_status as i32;
                                Vec::new()
                            }
                            ChannelMsg::ExitSignal { .. } => {
                                exit_code = 255;
                                Vec::new()
                            }
                            ChannelMsg::Close | ChannelMsg::Eof => break,
                            _ => Vec::new(),
                        };

                        if chunk.is_empty() {
                            continue;
                        }

                        pending.extend_from_slice(&chunk);
                        if let Some((code, before, _after)) = extract_sentinel(&pending, &prefix) {
                            let before = if first_output {
                                strip_leading_shell_noise(before)
                            } else {
                                before
                            };
                            if !before.is_empty() {
                                let _ = stdout_tx.send(before.to_vec());
                            }
                            exit_code = code;
                            break;
                        }

                        let keep = prefix.len() + 32;
                        if pending.len() > keep {
                            let safe_len = pending.len() - keep;
                            let chunk = if first_output {
                                first_output = false;
                                strip_leading_shell_noise(&pending[..safe_len]).to_vec()
                            } else {
                                pending[..safe_len].to_vec()
                            };
                            pending.drain(..safe_len);
                            if !chunk.is_empty() {
                                let _ = stdout_tx.send(chunk);
                            }
                        }
                    }
                }
            }

            let _ = channel.close().await;
            let _ = exit_tx.send(exit_code);
        });
        let abort_handles = vec![task.abort_handle()];

        Ok(crate::daemon::connection::InteractiveHandle {
            stdin_tx,
            resize_tx,
            stdout_rx,
            exit_rx,
            abort_handles,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_final_command, build_remote_command, extract_sentinel, resolve_shell, shell_flags,
        shell_quote, wrap_in_shell,
    };

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

    #[test]
    fn shell_flags_returns_ic_for_bash_zsh() {
        assert_eq!(shell_flags("bash"), "-ic");
        assert_eq!(shell_flags("zsh"), "-ic");
    }

    #[test]
    fn shell_flags_returns_c_for_others() {
        assert_eq!(shell_flags("sh"), "-c");
        assert_eq!(shell_flags("fish"), "-c");
        assert_eq!(shell_flags("ksh"), "-c");
        assert_eq!(shell_flags("unknown"), "-c");
    }

    #[test]
    fn wrap_in_shell_basic() {
        assert_eq!(wrap_in_shell("echo hello", "bash"), "bash -ic 'echo hello'");
        assert_eq!(wrap_in_shell("echo hello", "zsh"), "zsh -ic 'echo hello'");
        assert_eq!(wrap_in_shell("echo hello", "sh"), "sh -c 'echo hello'");
        assert_eq!(wrap_in_shell("echo hello", "fish"), "fish -c 'echo hello'");
    }

    #[test]
    fn wrap_in_shell_escapes_single_quotes() {
        assert_eq!(
            wrap_in_shell("echo 'hi'", "bash"),
            "bash -ic 'echo '\\''hi'\\'''",
        );
    }

    #[test]
    fn wrap_in_shell_empty_command() {
        assert_eq!(wrap_in_shell("", "bash"), "bash -ic ''");
    }

    #[test]
    fn build_final_command_with_shell() {
        let argv = vec!["ls".to_string(), "-la".to_string()];
        let result = build_final_command(&argv, "bash");
        let expected = r#"bash -ic 'ls '\''-la'\'''"#;
        assert_eq!(result, expected);

        let argv2 = vec!["ls".to_string()];
        let result2 = build_final_command(&argv2, "bash");
        assert_eq!(result2, "bash -ic 'ls'");
    }

    #[test]
    fn build_final_command_without_shell() {
        let argv = vec!["echo".to_string(), "hello".to_string()];
        let result = build_final_command(&argv, "");
        assert_eq!(result, build_remote_command(&argv));
    }

    #[test]
    fn resolve_shell_no_shell_flag_wins() {
        assert_eq!(resolve_shell(Some("bash"), true, Some("zsh"), "sh"), None);
    }

    #[test]
    fn resolve_shell_cli_shell_wins() {
        assert_eq!(
            resolve_shell(Some("bash"), false, Some("zsh"), "sh"),
            Some("bash".to_string())
        );
    }

    #[test]
    fn resolve_shell_cli_false_disables() {
        assert_eq!(resolve_shell(Some("false"), false, Some("zsh"), "sh"), None);
    }

    #[test]
    fn resolve_shell_server_shell_used() {
        assert_eq!(
            resolve_shell(None, false, Some("zsh"), "bash"),
            Some("zsh".to_string())
        );
    }

    #[test]
    fn resolve_shell_server_empty_disables() {
        assert_eq!(resolve_shell(None, false, Some(""), "bash"), None);
    }

    #[test]
    fn resolve_shell_defaults_used() {
        assert_eq!(
            resolve_shell(None, false, None, "bash"),
            Some("bash".to_string())
        );
    }

    #[test]
    fn resolve_shell_defaults_empty_returns_none() {
        assert_eq!(resolve_shell(None, false, None, ""), None);
    }
}
