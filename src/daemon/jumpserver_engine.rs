// Jumpserver bastion engine.
//
// A menu-driven bastion is not session-channel-shaped: you connect, navigate an
// interactive asset menu (MFA → search → asset selection → pagination), and end
// up with a PTY shell on the chosen asset. This module owns that engine:
//   - `PtyShell` wraps a russh PTY channel with a pending-buffer + prompt model.
//   - `navigate_to_asset` drives the menu state machine to the asset prompt.
//   - `run_command_plain` streams a command's stdout until the prompt reappears.
//   - `run_raw_passthrough` flips to byte-for-byte bidirectional I/O (interactive
//     shell + the sftp subsystem).
//
// Everything jumpserver-specific lives here, behind the `Gateway`/`TargetSession`
// traits — there is no jumpserver special-casing at any call site.

use std::io::Cursor;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use russh::ChannelMsg;
use russh::client;
use tokio::sync::mpsc;
use tokio::time::timeout;

pub(crate) const MENU_PROMPT_CONTAINS: &str = "Opt";
pub(crate) const MFA_PROMPT_CONTAINS: &str = "MFA";
pub(crate) const SHELL_PROMPT_SUFFIXES: &[&str] = &["$ ", "# "];
pub(crate) const PAGE_PROMPT_CONTAINS: &str = "上一页";
const DEFAULT_PTY_TERM: &str = "xterm";
const DEFAULT_PTY_COLS: u32 = 80;
const DEFAULT_PTY_ROWS: u32 = 24;

// ---------------------------------------------------------------------------
// Prompt detection
// ---------------------------------------------------------------------------

fn looks_like_prompt(buffer: &[u8], suffixes: &[String]) -> bool {
    let text = String::from_utf8_lossy(buffer);
    let tail = text
        .rsplit('\n')
        .next()
        .unwrap_or(text.as_ref())
        .trim_end_matches('\r');
    suffixes.iter().any(|suffix| tail.ends_with(suffix))
}

/// If `buffer` ends with a prompt, return the index where the prompt begins;
/// the command output is `buffer[..split]`.
fn prompt_output_split(buffer: &[u8], suffixes: &[String]) -> Option<usize> {
    if !looks_like_prompt(buffer, suffixes) {
        return None;
    }
    let last_nl = buffer.iter().rposition(|&b| b == b'\n');
    Some(last_nl.map(|p| p + 1).unwrap_or(0))
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

pub(crate) fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == 0x1b {
            index += 1;
            if index >= bytes.len() {
                break;
            }
            match bytes[index] {
                b'[' => {
                    index += 1;
                    while index < bytes.len() {
                        let byte = bytes[index];
                        index += 1;
                        if (0x40..=0x7e).contains(&byte) {
                            break;
                        }
                    }
                }
                b']' => {
                    index += 1;
                    while index < bytes.len() {
                        let byte = bytes[index];
                        index += 1;
                        if byte == 0x07 {
                            break;
                        }
                        if byte == 0x1b && bytes.get(index) == Some(&b'\\') {
                            index += 1;
                            break;
                        }
                    }
                }
                _ => {
                    index += 1;
                }
            }
            continue;
        }
        if let Some(ch) = input[index..].chars().next() {
            output.push(ch);
            index += ch.len_utf8();
        } else {
            break;
        }
    }
    output
}

// ---------------------------------------------------------------------------
// PtyShell
// ---------------------------------------------------------------------------

/// A PTY shell session on the bastion: a russh channel with a pending-buffer +
/// prompt model for menu navigation and command streaming.
pub(crate) struct PtyShell {
    channel: russh::Channel<client::Msg>,
    pending: Vec<u8>,
    prompt_suffixes: Vec<String>,
    shell_timeout: Duration,
}

impl PtyShell {
    pub(crate) fn new(
        channel: russh::Channel<client::Msg>,
        prompt_suffixes: Vec<String>,
        shell_timeout: Duration,
    ) -> Self {
        Self {
            channel,
            pending: Vec::new(),
            prompt_suffixes,
            shell_timeout,
        }
    }

    /// Optimistic liveness: real closure is detected on I/O failure. The gateway
    /// discards the shell when an operation errors.
    pub(crate) fn is_channel_open(&self) -> bool {
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

    pub(crate) async fn window_change(&mut self, cols: u32, rows: u32) {
        let _ = self.channel.window_change(cols, rows, 0, 0).await;
    }

    /// Sentinel-free command execution: write the command, stream stdout to
    /// `sender` until the prompt reappears, stripping the prompt. No exit code
    /// (a menu bastion PTY has no native exec/exit-status). The prompt is KEPT
    /// in `pending` so a subsequent command's roundtrip resolves immediately.
    pub(crate) async fn run_command_plain(
        &mut self,
        command: &str,
        sender: &mpsc::UnboundedSender<Vec<u8>>,
    ) -> Result<()> {
        self.clear_prompt_remainder();
        self.write_line(command).await?;
        let mut first_output = true;
        loop {
            let chunk = self.read_chunk().await?;
            self.pending.extend_from_slice(&chunk);
            if let Some(split) = prompt_output_split(&self.pending, &self.prompt_suffixes) {
                let out = if first_output {
                    strip_leading_shell_noise(&self.pending[..split])
                } else {
                    &self.pending[..split]
                };
                if !out.is_empty() {
                    let _ = sender.send(out.to_vec());
                }
                self.pending.drain(..split);
                return Ok(());
            }
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
                    let _ = sender.send(data);
                }
            }
        }
    }

    /// Byte-for-byte bidirectional passthrough: forward `stdin_rx` to the PTY and
    /// PTY output to `stdout_tx` until either side closes. Any `pending` bytes
    /// buffered during navigation are flushed first. Used by the interactive
    /// shell and the sftp subsystem. Returns the exit code (0 unless the channel
    /// reported one).
    pub(crate) async fn run_raw_passthrough(
        mut self,
        mut stdin_rx: mpsc::Receiver<Vec<u8>>,
        stdout_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) -> i32 {
        if !self.pending.is_empty() {
            let _ = stdout_tx.send(std::mem::take(&mut self.pending));
        }
        let mut exit_code = 0;
        let mut stdin_open = true;
        loop {
            tokio::select! {
                stdin = stdin_rx.recv(), if stdin_open => match stdin {
                    Some(data) => {
                        if self.channel.data(Cursor::new(data)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        let _ = self.channel.eof().await;
                        stdin_open = false;
                    }
                },
                msg = self.channel.wait() => match msg {
                    Some(ChannelMsg::Data { data }) => {
                        if stdout_tx.send(data.to_vec()).is_err() {
                            break;
                        }
                    }
                    Some(ChannelMsg::ExtendedData { data, .. }) => {
                        if stdout_tx.send(data.to_vec()).is_err() {
                            break;
                        }
                    }
                    Some(ChannelMsg::ExitStatus { exit_status }) => {
                        exit_code = exit_status as i32;
                    }
                    Some(ChannelMsg::ExitSignal { .. }) => {
                        exit_code = 255;
                    }
                    Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => break,
                    _ => {}
                },
            }
        }
        let _ = self.channel.close().await;
        exit_code
    }
}

pub(crate) async fn request_default_pty(channel: &russh::Channel<client::Msg>) -> Result<()> {
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

// ---------------------------------------------------------------------------
// Asset menu parsing
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JumpserverAssetRow {
    pub id: String,
    pub ip: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageStatus {
    pub current: u32,
    pub total: u32,
}

pub(crate) fn parse_asset_rows(text: &str) -> Vec<JumpserverAssetRow> {
    let clean = strip_ansi(text);
    clean
        .lines()
        .filter_map(|line| {
            let columns = line.split('|').map(str::trim).collect::<Vec<_>>();
            if columns.len() < 3 {
                return None;
            }
            let id = columns[0];
            let ip = columns[2];
            if id.chars().all(|ch| ch.is_ascii_digit()) && looks_like_ipv4(ip) {
                Some(JumpserverAssetRow {
                    id: id.to_string(),
                    ip: ip.to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn select_exact_asset_id(text: &str, ip: &str) -> Result<Option<String>> {
    let matches = parse_asset_rows(text)
        .into_iter()
        .filter(|row| row.ip == ip)
        .collect::<Vec<_>>();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(Some(matches[0].id.clone())),
        count => bail!(
            "jumpserver asset search for {} returned {} exact matches",
            ip,
            count
        ),
    }
}

fn looks_like_ipv4(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 4
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

pub(crate) fn parse_page_status(text: &str) -> Option<PageStatus> {
    let clean = strip_ansi(text);
    let (_, rest) = clean.split_once("页码：")?;
    let current = rest
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())?
        .parse()
        .ok()?;
    let (_, rest) = clean.split_once("总页数：")?;
    let total = rest
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())?
        .parse()
        .ok()?;
    Some(PageStatus { current, total })
}

pub(crate) fn contains_menu_prompt(text: &str) -> bool {
    strip_ansi(text).contains(MENU_PROMPT_CONTAINS)
}

pub(crate) fn contains_page_prompt(text: &str) -> bool {
    let clean = strip_ansi(text);
    clean.contains(PAGE_PROMPT_CONTAINS) && clean.trim_end().ends_with(':')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_asset_table_and_selects_exact_ip() {
        let text = r#"
  ID    | 主机名                                                                             | IP                                       | 备注
+-------+------------------------------------------------------------------------------------+------------------------------------------+------------------------------------------------+
  1     | asset-198.51.100.3                                           | 198.51.100.3                             |
  2     | asset-198.51.100.30                                          | 198.51.100.30                            |
  3     | asset-198.51.100.31                                          | 198.51.100.31                            |
页码：1，每页行数：9，总页数：1，总数量：3
Opt>
"#;
        assert_eq!(
            select_exact_asset_id(text, "198.51.100.3").unwrap(),
            Some("1".to_string())
        );
        assert_eq!(select_exact_asset_id(text, "198.51.100.4").unwrap(), None);
    }

    #[test]
    fn parses_ansi_paginated_asset_table() {
        let text = "\u{1b}[H\u{1b}[2J  \u{1b}[1;32mID\u{1b}[0m | \u{1b}[1;32m主机名\u{1b}[0m | \u{1b}[1;32mIP\u{1b}[0m | 备注\n\
  1  | ass....30 | 198.51.100.30 | \n\
\u{1b}[32m页码：1，每页行数：1，总页数：3，总数量：3\u{1b}[0m\n\
上一页：P/p  下一页：Enter|N/n  返回：B/b\n:";
        assert_eq!(
            select_exact_asset_id(text, "198.51.100.30").unwrap(),
            Some("1".to_string())
        );
        assert_eq!(
            parse_page_status(text),
            Some(PageStatus {
                current: 1,
                total: 3
            })
        );
        assert!(contains_page_prompt(text));
    }

    #[test]
    fn duplicate_exact_asset_ids_are_rejected() {
        let text = "\
  1 | host-a | 10.0.0.1 | \n\
  2 | host-b | 10.0.0.1 | \n\
页码：1，每页行数：9，总页数：1，总数量：2\n\
Opt>";
        let error = select_exact_asset_id(text, "10.0.0.1").unwrap_err();
        assert!(error.to_string().contains("returned 2 exact matches"));
    }
}
