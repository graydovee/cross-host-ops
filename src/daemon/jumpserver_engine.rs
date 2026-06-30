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
// 256-color so the asset shell's rc (e.g. the default `~/.bashrc`) enables its
// `ls --color=auto` alias, which it gates on `TERM` matching `*-256color`.
// Matches what every other backend requests (see `drive_exec`/`drive_interactive`).
const DEFAULT_PTY_TERM: &str = "xterm-256color";
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

    /// After `write_line`, the PTY echoes the typed command back. This drains
    /// everything up to and including the first newline byte (0x0A) in the
    /// output — the echoed command line — so the user never sees it. Remaining
    /// data (command output) stays in `pending` for the caller or passthrough.
    ///
    /// Times out after `timeout_ms` to avoid blocking if echo doesn't arrive.
    /// On timeout/EOF the pending bytes are LEFT INTACT (not cleared): the
    /// strict `SentinelScanner` downstream tolerates a leftover echo line (it
    /// only matches `marker:<digits>\n`, never the echoed `marker:%s`), so a
    /// slow echo degrades to "user sees the command line" instead of "output
    /// destroyed → empty result".
    pub(crate) async fn drain_echo_line(&mut self, timeout_ms: u64) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            if let Some(pos) = self.pending.iter().position(|&b| b == b'\n') {
                self.pending.drain(..=pos);
                return Ok(());
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(());
            }
            match tokio::time::timeout(deadline - now, self.read_chunk()).await {
                Ok(Ok(chunk)) => self.pending.extend_from_slice(&chunk),
                Ok(Err(_)) | Err(_) => return Ok(()),
            }
        }
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

    /// Execute `command`, stream stdout to `sender` until the prompt reappears,
    /// then return the exit code captured via a unique marker. The command is
    /// wrapped as `{ command; }; status=$?; printf '{marker}:%s\n' "$status"`.
    /// The marker contains a UUID so it cannot match user output.
    ///
    /// The prompt is KEPT in `pending` so a subsequent command resolves fast.
    pub(crate) async fn run_command_plain(
        &mut self,
        command: &str,
        sender: &mpsc::UnboundedSender<Vec<u8>>,
    ) -> Result<i32> {
        let marker = make_marker();
        let wrapped = format!(
            "{{ {command}; }}; status=$?; printf '{marker}:%s\\n' \"$status\""
        );
        self.clear_prompt_remainder();
        self.write_line(&wrapped).await?;
        self.drain_echo_line(3000).await?;
        let marker_bytes = marker.as_bytes();
        let keep = marker_bytes.len() + 32;
        let mut first_output = true;
        loop {
            let chunk = self.read_chunk().await?;
            self.pending.extend_from_slice(&chunk);
            if let Some(split) = prompt_output_split(&self.pending, &self.prompt_suffixes) {
                let raw = &self.pending[..split];
                let (exit_code, clean) = extract_marker(raw, marker_bytes);
                let out = if first_output {
                    strip_leading_shell_noise(clean)
                } else {
                    clean
                };
                if !out.is_empty() {
                    let _ = sender.send(out.to_vec());
                }
                self.pending.drain(..split);
                return Ok(exit_code);
            }
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
    /// PTY output to `stdout_tx` until either side closes. `resize_rx` carries
    /// terminal resize events (cols, rows) applied to the PTY channel.
    ///
    /// When `sentinel` is `Some(marker)`, ALL output — starting with any bytes
    /// already buffered in `pending` (e.g. a fast command's output that arrived
    /// during echo draining) — is fed through a strict [`SentinelScanner`] that
    /// matches only `marker:<digits>\n`. On match the exit code is parsed and
    /// the still-open shell is RETURNED (`Some`) so the caller can return it to
    /// the session cache: the user never sees the marker, the post-command
    /// prompt, or the JumpServer menu, and the next exec reuses the shell.
    ///
    /// Feeding `pending` through the scanner (instead of flushing it raw) is
    /// what prevents the `__XHO_E_<uuid>:0` marker + post-command prompt from
    /// leaking when a fast command's output was already buffered.
    ///
    /// Returns `(exit_code, Some(shell))` when the channel is still reusable
    /// (sentinel completed), else `(exit_code, None)` (channel closed/dead).
    pub(crate) async fn run_raw_passthrough(
        mut self,
        mut stdin_rx: mpsc::Receiver<Vec<u8>>,
        stdout_tx: mpsc::UnboundedSender<Vec<u8>>,
        mut resize_rx: mpsc::Receiver<(u32, u32)>,
        sentinel: Option<Vec<u8>>,
    ) -> (i32, Option<PtyShell>) {
        let mut exit_code = 0;
        let mut stdin_open = true;
        let mut scanner = sentinel.clone().map(SentinelScanner::new);
        // Only sentinel mode can leave the channel reusable (the asset shell is
        // still at its prompt after the wrapped command completes). `shell()` /
        // `subsystem("sftp")` (no sentinel) always consume the channel.
        let mut reusable = scanner.is_some();

        // Feed any bytes already buffered in `pending` through the scanner FIRST
        // (sentinel mode) — never flush them raw. In non-sentinel mode pending is
        // flushed raw (interactive login shell / sftp launch output).
        let pending = std::mem::take(&mut self.pending);
        if let Some(sc) = scanner.as_mut() {
            if !pending.is_empty() {
                let (forward, done) = sc.feed(&pending);
                if !forward.is_empty() {
                    let _ = stdout_tx.send(forward);
                }
                if done {
                    exit_code = sc.exit_code();
                    self.pending = sc.take_leftover();
                    return self.finish_passthrough(exit_code, reusable).await;
                }
            }
        } else if !pending.is_empty() {
            let _ = stdout_tx.send(pending);
        }

        loop {
            tokio::select! {
                stdin = stdin_rx.recv(), if stdin_open => match stdin {
                    Some(data) => {
                        if self.channel.data(Cursor::new(data)).await.is_err() {
                            reusable = false;
                            break;
                        }
                    }
                    None => {
                        let _ = self.channel.eof().await;
                        stdin_open = false;
                    }
                },
                resize = resize_rx.recv() => {
                    if let Some((cols, rows)) = resize {
                        let _ = self.channel.window_change(cols, rows, 0, 0).await;
                    }
                }
                msg = self.channel.wait() => match msg {
                    Some(ChannelMsg::Data { data }) => {
                        let chunk = data.to_vec();
                        if let Some(sc) = scanner.as_mut() {
                            let (forward, done) = sc.feed(&chunk);
                            if !forward.is_empty() && stdout_tx.send(forward).is_err() {
                                reusable = false;
                                break;
                            }
                            if done {
                                exit_code = sc.exit_code();
                                let leftover = sc.take_leftover();
                                self.pending = leftover;
                                return self.finish_passthrough(exit_code, reusable).await;
                            }
                        } else if stdout_tx.send(chunk).is_err() {
                            reusable = false;
                            break;
                        }
                    }
                    Some(ChannelMsg::ExtendedData { data, .. }) => {
                        if stdout_tx.send(data.to_vec()).is_err() {
                            reusable = false;
                            break;
                        }
                    }
                    Some(ChannelMsg::ExitStatus { exit_status }) => {
                        exit_code = exit_status as i32;
                    }
                    Some(ChannelMsg::ExitSignal { .. }) => {
                        exit_code = 255;
                        reusable = false;
                    }
                    Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => {
                        reusable = false;
                        break;
                    }
                    _ => {}
                },
            }
        }

        self.finish_passthrough(exit_code, reusable).await
    }

    /// Finalize passthrough: hand back the still-open shell when reusable, or
    /// close the channel and discard it otherwise.
    async fn finish_passthrough(self, exit_code: i32, reusable: bool) -> (i32, Option<PtyShell>) {
        if reusable {
            (exit_code, Some(self))
        } else {
            let _ = tokio::time::timeout(Duration::from_secs(3), self.channel.close()).await;
            (exit_code, None)
        }
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
// Exit-code sentinel
// ---------------------------------------------------------------------------

/// Generate a unique exit-code marker: `__XHO_E_{uuid}__`.
/// The UUID guarantees the marker cannot appear in user output.
pub(crate) fn make_marker() -> String {
    format!("__XHO_E_{}__", uuid::Uuid::new_v4().simple())
}

/// Search `buf` for `{marker}:{exit_code}\n`. Returns `(exit_code, cleaned_buf)`
/// where `cleaned_buf` is everything before the marker. If the marker is not
/// found, returns `(0, buf)`.
fn extract_marker<'a>(buf: &'a [u8], marker: &[u8]) -> (i32, &'a [u8]) {
    if let Some(pos) = find_subslice(buf, marker) {
        let after_marker = &buf[pos + marker.len()..];
        // Expect ":digits\n"
        if let Some(colon_end) = after_marker.iter().position(|&b| b == b':') {
            // Should be 0 (marker is immediately followed by ':')
            if colon_end == 0 {
                let status_start = &after_marker[1..];
                let line_end = status_start
                    .iter()
                    .position(|&b| b == b'\n')
                    .unwrap_or(status_start.len());
                let code_str = std::str::from_utf8(&status_start[..line_end])
                    .unwrap_or("0")
                    .trim();
                let code = code_str.parse::<i32>().unwrap_or(0);
                return (code, &buf[..pos]);
            }
        }
    }
    (0, buf)
}

/// Find the first occurrence of `needle` in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Returns the length of the longest suffix of `buf` that is also a prefix of
/// `pattern`. This determines how many bytes to retain at a chunk boundary for
/// a potential marker match — e.g. if `buf` ends with `__` and `pattern` starts
/// with `__XHO_E_`, returns 2. Only non-zero when `buf` ends with characters
/// that begin the pattern, so in practice the hold is 0 for most chunks.
fn partial_suffix_match(buf: &[u8], pattern: &[u8]) -> usize {
    if pattern.len() <= 1 || buf.is_empty() {
        return 0;
    }
    let max = buf.len().min(pattern.len() - 1);
    (1..=max).rev().find(|&len| &buf[buf.len() - len..] == &pattern[..len]).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// SentinelScanner — strict streaming match for `marker:<digits>\n`
// ---------------------------------------------------------------------------

/// A streaming scanner that matches a sentinel `marker:<digits>\n` across an
/// arbitrary byte stream, forwarding everything before it and parsing the exit
/// code. Pure (no I/O) so it is unit-testable; `run_raw_passthrough` drives it.
///
/// The match is **strict**: the marker counts only when immediately followed by
/// `:` + one or more ASCII digits + `\n`. This is what keeps the echoed command
/// line — which contains the literal marker followed by `:%s` (printf format)
/// — from being mistaken for the real sentinel. A false occurrence is skipped
/// and scanning continues, so an incomplete echo drain degrades to "the user
/// sees the command line" rather than "empty output / premature exit".
pub(crate) struct SentinelScanner {
    marker: Vec<u8>,
    hold: Vec<u8>,
    leftover: Vec<u8>,
    done: bool,
    exit_code: i32,
}

impl SentinelScanner {
    pub(crate) fn new(marker: Vec<u8>) -> Self {
        Self {
            marker,
            hold: Vec::new(),
            leftover: Vec::new(),
            done: false,
            exit_code: 0,
        }
    }

    /// Feed one chunk of output. Returns `(bytes safe to forward now, done)`.
    /// When `done` is true the sentinel matched and [`Self::exit_code`] /
    /// [`Self::take_leftover`] are available.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> (Vec<u8>, bool) {
        if self.done {
            return (Vec::new(), true);
        }
        self.hold.extend_from_slice(chunk);
        match find_sentinel(&self.hold, &self.marker) {
            SentinelFind::Confirmed { pos, code, end } => {
                let forward = self.hold[..pos].to_vec();
                self.leftover = self.hold[end..].to_vec();
                self.hold.clear();
                self.exit_code = code;
                self.done = true;
                (forward, true)
            }
            // A candidate marker is present but its `:digits\n` lookahead is
            // incomplete — hold back from the marker until more bytes arrive.
            SentinelFind::Incomplete(pos) => {
                let forward = self.hold[..pos].to_vec();
                self.hold.drain(..pos);
                (forward, false)
            }
            // No marker candidate; hold back only a possible partial-prefix suffix.
            SentinelFind::None => {
                let pm = partial_suffix_match(&self.hold, &self.marker);
                let safe = self.hold.len().saturating_sub(pm);
                let forward = self.hold[..safe].to_vec();
                self.hold.drain(..safe);
                (forward, false)
            }
        }
    }

    pub(crate) fn exit_code(&self) -> i32 {
        self.exit_code
    }

    /// Bytes after the sentinel (e.g. the post-command shell prompt), used to
    /// return the shell to the session cache in a known state.
    pub(crate) fn take_leftover(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.leftover)
    }
}

enum SentinelFind {
    /// Confirmed sentinel at `pos`; exit `code`; total consumed length is `end`.
    Confirmed { pos: usize, code: i32, end: usize },
    /// Marker at `pos` but the lookahead is incomplete (need more bytes).
    Incomplete(usize),
    /// No confirmed sentinel (no marker, or only false occurrences).
    None,
}

/// Search `buf` for the first confirmed `marker:digits\n`. False marker
/// occurrences (e.g. the echo, where the marker is followed by `:%s`) are
/// skipped so scanning can continue to the real sentinel.
fn find_sentinel(buf: &[u8], marker: &[u8]) -> SentinelFind {
    if marker.is_empty() {
        return SentinelFind::None;
    }
    let mut search_from = 0;
    while let Some(rel) = find_subslice(&buf[search_from..], marker) {
        let pos = search_from + rel;
        let after = &buf[pos + marker.len()..];
        match lookahead_sentinel(after) {
            Lookahead::Confirmed { code, consumed } => {
                return SentinelFind::Confirmed {
                    pos,
                    code,
                    end: pos + marker.len() + consumed,
                };
            }
            Lookahead::Incomplete => return SentinelFind::Incomplete(pos),
            Lookahead::Mismatch => {
                // False occurrence — skip past it and keep searching.
                search_from = pos + marker.len();
            }
        }
    }
    SentinelFind::None
}

enum Lookahead {
    Confirmed { code: i32, consumed: usize },
    Incomplete,
    Mismatch,
}

/// Inspect the bytes immediately following a marker occurrence (`after`).
fn lookahead_sentinel(after: &[u8]) -> Lookahead {
    match after.first() {
        None => return Lookahead::Incomplete,
        Some(&b':') => {}
        Some(_) => return Lookahead::Mismatch,
    }
    // Collect ASCII digits after the colon.
    let mut i = 1;
    while i < after.len() && after[i].is_ascii_digit() {
        i += 1;
    }
    let digit_count = i - 1;
    if digit_count == 0 {
        // No digit after ':'. Mismatch if more bytes follow, else incomplete.
        return if i < after.len() {
            Lookahead::Mismatch
        } else {
            Lookahead::Incomplete
        };
    }
    if i >= after.len() {
        return Lookahead::Incomplete; // digits present, waiting for the terminator
    }
    // Terminator: `\n` OR `\r\n`. A PTY translates `\n` -> `\r\n` (onlcr), so a
    // real bastion emits `marker:0\r\n`; requiring a bare `\n` here would reject
    // the CRLF form, leak the marker, and leave the session attached.
    let (term_len, matched) = match after[i] {
        b'\n' => (1, true),
        b'\r' if after.get(i + 1) == Some(&b'\n') => (2, true),
        // `\r` at the buffer boundary: wait for the following `\n` to confirm.
        b'\r' if i + 1 >= after.len() => return Lookahead::Incomplete,
        _ => (0, false),
    };
    if !matched {
        return Lookahead::Mismatch; // e.g. ":12x" or ":12:"
    }
    let code = std::str::from_utf8(&after[1..i])
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);
    // consumed = ':' + digits + terminator ('\n' or '\r\n')
    Lookahead::Confirmed { code, consumed: i + term_len }
}

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

    // ---- SentinelScanner ----

    fn scan(marker: &[u8], chunks: &[&[u8]]) -> (Vec<u8>, i32, Vec<u8>) {
        let mut sc = SentinelScanner::new(marker.to_vec());
        let mut out = Vec::new();
        let mut done = false;
        for chunk in chunks {
            if done {
                break;
            }
            let (forward, d) = sc.feed(chunk);
            out.extend_from_slice(&forward);
            done = d;
        }
        let code = if done { sc.exit_code() } else { -1 };
        let leftover = sc.take_leftover();
        (out, code, leftover)
    }

    /// The leak bug: marker fully present in the first "pending" feed must be
    /// stripped, not forwarded, with the exit code parsed.
    #[test]
    fn scanner_strips_marker_present_in_first_chunk() {
        let marker = b"__XHO_E_test__";
        let feed = b"file1 file2\n__XHO_E_test__:0\ndevops@host:~$ ";
        let (out, code, leftover) = scan(marker, &[feed]);
        assert_eq!(out, b"file1 file2\n");
        assert_eq!(code, 0);
        assert_eq!(leftover, b"devops@host:~$ ");
    }

    #[test]
    fn scanner_matches_marker_split_across_chunks() {
        let marker = b"__XHO_E_test__";
        let (out, code, _leftover) = scan(
            marker,
            &[
                b"file1\n__XHO_E_t",
                b"est__:42\ndevops@host:~$ ",
            ],
        );
        assert_eq!(out, b"file1\n");
        assert_eq!(code, 42);
    }

    #[test]
    fn scanner_lookahead_split_across_chunks() {
        // Marker complete in chunk 1, but the `:7\n` lookahead arrives later.
        let marker = b"__XHO_E_test__";
        let (out, code, _leftover) = scan(
            marker,
            &[b"out__XHO_E_test__", b":7\nprompt$ "],
        );
        assert_eq!(out, b"out");
        assert_eq!(code, 7);
    }

    /// The echoed command line contains the marker followed by `:%s` — it must
    /// NOT match. Scanning continues to the real sentinel.
    #[test]
    fn scanner_ignores_marker_in_echoed_command_line() {
        let marker = b"__XHO_E_test__";
        let echo = b"{ ls; }; status=$?; printf '__XHO_E_test__:%s\\n' \"$status\"\r\n";
        let output = b"file1\n__XHO_E_test__:0\n$ ";
        let (out, code, _leftover) = scan(marker, &[echo, output]);
        // Real output is recovered and the real sentinel is matched with code 0.
        assert!(out.ends_with(b"file1\n"));
        assert!(find_subslice(&out, marker).is_some()); // echo line forwarded
        assert_eq!(code, 0);
    }

    #[test]
    fn scanner_parses_nonzero_exit_codes() {
        let marker = b"__XHO_E_test__";
        let (_, code1, _) = scan(marker, &[b"__XHO_E_test__:1\n"]);
        assert_eq!(code1, 1);
        let (_, code255, _) = scan(marker, &[b"__XHO_E_test__:255\n"]);
        assert_eq!(code255, 255);
    }

    /// A real bastion is a PTY: `\n` is translated to `\r\n` (onlcr), so the
    /// sentinel arrives as `marker:0\r\n`. The scanner MUST accept CRLF — this
    /// is the form that actually occurs in production.
    #[test]
    fn scanner_matches_crlf_terminator() {
        let marker = b"__XHO_E_test__";
        let (out, code, leftover) =
            scan(marker, &[b"file1\r\n__XHO_E_test__:0\r\ndevops@host:~$ "]);
        assert_eq!(out, b"file1\r\n");
        assert_eq!(code, 0);
        assert_eq!(leftover, b"devops@host:~$ ");
    }

    #[test]
    fn scanner_matches_crlf_split_before_newline() {
        // `:0\r` in one chunk, `\n` in the next — the `\r` is held as incomplete.
        let marker = b"__XHO_E_test__";
        let (out, code, _) = scan(marker, &[b"out__XHO_E_test__:0\r", b"\n$ "]);
        assert_eq!(out, b"out");
        assert_eq!(code, 0);
    }

    #[test]
    fn scanner_marker_at_start_forwards_nothing() {
        let marker = b"__XHO_E_test__";
        let (out, code, leftover) = scan(marker, &[b"__XHO_E_test__:0\nafter$ "]);
        assert!(out.is_empty());
        assert_eq!(code, 0);
        assert_eq!(leftover, b"after$ ");
    }

    #[test]
    fn scanner_without_marker_forwards_everything() {
        // Many feeds, no marker -> all bytes forwarded, never done (code -1).
        let marker = b"__XHO_E_test__";
        let (out, code, _) = scan(marker, &[b"abc", b"def", b"ghi"]);
        assert_eq!(out, b"abcdefghi");
        assert_eq!(code, -1);
    }

    #[test]
    fn scanner_holds_partial_marker_suffix() {
        // Chunk ends with a prefix of the marker; it must be held, not flushed.
        let marker = b"__XHO_E_test__";
        let (out, code, _leftover) = scan(
            marker,
            &[b"data __XHO_E", b"_test__:0\n"],
        );
        assert_eq!(out, b"data ");
        assert_eq!(code, 0);
    }
}
