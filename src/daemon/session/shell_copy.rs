// Shell-based copy for jumpserver: base64-payload streaming over a raw PTY.
//
// Key design choices:
//   - `stty raw -echo` gives a clean channel (no ONLCR/ICRNL mangling), so
//     the base64 wire stream passes through verbatim.
//   - Every command is wrapped in a UUID sentinel by `run_command_plain`, so
//     completion is detected by a strict marker match — no prompt sniffing,
//     no forgeable heredoc terminators.
//   - ALL payloads travel as base64 (76-col MIME lines): bastion content
//     inspectors have been observed to wedge on structured byte sequences
//     (e.g. long periodic runs) in raw binary, while a safe-alphabet,
//     line-wrapped stream never triggers them. Safety over the 33% bloat.
//   - Download runs `base64 <path>` (`tail -c +N | base64` to resume) through
//     the raw runner's streaming decoder; a concurrent forwarder turns
//     decoded chunks into CopyFrame::FileData immediately.
//   - Upload encodes via the streaming encoder and feeds a length-bounded
//     remote receiver `head -c <wire_len> | base64 -d > tmp` — `head` bounds
//     the read (raw mode has no EOF) and its exit closes the pipe so
//     `base64 -d` finishes and the end sentinel fires.
//   - Remote dependencies: only coreutils/busybox `base64`, `head`, `tail`.
//   - mode/mtime are preserved via `chmod`/`touch` after transfer.
//   - A `RawGuard` ensures that if anything fails while in raw mode, the shell
//     is cleaned up (Ctrl-C + force-close) and never returned to the cache.
//
// `run` consumes the shell and returns it on success only when it is verified
// reusable (`stty sane` round-trip included); on error the shell is dropped.

use std::path::Path;

use anyhow::{Result, anyhow, bail};
use tokio::sync::mpsc;

use crate::daemon::jumpserver_engine::PtyShell;
use crate::daemon::session::b64;
use crate::types::{CopyDirection, CopyFrame, CopySpec};

/// True when an error looks like a relay wedge: the PTY went silent (channel
/// still open) or died mid-operation. Such failures are worth retrying on a
/// fresh session/transport; other errors (bad path, permission) are not.
pub(crate) fn is_stall_class(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("timed out waiting for shell output")
        || message.contains("shell closed unexpectedly")
}

/// Run a shell-based copy over a navigated PTY shell. Consumes the shell;
/// returns it only when the transfer completed and the shell was restored to
/// a verified-clean state (safe to return to the session cache).
pub(crate) async fn run(shell: PtyShell, spec: &mut CopySpec) -> Result<PtyShell> {
    match spec.direction {
        CopyDirection::Upload => upload(shell, spec).await,
        CopyDirection::Download => download(shell, spec).await,
    }
}

// ---------------------------------------------------------------------------
// RawGuard — RAII-style raw-mode management with error cleanup
// ---------------------------------------------------------------------------

/// Manages the `stty raw -echo` ↔ `stty sane` lifecycle around a copy session.
///
/// Does NOT hold a borrow of PtyShell — the shell is passed by `&mut` to each
/// method so the caller can use it freely between calls. This struct only
/// tracks whether raw mode was entered and whether it was cleanly exited.
///
/// **On success**: call `complete(shell)` which sends `stty sane` — the shell
/// is in cooked mode and can be cached for reuse. A failed `stty sane`
/// round-trip is an error: the shell's tty state is unknown.
///
/// **On error**: call `cleanup_and_close(shell)` which sends Ctrl-C +
/// force-closes the channel. After this, the shell must be discarded.
struct RawGuard {
    /// True after `stty raw -echo` succeeds. Controls whether cleanup runs.
    entered: bool,
    /// True after `complete()` runs. Prevents double-cleanup.
    completed: bool,
}

impl RawGuard {
    /// Enter raw mode: `stty raw -echo` (disables echo + line processing).
    /// PS1 is NOT cleared — instead, data commands use a "begin marker" to
    /// discard any prompt/noise before the real output. Verified via
    /// run_command_plain sentinel (runs in cooked mode before the switch).
    async fn enter(shell: &mut PtyShell) -> Result<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let code = shell.run_command_plain("stty raw -echo", &tx).await?;
        drop(tx);
        while rx.recv().await.is_some() {}
        if code != 0 {
            bail!("stty raw -echo failed (exit code {code})");
        }
        Ok(Self {
            entered: true,
            completed: false,
        })
    }

    /// Exit raw mode on success: `stty sane`. After this the shell is reusable
    /// for normal exec/copy operations. Runs in raw mode (echo off). The
    /// round-trip must succeed — a shell left in raw mode must not be cached.
    async fn complete(&mut self, shell: &mut PtyShell) -> Result<()> {
        if self.entered && !self.completed {
            let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let result = shell.run_command_raw("stty sane", &tx).await;
            drop(tx);
            while rx.recv().await.is_some() {}
            result?;
        }
        self.completed = true;
        Ok(())
    }

    /// Cleanup on error: the shell is stuck in raw mode. Send Ctrl-C to
    /// interrupt any lingering `head`/`dd`, then force-close the channel.
    /// After this, the caller must discard the shell — never cache it.
    async fn cleanup_and_close(&mut self, shell: &mut PtyShell) {
        if self.entered && !self.completed {
            shell.send_interrupt().await;
            shell.force_close().await;
        }
        self.completed = true;
    }
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

async fn upload(shell: PtyShell, spec: &mut CopySpec) -> Result<PtyShell> {
    let mut shell = shell;
    let upload_rx = spec
        .upload_rx
        .take()
        .ok_or_else(|| anyhow!("upload copy frame stream missing"))?;

    let mut guard = RawGuard::enter(&mut shell).await?;

    // Effective resume offsets were already probed by the gateway (the CLI
    // waits for the resume_ack before streaming, so incoming frames start at
    // `offset` and the remote receiver appends to the partial).
    let result = upload_loop(
        &mut shell,
        upload_rx,
        &spec.remote_path,
        spec.recursive,
        &spec.resume,
    )
    .await;

    match result {
        Ok(()) => {
            guard.complete(&mut shell).await?;
            Ok(shell)
        }
        Err(e) => {
            guard.cleanup_and_close(&mut shell).await;
            Err(e)
        }
    }
}

/// The upload loop drives the frame stream: for each file it starts the
/// length-bounded remote receiver and concurrently feeds base64-encoded
/// FileData chunks, detecting completion via the UUID sentinel.
async fn upload_loop(
    shell: &mut PtyShell,
    mut upload_rx: mpsc::Receiver<CopyFrame>,
    remote_root: &str,
    recursive: bool,
    resume: &[crate::types::ResumeEntry],
) -> Result<()> {
    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginFile {
                relative_path,
                mode,
                size,
                mtime,
                start_offset,
            } => {
                let dest = resolve_upload_path(remote_root, &relative_path, recursive);
                // Resume offset comes from the frame itself: the client
                // verified the remote partial against its source prefix
                // (resume_ack handshake) and streams only the remaining
                // suffix; the receiver appends instead of truncating.
                let skip = start_offset.min(size);
                let _ = resume; // decisions live in the frames + ack now
                // upload_single_file_streaming takes ownership of upload_rx
                // (moves it into the feeder task) and returns it afterwards.
                upload_rx =
                    upload_single_file_streaming(shell, &dest, size, mode, mtime, upload_rx, skip)
                        .await?;
            }
            CopyFrame::FileData { .. } => {
                bail!("copy stream sent file data outside of a BeginFile/EndFile pair");
            }
            CopyFrame::EndFile => {
                bail!("copy stream sent EndFile without a preceding BeginFile");
            }
            CopyFrame::BeginDirectory { relative_path, .. } => {
                if !recursive {
                    bail!("remote directory frame requires recursive copy");
                }
                let dir = if relative_path.is_empty() {
                    remote_root.to_string()
                } else {
                    join_remote(remote_root, &relative_path)
                };
                run_shell_command_raw(shell, &format!("mkdir -p {}", shell_quote(&dir))).await?;
            }
            CopyFrame::Symlink {
                relative_path,
                target,
            } => {
                let link_path = resolve_upload_path(remote_root, &relative_path, recursive);
                run_shell_command_raw(
                    shell,
                    &format!(
                        "ln -sf {} {}",
                        shell_quote(&target),
                        shell_quote(&link_path)
                    ),
                )
                .await?;
            }
            CopyFrame::EndOfStream => break,
        }
    }
    Ok(())
}

/// Upload one file using the three-phase base64 protocol:
/// 1. Remote command prints a ready marker, then runs the length-bounded
///    receiver `head -c <wire_len> | base64 -d > tmp`
/// 2. We feed base64-encoded FileData chunks after seeing the ready marker
/// 3. We wait for the end sentinel after all data is sent
///
/// `head -c` bounds the read (raw mode has no EOF) and its exit closes the
/// pipe so `base64 -d` sees EOF and the pipeline finishes — an invalid wire
/// stream makes `base64 -d` exit non-zero, which the sentinel surfaces.
///
/// Takes ownership of `upload_rx`, consumes FileData + EndFile frames,
/// returns the receiver for the outer loop.
#[allow(clippy::too_many_arguments)]
async fn upload_single_file_streaming(
    shell: &mut PtyShell,
    dest: &str,
    size: u64,
    mode: u32,
    mtime: i64,
    upload_rx: mpsc::Receiver<CopyFrame>,
    skip: u64,
) -> Result<mpsc::Receiver<CopyFrame>> {
    let tmp = format!("{}.xho_tmp", dest);
    let remaining = size - skip;

    // Generate unique markers for this transfer.
    let ready_marker = format!("__XHO_CP_READY_{}__", uuid::Uuid::new_v4().simple());
    let end_marker = format!("__XHO_CP_END_{}__", uuid::Uuid::new_v4().simple());

    // Remote command: print ready marker, run the receiver, print end marker.
    // Resuming appends to the partial; a fresh transfer truncates it.
    let wire_len = b64::wire_len_for(remaining);
    let redirect = if skip > 0 { ">>" } else { ">" };
    let receiver = format!(
        "head -c {} | base64 -d {} {} 2>/dev/null",
        wire_len,
        redirect,
        shell_quote(&tmp)
    );
    let cmd = format!(
        "printf '{}'; {{ {}; }}; printf '{}:%s\\n' \"$?\"",
        ready_marker, receiver, end_marker
    );

    // Bridge channel: feeder → upload_binary's data input.
    let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(4);

    // Feeder: pull FileData from upload_rx → base64-encode → data_tx.
    // Returns rx + byte counts.
    let ready_marker_bytes = ready_marker.clone().into_bytes();
    let end_marker_bytes = end_marker.clone().into_bytes();
    let feeder_task = tokio::spawn(async move {
        let mut total_sent = 0u64;
        let mut rx = upload_rx;
        let mut encoder = b64::B64Encoder::new();
        loop {
            match rx.recv().await {
                Some(CopyFrame::FileData { data }) => {
                    total_sent += data.len() as u64;
                    let wire = encoder.encode(&data);
                    if !wire.is_empty() && data_tx.send(wire).await.is_err() {
                        return Err((anyhow!("upload data channel closed"), rx, total_sent));
                    }
                }
                Some(CopyFrame::EndFile) => {
                    let tail = encoder.finish();
                    if !tail.is_empty() && data_tx.send(tail).await.is_err() {
                        return Err((anyhow!("upload data channel closed"), rx, total_sent));
                    }
                    drop(data_tx);
                    return Ok((rx, total_sent, encoder.encoded_total()));
                }
                Some(other) => {
                    return Err((
                        anyhow!("unexpected frame during file upload: {:?}", other),
                        rx,
                        total_sent,
                    ));
                }
                None => {
                    return Err((
                        anyhow!("upload stream closed during file transfer"),
                        rx,
                        total_sent,
                    ));
                }
            }
        }
    });

    // Drive the three-phase upload: wait ready → feed data → wait end.
    let code = shell
        .upload_binary(&cmd, &ready_marker_bytes, &end_marker_bytes, data_rx)
        .await?;

    let (rx, total_sent, wire_sent) = feeder_task
        .await
        .map_err(|e| anyhow!("feeder task panicked: {e}"))
        .and_then(|r| r.map_err(|(e, _rx, _)| e))?;

    if code != 0 {
        bail!("remote receiver failed (exit code {code})");
    }
    if total_sent != remaining {
        bail!(
            "upload size mismatch: expected {} remaining bytes, sent {}",
            remaining,
            total_sent
        );
    }
    if wire_sent != wire_len {
        bail!(
            "upload wire length mismatch: expected {} base64 bytes, sent {}",
            wire_len,
            wire_sent
        );
    }

    finalize_upload(shell, &tmp, dest, mode, mtime).await?;
    Ok(rx)
}

/// Set mode/mtime and atomically rename tmp → dest.
async fn finalize_upload(
    shell: &mut PtyShell,
    tmp: &str,
    dest: &str,
    mode: u32,
    mtime: i64,
) -> Result<()> {
    // Mask to permission bits only (S_ISUID|S_ISGID|S_ISVTX|rwxrwxrwx).
    // The mode from CopyFrame::BeginFile includes the file-type bits (e.g.
    // 0o100755 for a regular executable), which chmod rejects.
    let perm = mode & 0o7777;
    if perm != 0 {
        run_shell_command_raw(shell, &format!("chmod {:o} {}", perm, shell_quote(tmp))).await?;
    }
    if mtime != 0 {
        run_shell_command_raw(shell, &format!("touch -d @{} {}", mtime, shell_quote(tmp))).await?;
    }
    run_shell_command_raw(
        shell,
        &format!("mv {} {}", shell_quote(tmp), shell_quote(dest)),
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

/// Per-file download progress ledger: updated by forwarders as decoded bytes
/// stream out, written back into `spec.resume` on failure so a retry (within
/// the invocation or a fresh `--resume` run) continues from the forwarded
/// byte count. Entries carry the source size/mtime so a retry re-validates
/// the remote file has not changed.
#[derive(Default)]
struct ResumeLedger {
    entries: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, crate::types::ResumeEntry>>,
    >,
}

impl ResumeLedger {
    fn from(entries: &[crate::types::ResumeEntry]) -> Self {
        let map: std::collections::HashMap<String, crate::types::ResumeEntry> = entries
            .iter()
            .cloned()
            .map(|e| (e.relative_path.clone(), e))
            .collect();
        Self {
            entries: std::sync::Arc::new(std::sync::Mutex::new(map)),
        }
    }

    fn hint(&self, name: &str) -> Option<crate::types::ResumeEntry> {
        self.entries.lock().unwrap().get(name).cloned()
    }

    fn update(&self, name: &str, offset: u64, size: u64, mtime: i64) {
        self.entries.lock().unwrap().insert(
            name.to_string(),
            crate::types::ResumeEntry {
                relative_path: name.to_string(),
                offset,
                size,
                mtime,
                partial_sha256: String::new(),
            },
        );
    }

    fn into_entries(self) -> Vec<crate::types::ResumeEntry> {
        let mut entries: Vec<_> = self.entries.lock().unwrap().values().cloned().collect();
        entries.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        entries
    }
}

async fn download(shell: PtyShell, spec: &mut CopySpec) -> Result<PtyShell> {
    let mut shell = shell;
    let download_tx = spec
        .download_tx
        .take()
        .ok_or_else(|| anyhow!("download copy frame stream closed"))?;

    // Everything after the take() runs inside one block so the error path
    // below restores the frame sender for gateway retries on ANY failure —
    // an early `?` escape would otherwise leave the channel consumed and the
    // retry would die with "frame stream closed".
    let ledger = ResumeLedger::from(&spec.resume);
    let result = async {
        tracing::info!(encoding = "base64", remote_path = %spec.remote_path, "shell copy starting");
        // Determine if the remote path is a file or directory.
        let kind_output = run_and_capture(
            &mut shell,
            &format!(
                "test -d {} && echo XHO_DIR || echo XHO_FILE",
                shell_quote(&spec.remote_path)
            ),
        )
        .await?;
        let kind_str = String::from_utf8_lossy(&kind_output);
        let is_dir = kind_str.contains("XHO_DIR");

        // Enter raw mode for the actual data transfer.
        let mut guard = RawGuard::enter(&mut shell).await?;

        let inner = if is_dir {
            if !spec.recursive {
                guard.cleanup_and_close(&mut shell).await;
                bail!("copying a remote directory requires -r");
            }
            download_recursive(&mut shell, &spec.remote_path, &download_tx, &ledger).await
        } else {
            download_single_file(
                &mut shell,
                &spec.remote_path,
                &spec.source_name,
                &download_tx,
                &ledger,
            )
            .await
        };

        match inner {
            Ok(()) => {
                download_tx
                    .send(CopyFrame::EndOfStream)
                    .await
                    .map_err(|_| anyhow!("download copy frame stream closed"))?;
                guard.complete(&mut shell).await?;
                Ok(shell)
            }
            Err(e) => {
                guard.cleanup_and_close(&mut shell).await;
                Err(e)
            }
        }
    }
    .await;

    match result {
        Ok(shell) => Ok(shell),
        Err(e) => {
            // Give the frame sender back (and record per-file progress) so a
            // stall-class retry in the gateway resumes instead of restarting.
            spec.download_tx = Some(download_tx);
            spec.resume = ledger.into_entries();
            Err(e)
        }
    }
}

/// Download a single file: stream via the raw runner's base64 decoder into
/// FileData frames. A concurrent forwarder relays decoded chunks as they
/// arrive — the CLI sees real-time progress and memory stays proportional to
/// in-flight chunks. A validated resume hint skips the already-delivered
/// prefix (`tail -c +N | base64`) and reports it via `BeginFile.start_offset`.
async fn download_single_file(
    shell: &mut PtyShell,
    remote_path: &str,
    source_name: &str,
    tx: &mpsc::Sender<CopyFrame>,
    ledger: &ResumeLedger,
) -> Result<()> {
    // Get mode/mtime/size via stat. We're in raw mode, so use run_and_capture_raw
    // (skips drain_echo_line which would eat the output in raw mode).
    let stat_output = run_and_capture_raw(
        shell,
        &format!(
            "stat -c '%a %Y %s' {} 2>/dev/null",
            shell_quote(remote_path)
        ),
    )
    .await?;
    let stat_str = String::from_utf8_lossy(&stat_output);
    let parts: Vec<&str> = stat_str.split_whitespace().collect();
    let (mode, mtime, size) = if parts.len() >= 3 {
        (
            u32::from_str_radix(parts[0], 8).unwrap_or(0),
            parts[1].parse::<i64>().unwrap_or(0),
            parts[2].parse::<u64>().unwrap_or(0),
        )
    } else {
        (0o644, 0, 0)
    };

    let name = Path::new(remote_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(source_name)
        .to_string();

    // A resume hint is honored only when the remote source is unchanged
    // (size + mtime match) and the offset is sane; otherwise restart at 0.
    let start = ledger
        .hint(&name)
        .filter(|h| h.size == size && h.mtime == mtime && h.offset > 0 && h.offset <= size)
        .map(|h| h.offset)
        .unwrap_or(0);

    // Send BeginFile with the real mode/mtime/size so the CLI's progress bar
    // has an accurate total, and the effective start offset so it knows to
    // append rather than truncate.
    tx.send(CopyFrame::BeginFile {
        relative_path: name.clone(),
        mode,
        size,
        mtime,
        start_offset: start,
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;

    let progress = std::sync::Arc::new(std::sync::Mutex::new(start));
    let code = stream_remote_file(shell, remote_path, size, start, tx, &progress).await;
    let forwarded = *progress.lock().unwrap();
    ledger.update(&name, forwarded.min(size), size, mtime);

    let code = code?;
    if code != 0 {
        bail!("remote read of {} failed (exit code {code})", remote_path);
    }

    tx.send(CopyFrame::EndFile)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

/// Drive the raw-mode base64 reader and forward decoded payload chunks to
/// `tx` concurrently with the read. `skip` is the decoded byte offset to
/// start from (0 for a fresh transfer; >0 when resuming); `progress` is
/// updated with the total forwarded byte count (initialized to `skip`).
async fn stream_remote_file(
    shell: &mut PtyShell,
    remote_path: &str,
    size: u64,
    skip: u64,
    tx: &mpsc::Sender<CopyFrame>,
    progress: &std::sync::Arc<std::sync::Mutex<u64>>,
) -> Result<i32> {
    let (ptx, mut prx) = mpsc::unbounded_channel::<Vec<u8>>();
    let forward_tx = tx.clone();
    let forward_progress = progress.clone();
    let forwarder = tokio::spawn(async move {
        while let Some(chunk) = prx.recv().await {
            if forward_tx
                .send(CopyFrame::FileData {
                    data: chunk.clone(),
                })
                .await
                .is_err()
            {
                break;
            }
            *forward_progress.lock().unwrap() += chunk.len() as u64;
        }
    });

    let cmd = base64_read_command(remote_path, skip);
    let read_result = shell
        .run_command_raw_b64(&cmd, size.saturating_sub(skip), &ptx)
        .await;
    drop(ptx);
    let _ = forwarder.await;
    read_result
}

/// Download a directory recursively: list the tree via `find`, then download
/// each file individually. This avoids buffering the entire tar archive in
/// memory. Symlinks and empty directories are handled as standalone frames.
async fn download_recursive(
    shell: &mut PtyShell,
    remote_path: &str,
    tx: &mpsc::Sender<CopyFrame>,
    ledger: &ResumeLedger,
) -> Result<()> {
    // List the tree: type, mode, mtime, size, relative-path.
    //   f = regular file, d = directory, l = symlink (target in extra field)
    // find -printf '%y\t%m\t%T@\t%s\t%p\n' and for symlinks '%y\t%m\t%T@\t%s\t%p\t%l\n'
    let listing = run_and_capture_raw(
        shell,
        &format!(
            "find {} -printf '%y\\t%m\\t%T@\\t%s\\t%p\\t%l\\n' 2>/dev/null",
            shell_quote(remote_path)
        ),
    )
    .await?;
    let listing_str = String::from_utf8_lossy(&listing);

    let base = Path::new(remote_path);
    let base_name = base.file_name().and_then(|n| n.to_str()).unwrap_or("");

    for line in listing_str.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 5 {
            continue;
        }
        let ftype = fields[0].chars().next().unwrap_or('f');
        let mode = u32::from_str_radix(fields[1], 8).unwrap_or(0);
        let mtime = fields[2]
            .split('.')
            .next()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let size = fields[3].parse::<u64>().unwrap_or(0);
        let full_path = fields[4];
        // Compute relative path from the download root.
        let relative = if base_name.is_empty() {
            full_path.to_string()
        } else {
            full_path
                .strip_prefix(remote_path)
                .unwrap_or(full_path)
                .trim_start_matches('/')
                .to_string()
        };

        match ftype {
            'd' => {
                tx.send(CopyFrame::BeginDirectory {
                    relative_path: relative,
                    mode,
                    mtime,
                })
                .await
                .map_err(|_| anyhow!("download copy frame stream closed"))?;
            }
            'l' => {
                let target = fields.get(5).map(|s| s.to_string()).unwrap_or_default();
                tx.send(CopyFrame::Symlink {
                    relative_path: relative,
                    target,
                })
                .await
                .map_err(|_| anyhow!("download copy frame stream closed"))?;
            }
            // Regular file: download via streaming reader. Resume hints by
            // relative path (in-invocation retries; a fresh CLI run with
            // `--resume` does not send recursive hints in v1).
            _ => {
                let start = ledger
                    .hint(&relative)
                    .filter(|h| {
                        h.size == size && h.mtime == mtime && h.offset > 0 && h.offset <= size
                    })
                    .map(|h| h.offset)
                    .unwrap_or(0);
                tx.send(CopyFrame::BeginFile {
                    relative_path: relative.clone(),
                    mode,
                    size,
                    mtime,
                    start_offset: start,
                })
                .await
                .map_err(|_| anyhow!("download copy frame stream closed"))?;

                let progress = std::sync::Arc::new(std::sync::Mutex::new(start));
                let code = stream_remote_file(shell, full_path, size, start, tx, &progress).await;
                let forwarded = *progress.lock().unwrap();
                ledger.update(&relative, forwarded.min(size), size, mtime);
                let code = code?;
                if code != 0 {
                    bail!("remote read of {} failed (exit code {code})", full_path);
                }
                tx.send(CopyFrame::EndFile)
                    .await
                    .map_err(|_| anyhow!("download copy frame stream closed"))?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// PtyShell helpers
// ---------------------------------------------------------------------------

/// Run a single-line command in raw mode (echo is off — skip drain_echo_line).
/// Used for mkdir/chmod/touch/mv/ln during raw-mode upload sessions.
async fn run_shell_command_raw(shell: &mut PtyShell, command: &str) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    shell.run_command_raw(command, &tx).await?;
    drop(tx);
    while rx.recv().await.is_some() {}
    Ok(())
}

/// Run a command in cooked mode and collect all stdout into a Vec.
async fn run_and_capture(shell: &mut PtyShell, command: &str) -> Result<Vec<u8>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    shell.run_command_plain(command, &tx).await?;
    drop(tx);
    let mut output = Vec::new();
    while let Some(chunk) = rx.recv().await {
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

/// Run a command in raw mode and collect all stdout into a Vec. Used for stat
/// and find listing during raw-mode download sessions.
async fn run_and_capture_raw(shell: &mut PtyShell, command: &str) -> Result<Vec<u8>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    shell.run_command_raw(command, &tx).await?;
    drop(tx);
    let mut output = Vec::new();
    while let Some(chunk) = rx.recv().await {
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

// ---------------------------------------------------------------------------
// Upload resume probe (used by the gateway before the CLI streams frames)
// ---------------------------------------------------------------------------

/// Probe a remote upload partial `<dest>.xho_tmp`: returns `(size,
/// sha256-of-the-whole-partial)` when it exists. The hash is computed on the
/// remote host (`sha256sum`) — no partial bytes cross the link. Cooked-mode
/// commands, safe to run on a cached shell.
pub(crate) async fn probe_upload_partial(
    shell: &mut PtyShell,
    dest: &str,
) -> Option<(u64, String)> {
    let tmp = format!("{}.xho_tmp", dest);
    let quoted = shell_quote(&tmp);
    let output = run_and_capture(
        shell,
        &format!("stat -c %s {quoted} 2>/dev/null && sha256sum {quoted} 2>/dev/null"),
    )
    .await
    .ok()?;
    let text = String::from_utf8_lossy(&output);
    let mut fields = text.split_whitespace();
    let size = fields.next()?.parse::<u64>().ok()?;
    let hash = fields.next()?.to_string();
    Some((size, hash))
}

/// Report remote partial state for the CLI's resume decision (single-file
/// uploads; recursive uploads transfer fresh in v1). Entries carry the
/// partial's size and FULL sha256; the client verifies its own source
/// prefix against the hash and chooses the offset it streams from — the
/// daemon does not decide append-vs-fresh here (a head fingerprint cannot
/// distinguish append-growth from a same-header rewrite; only the whole
/// prefix can).
pub(crate) async fn probe_upload_resume(
    shell: &mut PtyShell,
    spec: &CopySpec,
) -> Vec<crate::types::ResumeEntry> {
    let mut out = Vec::with_capacity(spec.resume.len());
    for entry in &spec.resume {
        let mut effective = entry.clone();
        effective.offset = 0;
        effective.partial_sha256 = String::new();
        if !spec.recursive {
            if let Some((tmp_size, hash)) = probe_upload_partial(shell, &spec.remote_path).await {
                if tmp_size > 0 && tmp_size <= entry.size {
                    effective.offset = tmp_size;
                    effective.partial_sha256 = hash;
                }
            }
        }
        out.push(effective);
    }
    out
}

fn resolve_upload_path(remote_root: &str, relative_path: &str, recursive: bool) -> String {
    if !recursive || relative_path.is_empty() {
        return remote_root.to_string();
    }
    join_remote(remote_root, relative_path)
}

fn join_remote(root: &str, relative: &str) -> String {
    let root = root.trim_end_matches('/');
    format!("{root}/{relative}")
}

/// Remote command that streams `remote_path` base64-encoded, starting at
/// decoded byte `skip` (`tail -c +N` emits from byte N, 1-based).
pub(crate) fn base64_read_command(remote_path: &str, skip: u64) -> String {
    if skip > 0 {
        format!(
            "tail -c +{} {} | base64 2>/dev/null",
            skip + 1,
            shell_quote(remote_path)
        )
    } else {
        format!("base64 {} 2>/dev/null", shell_quote(remote_path))
    }
}

fn shell_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::base64_read_command;

    #[test]
    fn base64_read_command_shapes() {
        assert_eq!(
            base64_read_command("/tmp/a b.bin", 0),
            "base64 '/tmp/a b.bin' 2>/dev/null"
        );
        // Resume: `tail -c +N` starts at byte N (1-based), piped INTO base64.
        assert_eq!(
            base64_read_command("/tmp/a b.bin", 10),
            "tail -c +11 '/tmp/a b.bin' | base64 2>/dev/null"
        );
    }
}
