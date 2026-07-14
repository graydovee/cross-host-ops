// Shell-based copy for jumpserver: raw binary streaming over a `stty raw` PTY.
//
// This replaces the legacy base64/heredoc approach with a streaming binary
// protocol that is reliable, memory-constant, and gives real-time progress.
//
// Key design choices:
//   - `stty raw -echo` gives an 8-bit-clean channel (no ONLCR/ICRNL mangling),
//     so binary data passes through verbatim — no base64, no 33% bloat.
//   - Every command is wrapped in a UUID sentinel by `run_command_plain`, so
//     completion is detected by a strict marker match — no prompt sniffing,
//     no forgeable heredoc terminators.
//   - Upload uses `head -c <size>` (reads exactly N bytes — raw mode has no
//     EOF), fed chunk-by-chunk via `run_command_with_stdin`. Constant memory.
//   - Download uses `dd bs=16k` piped through `run_command_plain`'s streaming
//     sender — each PTY chunk becomes a CopyFrame::FileData immediately,
//     giving the CLI real-time progress updates.
//   - mode/mtime are preserved via `chmod`/`touch` after transfer.
//   - A `RawGuard` ensures that if anything fails while in raw mode, the shell
//     is cleaned up (Ctrl-C + force-close) and never returned to the cache.

use std::path::Path;

use anyhow::{Result, anyhow, bail};
use tokio::sync::mpsc;

use crate::daemon::jumpserver_engine::PtyShell;
use crate::types::{CopyDirection, CopyFrame, CopySpec};

/// Size of each chunk written to / read from the PTY in raw mode.
/// SSH channel windows are typically ~2 MB, so 16 KB is comfortably safe.
const RAW_CHUNK_SIZE: usize = 16 * 1024;

/// Run a shell-based copy over a navigated PTY shell.
pub(crate) async fn run(shell: &mut PtyShell, spec: &mut CopySpec) -> Result<()> {
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
/// is in cooked mode and can be cached for reuse.
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
    /// for normal exec/copy operations. Runs in raw mode (echo off).
    async fn complete(&mut self, shell: &mut PtyShell) -> Result<()> {
        if self.entered && !self.completed {
            let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let _ = shell.run_command_raw("stty sane", &tx).await;
            drop(tx);
            while rx.recv().await.is_some() {}
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

async fn upload(shell: &mut PtyShell, spec: &mut CopySpec) -> Result<()> {
    let upload_rx = spec
        .upload_rx
        .take()
        .ok_or_else(|| anyhow!("upload copy frame stream missing"))?;

    let mut guard = RawGuard::enter(shell).await?;

    let result = upload_loop(shell, upload_rx, &spec.remote_path, spec.recursive).await;

    match result {
        Ok(()) => guard.complete(shell).await,
        Err(e) => {
            guard.cleanup_and_close(shell).await;
            Err(e)
        }
    }
}

/// The upload loop drives the frame stream, and for each file it runs
/// `run_command_with_stdin` which concurrently feeds FileData chunks to the
/// remote `head -c` receiver while detecting completion via the UUID sentinel.
async fn upload_loop(
    shell: &mut PtyShell,
    mut upload_rx: mpsc::Receiver<CopyFrame>,
    remote_root: &str,
    recursive: bool,
) -> Result<()> {
    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginFile {
                relative_path,
                mode,
                size,
                mtime,
            } => {
                let dest = resolve_upload_path(remote_root, &relative_path, recursive);
                // upload_single_file_streaming takes ownership of upload_rx
                // (moves it into the feeder task) and returns it afterwards.
                upload_rx =
                    upload_single_file_streaming(shell, &dest, size, mode, mtime, upload_rx)
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

/// Upload one file using the three-phase binary protocol:
/// 1. Remote command prints a ready marker, then runs `head -c <size>`
/// 2. We feed FileData chunks after seeing the ready marker
/// 3. We wait for the end sentinel after all data is sent
///
/// Takes ownership of `upload_rx`, consumes FileData + EndFile frames,
/// returns the receiver for the outer loop.
async fn upload_single_file_streaming(
    shell: &mut PtyShell,
    dest: &str,
    size: u64,
    mode: u32,
    mtime: i64,
    upload_rx: mpsc::Receiver<CopyFrame>,
) -> Result<mpsc::Receiver<CopyFrame>> {
    let tmp = format!("{}.xho_tmp", dest);

    // Generate unique markers for this transfer.
    let ready_marker = format!("__XHO_CP_READY_{}__", uuid::Uuid::new_v4().simple());
    let end_marker = format!("__XHO_CP_END_{}__", uuid::Uuid::new_v4().simple());

    // Remote command: print ready marker, run head -c, print end marker + exit.
    let cmd = format!(
        "printf '{}'; head -c {} > {}; printf '{}:%s\\n' \"$?\"",
        ready_marker,
        size,
        shell_quote(&tmp),
        end_marker
    );

    // Bridge channel: feeder → upload_binary's data input.
    let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(4);

    // Feeder: pull FileData from upload_rx → data_tx. Returns rx + byte count.
    let ready_marker_bytes = ready_marker.clone().into_bytes();
    let end_marker_bytes = end_marker.clone().into_bytes();
    let feeder_task = tokio::spawn(async move {
        let mut total_sent = 0u64;
        let mut rx = upload_rx;
        loop {
            match rx.recv().await {
                Some(CopyFrame::FileData { data }) => {
                    total_sent += data.len() as u64;
                    if data_tx.send(data).await.is_err() {
                        return Err((anyhow!("upload data channel closed"), rx, total_sent));
                    }
                }
                Some(CopyFrame::EndFile) => {
                    drop(data_tx);
                    return Ok((rx, total_sent));
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

    let (rx, total_sent) = feeder_task
        .await
        .map_err(|e| anyhow!("feeder task panicked: {e}"))
        .and_then(|r| r.map_err(|(e, _rx, _)| e))?;

    if code != 0 {
        bail!("remote head -c failed (exit code {code})");
    }
    if total_sent != size {
        bail!(
            "upload size mismatch: declared {} bytes, sent {} bytes",
            size,
            total_sent
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

async fn download(shell: &mut PtyShell, spec: &mut CopySpec) -> Result<()> {
    let download_tx = spec
        .download_tx
        .take()
        .ok_or_else(|| anyhow!("download copy frame stream closed"))?;

    // Determine if the remote path is a file or directory.
    let kind_output = run_and_capture(
        shell,
        &format!(
            "test -d {} && echo XHO_DIR || echo XHO_FILE",
            shell_quote(&spec.remote_path)
        ),
    )
    .await?;
    let kind_str = String::from_utf8_lossy(&kind_output);
    let is_dir = kind_str.contains("XHO_DIR");

    // Enter raw mode for the actual data transfer.
    let mut guard = RawGuard::enter(shell).await?;

    let result = if is_dir {
        if !spec.recursive {
            guard.cleanup_and_close(shell).await;
            bail!("copying a remote directory requires -r");
        }
        download_recursive(shell, &spec.remote_path, &download_tx).await
    } else {
        download_single_file(shell, &spec.remote_path, &spec.source_name, &download_tx).await
    };

    match result {
        Ok(()) => {
            download_tx
                .send(CopyFrame::EndOfStream)
                .await
                .map_err(|_| anyhow!("download copy frame stream closed"))?;
            guard.complete(shell).await
        }
        Err(e) => {
            guard.cleanup_and_close(shell).await;
            Err(e)
        }
    }
}

/// Download a single file: `dd bs=16k if=path` → streaming FileData frames.
/// Uses run_command_plain's streaming sender so each PTY chunk becomes a
/// FileData immediately — real-time progress for the CLI.
async fn download_single_file(
    shell: &mut PtyShell,
    remote_path: &str,
    source_name: &str,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    // Get mode/mtime/size via stat (cooked-mode command, before raw transfer).
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

    // Send BeginFile with the real mode/mtime/size so the CLI's progress bar
    // has an accurate total for percentage/ETA.
    tx.send(CopyFrame::BeginFile {
        relative_path: name,
        mode,
        size,
        mtime,
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;

    // Stream the file via dd. run_command_plain forwards output chunks to the
    // sender; we relay each chunk as a FileData frame immediately — no buffering.
    let (ptx, mut prx) = mpsc::unbounded_channel::<Vec<u8>>();
    let dd_cmd = format!(
        "dd bs={} if={} 2>/dev/null",
        RAW_CHUNK_SIZE,
        shell_quote(remote_path)
    );
    let dd_result = shell.run_command_raw(&dd_cmd, &ptx).await;
    drop(ptx);

    // Forward chunks to the download channel as FileData frames.
    while let Some(chunk) = prx.recv().await {
        tx.send(CopyFrame::FileData { data: chunk })
            .await
            .map_err(|_| anyhow!("download copy frame stream closed"))?;
    }

    dd_result?;

    tx.send(CopyFrame::EndFile)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

/// Download a directory recursively: list the tree via `find`, then download
/// each file individually. This avoids buffering the entire tar archive in
/// memory. Symlinks and empty directories are handled as standalone frames.
async fn download_recursive(
    shell: &mut PtyShell,
    remote_path: &str,
    tx: &mpsc::Sender<CopyFrame>,
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
            // Regular file: download via streaming dd.
            _ => {
                let name = Path::new(&relative)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();
                tx.send(CopyFrame::BeginFile {
                    relative_path: relative,
                    mode,
                    size,
                    mtime,
                })
                .await
                .map_err(|_| anyhow!("download copy frame stream closed"))?;

                let (ptx, mut prx) = mpsc::unbounded_channel::<Vec<u8>>();
                let dd_cmd = format!(
                    "dd bs={} if={} 2>/dev/null",
                    RAW_CHUNK_SIZE,
                    shell_quote(full_path)
                );
                let dd_result = shell.run_command_raw(&dd_cmd, &ptx).await;
                drop(ptx);
                while let Some(chunk) = prx.recv().await {
                    tx.send(CopyFrame::FileData { data: chunk })
                        .await
                        .map_err(|_| anyhow!("download copy frame stream closed"))?;
                }
                dd_result?;
                let _ = name; // name unused for recursive (relative_path carries it)
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

/// Run a single-line command in cooked mode (echo is on). Used for `stty raw`,
/// `test -d`, and other pre-raw-mode probes.
async fn run_shell_command(shell: &mut PtyShell, command: &str) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    shell.run_command_plain(command, &tx).await?;
    drop(tx);
    while rx.recv().await.is_some() {}
    Ok(())
}

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

fn shell_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    let escaped = arg.replace('\'', "'\\''");
    format!("'{escaped}'")
}
