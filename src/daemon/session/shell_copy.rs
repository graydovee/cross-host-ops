// Shell-based copy for jumpserver: base64/tar over the navigated PTY.
//
// Unlike sftp-based copy (which launches sftp-server and switches to raw
// passthrough — consuming the PTY), shell-based copy runs ordinary shell
// commands via `run_command_plain` / `write_line`. The PTY shell stays at
// the asset prompt after each file, so it can be returned to the session
// cache for reuse.
//
// Upload: base64-encode file data, pipe through a heredoc to `base64 -d`.
// Download: `base64 -w 0` (single file) or `tar cf - | base64 -w 0` (recursive),
// capture stdout via run_command_plain, decode, emit CopyFrames.

use std::io::{Cursor, Read};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use tokio::sync::mpsc;

use crate::daemon::jumpserver_engine::PtyShell;
use crate::types::{CopyDirection, CopyFrame, CopySpec};

const HEREDOC_MARKER: &str = "XHO_CP_EOF";
const CHUNK_SIZE: usize = 32 * 1024;

/// Run a shell-based copy over a navigated PTY shell.
pub(crate) async fn run(shell: &mut PtyShell, spec: &mut CopySpec) -> Result<()> {
    match spec.direction {
        CopyDirection::Upload => upload(shell, spec).await,
        CopyDirection::Download => download(shell, spec).await,
    }
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

async fn upload(shell: &mut PtyShell, spec: &mut CopySpec) -> Result<()> {
    let mut upload_rx = spec
        .upload_rx
        .take()
        .ok_or_else(|| anyhow!("upload copy frame stream missing"))?;

    let mut current_file: Option<(String, Vec<u8>)> = None;

    while let Some(frame) = upload_rx.recv().await {
        match frame {
            CopyFrame::BeginFile { relative_path, .. } => {
                if current_file.is_some() {
                    bail!("copy stream began a new file before ending the previous file");
                }
                let dest = resolve_upload_path(&spec.remote_path, &relative_path, spec.recursive);
                current_file = Some((dest, Vec::new()));
            }
            CopyFrame::FileData { data } => {
                let file = current_file
                    .as_mut()
                    .ok_or_else(|| anyhow!("copy stream sent file data before BeginFile"))?;
                file.1.extend_from_slice(&data);
            }
            CopyFrame::EndFile => {
                let (dest, data) = current_file
                    .take()
                    .ok_or_else(|| anyhow!("copy stream sent EndFile before BeginFile"))?;
                upload_single_file(shell, &dest, &data).await?;
            }
            CopyFrame::BeginDirectory { relative_path, .. } => {
                if !spec.recursive {
                    bail!("remote directory frame requires recursive copy");
                }
                let dir = if relative_path.is_empty() {
                    spec.remote_path.clone()
                } else {
                    join_remote(&spec.remote_path, &relative_path)
                };
                run_shell_command(shell, &format!("mkdir -p {}", shell_quote(&dir))).await?;
            }
            CopyFrame::Symlink {
                relative_path,
                target,
            } => {
                let link_path =
                    resolve_upload_path(&spec.remote_path, &relative_path, spec.recursive);
                run_shell_command(
                    shell,
                    &format!("ln -sf {} {}", shell_quote(&target), shell_quote(&link_path)),
                )
                .await?;
            }
            CopyFrame::EndOfStream => break,
        }
    }
    if current_file.is_some() {
        bail!("copy stream ended before EndFile");
    }
    Ok(())
}

/// Upload a single file via base64 heredoc.
async fn upload_single_file(shell: &mut PtyShell, remote_path: &str, data: &[u8]) -> Result<()> {
    let encoded = BASE64.encode(data);
    shell.clear_prompt_remainder();
    // Write the heredoc start — shell enters heredoc mode and collects input.
    shell
        .write_line(&format!(
            "base64 -d > {} <<'{}'",
            shell_quote(remote_path),
            HEREDOC_MARKER
        ))
        .await?;
    // Stream the base64 data in chunks. The shell is in heredoc mode —
    // everything written is heredoc content until the terminator line.
    for chunk in encoded.as_bytes().chunks(CHUNK_SIZE) {
        shell.write_raw(chunk).await?;
        shell.write_raw(b"\r").await?;
    }
    // Terminate the heredoc — the shell runs `base64 -d` and writes the file.
    shell.write_line(HEREDOC_MARKER).await?;
    shell.wait_for_prompt().await?;
    shell.clear_pending();
    Ok(())
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

async fn download(shell: &mut PtyShell, spec: &mut CopySpec) -> Result<()> {
    let download_tx = spec
        .download_tx
        .take()
        .ok_or_else(|| anyhow!("download copy frame stream missing"))?;

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

    if is_dir {
        if !spec.recursive {
            bail!("copying a remote directory requires -r");
        }
        download_recursive(shell, &spec.remote_path, &download_tx).await?;
    } else {
        download_single_file(shell, &spec.remote_path, &spec.source_name, &download_tx).await?;
    }

    download_tx
        .send(CopyFrame::EndOfStream)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

/// Download a single file: `base64 -w 0 path` → decode → frames.
async fn download_single_file(
    shell: &mut PtyShell,
    remote_path: &str,
    source_name: &str,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    let raw = run_and_capture(
        shell,
        &format!("base64 -w 0 {} 2>/dev/null; echo", shell_quote(remote_path)),
    )
    .await?;
    let encoded = extract_base64(&raw);
    let data = BASE64
        .decode(encoded)
        .context("failed to decode base64 download payload")?;
    let name = Path::new(remote_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(source_name)
        .to_string();
    tx.send(CopyFrame::BeginFile {
        relative_path: name,
        mode: 0o644,
        size: data.len() as u64,
        mtime: 0,
    })
    .await
    .map_err(|_| anyhow!("download copy frame stream closed"))?;
    tx.send(CopyFrame::FileData { data })
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    tx.send(CopyFrame::EndFile)
        .await
        .map_err(|_| anyhow!("download copy frame stream closed"))?;
    Ok(())
}

/// Download a directory: `tar cf - name | base64 -w 0` → decode → frames.
async fn download_recursive(
    shell: &mut PtyShell,
    remote_path: &str,
    tx: &mpsc::Sender<CopyFrame>,
) -> Result<()> {
    let path = Path::new(remote_path);
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".to_string());
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .context("invalid remote path for recursive copy")?;

    let raw = run_and_capture(
        shell,
        &format!(
            "tar cf - -C {} {} 2>/dev/null | base64 -w 0; echo",
            shell_quote(&parent),
            shell_quote(name)
        ),
    )
    .await?;
    let encoded = extract_base64(&raw);
    let tar_bytes = BASE64
        .decode(encoded)
        .context("failed to decode base64 tar payload")?;

    // Parse the tar archive synchronously (tar::Entries is not Send).
    let frames = parse_tar_to_frames(&tar_bytes)?;

    for frame in frames {
        tx.send(frame)
            .await
            .map_err(|_| anyhow!("download copy frame stream closed"))?;
    }
    Ok(())
}

/// Parse a tar archive into CopyFrames. Done synchronously because
/// `tar::Entries` is not `Send`.
fn parse_tar_to_frames(tar_bytes: &[u8]) -> Result<Vec<CopyFrame>> {
    let mut archive = tar::Archive::new(Cursor::new(tar_bytes));
    let mut frames = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry.context("failed to read tar entry")?;
        let entry_path = entry.path().context("tar entry has invalid path")?;
        let relative = entry_path.to_string_lossy().to_string();

        if entry.header().entry_type().is_dir() {
            let clean = relative.trim_end_matches('/').to_string();
            frames.push(CopyFrame::BeginDirectory {
                relative_path: clean,
                mode: entry.header().mode().unwrap_or(0),
                mtime: entry.header().mtime().unwrap_or(0) as i64,
            });
            continue;
        }
        if entry.header().entry_type().is_symlink() {
            let target = entry
                .link_name()?
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            frames.push(CopyFrame::Symlink {
                relative_path: relative,
                target,
            });
            continue;
        }
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .context("failed to read tar file entry")?;
        frames.push(CopyFrame::BeginFile {
            relative_path: relative,
            mode: entry.header().mode().unwrap_or(0),
            size: data.len() as u64,
            mtime: entry.header().mtime().unwrap_or(0) as i64,
        });
        frames.push(CopyFrame::FileData { data });
        frames.push(CopyFrame::EndFile);
    }
    Ok(frames)
}

// ---------------------------------------------------------------------------
// PtyShell helpers
// ---------------------------------------------------------------------------

/// Run a single-line command that produces no output we care about (mkdir,
/// ln -s). Uses run_command_plain for prompt detection.
async fn run_shell_command(shell: &mut PtyShell, command: &str) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    shell.run_command_plain(command, &tx).await?;
    drop(tx);
    while rx.recv().await.is_some() {}
    Ok(())
}

/// Run a command and collect all stdout into a Vec.
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

/// Extract the base64 portion from captured output, stripping trailing
/// newlines and any leading non-base64 noise.
fn extract_base64(raw: &[u8]) -> &[u8] {
    let mut end = raw.len();
    while end > 0 && matches!(raw[end - 1], b'\n' | b'\r') {
        end -= 1;
    }
    let start = raw[..end]
        .iter()
        .position(|&b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'=')
        .unwrap_or(0);
    &raw[start..end]
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
