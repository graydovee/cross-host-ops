//! Cross-platform filesystem-metadata helpers.
//!
//! `CopyFrame` carries Unix-style `mode`/`mtime` fields that the daemon uses to
//! reproduce file permissions/timestamps on the (typically Unix) target. On the
//! *client* side we may run on Windows, where `MetadataExt`/`PermissionsExt`
//! don't exist. This module adapts the two platforms behind a single API:
//!
//! - reading: [`entry_mode`]/[`entry_mtime`] return a best-effort value
//!   (real on Unix, a sensible default on Windows) so the protocol fields are
//!   always populated.
//! - writing: [`apply_mode`] is a no-op on Windows, since `chmod`-style mode
//!   bits have no direct equivalent.
//! - symlinks: [`create_symlink`] maps to the platform's native symlink call.

use std::fs::Metadata;
use std::path::Path;
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

/// Best-effort Unix mode bits for a metadata entry.
///
/// On Unix this is the real st_mode. On Windows there is no equivalent, so we
/// return a conventional default: `0o755` for directories, `0o644` for files.
/// These are advisory; the receiving Unix side applies them when non-zero.
pub fn entry_mode(metadata: &Metadata) -> u32 {
    #[cfg(unix)]
    {
        metadata.permissions().mode()
    }
    #[cfg(not(unix))]
    {
        if metadata.is_dir() {
            0o755
        } else {
            0o644
        }
    }
}

/// Best-effort modification time, as seconds since the Unix epoch.
///
/// On Unix this is st_mtime directly. On Windows we fall back to
/// `SystemTime::modified()` converted to epoch seconds (or 0 on failure).
pub fn entry_mtime(metadata: &Metadata) -> i64 {
    #[cfg(unix)]
    {
        metadata.mtime()
    }
    #[cfg(not(unix))]
    {
        metadata
            .modified()
            .ok()
            .and_then(epoch_secs)
            .unwrap_or(0)
    }
}

#[cfg(not(unix))]
fn epoch_secs(t: SystemTime) -> Option<i64> {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() as i64)
}

/// Apply a Unix mode to a path, if the platform supports it.
///
/// No-op on Windows (mode bits have no direct equivalent; ACLs would be the
/// proper mechanism but are out of scope for copy fidelity).
pub fn apply_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    if mode == 0 {
        return Ok(());
    }
    #[cfg(unix)]
    {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Create a symbolic link `target` at `link`, using the platform's native call.
///
/// On Windows, file symlinks require the SeCreateSymbolicLink privilege (or
/// Developer Mode); on failure we return the underlying error so the caller can
/// log/skip. Directory symlinks use `symlink_dir`.
pub fn create_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link)
    }
    #[cfg(windows)]
    {
        // Pick file vs directory symlink based on the *link*'s intended type is
        // not knowable in general; the copy path resolves this by inspecting the
        // source metadata before calling. For files we use symlink_file.
        std::os::windows::fs::symlink_file(target, link)
            .or_else(|_| std::os::windows::fs::symlink_dir(target, link))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (target, link);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "symlinks are not supported on this platform",
        ))
    }
}
