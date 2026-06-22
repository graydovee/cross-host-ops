//! Shared authorized_keys file helpers.
//!
//! Used by the SSH `auth_publickey` handler (read-only membership check) and
//! the `BootstrapAuthorize` RPC (idempotent append). Centralizing the parser
//! keeps the two paths consistent.

use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use russh::keys::ssh_key;

/// Check if a candidate public key is present in an authorized_keys file.
/// Returns Ok(false) if the file does not exist.
pub(super) fn is_authorized_key(path: &Path, candidate: &ssh_key::PublicKey) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    for (idx, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let first = line
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow!("invalid authorized_keys line {}", idx + 1))?;
        if first.contains('=') || first.contains(',') {
            bail!(
                "authorized_keys options are not supported in {} line {}",
                path.display(),
                idx + 1
            );
        }
        let parsed = ssh_key::PublicKey::from_openssh(line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), idx + 1))?;
        if parsed.key_data() == candidate.key_data() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Append a public key to an authorized_keys file if it is not already
/// present. Creates the file (and its parent dir) if missing, and ensures
/// mode 0600 on unix. Returns `(appended, already_present)`.
pub(super) async fn append_authorized_key(
    path: &Path,
    key: &ssh_key::PublicKey,
) -> Result<(bool, bool)> {
    if is_authorized_key(path, key)? {
        return Ok((false, true));
    }
    let line = key.to_openssh().context("failed to serialize public key")?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await;
    }
    Ok((true, false))
}
