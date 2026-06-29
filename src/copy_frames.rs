use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};

pub(crate) fn path_to_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("path is not valid UTF-8: {}", path.display()))
}

pub(crate) fn relative_path_to_string(path: &Path) -> Result<String> {
    validate_relative_path(path)?;
    path_to_string(path)
}

pub(crate) fn validate_relative_path(path: &Path) -> Result<()> {
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

pub(crate) fn join_relative_path(root: &Path, relative_path: &str) -> Result<PathBuf> {
    if relative_path.is_empty() {
        return Ok(root.to_path_buf());
    }
    let relative = Path::new(relative_path);
    validate_relative_path(relative)?;
    Ok(root.join(relative))
}

pub(crate) fn non_empty_name(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn copy_entry_name(relative_path: &str, source_name: &str, fallback: &str) -> String {
    Path::new(relative_path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .or_else(|| (!source_name.trim().is_empty()).then_some(source_name.trim()))
        .unwrap_or(fallback)
        .to_string()
}

pub(crate) fn local_basename(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("failed to derive basename from {}", path.display()))
}

pub(crate) async fn path_is_existing_dir(path: &Path) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_dir()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

pub(crate) async fn validate_upload_source(path: &Path, recursive: bool) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to inspect upload source {}", path.display()))?;
    if metadata.is_dir() && !recursive {
        bail!(
            "{} is a directory; use -r to copy directories",
            path.display()
        );
    }
    Ok(())
}
