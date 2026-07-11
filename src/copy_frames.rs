//! Thin re-export layer over [`crate::filepath`].
//!
//! Historically the copy path helpers lived here; they have been consolidated
//! into the `filepath` module. This module re-exports them so existing call
//! sites (`crate::copy_frames::…`) keep compiling during the migration. New
//! code should import from [`crate::filepath`] directly.
//!
//! Deprecated; will be removed once all callers point at `filepath`.

pub use crate::filepath::{
    copy_entry_name, local_basename, non_empty_name, path_is_existing_dir, validate_upload_source,
};

/// Serialize a path to the wire form (forward-slash).
#[allow(dead_code)] // retained for completeness; sftp_copy now uses NormalizedPath directly.
pub fn path_to_string(path: &std::path::Path) -> anyhow::Result<String> {
    Ok(crate::filepath::NormalizedPath::from_local(path).to_string_normalized())
}

/// Serialize a relative path to the wire form (forward-slash), validating it
/// has no absolute/parent components.
pub fn relative_path_to_string(path: &std::path::Path) -> anyhow::Result<String> {
    let np = crate::filepath::NormalizedPath::from_local(path);
    np.validate_relative()?;
    Ok(np.to_string_normalized())
}

/// Validate a wire relative path string.
pub fn validate_relative_path(path: &std::path::Path) -> anyhow::Result<()> {
    crate::filepath::NormalizedPath::from_local(path).validate_relative()
}

/// Join a root path and a wire relative-path string, returning a local [`PathBuf`].
pub fn join_relative_path(
    root: &std::path::Path,
    relative_path: &str,
) -> anyhow::Result<std::path::PathBuf> {
    let np = crate::filepath::NormalizedPath::from_local(root).join(relative_path);
    Ok(np.to_local())
}
