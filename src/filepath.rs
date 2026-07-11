//! Unified, OS-agnostic path handling.
//!
//! Paths flow between three contexts that each have different separator
//! expectations:
//!
//! - **Local filesystem** — must use the running OS's native separator
//!   (`\` on Windows, `/` on Unix). `std::path` handles this; we expose it via
//!   [`NormalizedPath::to_local`] and the [`local`] helpers.
//! - **Wire / remote** — a path string crossing to another host (the copy
//!   protocol frames, SFTP paths, jumpserver shell commands). The on-wire form
//!   is always forward-slash; the remote OS re-materializes it itself. We
//!   expose this via [`NormalizedPath::to_string_normalized`].
//! - **`host:path` operand** — the `xho cp`/`xho exec` target syntax parsed by
//!   [`spec::parse_remote`].
//!
//! [`NormalizedPath`] is the OS-neutral intermediate representation: a vector
//! of segments plus a kind (absolute / relative / Windows drive). It never
//! guesses the source OS from path content; the caller picks the construction
//! entry point that matches where the path came from.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};

// ---------------------------------------------------------------------------
// NormalizedPath
// ---------------------------------------------------------------------------

/// The structural "kind" of a path, independent of its segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathKind {
    /// `/foo/bar` — POSIX-absolute.
    Absolute,
    /// `foo/bar` — no leading root.
    Relative,
    /// `C:\foo` — a Windows drive-absolute path. The drive letter is retained
    /// so the path can be re-materialized on Windows (`C:\…`) or emitted as a
    /// normalized `C:/…` wire string.
    WindowsDrive(char),
}

/// An OS-agnostic path held as ordered segments plus a kind.
///
/// Construct from a string ([`Self::from_str`], heuristic separator detection)
/// or from a local [`Path`] ([`Self::from_local`], authoritative split by the
/// running OS). Re-materialize to the local filesystem ([`Self::to_local`]) or
/// to a wire string ([`Self::to_string_normalized`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedPath {
    pub(crate) segments: Vec<String>,
    pub(crate) kind: PathKind,
}

impl NormalizedPath {
    /// Parse a path string with heuristic separator detection (no OS hint).
    ///
    /// Separator rules:
    /// - Only backslashes (no `/`) → backslash is the separator.
    /// - Only forward slashes (no `\`) → forward slash is the separator.
    /// - Both present → forward slash is the separator; backslashes are kept
    ///   verbatim inside segments (so a segment may legitimately contain `\`).
    ///
    /// Prefix detection: `<letter>:[/\]` → `WindowsDrive`; `/…` → `Absolute`;
    /// otherwise `Relative`. A leading `./` or `.\` is stripped.
    pub fn from_str(s: &str) -> Self {
        let s = s.trim();
        if s.is_empty() {
            return Self {
                segments: Vec::new(),
                kind: PathKind::Relative,
            };
        }

        // Windows drive prefix: `C:\` or `C:/`.
        let bytes = s.as_bytes();
        if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && (bytes[2] == b'\\' || bytes[2] == b'/')
        {
            let drive = bytes[0].to_ascii_uppercase() as char;
            let rest = &s[2..];
            let segments = split_segments(rest);
            return Self {
                segments,
                kind: PathKind::WindowsDrive(drive),
            };
        }

        // Absolute POSIX: `/…`.
        if s.starts_with('/') {
            return Self {
                segments: split_segments(s),
                kind: PathKind::Absolute,
            };
        }

        // Strip a leading `.\` / `./` (current-dir prefix) before splitting.
        let s = if (s.starts_with("./") || s.starts_with(".\\")) && s.len() > 2 {
            &s[2..]
        } else {
            s
        };
        Self {
            segments: split_segments(s),
            kind: PathKind::Relative,
        }
    }

    /// Build from a local [`Path`], split authoritatively by the running OS's
    /// `Path::components()`. This is the right entry point when the path came
    /// from the local filesystem — `std::path` resolves the `\` ambiguity
    /// (on Unix `foo\bar` is one segment; on Windows two).
    pub fn from_local(path: &Path) -> Self {
        use std::path::Component;

        let mut segments: Vec<String> = Vec::new();
        let mut kind = PathKind::Relative;
        for component in path.components() {
            match component {
                Component::Prefix(prefix) => {
                    // Windows drive letter (e.g. `C:`).
                    if let Some(s) = prefix.as_os_str().to_str() {
                        let s = s.trim_end_matches(':');
                        if s.len() == 1 && s.chars().next().unwrap().is_ascii_alphabetic() {
                            kind = PathKind::WindowsDrive(s.chars().next().unwrap().to_ascii_uppercase());
                        }
                    }
                }
                Component::RootDir => {
                    if matches!(kind, PathKind::Relative) {
                        kind = PathKind::Absolute;
                    }
                }
                Component::CurDir => {}
                Component::ParentDir => segments.push("..".to_string()),
                Component::Normal(name) => {
                    if let Some(s) = name.to_str() {
                        segments.push(s.to_string());
                    }
                }
            }
        }
        Self { segments, kind }
    }

    /// Re-materialize to the local filesystem using the running OS's native
    /// separator. Use this immediately before any local fs operation.
    pub fn to_local(&self) -> PathBuf {
        let mut pb = match &self.kind {
            PathKind::WindowsDrive(drive) => {
                let mut s = String::from(*drive);
                s.push(':');
                PathBuf::from(s)
            }
            PathKind::Absolute => {
                #[cfg(windows)]
                {
                    PathBuf::from("\\")
                }
                #[cfg(not(windows))]
                {
                    PathBuf::from("/")
                }
            }
            PathKind::Relative => PathBuf::new(),
        };
        for seg in &self.segments {
            if seg == ".." {
                pb.pop();
            } else if !seg.is_empty() {
                pb.push(seg);
            }
        }
        pb
    }

    /// Emit the wire/remote form: always forward-slash, regardless of host OS.
    pub fn to_string_normalized(&self) -> String {
        let mut out = String::new();
        match &self.kind {
            PathKind::WindowsDrive(drive) => {
                out.push(*drive);
                out.push(':');
                // Each segment is prefixed with '/', so `C:` + `/foo` = `C:/foo`.
                for seg in &self.segments {
                    out.push('/');
                    out.push_str(seg);
                }
                return out;
            }
            PathKind::Absolute => out.push('/'),
            PathKind::Relative => {}
        }
        for (i, seg) in self.segments.iter().enumerate() {
            if i > 0 {
                out.push('/');
            }
            out.push_str(seg);
        }
        out
    }

    /// Append a segment (string join; does not use `Path::join`).
    #[must_use]
    pub fn join(mut self, other: &str) -> Self {
        let extra = Self::from_str(other);
        self.segments.extend(extra.segments);
        self
    }

    /// The final segment, if any.
    pub fn basename(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    /// The path minus its final segment, if there was one.
    pub fn parent(&self) -> Option<Self> {
        if self.segments.is_empty() {
            return None;
        }
        let mut clone = self.clone();
        clone.segments.pop();
        Some(clone)
    }

    /// Whether the path is rooted (absolute or Windows drive).
    pub fn is_absolute(&self) -> bool {
        matches!(self.kind, PathKind::Absolute | PathKind::WindowsDrive(_))
    }

    /// Validate that this path is a *relative* wire path (no `..`, not rooted).
    /// Used by the copy-frame protocol to reject traversal/escape.
    pub fn validate_relative(&self) -> Result<()> {
        if !matches!(self.kind, PathKind::Relative) {
            bail!(
                "copy frame relative path must not be absolute: {}",
                self.to_string_normalized()
            );
        }
        if self.segments.iter().any(|s| s == "..") {
            bail!(
                "copy frame relative path must not contain `..`: {}",
                self.to_string_normalized()
            );
        }
        Ok(())
    }

    /// Number of segments (mainly for tests/diagnostics).
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

// ---------------------------------------------------------------------------
// Copy-protocol helpers (migrated from copy_frames.rs)
// ---------------------------------------------------------------------------

/// Return a trimmed value, falling back to `fallback` when empty/whitespace.
pub fn non_empty_name(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

/// The basename of a copy-frame entry name (the last segment of a wire
/// relative path), falling back to `source_name` / `fallback` when empty.
pub fn copy_entry_name(relative_path: &str, source_name: &str, fallback: &str) -> String {
    NormalizedPath::from_str(relative_path)
        .basename()
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .or_else(|| (!source_name.trim().is_empty()).then(|| source_name.trim().to_string()))
        .unwrap_or_else(|| fallback.to_string())
}

/// The basename of a local path string.
pub fn local_basename(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("failed to derive basename from {}", path.display()))
}

/// Whether a local path exists and is a directory.
pub async fn path_is_existing_dir(path: &Path) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_dir()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

/// Validate that a local upload source exists (and is not a non-recursive dir).
pub async fn validate_upload_source(path: &Path, recursive: bool) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to inspect upload source {}", path.display()))?;
    if metadata.is_dir() && !recursive {
        bail!("{} is a directory; use -r to copy directories", path.display());
    }
    Ok(())
}

/// Split a separator-bearing string into segments per the heuristic:
/// backslash-only → split on `\`; forward-slash present → split on `/` only.
fn split_segments(s: &str) -> Vec<String> {
    let has_forward = s.contains('/');
    let has_back = s.contains('\\');
    if has_back && !has_forward {
        // Backslash is the separator.
        s.split('\\')
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect()
    } else {
        // Forward slash is the separator (whether or not backslashes appear).
        // Backslashes inside a segment are preserved verbatim.
        s.split('/')
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// spec: host:path operand parsing
// ---------------------------------------------------------------------------

pub mod spec {
    use super::NormalizedPath;

    /// A parsed `host:path` / `gateway:host:path` operand.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct RemoteSpec {
        /// The target — a bare host alias or a multi-hop `gateway:host`.
        pub target: String,
        /// The path portion, as a [`NormalizedPath`].
        pub path: NormalizedPath,
    }

    /// Parse a `host:path` operand string.
    ///
    /// Returns `None` when `value` is a local path (e.g. `C:\Users\…` with no
    /// host context). Otherwise splits at the colon whose right side begins a
    /// path (starting with `/`, `~`, `<letter>:[/\]`, or a relative-segment
    /// char) and whose left side is a valid `host[:host]` chain. Multi-hop
    /// targets (`gw:host`) keep their interior colon on the left.
    pub fn parse_remote(value: &str) -> Option<RemoteSpec> {
        // A bare Windows drive path (`C:\…` / `C:/…`) is a LOCAL path.
        let bytes = value.as_bytes();
        if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && (bytes[2] == b'\\' || bytes[2] == b'/')
        {
            return None;
        }

        // Find the host:path boundary: the LAST colon whose right side begins
        // with an unambiguous path indicator (`/`, `~`, or `<letter>:[/\]`).
        // We require an unambiguous indicator (not a bare relative segment) so
        // that multi-hop targets like `gw:host:/p` keep their interior colon:
        // the `/p` after `host:` is the path start, so the boundary is there.
        // A Windows drive path `win-srv:C:\Users` splits at the drive colon's
        // predecessor because `C:\…` is an unambiguous path start and no later
        // colon has one.
        let mut best: Option<usize> = None;
        for (idx, _) in value.match_indices(':') {
            let rest = &value[idx + 1..];
            if rest.is_empty() {
                continue;
            }
            if looks_like_unambiguous_path_start(rest) {
                best = Some(idx);
            }
        }
        let colon_pos = best?;
        let target = &value[..colon_pos];
        let path_str = &value[colon_pos + 1..];
        if !is_valid_target(target) || path_str.is_empty() {
            return None;
        }
        Some(RemoteSpec {
            target: target.to_string(),
            path: NormalizedPath::from_str(path_str),
        })
    }

    /// Whether a string begins with an unambiguous path indicator:
    /// `/` (POSIX absolute), `~` (home), or `<letter>:[/\]` (Windows drive).
    /// A bare relative segment is NOT considered unambiguous, so multi-hop
    /// targets like `gw:host:/p` keep their interior `host` segment on the
    /// target side.
    fn looks_like_unambiguous_path_start(s: &str) -> bool {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return false;
        }
        if bytes[0] == b'/' || bytes[0] == b'~' {
            return true;
        }
        // `<letter>:\` or `<letter>:/`
        bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && (bytes[2] == b'\\' || bytes[2] == b'/')
    }

    /// A target is a non-empty chain of `host[:host]` segments where each
    /// segment matches `[A-Za-z0-9._-]`. It must not contain `/` or `\`.
    fn is_valid_target(target: &str) -> bool {
        if target.is_empty() || target == "." || target == ".." {
            return false;
        }
        if target.contains('/') || target.contains('\\') {
            return false;
        }
        target
            .split(':')
            .all(|seg| !seg.is_empty() && seg.chars().all(is_host_char))
    }

    fn is_host_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
    }
}

// ---------------------------------------------------------------------------
// local: filesystem-default helpers (config paths, tilde expansion)
// ---------------------------------------------------------------------------

pub mod local {
    use std::path::PathBuf;

    use anyhow::{Result, anyhow};
    use home::home_dir;

    /// `~`-expansion of a config path string (`~` → home, `~/x` → home/x).
    pub fn expand_tilde(value: &str) -> Result<String> {
        if value == "~" {
            return Ok(home_dir()
                .ok_or_else(|| anyhow!("home directory not found"))?
                .display()
                .to_string());
        }
        if let Some(rest) = value.strip_prefix("~/") {
            return Ok(home_dir()
                .ok_or_else(|| anyhow!("home directory not found"))?
                .join(rest)
                .display()
                .to_string());
        }
        Ok(value.to_string())
    }

    /// The current user's home directory, if known.
    #[allow(dead_code)] // part of the public path API; used by future helpers.
    pub fn home_root() -> Option<PathBuf> {
        home_dir()
    }

    pub fn default_xho_root() -> PathBuf {
        home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".xho")
    }

    /// Alias retained for callers expecting the historical name.
    pub fn default_root_dir() -> PathBuf {
        default_xho_root()
    }

    pub fn default_config_path() -> PathBuf {
        default_xho_root().join("config.toml")
    }

    pub fn default_client_config_path() -> PathBuf {
        default_xho_root().join("client.toml")
    }

    pub fn default_known_hosts_path() -> PathBuf {
        default_xho_root().join("known_hosts")
    }

    pub fn default_vault_path() -> PathBuf {
        default_xho_root().join("secrets")
    }

    pub fn default_tcp_lock_file() -> String {
        "~/.xho/xhod.tcp".to_string()
    }

    /// Default Unix-domain socket path (Unix only; empty placeholder on Windows
    /// where the TCP control channel is the default transport).
    #[cfg(unix)]
    pub fn default_socket_path() -> String {
        if unsafe { libc::geteuid() } == 0 {
            "/var/run/xho/xhod.sock".to_string()
        } else {
            "~/.xho/xhod.sock".to_string()
        }
    }

    #[cfg(not(unix))]
    pub fn default_socket_path() -> String {
        String::new()
    }

    /// The basename of a path string, splitting on either separator.
    #[allow(dead_code)] // part of the public path API.
    pub fn basename(path: &str) -> &str {
        path.rsplit(['/', '\\'])
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(path)
    }
}

// ---------------------------------------------------------------------------
// quote: shell quoting + shell-name helpers (deduplicated)
// ---------------------------------------------------------------------------

pub mod quote {
    /// Single-quote a string for a POSIX shell. Empty → `''`; internal
    /// single-quotes escaped via `'\''`.
    pub fn shell_quote(arg: &str) -> String {
        if arg.is_empty() {
            return "''".to_string();
        }
        let escaped = arg.replace('\'', "'\\''");
        format!("'{escaped}'")
    }

    /// Reduce a shell path/name to its basename (split on `/` and `\`).
    pub fn shell_basename(name: &str) -> &str {
        name.rsplit(['/', '\\'])
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(name)
    }

    /// Flags to run a single command in the given shell: bash/zsh → `-ic`,
    /// others (sh, fish, cmd.exe, …) → `-c`.
    pub fn shell_flags(shell_name: &str) -> &'static str {
        let basename = shell_basename(shell_name).to_ascii_lowercase();
        match basename.as_str() {
            "bash" | "bash.exe" | "zsh" | "zsh.exe" => "-ic",
            _ => "-c",
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use PathKind::*;

    fn segs(parts: &[&str], kind: PathKind) -> NormalizedPath {
        NormalizedPath {
            segments: parts.iter().map(|s| s.to_string()).collect(),
            kind,
        }
    }

    // --- from_str separator heuristics ---

    #[test]
    fn from_str_backslash_only_splits_on_backslash() {
        assert_eq!(NormalizedPath::from_str(r"foo\bar"), segs(&["foo", "bar"], Relative));
    }

    #[test]
    fn from_str_forward_only_splits_on_forward() {
        assert_eq!(NormalizedPath::from_str("foo/bar"), segs(&["foo", "bar"], Relative));
    }

    #[test]
    fn from_str_both_uses_forward_keeps_backslash_in_segment() {
        // `foo/bar\baz` → [foo, bar\baz] — the backslash stays in the segment.
        assert_eq!(
            NormalizedPath::from_str(r"foo/bar\baz"),
            segs(&["foo", r"bar\baz"], Relative)
        );
    }

    #[test]
    fn from_strips_dot_slash_prefix() {
        assert_eq!(NormalizedPath::from_str("./x/y"), segs(&["x", "y"], Relative));
        assert_eq!(NormalizedPath::from_str(r".\x\y"), segs(&["x", "y"], Relative));
    }

    #[test]
    fn from_str_windows_drive_absolute() {
        assert_eq!(NormalizedPath::from_str(r"C:\foo"), segs(&["foo"], WindowsDrive('C')));
        assert_eq!(NormalizedPath::from_str("D:/bar/baz"), segs(&["bar", "baz"], WindowsDrive('D')));
    }

    #[test]
    fn from_str_posix_absolute() {
        assert_eq!(NormalizedPath::from_str("/tmp/x"), segs(&["tmp", "x"], Absolute));
    }

    #[test]
    fn from_str_empty() {
        assert_eq!(NormalizedPath::from_str(""), segs(&[], Relative));
    }

    // --- accessors ---

    #[test]
    fn basename_and_parent() {
        let p = NormalizedPath::from_str("/a/b/c");
        assert_eq!(p.basename(), Some("c"));
        assert_eq!(p.parent(), Some(segs(&["a", "b"], Absolute)));
        assert_eq!(p.parent().unwrap().parent(), Some(segs(&["a"], Absolute)));
    }

    #[test]
    fn join_pushes_segments() {
        let p = NormalizedPath::from_str("/a").join("b/c");
        assert_eq!(p, segs(&["a", "b", "c"], Absolute));
    }

    // --- materialization ---

    #[test]
    fn to_string_normalized_always_forward() {
        assert_eq!(NormalizedPath::from_str(r"a\b").to_string_normalized(), "a/b");
        assert_eq!(NormalizedPath::from_str(r"C:\foo").to_string_normalized(), "C:/foo");
        assert_eq!(NormalizedPath::from_str("/a/b").to_string_normalized(), "/a/b");
    }

    #[test]
    fn validate_relative_rejects_absolute_and_parent() {
        assert!(NormalizedPath::from_str("a/b").validate_relative().is_ok());
        assert!(NormalizedPath::from_str("../a").validate_relative().is_err());
        assert!(NormalizedPath::from_str("/a").validate_relative().is_err());
        assert!(NormalizedPath::from_str(r"C:\a").validate_relative().is_err());
    }

    // --- spec::parse_remote ---

    #[test]
    fn parse_remote_single_hop() {
        let r = spec::parse_remote("host1:/tmp/x").unwrap();
        assert_eq!(r.target, "host1");
        assert_eq!(r.path, segs(&["tmp", "x"], Absolute));
    }

    #[test]
    fn parse_remote_multi_hop_keeps_interior_colon() {
        let r = spec::parse_remote("gw:host1:/tmp/x").unwrap();
        assert_eq!(r.target, "gw:host1");
        assert_eq!(r.path, segs(&["tmp", "x"], Absolute));
    }

    #[test]
    fn parse_remote_local_windows_drive_is_none() {
        assert!(spec::parse_remote(r"C:\Users\me").is_none());
        assert!(spec::parse_remote("C:/Users/me").is_none());
        assert!(spec::parse_remote(r"D:\tmp").is_none());
    }

    #[test]
    fn parse_remote_host_windows_path() {
        let r = spec::parse_remote(r"win-srv:C:\Users\x").unwrap();
        assert_eq!(r.target, "win-srv");
        assert_eq!(r.path.kind, WindowsDrive('C'));
        assert_eq!(r.path.to_string_normalized(), "C:/Users/x");
    }

    #[test]
    fn parse_remote_rejects_bad_target() {
        assert!(spec::parse_remote("/tmp/x:foo").is_none()); // target contains /
        assert!(spec::parse_remote("host:").is_none()); // empty path
        assert!(spec::parse_remote(":/x").is_none()); // empty target
    }

    // --- quote ---

    #[test]
    fn shell_quote_escapes() {
        assert_eq!(quote::shell_quote("plain"), "'plain'");
        assert_eq!(quote::shell_quote("a'b"), "'a'\\''b'");
        assert_eq!(quote::shell_quote(""), "''");
    }

    #[test]
    fn shell_basename_and_flags() {
        assert_eq!(quote::shell_basename("/usr/bin/bash"), "bash");
        assert_eq!(quote::shell_basename(r"C:\dir\cmd.exe"), "cmd.exe");
        assert_eq!(quote::shell_flags("bash"), "-ic");
        assert_eq!(quote::shell_flags("bash.exe"), "-ic");
        assert_eq!(quote::shell_flags("sh"), "-c");
        assert_eq!(quote::shell_flags("cmd.exe"), "-c");
    }

    // --- local ---

    #[test]
    fn local_basename_splits_either_separator() {
        assert_eq!(local::basename("/a/b"), "b");
        assert_eq!(local::basename(r"a\b"), "b");
        assert_eq!(local::basename("plain"), "plain");
    }

    #[test]
    fn defaults_live_under_xho_root() {
        assert!(local::default_config_path().ends_with(".xho/config.toml"));
        assert!(local::default_known_hosts_path().ends_with(".xho/known_hosts"));
        assert_eq!(local::default_tcp_lock_file(), "~/.xho/xhod.tcp");
    }
}
