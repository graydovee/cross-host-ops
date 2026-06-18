fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_prost_build::configure().compile_protos(&["proto/xho.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/xho.proto");

    // Inject a build version derived from `git describe --tags --always --dirty`
    // so `xho --version` / `xhod --version` show the exact source state (tag +
    // commit count + dirty marker) instead of just the Cargo.toml version.
    // Falls back to CARGO_PKG_VERSION when git is unavailable (e.g. tarball
    // builds) so compilation never breaks.
    let version = git_describe_version();
    println!("cargo:rustc-env=XHO_BUILD_VERSION={version}");
    // Re-run when HEAD moves (new commit, branch switch). Tags fetched without
    // a new commit are a known gap — workaround: `cargo clean` or touch build.rs.
    println!("cargo:rerun-if-changed=.git/HEAD");
    Ok(())
}

/// Return the build version: `git describe` output with the leading `v`
/// stripped (so `v0.2.2-3-gabc1234-dirty` becomes `0.2.2-3-gabc1234-dirty`).
fn git_describe_version() -> String {
    let output = std::process::Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty=-dirty"])
        .output();
    match output {
        Ok(o) if o.status.success() => {
            let raw = String::from_utf8_lossy(&o.stdout);
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                env!("CARGO_PKG_VERSION").to_string()
            } else {
                trimmed.strip_prefix('v').unwrap_or(trimmed).to_string()
            }
        }
        _ => env!("CARGO_PKG_VERSION").to_string(),
    }
}
