# AGENTS.md

Guidance for AI agents working in this repo. `cross-host-ops` is a Rust CLI (`xho`) plus daemon (`xhod`) for remote command execution and file copy over SSH.

## Build & test

```bash
cargo build                  # debug build
cargo build --release        # release binaries -> target/release/{xho,xhod}
cargo test                   # run all tests (unit + proptest + in-process gRPC)
cargo fmt --all              # format
```

- **Rust edition 2024** — requires a recent stable toolchain (1.85+). Older toolchains will fail to compile.
- Two binaries from one crate: `xho` (client) and `xhod` (daemon). Library crate name is `xho` (`src/lib.rs`), package name is `cross-host-ops`.
- Run a single test file by name: `cargo test --test prop_cli_exec_argv` or a single case: `cargo test --test prop_cli_exec_argv <test_name>`.

## Generated code — do not hand-edit

- `build.rs` compiles `proto/xho.proto` via `tonic-prost-build` into the `xho.rpc` module (included in `src/protocol.rs:8`). The `protoc` binary is vendored (`protoc-bin-vendored`), so no system protoc is required.
- Editing `proto/xho.proto` triggers a rebuild automatically (`cargo:rerun-if-changed`).
- `build.rs` also injects `XHO_BUILD_VERSION` from `git describe --tags --always --dirty`. If a tag is fetched without a new commit, run `cargo clean` or `touch build.rs` to refresh it.

## Testing conventions

- The suite is **proptest-heavy**: most files in `tests/` are `prop_*.rs` property tests. Failing cases are persisted under `proptest-regressions/` (committed) so regressions are reproducible.
- **No real SSH server in tests.** Integration tests use an in-process gRPC harness (`tests/support/in_process_rpc.rs`) built on `xho::daemon::test_support::make_test_rpc_service`. They verify protocol contracts and daemon control flow, not live SSH. Add new integration tests via this harness rather than spawning daemons.
- The CLI struct is `ArunCli` (legacy project name "arun"); the public binary is `xho`. Both names are expected — don't "fix" the mismatch.

## Layout

- `src/bin/{xho,xhod}.rs` — thin binary entrypoints.
- `src/cli/` — clap arg parsing + command dispatch (`mod.rs::run_cli` is the router).
- `src/daemon/` — daemon runtime: `gateway/` (direct, jumpserver, xhod), `connection/`, `resolver.rs` (target resolution), `rpc.rs`, `review.rs` (LLM command review), `ssh_server.rs`, `token_store.rs`.
- `src/config/` — TOML config parsing; split per section. `path.rs` holds all default-path logic.
- `src/secret/` — encrypted secret vault (HKDF-derived from an SSH key; no separate key file).
- `proto/xho.proto` — single source of truth for the gRPC wire protocol.

## Config & local files

- `config.toml` and `server.toml` at the repo root are **gitignored local dev configs**. Edit `config.example.toml` / `server.example.toml` instead; never commit the non-`.example` versions.
- Zero-config is supported: defaults resolve to `~/.xho/config.toml`. The local daemon socket path is **euid-dependent** — root → `/var/run/xho/xhod.sock`, non-root → `~/.xho/xhod.sock` (`src/config/path.rs:38`).
- Secrets are stored as references (`env:`, `file:`, `vault:`), not inline. `xho secret encrypt` migrates inline plaintext into the vault.

## Lint policy

`src/lib.rs` intentionally `#![allow(...)]` many clippy lints at the crate root. These are pre-existing decisions, not things to clean up. Clippy is not part of the documented CI flow; `cargo fmt --all` + `cargo test` are the canonical checks before submitting.

## Release

Pushing a `v*` tag triggers `.github/workflows/release.yml`, which builds musl/macOS binaries and publishes a multi-arch Docker image to `ghcr.io/<owner>/cross-host-ops`. There is no separate publish step to run locally.

## Operational skills

Repo-local deploy and smoke-test scripts live under `skills/xho-remote-ops/` and `skills/xho-e2e-smoke-test/` (deploy via Docker or systemd, end-to-end smoke tests). Prefer these over ad-hoc SSH/SCP when deploying or verifying `xhod`.
