# AGENTS.md

Workspace instructions for ZCode agents working on **Cross Host Ops** (`cross-host-ops`, the `xho`/`xhod` Rust project). Read this before editing sensitive areas.

## What this is

A Rust remote-operations tool: run commands, copy files, and act as a transparent SSH proxy through a local daemon (`xhod`). A local `xho` CLI talks to the daemon over a Unix-socket gRPC; the daemon brokers SSH to real targets via several interchangeable backends.

- `xho` — client binary (`src/bin/xho.rs`), thin: arg parsing, terminal raw mode, streaming display.
- `xhod` — daemon binary (`src/bin/xhod.rs`), holds connections, gateways, command review.

The CLI never connects to targets directly — every request goes through the daemon.

## Build / test / lint

```bash
cargo build                                   # dev build
cargo build --release                         # release (binaries: target/release/xho, xhod)
cargo test                                    # all tests (unit + property tests under tests/)
cargo test --test <name>                      # one integration test, e.g. in_process_rpc_test
cargo fmt --all                               # format
cargo clippy --all-targets                    # lint (note: lib.rs globally allows many clippy lints)
```

- Rust **edition 2024**, stable toolchain. Docker builds use `rust:1-bookworm`.
- A git tag push (`v*`) triggers `.github/workflows/release.yml` — multi-platform musl/macOS binaries + GHCR Docker image. Don't tag casually.
- `build.rs` derives `--version` from `git describe`. After a tag-only change, `cargo clean` or touch `build.rs` to pick it up.

## Architecture — the two layers (do not bypass)

Everything goes through two traits. This is the core invariant; edits must respect it.

1. **`Gateway` trait** (`src/daemon/gateway/mod.rs`) — high-level backend. Kinds: `Direct` (pooled SSH), `Xhod` (remote daemon over SSH subsystem), `Jumpserver` (interactive bastion, partial), `ReverseProxy`, `Localhost`. Each declares a `Capabilities` bitflag (`EXEC | COPY | PROXY | LIST`).
2. **`TargetSession` trait** (`src/daemon/session/mod.rs`) — one per *transport*: `DirectSshSession`, `LocalSession` (PTY + in-process sftp), `TunneledSession` (drives the `OpenSession` RPC over the control plane), `JumpserverSession`.

Rules:
- **Callers gate generically on `Capabilities` — no `match kind` special-casing outside a gateway's own impl.** A backend that lacks a capability returns `GatewayError::unsupported`.
- The transparent proxy (`ssh node@xhod`), multi-hop `OpenSession` tunnel, `xho exec`, and `xho cp` all drive a `TargetSession`. Add features at the trait layer, not in one caller.
- Error classification matters: `GatewayError` has `ErrorKind::{Resolution,Transport,Execution,Unsupported}`. Transport errors drive retry/fallback and get a "please retry" user hint — use the right constructor (`is_transport_error` / `is_resolution_error` helpers exist).
- Command string construction is **kind-aware** inside `Gateway::open_exec_session`; the caller just runs the returned string on the session.

## Ports (separate key stores — do not mix)

| Port | Server | `authorized_keys` | SSH user | Purpose |
|------|--------|-------------------|----------|---------|
| **2222** | `ProxySshServer` | `proxy_authorized_keys` | target node name | human `ssh`/`scp`/`sftp` |
| **12222** | `RemoteSshServer` | `authorized_keys` | `xho` (single) | daemon↔daemon `xho-rpc`/`xho-reverse` subsystems + `OpenSession` |
| Unix socket | gRPC | — | — | local `xho` ↔ daemon |

v0.4.0 moved control plane `2222 → 12222` to free 2222 for the proxy. If you see `:2222` in a gateway address or `reverse_proxy.server_address`, it's stale.

## Conventions

- **Logging:** `tracing` macros (`info!`/`debug!`/`warn!`/`error!`). Daemon uses `tracing-appender` with SIGHUP-driven log rotation (`logging::reopen_log_output`). Do not use `println!`/`eprintln!` in library/daemon code (CLI output formatting lives in `src/cli/output.rs`).
- **Config:** TOML, hot-reloadable via SIGHUP. `AppConfig` in `src/config.rs`; nested modules per section (`server`, `ssh`, `gateway`, `review`, `secret`, etc.). `serde(default)` throughout — new fields must keep defaults. Zero-config operation is a design goal: `~/.ssh/config` alone should work.
- **gRPC schema is `proto/xho.proto`** (`package xho.rpc`), codegen via `tonic-prost-build` in `build.rs`. Regenerate by rebuilding. Reserved field numbers exist in `StatusResponse` — never reuse them.
- **Machine output:** `xho --output=json` emits **NDJSON** (one JSON object per line). Keep human text on stdout parseable; property tests assert on NDJSON shape.
- **Exit codes** are part of the contract (see `src/exit_codes.rs`): `124`=timeout, `125`=daemon failure, `126`=auth/review denied, `127`=target not found. Don't repurpose.
- **Secrets:** encrypted vault in `src/secret/`; password/MFA material is brokered by the daemon. Use `zeroize`; never log secrets.

## Changelog

`CHANGELOG.md`（仓库根）是本项目的变更记录，**仅向前维护，不回填历史**。

- **每次 commit 前必须先更新 `CHANGELOG.md`**：在"条目"区最上方追加一行，简短描述本次变更。
- 格式严格为：`yyyy-MM-dd [tag] 内容`，例如 `2026-08-06 [feat] jumpserver 支持 -i stdin 转发`。
- tag 只有 4 种，**按本次变更的实质归类，而非看 commit 前缀**：

  | tag         | 何时使用 |
  |-------------|---------|
  | `[feat]`    | 新增功能或能力（含独立新增的测试/工具） |
  | `[bug]`     | 修复缺陷 |
  | `[refactor]`| 不改变外部行为的重构、内部清理、依赖升级、CI/构建调整 |
  | `[docs]`    | 文档变更 |

- 伴随某次 `fix`/`feat` 的测试、格式化、小清理等改动**不单独记一行**，并入对应那条；Merge / Revert 提交不单独记录。
- **changelog 行与代码改动放同一个 commit**，不要单独提一个 changelog commit。

## Platform compatibility (real gotchas here)

- **Unix-only PTY/terminal code.** `src/cli/exec.rs`, `src/cli/prompt.rs`, `src/daemon/session/local.rs` use `libc` calls (`tcgetattr`, `openpty`, `ioctl(TIOCSWINSZ)`). Guard with `#[cfg(unix)]`. There is no Windows support.
- **`ioctl` request casts differ by libc.** `TIOCSWINSZ` must be cast `as _` (not a fixed c_int) — a hardcoded cast broke the `x86_64-unknown-linux-musl` build before. Keep casts generic.
- **Darwin vs Linux libc** differ on some PTY/`libc::` signatures — cross-compile both (`cargo build --target aarch64-apple-darwin`) before shipping PTY changes. Recent commits fixed several; don't regress.
- **Root vs non-root paths** differ: root daemon uses `/var/run/xho/xhod.sock` and `/etc/xho/`; users use `~/.xho/`. See `src/config/path.rs`.

## Tests

- Heavy use of **`proptest`** property tests in `tests/prop_*.rs` (CLI arg parsing, gateway construction, resolver, shell wrapping, NDJSON, exit-code propagation). Add property tests for new parsing/classification logic.
- `tests/in_process_rpc_test.rs` uses an in-process gRPC harness (`tests/support/`); `tests/jumpserver_e2e.rs` needs a live bastion.
- `skills/xho-e2e-smoke-test/` has a bash smoke test to run against a live target after deploys.

## Read before changing sensitive areas

- `docs/en/architecture.md` — full system design, the Gateway/TargetSession rationale, proxy & multi-hop tunnel flows.
- `docs/en/usage.md` — config reference, command reference, troubleshooting.
- `skills/xho-remote-ops/references/config-and-usage.md` — pool tuning, review policy, current port layout.
- `config.example.toml` / `server.example.toml` — the canonical, commented config shapes.
- `proto/xho.proto` — wire contract before touching any RPC.
