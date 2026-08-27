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

The changelog exists in **two languages**, both at the repo root: `CHANGELOG.md` (English) and `CHANGELOG.zh-CN.md` (Chinese mirror). Content maps one-to-one between them; **whenever you change one, sync the other**. Each README links to its language's changelog in the Documentation section.

- **Before every commit, update both changelogs**: prepend one entry to the topmost open section of each.
- **Summarize each entry in one or two sentences**: state what changed and the user-visible effect — no implementation detail (that belongs in the commit message); semantic correspondence between the languages is enough, no word-for-word translation required.
- Entries in `CHANGELOG.md` are **English only**; entries in `CHANGELOG.zh-CN.md` are **Chinese only**.
- Every entry MUST be a **single markdown list item** (starts with `- ` so GitHub renders one line per entry), formatted exactly as:
  `- yyyy-MM-dd [tag] content`, e.g. `- 2026-08-06 [feat] jumpserver supports -i stdin forwarding`.
- The changelog is **forward-maintained only — never backfill history**; released sections stay frozen (except bulk rewrites explicitly requested by the user).
- Entries are **grouped per released version**, newest first within a group:
  - Each released git tag (`v*`) gets one `## v0.x.y` section covering only the changes within that release range.
  - **Released sections are frozen**: never append to or rewrite their entries afterwards.
  - While the next release is undecided, new entries go into the topmost `## latest` section (create it if missing); when the tag is cut, rename `## latest` to `## v0.x.y`.
- Only 4 tags exist; **classify by the substance of the change, not the commit prefix**:

  | tag         | When to use |
  |-------------|-------------|
  | `[feat]`    | New feature or capability (incl. independently added tests/tools) |
  | `[bug]`     | Bug fix |
  | `[refactor]`| Behavior-preserving refactor, internal cleanup, dependency upgrade, CI/build adjustments |
  | `[docs]`    | Documentation changes |

- Tests, formatting, and minor cleanup bundled with a `fix`/`feat` are **not logged separately** — fold them into that entry; Merge/Revert commits get no entry.
- **Keep the changelog lines in the same commit as the code change** — never a standalone changelog commit.

## Interaction & documentation conventions

### Language

All user-facing output — explanations, descriptions, answers — must be written in **Chinese**.

Code comments (inline comments, function docs, package docs) must be entirely in **English**; never mix languages.

Correct:
```rust
// Calculate the sum of two numbers
fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

Incorrect:
```rust
// 计算两个数字的和
fn 加法(a: i32, b: i32) -> i32 {
    a + b
}
```
(The negative example is quoted verbatim to show the violation.)

### Documentation generation

Do not proactively create any documentation files (.md, .txt, README, API docs, design docs, etc.) unless the user explicitly asks.

If a document seems truly necessary mid-task, first explain why (in Chinese) and get the user's consent before creating it.

> Principle: documentation is the user's decision, not a default behavior.

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
