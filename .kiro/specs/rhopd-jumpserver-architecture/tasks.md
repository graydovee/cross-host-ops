# Implementation Plan: rhopd Jumpserver Architecture

## Overview

This plan implements the unified `JumpHost` architecture for Remote Hop in the order specified by the design's Migration Plan → Rollout sequence (steps 1–12). The tree is kept green at every step. Property-based tests (proptest) are placed close to the code they cover; the integration-level properties (P1–P4) are consolidated into the rollout step 10 task because they require the full pipeline through the in-process gRPC harness.

Conventions used by this plan:

- All proptest sub-tasks run with `cases=100` (the `proptest!` `ProptestConfig { cases: 100, .. }` form) and carry the comment tag `// Feature: rhopd-jumpserver-architecture, Property N: {title}` exactly.
- Sub-tasks postfixed with `*` are optional and may be skipped for a faster MVP.
- Each major task ends with `Run cargo build && cargo test and confirm green.`

## Tasks

- [x] 1. Add proptest and async-trait dependencies to Cargo.toml
  - Add `proptest = "1"` under `[dev-dependencies]`.
  - Add `async-trait = "0.1"` under `[dependencies]` (or, if preferred, re-export `tonic::async_trait` from a single module so all `JumpHost` impls reach for the same alias).
  - Run `cargo build && cargo test` and confirm green.
  - _Requirements: 3.1, 16.1_

- [x] 2. (Rollout 1) Add `JumpHost` trait, `UnsupportedCapability`, `JumpHostKind`, and wrap existing connections
  - [x] 2.1 Create `src/jump/mod.rs` with the `JumpHost` trait
    - Methods: required `exec`, `copy`; default `tui_shell` and `list_servers` return `UnsupportedCapability`; identity `kind()` and `alias()`.
    - Trait is `Send`; uses `#[async_trait]`.
    - Re-export `pub mod jump;` from `src/lib.rs`.
    - _Requirements: 3.1, 3.2, 3.3, 3.5, 16.3_
  - [x] 2.2 Create `src/jump/error.rs` with `UnsupportedCapability` thiserror struct
    - Fields `kind: JumpHostKind`, `alias: String`, `method: &'static str`.
    - `Display` format: `jump host {alias} (kind={kind}) does not support method {method}`.
    - _Requirements: 3.4, 16.3, 16.4_
  - [x] 2.3 Add `JumpHostKind` enum in `src/jump/mod.rs`
    - Variants `Direct`, `Jumpserver`, `Rhopd` with `#[serde(rename_all = "snake_case")]` and `Display`.
    - _Requirements: 3.5, 16.5_
  - [x] 2.4 Create `src/jump/direct.rs` wrapping existing `DirectSshConnection`
    - `DirectJumpHost` holds `inner: DirectSshConnection`; `exec`/`copy` delegate to `inner`; `tui_shell`/`list_servers` use trait defaults.
    - Behavior unchanged; existing call sites continue to work via this wrapper.
    - _Requirements: 3.5, 3.6_
  - [x] 2.5 Create `src/jump/jumpserver.rs` wrapping existing `JumpSshConnection`
    - `JumpserverJumpHost` holds `inner: JumpSshConnection`; `exec`/`copy` delegate; defaults for the rest.
    - _Requirements: 3.5, 3.6_
  - [x] 2.6 Write proptest property test for Property 5 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 5: UnsupportedCapability error contract`
    - For arbitrary alias strings and method names in `{"tui_shell", "list_servers"}`, calling the default trait method on a synthesized `JumpHost` (use a mock impl that does not override the default) returns `Err`, the error downcasts to `UnsupportedCapability`, and its `Display` rendering contains the alias, the textual `JumpHostKind`, and the method name.
    - _Requirements: 3.4, 3.6, 4.5, 16.3, 16.4_
  - [x] 2.7 Run `cargo build && cargo test` and confirm green.

- [x] 3. (Rollout 2) Add `RhopdJumpHost` stub with in-process gRPC harness
  - [x] 3.1 Create `src/jump/rhopd.rs` with the `RhopdJumpHost` skeleton
    - Holds `alias: String`, `address: RemoteAddress` (placeholder type until task 7), `transport`, and `client: rpc::rhop_rpc_client::RhopRpcClient<Channel>`.
    - Stub `connect`, `exec`, `copy`, `tui_shell` (returns `UnsupportedCapability`), `list_servers`. The `exec`/`copy` paths translate to `RhopRpc.Execute`/`RhopRpc.Copy` and pass through events 1:1.
    - For `copy`: set `local_path = ""` in the outgoing `CopyStartRequest`.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_
  - [x] 3.2 Create the in-process gRPC harness under `tests/support/in_process_rpc.rs`
    - Two `RhopRpcService` instances connected via `tokio::io::duplex` so a "local daemon" client and a "remote daemon" server live in one process.
    - Expose helpers to drive `Execute`/`Copy`/`ListServers` against a stub end target backed by a tempdir.
    - _Requirements: 4.2, 4.6, 4.8_
  - [x] 3.3 Write proptest property test for Property 11 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 11: Wire-level identity for RhopdJumpHost::exec and RhopdJumpHost::list_servers`
    - For arbitrary `(end_target_alias e, argv v)`, the first `ExecuteRequest` the mocked remote receives equals `StartRequest { target: e, argv: v }` byte-for-byte.
    - For arbitrary `Vec<ServerEntry> R` returned by the mocked remote's `ListServers`, `RhopdJumpHost::list_servers` returns a vector equal to `R`.
    - _Requirements: 4.6, 4.8_
  - [x] 3.4 Run `cargo build && cargo test` and confirm green.

- [x] 4. (Rollout 3) Refactor `ConnectionPool` to be generic over `Box<dyn JumpHost>` and key by `PoolKey`
  - [x] 4.1 Define `PoolKey` enum in `src/pool.rs`
    - Variants `Direct { end_target: EndTargetId }` and `Aliased { alias: String, kind: JumpHostKind }`.
    - Implement `Eq`, `Hash`, `Clone`, `Debug`.
    - _Requirements: 5.1, 5.4, 7.5_
  - [x] 4.2 Refactor `ConnectionPool::pools` to `HashMap<PoolKey, Arc<TargetPool>>` and slot storage to `tokio::sync::Mutex<Option<Box<dyn JumpHost>>>`
    - Preserve per-key concurrency cap (`ssh.max_connections_per_ip`), idle reaper, and waiter notification.
    - `execute`/`copy` now call `JumpHost::exec`/`JumpHost::copy` against the slot's hop.
    - _Requirements: 3.7, 5.1, 5.2, 5.4, 5.5_
  - [x] 4.3 Add a temporary `ResolvedTarget → TargetRoute` adapter in `src/connection/resolver.rs`
    - Single-hop or zero-hop output, used until the resolver is rewritten in task 6.
    - _Requirements: 7.5_
  - [x] 4.4 Generalize the reconnect heuristic in `src/pool.rs`
    - Add `classify_transport_error` covering `tonic::Status` codes `Unavailable | Cancelled | Unknown | Internal`, `russh::Error`, and the existing string heuristic.
    - Retry exactly once on `Transport`; return `Application` errors immediately.
    - _Requirements: 4.9, 12.5_
  - [x] 4.5 Write proptest property test for Property 6 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 6: Pool reuse invariant`
    - Model `(acquire, release, reap)` sequences against a single `PoolKey` with a stub `JumpHost`. Assert: idle slot reuse before creating new ones; live slot count never exceeds `ssh.max_connections_per_ip`; `PoolKey` is a pure function of route's first hop alias + kind for non-direct routes and of `end_target_id` for direct routes.
    - _Requirements: 3.7, 4.3, 5.1, 5.2, 5.3, 5.4, 7.5_
  - [x] 4.6 Run `cargo build && cargo test` and confirm green.

- [x] 5. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. (Rollout 4) Rewrite the resolver to produce `Vec<TargetRoute>` directly
  - [x] 6.1 Create `src/jump/types.rs` with `TargetRoute`, `JumpHopRef`, `EndTarget`, `EndTargetId`, `ServerListSource`
    - Move shared types out of `src/connection/types.rs` into the new module.
    - _Requirements: 3.8, 7.1, 7.5_
  - [x] 6.2 Rewrite `Resolver::resolve` in `src/connection/resolver.rs` to return `Result<Vec<TargetRoute>>`
    - Implement the parsing rules from Components → Resolver: `<jump_alias>:<server_alias>` (explicit), bare `<server_alias>` (merged-view lookup), `<host_or_ip>` (legacy SSH-config / IP fallback).
    - Honor candidate ordering: server-config matches → `ssh.fallback`-driven candidates → no implicit fan-out to all `rhopd` jump hosts.
    - When `ssh.fallback` is empty or all-disabled, contribute zero candidates from that step while preserving the relative position of the others.
    - _Requirements: 3.8, 7.1, 7.2, 7.3, 7.5_
  - [x] 6.3 Drop the `ResolvedTarget → TargetRoute` adapter and update `ConnectionPool` callers to consume `Vec<TargetRoute>` directly
    - Remove `ResolvedTarget` if no longer used; otherwise mark it `#[deprecated]` until task 14.
    - _Requirements: 3.8, 7.1, 7.5_
  - [x] 6.4 Write proptest property test for Property 7 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 7: Resolver idempotence and ordering`
    - For arbitrary CLI input `s` and any fixed `(AppConfig, ServerConfigFile, Vec<JumpHostConfig>)`, two calls to `Resolver::resolve(s)` return equal `Vec<TargetRoute>`; the order matches the deterministic ordering function; every `JumpHopRef` has non-empty `alias` and a populated `JumpHostKind`.
    - _Requirements: 3.8, 7.1, 7.2, 7.5_
  - [x] 6.5 Run `cargo build && cargo test` and confirm green.

- [x] 7. (Rollout 5) Add `[[jump_hosts]]` schema, validation, hot-reload, and wire the factory
  - [x] 7.1 Add `Vec<JumpHostConfig>` and `JumpHostFields` to `AppConfig` in `src/config.rs`
    - `JumpHostConfig { alias, kind, #[serde(flatten)] fields: JumpHostFields }`.
    - `JumpHostFields` is `#[serde(untagged)]` over `RhopdJumpHostFields`, `JumpserverJumpHostFields`, `DirectJumpHostFields`.
    - _Requirements: 4.1, 10.12, 14.1, 14.2, 16.5_
  - [x] 7.2 Add `RESERVED_ALIASES = &["local"]` and `validate_jump_hosts` in `src/config.rs`
    - Reject empty alias, duplicate alias across kinds, alias equal to a reserved name, and `fields` variant that does not match `kind`.
    - Return typed `RhopError::ReservedAlias` / `RhopError::AliasCollision` variants.
    - _Requirements: 14.2, 14.3, 14.4, 14.5_
  - [x] 7.3 Create `src/jump/address.rs` with `RemoteAddress` and `AddressDefaults`
    - `RemoteAddress::parse(input, defaults)` accepts `[user@]host[:port]`, fills defaults when omitted, rejects empty input and empty host with an error naming the offending input.
    - `RemoteAddress::format` emits canonical `user@host:port`.
    - _Requirements: 11.1, 11.2, 11.4, 11.5_
  - [x] 7.4 Wire jump-hosts hot-reload in `src/daemon.rs`
    - On config-file change, re-run `validate_jump_hosts`; on success, swap the active list inside `Arc<RwLock<AppConfig>>`; on failure, log and keep the prior list.
    - _Requirements: 4.3, 5.5, 10.12, 10.13_
  - [x] 7.5 Create `src/jump/factory.rs` with `build_jump_host`
    - `match spec.kind { Direct => DirectJumpHost::connect(..), Jumpserver => JumpserverJumpHost::connect(..), Rhopd => RhopdJumpHost::connect(..) }`.
    - Single point of new-kind extension.
    - _Requirements: 16.1, 16.2, 16.5_
  - [x] 7.6 Write proptest property test for Property 9 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 9: RemoteAddress parser round-trip and default-filling`
    - For any `RemoteAddress addr` with non-empty `user`, `RemoteAddress::parse(addr.format(), defaults) == addr` regardless of `defaults`.
    - For any input string that omits `user` and/or `port`, the parser fills `defaults.user` / `defaults.port` and remaining fields equal the input's fields.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_
  - [x] 7.7 Write proptest property test for Property 10 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 10: Alias-uniqueness validation rejects collisions deterministically`
    - For arbitrary `Vec<JumpHostConfig>` containing either two entries with equal `alias` or any entry with `alias == "local"`, `validate_jump_hosts` returns `Err`.
    - For an arbitrary on-disk config and a candidate `name` ∈ `RESERVED_ALIASES ∪ existing_aliases`, the CLI validation step (mocked in this test against an in-memory config copy) rejects the request and the on-disk config bytes are unchanged before and after the failed attempt.
    - _Requirements: 10.6, 10.7, 14.2, 14.3, 14.5, 14.6_
  - [x] 7.8 Run `cargo build && cargo test` and confirm green.

- [x] 8. (Rollout 6) Rewrite the CLI: drop `ClientMode`, drop `enable`/`disable`, drop SSH-out-of-CLI, add `remote connect/remove/list`
  - [x] 8.1 Drop `ClientMode` branching from `src/cli.rs`
    - Collapse `connect_data_client` / `connect_remote_data_client` / `client_mode` into a single `connect_local_data_client`.
    - Remove `RemoteCommand::Enable` and `RemoteCommand::Disable` arms.
    - Remove `ensure_local_mode` and every call site.
    - _Requirements: 1.1, 1.3, 1.4, 9.4, 10.1_
  - [x] 8.2 Implement `rhop remote connect <name> [user@]host[:port]` with full validation
    - Fetch existing aliases via `ListJumpHosts` (RPC stub for now; wired in task 9).
    - Validate `<name>` against `RESERVED_ALIASES` and existing aliases before any network or filesystem write; emit `RhopError::ReservedAlias` / `RhopError::AliasCollision` and exit non-zero on failure.
    - Parse `<address>` via `RemoteAddress::parse`.
    - Open SSH, fetch host key, compare to `known_hosts`; prompt-and-trust on unknown, refuse on changed.
    - Persist via `UpdateConfig` RPC (stub for now; wired in task 9) so the daemon atomically rewrites `~/.rhop/config.toml`.
    - _Requirements: 10.2, 10.6, 10.7, 11.6, 12.2, 14.6_
  - [x] 8.3 Implement `rhop remote remove <name>`
    - Reject if `<name>` is missing (`RhopError::UnknownAlias`) or kind ≠ `rhopd` (`RhopError::AliasKindMismatch`); error message names the existing kind.
    - Otherwise delete via `UpdateConfig` and let the daemon hot-reload.
    - _Requirements: 10.3, 10.5, 10.8, 10.9_
  - [x] 8.4 Implement `rhop remote list`
    - Print every `[[jump_hosts]]` entry with columns `ALIAS  KIND  ADDRESS_OR_HOST`.
    - No connection opened.
    - _Requirements: 10.4, 10.5_
  - [x] 8.5 Trim `ClientConfig` in `src/config.rs`
    - Drop `ClientConfig::mode` and `RemoteClientConfig`. The `client.toml` schema now contains only `[local] socket_path, auto_start`.
    - _Requirements: 10.10, 10.11_
  - [x] 8.6 Update `rhop status` printing in `src/cli.rs`
    - Drop `local_enabled`, `remote_enabled`, `remote_listen_addr`, `remote_user` fields.
    - Print the new `JumpHostStatus` rows (alias, kind, address, optional sub-status block) when present.
    - _Requirements: 9.1, 9.2, 9.3, 9.4_
  - [x] 8.7 Run `cargo build && cargo test` and confirm green.

- [x] 9. (Rollout 7) Update `proto/rhop.proto`
  - [x] 9.1 Revise `proto/rhop.proto`
    - `StatusResponse`: drop `local_enabled`, `remote_enabled`, `remote_listen_addr`, `remote_user`. Add `repeated JumpHostStatus jump_hosts`.
    - Add messages `JumpHostStatus { string alias; string kind; string address; StatusResponse sub_status; }`, `MergedServerList { repeated ServerListRow rows; repeated SourceStatus source_status; }`, `ServerListRow { ServerEntry server; string source; }`, `SourceStatus { string source; string status; string detail; }`.
    - Add RPCs `UpdateConfig(UpdateConfigRequest) returns (UpdateConfigResponse)` and `ListJumpHosts(ListJumpHostsRequest) returns (ListJumpHostsResponse)`.
    - Mark `ConfigListRequest`/`ConfigListResponse` deprecated.
    - _Requirements: 9.1, 9.2, 9.4, 10.2, 10.3, 10.4, 15.1, 15.2, 15.3_
  - [x] 9.2 Regenerate prost types via `build.rs` and update `src/protocol.rs`
    - Add helper conversions for the new message types, including `ServerEvent` shapes for nested status.
    - _Requirements: 9.1, 9.2_
  - [x] 9.3 Implement daemon-side handlers in `src/daemon.rs`
    - `UpdateConfig`: typed mutation (`AddJumpHost`, `RemoveJumpHost`) with atomic rewrite of `~/.rhop/config.toml` via temp file + rename, then hot-reload.
    - `ListJumpHosts`: returns the current `Vec<JumpHostConfig>` projected to `(alias, kind, address)`.
    - Refuse `Copy` requests received over the `rhop-rpc` subsystem when `local_path` is non-empty (defense in depth from design Error Handling).
    - _Requirements: 2.4, 2.5, 4.7, 10.2, 10.3, 10.4, 10.12_
  - [x] 9.4 Run `cargo build && cargo test` and confirm green.

- [x] 10. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. (Rollout 8) Wire `ServerListAggregator` and the new `rhop server list` flow
  - [x] 11.1 Create `src/jump/server_list.rs` with `ServerListAggregator`, `ServerListRow`, `ServerListSourceStatus`, `MergedServerList`
    - Per-jump-host concurrent calls bounded by `tokio::time::timeout(connect_timeout)`.
    - `UnsupportedCapability` (via `downcast_ref`) → `Unsupported`; other errors → `Error(msg)`; both yield zero rows.
    - _Requirements: 15.1, 15.2, 15.3, 15.4_
  - [x] 11.2 Add the cache to the aggregator
    - `HashMap<ServerListSource, (Instant, Vec<ServerEntry>)>` with TTL = `ssh.max_idle_time`.
    - `refresh = true` evicts before re-fetching.
    - _Requirements: 15.8_
  - [x] 11.3 Replace the `ListServers` handler in `src/daemon.rs`
    - The handler now drives the aggregator and returns a `MergedServerList`.
    - _Requirements: 15.1, 15.2, 15.3, 15.4_
  - [x] 11.4 Update `rhop server list [--refresh]` in `src/cli.rs`
    - Print rows tagged `<source>:<alias>`; below the table, print one line per non-`Ok` source describing its status.
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.8_
  - [x] 11.5 Update the resolver bare-alias / explicit-prefix lookup to use the aggregator's view
    - Explicit `<jump_alias>:<server_alias>` looks up only that source.
    - Bare `<server_alias>` accepts when unique across sources; errors with `RhopError::AmbiguousServer { alias, candidates }` when not unique, listing every `<jump_alias>:<server_alias>` form including `local:`.
    - _Requirements: 15.5, 15.6, 15.7_
  - [x] 11.6 Write proptest property test for Property 12 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 12: Merged server-list aggregation correctness`
    - For arbitrary `(L, O)` where `O` maps each jump-host alias to one of `Ok(Vec<ServerEntry>) | Unsupported | Error(_)`, the aggregator's `MergedServerList` satisfies the three invariants: rows multiset equality, one `source_status` entry per source mirroring the outcome, overall `Ok(_)` even when every jump host failed.
    - _Requirements: 15.1, 15.2, 15.3, 15.4_
  - [x] 11.7 Write proptest property test for Property 13 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 13: Bare server-alias ambiguity reporting`
    - For arbitrary configs in which a `<server_alias>` appears in two or more `Server_List_Source` values, resolving the bare `<server_alias>` returns `Err(AmbiguousServer { alias, candidates })` whose `candidates` contains every `<jump_alias>:<server_alias>` form (including `local:`) where the server appears.
    - _Requirements: 15.7_
  - [x] 11.8 Run `cargo build && cargo test` and confirm green.

- [x] 12. (Rollout 9) Wire `AuthPromptRouter` to forward prompts through `RhopdJumpHost`
  - [x] 12.1 Create `src/jump/auth.rs` with `AuthPromptRouter`
    - Holds an upstream `Sender<AuthPromptMessage>` and a `HashMap<prompt_id, oneshot::Sender<String>>`.
    - `ask(msg)` returns `Result<String>`; `deliver_response(prompt_id, value)` matches by `prompt_id`.
    - _Requirements: 8.1, 8.2, 8.4_
  - [x] 12.2 Refactor `make_auth_prompter` in `src/daemon.rs` to back its `AuthPromptRequest` channel with the router
    - Each in-flight Execute/Copy request owns one router instance.
    - _Requirements: 8.1, 8.2, 8.4_
  - [x] 12.3 Wire `RhopdJumpHost::exec`/`copy` to forward `AuthPrompt` events from the inner gRPC stream
    - On receiving `AuthPrompt`, call `router.ask(msg)` (does not prompt locally); on user response, send `AuthInputRequest { prompt_id, value }` back into the gRPC stream with `prompt_id` unchanged at every layer.
    - On timeout (`ssh.connect_timeout`), abort and emit `auth prompt timed out for {target_label}`.
    - _Requirements: 8.1, 8.2, 8.4, 8.5_
  - [x] 12.4 Write proptest property test for Property 8 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 8: Auth-prompt forwarding identity`
    - For arbitrary `AuthPromptMessage p`, the message arriving at the CLI equals `p` byte-for-byte in `{prompt_id, target_label, kind, secret, message}`.
    - For arbitrary response string `r`, a CLI reply with the same `prompt_id` is delivered to the originating daemon as a string equal to `r`.
    - Test at depth 1 (CLI ↔ local daemon) and depth 2 (CLI ↔ local daemon ↔ remote daemon via the in-process harness).
    - _Requirements: 8.1, 8.2, 8.4_
  - [x] 12.5 Run `cargo build && cargo test` and confirm green.

- [x] 13. (Rollout 10) Implement integration Properties P1–P4 and example/edge tests from the coverage matrix
  - [x] 13.1 Build `TestHarness` under `tests/support/harness.rs`
    - Spawns local-daemon and remote-daemon `RhopRpcService` over `tokio::io::duplex`.
    - End target is a tempdir mounted on a stub `JumpHost` that performs filesystem ops directly so the harness exercises the same control flow as production.
    - Helpers: `cli_exec`, `cli_cp`, `target_for(route_kind, end_alias, remote_path)`, `read_local`, `write_local`, `fresh_local_path`, `route_kind_strategy()`.
    - _Requirements: 2.1, 2.2, 2.3, 6.1, 6.2_
  - [x] 13.2 Write proptest property test for Property 1 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 1: Cp byte round-trip across all route kinds`
    - For arbitrary `bytes ∈ [0, 64KiB]` and route kind `k ∈ {Direct, Jumpserver, Rhopd}`, upload-then-download via the harness yields a destination file equal to `bytes`.
    - _Requirements: 2.1, 2.2, 2.3, 2.6, 6.3_
  - [x] 13.3 Write proptest property test for Property 2 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 2: Cp Unix mode round-trip across all route kinds`
    - For arbitrary mode bits in `0o000..=0o777` and any route kind, with `copy.preserve_mode = true`, the upload-then-download cycle yields a file whose Unix permission bits equal the original.
    - _Requirements: 2.7, 6.4_
  - [x] 13.4 Write proptest property test for Property 3 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 3: Local-side filesystem authority for rhopd hops`
    - For arbitrary `CopySpec` with non-empty `local_path` routed through a `rhopd` jump host, the `CopyStartRequest` received by the remote daemon has `local_path == ""`, and the remote daemon never opens, reads, or writes any path equal to the original `local_path` on its own filesystem (use a guarded tempdir + filesystem-call recorder).
    - _Requirements: 2.4, 2.5, 4.7_
  - [x] 13.5 Write proptest property test for Property 4 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 4: Exec route-invariance`
    - For arbitrary deterministic argv `v` (modeled by a stub end target whose stdout/exit are fixed by the argv), running `rhop exec <route>:<target> v` through any route kind yields concatenated stdout bytes equal to the modeled `s` and exit code equal to `c`.
    - _Requirements: 6.1, 6.2_
  - [x] 13.6 Add example/edge tests from the coverage matrix
    - 1.2/1.5/1.6: socket auto-start, unreachable, stale (3 examples).
    - 4.9: tonic `Unavailable` retry once and I/O reset retry once (2 examples).
    - 5.5: idle reaper closes idle slot under a mock clock (1 example).
    - 6.5: `~`, `~user/`, `~/foo` expansion local vs end-target (3 examples).
    - 7.3 / 7.4: no-candidates error vs first-candidate-fail fallthrough (2 examples).
    - 8.3 / 8.5: TTY echo disabled when `secret = true`; auth-prompt timeout (2 examples).
    - 9.1 / 9.3: `daemon_origin` and CLI start args round-trip in `Status` (2 examples).
    - 10.8 / 10.9: `rhop remote remove` on missing alias and on non-`rhopd` alias (2 examples).
    - 11.4 / 11.6: invalid-input parse errors and `rhop remote connect` end-to-end address parsing (2 examples).
    - 12.1–12.5: SSH connect failure, host-key change refusal, subsystem missing, error forwarded verbatim, channel-drop pool eviction (5 examples).
    - 13.1–13.5: review-on-LD-then-forward, review-disabled passthrough, review-on-RD parallel run, deny short-circuits pool, confirm waits for reply (5 examples).
    - 15.5 / 15.6 / 15.8: explicit `<jump>:<server>` lookup, bare alias unique acceptance, `--refresh` evicts cache (3 examples).
    - _Requirements: 1.2, 1.5, 1.6, 4.9, 5.5, 6.5, 7.3, 7.4, 8.3, 8.5, 9.1, 9.3, 10.8, 10.9, 11.4, 11.6, 12.1, 12.2, 12.3, 12.4, 12.5, 13.1, 13.2, 13.3, 13.4, 13.5, 15.5, 15.6, 15.8_
  - [x] 13.7 Run `cargo build && cargo test` and confirm green.

- [x] 13A. (Requirement 17) Non-Interactive and Machine-Friendly Operation
  - [x] 13A.1 Add CLI global flags `--output` and `--non-interactive`, and `rhop exec` flags `--pty`, `--no-pty`, `--stdin`, `--timeout`
    - Add `OutputFormat` enum (`Text`, `Json`) with `#[derive(ValueEnum)]`.
    - Add `--output` and `--non-interactive` as `global = true` on `ArunCli`.
    - For `rhop exec`: add `--pty` / `--no-pty` with `conflicts_with`, `--stdin`, `--timeout <DURATION>`.
    - Ensure `trailing_var_arg = true` + `allow_hyphen_values = true` on `argv` so everything after `target` is opaque.
    - For `rhop cp`: add `--timeout <DURATION>`.
    - Write proptest property test for Property 16 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 16: Argv pass-through transparency`.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.1, 17.2, 17.3, 17.4, 17.5, 17.13, 17.19, 17.24, 17.27_
  - [x] 13A.2 Implement NDJSON output in `src/cli/output.rs`
    - Define `CliEvent` enum with `#[serde(tag = "event", rename_all = "snake_case")]`.
    - Byte payloads base64-encoded (`data_b64` field).
    - Implement `OutputSink` trait with `TextSink` (current behavior) and `JsonSink` (NDJSON to stdout).
    - Replace all `println!`/`eprintln!` in `run_command`, `run_copy`, `status`, `list_servers` with `sink.emit(event)`.
    - In text mode: remote stdout → CLI stdout, remote stderr → CLI stderr, Info/error → CLI stderr.
    - In json mode: all events as NDJSON on CLI stdout.
    - Write proptest property test for Property 14 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 14: NDJSON output is a valid stream`.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.5, 17.6, 17.7, 17.8, 17.9_
  - [x] 13A.3 Implement exit-code taxonomy
    - Add `RhopError::exit_code() -> i32` method mapping each variant to the documented code.
    - Add `cap_remote_exit_code(c: i32) -> i32` (caps ≥124 to 123).
    - Update CLI's main return path: `rhop exec` returns `cap_remote_exit_code(remote_code)` on success; returns `error.exit_code()` on failure.
    - Write proptest property test for Property 15 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 15: Exit-code semantics consistency`.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.10, 17.11, 17.12_
  - [x] 13A.4 Implement `--non-interactive` and env-credential bypass
    - Create `src/jump/auth_resolution.rs` with `EnvCredentials`, `AuthResolution`, `resolve_auth_response`.
    - `EnvCredentials::from_env()` reads `RHOP_PASSWORD` and `RHOP_TOTP_SECRET`.
    - Three-way logic: env var hit → Answer; non-interactive + no env → Fail(exit 126); else → PromptStdin.
    - Plumb `non_interactive` through `ExecuteRequest`/`CopyRequest` proto (add `bool non_interactive` to `StartRequest`).
    - Replace direct `prompt_for_auth_input` calls with `resolve_auth_response` dispatch.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.13, 17.14, 17.15, 17.16, 17.17, 17.18_
  - [x] 13A.5 Implement Effective PTY Decision
    - Add `ssh.auto_pty_detect: bool` (default `true`) to `SshConfig` in `src/config.rs`.
    - Add `effective_pty_decision(flags, ssh_config, stdout_is_tty) -> bool` in `src/jump/exec.rs`.
    - Add `stdout_is_tty`, `force_pty`, `force_no_pty` fields to proto `StartRequest`.
    - CLI populates `stdout_is_tty` from `std::io::IsTerminal::is_terminal(io::stdout())`.
    - Replace `if config.ssh.pty { ... }` with `if effective_pty_decision(...) { ... }`.
    - Write proptest property test for Property 17 (cases=100), tagged with `// Feature: rhopd-jumpserver-architecture, Property 17: PTY decision determinism`.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.19, 17.20, 17.21, 17.22, 17.23_
  - [x] 13A.6 Implement `--timeout`
    - CLI wraps response stream in `tokio::time::timeout(deadline)`.
    - On expiry: emit `CliEvent::Error { kind: "timeout", .. }`, send `CancelRequest` to daemon, exit 124.
    - Add `CancelRequest` variant to `ExecuteRequest` and `CopyRequest` in proto.
    - Daemon-side: on receiving `Cancel`, drop the in-flight task.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.24, 17.25, 17.26_
  - [x] 13A.7 Implement `--stdin` for `rhop exec`
    - Add `StdinChunk { bytes data }` and `StdinClose {}` variants to `ExecuteRequest` in proto.
    - CLI-side: when `--stdin` set, spawn task reading `tokio::io::stdin()` → send `StdinChunk` records → on EOF send `StdinClose`.
    - When `--stdin` not set: send `StdinClose` immediately.
    - Daemon-side: feed `StdinChunk` data into SSH channel stdin. For Rhopd hops: forward into inner gRPC stream.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.27, 17.28, 17.29_
  - [x] 13A.8 Implement host-key flags for `rhop remote connect`
    - Add `--accept-new-host-key` and `--fingerprint <sha256>` (mutually exclusive) to `rhop remote connect`.
    - Wire into host-key trust flow: TOFU mode trusts without prompt; pinned mode compares fingerprint.
    - Honor `--non-interactive` when neither flag set and host unknown → exit 126.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.30, 17.31, 17.32, 17.33, 17.34_
  - [x] 13A.9 Implement capability discovery
    - `rhop --version --output json` emits JSON with `version`, `capabilities`, `exit_codes`.
    - Add `rhop server list --no-cache` as synonym for `--refresh`.
    - Run `cargo build && cargo test` and confirm green.
    - _Requirements: 17.35, 17.36_

- [x] 14. (Rollout 11) Delete dead code identified in the design's "Code deletions"
  - [x] 14.1 Remove `ClientMode` and `RemoteClientConfig` from `src/config.rs`
    - Verify no references remain in `src/`.
    - _Requirements: 1.4, 9.4, 10.10, 10.11_
  - [x] 14.2 Remove `RemoteCommand::Enable` / `RemoteCommand::Disable` and the SSH-out-of-CLI surface from `src/cli.rs`
    - Verify the CLI no longer imports anything that opens an SSH connection.
    - _Requirements: 1.3, 1.4, 10.1_
  - [x] 14.3 Move the residual remote SSH client surface from `src/remote.rs` into `src/jump/rhopd.rs` and `src/jump/auth.rs`, then delete dead functions
    - Functions to move/delete: `RemoteClientHandler`, `parse_remote_target`, `connect_remote_client`, `enable_remote_mode`, `disable_remote_mode`, `apply_remote_target`, `fetch_remote_host_key`, `inspect_known_host`, `trust_known_host`, `known_hosts_path` helper, `identity_file` helper, `normalize_remote_paths`.
    - The CLI must not import any of these.
    - _Requirements: 1.3, 12.1, 12.2, 12.3_
  - [x] 14.4 Remove the `[jumpserver]` top-level block from the `config.toml` schema in `src/config.rs`
    - Move the equivalent fields onto `JumpserverJumpHostFields` if not already there.
    - Drop unused `JumpserverConfig`.
    - _Requirements: 10.12, 14.1_
  - [x] 14.5 Drop `ConfigListRequest`/`ConfigListResponse` from CLI use
    - Replace with `ListJumpHosts` everywhere they were consumed.
    - _Requirements: 9.4, 10.4_
  - [x] 14.6 Run `cargo build && cargo test` and confirm green.

- [x] 15. (Rollout 12) Update example configs and documentation
  - [x] 15.1 Rewrite `config.example.toml`
    - Use the new schema: `[server]`, `[server.local]`, `[server.remote]`, `[ssh]`, `[copy]`, one or more `[[jump_hosts]]` examples (one `rhopd`, one `jumpserver`).
    - No top-level `[jumpserver]`.
    - _Requirements: 4.1, 10.12, 14.1, 14.4_
  - [x] 15.2 Update `server.example.toml`
    - Reflect any field changes that surfaced during tasks 7 and 9 (alias, host, port, user, identity_file/password). No `[remote]` block.
    - _Requirements: 10.12_
  - [x] 15.3 Update `README.md`
    - Document the new CLI surface (`rhop remote connect/remove/list`), the `[[jump_hosts]]` schema, the unified pipeline diagram, and removal of `rhop remote enable/disable`.
    - _Requirements: 1.1, 1.3, 10.1, 10.4_
  - [x] 15.4 Run `cargo build && cargo test` and confirm green.

- [x] 16. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP. Property tests live close to the code they cover (P5 with the trait, P11 with `RhopdJumpHost`, P6 with the pool, P7 with the resolver, P9/P10 with config and address parsing, P12/P13 with the aggregator, P8 with the auth router, P1–P4 with the integration harness).
- Each major task ends with a build-and-test gate so the tree stays green at every rollout step, matching the design's promise that "this sequence keeps the tree green at the end of each step."
- The integration `TestHarness` in task 13.1 is the load-bearing test fixture for Properties 1–4 and the example tests in 13.6 — it spawns two `RhopRpcService` instances over `tokio::io::duplex` so every property runs hermetically in milliseconds.
- Every property test carries the exact comment tag `// Feature: rhopd-jumpserver-architecture, Property N: {title}` so reviewers can grep design properties to their implementations.
- The `[[jump_hosts]]` validation lives in two layers (CLI pre-flight and daemon startup) on purpose — Property 10 covers both layers.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0,  "tasks": ["1"] },
    { "id": 1,  "tasks": ["2.1", "2.2"] },
    { "id": 2,  "tasks": ["2.3"] },
    { "id": 3,  "tasks": ["2.4", "2.5"] },
    { "id": 4,  "tasks": ["2.6"] },
    { "id": 5,  "tasks": ["3.1", "3.2"] },
    { "id": 6,  "tasks": ["3.3"] },
    { "id": 7,  "tasks": ["4.1", "4.3"] },
    { "id": 8,  "tasks": ["4.2"] },
    { "id": 9,  "tasks": ["4.4"] },
    { "id": 10, "tasks": ["4.5"] },
    { "id": 11, "tasks": ["6.1"] },
    { "id": 12, "tasks": ["6.2"] },
    { "id": 13, "tasks": ["6.3"] },
    { "id": 14, "tasks": ["6.4"] },
    { "id": 15, "tasks": ["7.1", "7.3"] },
    { "id": 16, "tasks": ["7.2"] },
    { "id": 17, "tasks": ["7.5"] },
    { "id": 18, "tasks": ["7.4"] },
    { "id": 19, "tasks": ["7.6", "7.7"] },
    { "id": 20, "tasks": ["8.5"] },
    { "id": 21, "tasks": ["8.1"] },
    { "id": 22, "tasks": ["8.2"] },
    { "id": 23, "tasks": ["8.3"] },
    { "id": 24, "tasks": ["8.4"] },
    { "id": 25, "tasks": ["8.6"] },
    { "id": 26, "tasks": ["9.1"] },
    { "id": 27, "tasks": ["9.2"] },
    { "id": 28, "tasks": ["9.3"] },
    { "id": 29, "tasks": ["11.1"] },
    { "id": 30, "tasks": ["11.2", "11.5"] },
    { "id": 31, "tasks": ["11.3"] },
    { "id": 32, "tasks": ["11.4"] },
    { "id": 33, "tasks": ["11.6", "11.7"] },
    { "id": 34, "tasks": ["12.1"] },
    { "id": 35, "tasks": ["12.2"] },
    { "id": 36, "tasks": ["12.3"] },
    { "id": 37, "tasks": ["12.4"] },
    { "id": 38, "tasks": ["13.1"] },
    { "id": 39, "tasks": ["13.2", "13.3", "13.4", "13.5"] },
    { "id": 40, "tasks": ["13.6"] },
    { "id": 41, "tasks": ["13A.1"] },
    { "id": 42, "tasks": ["13A.2", "13A.3"] },
    { "id": 43, "tasks": ["13A.4", "13A.5"] },
    { "id": 44, "tasks": ["13A.6", "13A.7"] },
    { "id": 45, "tasks": ["13A.8", "13A.9"] },
    { "id": 46, "tasks": ["14.1"] },
    { "id": 47, "tasks": ["14.4"] },
    { "id": 48, "tasks": ["14.2"] },
    { "id": 49, "tasks": ["14.3"] },
    { "id": 50, "tasks": ["14.5"] },
    { "id": 51, "tasks": ["15.1", "15.2", "15.3"] }
  ]
}
```
