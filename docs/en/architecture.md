# Cross Host Ops Architecture

## Overview

Cross Host Ops (xho) is a Rust-based remote command execution and file copy tool with a **CLI + Daemon separation** architecture:

- **`xho`** (`src/bin/xho.rs`) — The client, responsible for user interaction, argument parsing, terminal raw mode, and streaming output display.
- **`xhod`** (`src/bin/xhod.rs`) — The daemon, responsible for target resolution, command review, connection pool management, command execution, and file transfer.

The CLI does not connect to target machines directly. Instead, it submits requests to the local daemon via gRPC, and the daemon handles all scheduling. The daemon has **two entry points** sharing the same RPC handling logic:

- **Local entry**: Unix Socket (`~/.xho/xhod.sock`), used by the local `xho` client.
- **Remote entry**: SSH Server (default TCP:2222), used by `xhod` on another machine as a stepping-stone.

The daemon uses the **Gateway** abstraction to unify three ways of reaching targets: direct SSH, remote xhod (SSH subsystem + gRPC), and jumpserver (interactive menu bastion).

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              用户终端                                         │
│  xho exec host1 -- ls                                                       │
│  xho cp local.txt host1:/tmp/                                               │
│  xho ls                                                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ gRPC over Unix Socket
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         xhod (本地 Daemon)                                  │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────────────┐ │
│  │ Resolver │  │ Reviewer │  │ Gateways │  │ Remote SSH Server          │ │
│  │ 目标解析  │  │ 命令审查  │  │ 网关管理  │  │ (接受远程 xhod 连接)       │ │
│  └─────┬────┘  └────┬─────┘  └─────┬────┘  └────────────────────────────┘ │
│        │             │              │                                       │
│        │             │     ┌────────┼────────────┐                          │
│        │             │     ▼        ▼            ▼                          │
│        │             │  Local    Xhod      Jumpserver                      │
│        │             │  Gateway  Gateway    Gateway                         │
│        │             │     │        │            │                          │
└────────┼─────────────┼─────┼────────┼────────────┼──────────────────────────┘
         │             │     │        │            │
         │             │     │SSH     │SSH sub     │SSH+PTY
         │             │     │        │system      │
         │             │     ▼        ▼            ▼
         │             │  End      远程         End
         │             │  Target   xhod        Target
         │             │           Daemon       (via menu)
         │             │              │
         │             │              │SSH
         │             │              ▼
         │             │           End Target
         │             │
         ▼             ▼
    Vec<Route>     allow/warn/confirm/deny
```

## Core Components

### 1. CLI (`src/cli/`)

The entry point for user interaction with xho.

- `mod.rs` — Main dispatch (`exec` / `cp` / `status` / `ls` / `host` / `daemon` subcommands), TTY/stdin intent resolution, timeout validation, `--` separator handling.
- `args.rs` — Clap argument definitions (`ArunCli` / `ArunCommand` / `DaemonCommand` / `HostCommand`).
- `exec.rs` / `copy.rs` / `host.rs` — Implementation of each operation: establishing RPC clients, streaming send/receive, interactive mode (raw mode, SIGWINCH forwarding), copy progress, host trust (trust-on-first-use).
- `client.rs` / `output.rs` / `progress.rs` / `prompt.rs` — RPC client wrapper, output formatting, progress bar, interactive prompts.

**Communication**: Connects to the local daemon via `~/.xho/xhod.sock` Unix Socket, using the gRPC bidirectional stream protocol defined in the proto. The CLI handles interactions for authentication prompts (`AuthPrompt`) and command confirmation (`ConfirmRequired`).

### 2. Daemon (`src/daemon/mod.rs`)

The core execution engine, listening for both local and remote connections, sharing the same `XhoRpcService` — only the transport layer differs.

**`DaemonState`** holds:
- `config: Arc<RwLock<AppConfig>>` — Hot-reloadable configuration
- `gateways: Vec<(String, Arc<dyn Gateway>)>` — Gateway list in declaration order (the first is always `"local"`)
- `reviewer` — Command reviewer
- `shutdown_tx` — Shutdown signal

**Startup flow**:
1. Load configuration → `gateway::build_gateways()` constructs all Gateways (no connections established during construction)
2. (Optional) Bind local Unix Socket listener
3. (Optional) Start remote SSH Server listener
4. Start idle connection reaper task (periodically calls each gateway's `prune_idle`)
5. Register SIGHUP handler (configuration hot-reload + log rotation)
6. Serve gRPC via `XhoRpcService`

**`XhoRpcService`** implements all RPCs defined in the proto (Execute / Copy / Status / ListServers / Shutdown / UpdateConfig / ListGateways).

### 3. Resolver (`src/daemon/resolver.rs`)

Parses the user-supplied target string into an ordered list of route candidates `Vec<Route>`.

**`Route`**:
```rust
pub struct Route {
    pub gateway_name: String,  // "local", "remote-xhod", "corp-jump" ...
    pub end_target: String,    // Final target alias or IP
}
```

**Resolution priority**:
1. **Explicit qualifier** `<gateway_name>:<server_alias>` — split at the first colon, route directly to the specified gateway. Example: `remote-xhod:sub-gw:server1` → gateway=`remote-xhod`, end_target=`sub-gw:server1`. Rejects port-like inputs (`host:22`) and IPv6 addresses.
2. **Merged view lookup** — Search for a bare alias across the aggregated `list_servers` results from all Gateways, requiring a unique match.
3. **Fallback list** — Generate candidates according to the `ssh.fallback` configuration order.

**`derive_target_ip`**: Derives an IP address from a hostname suffix, e.g., `foo-192-0-2-163` → `192.0.2.163` (takes the last 4 numeric segments joined by `.`).

### 4. Reviewer (`src/daemon/review.rs`)

Optional LLM-based command security review, intercepting before command execution.

**Two-layer filtering**:
1. **Local fast allowlist** — glob pattern matching (e.g., `ls *`, `cat *`); complex scripts (bash/python etc.) go directly to the LLM.
2. **LLM semantic review** — Sent to an OpenAI-compatible API, classifying the risk level.

**Risk levels and actions** (defined in `src/config/review.rs`):

| `RiskLevel` | Description | Default `ReviewAction` |
|-------------|-------------|------------------------|
| `Safe` | Safe | `Allow` |
| `Risky` | Risky | `Confirm` |
| `Dangerous` | Dangerous | `Deny` |

There are four `ReviewAction` variants: `Allow` / `Warn` / `Confirm` / `Deny`. The `[review.policy]` configuration maps each risk level to its corresponding action.

The Reviewer only reviews the raw command (`build_remote_command(argv)`), not the shell-wrapped command.

## Gateway Layer (`src/daemon/gateway/`)

Unified abstraction over all ways of reaching targets. Each Gateway manages its own connections, authentication, and reconnection internally.

### Gateway trait (`mod.rs`)

The sole interface for callers (daemon):

```rust
#[async_trait]
pub trait Gateway: Send + Sync {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError>;
    async fn exec_interactive(&self, target: &str, request: &InteractiveRequest)
        -> Result<InteractiveHandle, GatewayError>;
    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError>;
    /// Control-plane gRPC client (Xhod/ReverseProxy only) for OpenSession tunnels.
    async fn rpc_client(&self) -> Option<XhoRpcClient<Channel>> { None }
    fn kind(&self) -> GatewayKind;
    fn name(&self) -> &str;
    async fn prune_idle(&self);
}
```

> **v0.4.0**: `Gateway::copy` was removed — all copy operations now flow through `TargetSession` + SFTP-over-session (`session::sftp_copy`). The `rpc_client` accessor enables multi-hop tunnels.

**`GatewayKind`** enum: `Direct` / `Jumpserver` / `Xhod` / `ReverseProxy` / `Localhost`.

**`GatewayError`** carries an `ErrorKind` classification that drives error handling:
- `Resolution` — Target not found (try the next route candidate)
- `Transport` — Network failure (Gateway retries connection once internally)
- `Execution` — Command execution failure (return directly)
- `Unsupported` — Operation not supported (skip during `list_servers`)

### `build_gateways` (`mod.rs`)

Factory function that constructs the Gateway list according to the following rules:
1. **Always first** `"local"` → `LocalGateway` (reads `server.toml`, direct SSH).
2. Each `[[gateways]]` entry creates one Gateway in declaration order:
   - `kind = "xhod"` → `XhodGateway`
   - `kind = "jumpserver"` → `JumpserverGateway`
   - `kind = "direct"` → **`LocalGateway`** (uses the entry's own name, shares `server.toml` resolution logic, only differs for routing purposes)

> Note: There is no standalone `DirectGateway` type. The `direct` configuration reuses the `LocalGateway` implementation, only participating in Resolver routing under a different name.

### Three Gateway implementations

| Gateway | Connection method | Pool strategy | `list_servers` |
|---------|------------------|---------------|----------------|
| **LocalGateway** (`local.rs`) | Direct SSH | `ManagedPool<DirectPoolKey, DirectConnection>`, pooled by host/port/user/auth | Reads `server.toml`, zero I/O |
| **XhodGateway** (`xhod.rs`) | SSH subsystem → gRPC | `ManagedSingleton<XhoRpcClient>`, single shared client | gRPC `ListServers` (returns all Gateways aggregated by the remote daemon) |
| **JumpserverGateway** (`jumpserver.rs`) | SSH + PTY shell + menu | `ManagedSingleton<JumpserverTransport>` (one shared SSH connection) + `ManagedPool<target, JumpserverTargetShell>` (per-target cached PTY shell) | Not supported (`Unsupported`), zero I/O |

### Authentication (`auth.rs`)

Authentication is handled during internal connection establishment within each Gateway, transparent to the exec/copy caller.

## Session Layer (`src/daemon/session/`) — v0.4.0

The **unified `TargetSession` abstraction** is the single low-level contract every operation drives through — CLI `xho exec`/`cp`, the transparent SSH proxy, and the multi-hop `OpenSession` tunnel.

```rust
#[async_trait]
pub trait TargetSession: Send {
    async fn request_pty(&mut self, term: &str, cols: u32, rows: u32, modes: &[(Pty, u32)]) -> Result<()>;
    async fn set_env(&mut self, key: &str, value: &str) -> Result<()>;
    async fn exec(&mut self, command: &str) -> Result<()>;
    async fn shell(&mut self) -> Result<()>;
    async fn subsystem(&mut self, name: &str) -> Result<()>;      // "sftp"
    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()>;
    async fn signal(&mut self, signal: &str) -> Result<()>;
    async fn write_stdin(&mut self, data: &[u8]) -> Result<()>;
    async fn eof(&mut self) -> Result<()>;
    async fn next_event(&mut self) -> Option<SessionEvent>;        // Stdout / Stderr / ExitStatus / Eof
}
```

Four implementations (one per transport, not per feature):

| Implementation | Transport | Notes |
|---|---|---|
| `DirectSshSession` (`direct.rs`) | Raw russh client channel | Byte-perfect scp/sftp/exec/pty. Exit status via `Handler::exit_status` callback (russh drops it from `channel.wait()`). |
| `LocalSession` (`local.rs`) | Local PTY + spawned `sftp-server` | Full shell/exec/sftp for `_self` targets. |
| `TunneledSession` (`tunnel.rs`) | OpenSession RPC over control plane | Multi-hop: `ssh → local proxy → control plane → remote xhod → machine`. Recursive. |
| `JumpserverSession` (`jumpserver.rs`) | Wraps `JumpserverGateway` menu engine | Sentinel-free exec (prompt-based, exit code = 0); interactive shell via `exec_interactive`. |

**Factory**: `open_target_session(state, route)` → dispatches by `gateway.kind()`.  
**Copy**: `copy_via_session(state, route, spec)` → `subsystem("sftp")` + `russh-sftp` client over a duplex bridge (`sftp_copy.rs`).

## Transparent SSH Proxy (`src/daemon/proxy_server.rs`) — v0.4.0

A second russh server (`ProxySshServer`) on port **2222**. Human-facing: `ssh node@xhod -p 2222`.

- **Auth**: publickey via `proxy_authorized_keys` (separate from control plane's `authorized_keys`). SSH username = target node name.
- **Mechanism**: `ProxySshHandler` bridges inbound SSH requests (pty/exec/shell/subsystem/data/resize/signal) ↔ `TargetSession` obtained via `open_target_session`. Session events are written back via the inbound `Channel`'s `data()`/`exit_status()`/`eof()`/`close()` methods.
- **Full compatibility**: scp (sftp-mode + legacy `-O`), sftp subsystem, exec, interactive PTY, window resize — all transparent because the payload is never interpreted (raw bridge for direct targets).

## OpenSession Multi-hop Tunnel — v0.4.0

New bidirectional streaming RPC added to `XhoRpc`:

```proto
rpc OpenSession(stream SessionRequest) returns (stream SessionResponse);
```

Enables transparent `ssh`/`scp` to reach machines **behind another xhod**: `ssh node@xhod` → local proxy → control-plane `OpenSession` → remote xhod → `open_target_session` (recursive).

- **Transport**: `TunneledSession` uses the existing control-plane gRPC client (XhodGateway/ReverseProxyGateway's `rpc_client()`).
- **Server handler** (`daemon/mod.rs`): resolves the target, opens a `TargetSession`, and bridges the RPC stream ↔ session events.
- **Recursive**: each xhod can serve `OpenSession`, so arbitrary-depth hops are uniform.

## Port Layout (v0.4.0)

| Port | Service | Auth | Purpose |
|------|---------|------|---------|
| **2222** | `ProxySshServer` | `proxy_authorized_keys` (human pubkey, username=target) | Transparent `ssh`/`scp`/`sftp` |
| **12222** | `RemoteSshServer` (control plane) | `authorized_keys` (machine pubkey, user=xho) | `xho-rpc` + `xho-reverse` subsystems + `OpenSession` RPC |
| Unix socket | gRPC | (local) | CLI ↔ daemon |

## Authentication (`auth.rs`)

Authentication is handled during internal connection establishment within each Gateway, transparent to the exec/copy caller.

**`AuthPrompter`** callback signature:
```rust
pub type AuthPrompter = dyn Fn(AuthPrompt) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync;

pub struct AuthPrompt {
    pub prompt_id: String,
    pub target_label: String,
    pub kind: AuthPromptKind,
    pub secret: bool,
    pub message: String,
}
```

**Authentication modes**:

| Scenario | Handling |
|----------|----------|
| `identity_file` configured | SSH key authentication (automatic) |
| `password` configured | Password authentication (automatic) |
| Password not configured | Prompt user via `AuthPrompter` |
| `totp_secret_base32` configured | Auto-generate TOTP code (jumpserver MFA) |
| TOTP secret not configured | Prompt user for MFA code via `AuthPrompter` |

**Authentication data flow**: Gateway needs input → `(auth_prompter)(prompt)` → daemon forwards `AuthPrompt` to CLI via gRPC → CLI displays prompt and reads input → sends back to daemon via gRPC → passes to Gateway to complete authentication.

`auth.rs` also provides shared utilities: `parse_remote_target()` (parses `[user@]host[:port]`), known_hosts verification, and remote host key retrieval (trust-on-first-use).

## Connection Layer (`src/daemon/connection/`)

Internal implementation detail of Gateways, not visible outside the daemon (`pub(super)`).

### Connection trait (`mod.rs`)

```rust
#[async_trait]
pub(super) trait Connection: Send {
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32>;
    async fn copy(&mut self, spec: CopySpec) -> Result<()>;
    async fn exec_interactive(&mut self, request: &InteractiveRequest)
        -> Result<InteractiveHandle>;
    fn is_alive(&self) -> bool;
}
```

### Three implementations

| Connection | Transport | Created by |
|------------|-----------|------------|
| **DirectConnection** (`direct.rs`) | SSH channel (session) | LocalGateway |
| **XhodConnection** (`xhod.rs`) | gRPC Execute/Copy stream | XhodGateway |
| **JumpserverConnection** (`jumpserver.rs`) | PTY shell command interaction (with sentinel exit code extraction) | JumpserverGateway |

### `shared.rs`

Shared utilities for the connection layer:
- `shell_quote()` — Single-quote wrapping and `'\''` escaping
- `build_remote_command()` / `build_final_command()` — argv concatenation + shell configuration wrapping
- `wrap_in_shell()` — Wraps as `<shell> -ic '...'` (bash/zsh) or `<shell> -c '...'` (sh/fish, etc.)
- `PtyShell` — PTY management, prompt detection, sentinel exit code parsing

## Connection Management (`src/daemon/connection_manager.rs`)

Centralized connection pool/singleton management, reused by each Gateway:

- **`ManagedPool<K, T>`** — Reuses connections by key, with capacity semaphore, idle pruning, and automatic retry on transport errors. Used by LocalGateway (keyed by `DirectPoolKey`) and JumpserverGateway (keyed by target shell).
- **`ManagedSingleton<T>`** — Single shared connection, with generation-based invalidation and maximum lifetime pruning. Used by XhodGateway (shared gRPC client) and JumpserverGateway (shared SSH transport).
- **`RetryDecision`** — Connection establishment is phased (`Connect` / `Prepare` / `Started`); failures during the first two phases are retryable, failures after `Started` are not.

## Remote SSH Server (`src/daemon/ssh_server.rs`)

xhod can act as an SSH server to accept connections from remote xhod instances.

- **Listen**: `TCP:2222` (configurable via `server.remote.listen_addr`).
- **Authentication**: two paths are accepted —
  - `auth_publickey()` validates against `~/.xho/authorized_keys` (the normal path)
  - `auth_password()` validates a dynamic token (issued by `xho token gen`, in-memory) or the configured `bootstrap_token` (resolved via SecretResolver, supports `vault:`/`env:`/`file:`). After token auth the client can call the `BootstrapAuthorize` RPC on the same SSH session to have the daemon auto-append its public key to `authorized_keys`, avoiding manual key distribution.
- **Only accepted operation**: `subsystem_request("xho-rpc")` — treats the SSH channel's byte stream as a gRPC connection and passes it to the tonic Server (the same `XhoRpcService`).
- **Rejected operations**: `shell_request`, `exec_request`, `tcpip_forward` / `streamlocal_forward` (no shell login, direct exec, or port forwarding allowed).

Connections enter RPC processing via `IncomingConn::Remote` (carrying peer addr / user / key fingerprint).

## Communication Protocol (`proto/xho.proto`)

CLI ↔ daemon and daemon ↔ remote daemon share the same protocol.

| RPC | Type | Function |
|-----|------|----------|
| `Execute` | Bidirectional stream | Command execution (including interactive mode) |
| `Copy` | Bidirectional stream | File copy |
| `Status` | Unary | Query daemon status |
| `ListServers` | Unary | Retrieve server list (including merged view) |
| `Shutdown` | Unary | Shut down daemon |
| `UpdateConfig` | Unary | Hot-reload configuration |
| `ListGateways` | Unary | List configured Gateways |

**Execute stream messages**:
- Client → Daemon: `StartRequest`, `ConfirmRequest`, `AuthInputRequest`, `StdinData`, `WindowResize`
- Daemon → Client: `Stdout` / `Stderr`, `ExitStatus`, `ReviewResult`, `ConfirmRequired`, `AuthPrompt`, `Error`

## Error Classification & Retry

```
tonic::Status?
├─ NotFound                       → Resolution (try next route candidate)
├─ Unavailable/Cancelled/Unknown  → Transport (Gateway retries once internally)
└─ Other                          → Continue checking
russh::Error                      → Transport
Message contains "not found"                → Resolution
Message contains "channel closed"           → Transport
Default                           → Execution (return directly)
```

| Error Type | Handling |
|------------|----------|
| **Resolution** | Daemon tries the next route candidate |
| **Transport** | Gateway retries once internally; propagates upward on failure |
| **Execution** | Returns directly to CLI, no retry |
| **Unsupported** | Skip this Gateway during `list_servers` |

## Interactive Mode

Automatically activated when `--tty` + stdin is a TTY + stdout is a TTY:

```
┌─────────┐     StdinData      ┌────────┐    exec_interactive    ┌────────┐
│ Terminal │ ──────────────────▶│ Daemon │ ─────────────────────▶│ Remote │
│ (raw)   │                    │        │                        │  PTY   │
│         │ ◀──────────────────│        │ ◀───────────────────── │        │
└─────────┘     Stdout          └────────┘                        └────────┘
     │                              │
     │ SIGWINCH                     │ WindowResize
     └──────────────────────────────┘
```

The CLI puts the terminal into raw mode, forwards stdin byte-by-byte, synchronizes window size changes (SIGWINCH → `WindowResize`), and restores the terminal on exit.

## Shell Wrapping

Optional shell wrapping that executes remote commands within an interactive shell (loading `.bashrc`, aliases, `LS_COLORS`).

**Configuration priority** (resolved on the daemon side, `connection/shared.rs`):
1. CLI `--shell <name>` / `--no-shell` (highest)
2. `server.toml` per-server `shell = "zsh"`
3. `server.toml` `[defaults]` `shell = "bash"` (lowest)

**Wrapping format**:
- bash/zsh: `<shell> -ic '<escaped_command>'`
- sh/fish/others: `<shell> -c '<escaped_command>'`

Command names are not quoted (`build_interactive_shell_command`), ensuring alias expansion.

## Configuration Files

### `~/.xho/config.toml` — Main configuration (`AppConfig`)

```toml
[server]
log_path = "~/.xho/xhod.log"
log_level = "info"

[server.local]
enable = true
socket_path = "~/.xho/xhod.sock"

[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "corp-jump"]
pty = true
connect_timeout = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

[[gateways]]
kind = "xhod"
name = "remote-xhod"
address = "xho@203.0.113.10:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[[gateways]]
kind = "jumpserver"
name = "corp-jump"
host = "bastion.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"

[review]
enable = true
endpoint = "https://api.deepseek.com/v1/chat/completions"
model = "deepseek-v4-flash"
```

`AppConfig` fields: `server` (`ServerConfig`), `ssh` (`SshConfig`), `copy` (`CopyConfig`), `review` (`ReviewConfig`), `gateways` (`Vec<GatewayConfig>`).

### `~/.xho/server.toml` — Server inventory (`src/config/inventory.rs`)

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"
shell = "bash"

[servers.host1]
host = "203.0.113.10"
port = 22
user = "root"

[servers.host2]
host = "192.0.2.200"
user = "admin"
shell = "zsh"  # Overrides defaults
```

Each `ServerEntry` contains `alias` / `host` / `port` / `user` / `auth` (`DirectAuth::Key { identity_file }` or `DirectAuth::Password { password }`). When `password` is omitted, the user is prompted at connection time. Authentication priority: password > identity_file > defaults.identity_file.

`GatewayConfig` is a tag-discriminated enum: `Xhod` / `Jumpserver` / `Direct` (see `src/config/gateway.rs`).

## Directory Structure

```
src/
├── bin/
│   ├── xho.rs              # CLI entrypoint
│   └── xhod.rs             # Daemon entrypoint
├── cli/                    # CLI logic (argument parsing, interactive mode, raw mode, copy/exec)
│   ├── mod.rs  args.rs  client.rs  copy.rs  daemon.rs
│   ├── exec.rs  host.rs  output.rs  progress.rs  prompt.rs
├── config.rs               # AppConfig (shared library entry)
├── config/                 # Configuration types
│   ├── client.rs  copy.rs  duration.rs  gateway.rs  inventory.rs
│   ├── path.rs  review.rs  server.rs  ssh.rs
├── daemon/                 # Daemon business logic
│   ├── mod.rs              # Startup, listeners, shutdown, DaemonState, XhoRpcService
│   ├── rpc.rs              # Gateway dispatch, multi-candidate fallback
│   ├── resolver.rs         # target → Vec<Route>
│   ├── review.rs           # LLM command review
│   ├── ssh_server.rs       # RemoteSshServer, IncomingConn
│   ├── connection_manager.rs  # ManagedPool / ManagedSingleton
│   ├── gateway/            # Gateway abstraction and implementations
│   │   ├── mod.rs          # Gateway trait, GatewayKind, Route, GatewayError, build_gateways
│   │   ├── local.rs        # LocalGateway (direct SSH + ManagedPool)
│   │   ├── xhod.rs         # XhodGateway (SSH subsystem + gRPC + ManagedSingleton)
│   │   ├── jumpserver.rs   # JumpserverGateway (PTY shell + menu navigation)
│   │   └── auth.rs         # AuthPrompter, AuthPrompt, TOTP, known_hosts
│   └── connection/         # Connection trait (daemon-internal) and implementations
│       ├── mod.rs          # Connection trait (pub(super))
│       ├── direct.rs       # DirectConnection (SSH channel)
│       ├── xhod.rs         # XhodConnection (gRPC stream)
│       ├── jumpserver.rs   # JumpserverConnection (PTY shell)
│       └── shared.rs       # shell_quote, build_command, wrap_in_shell, PtyShell
├── copy_frames.rs          # File copy frame encode/decode (shared library)
├── protocol.rs             # gRPC types / internal protocol types (shared library)
├── exit_codes.rs           # Exit code handling (shared library)
├── output.rs               # Output formatting (shared library)
├── logging.rs              # Log configuration (shared library)
├── types.rs                # Shared types (shared library)
└── lib.rs                  # Module declarations
proto/
└── xho.proto               # gRPC protocol definition
```

Files at the `src/` root level contain only shared libraries that are independent of specific business logic (config, protocol, logging, etc.). All business logic lives under `cli/` and `daemon/`; Gateway and Connection are internal implementation details of the daemon.

## Data Flow

### `xho exec host1 -- ls` (local direct connection)

```
CLI
  → gRPC StartRequest { target:"host1", argv:["ls"], shell:"", no_shell:false }
  → Daemon
      → Resolver.resolve("host1") → [Route { gateway:"local", end_target:"host1" }]
      → Reviewer.review("host1", ["ls"], "'ls'") → allow
      → gateways["local"].exec("host1", ExecRequest { argv:["ls"], shell:"bash", ... })
          → LocalGateway.resolve_target("host1") → server.toml resolves host/port/user/auth
          → ManagedPool checkout or new DirectConnection (SSH)
          → DirectConnection.exec(): channel.exec("bash -ic 'ls'") → streaming stdout/stderr/exit_status
      → return exit_code
  → CLI displays output
```

### `xho exec remote-xhod:db01 -- ls` (via remote xhod)

```
CLI → Daemon
  → Resolver.resolve("remote-xhod:db01") → [Route { gateway:"remote-xhod", end_target:"db01" }]
  → gateways["remote-xhod"].exec("db01", req)
      → ManagedSingleton checkout shared gRPC client (new SSH subsystem "xho-rpc" connection if necessary)
      → Remote daemon's XhoRpcService handles Execute (remote Resolver/Gateway resolves "db01")
      → Results streamed back
```

### `xho ls` (merged view aggregation)

```
CLI → gRPC ListServers → Daemon
  → rpc::process_list_servers(): iterate gateways in declaration order
      ├─ LocalGateway.list_servers()        → Reads server.toml (zero I/O)
      ├─ XhodGateway.list_servers()         → gRPC ListServers (remote aggregation)
      └─ JumpserverGateway.list_servers()   → Err(Unsupported) → skip and mark
  → Merge all ServerListRow, attach source label (local / <gateway-name>)
  → CLI formats and displays
```

### Multi-candidate fallback `xho exec <bare-alias> -- ls`

```
CLI → Daemon
  → Resolver.resolve(<alias>) → multiple candidate Routes (in fallback order)
  → Try each:
      gateways[r0].exec(...) → Resolution error (target not in this gateway) → continue
      gateways[r1].exec(...) → Ok(exit_code) → return
  → If all fail, return the last error
```
