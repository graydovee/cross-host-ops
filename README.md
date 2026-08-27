**English** | [中文](README.zh-CN.md)

# Cross Host Ops

Remote command execution, file copy, and a **transparent SSH proxy**, brokered by a local daemon (`xhod`). Every backend — direct SSH, remote xhod instances, enterprise jumpserver bastions, self-registering NAT nodes — implements the same two-layer `Gateway` / `TargetSession` architecture and declares its capabilities explicitly, so higher-level features work uniformly across all of them.

## Features

### Operations

- **Remote execution** — `xho exec` runs one-shot commands or full interactive PTY sessions (vim, htop feel identical to native SSH); stdin forwarding, duration timeouts, and optional shell wrapping built in
- **File copy** — `xho cp` follows scp semantics: recursion, permission/mode preservation, directory destinations, live progress bars, and opt-in **resumable transfers** (`--resume/-c`: interrupted single-file uploads/downloads continue from the recorded offset after integrity checks, instead of starting over)
- **Transparent SSH proxy** — plain `ssh`/`scp`/`sftp`/`rsync` straight through the daemon on port 2222; the SSH *username* selects the target, no xho client or per-target config needed
- **Server inventory** — `xho ls` merges every configured source into one list; `xho status` shows daemon and connection-pool health

### Connectivity

- **Daemon-brokered access** — the CLI never talks to targets directly; `xhod` resolves routes, brokers credentials, and holds connections
- **Multiple gateways, one interface** — pooled direct SSH, remote **xhod** daemons, enterprise **Jumpserver** bastions (TOTP/MFA auto-login, navigated-shell session caching, target shell-history suppression), **reverse-proxy** nodes behind NAT that register themselves to a public hub, plus `_self` localhost — unified behind the `Gateway` trait with `EXEC | COPY | PROXY | LIST` capability flags; partial backends report unsupported operations clearly instead of misbehaving
- **Multi-hop tunnels** — reach machines behind other xhod instances: `ssh → local xhod → control plane → remote xhod → machine`, driven generically over the same `TargetSession` layer
- **Connection pooling** — authenticated SSH connections are reused per target IP (one handshake, many channels), with idle reaping and automatic reconnect
- **Unified target resolution** — server.toml aliases, explicit `gateway:target` routing, IP derived from hostname patterns, and a configurable fallback chain across sources
- **Reverse proxy topology** — a NAT-ed xhod dials out to a public server xhod and registers as a dynamic gateway; clients then reach it as `hub:node:target`, with keepalive-based half-open detection

### Security & compliance

- **AI command review** — optional LLM review before execution, configurable per operation kind (exec / copy): local fast-allowlist for trivially safe commands, copy blocklist/allowlist patterns (`.ssh`, `.kube`, …), and a risk policy mapping `safe | risky | dangerous` verdicts to `allow | confirm | deny`; LLM outages follow a configurable `failure_action`
- **Audit logging** — every machine operation (exec, copy, session tunnel, transparent proxy) is recorded as a JSON-Lines event with caller identity (peer address, SSH user, key fingerprint), operation details, and result; enabled by default (`~/.xho/audit.jsonl`, `/var/log/xho/audit.jsonl` for root)
- **Encrypted secret vault** — passwords/TOTP seeds/API keys never sit in plaintext: config files reference secrets as `vault:name`, `env:NAME`, or `file:/path`; the vault key is derived (HKDF) from an SSH private key so there's no separate key file to protect; `xho secret encrypt` migrates existing configs in one step
- **Token-based key bootstrap** — short-lived (optionally reusable) tokens let a client append its public key to a remote daemon's `authorized_keys` without shell access: `xho token gen` on the host, `xho host add --token` on the client
- **Separate trust domains** — human-facing proxy auth and machine-to-machine control-plane auth use distinct `authorized_keys` stores on distinct ports, so one key can't cross over

### Operator experience

- **Zero configuration** — works from `~/.ssh/config` alone; everything else is opt-in TOML with sane defaults, hot-reloadable via SIGHUP
- **Script-friendly contract** — `--output=json` emits stable NDJSON, and exit codes mean the same thing everywhere (`124` timeout, `125` daemon failure, `126` denied by auth/review, `127` target not found)
- **Lightweight deployment** — two static binaries (`xho`, `xhod`), systemd unit and Docker image included; multi-platform release artifacts built automatically on tag push

## Quick Start

```bash
cargo build --release          # binaries: target/release/xho, target/release/xhod

# Execute remote command (daemon starts automatically)
xho exec web1 -- hostname

# Interactive PTY session
xho exec -it web1 -- bash

# File copy — including resumable large-file transfer
xho cp app.tar.gz web1:/opt/
xho cp --resume big.tar.gz web1:/tmp/big.tar.gz   # re-run after an interruption

# List reachable servers / check daemon health
xho ls && xho status

# Transparent SSH proxy — any standard ssh/scp/sftp client
ssh web1@localhost -p 2222 -- uname -a
scp -P 2222 file.txt web1@localhost:/tmp/
rsync -e "ssh -p 2222" ./dist/ web1@localhost:/var/www/html/
```

## Architecture Overview

```
 xho CLI                                  ssh/scp/sftp
   │ gRPC / Unix socket                    │ SSH / TCP 2222
   ▼                                       ▼
┌───────────────────── xhod (Daemon) ─────────────────────────┐
│                                                              │
│  Execute/Copy/OpenSession RPC handlers                      │
│  + Oversight layer (AI command review + JSON-Lines audit)   │
│                  │                                           │
│                  ▼                                           │
│          gateway.open_session(target)                       │
│          gateway.open_exec_session(target, argv, …)         │
│                  │                                           │
│     ┌────────────┼────────────┬──────────────┬─────────┐    │
│     ▼            ▼            ▼              ▼         ▼    │
│  Direct     Localhost     Xhod          Reverse    Jumpserver│
│  Gateway    Gateway       Gateway       Proxy      Gateway  │
│  (pooled)                (tunneled)    (tunneled)  (partial)│
│     │            │            │              │         │    │
│     ▼            ▼            ▼              ▼         ▼    │
│  ┌──────────── TargetSession ─────────────────────────┐    │
│  │ DirectSshSession | LocalSession | TunneledSession   │    │
│  │  (pooled handle)   (PTY+pipe)    (OpenSession RPC)  │    │
│  │               JumpserverSession (PTY + sftp/shell)  │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  Control Plane SSH (port 12222)                              │
│  · xho-rpc subsystem (daemon↔daemon gRPC)                    │
│  · xho-reverse subsystem (reverse proxy registration)        │
│  · OpenSession RPC (multi-hop session tunnel)                │
└──────────────────────────────────────────────────────────────┘
```

- **Two-layer architecture**: the `Gateway` trait owns routing, pooling, and backend-specific command building; the `TargetSession` trait is the per-operation channel contract (exec, PTY, sftp, copy streams). Callers stay generic — capability flags decide what a route supports.
- **Oversight first**: requests surface through one facade that classifies them (LLM review, when enabled) and records the audit trail before anything touches a target.
- **Two ports**: transparent proxy **2222** (human-facing `ssh`/`scp`/`sftp`) and control plane **12222** (machine RPC, reverse-proxy registration, multi-hop tunnels).

See [architecture](docs/en/architecture.md) for the detailed design.

## Usage

```bash
# Basic execution / PTY mode / explicit gateway route
xho exec <target> -- <command>
xho exec --tty <target> -- ls --color
xho exec prod:web1 -- hostname

# Inventory & status
xho ls [--refresh]
xho status

# Transparent SSH proxy (username = target name, or _self for the daemon host)
ssh <node>@<xhod_host> -p 2222
sftp -P 2222 <node>@<xhod_host>

# Daemon management (auto-starts on first use anyway)
xho daemon start|stop|restart

# Gateway administration + key bootstrap
xho token gen                        # run on the daemon host
xho host add prod xho@bastion.example.com:12222 --token <TOKEN>
xho host login prod --token <TOKEN>  # re-enroll a changed client key

# Secrets vault (local file ops, nothing crosses the wire)
xho secret set db-password
xho secret encrypt [--dry-run]       # migrate plaintext creds into the vault
xho secret list / rekey
```

See [usage](docs/en/usage.md) for the complete command and configuration reference.

## Configuration

Zero-config by default (root daemon uses `/var/run/xho/xhod.sock` + `/etc/xho/`; regular users use `~/.xho/`). Create `~/.xho/config.toml` when you need more:

```toml
[ssh]
fallback = ["local", "prod"]         # resolution order across sources

[server.remote]                      # control plane (machine↔machine, port 12222)
enable = true
listen_addr = "0.0.0.0:12222"
user = "xho"

[server.proxy]                       # transparent SSH proxy (human-facing, port 2222)
enable = true
listen_addr = "0.0.0.0:2222"

[[gateways]]
name = "prod"
kind = "xhod"                        # or "jumpserver" / "direct"
address = "xho@bastion.example.com:12222"

[review.exec]                        # AI review, per operation kind
enable = true

[audit]                              # JSON-Lines audit trail (on by default)
enabled = true
```

All credential values accept `vault:` / `env:` / `file:` references instead of plaintext.

> **Port migration (v0.4.0)**: control plane moved 2222 → **12222**; the transparent proxy now occupies 2222.

See [config.example.toml](config.example.toml) for every section — including `reverse_proxy`, review policies/prompts, jumpserver session caching, and `[secret]` — fully commented.

## Deployment

### Binaries

```bash
cargo build --release   # target/release/xho, target/release/xhod
```

### Remote xhod

Prefer Docker or systemd for servers (supervised, clean upgrades):

```bash
# systemd
sudo install -m 0644 packaging/systemd/xhod.service /etc/systemd/system/
sudo systemctl enable --now xhod

# Docker
docker build -t xhod:latest .
docker run --rm -p 2222:2222 -p 12222:12222 -v /etc/xho:/etc/xho xhod:latest
```

### GitHub Release

Pushing a `v*` tag automatically publishes multi-platform musl/macOS binaries and a GHCR Docker image (`ghcr.io/graydovee/cross-host-ops:<tag>`). See [.agents/skills/xho-release/SKILL.md](.agents/skills/xho-release/SKILL.md) for the release checklist.

## Development

```bash
cargo build
cargo test
cargo fmt --all
cargo clippy --all-targets
```

## Documentation

- [changelog](CHANGELOG.md) — Release history ([中文](CHANGELOG.zh-CN.md))
- [architecture](docs/en/architecture.md) — System design, Gateway/TargetSession layers, proxy, multi-hop tunnel flows
- [usage](docs/en/usage.md) — Installation, configuration, command reference, troubleshooting
- [config.example.toml](config.example.toml) — Complete commented configuration reference
- [server.example.toml](server.example.toml) — server.toml format

## License

MIT
