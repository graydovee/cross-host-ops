**English** | [中文](README.zh-CN.md)

# Cross Host Ops

Remote command execution, file copy, and **transparent SSH proxy**. Manages SSH sessions through a local daemon with a unified `TargetSession` abstraction, supporting direct connection, jump host, remote xhod, and transparent `ssh`/`scp`/`sftp` access.

## Features

- **Transparent SSH Proxy** — `ssh node@xhod -p 2222` connects directly to the target. Full `scp`/`sftp`/`rsync` compatibility, no client-side setup needed
- **Multi-hop Tunnel** — `ssh → local xhod → control plane → remote xhod → machine` reaches servers behind other xhod instances
- **Interactive PTY** — Run full-screen programs like vim, htop, with an experience identical to native SSH
- **Connection Pool** — Reuse SSH connections by target IP, avoiding repeated handshakes
- **Multiple Gateways** — Direct SSH, enterprise jumpserver (MFA), remote xhod daemon — unified behind `TargetSession`
- **Unified Target Resolution** — server.toml aliases, explicit routing, IP derivation, fallback chain
- **Command Review** — Optional LLM security review, local allowlist + AI semantic analysis
- **File Copy** — `xho cp` aligns with scp semantics, supports recursion and mode preservation
- **Zero Configuration** — Works with just `~/.ssh/config`, no configuration files required

## Quick Start

```bash
# Build
cargo build --release

# Execute remote command (daemon starts automatically)
xho exec web1 -- hostname

# Interactive mode (auto-detected)
xho exec --tty host1 -- vim README.md

# File copy
xho cp local.txt host1:/tmp/

# List all reachable servers
xho ls

# Transparent SSH proxy (if proxy enabled on port 2222)
ssh web1@localhost -p 2222 -- hostname
scp file.txt web1@localhost:/tmp/ -P 2222
```

## Architecture Overview (v0.4.0)

```
 xho CLI (unchanged)                    ssh/scp/sftp (new)
   │ gRPC / Unix socket                   │ SSH / TCP 2222
   ▼                                      ▼
┌──────────────────── xhod (Daemon) ────────────────────────┐
│                                                            │
│  Execute/Copy RPC ──┐           ProxySshServer (port 2222)│
│  (CLI path)         │           (transparent proxy path)   │
│                     ▼                    │                 │
│              open_target_session(route)  │                 │
│                     │                    │                 │
│              ┌──────┴─── TargetSession ──┘                 │
│              │     (unified abstraction)                    │
│              ├── DirectSshSession  (raw SSH bridge)         │
│              ├── LocalSession      (PTY + sftp-server)      │
│              ├── TunneledSession   (OpenSession RPC)        │
│              └── JumpserverSession (menu-driven bastion)    │
│                                                            │
│  Control Plane SSH (port 12222)                            │
│  · xho-rpc subsystem (daemon↔daemon gRPC)                  │
│  · xho-reverse subsystem (reverse proxy registration)      │
│  · OpenSession RPC (multi-hop session tunnel)              │
└────────────────────────────────────────────────────────────┘
```

- **Two ports**: transparent proxy **2222** (human-facing `ssh`/`scp`), control plane **12222** (machine-to-machine RPC + reverse proxy + OpenSession tunnel)
- **Unified `TargetSession`**: all operations — CLI exec/cp, transparent proxy, multi-hop tunnel — flow through one session abstraction
- **Transparent proxy**: SSH username selects the target; xhod brokers credentials
- **Multi-hop**: `ssh node@xhod` → local proxy → control-plane `OpenSession` → remote xhod → machine

See [architecture](docs/en/architecture.md) for detailed architecture design.

## Usage

```bash
# Basic execution
xho exec <target> -- <command> [args...]

# PTY mode (color output, interactive programs)
xho exec --tty <target> -- ls --color

# Explicitly specify gateway route
xho exec prod:web1 -- hostname

# Transparent SSH proxy
ssh <node>@<xhod_host> -p 2222              # interactive shell
ssh <node>@<xhod_host> -p 2222 -- <command>  # exec
scp -P 2222 file.txt <node>@<xhod_host>:/tmp/ # file copy
sftp -P 2222 <node>@<xhod_host>               # sftp session

# Daemon management
xho status
xho daemon start --config ~/.xho/config.toml
xho daemon restart

# Gateway management
xho host add prod xho@bastion.example.com:12222
xho host list
```

See [usage](docs/en/usage.md) for complete usage instructions.

## Configuration

The program runs without any configuration files. When customization is needed, create `~/.xho/config.toml`:

```toml
[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "prod"]
pty = true

# Control plane: machine-to-machine RPC + reverse proxy (default 12222)
[server.remote]
enable = true
listen_addr = "0.0.0.0:12222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

# Transparent SSH proxy: human-facing ssh/scp/sftp (default 2222)
[server.proxy]
enable = true
listen_addr = "0.0.0.0:2222"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/proxy_authorized_keys"

[[gateways]]
name = "prod"
kind = "xhod"
address = "xho@bastion.example.com:12222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"
```

> **Port migration (v0.4.0)**: control plane moved from 2222 → **12222**. The transparent proxy now occupies 2222. Update `[[gateways]]` addresses and `reverse_proxy.server_address` from `:2222` to `:12222`.

See [config.example.toml](config.example.toml) for a complete configuration example.

## Deployment

### Local Usage

```bash
cargo build --release
# Binaries: target/release/xho, target/release/xhod
```

### Remote xhod

```bash
# Use deployment script
cargo build --release --bin xhod
scp target/release/xhod root@your-server.com:/usr/local/bin/xhod
```

### systemd / Docker

```bash
# systemd
sudo install -m 0644 packaging/systemd/xhod.service /etc/systemd/system/
sudo systemctl enable --now xhod

# Docker
docker build -t xhod:latest .
docker run --rm -p 2222:2222 -p 12222:12222 -v /etc/xho:/etc/xho xhod:latest
```

### GitHub Release

Pushing a `v*` tag automatically publishes multi-platform binaries and Docker images.

## Development

```bash
# Build
cargo build

# Test
cargo test

# Format
cargo fmt --all
```

## Documentation

- [architecture](docs/en/architecture.md) — System design, TargetSession abstraction, proxy, multi-hop tunnel
- [usage](docs/en/usage.md) — Installation, configuration, command reference, troubleshooting
- [config.example.toml](config.example.toml) — Complete configuration reference
- [server.example.toml](server.example.toml) — server.toml format

## License

MIT
