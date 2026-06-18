**English** | [中文](README.zh-CN.md)

# Cross Host Ops

Remote command execution and file copy tool. Manages SSH connection pools through a local daemon, supporting three routing methods to reach target servers: direct connection, jump host, and remote xhod.

## Features

- **Interactive PTY** — Run full-screen programs like vim, htop, with an experience identical to native SSH
- **Connection Pool** — Reuse SSH connections by target IP, avoiding repeated handshakes
- **Multiple Gateways** — Direct SSH, enterprise jumpserver (MFA), remote xhod daemon
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
```

## Architecture Overview

```
xho (CLI) ──Unix socket──▶ xhod (Daemon) ──Gateways──▶ End Target
```

- CLI communicates with the local daemon via gRPC over Unix socket
- Daemon manages connection pool, target resolution, command review
- Three Gateway types are fully interchangeable: direct, jumpserver, xhod

See [architecture](docs/en/architecture.md) for detailed architecture design.

## Usage

```bash
# Basic execution
xho exec <target> -- <command> [args...]

# PTY mode (color output, interactive programs)
xho exec --tty <target> -- ls --color

# Explicitly specify gateway route
xho exec prod:web1 -- hostname

# Daemon management
xho status
xho daemon start --config ~/.xho/config.toml
xho daemon restart

# Gateway management
xho host add prod xho@bastion.example.com:2222
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

[[gateways]]
name = "prod"
kind = "xhod"
address = "xho@bastion.example.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"
```

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
docker run --rm -p 2222:2222 -v /etc/xho:/etc/xho xhod:latest
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

- [architecture](docs/en/architecture.md) — System design, component interaction, data flow
- [usage](docs/en/usage.md) — Installation, configuration, command reference, troubleshooting
- [config.example.toml](config.example.toml) — Complete configuration reference
- [server.example.toml](server.example.toml) — server.toml format

## License

MIT
