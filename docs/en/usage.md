# Usage Guide

## Installation

### Build from Source

```bash
# Dependencies: Rust toolchain, protoc
cargo build --release
```

Generated binaries: `target/release/xho` and `target/release/xhod`

### Download from GitHub Release

Each push of a `v*` tag automatically publishes binaries for the following platforms:
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

### Docker

```bash
docker build -t xhod:latest .
docker run --rm -p 2222:2222 -v /etc/xho:/etc/xho xhod:latest
```

## Quick Start

### Zero-Configuration Run

As long as the target host is configured in `~/.ssh/config`, it can be used directly:

```bash
# The daemon starts automatically
xho exec 192.0.2.163 hostname
```

### Using server.toml

Create `~/.xho/server.toml`:

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"

[servers.web1]
host = "10.0.1.10"
user = "deploy"

[servers.db1]
host = "10.0.1.20"
user = "dba"
password = "secret"
```

Then execute using the alias:

```bash
xho exec web1 -- uname -a
```

## Command Reference

### Global Options

The following options apply to all subcommands (placed before the subcommand name):

- `--output text|json` — Output format, default `text`; `json` outputs NDJSON (one JSON object per line, convenient for script parsing)
- `--non-interactive` — Disable all interactive prompts (authentication, command confirmation); fail immediately instead of waiting when input is required

```bash
xho --output json ls
xho --non-interactive exec web1 -- hostname
```

### Executing Remote Commands

```bash
# Basic usage
xho exec <target> -- <command> [args...]

# Allocate PTY (colored output, interactive programs)
xho exec --tty <target> -- vim README.md

# Disable PTY
xho exec --no-tty <target> -- cat /etc/hosts

# Forward stdin
xho exec --stdin <target> -- bash < script.sh

# Set timeout
xho exec --timeout 30s <target> -- long-running-task

# Wrap command with a specified shell (load remote rc / aliases)
xho exec --shell zsh <target> -- ll

# Disable shell wrapping
xho exec --no-shell <target> -- /bin/ls

# Explicitly specify gateway route
xho exec remote-xhod:web1 -- hostname
```

### Interactive Mode

Automatically activated when the following conditions are met:
- `--tty` is set
- stdin is a TTY
- stdout is a TTY

```bash
# Automatically enter interactive mode
xho exec --tty host1 -- vim README.md
xho exec --tty host1 -- htop
xho exec --tty host1 -- bash
```

Interactive mode features:
- Terminal automatically enters raw mode
- All keystrokes are forwarded to the remote in real time
- Window size changes are automatically synchronized
- Terminal is automatically restored on exit

### File Copy

```bash
# Upload
xho cp local.txt host1:/tmp/

# Download
xho cp host1:/etc/hosts ./hosts

# Recursively copy directories
xho cp -r ./project host1:/opt/

# Quiet mode (hide progress bar and non-error messages)
xho cp -q local.txt host1:/tmp/

# Set timeout
xho cp --timeout 60s -r ./project host1:/opt/
```

### Server List

```bash
# List all reachable servers (local + each jump host)
xho ls

# Force refresh cache
xho ls --refresh
```

### Daemon Management

```bash
# Check status
xho status

# Manual start
xho daemon start
xho daemon start --config ~/.xho/config.toml --log-level debug

# Stop
xho daemon stop

# Restart (inherits last start parameters)
xho daemon restart
```

### Jump Host Management

```bash
# Add an xhod jump host
xho host add prod xho@bastion.example.com:2222

# Specify identity file when adding
xho host add prod xho@bastion.example.com:2222 --identity-file ~/.ssh/id_ed25519

# Specify known_hosts file
xho host add prod xho@bastion.example.com:2222 --known-hosts ~/.xho/known_hosts

# Automatically accept unknown host key on first connection (mutually exclusive with --fingerprint)
xho host add prod xho@bastion.example.com:2222 --accept-new-host-key

# Or explicitly verify a specific fingerprint (mutually exclusive with --accept-new-host-key)
xho host add prod xho@bastion.example.com:2222 --fingerprint SHA256:abcdef...

# List configured jump hosts
xho host list

# Remove
xho host remove prod
```

## Configuration

### Configuration File Locations

- Main config: `~/.xho/config.toml`
- Server inventory: `~/.xho/server.toml` (path can be changed in the main config)
- Known hosts: `~/.xho/known_hosts`

### Main Config (`config.toml`)

```toml
[server]
log_path = "/var/log/xhod.log"
log_level = "info"

[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "prod-xhod"]
pty = true
connect_timeout = "10s"
keepalive_interval = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

# Jump Hosts
[[gateways]]
name = "prod-xhod"
kind = "xhod"
address = "xho@bastion.example.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[[gateways]]
name = "corp-jump"
kind = "jumpserver"
host = "jumpserver.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
totp_secret_base32 = "YOUR_SECRET"
totp_digits = 6
totp_period = 30

# Command review (optional)
[review]
enable = true
endpoint = "https://api.openai.com/v1/chat/completions"
model = "gpt-4.1-mini"
timeout = "10s"
failure_action = "deny"

[review.fast_allowlist]
enable = true
commands = ["ls", "ls *", "cat *", "grep *"]

[review.policy]
safe = "allow"
risky = "confirm"
dangerous = "deny"
```

### Server Inventory (`server.toml`)

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"

[servers.web1]
host = "10.0.1.10"
port = 22
user = "deploy"

[servers.db1]
host = "10.0.1.20"
user = "dba"
password = "secret"
```

Authentication priority: `password > identity_file > defaults.identity_file`

## Target Resolution

### Resolution Rules

| Format | Example | Meaning |
|--------|---------|---------|
| `jump:server` | `prod:web1` | Access web1 explicitly via the prod jump host |
| `server_alias` | `web1` | Look up across all sources (must be unique) |
| `host_or_ip` | `10.0.1.10` | IP derivation + fallback |

### IP Derivation

IP suffixes in hostnames are automatically extracted:

```
foo-192-0-2-163  →  192.0.2.163
bar-192-168-1-1  →  192.168.1.1
```

### Fallback Order

`ssh.fallback` defines the order of attempts when server.toml is not matched:

```toml
[ssh]
fallback = ["local", "prod-xhod", "corp-jump"]
```

- `"local"` — Attempt direct connection via `~/.ssh/config`
- `"<name>"` — Route through the corresponding jump host

## Deploying xhod to a Remote Server

### Server-Side Configuration

1. Deploy the binary to the server
2. Create config `~/.xho/config.toml`:

```toml
[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

[ssh]
server_config_path = "~/.xho/server.toml"
```

3. Add the client public key to `~/.xho/authorized_keys`
4. Create `~/.xho/server.toml` to define reachable targets
5. Start: `xho daemon start --config ~/.xho/config.toml`

### Client-Side Configuration

```toml
[[gateways]]
name = "prod"
kind = "xhod"
address = "xho@your-server.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[ssh]
fallback = ["local", "prod"]
```

### Using the Deployment Script

```bash
cargo build --release --bin xhod
scp target/release/xhod root@your-server.com:/usr/local/bin/xhod
```

## Runtime Modes

### Auto-Start (Recommended)

The daemon starts automatically when executing commands, no manual management required:

```bash
xho exec web1 -- hostname  # Automatically launched if the daemon does not exist
```

### systemd

```bash
sudo install -m 0644 packaging/systemd/xhod.service /etc/systemd/system/
sudo systemctl enable --now xhod
```

In systemd mode, the daemon is marked as `external` and `xho daemon stop` will be rejected.

### Docker

```bash
docker run --rm -p 2222:2222 -v /etc/xho:/etc/xho xhod:latest
```

## Connection Pool

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_connections_per_ip` | 10 | Maximum connections per target |
| `max_idle_time` | 10m | Idle connection reclamation time |
| `connect_timeout` | 10s | Connection timeout |
| `keepalive_interval` | 30s | SSH keepalive interval |

Behavior:
- Idle connection available → Reuse
- No idle connection but below limit → Create new
- Limit reached → Wait
- Transport error → Auto-reconnect once

## Command Review

### Enabling

```toml
[review]
enable = true
```

API key is provided via environment variable: `XHO_REVIEW_API_KEY` or `OPENAI_API_KEY`

### Two-Layer Filtering

1. **Local allowlist** (zero latency):

```toml
[review.fast_allowlist]
enable = true
commands = ["ls", "ls *", "cat *", "kubectl get *"]
```

Rules: Entries containing `*` are wildcard matches, otherwise exact matches.

2. **LLM Review**: Complex commands (containing `&&`, `||`, `$()`, `bash -c`, etc.) are sent to the LLM.

### Policy

```toml
[review.policy]
safe = "allow"       # Execute directly
risky = "confirm"    # Requires user confirmation
dangerous = "deny"   # Deny
```

## Troubleshooting

### Daemon Won't Start

```bash
# Check if a process already exists
ps aux | grep xhod

# Check the socket
ls -la ~/.xho/xhod.sock

# View logs
tail -50 ~/.xho/xhod.log
```

### Connection Failure

```bash
# Check daemon status and connection pool
xho status

# Check target resolution
xho exec --no-tty <target> -- echo ok

# Check remote daemon
ssh root@server "/root/xho/xho status"
```

### Interactive Mode Issues

- Terminal not restored: the `reset` command can manually restore it
- Not entering interactive mode: verify `--tty` is set and both stdin/stdout are TTYs
- Automatically degrades to non-interactive mode when used through a pipe
