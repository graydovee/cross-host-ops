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

### Secret Management

```bash
# Encrypt all plaintext secrets in config.toml + server.toml (see "Secret Management")
xho secret encrypt

# Preview the changes without writing files
xho secret encrypt --dry-run

# Store a single secret interactively (hidden input)
xho secret set server.db1.password
xho secret list
xho secret rekey --old ~/.ssh/id_ed25519 --new ~/.ssh/id_new
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
# A password can be a plaintext string or a secret reference (see "Secret
# Management" below). The reference form is recommended.
password = "vault:server.db1.password"
```

Authentication priority: `password > identity_file > defaults.identity_file`

## Secret Management

Sensitive values (passwords, TOTP secrets, API keys) need not be stored as
plaintext in config files. Every secret field may be written as a *reference*
that the daemon resolves to plaintext only at the moment it is needed.

### Reference Syntax

| Prefix | Example | Meaning |
|--------|---------|---------|
| `env:` | `env:DB_PASSWORD` | Read from an environment variable |
| `file:` | `file:/run/secrets/db` | Read from a file (systemd LoadCredential, docker/k8s secrets) |
| `vault:` | `vault:server.db1.password` | Decrypt from the local encrypted vault (default `<config_dir>/secrets`) |
| no prefix | `secret` | Treated as legacy plaintext; emits a warning when used |

Fields that accept references: `servers.*.password` in `server.toml`; and in
`config.toml` the jumpserver `totp_secret_base32`, direct gateway `password`,
`review.api_key`, and `review.headers.*`.

### The Vault

The vault stores ciphertext next to the **config file** (mode 0600), encrypted
with XChaCha20-Poly1305. The encryption key is **not stored separately** — it is
derived from an SSH private key via HKDF-SHA256:

- The vault lives at `<config_dir>/secrets` by default, so it follows the config
  file: a local user's `~/.xho/config.toml` puts it at `~/.xho/secrets`, while a
  docker / systemd deployment started with `--config /etc/xho/config.toml` puts
  it at `/etc/xho/secrets` — which is inside the mounted dir and persists across
  restarts. Override with `[secret].vault_path`.
- The key is chosen by `[secret].key_source`; when unset, it defaults to the
  daemon's own SSH host key (`[server.remote].host_key_path`) — which always
  exists, is unencrypted, and is daemon-owned whenever remote is enabled, so
  **most deployments need zero configuration** to use the vault. Only when
  remote is disabled does it fall back to `server.toml`'s `[defaults]
  .identity_file`.
- That private key must be **unencrypted** (no passphrase), so the daemon can
  load it unattended.
- The vault header records the key's fingerprint; using a different identity
  fails with a clear error and a hint to `rekey`.

```toml
# config.toml (all optional; with remote enabled you usually omit the whole section)
[secret]
# vault_path defaults to <config_dir>/secrets
# vault_path = "/etc/xho/secrets"
# key_source defaults to [server.remote].host_key_path; set only to override
# key_source = "/etc/xho/host_key"
```

### Commands

All `xho secret` operations are **local file operations** — they never go
through the daemon. Run them on whichever host owns the config you are managing
(locally, or over SSH for a remote host).

They default to `~/.xho/config.toml`. For a docker / systemd deployment whose
config lives under `/etc/xho`, point at it with `--config` (the vault then
resolves to `/etc/xho/secrets`):

```bash
# Encrypt every plaintext secret in config.toml + server.toml into the vault
# and replace them in place with vault: references (comments/formatting are
# preserved; a .bak backup is written first).
xho secret encrypt

# Preview the changes without writing any files.
xho secret encrypt --dry-run

# Store a single secret interactively (input is hidden).
xho secret set server.db1.password

# List entry names in the vault (values are never shown).
xho secret list

# Re-encrypt the whole vault under a different derivation key.
xho secret rekey --old ~/.ssh/id_ed25519 --new ~/.ssh/id_new

# Manage a config in a non-default location (docker / systemd / root install).
xho secret --config /etc/xho/config.toml encrypt
```

### Security Boundary

The vault keeps secrets out of config files, protecting against accidental git
commits, backups, and shoulder-surfing. But the private key used for derivation
and the ciphertext live on the **same host** — anyone who can read that key can
decrypt the vault. This is the same level of trust as "anyone who can read the
key can already SSH in directly," so it does not widen the attack surface. For
stronger isolation, use an external KMS or secret manager.

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
