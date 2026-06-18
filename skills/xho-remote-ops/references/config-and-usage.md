# xho Configuration & Usage Reference

## Table of Contents

- [Config File Locations](#config-file-locations)
- [Main Config (config.toml)](#main-config)
- [Server List (server.toml)](#server-list)
- [Jump Host Types](#jump-host-types)
- [Target Resolution Rules](#target-resolution)
- [Connection Pool](#connection-pool)
- [Command Review](#command-review)
- [Troubleshooting](#troubleshooting)

## Config File Locations

| File | Path | Purpose |
|------|------|---------|
| Main config | `~/.xho/config.toml` | Daemon, SSH, gateways, review |
| Server list | `~/.xho/server.toml` | Target server definitions |
| Known hosts | `~/.xho/known_hosts` | xhod host key verification |
| Host key | `~/.xho/host_key` | Server-side identity (auto-generated) |
| Authorized keys | `~/.xho/authorized_keys` | Client public keys for auth |

## Main Config

```toml
[server]
log_path = "~/.xho/xhod.log"
log_level = "info"  # trace|debug|info|warn|error

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
ssh_config_path = "~/.ssh/config"
server_config_path = "~/.xho/server.toml"
fallback = ["local", "prod-xhod"]
tty = true
stdin = false
auto_tty_detect = true
connect_timeout = "10s"
keepalive_interval = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

[[gateways]]
kind = "xhod"
name = "prod-xhod"
address = "xho@bastion.example.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[[gateways]]
kind = "jumpserver"
name = "corp-jump"
host = "jumpserver.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
totp_secret_base32 = "YOUR_SECRET"
totp_digits = 6
totp_period = 30
```

## Server List

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"
shell = "bash"

[servers.web1]
host = "10.0.1.10"
port = 22
user = "deploy"

[servers.db1]
host = "10.0.1.20"
user = "dba"
password = "secret"
shell = "zsh"
```

Auth priority: `password > identity_file > defaults.identity_file`

## Jump Host Types

### xhod (remote daemon)

Routes through a remote xhod instance over its custom SSH protocol.

```toml
[[gateways]]
kind = "xhod"
name = "prod"
address = "xho@bastion.example.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"
```

### jumpserver (interactive with MFA)

Routes through an interactive jumpserver requiring menu navigation and TOTP.

```toml
[[gateways]]
kind = "jumpserver"
name = "corp"
host = "jump.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
totp_secret_base32 = "BASE32SECRET"
totp_digits = 6
totp_period = 30
```

### direct (named SSH entry)

Explicit SSH routing without fallback logic.

```toml
[[gateways]]
kind = "direct"
name = "lab"
host = "10.0.1.50"
port = 22
user = "admin"
```

## Target Resolution

1. If format is `jump:server` → route through named gateway
2. If alias exists in server.toml → use that server's config
3. If IP derivable from hostname → try fallback sources in order
4. IP derivation: `foo-192-0-2-163` → `192.0.2.163`

Fallback order defined by `ssh.fallback`:
- `"local"` = try `~/.ssh/config` direct connection
- `"<name>"` = try via named gateway

## Connection Pool

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_connections_per_ip` | 10 | Max SSH connections per target |
| `max_idle_time` | 10m | Idle connection reap time |
| `connect_timeout` | 10s | Connection establishment timeout |
| `keepalive_interval` | 30s | SSH keepalive ping interval |

Behavior: reuse idle → create new → wait at limit → auto-reconnect on transport error.

## Command Review

Optional LLM-based command safety review before execution.

```toml
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

API key: `XHO_REVIEW_API_KEY` or `OPENAI_API_KEY` env var.

## Troubleshooting

### Daemon won't start

```bash
ps aux | grep xhod          # Check existing process
ls -la ~/.xho/xhod.sock    # Check socket file
tail -50 ~/.xho/xhod.log   # Check logs
```

### Connection failures

```bash
xho status                           # Pool and gateway status
xho exec --no-tty <target> -- echo ok  # Minimal connectivity test
```

### Common errors

| Error | Cause | Fix |
|-------|-------|-----|
| "text file busy" | Daemon running during binary update | `xho daemon stop` first |
| "target not found" | Server not in any source | Add to server.toml or check fallback |
| "auth failure" | Wrong key or missing authorized_keys entry | Check key paths |
| "host key rejected" | Missing or mismatched known_hosts | `xho host add` or update known_hosts |
| "transport error" | SSH connection dropped | Auto-retries; check network |

### Terminal not restored after interactive session

Run `reset` to manually restore terminal state.
