# xho Configuration & Usage Reference

## Table of Contents

- [Config File Locations](#config-file-locations)
- [Main Config (config.toml)](#main-config)
- [Server List (server.toml)](#server-list)
- [Transparent SSH Proxy](#transparent-ssh-proxy)
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
| Control-plane authorized keys | `~/.xho/authorized_keys` | Machine pubkeys for daemon↔daemon auth (port 12222) |
| Proxy authorized keys | `~/.xho/proxy_authorized_keys` | Human pubkeys for the transparent SSH proxy (port 2222) |

## Main Config

```toml
[server]
log_path = "~/.xho/xhod.log"
log_level = "info"  # trace|debug|info|warn|error

[server.local]
enable = true
socket_path = "~/.xho/xhod.sock"

# Control plane: daemon↔daemon xho-rpc/xho-reverse subsystems + OpenSession
# multi-hop. v0.4.0 moved 2222 → 12222 to free 2222 for the proxy below.
[server.remote]
enable = true
listen_addr = "0.0.0.0:12222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"
# Optional long-lived token accepted by SSH password auth as a fallback
# when no dynamic token from `xho token gen` matches. Accepts plaintext or
# any secret reference (vault:NAME, env:VAR, file:PATH). Leave unset to
# only accept short-lived tokens issued via `xho token gen`.
bootstrap_token = "vault:bootstrap_token"

# Transparent SSH proxy: human-facing ssh/scp/sftp/rsync (v0.4.0). The SSH
# username is the target node; auth via a separate proxy_authorized_keys so a
# human key can't be reused for control-plane access. Enabled by default.
[server.proxy]
enable = true
listen_addr = "0.0.0.0:2222"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/proxy_authorized_keys"
# sftp_server_path = "/usr/lib/openssh/sftp-server"  # auto-detected when unset

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
address = "xho@bastion.example.com:12222"
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

## Transparent SSH Proxy

xhod's second SSH server (default port **2222**) lets humans connect with plain
`ssh`/`scp`/`sftp`/`rsync` — no `xho` client and no per-target client config.
The SSH **username is the target** (a `server.toml` alias, an IP, or `_self` for
the xhod host itself); auth is by public key against a **separate**
`proxy_authorized_keys`. xhod brokers each target's real credentials (key or
vault-resolved password), so a human needs just one key to reach every target.

```bash
ssh web1@bastion.example.com -p 2222                 # interactive shell
ssh web1@bastion.example.com -p 2222 -- uname -a     # one-off exec
scp -P 2222 file.txt web1@bastion.example.com:/tmp/  # upload (scp uses -P)
scp -P 2222 web1@bastion.example.com:/etc/hosts .    # download
sftp -P 2222 web1@bastion.example.com
ssh _self@bastion.example.com -p 2222                # the xhod host itself
```

Config: `[server.proxy]` (see [Main Config](#main-config)), enabled by default.
It is deliberately separate from the control-plane `[server.remote]` (port
12222) so a human key can't grant daemon-to-daemon access. Multi-hop —
`ssh node@xhod` reaching a machine *behind another xhod* — works via the
`OpenSession` tunnel.

> **scp gotcha:** `user@host:path` makes the first colon ambiguous for a
> multi-hop username containing a colon (`gateway:server`). Pass it separately:
> `ssh -p 2222 -o User=gateway:server bastion.example.com`.

## Jump Host Types

### xhod (remote daemon)

Routes through a remote xhod instance over its custom SSH protocol.

```toml
[[gateways]]
kind = "xhod"
name = "prod"
address = "xho@bastion.example.com:12222"
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
| "auth failure" | Wrong key or missing authorized_keys entry | Run `xho host login --token <T>` or add key manually |
| "host key rejected" | Missing or mismatched known_hosts | `xho host add` or update known_hosts |
| "transport error" | SSH connection dropped | Auto-retries; check network |
| "token rejected" | Token expired, already consumed, or unknown | Run `xho token gen` for a fresh token |

## Token-based bootstrap

`xho host add` and `xho host login` can auto-append the client's public key
to the remote xhod's `authorized_keys` by presenting a short-lived token
issued on the remote host. This avoids editing `authorized_keys` by hand.

### Workflow

```bash
# 1. On the remote host (where xhod runs): generate a token.
xho token gen                       # 5m, single-use (default)
xho token gen --ttl 1h --reusable --label ci

# 2. On the client: add or re-login with the token.
xho host add prod-xhod xho@bastion:12222 --token <TOKEN>
xho host login prod-xhod --token <TOKEN>
```

`xho host add` without a token (or with empty prompt input) skips bootstrap
and falls back to the legacy flow (trust host key, persist config). The
client must then have its key added to `authorized_keys` by other means.
`xho host login` always requires a token.

### Managing tokens

```bash
xho token list                       # prefix | expires_at | once | consumed | label
xho token invalid <prefix-or-full>   # invalidate
```

Tokens are in-memory only and vanish on daemon restart.

### Fixed bootstrap token

A long-lived fallback can be set in the daemon's config:

```toml
[server.remote]
bootstrap_token = "vault:bootstrap_token"   # or plaintext, env:VAR, file:PATH
```

Store it in the vault first via `xho secret set bootstrap_token`. If the
field is unset or empty, only dynamic tokens issued by `xho token gen` are
accepted by SSH password auth.

### Terminal not restored after interactive session

Run `reset` to manually restore terminal state.
