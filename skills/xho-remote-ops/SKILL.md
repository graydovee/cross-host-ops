---
name: xho-remote-ops
description: Install/deploy xho & xhod and operate remote machines via the xho CLI. Use when: (1) deploying xhod to a server (Docker image or systemd unit) or installing/upgrading the xho client, (2) running commands on remote hosts, (3) copying files to/from remote, (4) checking server status or listing available servers, (5) managing daemon or jump host configuration. Triggers on phrases like "install xho", "deploy xhod", "upgrade xho", "run xhod in docker", "set up jump host", "run on remote", "execute on server", "copy to remote", "deploy to", "check status of", "list servers", "ssh to", or any task requiring remote machine access via xho.
---

# xho Remote Operations

## Install / Deploy

Deploy the **remote xhod** (the jump-host daemon) and install the **local `xho`
client**. For a remote xhod, prefer **Docker** (supervised, restarts on failure,
clean upgrades); fall back to **systemd** when Docker is unavailable. Running
xhod "bare" via `xho daemon start` works but is unsupervised and not recommended
for servers.

The release ships a multi-arch image `ghcr.io/graydovee/cross-host-ops:<tag>`
(amd64 + arm64) and per-target tarballs. Use the bundled script:

```bash
# Remote xhod via Docker (DEFAULT): pulls the image and runs a restarted
# container with the config dir mounted at /etc/xho, port 2222 exposed.
bash scripts/deploy.sh root@bastion.example.com
bash scripts/deploy.sh root@bastion.example.com --version v0.2.0

# Remote xhod via systemd: downloads the tarball, installs binaries + the
# xhod.service unit, enables & starts the service.
bash scripts/deploy.sh root@bastion.example.com --method systemd

# Target can't reach ghcr.io? Pull through a ghcr mirror instead:
bash scripts/deploy.sh root@bastion.example.com --registry ghcr.nju.edu.cn

# Host uses password login instead of keys (needs sshpass locally):
bash scripts/deploy.sh root@bastion.example.com --password 's3cret'
#   equivalent: SSHPASS='s3cret' bash scripts/deploy.sh root@bastion.example.com

# Local xho client (installs the binary to ~/.bin).
bash scripts/deploy.sh --local
```

| Option | Default | Notes |
|--------|---------|-------|
| `--method` | `docker` | `docker` \| `systemd` \| `bare` (remote only) |
| `--version` | latest (GitHub API) | Release / image tag, e.g. `v0.2.0` |
| `--registry` | `ghcr.io` | Image registry host. Set to a ghcr mirror when the target can't reach ghcr.io directly (e.g. `ghcr.nju.edu.cn`); the ref becomes `<host>/graydovee/cross-host-ops:<tag>`. Docker only |
| `--target` | auto (`uname -m`/`-s`) | Rust triple for systemd/bare/local: `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `*-apple-darwin` |
| `--prefix` | `/usr/local/bin` (remote), `~/.bin` (local) | Binary dir for systemd/bare |
| `--config` | `/etc/xho/config.toml` (remote), `~/.xho/config.toml` (local) | Daemon config path. For docker, its parent dir is mounted into the container at `/etc/xho` |
| `--name` | `xhod` | Docker container name |
| `--build` | off | Compile from local source (local / systemd / bare; ignored for docker) |
| `--password <pw>` | (none) | SSH password for the remote host (password login). Requires `sshpass` locally. Key auth is the default; prefer `--password`/`SSHPASS` only when the host has no key |

**Docker config layout.** The container runs `xhod --config /etc/xho/config.toml
--origin external`. Put your real config, host key, authorized keys, and
`server.toml` under the mounted host dir (default `/etc/xho`), and make sure the
config references them by paths inside that dir (e.g. `host_key_path =
"/etc/xho/host_key"`) so they persist across container restarts. Enable
`[server.remote]` and add the client's public key to the authorized-keys file.
See [references/config-and-usage.md](references/config-and-usage.md) for the full
config format.

The control socket is at `/var/run/xho/xhod.sock` (root daemon default, matching
the Docker / systemd convention). The deploy script mount keeps this directory
shared between host and container, so tools like `xho status` and `xho exec`
work from the host without additional client configuration.

Release asset pattern (for systemd/bare/local; target is auto-detected):
```
https://github.com/graydovee/cross-host-ops/releases/download/<tag>/cross-host-ops-<tag>-<target>.tar.gz
```

## Token-based bootstrap

When adding a gateway, `xho host add` can auto-append the client's public key
to the remote daemon's `authorized_keys` if you present a valid token. This
avoids the manual `cat >> /etc/xho/authorized_keys` step.

**Generate a token on the remote host** (where xhod lives):

```bash
# Default: 5-minute, single-use (recommended)
xho token gen

# 1 hour, reusable, tagged for CI
xho token gen --ttl 1h --reusable --label ci-runner

xho token list                 # see active tokens (prefix, expiry, once, consumed)
xho token invalid <prefix>     # invalidate by 8-char prefix or full token
```

Tokens live in memory only and are lost on daemon restart — keep TTL short.

**Add a gateway with auto-bootstrap** (run on the client):

```bash
# Pass --token, or omit it and enter at the prompt.
xho host add prod-xhod xho@bastion.example.com --token <TOKEN>

# Empty input at the prompt skips bootstrap (legacy behavior).
xho host add prod-xhod xho@bastion.example.com
```

**Re-register the public key on an already-configured gateway** (e.g. after
authorized_keys was wiped, or the client key changed):

```bash
xho host login prod-xhod --token <TOKEN>
xho host login prod-xhod        # will prompt
```

`host login` does not modify config; it only runs the bootstrap RPC.

**Fixed bootstrap token** (optional, long-lived fallback). Set
`[server.remote] bootstrap_token` in the daemon's config to accept a fixed
token. The value can be plaintext or any secret reference (`vault:NAME`,
`env:VAR`, `file:PATH`); prefer `vault:` so the plaintext never lands in the
config file:

```bash
# On the daemon host:
xho secret set bootstrap_token    # prompts for the value, stores it in the vault
# Then in config.toml:
#   [server.remote]
#   bootstrap_token = "vault:bootstrap_token"
```

If `bootstrap_token` is unset or empty, only dynamic tokens issued by
`xho token gen` are accepted.

## Quick Reference

```bash
# Execute command on target
xho exec <target> -- <cmd> [args...]

# Interactive PTY session (auto when stdin+stdout are TTY)
xho exec -it <target> -- bash

# Copy files
xho cp local.txt <target>:/path/
xho cp <target>:/path/file ./local
xho cp -r ./dir <target>:/path/

# List servers
xho ls

# Status
xho status

# Daemon management
xho daemon start|stop|restart
```

## Target Resolution

| Format | Example | Meaning |
|--------|---------|---------|
| `jump:server` | `prod-xhod:web1` | Explicit jump host routing |
| `alias` | `web1` | Lookup in all sources |
| `ip` | `10.0.1.10` | Direct IP with fallback |

## exec Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--tty` | `-t` | Allocate PTY |
| `--no-tty` | | Disable PTY |
| `--stdin` | `-i` | Forward stdin |
| `--no-stdin` | | Disable stdin forwarding |
| `--timeout` | | Abort after duration (e.g. `30s`, `2m`) |
| `--shell` | | Wrap in shell (e.g. `--shell bash`) |
| `--no-shell` | | Disable shell wrapping |

Combined `-it` = allocate PTY + forward stdin (interactive mode when both are TTY).

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1-123 | Remote command exit code |
| 124 | Timeout |
| 125 | xho/daemon failure |
| 126 | Auth/review denied |
| 127 | Target not found |

## Common Patterns

```bash
# Run and capture output
output=$(xho exec prod-xhod:web1 -- cat /etc/os-release)

# Pipe stdin (non-interactive)
echo "SELECT 1;" | xho exec -i --no-tty db1 -- mysql

# Upload a file then verify
xho cp ./app prod-xhod:web1:/opt/app
xho exec prod-xhod:web1 -- ls -la /opt/app

# Recursive directory copy
xho cp -r ./dist prod-xhod:web1:/var/www/html/

# Timeout protection
xho exec --timeout 60s web1 -- ./long-task.sh
```

## Configuration

See [references/config-and-usage.md](references/config-and-usage.md) for full config file format, jump host setup, connection pool tuning, and troubleshooting.

## Notes

- Daemon auto-starts on first command; no manual `daemon start` needed
- Use `--` separator when remote args start with `-`
- Single command without `--` is wrapped in `sh -c`
- `xho ls --refresh` bypasses server list cache
