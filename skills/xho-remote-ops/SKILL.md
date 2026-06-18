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
