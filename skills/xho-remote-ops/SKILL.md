---
name: xho-remote-ops
description: Install/deploy xho & xhod and operate remote machines via the xho CLI. Use when: (1) deploying xhod to a server (Docker image or systemd unit) or installing/upgrading the xho client, (2) running commands on remote hosts, (3) copying files to/from remote, (4) checking server status or listing available servers, (5) managing daemon or jump host configuration, (6) connecting to a remote host directly through xhod's transparent SSH proxy (plain ssh/scp/sftp/rsync, no xho client needed). Triggers on phrases like "install xho", "deploy xhod", "upgrade xho", "run xhod in docker", "set up jump host", "run on remote", "execute on server", "copy to remote", "deploy to", "check status of", "list servers", "ssh to", "scp to remote", "sftp", "rsync to", "ssh node@xhod", "transparent ssh proxy", or any task requiring remote machine access via xho.
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
# container with the config dir mounted at /etc/xho, ports 2222 (proxy) +
# 12222 (control plane) exposed.
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
--origin external`. Put your real config, host key, authorized-keys files, and
`server.toml` under the mounted host dir (default `/etc/xho`), and make sure the
config references them by paths inside that dir (e.g. `host_key_path =
"/etc/xho/host_key"`) so they persist across container restarts. Enable **both**
listeners and publish both ports: `[server.remote]` (control plane, 12222) for
daemon-to-daemon routing and `[server.proxy]` (transparent proxy, 2222) for
human `ssh`/`scp`. The control plane trusts *machine* keys in `authorized_keys`;
the proxy trusts *human* keys in `proxy_authorized_keys` (see **Transparent SSH
Proxy** below). See [references/config-and-usage.md](references/config-and-usage.md)
for the full config format.

The control socket is at `/var/run/xho/xhod.sock` (root daemon default, matching
the Docker / systemd convention). The deploy script mount keeps this directory
shared between host and container, so tools like `xho status` and `xho exec`
work from the host without additional client configuration.

Release asset pattern (for systemd/bare/local; target is auto-detected):
```
https://github.com/graydovee/cross-host-ops/releases/download/<tag>/cross-host-ops-<tag>-<target>.tar.gz
```

## Port Layout

xhod listens on two SSH ports with **separate key stores**, so a key granted for
human access can't be reused for machine-to-machine control-plane access (and
vice versa):

| Port | Server | Key file | SSH user | Purpose |
|------|--------|----------|----------|---------|
| **2222** | Transparent proxy (`ProxySshServer`) | `proxy_authorized_keys` | target node name | Human `ssh`/`scp`/`sftp`/`rsync` — no xho client needed |
| **12222** | Control plane (`RemoteSshServer`) | `authorized_keys` | `xho` (single) | daemon↔daemon `xho-rpc`/`xho-reverse` subsystems + `OpenSession` multi-hop |
| Unix socket | gRPC | (local) | — | local `xho` CLI ↔ daemon |

> **v0.4.0 migration:** the control plane moved `2222 → 12222` to free 2222 for
> the transparent proxy. Update existing `[[gateways]]` `address` values and
> `reverse_proxy.server_address` from `:2222` to `:12222`.

## Transparent SSH Proxy — `ssh node@xhod`

A second SSH server (default port **2222**) lets a human reach any target the
daemon can resolve using plain `ssh`/`scp`/`sftp`/`rsync` — no `xho` client and
no per-target client config. This is the simplest entry point for ad-hoc shell
access or file transfer.

**Targeting.** The SSH *username* is the target — a `server.toml` alias, an IP,
or `_self` (the xhod host itself). It is resolved exactly like an `xho exec`
target, so anything `xho ls` shows is reachable, including machines behind
another xhod (the proxy drives the same `TargetSession` as everything else, so
multi-hop `OpenSession` tunneling works transparently).

**Auth.** Public key only, against a **separate** `proxy_authorized_keys`
(default `~/.xho/proxy_authorized_keys`) — not the control-plane
`authorized_keys`. xhod then brokers the real target's credentials (key or
vault-resolved password) on your behalf, so a human needs just one key in
`proxy_authorized_keys` regardless of how many targets they reach.

```bash
# Interactive shell on a target alias
ssh web1@bastion.example.com -p 2222

# One-off exec
ssh web1@bastion.example.com -p 2222 -- uname -a

# File transfer (scp uses -P for the port, not -p)
scp -P 2222 file.txt web1@bastion.example.com:/tmp/
scp -P 2222 web1@bastion.example.com:/etc/hosts ./hosts
sftp -P 2222 web1@bastion.example.com
rsync -e "ssh -p 2222" ./dist/ web1@bastion.example.com:/var/www/html/

# Reach the xhod host itself
ssh _self@bastion.example.com -p 2222
```

**Enable it** (on by default):

```toml
[server.proxy]
enable = true
listen_addr = "0.0.0.0:2222"
host_key_path = "~/.xho/host_key"                  # may share the control-plane host key
authorized_keys_path = "~/.xho/proxy_authorized_keys"
# sftp_server_path = "/usr/lib/openssh/sftp-server" # auto-detected when unset
```

Then authorize a human key:
```bash
echo "ssh-ed25519 AAAA… user@laptop" >> ~/.xho/proxy_authorized_keys
```

**Gotchas.**
- `ssh`/`scp` treat the first `user@host:path` colon as `host:path`, so a
  multi-hop username containing a colon (`gateway:server`) must be passed
  separately: `ssh -p 2222 -o User=gateway:server bastion.example.com`.
- Serving sftp to a `_self` (localhost) target needs an `sftp-server` binary on
  the xhod host (Debian: `apt-get install openssh-sftp-server`); for *remote*
  targets, xhod uses the target's own sftp subsystem.

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
# Pass --token, or omit it and enter at the prompt. Note the control-plane port
# (12222), not the proxy port.
xho host add prod-xhod xho@bastion.example.com:12222 --token <TOKEN>

# Empty input at the prompt skips bootstrap (legacy behavior).
xho host add prod-xhod xho@bastion.example.com:12222
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

# Transparent SSH proxy (plain ssh/scp/sftp — no xho client needed)
ssh <node>@<xhod-host> -p 2222
scp -P 2222 file.txt <node>@<xhod-host>:/tmp/

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
- The transparent proxy (port 2222) is enabled by default; set
  `[server.remote].enable = true` (port 12222) when other xhod instances or
  `xho` clients connect to this daemon
- v0.4.0 moved the control plane 2222→12222; old `[[gateways]]` addresses or
  `reverse_proxy.server_address` ending in `:2222` must be updated to `:12222`
