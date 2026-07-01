---
name: xho-remote-ops
description: Operate remote machines via xho — run commands, copy files, and connect through xhod's transparent SSH proxy; plus one-time install/deploy of xho & xhod. Use when: (1) running commands on remote hosts, (2) copying files to/from remote, (3) connecting directly via the transparent SSH proxy (ssh/scp/sftp/rsync, no xho client), (4) checking server status or listing available servers, (5) managing daemon or jump host configuration, (6) deploying xhod to a server (Docker/systemd) or installing/upgrading the xho client. Triggers on phrases like "run on remote", "execute on server", "ssh to", "ssh node@xhod", "scp to remote", "sftp", "rsync to", "transparent ssh proxy", "copy to remote", "xho exec", "xho cp", "install xho", "deploy xhod", "upgrade xho", "run xhod in docker", "set up jump host", "deploy to", "check status of", "list servers", or any task requiring remote machine access via xho.
---

# xho Remote Operations

xho reaches remote machines two ways, both hitting the same targets:

- **`xho` CLI** — `xho exec` / `cp` / `ls` for scripted, repeatable work.
- **Transparent SSH proxy** — `ssh node@xhod -p 2222` for ad-hoc shell/scp/sftp
  with *no* client install or per-target config.

xhod (the daemon) auto-starts on first command, so there's usually no setup
needed to just run things. One-time install/deploy and gateway bootstrap live in
[references/setup-and-deploy.md](references/setup-and-deploy.md); full config,
connection-pool tuning, and troubleshooting in
[references/config-and-usage.md](references/config-and-usage.md).

## Setup (one-time)

Installing xhod on a server and registering it as a gateway is done once. Reach
for [references/setup-and-deploy.md](references/setup-and-deploy.md) only when
actually installing (deploy-script options, Docker config layout, token-based
key bootstrap). The common path, for reference:

```bash
bash scripts/deploy.sh root@bastion.example.com     # deploy remote xhod (Docker)
bash scripts/deploy.sh --local                       # install the local xho client
xho host add prod-xhod xho@bastion.example.com:12222 --token <TOKEN>  # register gateway
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

# List servers / status
xho ls
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

## Configuration & Troubleshooting

Full config format, connection-pool tuning, command review, and troubleshooting:
[references/config-and-usage.md](references/config-and-usage.md).
