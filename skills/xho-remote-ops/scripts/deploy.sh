#!/usr/bin/env bash
#
# Install or upgrade xhod on a host.
#
# Remote xhod — deployment method priority: docker (default) > systemd > bare.
# Running xhod "bare" (via `xho daemon start`) works but is not recommended for
# servers because it is not supervised; prefer docker or systemd.
#
# Remote examples:
#   deploy.sh root@bastion.example.com                      # docker, :latest
#   deploy.sh root@bastion.example.com --version v0.2.0     # docker, pinned tag
#   deploy.sh root@bastion.example.com --method systemd     # systemd unit
#
# Local client install (the `xho` binary on this machine):
#   deploy.sh --local
#   deploy.sh --local --build
#
# The release ships:
#   - Docker image:  ghcr.io/graydovee/cross-host-ops:<tag>  (amd64 + arm64)
#   - Tarball:       cross-host-ops-<tag>-<target>.tar.gz    (xho, xhod, unit)

set -euo pipefail

REPO="graydovee/cross-host-ops"
REGISTRY="ghcr.io"   # default registry; override with --registry (e.g. a ghcr mirror)

usage() {
  cat <<'EOF'
Usage:
  deploy.sh <user@host> [options]   Install/upgrade xhod on a REMOTE host.
  deploy.sh --local [options]       Install the xho client on THIS machine.

Remote xhod method (--method, default docker):
  docker    Pull ghcr.io/graydovee/cross-host-ops:<tag>, run as a restarted
            container with the config dir mounted at /etc/xho.
  systemd   Download the release, install binaries + the xhod.service unit,
            enable & start under systemd.
  bare      Download and run via `xho daemon start` (NOT recommended — unsupervised).

Options:
  --method <m>      docker | systemd | bare (remote only; default docker)
  --version <tag>   Release / image tag (default: latest, resolved via GitHub API)
  --registry <host> Image registry, default ghcr.io. Use a ghcr mirror when the
                    target cannot reach ghcr.io (e.g. ghcr.nju.edu.cn). The image
                    ref becomes <host>/graydovee/cross-host-ops:<tag>. docker only.
  --target <triple> Rust target for systemd/bare/local downloads (default: auto)
  --prefix <dir>    Binary dir for systemd/bare (default: /usr/local/bin)
  --config <path>   Daemon config path (default: /etc/xho/config.toml remote,
                    ~/.xho/config.toml local). For docker, its parent dir is
                    mounted into the container at /etc/xho.
  --name <name>     Container name for docker (default: xhod)
  --build           Compile from local source instead of downloading a release
                    (local / systemd / bare only; ignored for docker)
  --password <pw>   SSH password for the remote host (password login). Prefer
                    key auth; if used, sshpass must be installed locally.
                    Equivalent: SSHPASS=<pw> bash deploy.sh ...
  -h, --help        Show this help
EOF
  exit "${1:-0}"
}

# --- Argument parsing -------------------------------------------------------
MODE=""            # "local" | "remote"
REMOTE=""
METHOD="docker"
VERSION=""
REGISTRY=""        # override ghcr.io via --registry (mirror)
TARGET=""
PREFIX_OVERRIDE=""
CONFIG_OVERRIDE=""
CONTAINER_NAME="xhod"
BUILD=false
PASSWORD=""

if [[ $# -eq 0 ]]; then usage 1; fi

if [[ "$1" == "-h" || "$1" == "--help" ]]; then usage 0; fi

if [[ "$1" == "--local" ]]; then
  MODE="local"; shift
elif [[ "$1" != -* ]]; then
  MODE="remote"; REMOTE="$1"; shift
else
  echo "error: first argument must be a host (user@host) or --local" >&2
  usage 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --method)  METHOD="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --registry) REGISTRY="$2"; shift 2 ;;
    --target)  TARGET="$2"; shift 2 ;;
    --prefix)  PREFIX_OVERRIDE="$2"; shift 2 ;;
    --config)  CONFIG_OVERRIDE="$2"; shift 2 ;;
    --name)    CONTAINER_NAME="$2"; shift 2 ;;
    --build)   BUILD=true; shift ;;
    --password) PASSWORD="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "unknown option: $1" >&2; usage 1 ;;
  esac
done

case "$METHOD" in
  docker|systemd|bare) ;;
  *) echo "error: --method must be docker|systemd|bare" >&2; exit 1 ;;
esac

# --- Resolve release version (latest if not pinned) -------------------------
if [[ -z "$VERSION" ]]; then
  echo "==> Resolving latest release for ${REPO}"
  # Fetch the body first, then grep: piping curl directly into `grep -m1` breaks
  # under `set -o pipefail` because grep closes the pipe early and curl exits 23.
  release_json="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest")"
  VERSION="$(printf '%s\n' "$release_json" \
    | grep -m1 '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')"
  if [[ -z "$VERSION" ]]; then
    echo "error: could not resolve latest release tag" >&2; exit 1
  fi
fi
echo "==> Version: ${VERSION} (method: ${MODE}$([[ "$MODE" == remote ]] && echo "/${METHOD}"))"

# Resolve the image registry (default ghcr.io; --registry for a mirror).
REGISTRY="${REGISTRY:-ghcr.io}"
IMAGE="${REGISTRY}/${REPO}"
if [[ "$REGISTRY" != "ghcr.io" ]]; then
  echo "==> Using registry mirror: ${REGISTRY}"
fi

# --- SSH/SCP transport ------------------------------------------------------
# Key auth by default. For password login, pass --password (or set SSHPASS);
# this routes every ssh/scp through sshpass. accept-new avoids the interactive
# known_hosts prompt on first connect so the script runs unattended.
if [[ -n "$PASSWORD" ]]; then
  export SSHPASS="$PASSWORD"
fi
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)
if [[ -n "${SSHPASS:-}" ]]; then
  command -v sshpass >/dev/null 2>&1 || {
    echo "error: password auth needs sshpass (apt-get install sshpass / brew install sshpass)" >&2
    exit 1
  }
  echo "==> Using password auth via sshpass"
  SSH=(sshpass -e ssh "${SSH_OPTS[@]}")
  SCP=(sshpass -e scp "${SSH_OPTS[@]}")
else
  SSH=(ssh "${SSH_OPTS[@]}")
  SCP=(scp "${SSH_OPTS[@]}")
fi

# ===========================================================================
# Core: download release tarball, stop, install binaries + unit (on the target)
# Used by systemd and bare. Runs on the TARGET host via `bash -s`.
# Env: VERSION, PREFIX, CONFIG_PATH, REPO, TARGET
# ===========================================================================
read -r -d '' INSTALL_CORE <<'INSTALL_EOF' || true
set -euo pipefail
: "${VERSION:?}" "${PREFIX:?}" "${REPO:?}"
CONFIG_PATH="${CONFIG_PATH:-/etc/xho/config.toml}"

# Auto-detect target triple if not provided.
if [[ -z "${TARGET:-}" ]]; then
  arch="$(uname -m)"; os="$(uname -s)"
  case "$os-$arch" in
    Linux-x86_64|Linux-amd64)  TARGET=x86_64-unknown-linux-musl ;;
    Linux-aarch64|Linux-arm64) TARGET=aarch64-unknown-linux-musl ;;
    Darwin-x86_64)             TARGET=x86_64-apple-darwin ;;
    Darwin-arm64)              TARGET=aarch64-apple-darwin ;;
    *) echo "error: unsupported $os/$arch — pass --target" >&2; exit 1 ;;
  esac
fi

url="https://github.com/${REPO}/releases/download/${VERSION}/cross-host-ops-${VERSION}-${TARGET}.tar.gz"
echo "==> Downloading ${url}"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
curl -fsSL "$url" | tar -xz -C "$tmp"
src="$tmp/cross-host-ops-${VERSION}-${TARGET}"
[[ -x "$src/xho" ]] || { echo "error: $src/xho not found after extract" >&2; exit 1; }

echo "==> Stopping existing xhod (systemd / cli / process)…"
systemctl stop xhod 2>/dev/null || true
if [[ -x "$PREFIX/xho" ]]; then
  "$PREFIX/xho" daemon stop --config "$CONFIG_PATH" 2>/dev/null || true
fi
pkill -x xhod 2>/dev/null || true
sleep 1

echo "==> Installing xho/xhod to ${PREFIX}"
mkdir -p "$PREFIX"
install -m 0755 "$src/xho" "$src/xhod" "$PREFIX/"

# Install the systemd unit too (harmless for bare; needed for systemd).
if [[ -f "$src/xhod.service" ]]; then
  install -d -m 0755 /etc/systemd/system
  install -m 0644 "$src/xhod.service" /etc/systemd/system/xhod.service
fi
INSTALL_EOF

# ===========================================================================
# Core: run xhod as a docker container (on the target host)
# Env: IMAGE_TAG, CONFIG_PATH, CONTAINER_NAME, IMAGE, REPO
# ===========================================================================
read -r -d '' DOCKER_CORE <<'DOCKER_EOF' || true
set -euo pipefail
: "${IMAGE:?}" "${IMAGE_TAG:?}" "${CONTAINER_NAME:?}"
CONFIG_PATH="${CONFIG_PATH:-/etc/xho/config.toml}"
data_dir="$(dirname "$CONFIG_PATH")"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker not found on this host — install Docker first, or use --method systemd" >&2
  exit 1
fi

echo "==> Pulling ${IMAGE}:${IMAGE_TAG}"
docker pull "${IMAGE}:${IMAGE_TAG}"

echo "==> Removing any existing container '${CONTAINER_NAME}'"
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

echo "==> Mounting host config dir ${data_dir} -> /etc/xho, socket /var/run/xho, exposing :2222"
mkdir -p "$data_dir" /var/run/xho

# The container runs as root, so default_socket_path() resolves to
# /var/run/xho/xhod.sock.  By bind-mounting the directory the host CLI
# (also root) finds the control socket at the same path automatically.
docker run -d \
  --name "${CONTAINER_NAME}" \
  --restart unless-stopped \
  -p 2222:2222 \
  -v "${data_dir}:/etc/xho" \
  -v /var/run/xho:/var/run/xho \
  "${IMAGE}:${IMAGE_TAG}"

sleep 2
echo "==> Container status:"
docker ps --filter "name=^/${CONTAINER_NAME}\$" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
echo "==> Recent logs:"
docker logs --tail 20 "${CONTAINER_NAME}" 2>&1 || true
DOCKER_EOF

# --- Build-from-source helper (runs locally) --------------------------------
build_locally() {
  local project_root target
  project_root="$(git rev-parse --show-toplevel)"
  target="${TARGET:-x86_64-unknown-linux-musl}"
  echo "==> Building release binaries (target ${target})" >&2
  cargo build --release --target "$target" --bin xho --bin xhod \
    --manifest-path "$project_root/Cargo.toml" >&2
  echo "$project_root/target/$target/release"
}

# --- Local install ----------------------------------------------------------
if [[ "$MODE" == "local" ]]; then
  PREFIX="${PREFIX_OVERRIDE:-$HOME/.bin}"
  CONFIG_PATH="${CONFIG_OVERRIDE:-$HOME/.xho/config.toml}"
  if [[ "$BUILD" == true ]]; then
    bindir="$(build_locally)"
    echo "==> Installing built binaries to ${PREFIX}"
    mkdir -p "$PREFIX"; install -m 0755 "$bindir/xho" "$bindir/xhod" "$PREFIX/"
  else
    # Run INSTALL_CORE locally (it downloads + installs; systemd steps are inert locally).
    env VERSION="$VERSION" REPO="$REPO" TARGET="$TARGET" \
        PREFIX="$PREFIX" CONFIG_PATH="$CONFIG_PATH" \
        bash -c "$INSTALL_CORE"
    # A local daemon start is unnecessary — it auto-starts on first use.
  fi
  echo "==> Done: xho installed to ${PREFIX}"
  exit 0
fi

# --- Remote install ---------------------------------------------------------
if [[ "$METHOD" == "docker" ]]; then
  "${SSH[@]}" "$REMOTE" \
    "IMAGE='$IMAGE' IMAGE_TAG='$VERSION' CONTAINER_NAME='$CONTAINER_NAME' CONFIG_PATH='$CONFIG_OVERRIDE' bash -s" \
    <<<"$DOCKER_CORE"

elif [[ "$METHOD" == "systemd" ]]; then
  PREFIX="${PREFIX_OVERRIDE:-/usr/local/bin}"
  CONFIG_PATH="${CONFIG_OVERRIDE:-/etc/xho/config.toml}"
  if [[ "$BUILD" == true ]]; then
    bindir="$(build_locally)"
    project_root="$(git rev-parse --show-toplevel)"
    echo "==> Uploading built binaries to ${REMOTE}:${PREFIX}"
    "${SSH[@]}" "$REMOTE" "systemctl stop xhod 2>/dev/null || true; pkill -x xhod 2>/dev/null || true; mkdir -p '${PREFIX}'"
    "${SCP[@]}" "$bindir/xho" "$bindir/xhod" "${REMOTE}:${PREFIX}/"
    echo "==> Installing xhod.service unit"
    "${SCP[@]}" "$project_root/packaging/systemd/xhod.service" "${REMOTE}:/etc/systemd/system/xhod.service"
  else
    "${SSH[@]}" "$REMOTE" \
      "VERSION='$VERSION' PREFIX='$PREFIX' CONFIG_PATH='$CONFIG_PATH' REPO='$REPO' TARGET='$TARGET' bash -s" \
      <<<"$INSTALL_CORE"
  fi
  echo "==> Enabling & starting xhod.service"
  "${SSH[@]}" "$REMOTE" "systemctl daemon-reload; systemctl enable --now xhod; sleep 1; systemctl --no-pager --full status xhod || true"

elif [[ "$METHOD" == "bare" ]]; then
  PREFIX="${PREFIX_OVERRIDE:-/usr/local/bin}"
  CONFIG_PATH="${CONFIG_OVERRIDE:-/etc/xho/config.toml}"
  echo "==> WARNING: 'bare' runs xhod via \`xho daemon start\` with no supervisor." >&2
  echo "    For servers, prefer --method docker (default) or --method systemd." >&2
  if [[ "$BUILD" == true ]]; then
    bindir="$(build_locally)"
    echo "==> Uploading built binaries to ${REMOTE}:${PREFIX}"
    "${SSH[@]}" "$REMOTE" "systemctl stop xhod 2>/dev/null || true; pkill -x xhod 2>/dev/null || true; mkdir -p '${PREFIX}'"
    "${SCP[@]}" "$bindir/xho" "$bindir/xhod" "${REMOTE}:${PREFIX}/"
  else
    "${SSH[@]}" "$REMOTE" \
      "VERSION='$VERSION' PREFIX='$PREFIX' CONFIG_PATH='$CONFIG_PATH' REPO='$REPO' TARGET='$TARGET' bash -s" \
      <<<"$INSTALL_CORE"
  fi
  echo "==> Starting xhod (bare)"
  "${SSH[@]}" "$REMOTE" "'${PREFIX}/xho' daemon start --config '${CONFIG_PATH}' 2>/dev/null || true; sleep 1; '${PREFIX}/xho' status"
fi
