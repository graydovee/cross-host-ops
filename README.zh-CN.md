[English](README.md) | **中文**

# Cross Host Ops

远程命令执行、文件复制和**透明 SSH 代理**工具。通过本地 daemon 以双层 `Gateway` / `TargetSession` 架构管理 SSH 会话 — 每种后端（直连 SSH、远程 xhod、jumpserver 堡垒机）实现同一套 trait，并通过 capability 标志声明自己支持的操作。

## 特性

- **透明 SSH 代理** — `ssh node@xhod -p 2222` 直接连到目标服务器，`scp`/`sftp`/`rsync` 全兼容，客户端无需任何配置
- **多跳隧道** — `ssh → 本机xhod → 控制面 → 远程xhod → 机器`，穿透其他 xhod 后面的服务器
- **交互式 PTY** — 运行 vim、htop 等全屏程序，体验与原生 SSH 一致
- **连接池** — 按目标 IP 复用 SSH 连接，避免重复握手
- **多种跳板** — 直连 SSH、企业 jumpserver（MFA）、远程 xhod daemon — 统一在 `Gateway` trait 下，通过 capability 标志声明功能；部分后端（如 jumpserver 不支持 `list_servers`）明确报错
- **统一目标解析** — server.toml 别名、显式路由、IP 推导、fallback 链
- **命令审查** — 可选 LLM 安全审查，本地白名单 + AI 语义分析
- **文件复制** — `xho cp` 对齐 scp 语义，支持递归和 mode 保留
- **零配置可用** — 只要有 `~/.ssh/config`，无需任何配置文件

## 快速开始

```bash
# 构建
cargo build --release

# 执行远程命令（daemon 自动启动）
xho exec web1 -- hostname

# 交互模式（自动检测）
xho exec --tty host1 -- vim README.md

# 文件复制
xho cp local.txt host1:/tmp/

# 列出所有可达服务器
xho ls

# 透明 SSH 代理（需在配置中启用 proxy）
ssh web1@localhost -p 2222 -- hostname
scp file.txt web1@localhost:/tmp/ -P 2222
```

## 架构概览 (v0.5.0)

```
 xho CLI                                ssh/scp/sftp
   │ gRPC / Unix socket                  │ SSH / TCP 2222
   ▼                                     ▼
┌───────────────────── xhod (Daemon) ─────────────────────────┐
│                                                              │
│  Execute/Copy/OpenSession RPC handlers                      │
│                  │                                           │
│                  ▼                                           │
│          gateway.open_session(target)                       │
│          gateway.open_exec_session(target, argv, …)         │
│                  │                                           │
│     ┌────────────┼────────────┬──────────────┬─────────┐    │
│     ▼            ▼            ▼              ▼         ▼    │
│  Direct     Localhost     Xhod          Reverse    Jumpserver│
│  Gateway    Gateway       Gateway       Proxy      Gateway  │
│  (pooled)                (tunneled)    (tunneled)  (partial)│
│     │            │            │              │         │    │
│     ▼            ▼            ▼              ▼         ▼    │
│  ┌──────────── TargetSession ─────────────────────────┐    │
│  │ DirectSshSession | LocalSession | TunneledSession   │    │
│  │  (连接池复用)       (PTY+pipe)    (OpenSession RPC)  │    │
│  │               JumpserverSession (PTY + raw/sftp)    │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  控制面 SSH (端口 12222)                                     │
│  · xho-rpc 子系统 (daemon↔daemon gRPC)                       │
│  · xho-reverse 子系统 (反向代理注册)                          │
│  · OpenSession RPC (多跳会话隧道)                             │
└──────────────────────────────────────────────────────────────┘
```

- **双层架构**：`Gateway` trait（路由、连接池、感知后端的命令构建）+ `TargetSession` trait（每次操作的 SSH channel 契约）。所有调度都在 trait 内部，调用方完全通用。
- **Capability 标志**：每个 `Gateway` 声明自己支持的操作（`EXEC | COPY | PROXY | LIST`）。调用方通过 capability 通用判断；部分后端（如 jumpserver 无 `LIST`）返回明确错误。
- **连接池复用**：`DirectGateway` 池化已认证的 `client::Handle` — 一次 SSH 握手，多个 session channel。可在 `xho status` POOLS 中查看。
- **双端口**：透明代理 **2222**（人类用 `ssh`/`scp`），控制面 **12222**（机器间 RPC + 反向代理 + OpenSession 隧道）
- **透明代理**：SSH 用户名 = 目标节点名；xhod 代理凭据
- **多跳**：`ssh node@xhod` → 本地代理 → 控制面 `OpenSession` → 远程 xhod → 机器

详见 [架构设计](docs/cn/architecture.md)。

## 用法

```bash
# 基本执行
xho exec <target> -- <command> [args...]

# PTY 模式（彩色输出、交互式程序）
xho exec --tty <target> -- ls --color

# 显式指定网关路由
xho exec prod:web1 -- hostname

# 透明 SSH 代理
ssh <node>@<xhod_host> -p 2222               # 交互 shell
ssh <node>@<xhod_host> -p 2222 -- <command>   # 执行命令
scp -P 2222 file.txt <node>@<xhod_host>:/tmp/ # 文件复制
sftp -P 2222 <node>@<xhod_host>                # sftp 会话

# Daemon 管理
xho status
xho daemon start --config ~/.xho/config.toml
xho daemon restart

# 网关管理
xho host add prod xho@bastion.example.com:12222
xho host list
```

详见 [使用指南](docs/cn/usage.md)。

## 配置

无需任何配置文件即可运行。需要自定义时，创建 `~/.xho/config.toml`：

```toml
[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "prod"]
pty = true

# 控制面：机器间 RPC + 反向代理（默认 12222）
[server.remote]
enable = true
listen_addr = "0.0.0.0:12222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

# 透明 SSH 代理：人类用 ssh/scp/sftp（默认 2222）
[server.proxy]
enable = true
listen_addr = "0.0.0.0:2222"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/proxy_authorized_keys"

[[gateways]]
name = "prod"
kind = "xhod"
address = "xho@bastion.example.com:12222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"
```

> **端口迁移说明 (v0.4.0)**：控制面从 2222 移到 **12222**，透明代理占用 2222。请把 `[[gateways]]` 的 `address` 和 `reverse_proxy.server_address` 中的 `:2222` 改为 `:12222`。

详见 [config.example.toml](config.example.toml)。

## 部署

### 本机使用

```bash
cargo build --release
# 二进制: target/release/xho, target/release/xhod
```

### 远程 xhod

```bash
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
docker run --rm -p 2222:2222 -p 12222:12222 -v /etc/xho:/etc/xho xhod:latest
```

### GitHub Release

推送 `v*` tag 自动发布多平台二进制和 Docker 镜像。

## 开发

```bash
# 构建
cargo build

# 测试
cargo test

# 格式化
cargo fmt --all
```

## 文档

- [架构设计](docs/cn/architecture.md) — 系统设计、TargetSession 抽象、透明代理、多跳隧道
- [使用指南](docs/cn/usage.md) — 安装、配置、命令参考、故障排查
- [config.example.toml](config.example.toml) — 完整配置参考
- [server.example.toml](server.example.toml) — server.toml 格式

## License

MIT
