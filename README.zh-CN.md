[English](README.md) | **中文**

# Cross Host Ops

远程命令执行、文件复制与**透明 SSH 代理**，由本地 daemon（`xhod`）统一托管。所有后端——直连 SSH、远程 xhod 实例、企业 jumpserver 堡垒机、自注册的 NAT 节点——都实现同一套双层 `Gateway` / `TargetSession` 架构并显式声明自身能力，上层功能因此在所有后端上一致可用。

## 特性

### 操作能力

- **远程执行** — `xho exec` 既可跑一次性命令，也可开完整交互式 PTY 会话（vim、htop 体验与原生 SSH 一致）；内置 stdin 转发、时长超时、可选 shell 包装
- **文件复制** — `xho cp` 对齐 scp 语义：递归、权限/mode 保留、目录目的地、实时进度条，以及可选开启的**断点续传**（`--resume/-c`：单文件上传/下载中断后经完整性校验从断点继续，不再从头重来）
- **透明 SSH 代理** — 标准 `ssh`/`scp`/`sftp`/`rsync` 直接经 daemon 的 2222 端口连到目标；SSH *用户名*即目标节点名，无需 xho 客户端、无需逐目标配置
- **服务器清单** — `xho ls` 汇聚全部配置源；`xho status` 查看 daemon 与连接池健康状态

### 连接能力

- **Daemon 托管访问** — CLI 不直连目标；`xhod` 负责路由解析、凭据代管、连接持有
- **多网关统一接口** — 直连 SSH 连接池、远程 **xhod** daemon、企业 **Jumpserver** 堡垒机（TOTP/MFA 自动登录、导航后会话缓存减少菜单耗时、抑制目标 shell 历史）、向公网枢纽自注册的**反向代理**节点，外加 `_self` 本机 —— 统一在 `Gateway` trait 下，以 `EXEC | COPY | PROXY | LIST` capability 标志声明能力；部分后端对不支持的操作明确报错而非错误执行
- **多跳隧道** — 穿透其他 xhod 实例后面的机器：`ssh → 本机 xhod → 控制面 → 远程 xhod → 机器`，全部走同一层 `TargetSession` 通用驱动
- **连接池** — 按 target IP 复用已认证 SSH 连接（一次握手多个 channel），空闲回收、断线自动重连
- **统一目标解析** — server.toml 别名、显式 `gateway:target` 路由、主机名模式推导 IP、可配置的多源 fallback 链
- **反向代理拓扑** — 无公网的 xhod 主动外连公网 server xhod 并注册为动态网关；客户端经 `hub:node:target` 访问，keepalive 快速检出半开连接

### 安全与合规

- **AI 命令审查** — 执行前可选 LLM 审查，按操作类型分别开关（exec / copy）：本地 fast-allowlist 放行显然安全的命令、copy 支持 blocklist/allowlist 路径模式（`.ssh`、`.kube` 等）、风险策略将 `safe | risky | dangerous` 判定映射为 `allow | confirm | deny`；LLM 服务故障时按可配的 `failure_action` 处置
- **审计日志** — 每个机器操作（exec、copy、会话隧道、透明代理）都以 JSON-Lines 事件落盘，含调用方身份（对端地址、SSH 用户、密钥指纹）、操作细节与结果；默认开启（用户在 `~/.xho/audit.jsonl`，root 在 `/var/log/xho/audit.jsonl`）
- **加密秘密保险库** — 密码/TOTP 种子/API key 不落明文：配置中以 `vault:name`、`env:NAME`、`file:/path` 引用；保险库密钥由 SSH 私钥经 HKDF 派生，无需另存密钥文件；`xho secret encrypt` 可一步迁移存量明文配置
- **基于 token 的密钥引导** — 短时效（可选可复用）token 让客户端把公钥追加到远端 daemon 的 `authorized_keys`，全程不需要 shell 登录：目标机 `xho token gen`，客户端 `xho host add --token`
- **信任域隔离** — 面向人类的代理认证与机器间控制面认证使用不同端口上独立的 `authorized_keys`，一把钥匙无法跨域复用

### 运维体验

- **零配置可用** — 只要有 `~/.ssh/config` 就能工作；其余全是带合理默认值的可选 TOML 配置，支持 SIGHUP 热加载
- **脚本友好契约** — `--output=json` 输出稳定 NDJSON；退出码全场景一致（`124` 超时、`125` daemon 故障、`126` 认证/审查拒绝、`127` 目标不存在）
- **轻量部署** — 仅两个静态二进制（`xho`、`xhod`），附 systemd unit 与 Docker 镜像；推送 tag 自动产出多平台发布件

## 快速开始

```bash
cargo build --release          # 二进制: target/release/xho, target/release/xhod

# 执行远程命令（daemon 自动启动）
xho exec web1 -- hostname

# 交互式 PTY 会话
xho exec -it web1 -- bash

# 文件复制 —— 含大文件断点续传
xho cp app.tar.gz web1:/opt/
xho cp --resume big.tar.gz web1:/tmp/big.tar.gz   # 中断后重跑同一命令续传

# 列出可达服务器 / 查看 daemon 健康
xho ls && xho status

# 透明 SSH 代理 —— 任何标准 ssh/scp/sftp 客户端直接用
ssh web1@localhost -p 2222 -- uname -a
scp -P 2222 file.txt web1@localhost:/tmp/
rsync -e "ssh -p 2222" ./dist/ web1@localhost:/var/www/html/
```

## 架构概览

```
 xho CLI                                ssh/scp/sftp
   │ gRPC / Unix socket                  │ SSH / TCP 2222
   ▼                                     ▼
┌───────────────────── xhod (Daemon) ─────────────────────────┐
│                                                              │
│  Execute/Copy/OpenSession RPC handlers                      │
│  + Oversight 层 (AI 命令审查 + JSON-Lines 审计)               │
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
│  │               JumpserverSession (PTY + sftp/shell)  │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  控制面 SSH (端口 12222)                                     │
│  · xho-rpc 子系统 (daemon↔daemon gRPC)                       │
│  · xho-reverse 子系统 (反向代理注册)                          │
│  · OpenSession RPC (多跳会话隧道)                             │
└──────────────────────────────────────────────────────────────┘
```

- **双层架构**：`Gateway` trait 负责路由、连接池与各后端的命令构建；`TargetSession` trait 是单次操作的 channel 契约（exec、PTY、sftp、复制流）。调用方完全通用——某条路由支持什么由 capability 标志决定。
- **Oversight 前置**：所有请求经过统一的 oversight 门面——先分类处置（启用时走 LLM 审查）、落审计日志，然后才会触碰目标。
- **双端口**：透明代理 **2222**（人类用 `ssh`/`scp`/`sftp`）与控制面 **12222**（机器 RPC、反向代理注册、多跳隧道）。

详见[架构设计](docs/cn/architecture.md)。

## 用法

```bash
# 基本执行 / PTY 模式 / 显式网关路由
xho exec <target> -- <command>
xho exec --tty <target> -- ls --color
xho exec prod:web1 -- hostname

# 清单与健康检查
xho ls [--refresh]
xho status

# 透明 SSH 代理（用户名 = 目标名，_self 表示 daemon 本机）
ssh <node>@<xhod_host> -p 2222
sftp -P 2222 <node>@<xhod_host>

# Daemon 管理（首次使用本来就会自动启动）
xho daemon start|stop|restart

# 网关管理与密钥引导
xho token gen                        # 在 daemon 所在机器上执行
xho host add prod xho@bastion.example.com:12222 --token <TOKEN>
xho host login prod --token <TOKEN>  # 客户端换钥匙后重新登记

# 秘密保险库（纯本地文件操作，不经过网络）
xho secret set db-password
xho secret encrypt [--dry-run]       # 把存量明文凭据迁入保险库
xho secret list / rekey
```

完整命令与配置参考见[使用指南](docs/cn/usage.md)。

## 配置

默认零配置即可运行（root daemon 用 `/var/run/xho/xhod.sock` + `/etc/xho/`；普通用户用 `~/.xho/`）。需要更多能力时创建 `~/.xho/config.toml`：

```toml
[ssh]
fallback = ["local", "prod"]         # 各解析源的先后顺序

[server.remote]                      # 控制面（机器↔机器，端口 12222）
enable = true
listen_addr = "0.0.0.0:12222"
user = "xho"

[server.proxy]                       # 透明 SSH 代理（面向人，端口 2222）
enable = true
listen_addr = "0.0.0.0:2222"

[[gateways]]
name = "prod"
kind = "xhod"                        # 或 "jumpserver" / "direct"
address = "xho@bastion.example.com:12222"

[review.exec]                        # AI 审查，按操作类型独立开关
enable = true

[audit]                              # JSON-Lines 审计日志（默认开启）
enabled = true
```

所有凭据类取值都接受 `vault:` / `env:` / `file:` 引用，不必写明文。

> **端口迁移说明 (v0.4.0)**：控制面从 2222 移至 **12222**；透明代理现占用 2222。

`config.example.toml` 对每个小节都有注释说明——包括 `reverse_proxy`、审查策略与提示词、jumpserver 会话缓存、`[secret]` 等。

## 部署

### 二进制构建

```bash
cargo build --release   # target/release/xho, target/release/xhod
```

### 远程 xhod

服务器上建议用 Docker 或 systemd（有守护、升级干净）：

```bash
# systemd
sudo install -m 0644 packaging/systemd/xhod.service /etc/systemd/system/
sudo systemctl enable --now xhod

# Docker
docker build -t xhod:latest .
docker run --rm -p 2222:2222 -p 12222:12222 -v /etc/xho:/etc/xho xhod:latest
```

### GitHub Release

推送 `v*` tag 自动发布多平台 musl/macOS 二进制和 GHCR Docker 镜像（`ghcr.io/graydovee/cross-host-ops:<tag>`）。发布清单见 [.agents/skills/xho-release/SKILL.md](.agents/skills/xho-release/SKILL.md)。

## 开发

```bash
cargo build
cargo test
cargo fmt --all
cargo clippy --all-targets
```

## 文档

- [更新日志](CHANGELOG.zh-CN.md) — 版本发布记录（[English](CHANGELOG.md)）
- [架构设计](docs/cn/architecture.md) — 系统设计、Gateway/TargetSession 分层、透明代理、多跳隧道
- [使用指南](docs/cn/usage.md) — 安装、配置、命令参考、故障排查
- [config.example.toml](config.example.toml) — 全量注释配置参考
- [server.example.toml](server.example.toml) — server.toml 格式

## License

MIT
