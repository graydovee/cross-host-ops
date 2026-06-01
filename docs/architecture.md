# 架构文档

## 概述

Remote Hop（rhop）是一个基于 Rust 的远程命令执行与文件复制工具，采用 CLI + Daemon 分离架构。CLI 负责用户交互，Daemon 负责连接管理和命令执行。

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                          用户终端                                     │
│                                                                     │
│  rhop exec host1 -- vim README.md                                    │
│  rhop cp local.txt host1:/tmp/                                       │
│  rhop ls                                                            │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ gRPC over Unix Socket
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        rhopd (本地 Daemon)                           │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │ 目标解析  │  │ 连接池    │  │ 命令审查  │  │ 交互模式管理      │   │
│  │ Resolver │  │   Pool   │  │  Review  │  │ Interactive PTY  │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘   │
│                       │                                             │
│              ┌────────┼────────┐                                    │
│              ▼        ▼        ▼                                    │
│  ┌──────────────┐ ┌────────┐ ┌──────────────┐                     │
│  │DirectJumpHost│ │Jumpserv│ │RhopdJumpHost │                      │
│  │  (直连 SSH)  │ │(跳板机) │ │(远程 rhopd)  │                      │
│  └──────┬───────┘ └───┬────┘ └──────┬───────┘                     │
└─────────┼──────────────┼─────────────┼──────────────────────────────┘
          │              │             │
          │ SSH          │ SSH+PTY     │ SSH subsystem (rhop-rpc)
          │              │ (菜单导航)   │ → gRPC over byte stream
          ▼              ▼             ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────────────────────┐
│  End Target  │ │  End Target  │ │     远程 rhopd Daemon             │
│  (直连服务器) │ │ (jumpserver  │ │                                   │
│              │ │  后的服务器)  │ │  ┌──────────┐  ┌──────────────┐  │
└──────────────┘ └──────────────┘ │  │ 目标解析  │  │ DirectJumpHost│  │
                                  │  └──────────┘  └──────┬───────┘  │
                                  │                       │          │
                                  └───────────────────────┼──────────┘
                                                          │ SSH
                                                          ▼
                                                  ┌──────────────┐
                                                  │  End Target  │
                                                  └──────────────┘
```

## 核心组件

### 1. CLI (`rhop`)

CLI 是用户的入口，负责：

- 解析命令行参数（clap）
- 连接本地 daemon（gRPC over Unix socket）
- 交互模式下管理终端状态（raw mode、SIGWINCH）
- 流式显示远端输出
- 处理认证提示和确认请求

### 2. Daemon (`rhopd`)

Daemon 是核心执行引擎，负责：

- 监听本地 Unix socket（CLI 连接）
- 可选监听 TCP 端口（远程 rhopd 连接）
- 目标解析（server.toml + SSH config + fallback）
- 连接池管理（按目标 IP 复用 SSH 连接）
- Jump Host 路由（选择到达目标的路径）
- 命令审查（可选 LLM 安全审查）
- 双向流转发（交互模式）

### 3. 连接池 (`ConnectionPool`)

- 按目标 IP 分组管理 SSH 连接
- 空闲连接自动复用
- 连接数上限可配置（默认 10/IP）
- 空闲超时自动回收（默认 10 分钟）
- Transport error 自动重连

### 4. Jump Host Trait

统一抽象，三种实现完全互换：

| 类型 | 连接方式 | 适用场景 |
|------|----------|----------|
| `DirectJumpHost` | 直连 SSH | 本地可达的服务器 |
| `JumpserverJumpHost` | SSH + PTY 交互式菜单 | 企业跳板机（MFA） |
| `RhopdJumpHost` | SSH subsystem → gRPC | 远程部署的 rhopd |

### 5. 目标解析器 (`Resolver`)

解析规则（优先级从高到低）：

1. `<jump_name>:<server_alias>` — 显式指定路由
2. `<server_alias>` — 在所有 source 中查找（唯一命中）
3. `<host_or_ip>` — IP 推导 + fallback 列表

## 通信协议

### 本地通信

```
CLI ←→ Daemon: gRPC over Unix socket (~/.rhop/rhopd.sock)
```

### 远程 rhopd 通信

```
本地 Daemon ←→ 远程 rhopd: gRPC over SSH subsystem (rhop-rpc)
```

SSH subsystem 提供了一个可靠的字节流通道，gRPC 直接运行在上面，无需额外的 TLS 或端口暴露。

### RPC 定义

| RPC | 类型 | 用途 |
|-----|------|------|
| `Execute` | 双向流 | 命令执行（含交互模式） |
| `Copy` | 双向流 | 文件复制 |
| `Status` | Unary | 查询 daemon 状态 |
| `ListServers` | Unary | 获取服务器列表 |

### Execute 流消息

**Client → Daemon:**
- `StartRequest` — 开始执行（含 target、argv、pty、interactive、terminal size）
- `ConfirmRequest` — 确认执行
- `AuthInputRequest` — 认证输入（密码/MFA）
- `StdinData` — 原始 stdin 字节（交互模式）
- `WindowResize` — 终端尺寸变更（交互模式）

**Daemon → Client:**
- `Stdout` / `Stderr` — 输出流
- `ExitStatus` — 退出码
- `ReviewResult` — 审查结果
- `ConfirmRequired` — 需要确认
- `AuthPrompt` — 认证提示
- `Error` — 错误信息

## 交互模式

当 `--pty` + stdin 是 TTY + stdout 是 TTY 时自动激活：

```
┌─────────┐     StdinData      ┌────────┐    channel.data()   ┌────────┐
│ Terminal │ ──────────────────▶│ Daemon │ ──────────────────▶ │ Remote │
│ (raw)   │                    │        │                     │  PTY   │
│         │ ◀──────────────────│        │ ◀────────────────── │        │
└─────────┘     Stdout          └────────┘    channel stdout   └────────┘
     │                              │
     │ SIGWINCH                     │ window_change()
     └──── WindowResize ───────────▶│
```

关键设计：
- 客户端设置 raw mode（`cfmakeraw`），RAII guard 保证恢复
- 双向字节流无编码转换
- SIGWINCH 信号触发终端尺寸同步
- PTY 执行使用 `request_pty()` + `exec()`（非 sentinel）

## 命令审查

可选的 LLM 安全审查，两层过滤：

1. **本地快速白名单** — 简单命令模式匹配，零延迟
2. **LLM 语义审查** — 复杂命令发送到 OpenAI 兼容接口

审查结果映射到策略：
- `safe` → `allow`（直接执行）
- `risky` → `confirm`（需要用户确认）
- `dangerous` → `deny`（拒绝执行）

## 数据流

### 批量执行模式

```
CLI → StartRequest → Daemon → resolve target → review → pool.execute()
                                                              │
Daemon ← Stdout/Stderr/ExitStatus ← SSH channel ◀────────────┘
  │
CLI ← Stdout/Stderr/ExitStatus (stream)
```

### 交互执行模式

```
CLI → StartRequest(interactive=true) → Daemon → pool.execute_interactive()
                                                        │
CLI ←→ StdinData/WindowResize/Stdout/ExitStatus ←→ Daemon ←→ SSH PTY channel
```

## 目录结构

```
src/
├── bin/           # 二进制入口 (rhop, rhopd)
├── cli.rs         # CLI 解析、交互模式、raw mode
├── daemon.rs      # Daemon 主循环、RPC handler
├── pool.rs        # 连接池
├── protocol.rs    # 内部协议类型
├── config.rs      # 配置解析
├── connection/    # SSH 连接层
│   ├── direct.rs  # DirectSshConnection
│   ├── jump.rs    # JumpSshConnection
│   ├── shared.rs  # 共享工具（PtyShell、shell_quote）
│   ├── resolver.rs# 目标解析
│   └── types.rs   # 连接类型定义
├── jump/          # Jump Host 抽象
│   ├── mod.rs     # JumpHost trait + InteractiveHandle
│   ├── direct.rs  # DirectJumpHost
│   ├── jumpserver.rs # JumpserverJumpHost
│   ├── rhopd.rs   # RhopdJumpHost
│   ├── factory.rs # Jump Host 工厂
│   ├── auth.rs    # 认证提示路由
│   └── pty.rs     # PTY 决策逻辑
├── remote.rs      # 远程连接工具
├── review.rs      # 命令审查
├── output.rs      # 输出格式化
└── logging.rs     # 日志配置
proto/
└── rhop.proto     # gRPC 协议定义
```
