# 架构文档（重构后）

## 概述

Remote Hop（rhop）是一个基于 Rust 的远程命令执行与文件复制工具，采用 CLI + Daemon 分离架构。CLI 负责用户交互，Daemon 负责目标解析、命令审查、连接管理和命令执行。

## 系统全景

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              用户终端                                         │
│  rhop exec host1 -- ls                                                       │
│  rhop cp local.txt host1:/tmp/                                               │
│  rhop ls                                                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ gRPC over Unix Socket
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         rhopd (本地 Daemon)                                  │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────────────┐ │
│  │ Resolver │  │ Reviewer │  │ Gateways │  │ Remote SSH Server          │ │
│  │ 目标解析  │  │ 命令审查  │  │ 网关管理  │  │ (接受远程 rhopd 连接)       │ │
│  └─────┬────┘  └────┬─────┘  └─────┬────┘  └────────────────────────────┘ │
│        │             │              │                                       │
│        │             │     ┌────────┼────────────┐                          │
│        │             │     ▼        ▼            ▼                          │
│        │             │  Local    Rhopd      Jumpserver                      │
│        │             │  Gateway  Gateway    Gateway                         │
│        │             │     │        │            │                          │
└────────┼─────────────┼─────┼────────┼────────────┼──────────────────────────┘
         │             │     │        │            │
         │             │     │SSH     │SSH sub     │SSH+PTY
         │             │     │        │system      │
         │             │     ▼        ▼            ▼
         │             │  End      远程         End
         │             │  Target   rhopd        Target
         │             │           Daemon       (via menu)
         │             │              │
         │             │              │SSH
         │             │              ▼
         │             │           End Target
         │             │
         ▼             ▼
    Vec<Route>     allow/deny/confirm
```

## 核心组件

### 1. CLI (`src/cli.rs`)

用户与 rhop 交互的入口。

**职责：**
- 解析命令行参数（clap）
- 连接本地 daemon（gRPC over Unix Socket）
- 发送 `StartRequest` 启动命令执行
- 交互模式管理：raw mode、SIGWINCH 转发、双向字节流
- 接收并显示远端输出（stdout/stderr）
- 处理认证提示（AuthPrompt）和命令确认（ConfirmRequired）

**通信方式：**
- 通过 `~/.rhop/rhopd.sock` Unix Socket 连接本地 daemon
- 使用 proto 定义的 gRPC 协议（双向流）

**支持的命令：**

| 命令 | 功能 |
|------|------|
| `exec <target> -- <cmd>` | 执行远程命令 |
| `cp <src> <dst>` | 文件复制 |
| `ls` | 列出所有可达服务器 |
| `daemon start/stop/status` | 管理本地 daemon |
| `host add/remove/list` | 管理已知主机 |

### 2. Daemon (`src/daemon.rs`)

核心执行引擎，同时监听本地和远程连接。

**职责：**
- 监听 Unix Socket（本地 CLI 连接）
- 监听 TCP:2222 SSH Server（远程 rhopd 连接）
- 目标解析（Resolver）
- 命令审查（Reviewer）
- 分发操作到 Gateway
- 多候选路由遍历（fallback）
- 空闲连接清理（reaper timer）

**双入口统一处理：**

```
Unix Socket (本地 CLI)  ─┐
                         ├─→ tonic gRPC Server (RhopRpcService)
SSH subsystem (远程)    ─┘        ↓
                         同一套 RPC handler 处理所有请求
```

本地连接和远程连接共用同一个 `RhopRpcService`，区别仅在入口传输层。

**执行流程：**

```
收到 exec 请求
  → Resolver.resolve(target) → Vec<Route>
  → Reviewer.review(command) → allow/deny/confirm
  → for route in routes:
      gateways[route.gateway_name].exec(route.end_target, request)
      → Ok: return exit code
      → Resolution error: try next route
      → Other error: return error
```

### 3. Resolver (`src/resolver.rs`)

将用户输入的 target 字符串解析为有序的路由候选列表。

**输入：** 用户提供的 target 字符串（如 `host1`、`ali-rhopd:host2`、`192.0.2.200`）

**输出：** `Vec<Route>` — 有序候选路由列表

```rust
pub struct Route {
    pub gateway_name: String,  // "local", "ali-rhopd", "corp-jump"
    pub end_target: String,    // "host1", "host2", "192.0.2.200"
}
```

**解析优先级：**

1. **显式指定** `<gateway_name>:<server_alias>` — 直接路由到指定 gateway
2. **Merged view 查找** — 在所有 Gateway 的 `list_servers` 结果中唯一匹配
3. **Fallback 列表** — 按 `ssh.fallback` 顺序生成候选

**Merged View：**
- 启动时/周期性调用所有 Gateway 的 `list_servers()`
- 缓存结果，TTL = `ssh.max_idle_time`
- 不支持 `list_servers` 的 Gateway 直接跳过（零成本）

### 4. Reviewer (`src/review.rs`)

可选的 LLM 命令安全审查。

**两层过滤：**

1. **本地快速白名单** — 模式匹配（如 `ls *`、`cat *`），零延迟通过
2. **LLM 语义审查** — 发送到 OpenAI 兼容接口，分类为 safe/risky/dangerous

**审查结果 → 策略映射：**

| 风险等级 | 策略 | 行为 |
|----------|------|------|
| safe | allow | 直接执行 |
| risky | confirm | 需要用户确认 |
| dangerous | deny | 拒绝执行 |

**配置：**
```toml
[review]
enable = true
endpoint = "https://api.deepseek.com/v1/chat/completions"
model = "deepseek-v4-flash"
timeout = "10s"
failure_action = "deny"  # 审查失败时的默认行为
```

**Shell Wrapping 不影响审查：** Reviewer 只看原始命令（`build_remote_command(argv)`），不看 shell 包装后的命令。

### 5. Gateway 层 (`src/gateway/`)

统一抽象所有跳板机/连接方式，内部管理连接池、认证、重连。

**Gateway trait — 调用方唯一接口：**

```rust
#[async_trait]
pub trait Gateway: Send + Sync {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError>;
    async fn copy(&self, target: &str, spec: &CopySpec) -> Result<(), GatewayError>;
    async fn exec_interactive(&self, target: &str, request: &InteractiveRequest) -> Result<InteractiveHandle, GatewayError>;
    async fn list_servers(&self) -> Result<Vec<ServerEntry>, GatewayError>;
    fn kind(&self) -> GatewayKind;
    fn name(&self) -> &str;
    async fn prune_idle(&self);
}
```

**三种实现：**

| Gateway | 连接方式 | 内部池策略 | list_servers |
|---------|----------|-----------|-------------|
| **LocalGateway** | 直连 SSH | `HashMap<host:port, Vec<Connection>>`，相同地址复用 | 读 server.toml，零 I/O |
| **RhopdGateway** | SSH subsystem → gRPC | 单个共享 `RhopdClient`，远程 daemon 管并发 | gRPC ListServers RPC（返回远程 daemon 聚合的所有 Gateway 列表） |
| **JumpserverGateway** | SSH + PTY Shell + Menu | 单个 `PtyShell`，串行访问 | `UnsupportedCapability`，零 I/O |

**关键特性：**
- **惰性连接** — Gateway 构造时不建连，首次操作时才连接
- **自动重连** — Transport error 时内部重试一次（新建连接），失败再向上传播
- **连接池内化** — 无外部 Pool 组件，每种 Gateway 自己的策略

### 6. Connection 层 (`src/connection/`)

纯操作接口，Gateway 的内部实现细节。

**Connection trait（不暴露给外部）：**

```rust
#[async_trait]
pub(crate) trait Connection: Send {
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32>;
    async fn copy(&mut self, spec: &CopySpec) -> Result<()>;
    async fn exec_interactive(&mut self, request: &InteractiveRequest) -> Result<InteractiveHandle>;
    fn is_alive(&self) -> bool;
}
```

**三种实现：**

| Connection | 传输方式 | 由谁创建 |
|-----------|----------|----------|
| **DirectConnection** | SSH channel (session) | LocalGateway |
| **RhopdConnection** | gRPC Execute/Copy stream | RhopdGateway |
| **JumpserverConnection** | PTY shell 命令交互 | JumpserverGateway |

### 7. 认证 (`src/gateway/auth.rs`)

认证在 Gateway 内部的连接建立阶段完成，对 exec/copy 调用方透明。

**AuthPrompter 回调：**

```rust
pub type AuthPrompter = dyn Fn(AuthPrompt) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync;

pub struct AuthPrompt {
    pub gateway_name: String,
    pub message: String,
    pub secret: bool,
}
```

**数据流：**

```
Gateway.ensure_connected()
  → 需要密码/MFA
  → (auth_prompter)(prompt)
  → Daemon 通过 gRPC 转发 AuthPrompt event 给 CLI
  → CLI 显示 prompt，读取用户输入
  → 用户输入通过 gRPC 返回 Daemon
  → Daemon 返回给 Gateway
  → Gateway 完成认证
```

**认证模式：**

| 场景 | 处理方式 |
|------|----------|
| 配置了 `identity_file` | SSH key auth（自动） |
| 配置了 `password` | password auth（自动） |
| 未配置密码 | 通过 AuthPrompter 向用户询问 |
| 配置了 `totp_secret_base32` | 自动生成 TOTP code |
| 未配置 TOTP secret | 通过 AuthPrompter 向用户询问 MFA code |

### 8. Remote SSH Server（`src/daemon.rs` 内）

rhopd 作为 SSH 服务端接受远程连接。

**监听：** `TCP:2222`（可配置）

**支持的操作：**
- `subsystem_request("rhop-rpc")` — 唯一接受的请求，将 SSH channel stream 作为 gRPC 连接处理
- `auth_publickey()` — 校验 `~/.rhop/authorized_keys`

**不支持的操作（全部拒绝）：**
- `shell_request` — 不允许 shell 登录
- `exec_request` — 不允许直接 exec
- `tcpip_forward` / `streamlocal_forward` — 不允许端口转发

**连接处理：**

```
远程 rhopd client (RhopdGateway)
  → SSH connect to TCP:2222
  → publickey auth (user=rhop)
  → request_subsystem("rhop-rpc")
  → channel stream → IncomingConn::Remote
  → tonic Server 处理 (同一个 RhopRpcService)
```

## 通信协议

### gRPC 协议 (`proto/rhop.proto`)

CLI ↔ Daemon 和 Daemon ↔ 远程 Daemon 共用同一套协议。

| RPC | 类型 | 功能 |
|-----|------|------|
| `Execute` | 双向流 | 命令执行（含交互模式） |
| `Copy` | 双向流 | 文件复制 |
| `Status` | Unary | 查询 daemon 状态 |
| `ListServers` | Unary | 获取服务器列表 |
| `Shutdown` | Unary | 关闭 daemon |
| `UpdateConfig` | Unary | 热更新配置 |
| `ListJumpHosts` | Unary | 列出跳板机 |

### Execute 流消息

**Client → Daemon：**
- `StartRequest` — 启动执行（target, argv, pty, interactive, shell, no_shell, terminal size）
- `ConfirmRequest` — 用户确认（allow/deny）
- `AuthInputRequest` — 认证输入（password/MFA）
- `StdinData` — stdin 字节流（交互模式）
- `WindowResize` — 终端尺寸变更

**Daemon → Client：**
- `Stdout` / `Stderr` — 输出流
- `ExitStatus` — 退出码
- `ReviewResult` — 审查结果
- `ConfirmRequired` — 需要确认
- `AuthPrompt` — 认证提示
- `Error` — 错误信息

## 错误分类与重试

```
┌─────────────────────────────────────────────────────┐
│                 错误分类决策树                         │
├─────────────────────────────────────────────────────┤
│                                                     │
│  tonic::Status?                                     │
│  ├─ NotFound → Resolution (fallback to next route)  │
│  ├─ Unavailable/Cancelled/Unknown → Transport       │
│  └─ Other → continue checks                        │
│                                                     │
│  russh::Error? → Transport                          │
│                                                     │
│  Message contains "not found"? → Resolution         │
│  Message contains "channel closed"? → Transport     │
│                                                     │
│  Default → Execution (return immediately)           │
└─────────────────────────────────────────────────────┘
```

| 错误类型 | 处理 |
|----------|------|
| **Resolution** | Daemon 尝试下一个路由候选 |
| **Transport** | Gateway 内部重连重试一次，失败后向上传播 |
| **Execution** | 直接返回给 CLI，不重试 |
| **Unsupported** | list_servers 时跳过该 Gateway |

## 交互模式

当 `--pty` + stdin 是 TTY + stdout 是 TTY 时自动激活：

```
┌─────────┐     StdinData      ┌────────┐    Connection.    ┌────────┐
│ Terminal │ ──────────────────▶│ Daemon │ ──exec_inter──▶   │ Remote │
│ (raw)   │                    │        │    active()        │  PTY   │
│         │ ◀──────────────────│        │ ◀────────────────  │        │
└─────────┘     Stdout          └────────┘                   └────────┘
     │                              │
     │ SIGWINCH                     │ WindowResize → resize_tx
     └──────────────────────────────┘
```

## Shell Wrapping

可选的 shell 包装功能，让远程命令在 interactive shell 中执行（加载 .bashrc、alias、LS_COLORS）。

**配置优先级（daemon 端解析）：**
1. CLI `--shell <name>` / `--no-shell`（最高）
2. server.toml per-server `shell = "zsh"`
3. server.toml `[defaults]` `shell = "bash"`（最低）

**包装格式：**
- bash/zsh: `<shell> -ic '<escaped_command>'`
- sh/fish/other: `<shell> -c '<escaped_command>'`

**命令名不 quote：** `build_shell_inner_command` 对第一个参数（命令名）不加引号，确保 alias 展开。

## 配置文件

### `~/.rhop/config.toml` — 主配置

```toml
[server]
log_path = "~/.rhop/rhopd.log"
log_level = "info"

[server.local]
enable = true
socket_path = "~/.rhop/rhopd.sock"

[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "rhop"
host_key_path = "~/.rhop/host_key"
authorized_keys_path = "~/.rhop/authorized_keys"

[ssh]
server_config_path = "~/.rhop/server.toml"
fallback = ["local", "corp-jump"]
pty = true
connect_timeout = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

[[jump_hosts]]
name = "ali-rhopd"
kind = "rhopd"
address = "rhop@203.0.113.10:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.rhop/known_hosts"

[[jump_hosts]]
name = "corp-jump"
kind = "jumpserver"
host = "bastion.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
[jump_hosts.mfa]
totp_secret_base32 = "..."

[review]
enable = true
endpoint = "https://api.deepseek.com/v1/chat/completions"
model = "deepseek-v4-flash"
```

### `~/.rhop/server.toml` — 服务器列表

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"
shell = "bash"

[servers.host1]
host = "203.0.113.10"
port = 22
user = "root"
password = "..."
# shell 省略 → 继承 defaults.shell = "bash"

[servers.host2]
host = "192.0.2.200"
user = "admin"
shell = "zsh"  # 覆盖 defaults
```

## 目录结构

```
src/
├── bin/
│   ├── rhop.rs              # CLI binary 入口
│   └── rhopd.rs             # Daemon binary 入口
├── cli/
│   └── mod.rs               # CLI 逻辑（参数解析、交互模式、raw mode）
├── daemon/
│   ├── mod.rs               # daemon 启动、监听、shutdown、DaemonState
│   ├── rpc.rs               # RhopRpcService (execute, copy, status, list_servers)
│   ├── ssh_server.rs        # RemoteSshServer, RemoteSshHandler, IncomingConn
│   ├── resolver.rs          # 目标解析 (target → Vec<Route>)
│   ├── review.rs            # LLM 命令审查
│   ├── gateway/
│   │   ├── mod.rs           # Gateway trait, GatewayKind, Route, GatewayError, build_gateways
│   │   ├── local.rs         # LocalGateway (直连 SSH + per-address 连接池)
│   │   ├── rhopd.rs         # RhopdGateway (SSH subsystem + gRPC client)
│   │   ├── jumpserver.rs    # JumpserverGateway (PTY shell + menu 导航)
│   │   └── auth.rs          # AuthPrompter, AuthPrompt, SSH 认证辅助, known_hosts
│   └── connection/
│       ├── mod.rs           # Connection trait (pub(super) — 仅 daemon 内部可见)
│       ├── direct.rs        # DirectConnection (SSH channel 操作)
│       ├── rhopd.rs         # RhopdConnection (gRPC stream 操作)
│       ├── jumpserver.rs    # JumpserverConnection (PTY shell 操作)
│       └── shared.rs        # shell_quote, build_command, wrap_in_shell, PtyShell
├── config.rs                # 配置解析（通用库）
├── protocol.rs              # gRPC 类型 / 内部协议类型（通用库）
├── exit_codes.rs            # 退出码处理（通用库）
├── output.rs                # 输出格式化（通用库）
├── logging.rs               # 日志配置（通用库）
└── lib.rs                   # 模块声明
proto/
└── rhop.proto               # gRPC 协议定义
```

**设计原则：** src 根只放通用库（config、protocol、logging 等与具体业务无关的基础设施）。所有业务逻辑收入 `cli/` 和 `daemon/` 子模块。Gateway 和 Connection 是 daemon 的内部实现细节，放在 `daemon/` 下。

## 数据流

### 批量执行 `rhop exec host1 -- ls`

```
CLI
  → gRPC StartRequest { target:"host1", argv:["ls"], shell:"", no_shell:false }
  → Daemon
      → Resolver.resolve("host1") → [Route { gateway:"local", end_target:"host1" }]
      → Daemon 解析 shell: 读 server.toml defaults.shell = "bash"
      → Reviewer.review("host1", ["ls"], "'ls'") → allow
      → gateways["local"].exec("host1", ExecRequest { argv:["ls"], shell:"bash", ... })
          → LocalGateway: resolve "host1" → 203.0.113.10:22
          → acquire DirectConnection (pool hit or new SSH)
          → DirectConnection.exec():
              channel.request_pty()
              channel.exec("bash -ic 'ls'")
              → stream stdout/stderr/exit_status
      → return exit_code
  → CLI 显示输出
```

### 列出服务器 `rhop ls`

```
CLI
  → gRPC ListServers
  → Daemon
      → for gw in gateways:
          gw.list_servers()
          ├─ LocalGateway: 读 server.toml → entries (零 I/O)
          ├─ RhopdGateway: ensure_client → gRPC ListServers RPC → entries
          └─ JumpserverGateway: Err(Unsupported) → skip
      → 合并所有 entries
  → CLI 格式化显示
```

### 多候选 fallback `rhop exec remote-asset-... -- ls`

```
CLI
  → gRPC StartRequest { target:"remote-asset-...", ... }
  → Daemon
      → Resolver.resolve("remote-asset-...") →
          [Route { gateway:"ali-rhopd", end_target:"remote-asset-..." },
           Route { gateway:"corp-jump", end_target:"remote-asset-..." }]
      → gateways["ali-rhopd"].exec("remote-asset-...", req)
          → Resolution error: "target not found on remote daemon"
          → continue
      → gateways["corp-jump"].exec("remote-asset-...", req)
          → Ok(exit_code)
  → CLI 显示输出
```
