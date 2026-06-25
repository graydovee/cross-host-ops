# Cross Host Ops 架构文档

## 概述

Cross Host Ops（xho）是一个基于 Rust 的远程命令执行与文件复制工具，采用 **CLI + Daemon 分离**架构：

- **`xho`**（`src/bin/xho.rs`）— 客户端，负责用户交互、参数解析、终端 raw mode、流式输出展示。
- **`xhod`**（`src/bin/xhod.rs`）— 守护进程，负责目标解析、命令审查、连接池管理、命令执行与文件传输。

CLI 不直接连接目标机器，而是通过 gRPC 把请求交给本地 daemon，由 daemon 统一调度。daemon 有**两个入口**，共用同一套 RPC 处理逻辑：

- **本地入口**：Unix Socket（`~/.xho/xhod.sock`），供本机 `xho` 连接。
- **远程入口**：SSH Server（默认 TCP:2222），供另一台机器上的 `xhod` 作为跳板接入。

daemon 通过 **Gateway** 抽象统一三种到达目标的方式：直连 SSH、远程 xhod（SSH subsystem + gRPC）、jumpserver（交互式菜单跳板）。

## 系统全景

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              用户终端                                         │
│  xho exec host1 -- ls                                                       │
│  xho cp local.txt host1:/tmp/                                               │
│  xho ls                                                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ gRPC over Unix Socket
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         xhod (本地 Daemon)                                  │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────────────┐ │
│  │ Resolver │  │ Reviewer │  │ Gateways │  │ Remote SSH Server          │ │
│  │ 目标解析  │  │ 命令审查  │  │ 网关管理  │  │ (接受远程 xhod 连接)       │ │
│  └─────┬────┘  └────┬─────┘  └─────┬────┘  └────────────────────────────┘ │
│        │             │              │                                       │
│        │             │     ┌────────┼────────────┐                          │
│        │             │     ▼        ▼            ▼                          │
│        │             │  Local    Xhod      Jumpserver                      │
│        │             │  Gateway  Gateway    Gateway                         │
│        │             │     │        │            │                          │
└────────┼─────────────┼─────┼────────┼────────────┼──────────────────────────┘
         │             │     │        │            │
         │             │     │SSH     │SSH sub     │SSH+PTY
         │             │     │        │system      │
         │             │     ▼        ▼            ▼
         │             │  End      远程         End
         │             │  Target   xhod        Target
         │             │           Daemon       (via menu)
         │             │              │
         │             │              │SSH
         │             │              ▼
         │             │           End Target
         │             │
         ▼             ▼
    Vec<Route>     allow/warn/confirm/deny
```

## 核心组件

### 1. CLI（`src/cli/`）

用户与 xho 交互的入口。

- `mod.rs` — 主分发（`exec` / `cp` / `status` / `ls` / `host` / `daemon` 子命令）、TTY/stdin 意图解析、超时校验、`--` 分隔符处理。
- `args.rs` — clap 参数定义（`ArunCli` / `ArunCommand` / `DaemonCommand` / `HostCommand`）。
- `exec.rs` / `copy.rs` / `host.rs` — 各操作的实现：建立 RPC 客户端、流式收发、交互模式（raw mode、SIGWINCH 转发）、复制进度、主机信任（trust-on-first-use）。
- `client.rs` / `output.rs` / `progress.rs` / `prompt.rs` — RPC 客户端封装、输出格式化、进度条、交互提示。

**通信方式**：通过 `~/.xho/xhod.sock` Unix Socket 连接本地 daemon，使用 proto 定义的 gRPC 双向流协议。CLI 处理认证提示（`AuthPrompt`）与命令确认（`ConfirmRequired`）的交互。

### 2. Daemon（`src/daemon/mod.rs`）

核心执行引擎，同时监听本地与远程连接，二者共用同一个 `XhoRpcService`，区别仅在入口传输层。

**`DaemonState`** 持有：
- `config: Arc<RwLock<AppConfig>>` — 可热更新的配置
- `gateways: Vec<(String, Arc<dyn Gateway>)>` — 按声明顺序排列的网关列表（首个恒为 `"local"`）
- `reviewer` — 命令审查器
- `shutdown_tx` — 关闭信号

**启动流程**：
1. 加载配置 → `gateway::build_gateways()` 构造所有 Gateway（构造时不建连）
2. （可选）绑定本地 Unix Socket 监听器
3. （可选）启动远程 SSH Server 监听器
4. 启动空闲连接回收任务（reaper，周期调用各 gateway 的 `prune_idle`）
5. 注册 SIGHUP 处理（配置热重载 + 日志重开）
6. 用 `XhoRpcService` 提供 gRPC 服务

**`XhoRpcService`** 实现 proto 定义的全部 RPC（Execute / Copy / Status / ListServers / Shutdown / UpdateConfig / ListGateways）。

### 3. Resolver（`src/daemon/resolver.rs`）

将用户输入的 target 字符串解析为有序的路由候选列表 `Vec<Route>`。

**`Route`**：
```rust
pub struct Route {
    pub gateway_name: String,  // "local", "remote-xhod", "corp-jump" ...
    pub end_target: String,    // 最终目标别名或 IP
}
```

**解析优先级**：
1. **显式限定** `<gateway_name>:<server_alias>` — 在首个冒号处分割，直接路由到指定 gateway。例：`remote-xhod:sub-gw:server1` → gateway=`remote-xhod`，end_target=`sub-gw:server1`。拒绝类似端口（`host:22`）和 IPv6 的输入。
2. **Merged view 查找** — 在所有 Gateway 的 `list_servers` 聚合结果中查找裸别名，要求唯一匹配。
3. **Fallback 列表** — 按 `ssh.fallback` 配置顺序生成候选。

**`derive_target_ip`**：从主机名后缀推导 IP，例如 `foo-192-0-2-163` → `192.0.2.163`（取末 4 段数字以 `.` 连接）。

### 4. Reviewer（`src/daemon/review.rs`）

可选的 LLM 命令安全审查，在命令执行前拦截。

**两层过滤**：
1. **本地快速白名单** — glob 模式匹配（如 `ls *`、`cat *`），复杂脚本（bash/python 等）直接走 LLM。
2. **LLM 语义审查** — 发送到 OpenAI 兼容接口，分类风险等级。

**风险等级与动作**（定义于 `src/config/review.rs`）：

| `RiskLevel` | 说明 | 默认 `ReviewAction` |
|-------------|------|---------------------|
| `Safe` | 安全 | `Allow` |
| `Risky` | 有风险 | `Confirm` |
| `Dangerous` | 危险 | `Deny` |

`ReviewAction` 共四种：`Allow` / `Warn` / `Confirm` / `Deny`，由 `[review.policy]` 配置每种风险等级对应的动作。

Reviewer 只审查原始命令（`build_remote_command(argv)`），不看 shell 包装后的命令。

## Gateway 层（`src/daemon/gateway/`）

统一抽象所有到达目标的方式。每个 Gateway 内部管理自己的连接、认证、重连。

### Gateway trait（`mod.rs`）

调用方（daemon）的唯一接口：

```rust
#[async_trait]
pub trait Gateway: Send + Sync {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32, GatewayError>;
    async fn exec_interactive(&self, target: &str, request: &InteractiveRequest)
        -> Result<InteractiveHandle, GatewayError>;
    async fn list_servers(&self) -> Result<Vec<ServerListRow>, GatewayError>;
    /// 控制面 gRPC 客户端（仅 Xhod/ReverseProxy），用于 OpenSession 隧道
    async fn rpc_client(&self) -> Option<XhoRpcClient<Channel>> { None }
    fn kind(&self) -> GatewayKind;
    fn name(&self) -> &str;
    async fn prune_idle(&self);
}
```

> **v0.4.0**：`Gateway::copy` 已移除 — 所有 copy 操作统一走 `TargetSession` + SFTP-over-session（`session::sftp_copy`）。`rpc_client` 访问器用于多跳隧道。

**`GatewayKind`** 枚举：`Direct` / `Jumpserver` / `Xhod` / `ReverseProxy` / `Localhost`。

**`GatewayError`** 携带 `ErrorKind` 分类，驱动错误处理：
- `Resolution` — 目标未找到（尝试下一个路由候选）
- `Transport` — 网络故障（Gateway 内部重连一次）
- `Execution` — 命令执行失败（直接返回）
- `Unsupported` — 操作不支持（`list_servers` 时跳过）

### `build_gateways`（`mod.rs`）

工厂函数，按以下规则构造 Gateway 列表：
1. **恒定首个** `"local"` → `LocalGateway`（读 `server.toml`，直连 SSH）。
2. 每个 `[[gateways]]` 条目按声明顺序创建一个 Gateway：
   - `kind = "xhod"` → `XhodGateway`
   - `kind = "jumpserver"` → `JumpserverGateway`
   - `kind = "direct"` → **`LocalGateway`**（用条目自己的 name，共享 `server.toml` 解析逻辑，仅用于路由区分）

> 注意：不存在独立的 `DirectGateway` 类型。`direct` 配置复用 `LocalGateway` 实现，仅以不同 name 参与 Resolver 路由。

### 三种 Gateway 实现

| Gateway | 连接方式 | 连接池策略 | `list_servers` |
|---------|----------|-----------|----------------|
| **LocalGateway**（`local.rs`） | 直连 SSH | `ManagedPool<DirectPoolKey, DirectConnection>`，按 host/port/user/auth 复用 | 读 `server.toml`，零 I/O |
| **XhodGateway**（`xhod.rs`） | SSH subsystem → gRPC | `ManagedSingleton<XhoRpcClient>`，单个共享客户端 | gRPC `ListServers`（返回远程 daemon 聚合的所有 Gateway） |
| **JumpserverGateway**（`jumpserver.rs`） | SSH + PTY shell + 菜单 | `ManagedSingleton<JumpserverTransport>`（一条共享 SSH 连接）+ `ManagedPool<target, JumpserverTargetShell>`（每目标缓存的 PTY shell） | 不支持（`Unsupported`），零 I/O |

## 会话层 Session Layer（`src/daemon/session/`）— v0.4.0

**统一 `TargetSession` 抽象**是所有操作的唯一低层契约 — CLI `xho exec`/`cp`、透明 SSH 代理、多跳 `OpenSession` 隧道都通过它驱动。

```rust
#[async_trait]
pub trait TargetSession: Send {
    async fn request_pty(&mut self, term: &str, cols: u32, rows: u32, modes: &[(Pty, u32)]) -> Result<()>;
    async fn set_env(&mut self, key: &str, value: &str) -> Result<()>;
    async fn exec(&mut self, command: &str) -> Result<()>;
    async fn shell(&mut self) -> Result<()>;
    async fn subsystem(&mut self, name: &str) -> Result<()>;      // "sftp"
    async fn window_change(&mut self, cols: u32, rows: u32) -> Result<()>;
    async fn signal(&mut self, signal: &str) -> Result<()>;
    async fn write_stdin(&mut self, data: &[u8]) -> Result<()>;
    async fn eof(&mut self) -> Result<()>;
    async fn next_event(&mut self) -> Option<SessionEvent>;        // Stdout / Stderr / ExitStatus / Eof
}
```

四个实现（按传输方式，不按功能）：

| 实现 | 传输方式 | 说明 |
|---|---|---|
| `DirectSshSession`（`direct.rs`） | russh 原始客户端通道 | byte-perfect scp/sftp/exec/pty。退出码通过 `Handler::exit_status` 回调获取（russh 会从 `channel.wait()` 丢弃 ExitStatus）。 |
| `LocalSession`（`local.rs`） | 本地 PTY + spawn `sftp-server` | `_self` 目标的完整 shell/exec/sftp。 |
| `TunneledSession`（`tunnel.rs`） | 控制面 OpenSession RPC | 多跳：`ssh → 本地代理 → 控制面 → 远程 xhod → 机器`。递归。 |
| `JumpserverSession`（`jumpserver.rs`） | 封装 `JumpserverGateway` 菜单引擎 | 无 sentinel exec（prompt 检测，退出码=0）；交互 shell 走 `exec_interactive`。 |

**工厂**：`open_target_session(state, route)` → 按 `gateway.kind()` 分发。  
**Copy**：`copy_via_session(state, route, spec)` → `subsystem("sftp")` + `russh-sftp` 客户端走 duplex 桥接（`sftp_copy.rs`）。

## 透明 SSH 代理（`src/daemon/proxy_server.rs`）— v0.4.0

第二个 russh 服务（`ProxySshServer`），监听端口 **2222**。面向人类：`ssh node@xhod -p 2222`。

- **认证**：publickey，使用独立的 `proxy_authorized_keys`（与控制面的 `authorized_keys` 分开）。SSH 用户名 = 目标节点名。
- **机制**：`ProxySshHandler` 将入站 SSH 请求（pty/exec/shell/subsystem/data/resize/signal）桥接到 `open_target_session` 获取的 `TargetSession`。会话事件通过入站 `Channel` 的 `data()`/`exit_status()`/`eof()`/`close()` 写回。
- **全兼容**：scp（sftp 模式 + legacy `-O`）、sftp 子系统、exec、交互 PTY、窗口 resize — 全透明，因为直连目标的载荷从不被解释（原始桥接）。

## OpenSession 多跳隧道 — v0.4.0

`XhoRpc` 新增的双向流式 RPC：

```proto
rpc OpenSession(stream SessionRequest) returns (stream SessionResponse);
```

使透明 `ssh`/`scp` 能到达**其他 xhod 后面**的机器：`ssh node@xhod` → 本地代理 → 控制面 `OpenSession` → 远程 xhod → `open_target_session`（递归）。

- **传输**：`TunneledSession` 使用现有的控制面 gRPC 客户端（XhodGateway/ReverseProxyGateway 的 `rpc_client()`）。
- **服务端 handler**（`daemon/mod.rs`）：解析目标，打开 `TargetSession`，桥接 RPC 流 ↔ 会话事件。
- **递归**：每个 xhod 都可以服务 `OpenSession`，任意深度跳转统一处理。

## 端口布局（v0.4.0）

| 端口 | 服务 | 认证 | 用途 |
|------|------|------|------|
| **2222** | `ProxySshServer` | `proxy_authorized_keys`（人类 pubkey，username=目标） | 透明 `ssh`/`scp`/`sftp` |
| **12222** | `RemoteSshServer`（控制面） | `authorized_keys`（机器 pubkey，user=xho） | `xho-rpc` + `xho-reverse` 子系统 + `OpenSession` RPC |
| Unix socket | gRPC | （本地） | CLI ↔ daemon |

## 认证（`auth.rs`）

认证在 Gateway 内部连接建立阶段完成，对 exec/copy 调用方透明。

**`AuthPrompter`** 回调签名：
```rust
pub type AuthPrompter = dyn Fn(AuthPrompt) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync;

pub struct AuthPrompt {
    pub prompt_id: String,
    pub target_label: String,
    pub kind: AuthPromptKind,
    pub secret: bool,
    pub message: String,
}
```

**认证模式**：

| 场景 | 处理方式 |
|------|----------|
| 配置了 `identity_file` | SSH key 认证（自动） |
| 配置了 `password` | 密码认证（自动） |
| 未配置密码 | 通过 `AuthPrompter` 向用户询问 |
| 配置了 `totp_secret_base32` | 自动生成 TOTP code（jumpserver MFA） |
| 未配置 TOTP secret | 通过 `AuthPrompter` 向用户询问 MFA code |

**认证数据流**：Gateway 需要输入 → `(auth_prompter)(prompt)` → daemon 经 gRPC 把 `AuthPrompt` 转发给 CLI → CLI 显示提示并读取输入 → 经 gRPC 回传 daemon → 交给 Gateway 完成认证。

`auth.rs` 还提供共享辅助：`parse_remote_target()`（解析 `[user@]host[:port]`）、known_hosts 校验、远程主机 key 获取（trust-on-first-use）。

## Connection 层（`src/daemon/connection/`）

Gateway 的内部实现细节，对 daemon 外部不可见（`pub(super)`）。

### Connection trait（`mod.rs`）

```rust
#[async_trait]
pub(super) trait Connection: Send {
    async fn exec(&mut self, request: &mut ExecRequest) -> Result<i32>;
    async fn copy(&mut self, spec: CopySpec) -> Result<()>;
    async fn exec_interactive(&mut self, request: &InteractiveRequest)
        -> Result<InteractiveHandle>;
    fn is_alive(&self) -> bool;
}
```

### 三种实现

| Connection | 传输方式 | 由谁创建 |
|------------|----------|----------|
| **DirectConnection**（`direct.rs`） | SSH channel（session） | LocalGateway |
| **XhodConnection**（`xhod.rs`） | gRPC Execute/Copy stream | XhodGateway |
| **JumpserverConnection**（`jumpserver.rs`） | PTY shell 命令交互（带 sentinel 提取退出码） | JumpserverGateway |

### `shared.rs`

连接层共用工具：
- `shell_quote()` — 单引号包裹与 `'\''` 转义
- `build_remote_command()` / `build_final_command()` — argv 拼接 + 按 shell 配置包装
- `wrap_in_shell()` — 包装成 `<shell> -ic '...'`（bash/zsh）或 `<shell> -c '...'`（sh/fish 等）
- `PtyShell` — PTY 管理、提示符检测、sentinel 退出码解析

## 连接管理（`src/daemon/connection_manager.rs`）

集中式的连接池/单例管理，被各 Gateway 复用：

- **`ManagedPool<K, T>`** — 按 key 复用连接，带容量信号量、空闲清理、传输错误自动重试。LocalGateway（按 `DirectPoolKey`）和 JumpserverGateway（按目标 shell）使用。
- **`ManagedSingleton<T>`** — 单个共享连接，带 generation 失效机制与最大寿命清理。XhodGateway（共享 gRPC 客户端）和 JumpserverGateway（共享 SSH transport）使用。
- **`RetryDecision`** — 连接建立分阶段（`Connect` / `Prepare` / `Started`）；前两阶段失败可重试，`Started` 后失败不重试。

## Remote SSH Server（`src/daemon/ssh_server.rs`）

xhod 可作为 SSH 服务端接受远程 xhod 的连接。

- **监听**：`TCP:2222`（`server.remote.listen_addr` 可配置）。
- **认证**：两条路径都接受 ——
  - `auth_publickey()` 校验 `~/.xho/authorized_keys`（常规路径）
  - `auth_password()` 校验动态 token（`xho token gen` 签发，in-memory）或配置里的 `bootstrap_token`（走 SecretResolver，支持 `vault:`/`env:`/`file:`）。token 校验通过后客户端可在同一 SSH 会话上调用 `BootstrapAuthorize` RPC，让 daemon 自动把它的公钥追加进 `authorized_keys`，免手动分发。
- **唯一接受的操作**：`subsystem_request("xho-rpc")` — 把 SSH channel 的字节流当作 gRPC 连接交给 tonic Server（同一个 `XhoRpcService`）。
- **拒绝的操作**：`shell_request`、`exec_request`、`tcpip_forward` / `streamlocal_forward`（不允许 shell 登录、直接 exec、端口转发）。

连接经 `IncomingConn::Remote`（携带对端 addr / user / key 指纹）进入 RPC 处理。

## 通信协议（`proto/xho.proto`）

CLI ↔ daemon、daemon ↔ 远程 daemon 共用同一套协议。

| RPC | 类型 | 功能 |
|-----|------|------|
| `Execute` | 双向流 | 命令执行（含交互模式） |
| `Copy` | 双向流 | 文件复制 |
| `Status` | Unary | 查询 daemon 状态 |
| `ListServers` | Unary | 获取服务器列表（含 merged view） |
| `Shutdown` | Unary | 关闭 daemon |
| `UpdateConfig` | Unary | 热更新配置 |
| `ListGateways` | Unary | 列出已配置的 Gateway |

**Execute 流消息**：
- Client → Daemon：`StartRequest`、`ConfirmRequest`、`AuthInputRequest`、`StdinData`、`WindowResize`
- Daemon → Client：`Stdout` / `Stderr`、`ExitStatus`、`ReviewResult`、`ConfirmRequired`、`AuthPrompt`、`Error`

## 错误分类与重试

```
tonic::Status?
├─ NotFound                       → Resolution（换下一个路由候选）
├─ Unavailable/Cancelled/Unknown  → Transport（Gateway 内部重连一次）
└─ Other                          → 继续判断
russh::Error                      → Transport
消息含 "not found"                → Resolution
消息含 "channel closed"           → Transport
默认                              → Execution（直接返回）
```

| 错误类型 | 处理 |
|----------|------|
| **Resolution** | daemon 尝试下一个路由候选 |
| **Transport** | Gateway 内部重连重试一次，失败后向上传播 |
| **Execution** | 直接返回给 CLI，不重试 |
| **Unsupported** | `list_servers` 时跳过该 Gateway |

## 交互模式

当 `--tty` + stdin 是 TTY + stdout 是 TTY 时自动激活：

```
┌─────────┐     StdinData      ┌────────┐    exec_interactive    ┌────────┐
│ Terminal │ ──────────────────▶│ Daemon │ ─────────────────────▶│ Remote │
│ (raw)   │                    │        │                        │  PTY   │
│         │ ◀──────────────────│        │ ◀───────────────────── │        │
└─────────┘     Stdout          └────────┘                        └────────┘
     │                              │
     │ SIGWINCH                     │ WindowResize
     └──────────────────────────────┘
```

CLI 将终端置为 raw mode，逐字节转发 stdin，同步窗口大小变化（SIGWINCH → `WindowResize`），退出时恢复终端。

## Shell Wrapping

可选的 shell 包装，让远程命令在交互 shell 中执行（加载 `.bashrc`、alias、`LS_COLORS`）。

**配置优先级**（daemon 端解析，`connection/shared.rs`）：
1. CLI `--shell <name>` / `--no-shell`（最高）
2. `server.toml` 每服务器 `shell = "zsh"`
3. `server.toml` `[defaults]` `shell = "bash"`（最低）

**包装格式**：
- bash/zsh：`<shell> -ic '<escaped_command>'`
- sh/fish/其他：`<shell> -c '<escaped_command>'`

命令名不加引号（`build_interactive_shell_command`），确保 alias 展开。

## 配置文件

### `~/.xho/config.toml` — 主配置（`AppConfig`）

```toml
[server]
log_path = "~/.xho/xhod.log"
log_level = "info"

[server.local]
enable = true
socket_path = "~/.xho/xhod.sock"

[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "corp-jump"]
pty = true
connect_timeout = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

[[gateways]]
kind = "xhod"
name = "remote-xhod"
address = "xho@203.0.113.10:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[[gateways]]
kind = "jumpserver"
name = "corp-jump"
host = "bastion.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"

[review]
enable = true
endpoint = "https://api.deepseek.com/v1/chat/completions"
model = "deepseek-v4-flash"
```

`AppConfig` 字段：`server`（`ServerConfig`）、`ssh`（`SshConfig`）、`copy`（`CopyConfig`）、`review`（`ReviewConfig`）、`gateways`（`Vec<GatewayConfig>`）。

### `~/.xho/server.toml` — 服务器清单（`src/config/inventory.rs`）

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"
shell = "bash"

[servers.host1]
host = "203.0.113.10"
port = 22
user = "root"

[servers.host2]
host = "192.0.2.200"
user = "admin"
shell = "zsh"  # 覆盖 defaults
```

每条 `ServerEntry` 含 `alias` / `host` / `port` / `user` / `auth`（`DirectAuth::Key { identity_file }` 或 `DirectAuth::Password { password }`），`password` 省略时连接时向用户询问。认证优先级：password > identity_file > defaults.identity_file。

`GatewayConfig` 是 tag 区分的枚举：`Xhod` / `Jumpserver` / `Direct`（见 `src/config/gateway.rs`）。

## 目录结构

```
src/
├── bin/
│   ├── xho.rs              # CLI 入口
│   └── xhod.rs             # Daemon 入口
├── cli/                    # CLI 逻辑（参数解析、交互模式、raw mode、复制/执行）
│   ├── mod.rs  args.rs  client.rs  copy.rs  daemon.rs
│   ├── exec.rs  host.rs  output.rs  progress.rs  prompt.rs
├── config.rs               # AppConfig（通用库入口）
├── config/                 # 配置类型
│   ├── client.rs  copy.rs  duration.rs  gateway.rs  inventory.rs
│   ├── path.rs  review.rs  server.rs  ssh.rs
├── daemon/                 # daemon 业务逻辑
│   ├── mod.rs              # 启动、监听、shutdown、DaemonState、XhoRpcService
│   ├── rpc.rs              # Gateway 分发、多候选 fallback
│   ├── resolver.rs         # target → Vec<Route>
│   ├── review.rs           # LLM 命令审查
│   ├── ssh_server.rs       # RemoteSshServer、IncomingConn
│   ├── connection_manager.rs  # ManagedPool / ManagedSingleton
│   ├── gateway/            # Gateway 抽象与实现
│   │   ├── mod.rs          # Gateway trait、GatewayKind、Route、GatewayError、build_gateways
│   │   ├── local.rs        # LocalGateway（直连 SSH + ManagedPool）
│   │   ├── xhod.rs         # XhodGateway（SSH subsystem + gRPC + ManagedSingleton）
│   │   ├── jumpserver.rs   # JumpserverGateway（PTY shell + 菜单导航）
│   │   └── auth.rs         # AuthPrompter、AuthPrompt、TOTP、known_hosts
│   └── connection/         # Connection trait（daemon 内部）与实现
│       ├── mod.rs          # Connection trait（pub(super)）
│       ├── direct.rs       # DirectConnection（SSH channel）
│       ├── xhod.rs         # XhodConnection（gRPC stream）
│       ├── jumpserver.rs   # JumpserverConnection（PTY shell）
│       └── shared.rs       # shell_quote、build_command、wrap_in_shell、PtyShell
├── copy_frames.rs          # 文件复制帧编解码（通用库）
├── protocol.rs             # gRPC 类型 / 内部协议类型（通用库）
├── exit_codes.rs           # 退出码处理（通用库）
├── output.rs               # 输出格式化（通用库）
├── logging.rs              # 日志配置（通用库）
├── types.rs                # 共享类型（通用库）
└── lib.rs                  # 模块声明
proto/
└── xho.proto               # gRPC 协议定义
```

`src/` 根只放与具体业务无关的通用库（config、protocol、logging 等）。所有业务逻辑在 `cli/` 与 `daemon/` 下；Gateway 与 Connection 是 daemon 的内部实现细节。

## 数据流

### `xho exec host1 -- ls`（本地直连）

```
CLI
  → gRPC StartRequest { target:"host1", argv:["ls"], shell:"", no_shell:false }
  → Daemon
      → Resolver.resolve("host1") → [Route { gateway:"local", end_target:"host1" }]
      → Reviewer.review("host1", ["ls"], "'ls'") → allow
      → gateways["local"].exec("host1", ExecRequest { argv:["ls"], shell:"bash", ... })
          → LocalGateway.resolve_target("host1") → server.toml 查得 host/port/user/auth
          → ManagedPool checkout 或新建 DirectConnection（SSH）
          → DirectConnection.exec(): channel.exec("bash -ic 'ls'") → 流式 stdout/stderr/exit_status
      → return exit_code
  → CLI 显示输出
```

### `xho exec remote-xhod:db01 -- ls`（经远程 xhod）

```
CLI → Daemon
  → Resolver.resolve("remote-xhod:db01") → [Route { gateway:"remote-xhod", end_target:"db01" }]
  → gateways["remote-xhod"].exec("db01", req)
      → ManagedSingleton checkout 共享 gRPC 客户端（必要时新建 SSH subsystem "xho-rpc" 连接）
      → 远程 daemon 的 XhoRpcService 处理 Execute（远程 Resolver/Gateway 再解析 "db01"）
      → 结果流式回传
```

### `xho ls`（merged view 聚合）

```
CLI → gRPC ListServers → Daemon
  → rpc::process_list_servers()：按声明顺序遍历 gateways
      ├─ LocalGateway.list_servers()        → 读 server.toml（零 I/O）
      ├─ XhodGateway.list_servers()         → gRPC ListServers（远程聚合）
      └─ JumpserverGateway.list_servers()   → Err(Unsupported) → 跳过并标记
  → 合并所有 ServerListRow，带上来源标签（local / <gateway-name>）
  → CLI 格式化显示
```

### 多候选 fallback `xho exec <bare-alias> -- ls`

```
CLI → Daemon
  → Resolver.resolve(<alias>) → 多个候选 Route（按 fallback 顺序）
  → 逐个尝试：
      gateways[r0].exec(...) → Resolution error（目标不在该 gateway）→ continue
      gateways[r1].exec(...) → Ok(exit_code) → return
  → 全部失败则返回最后一个错误
```
