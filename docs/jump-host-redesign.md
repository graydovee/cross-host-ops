# Gateway 与 Connection 架构重设计

## 核心模型

```
┌────────────────────────────────────────────────────────────┐
│  Gateway (跳板机)                                           │
│                                                            │
│  对外接口（调用方只看到这些）：                                │
│  - exec(target, request) → i32                             │
│  - copy(target, spec) → ()                                 │
│  - exec_interactive(target, request) → InteractiveHandle   │
│  - list_servers() → Vec<ServerEntry>                       │
│                                                            │
│  内部职责（对调用方透明）：                                    │
│  - 连接池管理（新建/复用/淘汰/重连）                           │
│  - 按需建立连接                                             │
│  - transport error 自动重试                                 │
│                                                            │
│  内部持有：                                                  │
│  - config（连接参数）                                        │
│  - pool: Vec<Connection>（连接池，每种 gateway 策略不同）      │
└────────────────────────┬───────────────────────────────────┘
                         │ 内部使用
                         ▼
┌────────────────────────────────────────────────────────────┐
│  Connection (到 end target 的操作通道)                       │
│                                                            │
│  纯操作接口，不暴露给 Gateway 的调用方：                       │
│  - exec(request) → i32                                     │
│  - copy(spec) → ()                                         │
│  - exec_interactive(request) → InteractiveHandle           │
│  - is_alive() → bool                                       │
└────────────────────────────────────────────────────────────┘
```

## Trait 定义

### Gateway — 调用方唯一接口

```rust
#[async_trait]
pub trait Gateway: Send + Sync {
    /// Execute a command on the specified end target.
    /// Internally acquires a connection (new or pooled), handles retries.
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32>;

    /// Copy files to/from the specified end target.
    async fn copy(&self, target: &str, spec: &CopySpec) -> Result<()>;

    /// Open an interactive PTY session to the specified end target.
    async fn exec_interactive(&self, target: &str, request: &InteractiveRequest) -> Result<InteractiveHandle>;

    /// List servers reachable through this gateway.
    /// Returns UnsupportedCapability for gateways that don't support discovery.
    async fn list_servers(&self) -> Result<Vec<ServerEntry>>;

    fn kind(&self) -> GatewayKind;
    fn name(&self) -> &str;
}
```

注意：没有 `open_connection`、`disconnect`、`is_alive` — 连接管理完全内部化。

### Connection — Gateway 内部使用

```rust
#[async_trait]
pub trait Connection: Send {
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32>;
    async fn copy(&mut self, spec: &CopySpec) -> Result<()>;
    async fn exec_interactive(&mut self, request: &InteractiveRequest) -> Result<InteractiveHandle>;
    fn is_alive(&self) -> bool;
}
```

Connection 不暴露给 daemon/调用方。它是 Gateway 的内部实现细节。

## 四种 Gateway 实现

### LocalGateway

本地直连 SSH — 替代当前的 `DirectJumpHost` + `ConnectionPool`。

```rust
pub struct LocalGateway {
    name: String,                       // "local"
    server_config: Arc<ServerConfigFile>,
    ssh_config: Arc<AppConfig>,
    /// 按 end target (host:port) 分组的连接池
    pools: Mutex<HashMap<String, Vec<PooledConn<DirectConnection>>>>,
}
```

**连接池策略**：
- 按 `host:port` 分组，相同地址复用连接
- 不同地址新建连接
- 空闲超时淘汰
- transport error → 丢弃旧连接，新建重试

**list_servers**：直接读 `server_config` 返回，不需要任何连接。

```rust
impl Gateway for LocalGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32> {
        let host_info = self.resolve_target(target)?;
        let mut conn = self.acquire_connection(&host_info).await?;
        match conn.exec(request).await {
            Ok(code) => { self.release(conn); Ok(code) }
            Err(e) if is_transport(&e) => {
                // discard broken conn, retry with new one
                let mut conn = self.new_connection(&host_info).await?;
                let result = conn.exec(request).await;
                self.release(conn);
                result
            }
            Err(e) => Err(e),
        }
    }

    async fn list_servers(&self) -> Result<Vec<ServerEntry>> {
        // Zero-cost: just read config
        Ok(self.server_config.to_entries())
    }
}
```

### RhopdGateway

通过 SSH subsystem 连接远程 rhopd daemon。

```rust
pub struct RhopdGateway {
    name: String,
    config: RhopdGatewayConfig,    // address, identity_file, known_hosts
    /// 单个 gRPC client 连接（复用，远程 daemon 管理并发）
    client: AsyncMutex<Option<RhopdClient>>,
}
```

**连接池策略**：
- 只维护一个到远程 rhopd 的 SSH subsystem 连接
- 所有请求复用同一个 gRPC client
- 远程 rhopd 自己管理到 end target 的并发连接
- 断线时惰性重连

**list_servers**：通过 gRPC `ListServers` RPC 查询远程 daemon。

```rust
impl Gateway for RhopdGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32> {
        let client = self.ensure_client().await?;
        // Send StartRequest with target via gRPC Execute stream
        client.execute(target, request).await
    }

    async fn list_servers(&self) -> Result<Vec<ServerEntry>> {
        let client = self.ensure_client().await?;
        client.list_servers().await
    }
}
```

### JumpserverGateway

交互式跳板机（SSH + PTY shell + menu）。

```rust
pub struct JumpserverGateway {
    name: String,
    config: JumpserverGatewayConfig,  // host, port, user, mfa, menu patterns
    /// 单个 PTY shell 连接（jumpserver 天然单会话）
    shell: AsyncMutex<Option<PtyShell>>,
}
```

**连接池策略**：
- 维护一个 PTY shell 连接
- Jumpserver 的 PTY shell 是有状态的（menu 导航），并发受限
- exec 时从 shell 导航到 target，执行，返回

**list_servers**：不支持，直接返回 `UnsupportedCapability`，零成本。

```rust
impl Gateway for JumpserverGateway {
    async fn exec(&self, target: &str, request: &ExecRequest) -> Result<i32> {
        let shell = self.ensure_shell().await?;
        shell.navigate_to(target).await?;
        shell.exec_command(request).await
    }

    async fn list_servers(&self) -> Result<Vec<ServerEntry>> {
        Err(UnsupportedCapability { .. }.into())
    }
}
```

### DirectGateway（可选，用于 config 中的 `kind = "direct"` 跳板）

和 LocalGateway 类似但目标固定为单个 host。可以是 LocalGateway 的特例。

## Connection 实现

### DirectConnection

一个 SSH channel（session），由 LocalGateway/DirectGateway 创建。

```rust
pub struct DirectConnection {
    handle: SshHandle,  // SSH handle (可开多个 channel)
}

impl Connection for DirectConnection {
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32> {
        let mut channel = self.handle.channel_open_session().await?;
        let command = build_final_command(&request.argv, &request.shell);
        if request.pty { channel.request_pty(...).await?; }
        channel.exec(command).await?;
        // stream stdout/stderr/exit
    }
}
```

### RhopdConnection

一个 gRPC Execute stream，由 RhopdGateway 创建。

```rust
pub struct RhopdConnection {
    client: RhopdClient,
    target: String,
}

impl Connection for RhopdConnection {
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32> {
        // Start Execute streaming RPC with target + argv
    }
}
```

### JumpserverConnection

PTY shell 上的命令执行，由 JumpserverGateway 创建。

```rust
pub struct JumpserverConnection {
    shell: Arc<AsyncMutex<PtyShell>>,
}

impl Connection for JumpserverConnection {
    async fn exec(&mut self, request: &ExecRequest) -> Result<i32> {
        // Send command through PTY shell, sentinel-based output extraction
    }
}
```

## 调用方视角（Daemon）

```rust
// Daemon 持有所有 gateway（从配置创建，不建立连接）
struct DaemonState {
    gateways: HashMap<String, Arc<dyn Gateway>>,  // name → gateway
    resolver: Resolver,
}

// exec 处理
async fn process_execute(request: ExecRequest, state: &DaemonState) -> Result<i32> {
    let routes = state.resolver.resolve(&request.target)?;

    // 多候选遍历
    let mut last_error = None;
    for route in &routes {
        let gateway = state.gateways.get(&route.gateway_name)
            .ok_or_else(|| anyhow!("gateway not found"))?;

        match gateway.exec(&route.end_target, &request).await {
            Ok(code) => return Ok(code),
            Err(e) if is_resolution_error(&e) => {
                last_error = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("no routes")))
}

// list_servers 处理
async fn process_list_servers(state: &DaemonState) -> MergedServerList {
    let mut rows = Vec::new();
    for gateway in state.gateways.values() {
        match gateway.list_servers().await {
            Ok(entries) => rows.extend(entries),
            Err(e) if is_unsupported(&e) => continue,  // 零成本跳过
            Err(e) => { warn!(...); continue; }
        }
    }
    rows
}
```

## 各 Gateway 的连接池策略总结

| Gateway | 池结构 | 复用策略 | list_servers |
|---------|--------|----------|-------------|
| LocalGateway | `HashMap<host:port, Vec<SshHandle>>` | 相同地址复用，不同地址新建 | 读配置文件，零连接 |
| RhopdGateway | 单个 `RhopdClient` | 所有请求复用同一连接，远程 daemon 管并发 | gRPC RPC，惰性连接 |
| JumpserverGateway | 单个 `PtyShell` | 串行复用（有状态 shell） | 不支持，零成本返回 |

## 层次关系图

```
Daemon
  │
  ├─ Resolver: target → Vec<Route { gateway_name, end_target }>
  │
  ├─ Gateways (从 config 创建，不建立连接)
  │   │
  │   ├─ LocalGateway
  │   │   ├─ list_servers() → 读 server.toml
  │   │   ├─ exec("host1", req) → acquire DirectConnection → conn.exec()
  │   │   └─ pools: { "203.0.113.10:22": [SshHandle, SshHandle, ...] }
  │   │
  │   ├─ RhopdGateway("ali-rhopd")
  │   │   ├─ list_servers() → ensure_client → gRPC ListServers
  │   │   ├─ exec("host2", req) → ensure_client → gRPC Execute
  │   │   └─ client: Option<RhopdClient>  (惰性，共享)
  │   │
  │   └─ JumpserverGateway("corp-jump")
  │       ├─ list_servers() → Err(UnsupportedCapability)  (不连接)
  │       ├─ exec("target", req) → ensure_shell → navigate → exec
  │       └─ shell: Option<PtyShell>  (惰性)
  │
  └─ 操作执行
      │
      ├─ exec: resolver.resolve() → for route in routes → gateway.exec()
      └─ ls:   for gw in gateways → gw.list_servers()
```

## 目录结构

```
src/
├── gateway/
│   ├── mod.rs           # Gateway trait + GatewayKind + Route
│   ├── local.rs         # LocalGateway (直连 SSH + 连接池)
│   ├── rhopd.rs         # RhopdGateway (SSH subsystem + gRPC)
│   ├── jumpserver.rs    # JumpserverGateway (PTY shell)
│   └── error.rs         # UnsupportedCapability, ErrorClass
├── connection/
│   ├── mod.rs           # Connection trait
│   ├── direct.rs        # DirectConnection (SSH channel 操作)
│   ├── rhopd.rs         # RhopdConnection (gRPC stream 操作)
│   ├── jumpserver.rs    # JumpserverConnection (PTY 操作)
│   └── shared.rs        # shell_quote, build_command, wrap_in_shell
├── resolver.rs          # 目标解析 (target → Vec<Route>)
├── daemon.rs            # RPC handler (只调用 gateway.exec/copy/list_servers)
├── cli.rs
├── config.rs
└── ...
```

## 关键设计决策

1. **Gateway 对外不暴露连接细节** — 调用方只看到 `exec/copy/list_servers`，连接池、重连、并发控制全在 Gateway 内部。

2. **Connection 是 Gateway 的私有实现** — 不 pub，不暴露给 daemon。Gateway 内部用 Connection trait 来抽象具体操作，方便测试和替换。

3. **每种 Gateway 自己的池策略** — LocalGateway 按地址分池（多连接），RhopdGateway 共享单连接，JumpserverGateway 单 shell。不需要统一的 Pool 组件。

4. **外部 Pool 消失** — 当前的 `ConnectionPool` 被各 Gateway 内部替代。Daemon 直接持有 `HashMap<String, Arc<dyn Gateway>>`。

5. **Resolver 产出 Route** — `Route { gateway_name: String, end_target: String }`。Daemon 用 gateway_name 查找 Gateway，把 end_target 传给它。

6. **多候选 fallback 在 Daemon 层** — 简单遍历 routes，resolution error 时 continue。不需要 pool 参与。

## 认证与登录

认证是 Gateway 内部"建立连接"阶段的一部分，对 `exec/copy/list_servers` 的调用方完全透明。认证可能需要多轮用户交互（密码、MFA、动态 prompt），通过注入的 `AuthPrompter` 回调解决。

### 认证流程

```
Gateway.exec(target, request)
  │
  └─ ensure_connected() / acquire_connection()
       │
       ├─ TCP connect
       ├─ SSH handshake
       └─ authenticate(auth_config, prompter)
            │
            ├─ 有 key → ssh key auth
            ├─ 有 password → password auth
            ├─ password 为空 → prompter.ask("Password:", secret=true)
            │                   → 用户输入 → password auth
            ├─ (JumpserverGateway 额外步骤)
            │   ├─ MFA prompt → prompter.ask("MFA:", secret=true)
            │   │               或 自动 TOTP（如果配置了 totp_secret）
            │   └─ menu 导航（等待菜单 prompt，选择目标）
            └─ 连接就绪
```

### AuthPrompter 接口

```rust
/// Callback for interactive authentication prompts.
/// Injected into Gateway at construction time.
/// May be called multiple times during a single connection establishment.
pub type AuthPrompter = dyn Fn(AuthPrompt) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync;

pub struct AuthPrompt {
    pub gateway_name: String,   // which gateway is asking
    pub message: String,        // prompt message to show user
    pub secret: bool,           // true = don't echo (password/MFA)
}
```

### 认证在各 Gateway 中的位置

**LocalGateway**：

```rust
async fn new_connection(&self, target: &HostInfo) -> Result<DirectConnection> {
    let handle = ssh_connect(&target.host, target.port).await?;

    match &target.auth {
        Auth::Key(path) => {
            authenticate_with_key(&handle, &target.user, path).await?;
        }
        Auth::Password(pw) => {
            authenticate_with_password(&handle, &target.user, pw).await?;
        }
        Auth::None => {
            // server.toml 中没有 password 字段 — 向用户询问
            let pw = (self.prompter)(AuthPrompt {
                gateway_name: self.name.clone(),
                message: format!("Password for {}@{}: ", target.user, target.host),
                secret: true,
            }).await?;
            authenticate_with_password(&handle, &target.user, &pw).await?;
        }
    }

    Ok(DirectConnection { handle })
}
```

**JumpserverGateway**（多轮交互）：

```rust
async fn new_shell(&self) -> Result<PtyShell> {
    let handle = ssh_connect(&self.config.host, self.config.port).await?;

    // Step 1: SSH auth (key or password)
    match &self.config.auth {
        Auth::Key(path) => authenticate_with_key(&handle, &self.config.user, path).await?,
        Auth::Password(pw) => authenticate_with_password(&handle, &self.config.user, pw).await?,
        Auth::None => {
            let pw = (self.prompter)(AuthPrompt {
                gateway_name: self.name.clone(),
                message: format!("Password for {}: ", self.config.user),
                secret: true,
            }).await?;
            authenticate_with_password(&handle, &self.config.user, &pw).await?;
        }
    }

    // Step 2: Open PTY shell
    let channel = handle.channel_open_session().await?;
    channel.request_pty(true, "xterm-256color", 80, 24, 0, 0, &[]).await?;
    channel.shell(true).await?;
    let mut shell = PtyShell::new(channel);

    // Step 3: MFA (may require user input or auto-TOTP)
    if let Some(mfa_config) = &self.config.mfa {
        shell.wait_for_prompt(&self.config.mfa_prompt_contains).await?;
        let code = if let Some(totp_secret) = &mfa_config.totp_secret_base32 {
            generate_totp(totp_secret, mfa_config)?  // automatic
        } else {
            // No TOTP secret configured — ask user
            (self.prompter)(AuthPrompt {
                gateway_name: self.name.clone(),
                message: "MFA code: ".to_string(),
                secret: true,
            }).await?
        };
        shell.send_line(&code).await?;
    }

    // Step 4: Wait for menu ready
    shell.wait_for_prompt(&self.config.menu_prompt_contains).await?;

    Ok(shell)
}
```

**RhopdGateway**：

```rust
async fn new_client(&self) -> Result<RhopdClient> {
    let handle = ssh_connect(&self.config.host, self.config.port).await?;

    // SSH auth — rhopd 通常用 key，但也支持 password
    match &self.config.auth {
        Auth::Key(path) => authenticate_with_key(&handle, "rhop", path).await?,
        Auth::Password(pw) => authenticate_with_password(&handle, "rhop", pw).await?,
        Auth::None => {
            let pw = (self.prompter)(AuthPrompt {
                gateway_name: self.name.clone(),
                message: "Password for rhop: ".to_string(),
                secret: true,
            }).await?;
            authenticate_with_password(&handle, "rhop", &pw).await?;
        }
    }

    // Open subsystem → gRPC
    let channel = handle.channel_open_session().await?;
    channel.request_subsystem(true, "rhop-rpc").await?;
    RhopdClient::from_channel(channel).await
}
```

### Prompter 数据流

```
Gateway (内部建连)
  │ 需要用户输入
  │
  ├─ (self.prompter)(AuthPrompt { message, secret })
  │       │
  │       ▼
  │   Daemon 层的 prompter 实现
  │       │
  │       ├─ 通过 gRPC stream 发送 AuthPrompt event 给 CLI
  │       │       │
  │       │       ▼
  │       │   CLI 显示 prompt，读取用户输入
  │       │       │
  │       │       ▼
  │       │   CLI 通过 gRPC stream 发回 AuthInputRequest
  │       │
  │       ▼
  │   Daemon 返回用户输入给 Gateway
  │
  └─ Gateway 用输入完成认证步骤
```

### 配置模型

```toml
# server.toml — password 可以省略，连接时向用户询问
[servers.host1]
host = "203.0.113.10"
user = "root"
# password = "..."  ← 省略时连接时 prompt

# config.toml — jumpserver with MFA
[[jump_hosts]]
name = "corp-jump"
kind = "jumpserver"
host = "bastion.example.com"
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"

[jump_hosts.mfa]
totp_secret_base32 = "..."   # 有值 → 自动生成 TOTP code
# totp_secret_base32 省略 → 连接时 prompt 用户输入 MFA code
```

