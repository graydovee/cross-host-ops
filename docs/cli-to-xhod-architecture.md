# CLI 到 xhod 通信架构（当前实现）

## 整体架构

```
┌──────────┐  gRPC/Unix Socket  ┌──────────────────────────────────────┐
│  xho    │ ──────────────────▶ │  xhod (本地 daemon)                 │
│  (CLI)   │                     │                                      │
└──────────┘                     │  两种监听方式：                        │
                                 │  1. Unix Socket (本地 CLI 连接)        │
                                 │  2. TCP:2222 SSH Server (远程连接)     │
                                 └──────────────┬───────────────────────┘
                                                │
                                                │ 内部路由到 end target
                                                │
                         ┌──────────────────────┼──────────────────────┐
                         │                      │                      │
                         ▼                      ▼                      ▼
                   直连 SSH               gRPC over SSH           PTY Shell
                  (DirectJumpHost)      subsystem(XhodJumpHost)  (JumpserverJumpHost)
                         │                      │                      │
                         ▼                      ▼                      ▼
                   End Target            远程 xhod daemon         End Target
                                         (再次解析+直连SSH)
```

## 通信路径详解

### 路径 1: CLI → 本地 xhod（本地 Unix Socket）

```
xho exec host1 -- ls
  │
  ├─ CLI 连接到 ~/.xho/xhod.sock (gRPC over Unix Socket)
  ├─ 发送 StartRequest { target: "host1", argv: ["ls"], ... }
  ├─ xhod 收到请求
  │   ├─ Resolver 解析 "host1" → TargetRoute (hops=0, direct)
  │   ├─ Pool 获取/创建到 host1 的 SSH 连接
  │   ├─ DirectJumpHost.exec() → DirectSshConnection.execute()
  │   │   ├─ channel.request_pty() (如果需要)
  │   │   └─ channel.exec("bash -ic 'ls'")
  │   └─ 流式返回 Stdout/Stderr/ExitStatus
  └─ CLI 接收输出并显示
```

### 路径 2: CLI → 本地 xhod → 远程 xhod（SSH subsystem 链路）

```
xho exec host2 -- ls
  │
  ├─ CLI 连接到 ~/.xho/xhod.sock
  ├─ 发送 StartRequest { target: "host2", ... }
  ├─ 本地 xhod 收到请求
  │   ├─ Resolver 解析 "host2"
  │   │   ├─ 本地 server.toml 没有 → 检查 merged view
  │   │   └─ merged view: remote-xhod:host2 → hops=1 (remote-xhod)
  │   ├─ Pool 获取 remote-xhod 的 XhodJumpHost 连接
  │   │   └─ XhodJumpHost::connect() (如果不存在)
  │   │       ├─ SSH connect 到 203.0.113.10:2222
  │   │       ├─ SSH publickey auth (user=xho, key=~/.ssh/id_ed25519)
  │   │       ├─ channel.request_subsystem("xho-rpc")
  │   │       └─ 建立 gRPC Channel over SSH subsystem stream
  │   ├─ XhodJumpHost.exec("host2", argv)
  │   │   ├─ 通过 gRPC Execute stream 发送 StartRequest { target: "host2", argv }
  │   │   └─ 远程 xhod 处理（见下）
  │   └─ 流式返回结果
  └─ CLI 接收输出
```

### 路径 3: 远程 xhod 处理入站请求

```
远程 xhod (203.0.113.10:2222)
  │
  ├─ SSH Server (russh::server) 监听 TCP:2222
  │   ├─ 收到连接 → RemoteSshHandler
  │   ├─ auth_publickey() → 检查 ~/.xho/authorized_keys
  │   ├─ subsystem_request("xho-rpc") → channel stream 送入 incoming_tx
  │   └─ 这个 stream 被 tonic Server 当作一个 gRPC 连接处理
  │
  ├─ tonic gRPC Server (与本地 Unix Socket 共用同一个 service)
  │   ├─ XhoRpcService::execute() 收到请求
  │   ├─ Resolver 解析 "host2" → 本地 server.toml 匹配
  │   ├─ Pool 获取/创建 SSH 连接到 host2 (192.0.2.200:22)
  │   ├─ DirectJumpHost.exec() → SSH channel.exec()
  │   └─ 流式返回 Stdout/Stderr/ExitStatus
  │
  └─ 结果通过 gRPC stream → SSH subsystem → 回到本地 xhod → CLI
```

## xhod 的双重角色

同一个 `xhod` 二进制同时扮演两个角色：

| 角色 | 监听方式 | 接收来源 | 用途 |
|------|----------|----------|------|
| **本地 daemon** | Unix Socket | 本地 `xho` CLI | 处理用户命令 |
| **远程 daemon** | TCP:2222 (SSH Server) | 其他机器的 `xhod` (通过 XhodJumpHost) | 作为跳板机服务远端请求 |

两种来源最终共用同一套 gRPC service（`XhoRpcService`），只是入口不同：
- 本地：`IncomingConn::Local(UnixStream)`
- 远程：`IncomingConn::Remote(RemoteChannelStream)` — 一个 SSH channel stream

## XhodJumpHost 连接建立流程

```
XhodJumpHost::connect(alias, address, identity_file, known_hosts_path, target_label)
  │
  ├─ 1. parse_remote_target("xho@203.0.113.10:2222") → RemoteTarget
  ├─ 2. normalize_remote_paths() → expand ~
  ├─ 3. russh::client::connect(host, port) + host key 校验
  │       └─ XhodAuthClientHandler::check_server_key() → 查 known_hosts
  ├─ 4. authenticate_publickey(user="xho", key=identity_file)
  ├─ 5. channel_open_session() → request_subsystem("xho-rpc")
  ├─ 6. 把 SSH channel stream 包装成 XhodSubsystemStream
  │       └─ 通过 tonic Endpoint.connect_with_connector() 建立 gRPC Channel
  └─ 7. 返回 XhodJumpHost { client: XhoRpcClient, transport, ... }
```

## xhod 远程 SSH Server 实现

`daemon.rs` 中 `RemoteSshServer` / `RemoteSshHandler` 实现了 `russh::server`：

```rust
// 支持的操作：
auth_publickey()       → 校验 authorized_keys
channel_open_session() → 接受 channel
subsystem_request()    → 只接受 "xho-rpc"，其他拒绝
                         把 channel stream 送入 incoming_tx → tonic Server 处理

// 不支持的操作（全部拒绝）：
shell_request()        → channel_failure
exec_request()         → channel_failure  (不允许直接 exec)
tcpip_forward()        → false
streamlocal_forward()  → false
```

## gRPC 协议复用

本地和远程共用同一个 `XhoRpcService`，提供以下 RPC：

| RPC | 类型 | 本地/远程 | 功能 |
|-----|------|-----------|------|
| Execute | 双向流 | 两者 | 命令执行 |
| Copy | 双向流 | 两者 | 文件复制 |
| Status | Unary | 两者 | daemon 状态 |
| ListServers | Unary | 两者 | 服务器列表 |
| Shutdown | Unary | 仅本地 | 关闭 daemon |
| UpdateConfig | Unary | 仅本地 | 更新配置 |
| ListJumpHosts | Unary | 两者 | 跳板机列表 |

## 数据流图（完整链路）

```
用户终端
  │ 键盘输入/显示输出
  ▼
xho CLI (src/cli.rs)
  │ gRPC over Unix Socket
  ▼
本地 xhod
  │ tonic Server (XhoRpcService)
  │
  ├─ [直连] SSH channel.exec()
  │   └─ End Target
  │
  ├─ [xhod 跳板] gRPC over SSH subsystem ("xho-rpc")
  │   │
  │   ▼
  │   远程 xhod (TCP:2222 SSH Server)
  │     │ tonic Server (同一个 XhoRpcService)
  │     │
  │     └─ [直连] SSH channel.exec()
  │         └─ End Target
  │
  └─ [jumpserver] SSH + PTY Shell → 交互式菜单导航
      └─ End Target
```

## 当前问题

1. **XhodJumpHost 在工厂时立即连接** — `build_jump_host()` 调用 `XhodJumpHost::connect()`，即使只需要 `list_servers` 也要先建立完整的 SSH + subsystem + gRPC 链路。

2. **连接池在外部（Pool），不在跳板机内部** — Pool 持有 `Box<dyn JumpHost>` 的 slot，JumpHost 内部又持有连接。重连逻辑分散。

3. **没有 fallback 重试** — 多候选路由只用 first()，第一个失败不尝试下一个。

4. **CLI 不直接连接远程 xhod** — CLI 只能连本地 daemon。要访问远程 xhod 必须通过本地 daemon 中继。这是设计如此（安全性：所有操作经过本地 daemon 审查）。
