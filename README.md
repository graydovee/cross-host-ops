# Remote Hop

`Remote Hop` 是一个基于 Rust 实现的远端命令执行与文件复制工具。

它拆分为两个二进制：

- `rhop`：CLI 前端
- `rhup`：CLI 前端别名
- `rhopd`：本地 daemon 后端

CLI 通过本地 `gRPC over Unix socket` 或远程 `gRPC over SSH subsystem` 与 daemon 通信。daemon 负责 SSH 连接管理、连接池、可选 `server.toml` 目标解析、可选 jumpserver、可选 LLM 命令审查，以及远端输出流式转发。

## 功能

- CLI 用法：`rhop exec <target> <cmd> [arg...]`
- 前后端分离，支持本地 `gRPC over Unix socket` 与远程 `gRPC over SSH subsystem`
- 按目标 IP 复用 SSH 连接
- 没有空闲连接时自动新建连接
- 单 IP 连接数上限可配置，默认 `10`
- 空闲连接超时自动关闭
- 支持 `server.toml` 机器清单、密码或 SSH key 直连
- 支持可选 jumpserver + TOTP MFA
- 支持可选 OpenAI 兼容接口的命令审查
- 无配置文件也可运行，前提是 `~/.ssh/config` 已配置好直连目标

## 依赖前提

- 本机已安装 Rust 工具链和 `cargo`
- 本机已安装 `protoc`
- 当前用户有可用的 `~/.ssh/config`
- 当前用户可以访问对应 SSH 私钥

可选依赖：

- 如果要走 jumpserver，需要 jumpserver 账号信息和 TOTP 秘钥
- 如果要开启命令审查，需要 OpenAI 兼容接口的 API key

## 构建

```bash
cargo build
```

生成的二进制位于：

- `target/debug/rhop`
- `target/debug/rhopd`

## 运行模式

`rhopd` 默认前台运行，适合：

- systemd
- docker
- 手工前台调试

只有显式传入 `--daemon` 时，`rhopd` 才会自行转入后台运行。

daemon 管理保护：

- 由 `rhop`/`rhup` 拉起的 daemon 会标记为 `cli_spawned`
- 由 systemd、docker 或手工 `rhopd` 启动的 daemon 会标记为 `external`
- 只有 `cli_spawned` 的 daemon 允许执行 `daemon stop` 和 `daemon restart`
- 可通过 `rhop status` 查看 `daemon_origin`、`cli_controllable` 和记录的 CLI 启动参数

## 工作方式

1. `rhop exec` 接收目标和命令。
2. `rhop` 默认通过本地 `gRPC over Unix socket` 连接 `rhopd`。
3. 如果 `rhopd` 尚未运行，`rhop` 会自动以 `--daemon` 模式拉起它。
4. daemon 根据第一个参数推导目标 IP。
5. daemon 先检查 `server.toml`，支持按别名或 `host` 字段命中目标。
6. 如果 `server.toml` 命中，则优先尝试该条目配置的直连认证。
7. 如果 `server.toml` 未命中，或连接/认证失败，则按 `ssh.fallback` 继续尝试 `~/.ssh/config` 或 jumpserver。
8. 如果 jumpserver 要求 MFA 且未配置 TOTP secret，CLI 会提示输入一次性验证码。
9. 同一逻辑目标有空闲连接时优先复用。
10. 如果没有空闲连接，则在上限内新建连接。
11. 如果启用了 review，daemon 会先做命令审查。
12. 命令输出会实时流式返回到 CLI。

## 通信方式

前后端使用统一的 gRPC RPC：

- local：`gRPC over Unix socket`
- remote：`gRPC over SSH subsystem (rhop-rpc)`

- `Execute`：双向流 RPC
- `Copy`：双向流 RPC
- `Status`：普通 unary RPC

`Execute` 流里：

- CLI 先发送开始执行请求
- daemon 回流 review、stdout、stderr、confirm、auth prompt、exit 等事件
- 如果需要确认或输入 MFA，CLI 会在同一条流里回发对应消息

## One By One：零配置首次运行

这是最简单的路径，只依赖 `~/.ssh/config`。

### 第 1 步：确认 `~/.ssh/config` 中有目标 IP 的直连配置

示例：

```sshconfig
Host 192.0.2.163
    HostName 192.0.2.163
    Port 22
    User wzy
    IdentityFile ~/.ssh/id_rsa
```

关键点是：`rhop` 推导出的 IP 必须与 `Host` 名称匹配。

### 第 2 步：构建项目

```bash
cargo build
```

### 第 2.5 步：可选，手工前台启动 daemon

如果你想以前台方式运行 `rhopd`，例如用于 systemd、docker 或调试，可以单独启动：

```bash
./target/debug/rhopd
```

此时 `rhopd` 会保持在前台运行。

### 第 3 步：执行远端命令

```bash
./target/debug/rhop exec foo-192-0-2-163 hostname
```

这里 `rhop` 会把 `foo-192-0-2-163` 推导成 `192.0.2.163`，然后按 `server.toml -> fallback` 的顺序解析目标；如果 daemon 没启动则自动启动，然后执行远端命令。

### 第 4 步：查看 daemon 状态

```bash
./target/debug/rhop status
```

会显示：

- socket 路径
- 当前活跃执行数
- 每个目标连接池的状态

### 第 5 步：在同一 IP 上再次执行命令

```bash
./target/debug/rhop exec foo-192-0-2-163 whoami
```

如果该 IP 已有空闲连接，则会直接复用。

## One By One：使用配置文件运行

程序不强制要求配置文件，但在你需要自定义行为时可以增加配置。

默认配置路径：

```text
~/.rhop/config.toml
```

仓库中提供了一个完整但全部注释掉的示例：

- [config.example.toml](/home/admin/Projects/agora/arun/config.example.toml)

### 第 1 步：创建配置目录

```bash
mkdir -p ~/.rhop
```

### 第 2 步：复制示例配置

```bash
cp config.example.toml ~/.rhop/config.toml
```

### 第 3 步：只打开你需要的配置项

最常见的是：

- 配置 `server.toml` 机器清单
- 调整 `ssh.pty`
- 开启 jumpserver
- 开启 review
- 修改 socket 路径
- 调整连接池上限和空闲超时

### 第 3.5 步：可选，创建 `server.toml`

默认路径：

```text
~/.rhop/server.toml
```

示例：

```toml
[defaults]
identity_file = "~/.ssh/id_rsa"

[servers.devbox]
host = "192.0.2.163"
user = "root"

[servers.ops]
host = "foo-192-0-2-163"
user = "ops"
identity_file = "~/.ssh/id_ed25519"

[servers.db]
host = "192.0.2.200"
user = "dba"
password = "REPLACE_ME"
```

规则：

- 表键别名可直接命中，例如 `rhop exec devbox hostname`
- 未命中表键时，会再用原始 target 和推导 IP 精确匹配 `host`
- 单机认证优先级是 `password > identity_file > defaults.identity_file`

### 第 3.6 步：可选，调整 `ssh.pty`

在 `~/.rhop/config.toml` 中：

```toml
[ssh]
pty = true
```

语义：

- `pty = true`
  - 直连命令更像手工 SSH，`ls` 等命令会正常输出颜色
  - jumpserver 保持现有 PTY 交互与颜色表现
- `pty = false`
  - 直连命令按非 PTY 风格执行，颜色不输出
  - jumpserver 不受该开关影响，仍然始终使用 PTY

### 第 4 步：重启 daemon 使配置生效

```bash
./target/debug/rhop daemon restart
```

如果 daemon 还没有运行，那么第一次执行命令时会自动读取新配置。

## One By One：连接远程 daemon

远程模式的默认体验接近 `ssh`：

1. 在远端机器上开启 `server.remote.enable = true`
2. 首次连接：

```bash
./target/debug/rhop remote connect rhop@example.com:2222
```

如果主机还不在 `~/.rhop/known_hosts` 中，CLI 会展示 SSH host key 的 SHA256 指纹并等待确认。确认后会把主机记录写入 `~/.rhop/known_hosts`，并把远程目标保存到 `~/.rhop/client.toml`。

3. 切换默认执行目标到远程 daemon：

```bash
./target/debug/rhop remote enable
```

4. 之后普通命令默认走远程 daemon：

```bash
./target/debug/rhop status
./target/debug/rhop exec foo-192-0-2-163 hostname
```

5. 切回本地 daemon：

```bash
./target/debug/rhop remote disable
```

## One By One：开启 jumpserver

只有当目标既不能通过 `server.toml` 成功直连，也不能通过 `~/.ssh/config` 成功直连时，才需要启用 jumpserver。

### 第 1 步：打开 jumpserver 配置段

在 `~/.rhop/config.toml` 中取消注释并填写：

```toml
[jumpserver]
enabled = true
host = "bastion.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
menu_prompt_contains = "Opt"
mfa_prompt_contains = "MFA"
shell_prompt_suffixes = ["$ ", "# "]

[jumpserver.mfa]
totp_secret_base32 = "REPLACE_ME"
digits = 6
period = 30
digest = "sha1"
```

### 第 2 步：重启 daemon 使配置生效

```bash
./target/debug/rhop daemon restart
```

### 第 3 步：执行一个未在 `~/.ssh/config` 中定义直连的目标

```bash
./target/debug/rhop exec foo-192-0-2-200 uname -a
```

这时 daemon 会：

- 连接 jumpserver
- 在需要时自动提交 TOTP，或由 CLI 提示输入 MFA
- 选择目标 IP
- 保持 jumpserver 后面的连接可复用

## One By One：开启命令审查

命令审查默认关闭。

### 第 1 步：设置 API key

可以使用：

```bash
export RHOP_REVIEW_API_KEY=...
```

或者：

```bash
export OPENAI_API_KEY=...
```

### 第 2 步：在配置中开启 review

```toml
[review]
enable = true
```

对于默认的 OpenAI 兼容调用路径，这样就已经够了。

这些内容在代码中已有默认值：

- endpoint
- model
- timeout
- 简单命令本地快速白名单
- prompts
- 风险等级策略
- 语义白名单

### 第 3 步：重启 daemon 使配置生效

```bash
./target/debug/rhop daemon restart
```

### 第 4 步：执行命令

```bash
./target/debug/rhop exec foo-192-0-2-163 cat /etc/hosts
```

执行前你会先看到审查结果。

如果策略把某个风险等级映射为 `confirm`，CLI 会在本地要求你二次确认。

### 本地白名单与 AI 安审

review 现在分两层：

- 简单命令先走本地快速白名单
- 复杂 shell / bash / python 脚本再走 LLM 语义安审

本地白名单规则：

- 如果规则里包含 `*`，按通配匹配完整命令
- 如果规则里不包含 `*`，则必须精确匹配

例如：

- `ls`
- `ls *`
- `kubectl get *`

而像下面这类命令会直接进入 AI 审查：

- `bash -lc '...'`
- `python -c '...'`
- 含 `&&`、`||`、`;`、`$()` 的复杂命令

## 连接池行为

对于同一个 IP：

- 有空闲连接就复用
- 所有连接都忙时就新建连接
- 达到单 IP 上限后，新请求进入等待
- 空闲超过 `max_idle_time` 后自动关闭连接

默认值：

- `max_connections_per_ip = 10`
- `max_idle_time = "10m"`

## 常用命令

执行远端命令：

```bash
./target/debug/rhop exec <target> <cmd> [arg...]
```

查看 daemon 和连接池状态：

```bash
./target/debug/rhop status
```

如果 `daemon_origin=external`，则 `daemon stop` 和 `daemon restart` 会被拒绝。

查看 daemon 当前使用的 `server.toml` 中有哪些服务器：

```bash
./target/debug/rhop server list
```

手动启动 daemon：

```bash
./target/debug/rhop daemon start
```

带显式参数启动 daemon：

```bash
./target/debug/rhop daemon start --config ~/.rhop/config.toml --log-level debug
```

停止 daemon：

```bash
./target/debug/rhop daemon stop
```

重启 daemon：

```bash
./target/debug/rhop daemon restart
```

`daemon restart` 会继承最近一次 CLI 拉起 daemon 时显式传入的 `--config` 和 `--log-level`。

以前台方式运行 daemon：

```bash
./target/debug/rhopd
```

以后台方式运行 daemon：

```bash
./target/debug/rhopd --daemon
```

指定配置文件和日志级别：

```bash
./target/debug/rhopd -c ~/.rhop/config.toml --log-level debug
```

修改配置后重启 daemon：

```bash
./target/debug/rhop daemon restart
```

## 目标解析规则

第一个 CLI 参数会按约定格式转换为 IP。

示例：

```text
foo-192-0-2-163 -> 192.0.2.163
```

解析顺序：

1. 根据 CLI 第一个参数推导 IP
2. 在 `~/.ssh/config` 中查找匹配的 `Host`
3. 如果命中，则走直连 SSH
4. 如果未命中且 jumpserver 已启用，则走 jumpserver
5. 否则直接报错

## 配置说明

配置文件是可选的。

代码中已经内置了这些默认值：

- socket 路径
- 日志默认输出到标准输出
- 日志级别默认是 `info`
- 如果配置了日志文件路径，修改该路径后需要重启 daemon 才会切换输出目标
- SSH 超时
- keepalive 间隔
- 空闲连接回收周期
- 单 IP 连接池大小
- review prompt
- review 风险策略
- review endpoint 和 model

因此推荐的使用方式是：

1. 先无配置直接运行
2. 确认需要偏离默认行为时，再增加配置

## 当前限制

- 直连模式要求推导出的 IP 能匹配 `~/.ssh/config` 中的 `Host`
- jumpserver 模式依赖交互提示匹配
- 直连模式暂不支持 `ProxyCommand`
- review 目前使用 OpenAI 兼容的 `chat/completions` 风格接口

## 开发

格式化：

```bash
cargo fmt --all
```

测试：

```bash
cargo test
```
