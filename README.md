# Remote Hop

`Remote Hop` 是一个基于 Rust 实现的远端命令执行与文件复制工具。

它拆分为两个二进制：

- `rhop`：CLI 前端
- `rhopd`：本地 daemon 后端

CLI 通过 `gRPC over Unix socket` 与 daemon 通信。daemon 负责 SSH 连接管理、连接池、可选 jumpserver、可选 LLM 命令审查，以及远端输出流式转发。

## 功能

- CLI 用法：`rhop exec <target> <cmd> [arg...]`
- 前后端分离，使用 `gRPC over Unix socket` 通信
- 按目标 IP 复用 SSH 连接
- 没有空闲连接时自动新建连接
- 单 IP 连接数上限可配置，默认 `10`
- 空闲连接超时自动关闭
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

## 工作方式

1. `rhop exec` 接收目标和命令。
2. `rhop` 通过 `gRPC over Unix socket` 连接 `rhopd`。
3. 如果 `rhopd` 尚未运行，`rhop` 会自动以 `--daemon` 模式拉起它。
4. daemon 根据第一个参数推导目标 IP。
5. 如果该 IP 能命中 `~/.ssh/config` 的 `Host` 条目，则走直连 SSH。
6. 如果命不中且配置中启用了 jumpserver，则走 jumpserver。
7. 同一 IP 有空闲连接时优先复用。
8. 如果没有空闲连接，则在上限内新建连接。
9. 如果启用了 review，daemon 会先做命令审查。
10. 命令输出会实时流式返回到 CLI。

## 通信方式

前后端当前使用 `gRPC over Unix socket`。

- `Execute`：双向流 RPC
- `Status`：普通 unary RPC
- `ReloadConfig`：普通 unary RPC

`Execute` 流里：

- CLI 先发送开始执行请求
- daemon 回流 review、stdout、stderr、confirm、exit 等事件
- 如果需要确认，CLI 会在同一条流里回发确认消息

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

这里 `rhop` 会把 `foo-192-0-2-163` 推导成 `192.0.2.163`，再去匹配 `~/.ssh/config`，如果 daemon 没启动则自动启动，然后执行远端命令。

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
~/.config/rhop/config.toml
```

仓库中提供了一个完整但全部注释掉的示例：

- [config.example.toml](/home/admin/Projects/agora/arun/config.example.toml)

### 第 1 步：创建配置目录

```bash
mkdir -p ~/.config/rhop
```

### 第 2 步：复制示例配置

```bash
cp config.example.toml ~/.config/rhop/config.toml
```

### 第 3 步：只打开你需要的配置项

最常见的是：

- 开启 jumpserver
- 开启 review
- 修改 socket 路径
- 调整连接池上限和空闲超时

### 第 4 步：重载 daemon 配置

```bash
./target/debug/rhop reload-config
```

如果 daemon 还没有运行，那么第一次执行命令时会自动读取新配置。

## One By One：开启 jumpserver

只有当目标 IP 不能通过 `~/.ssh/config` 直连时，才需要启用 jumpserver。

### 第 1 步：打开 jumpserver 配置段

在 `~/.config/rhop/config.toml` 中取消注释并填写：

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

### 第 2 步：重载配置

```bash
./target/debug/rhop reload-config
```

### 第 3 步：执行一个未在 `~/.ssh/config` 中定义直连的目标

```bash
./target/debug/rhop exec foo-192-0-2-200 uname -a
```

这时 daemon 会：

- 连接 jumpserver
- 在需要时提交 MFA
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

### 第 3 步：重载配置

```bash
./target/debug/rhop reload-config
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

手动启动 daemon：

```bash
./target/debug/rhop daemon-start
```

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
./target/debug/rhopd -c ~/.config/rhop/config.toml --log-level debug
```

重载配置：

```bash
./target/debug/rhop reload-config
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
