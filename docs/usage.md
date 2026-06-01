# 使用文档

## 安装

### 从源码构建

```bash
# 依赖：Rust 工具链、protoc
cargo build --release
```

生成二进制：`target/release/rhop` 和 `target/release/rhopd`

### 从 GitHub Release 下载

每次推送 `v*` tag 会自动发布以下平台的二进制：
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

### Docker

```bash
docker build -t rhopd:latest .
docker run --rm -p 2222:2222 -v /etc/rhop:/etc/rhop rhopd:latest
```

## 快速开始

### 零配置运行

只要 `~/.ssh/config` 中有目标主机的配置，即可直接使用：

```bash
# daemon 会自动启动
rhop exec 192.0.2.163 hostname
```

### 使用 server.toml

创建 `~/.rhop/server.toml`：

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"

[servers.web1]
host = "10.0.1.10"
user = "deploy"

[servers.db1]
host = "10.0.1.20"
user = "dba"
password = "secret"
```

然后按别名执行：

```bash
rhop exec web1 -- uname -a
```

## 命令参考

### 执行远程命令

```bash
# 基本用法
rhop exec <target> -- <command> [args...]

# 分配 PTY（颜色输出、交互程序）
rhop exec --pty <target> -- vim README.md

# 禁用 PTY
rhop exec --no-pty <target> -- cat /etc/hosts

# 转发 stdin
rhop exec --stdin <target> -- bash < script.sh

# 设置超时
rhop exec --timeout 30s <target> -- long-running-task

# 显式指定 jump host
rhop exec ali-rhopd:web1 -- hostname
```

### 交互模式

当满足以下条件时自动激活：
- `--pty` 已设置
- stdin 是 TTY
- stdout 是 TTY

```bash
# 自动进入交互模式
rhop exec --pty host1 -- vim README.md
rhop exec --pty host1 -- htop
rhop exec --pty host1 -- bash
```

交互模式特性：
- 终端自动进入 raw mode
- 所有按键实时转发到远程
- 窗口大小变化自动同步
- 退出时终端自动恢复

### 文件复制

```bash
# 上传
rhop cp local.txt host1:/tmp/

# 下载
rhop cp host1:/etc/hosts ./hosts

# 递归复制目录
rhop cp -r ./project host1:/opt/
```

### 服务器列表

```bash
# 列出所有可达服务器（本地 + 各 jump host）
rhop ls

# 强制刷新缓存
rhop ls --refresh
```

### Daemon 管理

```bash
# 查看状态
rhop status

# 手动启动
rhop daemon start
rhop daemon start --config ~/.rhop/config.toml --log-level debug

# 停止
rhop daemon stop

# 重启（继承上次启动参数）
rhop daemon restart
```

### Jump Host 管理

```bash
# 添加 rhopd jump host
rhop remote connect prod rhop@bastion.example.com:2222

# 添加时指定 identity file
rhop remote connect prod rhop@bastion.example.com:2222 --identity-file ~/.ssh/id_ed25519

# 列出已配置的 jump hosts
rhop remote list

# 移除
rhop remote remove prod
```

## 配置

### 配置文件位置

- 主配置：`~/.rhop/config.toml`
- 服务器清单：`~/.rhop/server.toml`（路径可在主配置中修改）
- 已知主机：`~/.rhop/known_hosts`

### 主配置 (`config.toml`)

```toml
[server]
log_path = "/var/log/rhopd.log"
log_level = "info"

[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "rhop"
host_key_path = "~/.rhop/host_key"
authorized_keys_path = "~/.rhop/authorized_keys"

[ssh]
server_config_path = "~/.rhop/server.toml"
fallback = ["local", "prod-rhopd"]
pty = true
connect_timeout = "10s"
keepalive_interval = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

# Jump Hosts
[[jump_hosts]]
name = "prod-rhopd"
kind = "rhopd"
address = "rhop@bastion.example.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.rhop/known_hosts"

[[jump_hosts]]
name = "corp-jump"
kind = "jumpserver"
host = "jumpserver.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
menu_prompt_contains = "Opt"
mfa_prompt_contains = "MFA"
shell_prompt_suffixes = ["$ ", "# "]
[jump_hosts.mfa]
totp_secret_base32 = "YOUR_SECRET"
digits = 6
period = 30
digest = "sha1"

# 命令审查（可选）
[review]
enable = true
endpoint = "https://api.openai.com/v1/chat/completions"
model = "gpt-4.1-mini"
timeout = "10s"
failure_action = "deny"

[review.fast_allowlist]
enable = true
commands = ["ls", "ls *", "cat *", "grep *"]

[review.policy]
safe = "allow"
risky = "confirm"
dangerous = "deny"
```

### 服务器清单 (`server.toml`)

```toml
[defaults]
identity_file = "~/.ssh/id_ed25519"

[servers.web1]
host = "10.0.1.10"
port = 22
user = "deploy"

[servers.db1]
host = "10.0.1.20"
user = "dba"
password = "secret"
```

认证优先级：`password > identity_file > defaults.identity_file`

## 目标解析

### 解析规则

| 格式 | 示例 | 含义 |
|------|------|------|
| `jump:server` | `prod:web1` | 显式通过 prod 跳板访问 web1 |
| `server_alias` | `web1` | 在所有 source 中查找（需唯一） |
| `host_or_ip` | `10.0.1.10` | IP 推导 + fallback |

### IP 推导

主机名中的 IP 后缀会被自动提取：

```
foo-192-0-2-163  →  192.0.2.163
bar-192-168-1-1  →  192.168.1.1
```

### Fallback 顺序

`ssh.fallback` 定义了当 server.toml 未命中时的尝试顺序：

```toml
[ssh]
fallback = ["local", "prod-rhopd", "corp-jump"]
```

- `"local"` — 尝试 `~/.ssh/config` 直连
- `"<name>"` — 通过对应的 jump host 路由

## 部署 rhopd 到远程服务器

### 服务端配置

1. 部署二进制到服务器
2. 创建配置 `~/.rhop/config.toml`：

```toml
[server.remote]
enable = true
listen_addr = "0.0.0.0:2222"
user = "rhop"
host_key_path = "~/.rhop/host_key"
authorized_keys_path = "~/.rhop/authorized_keys"

[ssh]
server_config_path = "~/.rhop/server.toml"
```

3. 将客户端公钥添加到 `~/.rhop/authorized_keys`
4. 创建 `~/.rhop/server.toml` 定义可达目标
5. 启动：`rhop daemon start --config ~/.rhop/config.toml`

### 客户端配置

```toml
[[jump_hosts]]
name = "prod"
kind = "rhopd"
address = "rhop@your-server.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.rhop/known_hosts"

[ssh]
fallback = ["local", "prod"]
```

### 使用部署脚本

```bash
.kiro/skills/rhopd-deploy-debug/scripts/deploy.sh root@your-server.com
```

## 运行模式

### 自动启动（推荐）

执行命令时 daemon 自动启动，无需手动管理：

```bash
rhop exec web1 -- hostname  # daemon 不存在时自动拉起
```

### systemd

```bash
sudo install -m 0644 packaging/systemd/rhopd.service /etc/systemd/system/
sudo systemctl enable --now rhopd
```

systemd 模式下 daemon 标记为 `external`，`rhop daemon stop` 会被拒绝。

### Docker

```bash
docker run --rm -p 2222:2222 -v /etc/rhop:/etc/rhop rhopd:latest
```

## 连接池

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_connections_per_ip` | 10 | 单目标最大连接数 |
| `max_idle_time` | 10m | 空闲连接回收时间 |
| `connect_timeout` | 10s | 连接超时 |
| `keepalive_interval` | 30s | SSH keepalive 间隔 |

行为：
- 有空闲连接 → 复用
- 无空闲但未达上限 → 新建
- 达到上限 → 等待
- Transport error → 自动重连一次

## 命令审查

### 启用

```toml
[review]
enable = true
```

API key 通过环境变量提供：`RHOP_REVIEW_API_KEY` 或 `OPENAI_API_KEY`

### 两层过滤

1. **本地白名单**（零延迟）：

```toml
[review.fast_allowlist]
enable = true
commands = ["ls", "ls *", "cat *", "kubectl get *"]
```

规则：含 `*` 为通配匹配，否则精确匹配。

2. **LLM 审查**：复杂命令（含 `&&`、`||`、`$()`、`bash -c` 等）发送到 LLM。

### 策略

```toml
[review.policy]
safe = "allow"       # 直接执行
risky = "confirm"    # 需要用户确认
dangerous = "deny"   # 拒绝
```

## 故障排查

### Daemon 无法启动

```bash
# 检查是否已有进程
ps aux | grep rhopd

# 检查 socket
ls -la ~/.rhop/rhopd.sock

# 查看日志
tail -50 ~/.rhop/rhopd.log
```

### 连接失败

```bash
# 查看 daemon 状态和连接池
rhop status

# 检查目标解析
rhop exec --no-pty <target> -- echo ok

# 检查远程 daemon
ssh root@server "/root/rhop/rhop status"
```

### 交互模式问题

- 终端没恢复：`reset` 命令可手动恢复
- 不进入交互模式：确认 `--pty` 已设置且 stdin/stdout 都是 TTY
- 通过管道使用时自动降级为非交互模式
