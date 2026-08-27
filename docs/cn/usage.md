# 使用文档

## 安装

### 从源码构建

```bash
# 依赖：Rust 工具链、protoc
cargo build --release
```

生成二进制：`target/release/xho` 和 `target/release/xhod`

### 从 GitHub Release 下载

每次推送 `v*` tag 会自动发布以下平台的二进制：
- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

### Docker

```bash
docker build -t xhod:latest .
docker run --rm -p 2222:2222 -p 12222:12222 -v /etc/xho:/etc/xho xhod:latest
```

## 快速开始

### 零配置运行

只要 `~/.ssh/config` 中有目标主机的配置，即可直接使用：

```bash
# daemon 会自动启动
xho exec 192.0.2.163 hostname
```

### 使用 server.toml

创建 `~/.xho/server.toml`：

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
xho exec web1 -- uname -a
```

## 命令参考

### 全局选项

以下选项可用于所有子命令（放在子命令名之前）：

- `--output text|json` — 输出格式，默认 `text`；`json` 输出 NDJSON（每行一个 JSON 对象，便于脚本解析）
- `-y/--yes` — 自动确认审查提示，不再弹出交互式 `[y/N]` 询问

```bash
xho --output json ls
xho -y exec web1 -- hostname
```

### 执行远程命令

```bash
# 基本用法
xho exec <target> -- <command> [args...]

# 分配 PTY（颜色输出、交互程序）
xho exec --tty <target> -- vim README.md

# 禁用 PTY
xho exec --no-tty <target> -- cat /etc/hosts

# 转发 stdin
xho exec --stdin <target> -- bash < script.sh

# 设置超时
xho exec --timeout 30s <target> -- long-running-task

# 用指定 shell 包装命令（加载远程 rc / alias）
xho exec --shell zsh <target> -- ll

# 禁用 shell 包装
xho exec --no-shell <target> -- /bin/ls

# 显式指定 gateway 路由
xho exec remote-xhod:web1 -- hostname
```

### 交互模式

当满足以下条件时自动激活：
- `--tty` 已设置
- stdin 是 TTY
- stdout 是 TTY

```bash
# 自动进入交互模式
xho exec --tty host1 -- vim README.md
xho exec --tty host1 -- htop
xho exec --tty host1 -- bash
```

交互模式特性：
- 终端自动进入 raw mode
- 所有按键实时转发到远程
- 窗口大小变化自动同步
- 退出时终端自动恢复

### 文件复制

目的地遵循 scp 语义：目录目的地会把文件放到目录内部、沿用源文件名
（`host1:/opt/` + `./project` → `/opt/project`）。

```bash
# 上传
xho cp local.txt host1:/tmp/

# 下载
xho cp host1:/etc/hosts ./hosts

# 递归复制目录
xho cp -r ./project host1:/opt/

# 静默模式（隐藏进度条与非错误信息）
xho cp -q local.txt host1:/tmp/

# 设置超时
xho cp --timeout 60s -r ./project host1:/opt/

# 断点续传（双向单文件）：中断后保留已传部分，
# 重跑同一命令即从断点继续，不再从头重来
xho cp --resume big.tar.gz host1:/data/big.tar.gz
```

续传细节（`--resume/-c`，按次开启；不带该标志时失败的复制会清理残留部分文件）：

- 下载保留 `<dest>.part` 加 `.meta` sidecar；上传保留远端临时文件直到完成。传输成功后原子发布。
- 续传前 daemon 校验远端部分文件大小，CLI 再比对部分数据全量 sha256 与本地源同长度前缀——追加生长的源能正确续传，仅共享旧头部的改写源会自动从头重传而不是静默拼出损坏文件。
- 仅限单文件；递归目录复制重跑即重传。

### 服务器列表

```bash
# 列出所有可达服务器（本地 + 各 jump host）
xho ls

# 强制刷新缓存
xho ls --refresh
```

### Daemon 管理

```bash
# 查看状态
xho status

# 手动启动
xho daemon start
xho daemon start --config ~/.xho/config.toml --log-level debug

# 停止
xho daemon stop

# 重启（继承上次启动参数）
xho daemon restart
```

### Jump Host 管理

```bash
# 添加 xhod jump host
xho host add prod xho@bastion.example.com:12222

# 添加时指定 identity file
xho host add prod xho@bastion.example.com:12222 --identity-file ~/.ssh/id_ed25519

# 指定 known_hosts 文件
xho host add prod xho@bastion.example.com:12222 --known-hosts ~/.xho/known_hosts

# 自动接受首次连接的未知主机 key（与 --fingerprint 互斥）
xho host add prod xho@bastion.example.com:12222 --accept-new-host-key

# 或显式校验指定指纹（与 --accept-new-host-key 互斥）
xho host add prod xho@bastion.example.com:12222 --fingerprint SHA256:abcdef...

# 列出已配置的 jump hosts
xho host list

# 移除
xho host remove prod
```

### Token 管理

`xho token` 子命令管理远端 daemon 接受的短期 token。**必须在 xhod 所在的那台机器上运行**（走本地 socket）：

```bash
# 生成 token（默认 5 分钟、一次性消费）
xho token gen

# 自定义 TTL、可重复使用、打标签
xho token gen --ttl 1h --reusable --label ci-runner

# 列出所有有效 token（前缀、过期时间、是否一次性、是否已消费、标签）
xho token list

# 按 8 字符前缀或完整 token 失效
xho token invalid <prefix>
```

token 仅保存在 daemon 内存中，重启即失效；`[server.remote].bootstrap_token` 是长期兜底（推荐用 `xho secret set bootstrap_token` 存进 vault 后写 `bootstrap_token = "vault:bootstrap_token"` 引用）。

#### 自动注册公钥到远端 daemon

`xho host add --token <T>` / `xho host login --token <T>` 会用 token 作为 SSH password 登入远端 daemon，然后调 `BootstrapAuthorize` RPC 让 daemon 把本地 `<identity_file>.pub` 追加进 `authorized_keys` —— 免去手动 `cat >> authorized_keys`。

```bash
# 1. 在 xhod 所在主机生成 token
xho token gen --ttl 5m

# 2. 客户端：带 token 添加 gateway，自动完成公钥注册
xho host add prod xho@bastion.example.com:12222 --token <TOKEN>
# 不带 --token 则交互提示；空输入跳过 bootstrap（仅信任 host key 并写入 config）

# 3. authorized_keys 被清空、或换客户端机器后，对已配置的 gateway 重新注册
xho host login prod --token <TOKEN>
xho host login prod                   # 交互提示输入 token
```

### 密钥管理

```bash
# 一键加密 config.toml + server.toml 中的明文密钥（详见「密钥管理」一节）
xho secret encrypt

# 预览改动
xho secret encrypt --dry-run

# 录入 / 列出 / 轮换
xho secret set server.db1.password
xho secret list
xho secret rekey --old ~/.ssh/id_ed25519 --new ~/.ssh/id_new
```

## 配置

### 配置文件位置

- 主配置：`~/.xho/config.toml`
- 服务器清单：`~/.xho/server.toml`（路径可在主配置中修改）
- 已知主机：`~/.xho/known_hosts`

### 主配置 (`config.toml`)

```toml
[server]
log_path = "/var/log/xhod.log"
log_level = "info"

[server.remote]
enable = true
# 控制面：daemon↔daemon RPC + 反向代理 + OpenSession（v0.4.0：2222 → 12222）
listen_addr = "0.0.0.0:12222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"
# 可选：长期 token，SSH password auth 兜底接受（动态 token 不命中时）
# 支持明文或 vault:/env:/file: 引用；不写则只接受 `xho token gen` 签发的短期 token
# bootstrap_token = "vault:bootstrap_token"

[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "prod-xhod"]
pty = true
connect_timeout = "10s"
keepalive_interval = "30s"
max_idle_time = "10m"
max_connections_per_ip = 10

# Jump Hosts
[[gateways]]
name = "prod-xhod"
kind = "xhod"
address = "xho@bastion.example.com:12222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[[gateways]]
name = "corp-jump"
kind = "jumpserver"
host = "jumpserver.example.com"
port = 20221
user = "user@example.com"
identity_file = "~/.ssh/id_rsa"
totp_secret_base32 = "YOUR_SECRET"
totp_digits = 6
totp_period = 30
# 抑制目标机的 shell 历史，避免 xho 自身的 exec/copy 命令污染 ~/.bash_history（默认 true）。
# 设为 false 可保留历史。
suppress_history = true

# 命令审查（可选）——共享 LLM 连接配置 + 按操作类型的子表。
# 默认全部关闭；完整的 policy / allowlist 选项见「命令审查 (AI)」一节。
[review]
endpoint = "https://api.openai.com/v1/chat/completions"
model = "gpt-4.1-mini"
timeout = "10s"
failure_action = "deny"

[review.exec]
enable = true
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
password = "vault:server.db1.password"   # 密钥引用，而非明文
```

认证优先级：`password > identity_file > defaults.identity_file`

`password` 既可以是明文，也可以是密钥引用（见下节「密钥管理」），推荐用后者。

## 密钥管理

密码、TOTP 密钥、API key 等敏感信息不必以明文存放在配置文件里。所有密钥字段都可以写成一个**引用**，daemon 在真正用到时才解析成明文。

### 引用语法

| 前缀 | 示例 | 含义 |
|------|------|------|
| `env:` | `env:DB_PASSWORD` | 从环境变量读取 |
| `file:` | `file:/run/secrets/db` | 从文件读取（适配 systemd LoadCredential、docker/k8s secrets） |
| `vault:` | `vault:server.db1.password` | 从本地加密库解密（默认 `<config 目录>/secrets`） |
| 无前缀 | `secret` | 视为明文（兼容旧配置），使用时会打告警 |

支持引用的字段：`server.toml` 的 `servers.*.password`；`config.toml` 的 jumpserver `totp_secret_base32`、direct gateway `password`、`review.api_key` 及 `review.headers.*`。

### 加密库（vault）

vault 把密文存在 config 文件**同目录下的 `secrets`**（权限 0600），用 XChaCha20-Poly1305 加密。加密密钥**不单独保存**，而是从一个 SSH 私钥经 HKDF-SHA256 派生而来：

- vault 默认位置 = config 文件所在目录 + `secrets`。本地用户 config 在 `~/.xho/config.toml`，则 vault 在 `~/.xho/secrets`；docker / systemd 用 `--config /etc/xho/config.toml` 启动，则 vault 自动落在 `/etc/xho/secrets`，随挂载目录一起持久化。可用 `[secret].vault_path` 显式覆盖。
- 使用的私钥由 `[secret].key_source` 指定；未设置时默认用 daemon 自己的 SSH 主机密钥 `[server.remote].host_key_path`（开了 remote 就必然存在、未加密、daemon 自己持有，所以**绝大多数部署零配置即可用 vault**）；remote 未开时才回退到 `server.toml` 的 `[defaults].identity_file`。
- 该私钥必须是**未加密的**（无 passphrase），否则 daemon 无法无人值守地加载它。
- vault 文件头记录所用私钥的指纹，换了私钥会明确报错并提示 `rekey`。

```toml
# config.toml（均可选；remote 启用时通常整段都不用写）
[secret]
# vault_path 不写则用 <config 目录>/secrets
# vault_path = "/etc/xho/secrets"
# key_source 不写则默认用 [server.remote].host_key_path；此处仅作显式覆盖
# key_source = "/etc/xho/host_key"
```

### 命令

所有 `xho secret` 操作都是**本地文件操作**，不经过 daemon。要管理哪台机器的配置，就在那台机器上执行（本地直接跑，远程则 SSH 上去跑）。

默认操作 `~/.xho/config.toml`。docker / systemd 部署在 `/etc/xho` 时，用 `--config` 指向那份配置（vault 会随之落在 `/etc/xho/secrets`）：

```bash
# 一键把 config.toml + server.toml 中所有明文密钥加密进 vault，
# 并就地替换为 vault: 引用（保留注释与格式，先备份 .bak）
xho secret encrypt

# 预览将要做的改动，不写文件
xho secret encrypt --dry-run

# 交互式录入单条密钥（输入不回显）
xho secret set server.db1.password

# 列出 vault 中的条目名（不显示明文）
xho secret list

# 更换派生私钥时，把整个 vault 重新加密一遍
xho secret rekey --old ~/.ssh/id_ed25519 --new ~/.ssh/id_new

# 管理非默认位置的配置（docker / systemd / root 部署）
xho secret --config /etc/xho/config.toml encrypt
```

### 安全边界

vault 把密钥从配置文件里移除，可防止误提交进 git、被备份带走、被人随手 `cat`。但派生密钥所用的私钥与密文在**同一台机器**上——能读取该私钥的人即可解密，这与「能读私钥者本就能直接 SSH 登录」属同一等级，不会放大风险。若需要更强的隔离，应改用外部 KMS 或密钥托管服务。

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
fallback = ["local", "prod-xhod", "corp-jump"]
```

- `"local"` — 尝试 `~/.ssh/config` 直连
- `"<name>"` — 通过对应的 jump host 路由

## 部署 xhod 到远程服务器

### 服务端配置

1. 部署二进制到服务器
2. 创建配置 `~/.xho/config.toml`：

```toml
[server.remote]
enable = true
listen_addr = "0.0.0.0:12222"
user = "xho"
host_key_path = "~/.xho/host_key"
authorized_keys_path = "~/.xho/authorized_keys"

[ssh]
server_config_path = "~/.xho/server.toml"
```

3. **注册客户端公钥** —— 两种方式二选一：
   - **手动**：把客户端 `~/.ssh/id_ed25519.pub` 追加到 `~/.xho/authorized_keys`
   - **自动（推荐）**：本机运行 `xho token gen`，客户端用 `xho host add prod xho@your-server:12222 --token <T>` 添加 gateway 时会自动注册（详见「Token 自动注册公钥」）
4. 创建 `~/.xho/server.toml` 定义可达目标
5. 启动：`xho daemon start --config ~/.xho/config.toml`

### 客户端配置

```toml
[[gateways]]
name = "prod"
kind = "xhod"
address = "xho@your-server.com:12222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"

[ssh]
fallback = ["local", "prod"]
```

### 使用部署脚本

```bash
cargo build --release --bin xhod
scp target/release/xhod root@your-server.com:/usr/local/bin/xhod
```

## 运行模式

### 自动启动（推荐）

执行命令时 daemon 自动启动，无需手动管理：

```bash
xho exec web1 -- hostname  # daemon 不存在时自动拉起
```

### systemd

```bash
sudo install -m 0644 packaging/systemd/xhod.service /etc/systemd/system/
sudo systemctl enable --now xhod
```

systemd 模式下 daemon 标记为 `external`，`xho daemon stop` 会被拒绝。

### Docker

```bash
docker run --rm -p 2222:2222 -p 12222:12222 -v /etc/xho:/etc/xho xhod:latest
```

## 退出码

退出码是所有子命令的稳定契约：

| 码 | 含义 |
|----|------|
| 0 | 成功 |
| 1-123 | 远程命令自身退出码 |
| 124 | 超时（`--timeout` 到期） |
| 125 | xho / daemon 故障 |
| 126 | 认证或命令审查拒绝 |
| 127 | 目标不存在 |

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

## 命令审查 (AI)

基于 LLM 的安全审查，按操作类型分别配置。`[review]` 放共享的 LLM 连接配置；每种操作类型有自己的子表，独立开关、提示词、策略与黑白名单。**默认全部关闭**。

```toml
[review]
endpoint = "https://api.openai.com/v1/chat/completions"
model = "gpt-4.1-mini"
# api_key 接受密钥引用（vault:/env:/file:）或字面量；
# 不写时依次尝试环境变量 XHO_REVIEW_API_KEY、OPENAI_API_KEY
timeout = "10s"
failure_action = "deny"   # LLM 服务本身出错时的动作

# --- exec 审查 ---
[review.exec]
enable = true

[review.exec.fast_allowlist]
enable = true
commands = ["ls", "ls *", "cat *", "grep *", "kubectl get *"]

[review.exec.policy]
safe = "allow"       # 直接执行
risky = "confirm"    # 交互式 [y/N] 确认（--yes 可自动应答）
dangerous = "deny"

# --- copy 审查 ---
[review.copy]
enable = true
blocklist = [".ssh", ".aws", ".gnupg", ".kube", "/etc/shadow", "/etc/ssh"]
allowlist = ["/var/log/*", "/tmp/*"]

[review.copy.policy]
safe = "allow"
risky = "confirm"
dangerous = "deny"
```

过滤顺序：

1. **copy 黑白名单** — 对远端路径做模式匹配，命中即秒判（blocklist 拒绝、allowlist 放行），不经过 LLM。glob 模式，条目可匹配路径本身或源文件名。
2. **exec fast allowlist** — 对命令行做 glob 匹配（零延迟）；未命中的交给 LLM。
3. **LLM 审查** — 分类为 `safe | risky | dangerous`，再由 `[review.*.policy]` 映射为 `allow | confirm | deny`。策略还可以配 `warn`。

被拒绝的操作以退出码 `126` 失败，并透出审查理由。

## 审计日志

每个机器操作——exec、copy、OpenSession 隧道、透明代理——都会记录成一条 JSON-Lines 事件，包含：

- 调用方身份：对端地址、SSH 用户、公钥指纹
- 操作细节：目标、网关链、argv（exec）、路径（copy）
- 结果状态与事件 id / 时间戳对（RFC3339 + epoch ms）

**默认开启**；日志位于 `~/.xho/audit.jsonl`（root daemon 为 `/var/log/xho/audit.jsonl`）。它是独立的非阻塞 appender，与 tracing 调试日志完全解耦，同样跟随 SIGHUP 轮转。

```toml
[audit]
enabled = true                      # 设 false 整体关闭
path = "/var/log/xho/audit.jsonl"   # 可选覆盖
include_identity = true             # 记录调用方身份字段
```

## 故障排查

### Daemon 无法启动

```bash
# 检查是否已有进程
ps aux | grep xhod

# 检查 socket
ls -la ~/.xho/xhod.sock

# 查看日志
tail -50 ~/.xho/xhod.log
```

### 连接失败

```bash
# 查看 daemon 状态和连接池
xho status

# 检查目标解析
xho exec --no-tty <target> -- echo ok

# 检查远程 daemon
ssh root@server "/root/xho/xho status"
```

### 交互模式问题

- 终端没恢复：`reset` 命令可手动恢复
- 不进入交互模式：确认 `--tty` 已设置且 stdin/stdout 都是 TTY
- 审查确认提示即使 stdin 不是 TTY 也会从 stdin 读答案；管道关闭等价于空输入，即方括号里的默认项（`[y/N]`）
