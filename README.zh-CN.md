# Cross Host Ops

远程命令执行与文件复制工具。通过本地 daemon 管理 SSH 连接池，支持直连、跳板机、远程 xhod 三种路由方式到达目标服务器。

[English](README.md) | **中文**

## 特性

- **交互式 PTY** — 运行 vim、htop 等全屏程序，体验与原生 SSH 一致
- **连接池** — 按目标 IP 复用 SSH 连接，避免重复握手
- **多种跳板** — 直连 SSH、企业 jumpserver（MFA）、远程 xhod daemon
- **统一目标解析** — server.toml 别名、显式路由、IP 推导、fallback 链
- **命令审查** — 可选 LLM 安全审查，本地白名单 + AI 语义分析
- **文件复制** — `xho cp` 对齐 scp 语义，支持递归和 mode 保留
- **零配置可用** — 只要有 `~/.ssh/config`，无需任何配置文件

## 快速开始

```bash
# 构建
cargo build --release

# 执行远程命令（daemon 自动启动）
xho exec web1 -- hostname

# 交互模式（自动检测）
xho exec --tty host1 -- vim README.md

# 文件复制
xho cp local.txt host1:/tmp/

# 查看所有可达服务器
xho ls
```

## 架构概览

```
xho (CLI) ──Unix socket──▶ xhod (Daemon) ──Gateways──▶ End Target
```

- CLI 通过 gRPC over Unix socket 与本地 daemon 通信
- Daemon 管理连接池、目标解析、命令审查
- 三种 Gateway 类型完全互换：direct、jumpserver、xhod

详细架构设计见 [docs/cn/architecture.md](docs/cn/architecture.md)。

## 使用

```bash
# 基本执行
xho exec <target> -- <command> [args...]

# PTY 模式（颜色输出、交互程序）
xho exec --tty <target> -- ls --color

# 显式指定跳板路由
xho exec prod:web1 -- hostname

# Daemon 管理
xho status
xho daemon start --config ~/.xho/config.toml
xho daemon restart

# Gateway 管理
xho host add prod xho@bastion.example.com:2222
xho host list
```

完整使用说明见 [docs/cn/usage.md](docs/cn/usage.md)。

## 配置

程序无需配置文件即可运行。需要自定义时，创建 `~/.xho/config.toml`：

```toml
[ssh]
server_config_path = "~/.xho/server.toml"
fallback = ["local", "prod"]
pty = true

[[gateways]]
name = "prod"
kind = "xhod"
address = "xho@bastion.example.com:2222"
identity_file = "~/.ssh/id_ed25519"
known_hosts_path = "~/.xho/known_hosts"
```

完整配置示例见 [config.example.toml](config.example.toml)。

## 部署

### 本地使用

```bash
cargo build --release
# 二进制：target/release/xho, target/release/xhod
```

### 远程 xhod

```bash
# 使用部署脚本
cargo build --release --bin xhod
scp target/release/xhod root@your-server.com:/usr/local/bin/xhod
```

### systemd / Docker

```bash
# systemd
sudo install -m 0644 packaging/systemd/xhod.service /etc/systemd/system/
sudo systemctl enable --now xhod

# Docker
docker build -t xhod:latest .
docker run --rm -p 2222:2222 -v /etc/xho:/etc/xho xhod:latest
```

### GitHub Release

推送 `v*` tag 自动发布多平台二进制和 Docker 镜像。

## 开发

```bash
# 构建
cargo build

# 测试
cargo test

# 格式化
cargo fmt --all
```

## 文档

- [架构文档](docs/cn/architecture.md) — 系统设计、组件交互、数据流
- [使用文档](docs/cn/usage.md) — 安装、配置、命令参考、故障排查
- [配置示例](config.example.toml) — 完整配置项说明
- [服务器清单示例](server.example.toml) — server.toml 格式

## License

MIT
