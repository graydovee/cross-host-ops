# 更新日志

<!-- 维护规则：每次 commit 前在本文件与英文版 CHANGELOG.md 的最顶部开放分组中各追加一条条目，两份内容一一对应。
     每条条目必须是 Markdown 列表项：- yyyy-MM-dd [tag] 概括。
     每条用一两句话概括"改了什么、对使用者的直接影响"，不展开实现细节（细节写进 commit message）。
     本文件条目一律中文；英文版条目一律英文。tag 取值 feat | bug | refactor | docs，按变更实质归类。
     条目按发布版本分组（## v0.x.y 对应 git tag），已发布小节冻结；下一版未定稿时写入 ## v0.5.7。
     完整规则见 AGENTS.md 与 CHANGELOG.md 头部注释。 -->

## latest

- 2026-08-28 [bug] 隧道会话不再因客户端关闭 stdin 侧而提前结束：tunnel 驱动在上行结束后继续排空远端输出与退出状态，管道式 exec（-i）能拿到全部输出而不是空结果。
- 2026-08-28 [bug] 修复会话流式传输的最后一个互冻路径：direct SSH 驱动的通道写与读原先共用一个 select，远端程序停止读 stdin 而输出又未被消费时两个方向可能同时永久停摆（表现为 2222 代理 shell 在大输出后冻结，如 git log）；现在 direct 驱动读写各自独立任务，exec/交互/sftp 的 stdin 转发同样移入独立任务。
- 2026-08-28 [feat] 新增 `[reverse_proxy] workdir` 配置：daemon 本机会话（透明代理 shell、`xho exec`）从该目录启动，未配置时默认为 home 目录而非 daemon 的 cwd。
- 2026-08-27 [docs] 刷新文档、配置示例与 agent 协作约定以匹配当前行为。
- 2026-08-27 [docs] 将发布清单从 AGENTS.md 抽离为独立的 `xho-release` skill。

## v0.5.6

- 2026-08-27 [docs] 变更日志条目全部精简为简短概括，并拆分为中英文双份（`CHANGELOG.md` / `CHANGELOG.zh-CN.md`），README 各自链接对应语言版本；AGENTS.md 改为纯英文，并同步刷新自带 skill 与 README 以符合当前行为（`cp --resume`、scp 式目录目的地、jumpserver base64 传输）。
- 2026-08-27 [bug] 修复 jumpserver 路径下 `xho cp` 上传到目录目的地（如 `host:/tmp`）永久卡死的问题——目录目的地现按 scp 语义解析到目录内部，中继卡住或远端接收进程死亡时约 60 秒内快速失败并保留部分文件供 `--resume` 续传。
- 2026-08-27 [feat] 新增 `xho cp --resume/-c` 断点续传：单文件上传/下载中断后重跑同一命令即可从断点继续；以源文件 size/mtime 加部分数据完整 sha256 做校验，源发生变化则自动从头重传。
- 2026-08-27 [bug] jumpserver 文件传输不再中途卡死：堡垒机内容审计会楔住结构化裸二进制流，shell copy 载荷改为全部走换行分行的 base64；远端依赖降为 coreutils/busybox 的 `base64`/`head`/`tail`。
- 2026-08-26 [bug] CLI 不再掩盖复制失败：进度条显示真实字节数而非直接跳 100%，超时报错透出 daemon 的原始信息，失败的下载会清理残留的截断文件。
- 2026-08-25 [bug] 从根上消除隧道会话冻结：TargetSession 拆分为独立的 writer/stream 两半、每个方向各一个任务，流控背压不再造成两端死锁；同时移除早前的 30 秒发送超时变通方案。
- 2026-08-25 [bug] 修复经 xhod 网关的 2222 透明代理交互会话偶发永久冻死：跨流有界发送与读循环共用同一个 select，输出突发时相互死锁。
- 2026-08-24 [docs] 变更日志改为按版本分组的 Markdown 列表项渲染；AGENTS.md 相应更新约定。

## v0.5.5

- 2026-08-21 [bug] 反向代理隧道应用配置的 SSH keepalive，休眠或切换网络后的半开连接能在约 90 秒内检出，节点自动重新注册。
- 2026-08-17 [bug] 修复流式操作（`exec -i`、`cp`）偶发卡死与 stdin 丢失：控制与 stdin 统一承载在一条有序 channel 上；卡死的会话不再泄漏连接池租约。
- 2026-08-07 [refactor] 用 `-y/--yes`（自动确认审查提示）替换已无作用的 `--non-interactive` 标志。
- 2026-08-06 [refactor] 将 AI 命令审查与审计日志统一进单一 oversight 模块；cp 接入可配 allowlist/blocklist 的审查，所有机器操作写入 JSON-Lines 审计记录。
- 2026-08-06 [bug] `list_servers` 跳过没有 LIST 能力的网关（如 jumpserver 堡垒机），不再把不支持的后端返回给客户端。
