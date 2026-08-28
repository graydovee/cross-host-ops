# Changelog

<!-- Maintenance rules: before every commit, prepend one entry to BOTH this file
     and its Chinese mirror CHANGELOG.zh-CN.md, keeping the two in sync.
     Each entry MUST be a markdown list item so GitHub renders one line per entry:
     - yyyy-MM-dd [tag] summary
     Keep each entry to one or two sentences: state WHAT changed and the user-visible
     effect; no implementation detail (that belongs in commit messages).
     English entries in this file, Chinese entries in the mirror.
     tag values: feat | bug | refactor | docs
     Classify by the substance of the change, not the commit prefix:
       new feature or capability (incl. independently added tests/tools) -> [feat]
       bug fix -> [bug]
       behavior-preserving refactor, internal cleanup, dependency upgrade, CI/build adjustment -> [refactor]
       documentation change -> [docs]
     Tests/formatting/minor cleanup bundled with a fix/feat are NOT logged separately; fold them into that entry.
     Entries are grouped per released version (## v0.x.y matching a git tag); released sections are frozen.
     While the next tag is undecided, write new entries under ## latest and rename it when the tag is cut. -->

## latest

- 2026-08-28 [bug] tunnel sessions no longer end early when the client closes its stdin side: the tunnel driver kept draining remote output and the exit status after the uplink finishes, so piped exec (-i) returns all output instead of returning an empty result
- 2026-08-28 [bug] fix the last mutual-freeze path in session streaming: the direct SSH driver kept channel writes and reads in one select, so a remote program that stopped reading stdin while its output was unconsumed could park both directions forever (seen as a 2222 proxy shell freezing after big output, e.g. git log); writes and reads now run as independent tasks in the direct driver, and exec/interactive/sftp stdin forwarding moved to dedicated tasks to match
- 2026-08-28 [feat] Added `[reverse_proxy] workdir`: sessions on the daemon host (transparent-proxy shells, `xho exec`) now start there, defaulting to the home directory instead of the daemon's cwd.
- 2026-08-27 [docs] Refreshed guides, config examples, and agent conventions to match current behavior.
- 2026-08-27 [docs] Extracted the release checklist from AGENTS.md into a dedicated `xho-release` skill.

## v0.5.6

- 2026-08-27 [docs] Condensed every changelog entry into short summaries and split the changelog into English/Chinese mirrors (`CHANGELOG.md` / `CHANGELOG.zh-CN.md`) linked from each README; rewrote AGENTS.md in English and refreshed the bundled skills and READMEs to match current behavior (`cp --resume`, scp-style destinations, jumpserver base64 transport).
- 2026-08-27 [bug] Fixed `xho cp` uploads to a directory destination (e.g. `host:/tmp`) hanging forever over the jumpserver path — directory destinations now resolve scp-style, and a stalled relay or dead remote receiver fails fast (~60s), leaving the partial for `--resume`.
- 2026-08-27 [feat] Added `xho cp --resume/-c`: interrupted single-file transfers in both directions can be continued by re-running the same command; resume is validated against source size/mtime plus a full sha256 of the partial data, so a changed source restarts cleanly.
- 2026-08-27 [bug] Jumpserver transfers no longer stall mid-stream: bastion content inspection wedges on structured raw binary, so shell-copy payloads now travel exclusively as line-wrapped base64; remote dependencies drop to coreutils/busybox `base64`/`head`/`tail`.
- 2026-08-26 [bug] The CLI no longer masks copy failures: progress bars report real bytes instead of snapping to 100%, timeout errors surface the daemon's message, and failed downloads remove their truncated file.
- 2026-08-25 [bug] Eliminated tunneled-session freezes at the root by splitting TargetSession into independent writer/stream halves with one task per direction; removed the earlier 30s send-timeout workaround.
- 2026-08-25 [bug] Fixed occasional permanent freeze of tunneled interactive sessions (2222 proxy via xhod gateway): bounded cross-stream sends shared a select with the read loop and deadlocked under output bursts.
- 2026-08-24 [docs] Changelog switched to per-version sections rendered as separate markdown list items; AGENTS.md conventions updated to match.

## v0.5.5

- 2026-08-21 [bug] Reverse proxy tunnels apply the configured SSH keepalive, so half-open connections after sleep or network switch are detected within ~90s and nodes re-register automatically.
- 2026-08-17 [bug] Fixed intermittent hangs and dropped stdin in streaming operations (`exec -i`, `cp`) by carrying control and stdin on one ordered channel; hung sessions no longer leak pool leases.
- 2026-08-07 [refactor] Replaced the dead `--non-interactive` flag with `-y/--yes`, which auto-confirms review prompts in exec/cp.
- 2026-08-06 [refactor] Unified AI command review and audit logging into a single oversight module; cp now passes through review with a configurable allowlist/blocklist, and every machine operation writes a JSON-Lines audit record.
- 2026-08-06 [bug] `list_servers` skips gateways without the LIST capability (e.g. jumpserver bastions) instead of returning unsupported sources.
