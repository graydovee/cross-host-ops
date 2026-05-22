# Requirements Document

## Introduction

Remote Hop currently exposes two CLI client modes (`local` and `remote`) that take fundamentally different paths through the system. In local mode, `rhop` talks to a local `rhopd` daemon, which then opens SSH connections to end targets. In remote mode, `rhop` bypasses the local daemon and opens its own SSH+gRPC subsystem connection to a remote `rhopd`, which then opens SSH connections to end targets.

This split breaks the project's stated architectural principle, which is that the daemon owns and reuses connections. It also produces user-visible bugs: in remote mode, `rhop cp ~/local/path host:remote/path` fails because the *remote* daemon tries to `stat` `~/local/path` on its own filesystem.

This refactor restructures Remote Hop around a single, uniform pipeline:

1. `rhop` (CLI) always talks to the local `rhopd` over a Unix socket.
2. The local `rhopd` owns local-side state (local files, working directory, local home expansion, prompts to the human).
3. The path from the local `rhopd` to an end target is composed of zero or more *Jump Hosts*. `direct` SSH, an interactive `jumpserver`, and another `rhopd` are all interchangeable Jump Host implementations.
4. Connection reuse happens at every hop: the local daemon pools connections to Jump Hosts, and each Jump Host (where applicable) pools its outgoing connections to end targets.

In addition to the architectural unification, this refactor establishes a contract for non-interactive use of `rhop` so that AI agents, CI pipelines, and other automated callers can drive `rhop` reliably. Concretely: structured output (`--output json` NDJSON), an explicit exit-code taxonomy, prompt-free operation under `--non-interactive`, env-var credential channels, deterministic PTY decisions, and clean stdout/stderr separation.

Because the user has explicitly opted out of backward compatibility, the refactor may freely remove the legacy `rhop remote enable/disable` flow and the `ClientMode::Remote` code path, replacing them with a Jump Host configuration model.

This refactor also makes `rhop` reliably callable from non-interactive contexts such as AI agents, CI pipelines, and automation scripts. In those contexts there is no controlling TTY, no human available to answer host-key, password, MFA, or review-confirmation prompts, and the consumer of the CLI's stdout is another program rather than a terminal. The CLI surface is therefore extended with a structured output mode, a strict non-interactive mode, an explicit PTY-control policy, deterministic exit-code semantics, a timeout, env-var-based credentials, and machine-readable capability discovery, so an agent can drive `rhop` without ever blocking on stdin and without parsing prose.

## Glossary

- **Rhop_Cli**: The user-facing `rhop` binary that parses CLI arguments and forwards requests over a Unix socket.
- **Local_Daemon**: A `rhopd` process running on the same machine as `Rhop_Cli`, accepting gRPC over a Unix socket. It is the only daemon `Rhop_Cli` ever talks to.
- **Remote_Daemon**: A `rhopd` process running on another machine, reachable from a `Local_Daemon` via SSH and the `rhop-rpc` subsystem. It exposes the same gRPC service as `Local_Daemon`.
- **Jump_Host**: An abstract intermediate hop used by a daemon to reach an `End_Target`. Concrete kinds are `direct` (no jump), `jumpserver` (interactive menu shell with optional MFA), and `rhopd` (a `Remote_Daemon` reached over the SSH `rhop-rpc` subsystem).
- **End_Target**: The host on which user commands ultimately run, or which holds the remote side of a copy operation.
- **Target_Route**: An ordered list of Jump Hosts followed by an `End_Target`, produced by the resolver from configuration plus a CLI target string.
- **Connection_Pool**: A cache of live transport connections keyed by `Target_Route`. Every daemon (local or remote) owns its own pool.
- **Copy_Operation**: A single `rhop cp` invocation, with a `Local_Side` (a path on the machine running `Rhop_Cli` / `Local_Daemon`) and a `Remote_Side` (a path on the `End_Target`).
- **Auth_Prompt**: A request from any daemon to `Rhop_Cli` for human input (TOTP, password, host-key trust, command confirmation).
- **Client_Config**: The `~/.rhop/client.toml` file describing how `Rhop_Cli` reaches `Local_Daemon`.
- **Daemon_Config**: The `~/.rhop/config.toml` file describing how a `rhopd` daemon listens, resolves targets, and reaches Jump Hosts.
- **Jump_Host_Alias**: A user-chosen, unique short name that identifies a single Jump_Host entry in the Daemon_Config and that Rhop_Cli accepts as a prefix when disambiguating End_Target names.
- **Reserved_Alias**: A Jump_Host_Alias value that is reserved by the system and that the user cannot assign to a Jump_Host entry. The current Reserved_Alias set is `{ "local" }`.
- **Server_List_Source**: A logical origin of End_Target entries returned by `rhop server list`, which is either the Local_Daemon's own `server.toml` (named `local`) or a configured Jump_Host_Alias whose Jump_Host implements the `list_servers` trait method.
- **Unsupported_Capability**: A documented error returned by an optional Jump_Host trait method when the concrete Jump_Host kind does not implement that method.
- **Output_Format**: One of the values `text` or `json` controlled by the global `--output` flag. In `text` mode, Rhop_Cli emits human-readable lines on stdout/stderr. In `json` mode, Rhop_Cli emits NDJSON event records (one JSON object per line) on stdout.
- **Non_Interactive_Mode**: A Rhop_Cli mode enabled by the global `--non-interactive` flag, in which no prompt is ever read from stdin and any prompt-requiring code path produces an error event.
- **Effective_Pty_Decision**: The boolean PTY-allocation decision computed by combining the Rhop_Cli's `--pty`/`--no-pty` flags, the Daemon_Config's `ssh.auto_pty_detect` and `ssh.pty`, and the Rhop_Cli's reported `stdout_is_tty`, by the priority rule defined in Requirement 17.
- **Output_Format**: One of the values `text` or `json` controlled by the global `--output` flag. In `text` mode, Rhop_Cli emits human-readable lines on stdout/stderr. In `json` mode, Rhop_Cli emits NDJSON event records (one JSON object per line) on stdout.
- **Non_Interactive_Mode**: A Rhop_Cli mode enabled by the global `--non-interactive` flag, in which no prompt is ever read from stdin and any prompt-requiring code path produces an error event.
- **Effective_Pty_Decision**: The boolean PTY-allocation decision computed by combining the Rhop_Cli's `--pty`/`--no-pty` flags, the Daemon_Config's `ssh.auto_pty_detect` and `ssh.pty`, and the Rhop_Cli's reported `stdout_is_tty`, by the priority rule defined in Requirement 17.

## Requirements

### Requirement 1: Single CLI Entry Point

**User Story:** As a developer using `rhop`, I want every command I run to go through one consistent path, so that command behavior does not silently change based on which daemon is "active".

#### Acceptance Criteria

1. THE Rhop_Cli SHALL connect only to the Local_Daemon over a Unix socket for every subcommand that talks to a daemon.
2. WHEN the Rhop_Cli is invoked and the Local_Daemon is not running, THE Rhop_Cli SHALL spawn the Local_Daemon if `local.auto_start` is true and then connect to the Local_Daemon.
3. THE Rhop_Cli SHALL NOT open any SSH connection on its own and SHALL NOT contain SSH client code paths.
4. THE Rhop_Cli SHALL NOT contain a `ClientMode` enum and SHALL NOT branch based on local-versus-remote daemon.
5. IF the Local_Daemon socket is unreachable and `local.auto_start` is false, THEN THE Rhop_Cli SHALL exit with a non-zero status and SHALL print a message that names the configured socket path.
6. IF the Local_Daemon socket exists but the connection fails because of permissions or a stale socket file, THEN THE Rhop_Cli SHALL exit with a non-zero status and SHALL print a message that names the configured socket path, and THE Rhop_Cli SHALL NOT attempt to restart the Local_Daemon.

### Requirement 2: Local Filesystem Authority

**User Story:** As a developer running `rhop cp` in any configuration, I want local paths in my command line to refer to files on my own machine, so that `cp` works the same whether or not a Remote_Daemon is involved.

#### Acceptance Criteria

1. WHEN the Rhop_Cli sends a Copy_Operation to the Local_Daemon, THE Local_Daemon SHALL interpret the `Local_Side` path against its own filesystem and its own `$HOME`.
2. WHEN a Copy_Operation has direction Upload, THE Local_Daemon SHALL read the `Local_Side` bytes from its own filesystem before any data is sent toward the End_Target.
3. WHEN a Copy_Operation has direction Download, THE Local_Daemon SHALL receive the data on its own process and SHALL write the bytes to the `Local_Side` on its own filesystem rather than asking any Jump_Host to perform the local write.
4. WHERE the Target_Route includes a Remote_Daemon Jump_Host, THE Local_Daemon SHALL NOT delegate `Local_Side` filesystem reads or writes to the Remote_Daemon, and THE Local_Daemon SHALL still perform the local-side filesystem operation that matches the Copy_Operation's direction.
5. WHERE the Target_Route includes a Remote_Daemon Jump_Host, THE Remote_Daemon SHALL only perform `Remote_Side` filesystem operations on the End_Target.
6. WHEN a Copy_Operation succeeds end-to-end, THE Local_Daemon SHALL produce the same byte content on the destination side as a `scp` of the same source and destination would produce.
7. WHERE `copy.preserve_mode` is true and a Copy_Operation succeeds, THE destination file SHALL have the same Unix permission bits as the source file.

### Requirement 3: Unified Jump Host Abstraction

**User Story:** As a maintainer adding a new way to reach hosts (for example, a future bastion or proxy implementation), I want one place to plug it in, so that I do not have to special-case every consumer of the connection layer.

#### Acceptance Criteria

1. THE Local_Daemon SHALL expose a single trait that represents a Jump_Host and that defines the operations `exec`, `copy`, `tui_shell`, and `list_servers`.
2. THE Jump_Host trait SHALL require every concrete implementation to implement `exec` and `copy`.
3. THE Jump_Host trait SHALL define `tui_shell` and `list_servers` as optional methods that have a default implementation returning a documented Unsupported_Capability error, so that a new Jump_Host kind compiles without writing those methods.
4. WHEN any Jump_Host trait method is invoked on a concrete Jump_Host that has not implemented the method, THE Jump_Host SHALL return an Unsupported_Capability error whose message names the Jump_Host kind, the Jump_Host_Alias, and the method name.
5. THE Local_Daemon SHALL provide concrete Jump_Host implementations for `direct`, `jumpserver`, and `rhopd`.
6. THE `direct` and `jumpserver` Jump_Host implementations SHALL implement `exec` and `copy` and MAY return Unsupported_Capability for `tui_shell` and `list_servers`.
7. THE Connection_Pool SHALL store and reuse Jump_Host connections without inspecting the concrete Jump_Host kind, and THE Connection_Pool MAY hold a Jump_Host connection in its pool even when no caller is currently waiting for that connection.
8. WHEN the resolver produces a Target_Route, THE Target_Route SHALL identify each hop by a Jump_Host kind and a Jump_Host_Alias.
9. THE Local_Daemon SHALL select the connect path solely from the Target_Route and SHALL NOT branch on a global `ClientMode` flag.

### Requirement 4: Rhopd as a Jump Host

**User Story:** As an operator, I want to declare a Remote_Daemon as a Jump_Host in my config, so that my local daemon reaches end targets through it just like it would through a jumpserver.

#### Acceptance Criteria

1. THE Daemon_Config SHALL allow declaring named `rhopd` Jump_Host entries, each containing at least a Jump_Host_Alias, an SSH address, an SSH user, an identity file path, and a known-hosts path.
2. WHERE the resolver matches an End_Target to a `rhopd` Jump_Host, THE Local_Daemon SHALL open a single gRPC channel to the Remote_Daemon over the `rhop-rpc` SSH subsystem.
3. THE Local_Daemon SHALL reuse one gRPC channel per `rhopd` Jump_Host entry across multiple `exec` and `cp` calls until the channel is closed or pruned.
4. THE `rhopd` Jump_Host implementation SHALL implement all four Jump_Host trait methods: `exec`, `copy`, `tui_shell`, and `list_servers`.
5. WHERE the `tui_shell` method is invoked on a `rhopd` Jump_Host before the interactive shell session feature has shipped, THE `rhopd` Jump_Host SHALL return an Unsupported_Capability error whose message names the `rhopd` kind and the `tui_shell` method.
6. WHEN the Local_Daemon needs the End_Target to run a command, THE Local_Daemon SHALL invoke the Remote_Daemon's `Execute` RPC with the End_Target name and argv that the Remote_Daemon's own resolver would accept.
7. WHEN the Local_Daemon needs the End_Target to participate in a Copy_Operation, THE Local_Daemon SHALL stream the `Remote_Side` half of the operation to the Remote_Daemon's `Copy` RPC and SHALL keep the `Local_Side` half on its own filesystem.
8. WHEN the Local_Daemon invokes the `list_servers` trait method on a `rhopd` Jump_Host, THE `rhopd` Jump_Host SHALL call the Remote_Daemon's server-list RPC over the same pooled gRPC channel and SHALL return the End_Target entries reported by the Remote_Daemon.
9. IF the Remote_Daemon's gRPC channel returns a transport-level error during a request, THEN THE Local_Daemon SHALL discard the cached channel and SHALL retry the request once on a freshly opened channel before returning an error to the Rhop_Cli.

### Requirement 5: Connection Reuse at Every Hop

**User Story:** As a developer issuing many commands against the same target, I want connections to be reused at every layer, so that the second `rhop exec` against a host does not pay the SSH handshake cost again.

#### Acceptance Criteria

1. THE Local_Daemon SHALL maintain a Connection_Pool keyed by Jump_Host identity (and by End_Target for `direct` routes).
2. WHEN two `exec` or `cp` requests target the same Target_Route within `ssh.max_idle_time`, THE Local_Daemon SHALL reuse the same pooled connection for both requests where the per-key concurrency limit is not exceeded.
3. WHERE the Target_Route includes a Remote_Daemon, THE Remote_Daemon SHALL maintain its own Connection_Pool to its End_Targets and SHALL reuse pooled SSH connections across requests received over the same gRPC channel.
4. THE Local_Daemon SHALL enforce a per-key concurrency limit equal to `ssh.max_connections_per_ip` for every Jump_Host kind, including `rhopd`.
5. WHEN a pooled connection has been idle for at least `ssh.max_idle_time`, THE Local_Daemon SHALL close that connection on the next reaper tick.

### Requirement 6: Mode-Invariant Command Behavior

**User Story:** As a developer, I want `rhop exec foo hostname` and `rhop cp src foo:dst` to behave the same regardless of how `foo` is reached, so that I do not have to remember which routes are "different".

#### Acceptance Criteria

1. FOR ALL configurations of the Target_Route, WHEN `rhop exec <target> <argv>` succeeds, THE Rhop_Cli SHALL receive the same exit code that running `<argv>` directly on the End_Target would produce.
2. FOR ALL configurations of the Target_Route, WHEN `rhop exec` succeeds, THE concatenated `stdout` bytes seen by the Rhop_Cli SHALL equal the concatenated `stdout` bytes the End_Target wrote, after stripping daemon-injected control sequences documented in the design.
3. FOR ALL configurations of the Target_Route, WHEN `rhop cp` uploads a file `f` and a subsequent `rhop cp` downloads `f` to a fresh local path, THE downloaded bytes SHALL equal the original bytes (round-trip property).
4. WHERE `copy.preserve_mode` is true and a Copy_Operation upload-then-download cycle succeeds, THE downloaded file's Unix permission bits SHALL equal the original file's Unix permission bits (round-trip property).
5. THE Local_Daemon SHALL apply `~` expansion of the `Remote_Side` path against the End_Target's environment and SHALL apply `~` expansion of the `Local_Side` path against its own environment.

### Requirement 7: Resolver and Target Route Construction

**User Story:** As a developer, I want target resolution to be deterministic and explainable, so that I can predict which Jump_Host my command will use.

#### Acceptance Criteria

1. WHEN the Local_Daemon resolves a CLI target string against a fixed Daemon_Config, THE resolver SHALL produce the same ordered list of Target_Route candidates on every call (idempotence).
2. THE resolver SHALL evaluate Jump_Host candidates in the order defined by `ssh.fallback`, plus `server.toml` matches, plus any `rhopd` Jump_Host entries, with the precise ordering specified in the design, and WHERE `ssh.fallback` is empty or contains only disabled transports, THE resolver SHALL contribute zero candidates from the `ssh.fallback` step while preserving the relative position of the other steps.
3. IF no Jump_Host candidate matches a CLI target, THEN THE Local_Daemon SHALL return an error whose message names the target string and the resolver order that was tried, and THE Local_Daemon SHALL NOT enter the connection-failure retry loop in this case.
4. WHEN at least one Target_Route candidate exists and the first candidate fails to connect with a connection-level error, THE Local_Daemon SHALL try the next candidate in order before reporting failure.
5. THE resolver SHALL classify each Target_Route candidate by Jump_Host kind, and the Connection_Pool key SHALL be derived from that classification plus the End_Target identifier.

### Requirement 8: Authentication Prompting Through One Path

**User Story:** As a developer, I want password, TOTP, and host-key prompts to always appear in my terminal, so that I can answer them no matter which Jump_Host needs the input.

#### Acceptance Criteria

1. WHEN any Jump_Host on the Target_Route requires interactive input, THE owning daemon SHALL emit an Auth_Prompt event upstream rather than reading from its own TTY.
2. WHERE a Remote_Daemon Jump_Host needs an Auth_Prompt for the End_Target hop, THE Remote_Daemon SHALL forward the Auth_Prompt over its gRPC channel, and THE Local_Daemon SHALL forward it again to the Rhop_Cli.
3. WHEN the Rhop_Cli receives an Auth_Prompt with `secret = true`, THE Rhop_Cli SHALL read input with terminal echo disabled.
4. WHEN the Rhop_Cli replies to an Auth_Prompt, THE response SHALL be routed back to the daemon that originated the prompt using the prompt's `prompt_id`.
5. IF an Auth_Prompt is not answered before the configured connection timeout, THEN THE owning daemon SHALL abort the operation and SHALL emit an error event naming the prompt's target label.

### Requirement 9: Status Reflects the Whole Topology

**User Story:** As an operator, I want `rhop status` to show me every active connection, including connections through a Remote_Daemon, so that I can see what is being reused.

#### Acceptance Criteria

1. WHEN `rhop status` is invoked, THE Local_Daemon SHALL return its own pool entries grouped by Jump_Host kind.
2. WHERE the Local_Daemon holds at least one open `rhopd` Jump_Host channel, THE status response SHALL include for each such channel the Jump_Host name, the SSH address, and the Remote_Daemon's reported pool entries; IF the Remote_Daemon has not yet returned a complete status response for a channel, THEN THE Local_Daemon SHALL wait for that response before answering the Rhop_Cli, up to the configured connect timeout.
3. THE Local_Daemon SHALL include in the status response its `daemon_origin` and the CLI start arguments it currently retains.
4. THE Local_Daemon SHALL NOT include any field whose meaning depends on a `ClientMode` distinction.

### Requirement 10: Configuration Migration and Removal of Legacy Modes

**User Story:** As the project owner, I have stated that backward compatibility is not required, so I want the legacy `rhop remote enable/disable` flow removed and replaced cleanly.

#### Acceptance Criteria

1. THE Rhop_Cli SHALL NOT expose the subcommands `rhop remote enable` and `rhop remote disable`.
2. THE Rhop_Cli SHALL expose the subcommand `rhop remote connect <name> <user>@<host>[:port]` that adds a `rhopd` Jump_Host entry with Jump_Host_Alias `<name>` to the Daemon_Config and that performs an SSH host-key trust prompt against the new entry.
3. THE Rhop_Cli SHALL expose the subcommand `rhop remote remove <name>` that deletes a `rhopd` Jump_Host entry whose Jump_Host_Alias is `<name>` from the Daemon_Config.
4. THE Rhop_Cli SHALL expose the subcommand `rhop remote list` that prints every Jump_Host entry currently declared in the Daemon_Config, including its Jump_Host_Alias and Jump_Host kind.
5. THE Rhop_Cli SHALL only support quick-add and quick-remove of `rhopd` Jump_Host entries; WHERE the user wants to add a Jump_Host of kind `jumpserver` or any future kind, THE user SHALL edit the Daemon_Config directly.
6. IF `rhop remote connect <name> ...` is invoked with `<name>` equal to a Reserved_Alias, THEN THE Rhop_Cli SHALL exit with a non-zero status and SHALL print an error that names the rejected alias and lists the Reserved_Alias set.
7. IF `rhop remote connect <name> ...` is invoked with `<name>` equal to an existing Jump_Host_Alias of any kind, THEN THE Rhop_Cli SHALL exit with a non-zero status and SHALL print an error that names the existing alias and its current kind, without modifying the Daemon_Config.
8. IF `rhop remote remove <name>` is invoked with `<name>` not declared in the Daemon_Config, THEN THE Rhop_Cli SHALL exit with a non-zero status and SHALL print an error that names the missing alias.
9. IF `rhop remote remove <name>` is invoked with `<name>` declared in the Daemon_Config but whose kind is not `rhopd`, THEN THE Rhop_Cli SHALL exit with a non-zero status and SHALL print an error stating that quick-remove only manages `rhopd` Jump_Host entries.
10. THE Client_Config SHALL NOT contain a `mode` field nor a top-level `[remote]` block describing the daemon Rhop_Cli connects to.
11. THE Client_Config SHALL describe only how Rhop_Cli reaches the Local_Daemon (socket path and auto-start).
12. THE Daemon_Config SHALL be the only place that declares `rhopd` Jump_Host entries, jumpserver entries, and direct-SSH fallbacks.
13. WHEN a daemon starts and reads a Daemon_Config that uses the new schema, THE daemon SHALL start successfully without requiring any environment variables that named the old `ClientMode`.

### Requirement 11: Remote Target Spec Parsing and Formatting

**User Story:** As a developer, I want the `[user@]host[:port]` strings I type to round-trip cleanly with the configuration the daemon stores, so that re-reading my saved jump host gives me back what I wrote.

#### Acceptance Criteria

1. THE Local_Daemon SHALL provide a parser that converts a string of the form `[user@]host[:port]` into a structured Jump_Host address with explicit `user`, `host`, and `port` fields.
2. THE Local_Daemon SHALL provide a formatter that converts a structured Jump_Host address back into a string of the form `user@host:port`.
3. FOR ALL valid structured Jump_Host addresses with `user` non-empty, parsing the formatter's output SHALL produce a structured Jump_Host address equal to the input (round-trip property).
4. IF a string fails to parse as `[user@]host[:port]`, THEN the parser SHALL return an error whose message names the offending input.
5. WHERE the input string omits the user, THE parser SHALL fill the configured default user; WHERE the input string omits the port, THE parser SHALL fill the configured default port.
6. THE Rhop_Cli command `rhop remote connect <name> <address>` SHALL accept `<address>` as a string of the form `[user@]host[:port]` and SHALL pass it to the parser defined by this requirement.

### Requirement 12: Failure Semantics for Remote Daemon Hops

**User Story:** As a developer, I want clear and actionable errors when a `rhopd` Jump_Host is misconfigured or unreachable, so that I can fix the right thing.

#### Acceptance Criteria

1. IF the Local_Daemon cannot establish an SSH connection to a configured `rhopd` Jump_Host, THEN THE Local_Daemon SHALL return an error to the Rhop_Cli that names the Jump_Host entry and the underlying SSH error.
2. IF the SSH host key for a `rhopd` Jump_Host has changed compared to the recorded `known_hosts` entry, THEN THE Local_Daemon SHALL refuse to connect and SHALL emit an error naming the Jump_Host entry, the recorded fingerprint, and the seen fingerprint.
3. IF a Remote_Daemon's `rhop-rpc` subsystem cannot be opened on an otherwise-valid SSH session, THEN THE Local_Daemon SHALL return an error naming the Jump_Host entry and the subsystem name.
4. WHEN a Remote_Daemon returns an error event during `Execute` or `Copy`, THE Local_Daemon SHALL forward that error event to the Rhop_Cli without rewriting its message.
5. WHILE a `rhopd` Jump_Host channel is open, IF the underlying SSH connection drops, THEN THE Local_Daemon SHALL evict the cached channel from the Connection_Pool before the next request reuses it.

### Requirement 13: Audit and Review Events Cross All Hops

**User Story:** As an operator who has enabled command review, I want review decisions and confirmations to behave the same when reaching End_Targets through a Remote_Daemon, so that I cannot accidentally bypass review by routing through `rhopd`.

#### Acceptance Criteria

1. WHEN a `rhop exec` request reaches the Local_Daemon and `review.enable` is true, THE Local_Daemon SHALL run its configured command review before any data is sent to a Jump_Host, including a `rhopd` Jump_Host.
2. WHERE `review.enable` is false on the Local_Daemon, THE Local_Daemon SHALL forward the request to the Jump_Host without performing review.
3. WHERE a Remote_Daemon also has command review enabled, THE Remote_Daemon SHALL run its review on the End_Target-side argv it receives over the gRPC channel.
4. WHEN the Local_Daemon's review action is `deny`, THE Local_Daemon SHALL NOT open or reuse any Jump_Host connection for that request.
5. WHEN the Local_Daemon's review action is `confirm`, THE Local_Daemon SHALL emit a `ConfirmRequired` event to the Rhop_Cli and SHALL wait for the matching reply before forwarding the request to any Jump_Host.

### Requirement 14: Jump Host Aliases and Reserved Names

**User Story:** As an operator who runs many jump hosts, I want every jump host I configure to have a unique short name that I choose, so that I can refer to it in CLI commands without typing its address.

#### Acceptance Criteria

1. THE Daemon_Config SHALL allow declaring an unbounded number of Jump_Host entries across all supported Jump_Host kinds, including `rhopd`, `jumpserver`, and any future kind.
2. THE Daemon_Config SHALL require every Jump_Host entry to have a Jump_Host_Alias that is non-empty and that is unique across all Jump_Host entries regardless of kind.
3. WHEN a daemon loads a Daemon_Config in which two Jump_Host entries share the same Jump_Host_Alias, THE daemon SHALL fail to start and SHALL emit an error that names the duplicated alias and the kinds that share it.
4. THE Daemon_Config SHALL reserve the alias `local` as the Reserved_Alias that names the Local_Daemon's own `server.toml` Server_List_Source, and THE Daemon_Config SHALL NOT permit `local` as the Jump_Host_Alias of any Jump_Host entry.
5. WHEN a daemon loads a Daemon_Config in which any Jump_Host entry has Jump_Host_Alias `local`, THE daemon SHALL fail to start and SHALL emit an error that names the rejected alias.
6. WHEN `rhop remote connect <name> <address>` is invoked, THE Rhop_Cli SHALL validate `<name>` against the Reserved_Alias set and against the existing Jump_Host_Alias set before writing to the Daemon_Config.

### Requirement 15: Merged Server Listing with Disambiguation

**User Story:** As a developer, I want `rhop server list` to show me every End_Target I can reach, no matter which jump host I would reach it through, so that I do not have to log into each jump host to discover what is available.

#### Acceptance Criteria

1. WHEN `rhop server list` is invoked, THE Local_Daemon SHALL return the union of (a) the End_Target entries declared in its own `server.toml` and (b) the End_Target entries returned by invoking `list_servers` on every Jump_Host entry whose kind implements `list_servers`.
2. THE Local_Daemon SHALL tag every row in the merged server-list response with its Server_List_Source, where the Server_List_Source is `local` for entries from the Local_Daemon's `server.toml` and is the Jump_Host_Alias of the originating Jump_Host for entries fetched from a Jump_Host.
3. THE Local_Daemon SHALL include in the merged server-list response a per-Server_List_Source status field that is `ok`, `unsupported`, or a transport error message, where `unsupported` is used WHEN the Jump_Host returned an Unsupported_Capability error for `list_servers`.
4. WHERE a Jump_Host returns an Unsupported_Capability error or a transport error from `list_servers`, THE Local_Daemon SHALL contribute zero rows for that Server_List_Source and SHALL NOT cause `rhop server list` to fail overall.
5. WHEN a CLI target string has the form `<jump_alias>:<server_alias>` and `<jump_alias>` matches a known Jump_Host_Alias or the Reserved_Alias `local`, THE resolver SHALL look up `<server_alias>` only on the Server_List_Source named by `<jump_alias>`.
6. WHEN a CLI target string is a bare `<server_alias>` and that `<server_alias>` is unique across all Server_List_Source values currently visible to the Local_Daemon, THE resolver SHALL accept the bare form and SHALL treat it as resolving to the unique matching End_Target.
7. IF a CLI target string is a bare `<server_alias>` and the same `<server_alias>` appears in more than one Server_List_Source, THEN THE resolver SHALL return an error that lists every candidate `<jump_alias>:<server_alias>` form.
8. WHEN the Local_Daemon caches the result of a Jump_Host's `list_servers` response, THE Local_Daemon SHALL evict that cache entry on the next `rhop server list` invocation that explicitly requests a refresh.

### Requirement 16: Trait Extensibility Discipline

**User Story:** As a maintainer adding a new Jump_Host kind, I want the change to be confined to the trait implementation and the configuration schema, so that I do not have to touch unrelated parts of the system.

#### Acceptance Criteria

1. WHEN a new Jump_Host kind is added to the system, THE change SHALL be limited to (a) implementing the Jump_Host trait for the new kind, (b) registering the new kind in the Daemon_Config schema, and (c) registering the new kind in the resolver factory.
2. WHEN a new Jump_Host kind is added to the system, THE change SHALL NOT modify the Rhop_Cli command handlers, the Connection_Pool implementation, the gRPC service definitions, or the daemon main loop.
3. THE Jump_Host trait SHALL provide a default implementation for every optional method that returns an Unsupported_Capability error, so that a new Jump_Host kind compiles when only `exec` and `copy` are implemented.
4. WHEN any caller invokes an optional Jump_Host trait method on a kind that has not overridden the default, THE caller SHALL receive an Unsupported_Capability error rather than a panic or an undefined behavior.
5. THE Daemon_Config SHALL identify Jump_Host kind by a string tag declared in the schema, and THE resolver factory SHALL select the concrete Jump_Host implementation by matching that string tag.


### Requirement 17: AI- and Agent-Friendly CLI Surface

**User Story:** As an AI agent or automation script driving `rhop`, I want the CLI to be fully scriptable with no interactive prompts, structured output, deterministic exit codes, an explicit PTY policy, a timeout, stdin forwarding, env-var credentials, non-interactive host-key handling, and machine-readable capability discovery, so that I can call `rhop` reliably without a controlling terminal and without parsing prose.

#### Acceptance Criteria

**A. Argument parsing rules**

1. THE Rhop_Cli SHALL parse global rhop flags only when they appear before the TARGET positional argument in `rhop exec`.
2. WHEN parsing `rhop exec <target> <argv>...`, THE Rhop_Cli SHALL treat every token after `<target>` as an opaque element of argv, including tokens that begin with `-` or `--`.
3. WHEN no `<argv>` token is provided after `<target>`, THE Rhop_Cli SHALL exit with a non-zero status and SHALL print an error naming the missing operand.

**B. Output format and exit codes**

4. THE Rhop_Cli SHALL accept a global flag `--output {text|json}` (default `text`) on every subcommand.
5. WHERE `--output json` is set, THE Rhop_Cli SHALL emit one JSON object per line (NDJSON) on stdout for streaming subcommands (`exec`, `cp`, `server list`, `status`).
6. THE Rhop_Cli SHALL define an exit-code taxonomy that lets callers distinguish remote-command failure from rhop-self failure, with the following codes:
   - `0` — operation succeeded (and remote command exit `0` where applicable)
   - `1..=123` — remote command's exit code, transparently forwarded
   - `124` — `--timeout` deadline expired (consistent with the GNU `timeout` utility)
   - `125` — Rhop_Cli or daemon itself failed (config error, daemon unreachable, resolver failure, transport error)
   - `126` — authentication failure, host-key rejection, or review-deny
   - `127` — target not found, Unknown_Alias, or Unsupported_Capability for the requested operation
   - `200..=255` — internal or unexpected errors
7. THE Rhop_Cli SHALL document the exit-code taxonomy defined in criterion 6 in its `--help` output and SHALL expose the same taxonomy in machine-readable form via `rhop --version --output json`.
8. THE Rhop_Cli SHALL emit remote-command stdout bytes only on its own stdout, and SHALL emit Rhop_Cli and daemon Info events on its own stderr in `text` mode or as separate NDJSON event types in `json` mode, and these streams SHALL NOT be interleaved into the remote-command stdout stream.

**C. Non-interactive mode**

9. THE Rhop_Cli SHALL accept a global flag `--non-interactive` on every subcommand.
10. WHERE `--non-interactive` is set and any code path would prompt for human input (host-key trust, password, MFA, review confirmation, or any other secret), THE Rhop_Cli SHALL exit with code `126` and SHALL emit an error event naming the prompt's purpose and target_label, without reading from stdin.
11. WHERE `--non-interactive` is set, THE Rhop_Cli SHALL NOT call termios `tcsetattr` and SHALL NOT disable terminal echo.
12. THE Rhop_Cli SHALL accept the env vars `RHOP_PASSWORD` and `RHOP_TOTP_SECRET` as alternative inputs for the `password` and `jump_mfa` Auth_Prompt kinds, and WHERE the corresponding env var is set, THE Rhop_Cli SHALL use its value to answer the matching Auth_Prompt without prompting interactively.
13. WHERE `RHOP_PASSWORD` or `RHOP_TOTP_SECRET` is set and `--non-interactive` is also set, THE Rhop_Cli SHALL still answer the matching Auth_Prompt from the env var rather than failing.

**D. PTY control**

14. THE Rhop_Cli SHALL accept the mutually exclusive flags `--pty` and `--no-pty` on `rhop exec`.
15. THE Daemon_Config SHALL include a setting `ssh.auto_pty_detect` of type bool with default `true`.
16. THE Local_Daemon SHALL compute the Effective_Pty_Decision by the following priority: an explicit `--pty` or `--no-pty` flag on the invocation overrides all other inputs; otherwise, WHEN `ssh.auto_pty_detect` is true and the Rhop_Cli reports its stdout is not a TTY, the decision is "no PTY"; otherwise, the decision is the value of `ssh.pty`.
17. THE Rhop_Cli SHALL include in its `Execute` request a boolean `stdout_is_tty` field whose value is derived from `IsTerminal::is_terminal(io::stdout())`, so that the Local_Daemon can honor `ssh.auto_pty_detect`.

**E. Timeout**

18. THE Rhop_Cli SHALL accept `--timeout <duration>` on `rhop exec` and `rhop cp`, accepting the same duration syntax as the Daemon_Config (for example `30s`, `2m`).
19. WHEN `--timeout` is set and the operation has not produced an `ExitStatus` event by the deadline, THE Rhop_Cli SHALL emit an error event with kind `timeout`, SHALL send a cancellation to the Local_Daemon, and SHALL exit with code `124`.

**F. Stdin forwarding**

20. THE Rhop_Cli SHALL accept the flag `--stdin` on `rhop exec`.
21. WHERE `--stdin` is set, THE Rhop_Cli SHALL forward its own stdin bytes to the remote command's stdin until the local stdin reaches EOF.
22. WHERE `--stdin` is NOT set, THE Rhop_Cli SHALL close the remote command's stdin immediately after the remote command starts.

**G. Host-key handling for non-interactive setup**

23. THE Rhop_Cli SHALL accept on `rhop remote connect` the mutually exclusive flags `--accept-new-host-key` (TOFU mode) and `--fingerprint <sha256-base64>` (pinned-fingerprint mode).
24. WHEN `--accept-new-host-key` is set and the host key is unknown, THE Rhop_Cli SHALL trust the key without prompting and SHALL append it to the configured known_hosts file.
25. WHEN `--fingerprint <expected>` is set and the host key's SHA256 fingerprint equals `<expected>`, THE Rhop_Cli SHALL trust the key without prompting; IF the fingerprint does not match, THEN THE Rhop_Cli SHALL exit with code `126`.
26. WHERE `--non-interactive` is set without `--accept-new-host-key` and without `--fingerprint`, AND the host key is unknown, THEN THE Rhop_Cli SHALL exit with code `126` without prompting.

**H. Capability and version discovery**

27. WHEN `rhop --version --output json` is invoked, THE Rhop_Cli SHALL emit a single JSON object on stdout containing at least the fields `version` (a semver string), `capabilities` (a list of supported subcommands and notable flags), and `exit_codes` (a map describing the exit-code taxonomy from criterion 6).
28. THE Rhop_Cli SHALL accept `rhop server list --no-cache` as a synonym for `rhop server list --refresh`.

### Requirement 17: Non-Interactive and Machine-Friendly Operation

**User Story:** As an AI agent or CI script invoking `rhop`, I want predictable argument parsing, structured output, deterministic exit codes, and the ability to opt out of every interactive prompt, so that I can drive `rhop` reliably without a TTY.

#### Acceptance Criteria

##### A. Argument parsing for `rhop exec`

1. THE Rhop_Cli SHALL parse Rhop_Cli flags only when they appear before the `<TARGET>` positional argument in `rhop exec`.
2. WHEN parsing `rhop exec <flags> <target> <argv>...`, THE Rhop_Cli SHALL treat every token after `<target>` as an opaque element of `argv`, including tokens that begin with `-`, `--`, or `--<word>`.
3. WHEN `rhop exec <target>` is invoked with no `<argv>` token, THE Rhop_Cli SHALL exit with code `125` and print an error naming the missing operand.
4. THE Rhop_Cli SHALL NOT interpret a literal `--` token after `<target>` as a separator and SHALL pass it through as part of `argv` if present.

##### B. Output format and stream separation

5. THE Rhop_Cli SHALL accept a global flag `--output {text|json}` on every subcommand, with default value `text`.
6. WHERE `--output json` is set, THE Rhop_Cli SHALL emit one JSON object per line (NDJSON) on stdout for the streaming subcommands `exec`, `cp`, `server list`, `status`, with one event record per line and a trailing newline after each record.
7. THE Rhop_Cli SHALL emit remote-command stdout bytes only on its own stdout in `text` mode, and SHALL emit Rhop_Cli/daemon Info events on its own stderr in `text` mode.
8. WHERE `--output json` is set, THE Rhop_Cli SHALL emit every event record (remote stdout chunks, remote stderr chunks, Info events, ExitStatus, error events, AuthPrompt events) as NDJSON on stdout, with each record carrying an `event` discriminator field naming the event kind.
9. THE Rhop_Cli SHALL document the NDJSON event schema in a stable form, and the schema SHALL include at least the event kinds `stdout`, `stderr`, `info`, `error`, `exit`, `auth_prompt`, and `confirm_required`.

##### C. Exit-code taxonomy

10. THE Rhop_Cli SHALL define an exit-code taxonomy that distinguishes remote-command failure from Rhop_Cli/daemon failure as follows:
    - `0` — operation succeeded; for `rhop exec`, the remote command also exited with `0`.
    - `1..=123` — for `rhop exec`, the remote command's exit code, transparently forwarded.
    - `124` — operation aborted because `--timeout` deadline expired.
    - `125` — Rhop_Cli or daemon itself failed (config error, daemon unreachable, resolver failure, transport error, missing operand).
    - `126` — authentication failure, host-key rejection, or review action `deny`.
    - `127` — target not found, Unknown_Alias, or Unsupported_Capability for the requested operation.
    - `200..=255` — internal/unexpected errors.
11. THE Rhop_Cli SHALL document this exit-code taxonomy in `rhop --help` text and SHALL emit it in machine-readable form when `rhop --version --output json` is invoked.
12. WHEN `rhop exec` succeeds, THE Rhop_Cli SHALL exit with code `c` where `c` is the remote command's exit code, capped to the range `0..=123` (any remote exit code at or above `124` SHALL be capped to `123` so it does not collide with the timeout/Rhop_Cli/auth/target-not-found bands).

##### D. Non-interactive mode and credential bypass

13. THE Rhop_Cli SHALL accept a global flag `--non-interactive` on every subcommand.
14. WHERE `--non-interactive` is set and any code path would prompt for human input (host-key trust, password, MFA, review confirmation, secret), THE Rhop_Cli SHALL exit with code `126` and emit an error event naming the prompt's purpose and the prompt's `target_label`, without reading from stdin.
15. WHERE `--non-interactive` is set, THE Rhop_Cli SHALL never call termios `tcsetattr` and SHALL never disable terminal echo.
16. THE Rhop_Cli SHALL accept the env vars `RHOP_PASSWORD` and `RHOP_TOTP_SECRET` as alternative sources for `password` and `jump_mfa` Auth_Prompt kinds.
17. WHERE the env var matching the Auth_Prompt's kind is set (`RHOP_PASSWORD` for `password`, `RHOP_TOTP_SECRET` for `jump_mfa`), THE Rhop_Cli SHALL use its value to answer the prompt without reading stdin.
18. WHERE both `--non-interactive` is set and the matching env var is set, THE Rhop_Cli SHALL still answer the Auth_Prompt from the env var rather than failing.

##### E. PTY control (Plan C)

19. THE Rhop_Cli SHALL accept mutually exclusive flags `--pty` and `--no-pty` on `rhop exec`.
20. THE Daemon_Config SHALL include a setting `ssh.auto_pty_detect` of type bool with default value `true`.
21. THE Rhop_Cli SHALL include in its `Execute` request a boolean `stdout_is_tty` derived from `IsTerminal::is_terminal(io::stdout())`.
22. THE Local_Daemon SHALL compute the Effective_Pty_Decision by the following priority, with each step short-circuiting when it produces a value:
    1. If the Rhop_Cli passed `--no-pty`, the decision is `false`.
    2. If the Rhop_Cli passed `--pty`, the decision is `true`.
    3. If `ssh.auto_pty_detect` is `true` and the Rhop_Cli reported `stdout_is_tty = false`, the decision is `false`.
    4. Otherwise, the decision is the value of `ssh.pty` from the Daemon_Config.
23. THE Effective_Pty_Decision SHALL be a pure function of the inputs in clause 22, and two `rhop exec` invocations with the same inputs SHALL produce the same decision.

##### F. Timeout

24. THE Rhop_Cli SHALL accept `--timeout <duration>` on `rhop exec` and `rhop cp`, where `<duration>` uses the same syntax as the Daemon_Config (`30s`, `2m`, `1h`).
25. WHEN `--timeout` is set and the operation has not produced an `ExitStatus` event (for `exec`) or a `Complete` event (for `cp`) by the deadline, THE Rhop_Cli SHALL emit an error event with kind `timeout`, send a cancellation request to the daemon, and exit with code `124`.
26. WHERE `--timeout` is not set, THE Rhop_Cli SHALL wait without an upper bound for the operation to complete.

##### G. Stdin forwarding

27. THE Rhop_Cli SHALL accept a flag `--stdin` on `rhop exec`.
28. WHERE `--stdin` is set, THE Rhop_Cli SHALL forward bytes read from its own stdin to the remote command's stdin until the local stdin reaches EOF, and SHALL then close the remote command's stdin.
29. WHERE `--stdin` is NOT set, THE Rhop_Cli SHALL close the remote command's stdin immediately upon starting the command.

##### H. Host-key handling for non-interactive setup

30. THE Rhop_Cli SHALL accept on `rhop remote connect` two mutually exclusive flags: `--accept-new-host-key` (TOFU mode) and `--fingerprint <sha256-base64>` (pinned-fingerprint mode).
31. WHEN `--accept-new-host-key` is set and the host key is not present in the configured `known_hosts` file, THE Rhop_Cli SHALL trust the key without prompting and append it to the `known_hosts` file.
32. WHEN `--fingerprint <expected>` is set, THE Rhop_Cli SHALL compute the fetched host key's SHA256 fingerprint in the same encoding as `<expected>` and SHALL trust the key without prompting if and only if the values match; otherwise THE Rhop_Cli SHALL exit with code `126` and emit an error event naming both the expected and the seen fingerprint.
33. WHERE `--non-interactive` is set, neither `--accept-new-host-key` nor `--fingerprint` is set, and the host key is not present in the configured `known_hosts` file, THEN THE Rhop_Cli SHALL exit with code `126` and emit an error event naming the unknown host without prompting.
34. WHERE the host key is already present in the configured `known_hosts` file and matches, THE Rhop_Cli SHALL trust the key without prompting regardless of the flags above.

##### I. Capability and version discovery

35. WHEN `rhop --version --output json` is invoked, THE Rhop_Cli SHALL emit a single JSON object on stdout containing at least the keys `version` (string semver), `capabilities` (array of supported subcommand names), and `exit_codes` (object mapping each documented exit code to a human-readable description), and SHALL exit with code `0`.
36. THE Rhop_Cli SHALL accept `rhop server list --no-cache` as a synonym for `rhop server list --refresh`.

