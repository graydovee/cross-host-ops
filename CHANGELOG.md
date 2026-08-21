# Changelog

<!-- Maintenance rules: before every commit, prepend one line to the "Entries" section below.
     Format: yyyy-MM-dd [tag] brief content
     tag values: feat | bug | refactor | docs
     Classify by the substance of the change, not the commit prefix:
       new feature or capability (incl. independently added tests/tools) -> [feat]
       bug fix -> [bug]
       behavior-preserving refactor, internal cleanup, dependency upgrade, CI/build adjustment -> [refactor]
       documentation change -> [docs]
     Tests/formatting/minor cleanup bundled with a fix/feat are NOT logged separately; fold them into that entry. -->

## Entries

<!-- Prepend new entries at the very top -->
2026-08-21 [bug] apply configured keepalive to the reverse proxy client SSH connection; half-open tunnels after sleep or network switch are now detected within ~90s and the reconnect loop re-registers the node automatically
2026-08-17 [bug] fix intermittent hang and dropped stdin in streaming ops (exec -i, cp): session drivers used two channels for control vs stdin so eof/data could overtake exec/subsystem on the wire at random; unify into one ordered channel (tunnel/direct/local sessions); hung sessions previously leaked pool leases and exhausted target sshd MaxStartups under sustained load
2026-08-07 [refactor] replace dead --non-interactive flag with -y/--yes that auto-confirms review prompts (exec/cp); clean up stale exit-code docs
2026-08-06 [refactor] unify AI command review and audit logging into a single oversight module; cp now goes through AI review with configurable allowlist/blocklist; add JSON-Lines audit log recording source IP/SSH user/key fingerprint for every machine operation (exec/cp/open_session/2222 proxy); per-operation review toggles (review.exec/review.copy); MfaConfig moved to its own config module
2026-08-06 [bug] list_servers skips gateways without LIST capability (e.g. Jumpserver bastions); no longer returns Unsupported sources to clients
