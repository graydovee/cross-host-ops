---
name: xho-release
description: Cut and publish a Cross Host Ops (xho/xhod) release — finalize the bilingual changelogs, push master, cut the annotated v* tag, and watch the GitHub Actions release workflow publish binaries and the Docker image. Use whenever the user wants to release, ship, or publish a version: triggers on "release", "release为vX.Y.Z", "发版", "发布一个版本", "打tag", "cut a tag", "publish binaries", "bump version", or questions about release readiness/prerequisites. Never cut a tag without an explicit user request.
---

# xho Release

Cutting a release of **Cross Host Ops** is nothing more than pushing a `v*`
git tag: `.github/workflows/release.yml` then builds multi-platform
musl/macOS tarballs and the GHCR Docker image and publishes a GitHub Release.
There is **no `Cargo.toml` version to bump** — `build.rs` derives `--version`
from `git describe`, so built binaries report the tag automatically.

Never cut a tag on your own initiative: a tag publishes artifacts permanently
and consumes a version number. Only act on an explicit user request ("release
为 v0.5.7", "发个版"). The steps below assume you are on `master`.

## Preconditions (check all before tagging)

1. **Target version is explicit** from the user, or proposed and confirmed.
   If unspecified, propose the next one:
   ```bash
   git fetch --tags --quiet && git tag --list 'v*' | sort -V | tail -1
   ```
   then +1 patch within the current `vX.Y` series — project history bumps the
   patch level even when the cycle contains features (v0.5.5 → v0.5.6).
2. **Green tree**: `cargo test` passing; `cargo fmt --all --check` clean;
   `git status` shows at most the final changelog commit pending.
3. **Synced with origin**: no divergence
   (`git rev-list --left-right --count origin/master...master` → `0 N`).
4. **Both changelogs finalized** (`CHANGELOG.md`, `CHANGELOG.zh-CN.md`):
   - Any entries missing for this cycle prepended under the open section.
   - Section renamed `## latest` → `## vX.Y.Z`.
   - English and Chinese still map one-to-one: same dates, same tags, same order.
   - Entry format rules live in AGENTS.md § Changelog — follow them exactly.

Abort and report if any check fails; do not "fix forward" silently mid-release.

## Procedure

```bash
# 1. Commit anything still pending (the latest→vX.Y.Z rename rides along)
git add -A && git commit -m "docs: finalize v0.5.7 changelog"

# 2. Push master BEFORE tagging
git push origin master

# 3. Cut an ANNOTATED tag and push it — this triggers the release workflow
git tag -a v0.5.7 -m "v0.5.7"
git push origin v0.5.7
```

Always annotated tags, message = the bare version string, matching prior
releases (v0.5.4…v0.5.6).

## Watch & verify

The tag push starts a GitHub Actions run of the `release` workflow:

```bash
gh run list --repo graydovee/cross-host-ops --limit 3
gh run watch <run-id>    # optional live view
```

Historical duration ≈ 55–70 min (v0.5.5 took 54m, v0.5.4 71m) — don't block on
it; report the run link, then offer to check back. When green, verify the
GitHub Release lists the platform tarballs and that
`ghcr.io/graydovee/cross-host-ops:<tag>` was published.

## Failure handling

- **Workflow red**: pull logs with `gh run view <id> --log-failed`, diagnose,
  and land the fix as a normal commit. Do NOT silently delete and re-push the
  same tag — offer re-tagging explicitly or cutting the next patch version,
  and let the user choose.
- **Push rejected (non-fast-forward)**: someone moved `origin/master`; stop
  and reconcile with the user. Force-push is never part of a release.
