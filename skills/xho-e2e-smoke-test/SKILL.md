---
name: xho-e2e-smoke-test
description: Run end-to-end smoke tests covering all xho CLI functionality against a live remote target. Use when: (1) verifying xho works after a deploy or upgrade, (2) running smoke tests, e2e tests, or integration tests against remote servers, (3) validating exec, cp, ls, status, daemon, and host commands work correctly. Triggers on phrases like "run smoke test", "verify deployment", "e2e test", "integration test", "test xho", "validate functionality".
---

# xho E2E Smoke Test

## Workflow

1. Ensure local daemon is running: `xho daemon start`
2. Run the smoke test script against a target (path is relative to this skill's directory):

```bash
bash scripts/smoke-test.sh <target>
```

Where `<target>` is a reachable server alias or `jump:server` pattern (e.g., `prod-xhod:web1`).

## What It Tests

| Category | Test Cases |
|----------|-----------|
| **exec** | basic command, exit code propagation, -t flag, --no-tty, --timeout, multi-arg with --, shell wrapping |
| **cp** | upload file, download file, recursive directory copy |
| **ls** | server list returns output |
| **status** | daemon status returns successfully |
| **daemon** | stop + start cycle |
| **host** | list configured jump hosts |

## Script Parameters

```
smoke-test.sh <target> [--skip-daemon] [--skip-cp] [--verbose]
```

- `<target>`: Required. The remote target to test against.
- `--skip-daemon`: Skip daemon stop/start tests (useful if daemon is managed externally).
- `--skip-cp`: Skip file copy tests.
- `--verbose`: Print each command before execution.

## Interpreting Results

- Each test prints `[PASS]` or `[FAIL]` with the test name.
- Final summary shows pass/fail counts.
- Exit code 0 = all passed, 1 = at least one failure.

## Adding New Test Cases

Edit `scripts/smoke-test.sh`. Each test follows the pattern:

```bash
run_test "test name" xho exec "$TARGET" -- some-command
```

The `run_test` function checks exit code 0 = pass, non-zero = fail.

For tests that expect specific output, use `run_test_output`:

```bash
run_test_output "test name" "expected_substring" xho exec "$TARGET" -- echo hello
```
