#!/usr/bin/env bash
# xho end-to-end smoke test suite
# Usage: smoke-test.sh <target> [--skip-daemon] [--skip-cp] [--verbose]
set -uo pipefail

# --- Argument Parsing ---
TARGET=""
SKIP_DAEMON=false
SKIP_CP=false
VERBOSE=false

for arg in "$@"; do
  case "$arg" in
    --skip-daemon) SKIP_DAEMON=true ;;
    --skip-cp) SKIP_CP=true ;;
    --verbose) VERBOSE=true ;;
    -*) echo "Unknown option: $arg"; exit 1 ;;
    *) TARGET="$arg" ;;
  esac
done

if [[ -z "$TARGET" ]]; then
  echo "Usage: smoke-test.sh <target> [--skip-daemon] [--skip-cp] [--verbose]"
  exit 1
fi

# --- Configuration ---
# Timeout (seconds) for commands that might hang due to known bugs.
HANG_TIMEOUT=5

# --- Test Framework ---
PASS_COUNT=0
FAIL_COUNT=0
FAILURES=()

run_test() {
  local name="$1"; shift
  $VERBOSE && echo "+ $*"
  set +e
  timeout "$HANG_TIMEOUT" "$@" >/dev/null 2>&1
  local rc=$?
  set -e
  if [[ "$rc" -eq 124 ]]; then
    echo "[FAIL] $name (HUNG — killed after ${HANG_TIMEOUT}s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name [HUNG]")
  elif [[ "$rc" -eq 0 ]]; then
    echo "[PASS] $name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] $name (exit code $rc)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
  fi
}

run_test_output() {
  local name="$1"; shift
  local expected="$1"; shift
  $VERBOSE && echo "+ $*"
  set +e
  local output
  output=$(timeout "$HANG_TIMEOUT" "$@" 2>&1)
  local rc=$?
  set -e
  if [[ "$rc" -eq 124 ]]; then
    echo "[FAIL] $name (HUNG — killed after ${HANG_TIMEOUT}s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name [HUNG]")
    return
  fi
  if [[ "$rc" -ne 0 ]]; then
    echo "[FAIL] $name (exit code $rc)"
    $VERBOSE && echo "  output: $output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
    return
  fi
  if echo "$output" | grep -q "$expected"; then
    echo "[PASS] $name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] $name (expected '$expected' in output)"
    $VERBOSE && echo "  actual: $output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
  fi
}

run_test_exit_code() {
  local name="$1"; shift
  local expected_code="$1"; shift
  $VERBOSE && echo "+ $*"
  set +e
  timeout "$((HANG_TIMEOUT + 5))" "$@" >/dev/null 2>&1
  local actual_code=$?
  set -e
  if [[ "$actual_code" -eq 124 ]] && [[ "$expected_code" -ne 124 ]]; then
    echo "[FAIL] $name (HUNG — killed after $((HANG_TIMEOUT + 5))s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name [HUNG]")
  elif [[ "$actual_code" -eq "$expected_code" ]]; then
    echo "[PASS] $name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] $name (expected exit $expected_code, got $actual_code)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
  fi
}

# Run a test with a timeout guard. If the command hangs beyond HANG_TIMEOUT,
# it is killed and reported as FAIL (timeout/hang detected).
run_test_with_timeout() {
  local name="$1"; shift
  local expected="$1"; shift
  $VERBOSE && echo "+ timeout ${HANG_TIMEOUT}s $*"
  set +e
  local output
  output=$(timeout "$HANG_TIMEOUT" "$@" 2>&1)
  local rc=$?
  set -e
  if [[ "$rc" -eq 124 ]]; then
    echo "[FAIL] $name (HUNG — killed after ${HANG_TIMEOUT}s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name [HUNG]")
    return
  fi
  if [[ "$rc" -ne 0 ]]; then
    echo "[FAIL] $name (exit code $rc)"
    $VERBOSE && echo "  output: $output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
    return
  fi
  if echo "$output" | grep -q "$expected"; then
    echo "[PASS] $name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] $name (expected '$expected' in output)"
    $VERBOSE && echo "  actual: $output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
  fi
}

# Run a test that pipes stdin, with timeout guard against hangs.
run_test_stdin() {
  local name="$1"; shift
  local input="$1"; shift
  local expected="$1"; shift
  $VERBOSE && echo "+ echo '$input' | timeout ${HANG_TIMEOUT}s $*"
  set +e
  local output
  output=$(echo "$input" | timeout "$HANG_TIMEOUT" "$@" 2>&1)
  local rc=$?
  set -e
  if [[ "$rc" -eq 124 ]]; then
    echo "[FAIL] $name (HUNG — killed after ${HANG_TIMEOUT}s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name [HUNG]")
    return
  fi
  if [[ "$rc" -ne 0 ]]; then
    echo "[FAIL] $name (exit code $rc)"
    $VERBOSE && echo "  output: $output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
    return
  fi
  if echo "$output" | grep -q "$expected"; then
    echo "[PASS] $name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] $name (expected '$expected' in output)"
    $VERBOSE && echo "  actual: $output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
  fi
}

# Run cp with timeout guard against hangs.
run_test_cp() {
  local name="$1"; shift
  $VERBOSE && echo "+ timeout ${HANG_TIMEOUT}s $*"
  set +e
  timeout "$HANG_TIMEOUT" "$@" >/dev/null 2>&1
  local rc=$?
  set -e
  if [[ "$rc" -eq 124 ]]; then
    echo "[FAIL] $name (HUNG — killed after ${HANG_TIMEOUT}s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name [HUNG]")
  elif [[ "$rc" -eq 0 ]]; then
    echo "[PASS] $name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] $name (exit code $rc)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("$name")
  fi
}

# --- Tests ---
echo "=== xho E2E Smoke Tests ==="
echo "Target: $TARGET"
echo ""

# 1. exec - basic command
echo "--- exec ---"
run_test_output "exec: basic echo" "smoke_test_ok" \
  xho exec "$TARGET" -- echo smoke_test_ok

# 2. exec - exit code propagation
run_test_exit_code "exec: exit code 42" 42 \
  xho exec "$TARGET" -- bash -c "exit 42"

# 3. exec - multi-arg with --
run_test_output "exec: multi-arg ls" "tmp" \
  xho exec "$TARGET" -- ls /

# 4. exec - -t flag (TTY allocation)
run_test "exec: -t flag accepted" \
  xho exec -t "$TARGET" -- echo tty_test

# 5. exec - --no-tty
run_test_output "exec: --no-tty" "no_tty_ok" \
  xho exec --no-tty "$TARGET" -- echo no_tty_ok

# 6. exec - timeout (should complete before timeout)
run_test_output "exec: --timeout 10s" "timeout_ok" \
  xho exec --timeout 10s "$TARGET" -- echo timeout_ok

# 7. exec - timeout fires (1s timeout on sleep 10)
run_test_exit_code "exec: timeout fires" 124 \
  xho exec --timeout 1s "$TARGET" -- sleep 10

# 8. exec - shell wrapping
run_test_output "exec: --shell bash" "shell_ok" \
  xho exec --shell bash "$TARGET" -- echo shell_ok

# 9. exec - whoami returns something
run_test "exec: whoami" \
  xho exec "$TARGET" -- whoami

# 10. exec -i stdin forwarding (pipe data through cat)
echo ""
echo "--- exec -i (stdin forwarding) ---"
run_test_stdin "exec -i: pipe echo to cat" \
  "stdin_test_data" "stdin_test_data" \
  xho exec -i --no-tty "$TARGET" -- cat

run_test_stdin "exec -i: pipe multiline" \
  "line1" "line1" \
  xho exec -i --no-tty "$TARGET" -- cat

# 11. exec -i via direct target (to isolate gateway path)
if [[ "$TARGET" == *:* ]]; then
  DIRECT_TARGET="${TARGET#*:}"
  run_test_stdin "exec -i (direct): pipe to cat" \
    "direct_stdin_test" "direct_stdin_test" \
    xho exec -i --no-tty "$DIRECT_TARGET" -- cat
fi

# 10. ls
echo ""
echo "--- ls ---"
run_test "ls: server list" \
  xho ls

# 11. status
echo ""
echo "--- status ---"
run_test "status: daemon status" \
  xho status

# 12. host list
echo ""
echo "--- host ---"
run_test "host: list" \
  xho host list

# 13. cp tests
if [[ "$SKIP_CP" == "false" ]]; then
  echo ""
  echo "--- cp (direct) ---"
  
  # Determine direct target for cp
  if [[ "$TARGET" == *:* ]]; then
    DIRECT_TARGET="${TARGET#*:}"
  else
    DIRECT_TARGET="$TARGET"
  fi
  
  # Create temp file for upload
  TMPFILE=$(mktemp /tmp/xho_smoke_XXXXXX)
  echo "smoke_test_upload_$(date +%s)" > "$TMPFILE"
  REMOTE_PATH="/tmp/xho_smoke_test_file"
  
  # Upload via direct target
  run_test_cp "cp (direct): upload file" \
    xho cp "$TMPFILE" "$DIRECT_TARGET:$REMOTE_PATH"
  
  # Verify upload by reading back
  run_test_output "cp (direct): verify upload" "smoke_test_upload_" \
    xho exec "$TARGET" -- cat "$REMOTE_PATH"
  
  # Download via direct target
  DLFILE=$(mktemp /tmp/xho_smoke_dl_XXXXXX)
  rm -f "$DLFILE"
  run_test_cp "cp (direct): download file" \
    xho cp "$DIRECT_TARGET:$REMOTE_PATH" "$DLFILE"
  
  # Verify download content matches
  if [[ -f "$DLFILE" ]] && diff -q "$TMPFILE" "$DLFILE" >/dev/null 2>&1; then
    echo "[PASS] cp (direct): download content matches"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] cp (direct): download content matches"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILURES+=("cp (direct): download content matches")
  fi
  
  # Recursive copy via direct target
  TMPDIR=$(mktemp -d /tmp/xho_smoke_dir_XXXXXX)
  echo "file1" > "$TMPDIR/a.txt"
  echo "file2" > "$TMPDIR/b.txt"
  mkdir -p "$TMPDIR/sub"
  echo "file3" > "$TMPDIR/sub/c.txt"
  
  run_test_cp "cp (direct): recursive upload" \
    xho cp -r "$TMPDIR" "$DIRECT_TARGET:/tmp/xho_smoke_dir"
  
  run_test_output "cp (direct): verify recursive" "file3" \
    xho exec "$TARGET" -- cat /tmp/xho_smoke_dir/sub/c.txt
  
  # Cleanup direct cp test files
  rm -f "$TMPFILE" "$DLFILE"
  rm -rf "$TMPDIR"
  xho exec "$TARGET" -- rm -rf "$REMOTE_PATH" /tmp/xho_smoke_dir 2>/dev/null || true

  # --- cp via xhod jump host (tests the full relay path) ---
  if [[ "$TARGET" == *:* ]]; then
    echo ""
    echo "--- cp (via xhod jump host) ---"
    
    TMPFILE2=$(mktemp /tmp/xho_smoke_jh_XXXXXX)
    echo "jump_host_cp_test_$(date +%s)" > "$TMPFILE2"
    REMOTE_PATH2="/tmp/xho_smoke_jh_file"
    
    # Upload via jump host target (e.g., prod-xhod:web1:/path)
    run_test_cp "cp (xhod): upload file" \
      xho cp "$TMPFILE2" "$TARGET:$REMOTE_PATH2"
    
    # Download via jump host target
    DLFILE2=$(mktemp /tmp/xho_smoke_jh_dl_XXXXXX)
    rm -f "$DLFILE2"
    run_test_cp "cp (xhod): download file" \
      xho cp "$TARGET:$REMOTE_PATH2" "$DLFILE2"
    
    # Cleanup
    rm -f "$TMPFILE2" "$DLFILE2"
    xho exec "$TARGET" -- rm -f "$REMOTE_PATH2" 2>/dev/null || true
  fi
fi

# 14. daemon tests
if [[ "$SKIP_DAEMON" == "false" ]]; then
  echo ""
  echo "--- daemon ---"
  
  run_test "daemon: stop" \
    xho daemon stop
  
  # Small delay for socket cleanup
  sleep 1
  
  run_test "daemon: start" \
    xho daemon start
  
  # Verify daemon is back
  sleep 1
  run_test "daemon: status after restart" \
    xho status
fi

# --- Summary ---
echo ""
echo "=== Summary ==="
TOTAL=$((PASS_COUNT + FAIL_COUNT))
echo "Total: $TOTAL | Pass: $PASS_COUNT | Fail: $FAIL_COUNT"

if [[ ${#FAILURES[@]} -gt 0 ]]; then
  echo ""
  echo "Failed tests:"
  for f in "${FAILURES[@]}"; do
    echo "  - $f"
  done
  exit 1
fi

echo ""
echo "All tests passed!"
exit 0
