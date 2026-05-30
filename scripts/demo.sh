#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

WAL_SYNC_WRITES="${WAL_SYNC_WRITES:-true}"
export WAL_SYNC_WRITES
DEMO_QUICK_MODE="${DEMO_QUICK_MODE:-false}"
DEMO_ONLY="${DEMO_ONLY:-}"
DEMO_LIST="${DEMO_LIST:-false}"
DEMO_SUMMARY_JSON="${DEMO_SUMMARY_JSON:-false}"
DEMO_SUMMARY_JSON_PATH="${DEMO_SUMMARY_JSON_PATH:-}"

DEMO_TARGETS=(basic restart sqlite cache fuse_api fuse_daemon)

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not installed or not on PATH."
  echo "Install Rust toolchain, then run:"
  echo "  cargo run -p fs_core --example basic_demo"
  echo "  cargo run -p fs_core --example restart_recovery_demo"
  echo "  cargo run -p fs_core --example sqlite_metadata_demo"
  echo "  cargo run -p fs_core --example persistent_cache_demo"
  echo "  cargo run -p fuse --example fuse_api_demo"
  exit 1
fi

echo "==> WAL_SYNC_WRITES=${WAL_SYNC_WRITES}"
if [[ -n "${CHUNK_SIZE_BYTES:-}" ]]; then
  echo "==> CHUNK_SIZE_BYTES=${CHUNK_SIZE_BYTES}"
fi
if [[ -n "${GC_RETENTION_MS:-}" ]]; then
  echo "==> GC_RETENTION_MS=${GC_RETENTION_MS}"
fi
if [[ -n "${GC_DELETE_BUDGET:-}" ]]; then
  echo "==> GC_DELETE_BUDGET=${GC_DELETE_BUDGET}"
fi
if [[ -n "${GC_MAX_ENQUEUED_SCANS:-}" ]]; then
  echo "==> GC_MAX_ENQUEUED_SCANS=${GC_MAX_ENQUEUED_SCANS}"
fi
if [[ -n "${GC_SCHEDULER_INTERVAL_MS:-}" ]]; then
  echo "==> GC_SCHEDULER_INTERVAL_MS=${GC_SCHEDULER_INTERVAL_MS}"
fi
if [[ -n "${GC_SCHEDULER_WAIT_MS:-}" ]]; then
  echo "==> GC_SCHEDULER_WAIT_MS=${GC_SCHEDULER_WAIT_MS}"
fi
if [[ -n "${FUSE_DAEMON_REQUEST_TIMEOUT_MS:-}" ]]; then
  echo "==> FUSE_DAEMON_REQUEST_TIMEOUT_MS=${FUSE_DAEMON_REQUEST_TIMEOUT_MS}"
fi
if [[ -n "${FUSE_TIMEOUT_SMOKE:-}" ]]; then
  echo "==> FUSE_TIMEOUT_SMOKE=${FUSE_TIMEOUT_SMOKE}"
fi
if [[ -n "${FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT:-}" ]]; then
  echo "==> FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT=${FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT}"
fi
if [[ -n "${FUSE_TIMEOUT_SMOKE_DELAY_MS:-}" ]]; then
  echo "==> FUSE_TIMEOUT_SMOKE_DELAY_MS=${FUSE_TIMEOUT_SMOKE_DELAY_MS}"
fi
if [[ -n "${FUSE_TIMEOUT_SMOKE_NEGATIVE_CHECK:-}" ]]; then
  echo "==> FUSE_TIMEOUT_SMOKE_NEGATIVE_CHECK=${FUSE_TIMEOUT_SMOKE_NEGATIVE_CHECK}"
fi
echo "==> DEMO_QUICK_MODE=${DEMO_QUICK_MODE}"
if [[ -n "${DEMO_ONLY}" ]]; then
  case "${DEMO_ONLY}" in
    all|basic|restart|sqlite|cache|fuse_api|fuse_daemon) ;;
    *)
      echo "error: invalid DEMO_ONLY='${DEMO_ONLY}'."
      echo "Valid values: all, basic, restart, sqlite, cache, fuse_api, fuse_daemon"
      exit 1
      ;;
  esac
  if [[ "${DEMO_ONLY}" == "all" ]]; then
    DEMO_ONLY=""
    echo "==> DEMO_ONLY=all (no single-target filter)"
  else
  echo "==> DEMO_ONLY=${DEMO_ONLY}"
  fi
fi
echo "==> DEMO_LIST=${DEMO_LIST}"
echo "==> DEMO_SUMMARY_JSON=${DEMO_SUMMARY_JSON}"
if [[ -n "${DEMO_SUMMARY_JSON_PATH}" ]]; then
  echo "==> DEMO_SUMMARY_JSON_PATH=${DEMO_SUMMARY_JSON_PATH}"
fi

if [[ "${DEMO_LIST}" == "true" ]]; then
  echo "Available demo targets:"
  for target in "${DEMO_TARGETS[@]}"; do
    echo "  - ${target}"
  done
  echo "Use DEMO_ONLY=<target> to run exactly one demo, or DEMO_ONLY=all for normal flow."
  exit 0
fi

run_demo() {
  local key="$1"
  local label="$2"
  shift 2
  if [[ -n "${DEMO_ONLY}" && "${DEMO_ONLY}" != "${key}" ]]; then
    return 0
  fi
  echo "==> Running ${label}"
  "$@"
  RAN_DEMOS+=("${key}")
}

RAN_DEMOS=()
NEGATIVE_SMOKE_RAN=false

run_demo "basic" "basic demo" cargo run -p fs_core --example basic_demo

if [[ "${DEMO_QUICK_MODE}" != "true" ]]; then
  run_demo "restart" "restart recovery demo" cargo run -p fs_core --example restart_recovery_demo

  run_demo "sqlite" "SQLite metadata demo" cargo run -p fs_core --example sqlite_metadata_demo

  run_demo "cache" "persistent cache demo" cargo run -p fs_core --example persistent_cache_demo
else
  echo "==> Quick mode: skipping restart/sqlite/persistent cache demos"
fi

run_demo "fuse_api" "FUSE API demo" cargo run -p fuse --example fuse_api_demo

run_demo "fuse_daemon" "FUSE daemon demo" cargo run -p fuse --example fuse_daemon_demo

if [[ "${FUSE_TIMEOUT_SMOKE_NEGATIVE_CHECK:-}" == "true" ]]; then
  echo "==> Running FUSE daemon negative timeout-smoke validation check"
  set +e
  FUSE_TIMEOUT_SMOKE=true \
  FUSE_TIMEOUT_SMOKE_EXPECT_TIMEOUT=true \
  FUSE_TIMEOUT_SMOKE_DELAY_MS="${FUSE_TIMEOUT_SMOKE_DELAY_MS:-80}" \
  cargo run -p fuse --example fuse_daemon_demo >/tmp/fuse_timeout_smoke_negative.log 2>&1
  neg_rc=$?
  set -e
  if [[ $neg_rc -eq 0 ]]; then
    echo "error: expected fuse_daemon_demo to fail strict timeout-smoke validation, but it succeeded."
    cat /tmp/fuse_timeout_smoke_negative.log
    exit 1
  fi
  echo "Negative timeout-smoke validation failed as expected."
  NEGATIVE_SMOKE_RAN=true
fi

if [[ ${#RAN_DEMOS[@]} -gt 0 ]]; then
  echo "==> Demo summary: ran targets: ${RAN_DEMOS[*]}"
else
  echo "==> Demo summary: no demo targets were run"
fi
echo "==> Demo summary: negative_timeout_smoke_check=${NEGATIVE_SMOKE_RAN}"

if [[ "${DEMO_SUMMARY_JSON}" == "true" ]]; then
  ran_targets_json="["
  if [[ ${#RAN_DEMOS[@]} -gt 0 ]]; then
    for idx in "${!RAN_DEMOS[@]}"; do
      target="${RAN_DEMOS[$idx]}"
      if [[ $idx -gt 0 ]]; then
        ran_targets_json+=","
      fi
      ran_targets_json+="\"${target}\""
    done
  fi
  ran_targets_json+="]"
  summary_demo_only_json="null"
  if [[ -n "${DEMO_ONLY}" ]]; then
    summary_demo_only_json="\"${DEMO_ONLY}\""
  fi
  wal_sync_writes_json=false
  case "${WAL_SYNC_WRITES}" in
    true|TRUE|True|1|yes|YES|on|ON) wal_sync_writes_json=true ;;
  esac
  summary_json=$(printf '{"ran_targets":%s,"negative_timeout_smoke_check":%s,"quick_mode":%s,"demo_only":%s,"demo_list":%s,"wal_sync_writes":%s}' \
    "${ran_targets_json}" \
    "${NEGATIVE_SMOKE_RAN}" \
    "${DEMO_QUICK_MODE}" \
    "${summary_demo_only_json}" \
    "${DEMO_LIST}" \
    "${wal_sync_writes_json}")
  printf '==> Demo summary json: %s\n' "${summary_json}"
  if [[ -n "${DEMO_SUMMARY_JSON_PATH}" ]]; then
    summary_dir="$(dirname "${DEMO_SUMMARY_JSON_PATH}")"
    mkdir -p "${summary_dir}"
    printf '%s\n' "${summary_json}" > "${DEMO_SUMMARY_JSON_PATH}"
    echo "==> Demo summary json written to ${DEMO_SUMMARY_JSON_PATH}"
  fi
fi

echo "==> Demo run completed"
