#!/usr/bin/env bash
# Stops the consumer started by scripts/run_consumer.sh.
# Finds it by its binary path (the consumer has no port), sends SIGTERM, waits so it
# can log its totals and commit its last offset, then SIGKILL if it doesn't exit in time.
# Usage: ./scripts/stop_consumer.sh [--timeout <seconds>]
#   --timeout <seconds>  How long to wait after SIGTERM (default: 30)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BINARY="${REPO_ROOT}/bin/consumer"
PID_FILE="${REPO_ROOT}/scripts/logs/consumer.pid"
TIMEOUT=30

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout) TIMEOUT="$2"; shift 2 ;;
    *) echo "❌  Unknown flag: $1" >&2; exit 1 ;;
  esac
done

# A running consumer is the binary started with no arguments; a `rebuild` run is not one.
find_pids() { pgrep -f "^${BINARY}\$" 2>/dev/null || true; }

PIDS="$(find_pids)"

if [ -z "${PIDS}" ]; then
  rm -f "${PID_FILE}"
  echo "ℹ️   No consumer is running."
  exit 0
fi

echo "🛑  Stopping the consumer (PIDs: ${PIDS})..."
echo "${PIDS}" | xargs kill 2>/dev/null || true

for _ in $(seq 1 "${TIMEOUT}"); do
  if [ -z "$(find_pids)" ]; then break; fi
  sleep 1
done

STILL_RUNNING="$(find_pids)"
if [ -n "${STILL_RUNNING}" ]; then
  echo "⚠️   Not responding to SIGTERM, sending SIGKILL..."
  echo "${STILL_RUNNING}" | xargs kill -9 2>/dev/null || true
  sleep 1
fi

if [ -n "$(find_pids)" ]; then
  echo "❌  Failed to stop the consumer." >&2
  exit 1
fi

rm -f "${PID_FILE}"
echo "✅  Consumer stopped."
