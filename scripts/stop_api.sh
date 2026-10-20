#!/usr/bin/env bash
# Stops the API server started by scripts/run_api.sh.
# Finds any process listening on the port, sends SIGTERM, waits, then SIGKILL
# if it doesn't exit in time.
# Usage: ./scripts/stop_api.sh [--port <port>]
#   --port <port>  Port to stop (default: ${SERVER_PORT:-3100})

set -euo pipefail

PORT="${SERVER_PORT:-3100}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port) PORT="$2"; shift 2 ;;
    *) echo "❌  Unknown flag: $1" >&2; exit 1 ;;
  esac
done

PIDS="$(lsof -ti :"${PORT}" 2>/dev/null || true)"

if [ -z "${PIDS}" ]; then
  echo "ℹ️   No process found listening on port ${PORT}."
  exit 0
fi

echo "🛑  Stopping process(es) on port ${PORT} (PIDs: ${PIDS})..."
echo "${PIDS}" | xargs kill 2>/dev/null || true

for _ in $(seq 1 10); do
  STILL_RUNNING="$(lsof -ti :"${PORT}" 2>/dev/null || true)"
  if [ -z "${STILL_RUNNING}" ]; then break; fi
  sleep 1
done

STILL_RUNNING="$(lsof -ti :"${PORT}" 2>/dev/null || true)"
if [ -n "${STILL_RUNNING}" ]; then
  echo "⚠️   Processes not responding to SIGTERM, sending SIGKILL..."
  echo "${STILL_RUNNING}" | xargs kill -9 2>/dev/null || true
  sleep 1
fi

if lsof -ti :"${PORT}" > /dev/null 2>&1; then
  echo "❌  Failed to stop process(es) on port ${PORT}." >&2
  exit 1
fi

echo "✅  API server on port ${PORT} stopped."
