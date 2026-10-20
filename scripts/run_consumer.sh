#!/usr/bin/env bash
# Starts the history consumer locally from a compiled binary.
# Builds bin/consumer on first run or when any file under cmd/ or internal/ (or
# go.mod / go.sum) is newer than the binary; --rebuild forces it. The binary is the
# same artifact `make build` produces, and /bin/ is gitignored.
# By default runs detached in the background with logs redirected to
# scripts/logs/ms-ml-conv-history-bs-consumer-<TIMESTAMP>.log, its PID in
# scripts/logs/consumer.pid, and a wait loop for the line the binary logs once it
# has read its config and created its Kafka client. Use --foreground to run attached.
# The consumer has no port or health endpoint, so an existing consumer is found by its
# binary path. It is stopped first (unless --no-kill), with SIGTERM so it logs its
# totals and exits with its last offset committed. The build happens before that stop,
# so a broken build leaves the running consumer untouched.
# The group, topic and offset reset come from config.yaml and .env (KAFKA_GROUP_ID,
# KAFKA_TOPIC_OUTBOX, KAFKA_AUTO_OFFSET_RESET), or from the environment of this shell.
# Usage: ./scripts/run_consumer.sh [--rebuild] [--foreground] [--no-kill] [--replay]
#   --rebuild      Force delete the existing binary and rebuild before starting
#   --foreground   Run the consumer attached to this shell instead of in the background
#   --no-kill      Do not stop an existing consumer; exit if one is running
#   --replay       Run `consumer rebuild` (recreate the chat messages from the inbox) in
#                  the foreground and exit. Does not start or stop the consumer.

set -euo pipefail

# Resolve repo root relative to this script's location so it works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BINARY="${REPO_ROOT}/bin/consumer"
PID_FILE="${REPO_ROOT}/scripts/logs/consumer.pid"
GO_BIN="$(command -v go || echo /usr/local/go/bin/go)"

FORCE_REBUILD=false
BACKGROUND=true
NO_KILL=false
REPLAY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild) FORCE_REBUILD=true; shift ;;
    --foreground) BACKGROUND=false; shift ;;
    --no-kill) NO_KILL=true; shift ;;
    --replay) REPLAY=true; shift ;;
    *) echo "❌  Unknown flag: $1" >&2; exit 1 ;;
  esac
done

# The binary resolves .env and config.yaml relative to the working directory, so it
# only starts correctly from the repo root.
cd "${REPO_ROOT}"

NEEDS_BUILD=false
if [ "${FORCE_REBUILD}" = true ]; then
  echo "🗑️   --rebuild flag set, removing existing binary..."
  rm -f "${BINARY}"
  NEEDS_BUILD=true
elif [ ! -f "${BINARY}" ]; then
  NEEDS_BUILD=true
elif [ go.mod -nt "${BINARY}" ] || [ go.sum -nt "${BINARY}" ]; then
  NEEDS_BUILD=true
elif [ -n "$(find cmd internal -type f -newer "${BINARY}" 2>/dev/null | head -1)" ]; then
  NEEDS_BUILD=true
fi

if [ "${NEEDS_BUILD}" = true ]; then
  echo "🔨  Building ${BINARY}..."
  "${GO_BIN}" build -o "${BINARY}" ./cmd/consumer
  echo "✅  Build complete"
else
  echo "✅  Binary is up to date, skipping build"
fi

if [ "${REPLAY}" = true ]; then
  echo "♻️   Recreating the chat messages from the inbox"
  exec "${BINARY}" rebuild
fi

# A running consumer is the binary started with no arguments; a `rebuild` run is not one.
EXISTING_PIDS="$(pgrep -f "^${BINARY}\$" 2>/dev/null || true)"
if [ -n "${EXISTING_PIDS}" ]; then
  if [ "${NO_KILL}" = true ]; then
    echo "❌  A consumer is already running (PIDs: ${EXISTING_PIDS}). Exiting (--no-kill set)." >&2
    exit 1
  fi
  echo "⚠️   Stopping the running consumer (PIDs: ${EXISTING_PIDS})..."
  echo "${EXISTING_PIDS}" | xargs kill 2>/dev/null || true
  # SIGTERM lets it commit its last offset and log its totals; give it time.
  for _ in $(seq 1 30); do
    STILL_RUNNING="$(pgrep -f "^${BINARY}\$" 2>/dev/null || true)"
    if [ -z "${STILL_RUNNING}" ]; then break; fi
    sleep 1
  done
  STILL_RUNNING="$(pgrep -f "^${BINARY}\$" 2>/dev/null || true)"
  if [ -n "${STILL_RUNNING}" ]; then
    echo "⚠️   Not responding to SIGTERM, sending SIGKILL..."
    echo "${STILL_RUNNING}" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
  rm -f "${PID_FILE}"
fi

if [ "${BACKGROUND}" != true ]; then
  echo "🚀  Starting the consumer (foreground)"
  exec "${BINARY}"
fi

mkdir -p "${REPO_ROOT}/scripts/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${REPO_ROOT}/scripts/logs/ms-ml-conv-history-bs-consumer-${TIMESTAMP}.log"

echo "🚀  Starting the consumer (background)"
echo "📄  Logs: ${LOG_FILE}"

nohup "${BINARY}" > "${LOG_FILE}" 2>&1 &
CONSUMER_PID=$!
echo "${CONSUMER_PID}" > "${PID_FILE}"

MAX_RETRIES=30
for i in $(seq 1 "${MAX_RETRIES}"); do
  sleep 1
  # A consumer that refuses its config or cannot reach the database exits at once.
  if ! kill -0 "${CONSUMER_PID}" 2>/dev/null; then
    rm -f "${PID_FILE}"
    echo "❌  The consumer exited. Check logs: ${LOG_FILE}" >&2
    exit 1
  fi
  if grep -q '"message":"consuming ' "${LOG_FILE}" 2>/dev/null; then
    echo "✅  Consumer is up (PID ${CONSUMER_PID}): $(grep -o '"message":"consuming [^"]*' "${LOG_FILE}" | head -1 | cut -d'"' -f4)"
    exit 0
  fi
  echo "   Waiting for the consumer to start... (${i}/${MAX_RETRIES})"
done

echo "❌  The consumer did not report it was consuming in time. Check logs: ${LOG_FILE}" >&2
exit 1
