#!/usr/bin/env bash
# Starts the history MCP server locally from a compiled binary.
# Builds bin/mcp on first run or when any file under cmd/ or internal/ (or go.mod /
# go.sum) is newer than the binary; --rebuild forces it. The binary is the same
# artifact `make build` produces, and /bin/ is gitignored.
# By default runs detached in the background with logs redirected to
# scripts/logs/ms-ml-conv-history-bs-mcp-<TIMESTAMP>.log and a health-check wait
# loop against GET /health. Use --foreground to run attached instead.
# If a process is already listening on the port, it is stopped first (unless --no-kill).
# The build happens before that stop, so a broken build leaves the running server
# untouched.
# Usage: ./scripts/run_mcp.sh [--rebuild] [--foreground] [--no-kill] [--port <port>]
#   --rebuild      Force delete the existing binary and rebuild before starting
#   --foreground   Run the server attached to this shell instead of in the background
#   --no-kill      Do not stop an existing process on the port; exit if already in use
#   --port <port>  Port to check and wait on (default: ${MCP_PORT:-3200}). The server
#                  listens on mcp.port of config.yaml, so pass the same value to both.

set -euo pipefail

# Resolve repo root relative to this script's location so it works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BINARY="${REPO_ROOT}/bin/mcp"
GO_BIN="$(command -v go || echo /usr/local/go/bin/go)"

FORCE_REBUILD=false
BACKGROUND=true
NO_KILL=false
PORT="${MCP_PORT:-3200}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rebuild) FORCE_REBUILD=true; shift ;;
    --foreground) BACKGROUND=false; shift ;;
    --no-kill) NO_KILL=true; shift ;;
    --port) PORT="$2"; shift 2 ;;
    *) echo "❌  Unknown flag: $1" >&2; exit 1 ;;
  esac
done

# The binary resolves .env and config.yaml relative to the working directory, so it
# only starts correctly from the repo root.
cd "${REPO_ROOT}"

# Everything the compiler reads lives under cmd/ and internal/ (including files
# pulled in with go:embed), plus the module files. Any file newer than the binary
# means it is stale.
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
  "${GO_BIN}" build -o "${BINARY}" ./cmd/mcp
  echo "✅  Build complete"
else
  echo "✅  Binary is up to date, skipping build"
fi

EXISTING_PIDS="$(lsof -ti :"${PORT}" 2>/dev/null || true)"
if [ -n "${EXISTING_PIDS}" ]; then
  if [ "${NO_KILL}" = true ]; then
    echo "❌  Port ${PORT} is already in use (PIDs: ${EXISTING_PIDS}). Exiting (--no-kill set)." >&2
    exit 1
  fi
  echo "⚠️   Stopping existing process(es) on port ${PORT} (PIDs: ${EXISTING_PIDS})..."
  echo "${EXISTING_PIDS}" | xargs kill 2>/dev/null || true
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
fi

if [ "${BACKGROUND}" != true ]; then
  echo "🚀  Starting MCP server on port ${PORT} (foreground)"
  exec "${BINARY}"
fi

mkdir -p "${REPO_ROOT}/scripts/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${REPO_ROOT}/scripts/logs/ms-ml-conv-history-bs-mcp-${TIMESTAMP}.log"

echo "🚀  Starting MCP server on port ${PORT} (background)"
echo "🔗  Health endpoint: http://localhost:${PORT}/health"
echo "📄  Logs: ${LOG_FILE}"

"${BINARY}" > "${LOG_FILE}" 2>&1 &
SERVER_PID=$!

MAX_RETRIES=30
for i in $(seq 1 "${MAX_RETRIES}"); do
  sleep 1
  # A binary that refuses its config (a wrong working directory, a bad value) exits at
  # once: say so instead of waiting out the whole loop.
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "❌  The MCP server exited. Check logs: ${LOG_FILE}" >&2
    exit 1
  fi
  if curl -sf "http://localhost:${PORT}/health" > /dev/null 2>&1; then
    echo "✅  MCP server is up (endpoint: http://localhost:${PORT}/mcp) (PID ${SERVER_PID})"
    exit 0
  fi
  echo "   Waiting for health check... (${i}/${MAX_RETRIES})"
done

echo "❌  MCP server did not become healthy in time. Check logs: ${LOG_FILE}" >&2
exit 1
