#!/bin/sh
#
# Project conventions that golangci-lint cannot express, in the style of
# cpgs-clg-core. They keep race conditions from being introduced.
#
#   goroutine  production code starts no goroutine of its own: the service reads and
#              answers, and the HTTP server runs each request. Shared state is
#              covered with the helpers of internal/testsupport/concurrent under
#              -race. cmd/ wires the process and may start one.
#   sleep      a test does not wait with time.Sleep: a sleep makes a race pass by
#              luck. Use the concurrent helpers, which wait for the tasks
#              themselves. The integration helpers that poll a cluster are listed.
#
# POSIX sh and grep only, so it runs on macOS and under busybox.
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
status=0

# Files that may start goroutines or sleep, for a stated reason.
GO_ALLOWED='^(internal/testsupport/concurrent/concurrent\.go|cmd/.*)$'
SLEEP_ALLOWED='^(it/history/binary_test\.go)$'

files=$(find internal cmd it -type f -name '*.go' 2>/dev/null | sort)
[ -n "$files" ] || { echo "check-conventions: no Go sources found, refusing to pass" >&2; exit 2; }

# violations RULE PATTERN ALLOWED PRODUCTION_ONLY: print file:line for each match.
violations() {
	rule="$1"; pattern="$2"; allowed="$3"; production_only="$4"
	for f in $files; do
		case "$f" in *_test.go) is_test=1 ;; *) is_test=0 ;; esac
		[ "$production_only" = 1 ] && [ "$is_test" = 1 ] && continue
		[ "$production_only" = 0 ] && [ "$is_test" = 0 ] && continue
		printf '%s\n' "$f" | grep -Eq "$allowed" && continue
		grep -nE "$pattern" "$f" /dev/null | sed "s|^|$rule: |" || true
	done
}

found=$(violations goroutine '^[[:space:]]*go[[:space:]]+(func|[A-Za-z_])' "$GO_ALLOWED" 1)
if [ -n "$found" ]; then
	echo "$found"
	echo "check-conventions: production code starts a goroutine. Keep goroutines in cmd/, and cover shared state with internal/testsupport/concurrent under -race." >&2
	status=1
fi

found=$(violations sleep 'time\.Sleep\(' "$SLEEP_ALLOWED" 0)
if [ -n "$found" ]; then
	echo "$found"
	echo "check-conventions: a test waits with time.Sleep. Use internal/testsupport/concurrent." >&2
	status=1
fi

[ "$status" -eq 0 ] && echo "check-conventions: ok"
exit "$status"
