#!/usr/bin/env bash
# Compares the chat history with the agent's event store, after the explorer flows
# (ml-genai-agents-bs-testing: cmd/p2p-explore/scripts/run.sh --flow <flow>) and a
# consumer run have filled both. It prints one line per conversation and ends with
# OK or MISMATCH (exit 0 or 1). It checks:
#   1. every event of the event store is in the inbox;
#   2. per conversation, the history holds exactly the user, agent and guardrail
#      messages of the event store, with the same text, event and position;
#   3. no text of a blocked input, a tool summary or a notification is in the history
#      or in any API reply;
#   4. the API serves each conversation completely (it follows the prev links);
#   5. `consumer rebuild` reproduces the messages table.
# Settings (environment): PG_CONTAINER (yape-postgres), API_URL (http://localhost:3100),
# REQUIRE_API=1 to fail when the API is down, REBUILD=no to skip the rebuild,
# WAIT_SECONDS (60) for the consumer to catch up.
set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-yape-postgres}"
API_URL="${API_URL:-http://localhost:3100}"
REBUILD="${REBUILD:-yes}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

psql_q() { docker exec -i "${PG_CONTAINER}" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -At -F '|' "$@"; }
mismatches=0
note() { echo "  MISMATCH: $*"; mismatches=$((mismatches + 1)); }

CHAT="'UserMessageSent','AgentMessageSent','GuardrailResponseSent'"

echo "== 1. Pipeline: event store -> inbox"
events="$(psql_q -c 'select count(*) from conversation_events')"
[[ "${events}" -gt 0 ]] || { echo "No events in the event store. Run the explorer flows first."; exit 2; }

for ((i = 0; i < WAIT_SECONDS; i += 2)); do
  missing="$(psql_q -c 'select count(*) from conversation_events e left join conversation_inbox i using (conversation_id, sequence) where i.sequence is null')"
  [[ "${missing}" -eq 0 ]] && break
  sleep 2
done
inbox="$(psql_q -c 'select count(*) from conversation_inbox')"
echo "  events=${events} inbox=${inbox} events-missing-from-inbox=${missing}"
[[ "${missing}" -eq 0 ]] || note "${missing} events never reached the inbox (is the consumer running?)"

echo
echo "== 2. Per conversation: event store vs history vs API"
api_up=no
if curl -fsS -m 3 "${API_URL}/health" >/dev/null 2>&1; then api_up=yes; fi
[[ "${api_up}" == yes ]] || echo "  (API at ${API_URL} is not answering; its checks are skipped)"

printf '  %-20s %7s %8s %5s %9s %7s %6s  %s\n' conversation chat history api text-diff missing extra status
rows="$(psql_q <<SQL
WITH chat AS (
  SELECT conversation_id, sequence, event_name, COALESCE(payload->>'text', payload->>'response_text') AS text
  FROM conversation_events WHERE event_name IN (${CHAT})
)
SELECT c.conversation_id,
  (SELECT count(*) FROM chat WHERE chat.conversation_id = c.conversation_id),
  (SELECT count(*) FROM conversation_messages m WHERE m.conversation_id = c.conversation_id),
  (SELECT count(*) FROM chat JOIN conversation_messages m USING (conversation_id, sequence)
     WHERE chat.conversation_id = c.conversation_id AND (m.text IS DISTINCT FROM chat.text OR m.source_event <> chat.event_name)),
  (SELECT count(*) FROM chat WHERE chat.conversation_id = c.conversation_id
     AND NOT EXISTS (SELECT 1 FROM conversation_messages m WHERE m.conversation_id = chat.conversation_id AND m.sequence = chat.sequence)),
  (SELECT count(*) FROM conversation_messages m WHERE m.conversation_id = c.conversation_id
     AND NOT EXISTS (SELECT 1 FROM chat WHERE chat.conversation_id = m.conversation_id AND chat.sequence = m.sequence))
FROM (SELECT conversation_id, min(occurred_at) AS first FROM conversation_events GROUP BY 1) c
ORDER BY c.first
SQL
)"

api_texts="$(mktemp)"; trap 'rm -f "${api_texts}"' EXIT
conversations=0
while IFS='|' read -r conv expected stored textdiff missing_c extra <&3; do
  conversations=$((conversations + 1))
  api_count="-"; api_status=""
  if [[ "${api_up}" == yes ]]; then
    path="/v1/conversations/${conv}/messages"; api_lines="$(mktemp)"
    while [[ -n "${path}" ]]; do
      body="$(curl -fsS -g -m 10 "${API_URL}${path}")" || { api_status=" api-error"; break; }
      jq -r '.messages[] | "\(.metadata.sequence)|\(.text | @json)"' <<<"${body}" >>"${api_lines}"
      path="$(jq -r '.links.prev // empty' <<<"${body}")"
    done
    api_count="$(wc -l <"${api_lines}" | tr -d ' ')"
    cut -d'|' -f2- "${api_lines}" >>"${api_texts}"
    db_lines="$(psql_q -c "select sequence || '|' || to_json(text)::text from conversation_messages where conversation_id = '${conv}'" | sort)"
    [[ "$(sort "${api_lines}")" == "${db_lines}" ]] || api_status="${api_status} api-differs"
    rm -f "${api_lines}"
  fi

  status=OK
  if [[ "${expected}" != "${stored}" || "${textdiff}" != 0 || "${missing_c}" != 0 || "${extra}" != 0 || -n "${api_status}" || ( "${api_up}" == yes && "${api_count}" != "${stored}" ) ]]; then
    status="MISMATCH${api_status}"; mismatches=$((mismatches + 1))
  fi
  printf '  %-20s %7s %8s %5s %9s %7s %6s  %s\n' "${conv:0:20}" "${expected}" "${stored}" "${api_count}" "${textdiff}" "${missing_c}" "${extra}" "${status}"
done 3<<<"${rows}" # fd 3, so docker exec -i inside the loop cannot eat the rows
[[ "${api_up}" == yes || "${REQUIRE_API:-}" != 1 ]] || note "the API is not answering and REQUIRE_API=1"

echo
echo "== 3. Nothing from blocked inputs, tool summaries or notifications"
forbidden="$(psql_q <<'SQL'
SELECT DISTINCT to_json(t)::text FROM (
  SELECT payload->>'text' AS t FROM conversation_events WHERE event_name IN ('UserMessageBlocked', 'AgentNotificationDelivered')
  UNION ALL SELECT payload->>'outcome_summary' FROM conversation_events WHERE event_name = 'ToolActionSummarized'
) s WHERE t IS NOT NULL AND t <> '' ORDER BY 1
SQL
)"
other_events="$(psql_q -c "select count(*) from conversation_messages where source_event not in (${CHAT})")"
in_history="$(psql_q -c "select count(*) from conversation_messages m where to_json(m.text)::text in (select to_json(t)::text from (select payload->>'text' t from conversation_events where event_name in ('UserMessageBlocked','AgentNotificationDelivered') union all select payload->>'outcome_summary' from conversation_events where event_name = 'ToolActionSummarized') s where t is not null and t <> '')")"
in_api=0
if [[ "${api_up}" == yes && -n "${forbidden}" ]]; then
  in_api="$(sort -u "${api_texts}" | grep -c -x -F -f <(echo "${forbidden}") || true)"
fi
echo "  messages-from-other-events=${other_events} texts-of-excluded-events-in-history=${in_history} in-api=${in_api}"
[[ "${other_events}" -eq 0 && "${in_history}" -eq 0 && "${in_api}" -eq 0 ]] || note "text of an excluded event reached the history"

echo
echo "== 4. Rebuild reproduces the messages"
digest() { psql_q -c "select coalesce(md5(string_agg(m::text, E'\n' order by conversation_id, sequence)), '') from conversation_messages m"; }
if [[ "${REBUILD}" == yes ]]; then
  before="$(digest)"
  (cd "${REPO}" && go run ./cmd/consumer rebuild) >/dev/null 2>&1 || note "consumer rebuild failed"
  after="$(digest)"
  if [[ "${before}" == "${after}" ]]; then echo "  digest ${before} before and after"; else note "digest changed: ${before} -> ${after}"; fi
else
  echo "  skipped (REBUILD=no)"
fi

expected_conversations="$(psql_q -c 'select count(distinct conversation_id) from conversation_events')"
[[ "${conversations}" -eq "${expected_conversations}" ]] || note "checked ${conversations} of ${expected_conversations} conversations"

echo
echo "conversations=${conversations} mismatches=${mismatches}"
if [[ "${mismatches}" -eq 0 ]]; then echo "OK"; else echo "MISMATCH"; exit 1; fi
