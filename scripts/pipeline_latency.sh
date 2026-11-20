#!/usr/bin/env bash
# Measures how long an event takes to get from the agent to the history: the seconds between
# occurred_at (when the agent recorded the event) and received_at (when the consumer stored it in
# conversation_inbox), as percentiles over a window. It only reads: one SELECT, and with --lag the
# consumer groups' offsets from Kafka.
#
# The two times come from two clocks (the agent's and the database's): on one machine they agree, and
# across machines the skew is part of what is measured, so read a figure of a few milliseconds with that in mind.
#
# Usage: ./scripts/pipeline_latency.sh [--window "1 day"] [--schema history] [--account <id>] [--by-partition] [--lag]
#   --window <n unit>   The time looked back, such as "30 minutes", "6 hours" or "1 day" (default "1 day")
#   --schema <name>     The schema of the inbox: history (default), the one the Camel consumers fill
#   --account <id>      Only the events of one account
#   --by-partition      Also show each Kafka partition apart: a partition that is slower than the rest is a hot key
#   --lag               Also show the lag of each consumer group of the topic (needs the local Kafka container)
#
# Where the database is: psql with POSTGRES_DSN when psql is installed and the variable is set, else the local cluster's
# container (POSTGRES_CONTAINER, default yape-postgres) as the user POSTGRES_USER (default postgres) in the database POSTGRES_DB
# (default postgres). Kafka for --lag: KAFKA_CONTAINER (default yape-kafka).

set -euo pipefail

WINDOW="1 day"
SCHEMA="history"
ACCOUNT=""
BY_PARTITION=false
LAG=false
TOPIC="yape.event_store.conversation_events"

usage() { sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --window)       WINDOW="${2:?--window needs a value}"; shift 2 ;;
    --schema)       SCHEMA="${2:?--schema needs a value}"; shift 2 ;;
    --account)      ACCOUNT="${2:?--account needs a value}"; shift 2 ;;
    --by-partition) BY_PARTITION=true; shift ;;
    --lag)          LAG=true; shift ;;
    -h|--help)      usage; exit 0 ;;
    *)              echo "pipeline_latency: unknown option $1" >&2; usage >&2; exit 2 ;;
  esac
done

# What goes into the SQL is checked here: the window and the account travel as psql variables, the schema is a name.
[[ "$WINDOW" =~ ^[0-9]+\ (second|minute|hour|day)s?$ ]] || { echo "pipeline_latency: --window is a number and a unit, such as \"6 hours\"" >&2; exit 2; }
[[ "$SCHEMA" =~ ^[a-z_][a-z0-9_]{0,62}$ ]] || { echo "pipeline_latency: --schema is a schema name" >&2; exit 2; }
[[ -z "$ACCOUNT" || "$ACCOUNT" =~ ^[A-Za-z0-9._:-]{1,128}$ ]] || { echo "pipeline_latency: --account is not an account id" >&2; exit 2; }

run_sql() {
  if command -v psql >/dev/null 2>&1 && [ -n "${POSTGRES_DSN:-}" ]; then
    psql "$POSTGRES_DSN" -X -q -v ON_ERROR_STOP=1 -v window="$WINDOW" -v account="$ACCOUNT" "$@"
  else
    docker exec -i "${POSTGRES_CONTAINER:-yape-postgres}" \
      psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-postgres}" -X -q -v ON_ERROR_STOP=1 -v window="$WINDOW" -v account="$ACCOUNT" "$@"
  fi
}

# One row for the whole window, and with --by-partition one for each partition as well.
GROUP_LABEL="'all'"
GROUP_CLAUSE=""
if $BY_PARTITION; then
  GROUP_LABEL="CASE WHEN GROUPING(kafka_partition) = 1 THEN 'all' ELSE 'partition ' || kafka_partition END"
  GROUP_CLAUSE="GROUP BY ROLLUP (kafka_partition)"
fi

echo "Seconds from the event to the inbox (${SCHEMA}.conversation_inbox), last ${WINDOW}${ACCOUNT:+, account ${ACCOUNT}}:"
run_sql <<SQL
SELECT ${GROUP_LABEL} AS "group",
       count(*)                                                                    AS events,
       round(avg(lat)::numeric, 3)                                                 AS avg_s,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY lat)::numeric, 3)        AS p50_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY lat)::numeric, 3)        AS p95_s,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY lat)::numeric, 3)        AS p99_s,
       round(max(lat)::numeric, 3)                                                 AS max_s
FROM (
    SELECT extract(epoch FROM received_at - occurred_at) AS lat, kafka_partition
    FROM ${SCHEMA}.conversation_inbox
    WHERE received_at > now() - :'window'::interval
      AND (:'account' = '' OR id_yape_account = :'account')
) x
${GROUP_CLAUSE}
ORDER BY 1;
SQL

if $LAG; then
  echo
  echo "Lag of the consumer groups of ${TOPIC} (records the group has not read yet; 0 is caught up):"
  KAFKA="${KAFKA_CONTAINER:-yape-kafka}"
  for group in $(docker exec "$KAFKA" kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null | sort); do
    # kafka-consumer-groups prints GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
    docker exec "$KAFKA" kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "$group" 2>/dev/null \
      | awk -v t="$TOPIC" '$2 == t { printf "  %-30s partition %-3s lag %s\n", $1, $3, $6 }'
  done
fi
