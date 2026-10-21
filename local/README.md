# Running the pipeline locally

The cluster (`../cluster`) provides Postgres, Kafka and Kafka Connect. The agent's
conversation events connector is registered from `../ms-ml-genai-agents-bs/local/debezium/register.sh`.

```bash
# tables (testing repo, migrations/history/conversation)
make migrations/up

# read the topic and build the history; stop with Ctrl-C (SIGTERM)
go run ./cmd/consumer

# recreate the chat messages from the inbox, then exit
go run ./cmd/consumer rebuild
```

Settings come from `config.yaml`; a local `.env` (git-ignored) can override them.

| Variable | Default |
|----------|---------|
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_DB` | `localhost`, `5432`, `postgres`, `postgres` |
| `KAFKA_CLIENT_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC_EVENTS` | `yape.public.conversation_events` |
| `KAFKA_GROUP_ID` | `ms-ml-conv-history-bs` |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` (`latest` logs a startup warning: it skips the history before the group's first offset) |

## End-to-end tests

```bash
make pipeline
```

Needs the cluster up and the connector `RUNNING`. It starts a throwaway Postgres for the
history, runs the real consumer binary and checks:

- rows inserted in the agent's `conversation_events` reach `conversation_messages` through
  Debezium and Kafka (the test removes its rows afterwards);
- the same record sent three times leaves one inbox row;
- records sent in reverse order across partitions read back in order;
- a tombstone and malformed records change nothing and do not block the partition;
- SIGTERM stops the process with exit 0 and every offset committed.

The Debezium test leaves its insert records on the shared topic (`conversation_id` starting
with `pipeline-dbz-`, account `acc-pipeline`). A consumer that reads the topic from the start
will project them; filter on that prefix or rebuild after a `--clean`.

## Verify the history against the event store

After running the explorer flows (`ml-genai-agents-bs-testing`: `cmd/p2p-explore/scripts/run.sh --flow <flow>`)
with the consumer running, and the API up for its part of the check:

```bash
local/scripts/verify.sh
```

It waits for the consumer to catch up, prints one line per conversation and ends with `OK` or
`MISMATCH` (exit 0 or 1):

| Check | What it compares |
|-------|------------------|
| Pipeline | every row of `conversation_events` is in the inbox |
| Per conversation | the user, agent and guardrail events of `conversation_events` against `conversation_messages` and against the API (it follows the `prev` links): same count, same text, same event, nothing missing or extra |
| Excluded events | no text of a blocked input, tool summary or notification is in the history or in any API reply |
| Rebuild | `consumer rebuild` leaves the messages table identical |

Settings: `PG_CONTAINER` (`yape-postgres`), `API_URL` (`http://localhost:3100`), `REQUIRE_API=1` to fail when
the API is down, `REBUILD=no` to skip the rebuild, `WAIT_SECONDS` (60). The script fails when it checks
fewer conversations than the event store holds. After `run.sh --clean`, empty the history too:

```bash
docker exec yape-postgres psql -U postgres -c "TRUNCATE conversation_messages, conversation_inbox"
```

## When events are deleted and written again

The inbox dedupes on the stream position, `(conversation_id, sequence)`. A repeat of a stored event
is ignored. A different event at a position the inbox already holds (name, version, payload or
metadata differ) is refused, counted as rejected and logged at error level.

That happens when events are deleted from the tail of a live conversation and the agent writes
new ones at the same sequence. Delete whole, closed conversations only. To start over, reset the
history and the consumer group's offsets:

```bash
(cd ../ml-genai-agents-bs-testing && make migrations/down SERVICE=history FLOW=conversation CONFIRM=yes && make migrations/up SERVICE=history)
docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group ms-ml-conv-history-bs --reset-offsets --to-earliest --topic yape.public.conversation_events --execute
```
