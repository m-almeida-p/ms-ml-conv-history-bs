# Running the pipeline locally

The cluster (`../cluster`) provides Postgres, Kafka and Kafka Connect. The agent's
outbox connector is registered from `../ms-ml-genai-agents-bs/local/debezium/register.sh`.

```bash
# tables (testing repo, migrations/history/p2p)
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
| `KAFKA_TOPIC_OUTBOX` | `yape.public.conversation_outbox` |
| `KAFKA_GROUP_ID` | `ms-ml-conv-history-bs` |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` |

## End-to-end tests

```bash
make pipeline
```

Needs the cluster up and the connector `RUNNING`. It starts a throwaway Postgres for the
history, runs the real consumer binary and checks:

- rows inserted in the agent's `conversation_outbox` reach `conversation_messages` through
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
| Pipeline | every outbox row is in the inbox |
| Per conversation | the user, agent and guardrail events of `conversation_events` against `conversation_messages` and against the API (it follows the `prev` links): same count, same text, same event, nothing missing or extra |
| Excluded events | no text of a blocked input, tool summary or notification is in the history or in any API reply |
| Rebuild | `consumer rebuild` leaves the messages table identical |

Settings: `PG_CONTAINER` (`yape-postgres`), `API_URL` (`http://localhost:3100`), `REQUIRE_API=1` to fail when
the API is down, `REBUILD=no` to skip the rebuild, `WAIT_SECONDS` (60). The script fails when it checks
fewer conversations than the event store holds. After `run.sh --clean`, empty the history too:

```bash
docker exec yape-postgres psql -U postgres -c "TRUNCATE conversation_messages, conversation_inbox"
```

## When the agent's outbox is recreated

The inbox dedupes on the outbox `id`. A repeated id is compared with the stored
`(conversation_id, sequence)`: a genuine repeat is ignored, and an event that reuses the id of a
different event is refused, counted as rejected and logged at error level.

If the agent's tables are dropped and created again, its ids restart at 1, and a history that kept
old rows refuses every new event that reuses an old id. Reset the history too, and reset the
consumer group's offsets:

```bash
(cd ../ml-genai-agents-bs-testing && make migrations/down SERVICE=history FLOW=p2p CONFIRM=yes && make migrations/up SERVICE=history)
docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group ms-ml-conv-history-bs --reset-offsets --to-earliest --topic yape.public.conversation_outbox --execute
```
