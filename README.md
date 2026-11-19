# ms-ml-conv-history-bs

Conversation history for the Yape assistant. It receives the agent's conversation events from
Kafka (published by Debezium), keeps every one in an inbox, projects the chat between the
user and the assistant into a history table, and serves that history over a small HTTP API
shaped after the Smooch message.

PoC for AIP-472. The API and the MCP server have no authentication, so this service must not be deployed.

```mermaid
flowchart LR
    A["Agent<br/>conversation_events<br/>one transaction"] -->|"WAL"| D["Kafka Connect + Debezium<br/>only public.conversation_events"]
    D --> K[("Topic<br/>yape.public.conversation_events<br/>3 partitions")]
    K --> C["cmd/consumer"]
    C -->|"one transaction"| I[("conversation_inbox")]
    C -->|"same transaction"| M[("conversation_messages")]
    C -->|"same transaction"| S[("conversation_summaries")]
    M --> H["cmd/api :3100"]
    S --> H
    H --> U["Client"]
    I --> G["cmd/mcp :3200"]
    S --> G
    G --> AG["Agent<br/>(MCP client)"]
```

| Binary | Role | Port |
|--------|------|------|
| `cmd/consumer` | Kafka -> inbox -> chat messages. `consumer rebuild` recreates the messages from the inbox | none |
| `cmd/api` | Reads the history | `3100` |
| `cmd/mcp` | Serves the history and statistics about it to an agent as MCP tools | `3200` |

## Run

Both binaries read `config.yaml` from the working directory, so start them from the repo root.

```bash
make migrations/up        # create the tables (the testing repo owns the SQL)
go run ./cmd/consumer     # read the topic; stop with Ctrl-C (SIGTERM)
go run ./cmd/api          # serve on :3100
go run ./cmd/mcp          # serve MCP on :3200
go run ./cmd/consumer rebuild
```

The same, detached, with logs and a start check, as in the agent repo (`scripts/`):

```bash
./scripts/run_api.sh                 # builds bin/api if stale, starts it in the background, waits for /health
./scripts/run_consumer.sh            # the same for the consumer; waits for its "consuming ..." line
./scripts/run_consumer.sh --replay   # consumer rebuild: recreate the chat messages from the inbox
./scripts/stop_api.sh
./scripts/run_mcp.sh                 # the same for the MCP server; waits for /health
./scripts/stop_mcp.sh
./scripts/stop_consumer.sh           # SIGTERM, so it logs its totals and commits its last offset
```

Both run scripts take `--rebuild` (force a build), `--foreground` and `--no-kill` (refuse to replace a
running instance); `run_api.sh` also takes `--port`. They work from any directory, stop a previous
instance first, and exit at once, with the log path, when the binary dies (a wrong config, an unreachable
database). Logs go to `scripts/logs/` and the consumer's pid to `scripts/logs/consumer.pid`; both are
gitignored. `check-conventions.sh` in the same folder is tracked.

Needs the local cluster (`../cluster`: Postgres with `wal_level=logical`, Kafka, Kafka Connect)
and the connector registered from `../ms-ml-genai-agents-bs/local/debezium/register.sh`.
`local/README.md` has the end-to-end steps.

## API

```bash
curl -s "localhost:3100/v1/conversations?idYapeAccount=<account>"
curl -s "localhost:3100/v1/conversations/<conversationId>/messages"
curl -sg "localhost:3100/v1/conversations/<conversationId>/messages?page[before]=<_id>&page[size]=20"
curl -sg "localhost:3100/v1/conversations?idYapeAccount=<account>&page[before]=<cursor>"   # the cursor of a conversation, or the one in the Link header
curl -s  localhost:3100/health
```

| Route | Answer |
|-------|--------|
| `GET /v1/conversations/{conversationId}/messages` | up to 100 messages, oldest first; `page[before]` or `page[after]` (one at most) take a message `_id`, `page[size]` asks for fewer; more pages are announced in the `Link` header |
| `GET /v1/conversations?idYapeAccount=&page[before]=&page[after]=&page[size]=` | a page of the account's conversations with chat messages, most recent first; each carries an opaque `cursor`; more pages are announced in the `Link` header |
| `GET /health` | `200` when the database answers, `503` otherwise |

The contract is `docs/ms-ml-conv-history-bs-openapi.yml` (Swagger 2.0), generated from the swag annotations
in `cmd/api/main.go` and the handlers, as in `cpgs-clg-core`: `make openapi` regenerates it and
`make openapi/check` fails when it is stale. A test fails when a served route is not documented, and
another when a reply carries a field the spec does not describe.

Errors are RFC 9457 problem details served as `application/problem+json` (`type`, `title`,
`status`, `detail`): `400` for a malformed id or account, both cursors, a bad `page[size]` or a
cursor the API did not issue; `404` for a conversation with no chat messages; `503` for a request
that ran past its 10 second deadline. A client decides from `type`. `docs/design.md` has the
problem types and the paging rules.

## MCP

`cmd/mcp` gives an agent read-only tools over the same tables. It speaks MCP over streamable HTTP, with no
session kept between calls, at `http://localhost:3200/mcp` (`GET /health` for the database). Connect GitHub Copilot to it with:

```bash
copilot mcp add --transport http conv-history-local http://localhost:3200/mcp
```

| Tool | Answers | Reads |
|------|---------|-------|
| `list_accounts` | which accounts have history, how many conversations and messages, when last active | `conversation_summaries` |
| `get_recent_conversations` | an account's latest conversations (no text) | the `History` service, as the API |
| `get_transcript` | the chat of a conversation, newest page first, paged back with `before` | the `History` service, as the API |
| `get_activity_funnel` | how an account's attempts of a flow ended: approved, rejected, failed, abandoned, expired, open, none | `conversation_flow_segments` |
| `get_latency_stats` | seconds from the request to the card and from the card to the decision, with and without an OTP | `conversation_flow_segments`, `conversation_inbox` |
| `get_tool_health` | calls and failures of each tool of the agent | `conversation_inbox` |

The account is an argument of each tool, because the PoC has no authentication: an agent that can call
a tool can read any account. `list_accounts` is how it finds the ids. `docs/design.md` has the rules each tool follows.

## Configuration

`config.yaml` expands `${VAR:-default}`; a local `.env` (git-ignored) can set the variables.

| Variable | Default |
|----------|---------|
| `SERVER_PORT` | `3100` |
| `MCP_HOST`, `MCP_PORT` | `127.0.0.1`, `3200` (read by `cmd/mcp` only; it stays on the loopback interface because it has no authentication) |
| `POSTGRES_DSN` | empty; when set it wins over the fields below |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `localhost`, `5432`, `postgres`, empty, `postgres` |
| `POSTGRES_SSL_MODE` | `disable` |
| `POSTGRES_SEARCH_PATH` | `history`, the tables the Camel consumers fill; `camel_history` is the old consumer's; empty keeps the database user's own. With `POSTGRES_DSN`, put `search_path` in the DSN |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `10`, `2` |
| `POSTGRES_CONN_MAX_LIFETIME`, `POSTGRES_CONN_MAX_IDLE_TIME`, `POSTGRES_CONNECT_TIMEOUT` | `3600`, `300`, `10` (seconds) |
| `KAFKA_CLIENT_BROKERS` | `localhost:9092` |
| `KAFKA_CLIENT_SASL_KEY`, `KAFKA_CLIENT_SASL_SECRET` | empty; both set turn on SASL over TLS |
| `KAFKA_TOPIC_EVENTS` | `yape.public.conversation_events` |
| `KAFKA_GROUP_ID` | `ms-ml-conv-history-bs` |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest`; `latest` starts a group with no committed offset at the end of the topic and skips the history before it, so the consumer logs a warning at startup |
| `KAFKA_MAX_POLL_INTERVAL_MS`, `KAFKA_SESSION_TIMEOUT_MS` | `300000`, `45000` (the first must not be below the second) |
| `LOG_LEVEL` | `info` |
| `ENVIRONMENT` | `development`; `production` skips reading `.env` |

## Database

The tables (`conversation_inbox`, `conversation_messages`, `conversation_summaries`, which the conversation list reads) are created by the migrations in
`../ml-genai-agents-bs-testing/migrations/history/conversation/`, next to the agent's
(`migrations/agent/conversation/`). Each flow gets its own folder.

```bash
make migrations/up
make migrations/down CONFIRM=yes
```

If the testing repo is checked out elsewhere, set `MIGRATIONS_REPO` for the make targets and
`HISTORY_MIGRATIONS_DIR` for the tests.

## Tests

```bash
make test         # clean cache, every package suite
make test-race
make cover        # fails under 96% per package
make build        # bin/api, bin/consumer and bin/mcp
make it           # integration tests; they start their own Postgres, Kafka, Schema Registry and Connect in Docker (3 minutes) and touch nothing else
make check        # vet, conventions (below) and the OpenAPI spec
make openapi      # regenerate the OpenAPI spec (needs swag)
make pipeline     # only the pipeline tests (Kafka, Schema Registry, Connect with Debezium, Postgres), also all in Docker
local/scripts/verify.sh   # compares the history with the agent's event store
```

### Concurrency

Race conditions are kept out by tests that run under `make test-race`, written in the style of
`cpgs-clg-core`'s async executor: how a batch of work runs is a function value
(`internal/testsupport/concurrent`). A scenario runs once one task at a time, which gives the reference
outcome, and once with every task released at the same moment, and the two outcomes must be equal. The
helpers are small pure functions (`Collect`, `Count`, `Map`, `Orders`, `Outcomes`): each task returns its
result into its own slot, so a test needs no mutex and no sleep.

Guards exist for the HTTP server, the history service, the projection, the decoder, the consumer's
counters and config loading (no database), and for writers racing on a real Postgres (`make it`). Planted
races (a shared counter, a shared map) make them fail. `scripts/check-conventions.sh` keeps new ones out:
production code starts no goroutine outside `internal/core/async`, and a test does not wait with
`time.Sleep`.

### Integration tests

`make it` needs Docker and nothing else: it does not look for the local cluster, does not read its ports and writes
nothing to it. The tests start what they need, with host ports picked free at that moment, a memory cap on every
container, and unique names for every topic, consumer group, connector, replication slot and conversation.

| Tests | What they start |
|-------|-----------------|
| `TestInsights`, the read API, the schema, summaries, paging, concurrency | one Postgres 18 per test |
| `TestPipelineTopic`, `TestPipelineStoresRecordsOfEveryShapeTheBrokerCanCarry`, the two outage tests | a Kafka broker (`cp-kafka` 7.5.3, KRaft, no ZooKeeper) shared by the run, and a Postgres 18 per test; the outage tests stop that Postgres |
| `TestPipelineThroughDebezium`, `TestPipelineRecoversFromALostReplicationSlot` | the same Kafka, plus Schema Registry 7.5.3, a Connect 7.5.3 worker with the Debezium PostgreSQL connector 2.5.4, and the agent's Postgres 18 with `wal_level=logical` and its `conversation_events` |

The pipeline tests run the built consumer as a process: a database outage longer than `max.poll.interval.ms` (the Postgres
container is stopped and started again), SIGTERM while the consumer retries against a dead database, records of every
shape the broker can carry (near 1 MB, each compression codec, another schema id, extra fields, no key), a lost replication
slot rehearsed with a second connector that has its own slot, publication and topic, and the whole chain from an insert in
`conversation_events` to the chat in the history.

The Kafka, Schema Registry, Connect and agent Postgres are heavy, so the first test that needs them starts them and the
rest of the run shares them; `TestMain` removes them at the end. Together they are capped at about 2.6 GiB (Kafka 900 MiB,
Connect 1100, Schema Registry 450, Postgres 256), and the whole suite takes about 3 minutes on one CPU. A run needs:

- Docker with room for that, beside whatever else it runs. The first run pulls the images and builds
  `conv-history-it/connect:7.5.3-debezium-2.5.4` from `it/history/testdata/connect/Dockerfile`, which downloads the Debezium
  connector from Maven Central and checks its SHA-1. Docker keeps the image, so later runs need no network.
- The testing repo checked out beside this one: the history migrations and the agent's
  (`ml-genai-agents-bs-testing/migrations/agent/conversation`; `AGENT_MIGRATIONS_DIR` and `HISTORY_MIGRATIONS_DIR` point elsewhere).

A run killed before `TestMain` leaves its containers behind (Ryuk is off: Colima does not give it the Docker socket).
They are labelled: `docker rm -f $(docker ps -aq --filter label=conv-history-it)`.

Mocks come from `//go:generate mockery` directives next to each port; regenerate one with
`go generate ./internal/core/port/service/<file>.go`.

## Layout

Hexagonal, as `cpgs-clg-core`: `internal/core` never imports `internal/infrastructure`.

```text
cmd/{api,consumer,mcp}/         wiring only
internal/config/                config.yaml loader
internal/core/domain/           types and sentinel errors
internal/core/projection/       event -> message, ingest, rebuild
internal/core/history/          paging rules
internal/core/insights/         statistics: funnel, latency, tool health, accounts
internal/core/async/            the one place that starts goroutines (swappable executor)
internal/core/port/{repository,service,resource}/   interfaces + mocks/
internal/infrastructure/
  api/ api/handler/             Echo server and handlers
  mcp/                          MCP server: tools, output types, error wording
  kafka/                        decoder, consumer loop, source (lib-go-kafkalizer)
  logging/                      lib-go-log adapter
  postgresql/ postgresql/repository/   pgx pool, inbox, messages, SQL/
internal/testsupport/           test helpers: throwaway Postgres, concurrent/ (executors)
it/history/                     integration and pipeline tests
local/                          pipeline README and verify.sh
scripts/                        run_*/stop_* for both binaries, check-conventions.sh (rules the linter cannot express)
docs/                           design.md and the OpenAPI spec
testdata/                       a captured Debezium record
docs/design.md                  design notes
```
