# Pending: from the PoC to production

Ticket AIP-472, as of 2026-10-20. The pipeline runs end to end locally (agent event store, Debezium, Kafka,
inbox, chat history, API). This file lists what is not proven or not built for production.

Status values: **Unproven** (designed from research, never run), **Not started**, **Decision** (someone has
to choose), **Known gap** (built, with a stated limit).

## 1. Production CDC: the request to DevOps

Production declares connectors in Terraform, in `yaperos/devops`, through the module
`terraform-modules//confluent_connector_debezium` (a Confluent Cloud `PostgresCdcSourceV2`). The squad that
owns the table declares it. The agent's folder, `terraform/apps/squads/data/ml_genai_agents_bs/`, has Redis
only: no Postgres, no connector.

| # | Item | What to ask or decide | Status |
|---|------|-----------------------|--------|
| 1.1 | Where the event store lives in production | The squad's Terraform defines no Postgres. Confirm the instance and database that hold `conversation_events` | Decision |
| 1.2 | The connector | Declare it in the squad's folder with the module above: service account with replication rights, database, `table.include.list = public.conversation_events`, `message.key.columns = public.conversation_events:conversation_id`, topic prefix `yape` (topic `yape.public.conversation_events`, partitions to confirm) | Not started |
| 1.3 | Publication mode | The module's `publication_autocreate_mode` defaults to `all_tables`, which would publish every table of the database. Override it so only `conversation_events` is published | Not started |
| 1.4 | Snapshot mode | The module defaults to `initial` (republishes every row on a first start or a slot reset); `ms-scheduler-engine` uses `when_needed`. Pick one. The consumer absorbs repeats either way | Decision |
| 1.5 | Slot name | The module defaults to `debezium`; the local connector uses `conversation_events`. A named slot is easier to find when WAL lag grows | Decision |
| 1.6 | Heartbeat table | The module creates one (`public.<name>`, a single row) so the slot keeps advancing on a quiet database. The local setup has none | Decision |
| 1.7 | Schema Registry | Format is `JSON_SR`. The module can register a JSON schema per table from a file in the service's Terraform folder (`confluent_schemas/<table>.json`) and sets subject compatibility to `NONE`. Check whether `JSON_SR` needs the file | Unproven |
| 1.8 | Record format on Confluent Cloud | The decoder was built from the local connector's record (5-byte header, flat row, JSONB as strings, tombstones). Capture one record from the managed connector and run it through the decoder tests | Unproven |
| 1.9 | Settings that differ from local | `database.sslmode` defaults to `require` (local: `disable`); `decimal.format`, `tasks.max` (1) and the transforms come from the module. The local settings were mapped by hand | Unproven |
| 1.10 | Event store retention | No job prunes `conversation_events`. Delete whole, closed conversations only: the agent reloads a conversation from its events and takes the next sequence from the highest one. Deletes are not published, so the history keeps its copy; decide whether the history is cleared too (erasure). Decide the rule for "closed" and who runs the job | Decision |
| 1.11 | Key columns on the managed connector | The local connector sets `message.key.columns` so a conversation stays on one partition. Check that the managed connector offers it. Without it the key is the primary key `(conversation_id, sequence)`: ordering across partitions is lost, which the inbox absorbs | Unproven |

## 2. The history service in production

| # | Item | Detail | Status |
|---|------|--------|--------|
| 2.1 | Authentication on the API | There is none, so the service must not be deployed. Decide the model (the agent validates a JWT with `idYapeAccount`) | Decision |
| 2.2 | Migrations | They live in `ml-genai-agents-bs-testing/migrations/history/conversation/`. Deployed environments run a golang-migrate project (`repos/migrations` for the agent's tables). Confirm where the history's migrations go | Decision |
| 2.3 | Database | The PoC shares the `postgres` database with the agent. Decide whether production gives the service its own database | Decision |
| 2.4 | Build and deploy | No Dockerfile or manifests. `lib-go-kafkalizer` wraps `confluent-kafka-go`, so the image needs cgo | Not started |
| 2.5 | Kafka credentials | `KAFKA_CLIENT_BROKERS`, `KAFKA_CLIENT_SASL_KEY` and `KAFKA_CLIENT_SASL_SECRET` come from the environment. Wire them from the secret store | Not started |
| 2.6 | Metrics and alerts | The consumer logs its counters when it stops; there is no metrics endpoint and no alert on consumer lag or on the replication slot | Not started |
| 2.7 | Dead-letter queue | Malformed and rejected records are logged, counted and skipped. Production needs a DLQ to keep them | Known gap |
| 2.8 | Throughput | The library's loop handles the 3 partitions one record at a time. `internal/core/async/usage_test.go` shows the one-worker-per-partition pattern for when volume asks for it | Known gap |
| 2.9 | Conversation list | The latest 100 conversations with `hasMore`, no paging. Its cost grows with the account's history (131 buffers for 20,000 messages); a conversations table is the step for accounts with millions | Known gap |

## 3. The agent side

| # | Item | Detail | Status |
|---|------|--------|--------|
| 3.1 | Other flows' facts | Bill payment writes conversation facts through its agents (stamped `bill_payment`) but records no payment facts of its own. When it does, it builds its own `FlowRecorder` with `WithFlowName(flows.BillPayment)` and, if it needs tables, a folder and table names carrying the flow | Not started |
| 3.2 | Repos without a remote | `ms-ml-conv-history-bs`, `ml-genai-agents-bs-testing` and `cluster` (Kafka Connect, `wal_level=logical`) exist only on the laptop; `ms-ml-genai-agents-bs` is pushed. Where they live is a decision for the squad | Decision |

## 4. Limits of the tests

The process-level tests (outage longer than the poll interval, SIGTERM during an outage, records of every
shape, lost replication slot) run against the local cluster. They do not run against Confluent Cloud.

| # | Limit | Status |
|---|-------|--------|
| 4.1 | The size test sends 800 KB; the broker's limit is 1 MB and Confluent Cloud's per-topic limit is not checked | Unproven |
| 4.2 | The slot-loss rehearsal uses a second local connector; the managed connector's reaction to a lost slot is not rehearsed | Unproven |
| 4.3 | Schema changes are simulated by another schema id and extra fields; no test registers a new schema version in a real Schema Registry | Unproven |

## 5. Ticket workflow

The prompt for AIP-472 has seven phases. Phases 1 to 4 (define, plan, build, test) are done. Phases 5
(review), 6 (simplify) and 7 (ship, with `AIP-472-changes.md`) have not started. The phase documents are in
`prompts/historial_conversaciones/PoC/v2/p2p/prompts/skills_output/`.

## Sources

- `docs/design.md` and `README.md` of this repo
- `guides/debezium-en-yape.md`, section 7 (where each piece lives, and the snapshot-mode note)
- `yaperos/devops` and `yaperos/terraform-modules` (`confluent_connector_debezium`: `main.tf`, `postgres.tf`, `variables.tf`)
- `AIP-472-phase-3-build.md`, "Edge and corner cases" (the not-covered list)
