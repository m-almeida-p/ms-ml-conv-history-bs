# Running the API locally

The cluster (`../cluster`) provides Postgres, Kafka and Kafka Connect. The history is written by a Camel consumer
(`../ms-ml-conv-history-consumer`, schema `history`; or the older `../ms-ml-camel-conv-history-bs`, schema `camel_history`), so
start the cluster and one of them first. This service only reads.

```bash
./scripts/run_api.sh      # :3100, GET /health
./scripts/run_mcp.sh      # :3200, GET /health
```

Settings come from `config.yaml`; `.env` (committed, with the local development values only) can
override them. The one that matters here is the schema:

| Variable | Default |
|----------|---------|
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_DB` | `localhost`, `5432`, `postgres`, `postgres` |
| `POSTGRES_SEARCH_PATH` | `history` (`camel_history` for the old consumer's tables) |

To check it against what the agent wrote, produce a conversation (for example with the explorer flows in
`ml-genai-agents-bs-testing`: `cmd/p2p-explore/scripts/run.sh --flow card`) and compare the API with the table:

```bash
curl -s "localhost:3100/v1/conversations?idYapeAccount=<account>"
curl -s "localhost:3100/v1/conversations/<conversationId>/messages"
docker exec yape-postgres psql -U postgres -c "select role, text from history.conversation_messages where conversation_id = '<conversationId>' order by sequence"
```
