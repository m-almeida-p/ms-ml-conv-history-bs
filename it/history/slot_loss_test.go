package history_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connector is a copy of the conversation-events connector (cluster: ms-ml-genai-agents-bs/local/debezium) with
// a slot, a publication and a topic prefix of its own, so no test shares a slot or a topic with another.
type connector struct {
	name, slot, prefix string
	connectURL         string
	db                 agentDatabase
}

// newConnector returns a connector whose names are unique to the test, and whose slot and publication share
// the id, as the conversation-events connector's do.
func newConnector(s stack, id string) connector {
	return connector{name: id, slot: id, prefix: id, connectURL: s.connectURL, db: s.agentDB}
}

func (c connector) topic() string { return c.prefix + ".public.conversation_events" }

func (c connector) config() map[string]any {
	return map[string]any{
		"connector.class":                        "io.debezium.connector.postgresql.PostgresConnector",
		"tasks.max":                              "1",
		"database.hostname":                      c.db.host,
		"database.port":                          "5432",
		"database.user":                          c.db.user,
		"database.password":                      c.db.password,
		"database.dbname":                        c.db.name,
		"plugin.name":                            "pgoutput",
		"slot.name":                              c.slot,
		"publication.name":                       c.slot,
		"publication.autocreate.mode":            "filtered",
		"table.include.list":                     "public.conversation_events",
		"message.key.columns":                    "public.conversation_events:conversation_id",
		"snapshot.mode":                          "initial",
		"topic.prefix":                           c.prefix,
		"transforms":                             "unwrap",
		"transforms.unwrap.type":                 "io.debezium.transforms.ExtractNewRecordState",
		"transforms.unwrap.drop.tombstones":      "true",
		"transforms.unwrap.delete.handling.mode": "drop",
		"key.converter":                          "io.confluent.connect.json.JsonSchemaConverter",
		"key.converter.schema.registry.url":      "http://schema-registry:8081",
		"value.converter":                        "io.confluent.connect.json.JsonSchemaConverter",
		"value.converter.schema.registry.url":    "http://schema-registry:8081",
	}
}

func (c connector) register(t *testing.T) {
	t.Helper()

	body, err := json.Marshal(map[string]any{"name": c.name, "config": c.config()})
	require.NoError(t, err)
	resp, err := http.Post(c.connectURL+"/connectors", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "the connector was registered")

	eventually(t, 60*time.Second, c.name+" running", func() bool {
		r, err := http.Get(c.connectURL + "/connectors/" + c.name + "/status")
		if err != nil {
			return false
		}
		defer r.Body.Close()
		var s struct {
			Connector struct{ State string } `json:"connector"`
			Tasks     []struct{ State string }
		}

		return json.NewDecoder(r.Body).Decode(&s) == nil && s.Connector.State == "RUNNING" &&
			len(s.Tasks) > 0 && s.Tasks[0].State == "RUNNING"
	})
}

func (c connector) remove(t *testing.T) {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, c.connectURL+"/connectors/"+c.name, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
}

// dropSlot removes the replication slot and the publication, as a lost slot would
// be, and as the cleanup of the rehearsal does. The slot is held by the running
// task until the connector is gone, so it retries for a few seconds.
func dropSlot(t *testing.T, agent *pgxpool.Pool, slot string) {
	t.Helper()

	eventually(t, 30*time.Second, "slot "+slot+" dropped", func() bool {
		var n int
		err := agent.QueryRow(context.Background(),
			`SELECT count(pg_drop_replication_slot(slot_name)) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&n)

		return err == nil
	})
	_, err := agent.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", slot))
	require.NoError(t, err)
}

func insertChat(t *testing.T, agent *pgxpool.Pool, conversation string, from, to int) {
	t.Helper()

	for seq := from; seq <= to; seq++ {
		_, err := agent.Exec(context.Background(), `
			INSERT INTO conversation_events (conversation_id, sequence, event_name, event_version, payload, metadata, correlation_id, id_yape_account, actor, occurred_at)
			VALUES ($1, $2::int, 'UserMessageSent', 1, jsonb_build_object('text', 'mensaje ' || $2::text), '{"actor": "user"}'::jsonb, 'corr-slot', 'acc-slot', 'user', now())`,
			conversation, seq)
		require.NoError(t, err)
	}
}

// TestPipelineRecoversFromALostReplicationSlot rehearses the incident the runbook
// describes: the slot is lost, rows are written while nothing reads them, and a
// new connector takes a new snapshot. The history must end with every message once,
// the rows written during the gap included, and the repeats absorbed by the inbox.
func TestPipelineRecoversFromALostReplicationSlot(t *testing.T) {
	s := requireDebezium(t)

	history, historyDSN := newHistoryDB(t)
	ctx := context.Background()

	agent, err := pgxpool.New(ctx, s.agentDSN)
	require.NoError(t, err)
	t.Cleanup(agent.Close)

	id := strings.ReplaceAll(uniqueName("rehearsal"), "-", "_")
	first, second := newConnector(s, id), newConnector(s, id)
	first.name, second.name = id+"_a", id+"_b"

	conversation := uniqueName("slot-loss")
	// The connectors and the slot go when the test ends: the agent's database is shared by the tests of the
	// run, and a slot that nobody reads holds WAL.
	t.Cleanup(func() {
		first.remove(t)
		second.remove(t)
		dropSlot(t, agent, id)
	})

	createTopic(t, s.brokers, first.topic())
	consumer := startConsumer(t, consumerBinary(t), s.brokers, first.topic(), uniqueName("slot-group"), historyDSN)

	// 1. A first connector streams the first three messages.
	insertChat(t, agent, conversation, 1, 3)
	first.register(t)
	waitForMessages(t, history, conversation, 3)

	// 2. The connector is lost with its slot; two more messages are written to nobody.
	first.remove(t)
	dropSlot(t, agent, id)
	insertChat(t, agent, conversation, 4, 5)
	assert.Equal(t, 3, countWhere(t, history, "conversation_messages", conversation), "the gap rows are not in the history")

	// 3. A new connector takes a new snapshot and republishes every row.
	second.register(t)
	waitForMessages(t, history, conversation, 5)

	// 4. A message written after the recovery arrives through the new slot.
	insertChat(t, agent, conversation, 6, 6)
	waitForMessages(t, history, conversation, 6)

	got := messagesOf(t, history, conversation)
	require.Len(t, got, 6)
	for i, m := range got {
		assert.Equal(t, i+1, m.Sequence)
		assert.Equal(t, fmt.Sprintf("mensaje %d", i+1), m.Text)
	}
	assert.Equal(t, 6, countWhere(t, history, "conversation_inbox", conversation), "the republished rows did not add inbox rows")

	consumer.stop(t)
	totals := summaryTotals(t, consumer.out.String())
	assert.GreaterOrEqual(t, totals["duplicates"], 3, "the snapshot republished the three messages the history already had")
	assert.Zero(t, totals["rejected"])
	assert.Zero(t, totals["malformed"])
}
