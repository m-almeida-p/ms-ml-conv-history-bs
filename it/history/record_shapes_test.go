package history_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shapedValue is a Debezium record with the schema id, the text and any extra
// fields the case needs, so a record can differ from the usual one in ways the
// connector or Schema Registry may change over time.
func shapedValue(conversationID string, schemaID byte, text string, extra map[string]any) []byte {
	payload, _ := json.Marshal(map[string]any{"text": text})
	metadata, _ := json.Marshal(map[string]string{"actor": "user", "id_yape_account": "acc-shapes"})
	row := map[string]any{
		"conversation_id": conversationID, "sequence": 1, "event_name": "UserMessageSent", "event_version": 1,
		"payload": string(payload), "metadata": string(metadata),
		"correlation_id": "corr", "id_yape_account": "acc-shapes", "actor": "user",
		"occurred_at": time.Date(2026, 10, 20, 15, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	for k, v := range extra {
		row[k] = v
	}
	body, _ := json.Marshal(row)

	return framed(schemaID, body)
}

// TestPipelineStoresRecordsOfEveryShapeTheBrokerCanCarry: a large record, every
// compression codec the producers may use, a changed schema id, and a row with new
// fields. Each must end as the same stored message.
func TestPipelineStoresRecordsOfEveryShapeTheBrokerCanCarry(t *testing.T) {
	s := requireKafka(t)
	history, historyDSN := newHistoryDB(t)
	topic, group := uniqueName("shapes-test"), uniqueName("shapes-group")
	createTopic(t, s.brokers, topic)
	consumer := startConsumer(t, consumerBinary(t), s.brokers, topic, group, historyDSN)

	// Close to the broker's 1 MB record limit once the JSON around it is counted.
	large := strings.Repeat("ñ", 400_000) // 800 000 bytes in UTF-8
	tests := []struct {
		name        string
		compression string
		schemaID    byte
		text        string
		extra       map[string]any
		nilKey      bool
	}{
		{name: "aRecordNearTheBrokersSizeLimit", text: large, schemaID: 4},
		{name: "gzip", compression: "gzip", text: "comprimido con gzip", schemaID: 4},
		{name: "snappy", compression: "snappy", text: "comprimido con snappy", schemaID: 4},
		{name: "lz4", compression: "lz4", text: "comprimido con lz4", schemaID: 4},
		{name: "zstd", compression: "zstd", text: "comprimido con zstd", schemaID: 4},
		{name: "aLargeRecordCompressed", compression: "zstd", text: large, schemaID: 4},
		{name: "aNewSchemaID", text: "otro esquema", schemaID: 99},
		{name: "theHighestSchemaID", text: "id máximo", schemaID: 255},
		{
			name: "newColumnsAndNestedFieldsAreIgnored", text: "con columnas nuevas", schemaID: 7,
			extra: map[string]any{"new_column": "x", "nested": map[string]any{"a": []int{1, 2}}, "published": true, "created_by": nil},
		},
		{name: "aRecordWithoutAKey", text: "sin clave", schemaID: 4, nilKey: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conversation := uniqueName("shape")
			config := ckafka.ConfigMap{"bootstrap.servers": s.brokers}
			if tt.compression != "" {
				config["compression.type"] = tt.compression
			}
			p, err := ckafka.NewProducer(&config)
			require.NoError(t, err)
			defer p.Close()
			prod := &producer{t: t, p: p}

			var key []byte
			if !tt.nilKey {
				key = eventKey(conversation)
			}
			prod.send(topic, 0, key, shapedValue(conversation, tt.schemaID, tt.text, tt.extra))

			waitForMessages(t, history, conversation, 1)

			got := messagesOf(t, history, conversation)
			require.Len(t, got, 1)
			assert.True(t, got[0].Text == tt.text, "the text came back changed (length %d, want %d)", len(got[0].Text), len(tt.text))
			assert.Equal(t, "appUser", got[0].Role)
			assert.Equal(t, "acc-shapes", got[0].Author)
		})
	}

	consumer.stop(t)
	assertCommittedEverything(t, s.brokers, topic, group)
	totals := summaryTotals(t, consumer.out.String())
	assert.Equal(t, len(tests), totals["stored"])
	assert.Zero(t, totals["malformed"])
	assert.Zero(t, totals["rejected"])
}
