package kafka

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
)

const (
	topic      = "yape.public.conversation_outbox"
	valuePath  = "../../../testdata/outbox-record.value.bin"
	keyPath    = "../../../testdata/outbox-record.key.bin"
	validBody  = `{"id":7,"conversation_id":"conv_a","sequence":3,"event_name":"UserMessageSent","event_version":1,"payload":"{\"text\": \"hola\"}","metadata":"{\"actor\": \"user\"}","created_at":"2026-10-20T14:07:23.792287Z"}`
	headerOnly = "\x00\x00\x00\x00\x04"
)

func record(value string) resource.Message {
	return resource.Message{Topic: topic, Partition: 2, Offset: 41, Value: []byte(value)}
}

// without returns validBody with one top-level field removed.
func without(field string) string {
	fields := map[string]string{
		"id": `"id":7,`, "conversation_id": `"conversation_id":"conv_a",`, "sequence": `"sequence":3,`,
		"event_name": `"event_name":"UserMessageSent",`, "event_version": `"event_version":1,`,
		"payload": `"payload":"{\"text\": \"hola\"}",`, "metadata": `"metadata":"{\"actor\": \"user\"}",`,
	}
	body := validBody
	if field == "created_at" {
		return `{"id":7,"conversation_id":"conv_a","sequence":3,"event_name":"UserMessageSent","event_version":1,"payload":"{}","metadata":"{}"}`
	}
	out := ""
	for i := 0; i+len(fields[field]) <= len(body); i++ {
		if body[i:i+len(fields[field])] == fields[field] {
			out = body[:i] + body[i+len(fields[field]):]
			break
		}
	}

	return out
}

func TestDecodeSample(t *testing.T) {
	value, err := os.ReadFile(valuePath)
	require.NoError(t, err)
	key, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	got, err := Decode(resource.Message{Topic: topic, Partition: 0, Offset: 0, Key: key, Value: value})

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, int64(1), got.OutboxID)
	assert.Equal(t, "spike-aip472", got.ConversationID)
	assert.Equal(t, 1, got.Sequence)
	assert.Equal(t, "UserMessageSent", got.EventName)
	assert.Equal(t, 1, got.EventVersion)
	assert.JSONEq(t, `{"text": "hola", "sequence": 1, "message_id": "m-1"}`, string(got.Payload))
	assert.JSONEq(t, `{"actor": "user", "correlation_id": "c-1", "id_yape_account": "acc-1"}`, string(got.Metadata))
	assert.Equal(t, "c-1", got.CorrelationID)
	assert.Equal(t, "acc-1", got.IDYapeAccount)
	assert.Equal(t, "user", got.Actor)
	assert.Equal(t, time.Date(2026, 10, 20, 14, 7, 23, 792287000, time.UTC), got.OccurredAt)
	assert.Equal(t, topic, got.KafkaTopic)
}

func TestDecode(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantSkip  bool
		check     func(t *testing.T, e *domain.InboxEvent)
		wantErr   Reason
		wantField string
	}{
		{
			name:  "headerThenJSON",
			value: "\x00\x00\x00\x00\x09" + validBody,
			check: func(t *testing.T, e *domain.InboxEvent) {
				assert.Equal(t, int64(7), e.OutboxID)
				assert.Equal(t, 3, e.Sequence)
				assert.JSONEq(t, `{"text": "hola"}`, string(e.Payload))
			},
		},
		{
			name:  "plainJSON",
			value: validBody,
			check: func(t *testing.T, e *domain.InboxEvent) { assert.Equal(t, "conv_a", e.ConversationID) },
		},
		{
			name:  "surroundingWhitespaceIsIgnored",
			value: "  \n" + validBody + "\n",
			check: func(t *testing.T, e *domain.InboxEvent) { assert.Equal(t, int64(7), e.OutboxID) },
		},
		{
			name:  "payloadAndMetadataAsObjectsAreAccepted",
			value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":{"text":"hi"},"metadata":{},"created_at":"2026-10-20T14:07:23Z"}`,
			check: func(t *testing.T, e *domain.InboxEvent) {
				assert.JSONEq(t, `{"text":"hi"}`, string(e.Payload))
				assert.JSONEq(t, `{}`, string(e.Metadata))
			},
		},
		{
			name:  "optionalColumnsAbsentOrNullReadAsEmpty",
			value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}","correlation_id":null,"actor":null,"created_at":"2026-10-20T14:07:23Z"}`,
			check: func(t *testing.T, e *domain.InboxEvent) {
				assert.Empty(t, e.CorrelationID)
				assert.Empty(t, e.IDYapeAccount)
				assert.Empty(t, e.Actor)
			},
		},
		{
			name:  "unknownFieldsAreIgnored",
			value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}","created_at":"2026-10-20T14:07:23Z","published":false,"future":{"a":1}}`,
			check: func(t *testing.T, e *domain.InboxEvent) { assert.Equal(t, "E", e.EventName) },
		},
		{
			name:  "timeWithOffsetIsConvertedToUTC",
			value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}","created_at":"2026-10-20T09:07:23-05:00"}`,
			check: func(t *testing.T, e *domain.InboxEvent) {
				assert.Equal(t, time.Date(2026, 10, 20, 14, 7, 23, 0, time.UTC), e.OccurredAt)
				assert.Equal(t, time.UTC, e.OccurredAt.Location())
			},
		},
		{
			name:  "payloadArrayIsAccepted",
			value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"[1,2]","metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`,
			check: func(t *testing.T, e *domain.InboxEvent) { assert.JSONEq(t, `[1,2]`, string(e.Payload)) },
		},

		{name: "nilValueIsATombstone", value: "", wantSkip: true},
		{name: "headerOnlyIsATombstone", value: headerOnly, wantSkip: true},
		{name: "nullLiteralIsATombstone", value: "null", wantSkip: true},
		{name: "headerThenNullIsATombstone", value: headerOnly + "null", wantSkip: true},

		{name: "headerCutShort", value: "\x00\x00\x00", wantErr: ReasonHeader},
		{name: "notJSON", value: `{"id":`, wantErr: ReasonJSON},
		{name: "jsonArrayInsteadOfAnObject", value: `[1,2,3]`, wantErr: ReasonJSON},
		{name: "wrongFirstByteIsReadAsPlainJSON", value: "\x01" + validBody, wantErr: ReasonJSON},
		{name: "idIsAString", value: `{"id":"7"}`, wantErr: ReasonFieldMalformed, wantField: "id"},
		{name: "sequenceIsAString", value: `{"id":7,"sequence":"3"}`, wantErr: ReasonFieldMalformed, wantField: "sequence"},
		{name: "idMissing", value: without("id"), wantErr: ReasonFieldMissing, wantField: "id"},
		{name: "conversationIDMissing", value: without("conversation_id"), wantErr: ReasonFieldMissing, wantField: "conversation_id"},
		{name: "sequenceMissing", value: without("sequence"), wantErr: ReasonFieldMissing, wantField: "sequence"},
		{name: "eventNameMissing", value: without("event_name"), wantErr: ReasonFieldMissing, wantField: "event_name"},
		{name: "eventVersionMissing", value: without("event_version"), wantErr: ReasonFieldMissing, wantField: "event_version"},
		{name: "payloadMissing", value: without("payload"), wantErr: ReasonFieldMissing, wantField: "payload"},
		{name: "metadataMissing", value: without("metadata"), wantErr: ReasonFieldMissing, wantField: "metadata"},
		{name: "createdAtMissing", value: without("created_at"), wantErr: ReasonFieldMissing, wantField: "created_at"},
		{
			name:      "conversationIDEmpty",
			value:     `{"id":7,"conversation_id":"","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`,
			wantErr:   ReasonFieldMissing,
			wantField: "conversation_id",
		},
		{
			name:      "payloadNull",
			value:     `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":null,"metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`,
			wantErr:   ReasonFieldMissing,
			wantField: "payload",
		},
		{
			name:      "payloadStringIsNotJSON",
			value:     `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"not json","metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`,
			wantErr:   ReasonPayload,
			wantField: "payload",
		},
		{
			name:      "payloadIsABareNumber",
			value:     `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":5,"metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`,
			wantErr:   ReasonPayload,
			wantField: "payload",
		},
		{
			name:      "payloadStringIsEmpty",
			value:     `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"  ","metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`,
			wantErr:   ReasonPayload,
			wantField: "payload",
		},
		{
			name:      "metadataStringIsNotJSON",
			value:     `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{broken","created_at":"2026-10-20T14:07:23Z"}`,
			wantErr:   ReasonMetadata,
			wantField: "metadata",
		},
		{
			name:      "createdAtIsNotATime",
			value:     `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}","created_at":"yesterday"}`,
			wantErr:   ReasonCreatedAt,
			wantField: "created_at",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode(record(tt.value))

			switch {
			case tt.wantSkip:
				require.NoError(t, err)
				assert.Nil(t, got)
			case tt.wantErr != "":
				require.Error(t, err)
				var decodeErr *DecodeError
				require.True(t, errors.As(err, &decodeErr), "a typed error")
				assert.Equal(t, tt.wantErr, decodeErr.Reason)
				assert.Equal(t, tt.wantField, decodeErr.Field)
				assert.Equal(t, topic, decodeErr.Topic)
				assert.Equal(t, int32(2), decodeErr.Partition)
				assert.Equal(t, int64(41), decodeErr.Offset)
				assert.Nil(t, got)
			default:
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, topic, got.KafkaTopic)
				assert.Equal(t, int32(2), got.KafkaPartition)
				assert.Equal(t, int64(41), got.KafkaOffset)
				tt.check(t, got)
			}
		})
	}
}

func TestDecodeErrorNeverCarriesTheRecordContent(t *testing.T) {
	const secret = "card-4111-1111"

	tests := []struct {
		name  string
		value string
	}{
		{name: "brokenJSON", value: `{"id": 1, "payload": "` + secret + `" oops`},
		{name: "wrongType", value: `{"id": "` + secret + `"}`},
		{name: "badPayload", value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"` + secret + `","metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`},
		{name: "badMetadata", value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"` + secret + `","created_at":"2026-10-20T14:07:23Z"}`},
		{name: "badTime", value: `{"id":7,"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}","created_at":"` + secret + `"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(record(tt.value))

			require.Error(t, err)
			assert.NotContains(t, err.Error(), secret)
			assert.Contains(t, err.Error(), topic+"/2/41")
		})
	}
}

func TestDecodeErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		err  DecodeError
		want string
	}{
		{name: "withoutAField", err: DecodeError{Topic: "t", Partition: 1, Offset: 9, Reason: ReasonJSON}, want: "decode record t/1/9: value is not a JSON object"},
		{name: "withAField", err: DecodeError{Topic: "t", Partition: 1, Offset: 9, Reason: ReasonFieldMissing, Field: "id"}, want: "decode record t/1/9: a required field is missing (id)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.Error())
		})
	}
}
