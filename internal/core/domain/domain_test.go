package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInboxEventRef(t *testing.T) {
	tests := []struct {
		name  string
		event InboxEvent
		want  string
	}{
		{name: "kafkaCoordinates", event: InboxEvent{KafkaTopic: "yape.public.conversation_events", KafkaPartition: 2, KafkaOffset: 41}, want: "yape.public.conversation_events/2/41"},
		{name: "zeroValue", event: InboxEvent{}, want: "/0/0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.event.Ref())
		})
	}
}

// The reference names where the record sits in Kafka and nothing else: no
// conversation id, account, correlation id, payload or metadata reaches a log
// through it.
func TestInboxEventRefCarriesNoContent(t *testing.T) {
	event := InboxEvent{
		ConversationID: "conv-secret", Sequence: 1, Payload: []byte(`{"text":"card 4111"}`),
		Metadata: []byte(`{"secret":"x"}`), IDYapeAccount: "acc-1", CorrelationID: "corr-1",
		KafkaTopic: "t", KafkaPartition: 1, KafkaOffset: 7,
	}

	ref := event.Ref()

	for _, leaked := range []string{"conv-secret", "4111", "secret", "acc-1", "corr-1"} {
		assert.False(t, strings.Contains(ref, leaked), "the reference leaked %q", leaked)
	}
}

func TestValidIdentifier(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{name: "bffConversationID", id: "conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52", want: true},
		{name: "accountUUID", id: "896900db-eddd-4c54-99e5-5a32f0830f5e", want: true},
		{name: "allowedPunctuation", id: "a.b_c:d-e", want: true},
		{name: "largestLength", id: strings.Repeat("a", 128), want: true},
		{name: "empty", id: ""},
		{name: "oneTooLong", id: strings.Repeat("a", 129)},
		{name: "space", id: "conv 1"},
		{name: "slash", id: "../etc"},
		{name: "quote", id: "conv'--"},
		{name: "newline", id: "conv\n1"},
		{name: "nonASCII", id: "conv-ü"},
		{name: "nul", id: "a\x00b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ValidIdentifier(tt.id))
		})
	}
}
