// Package kafka reads the conversation events topic and turns its records into inbox events.
package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/resource"
)

// headerSize is the Schema Registry framing: a zero magic byte and a 4-byte
// schema id, followed by the JSON.
const headerSize = 5

// Reason says why a record could not be decoded. It never holds record content.
type Reason string

// The reasons a record is rejected.
const (
	ReasonHeader         Reason = "value is shorter than its schema registry header"
	ReasonJSON           Reason = "value is not a JSON object"
	ReasonPayload        Reason = "payload is not valid JSON"
	ReasonMetadata       Reason = "metadata is not valid JSON"
	ReasonOccurredAt     Reason = "occurred_at is not an RFC 3339 time"
	ReasonFieldMissing   Reason = "a required field is missing"
	ReasonFieldMalformed Reason = "a field has the wrong type"
	ReasonFieldRange     Reason = "a field has a value the history cannot hold"
)

// DecodeError is a record the decoder rejects. It names the record by its Kafka
// coordinates so it can be found, and never carries its content.
type DecodeError struct {
	Topic     string
	Partition int32
	Offset    int64
	Reason    Reason
	// Field names the offending field when the reason is about one.
	Field string
}

// Error implements error.
func (e *DecodeError) Error() string {
	field := ""
	if e.Field != "" {
		field = " (" + e.Field + ")"
	}

	return fmt.Sprintf("decode record %s/%d/%d: %s%s", e.Topic, e.Partition, e.Offset, e.Reason, field)
}

// maxNameLength is the size of the VARCHAR column that holds an event name.
const maxNameLength = 128

// row is the flat conversation_events row Debezium publishes. Required fields are
// pointers so an absent key differs from a zero value. Unknown fields, such as a
// column added later, are ignored.
type row struct {
	ConversationID *string         `json:"conversation_id"`
	Sequence       *int            `json:"sequence"`
	EventName      *string         `json:"event_name"`
	EventVersion   *int            `json:"event_version"`
	Payload        json.RawMessage `json:"payload"`
	Metadata       json.RawMessage `json:"metadata"`
	CorrelationID  *string         `json:"correlation_id"`
	IDYapeAccount  *string         `json:"id_yape_account"`
	Actor          *string         `json:"actor"`
	OccurredAt     *string         `json:"occurred_at"`
}

// problem is why a record could not be decoded, before it is tied to its record.
type problem struct {
	reason Reason
	field  string
}

func (p *problem) at(record resource.Message) *DecodeError {
	return &DecodeError{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Reason: p.reason, Field: p.field}
}

// Decode turns a record into an inbox event. A tombstone (an empty or null
// value) returns nil without error, since there is nothing to store. Any other
// problem returns a *DecodeError.
func Decode(record resource.Message) (*domain.InboxEvent, error) {
	body, tombstone, p := unframe(record.Value)
	if p != nil {
		return nil, p.at(record)
	}
	if tombstone {
		return nil, nil
	}

	r, p := parseRow(body)
	if p != nil {
		return nil, p.at(record)
	}
	if p := r.missingField(); p != nil {
		return nil, p.at(record)
	}
	if p := r.outOfRangeField(); p != nil {
		return nil, p.at(record)
	}

	payload, metadata, p := r.documents()
	if p != nil {
		return nil, p.at(record)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, *r.OccurredAt)
	if err != nil {
		return nil, (&problem{ReasonOccurredAt, "occurred_at"}).at(record)
	}

	return &domain.InboxEvent{
		ConversationID: *r.ConversationID,
		Sequence:       *r.Sequence,
		EventName:      *r.EventName,
		EventVersion:   *r.EventVersion,
		Payload:        payload,
		Metadata:       metadata,
		CorrelationID:  text(r.CorrelationID),
		IDYapeAccount:  text(r.IDYapeAccount),
		Actor:          text(r.Actor),
		OccurredAt:     occurredAt.UTC(),
		KafkaTopic:     record.Topic,
		KafkaPartition: record.Partition,
		KafkaOffset:    record.Offset,
	}, nil
}

// unframe strips the Schema Registry header, when there is one, and says whether
// what is left is a tombstone: nothing, or a JSON null.
func unframe(value []byte) (body []byte, tombstone bool, p *problem) {
	body = value
	if len(body) > 0 && body[0] == 0 {
		if len(body) < headerSize {
			return nil, false, &problem{ReasonHeader, ""}
		}
		body = body[headerSize:]
	}
	body = bytes.TrimSpace(body)

	return body, len(body) == 0 || bytes.Equal(body, []byte("null")), nil
}

func parseRow(body []byte) (row, *problem) {
	var r row
	if err := json.Unmarshal(body, &r); err != nil {
		// The json error can quote part of the value, so it is not passed on; only
		// the name of a field with the wrong type is kept.
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) && typeErr.Field != "" {
			return row{}, &problem{ReasonFieldMalformed, typeErr.Field}
		}

		return row{}, &problem{ReasonJSON, ""}
	}

	return r, nil
}

// missingField names the first required field the row lacks.
func (r row) missingField() *problem {
	switch {
	case r.ConversationID == nil || *r.ConversationID == "":
		return &problem{ReasonFieldMissing, "conversation_id"}
	case r.Sequence == nil:
		return &problem{ReasonFieldMissing, "sequence"}
	case r.EventName == nil || *r.EventName == "":
		return &problem{ReasonFieldMissing, "event_name"}
	case r.EventVersion == nil:
		return &problem{ReasonFieldMissing, "event_version"}
	case r.OccurredAt == nil:
		return &problem{ReasonFieldMissing, "occurred_at"}
	}

	return nil
}

// outOfRangeField names the first field with a value the columns cannot hold or
// the API could not serve. They are refused here, so they are skipped like any
// malformed record and never reach the database to be retried.
func (r row) outOfRangeField() *problem {
	switch {
	case *r.Sequence < 1 || *r.Sequence > math.MaxInt32:
		return &problem{ReasonFieldRange, "sequence"}
	case *r.EventVersion < 1 || *r.EventVersion > math.MaxInt32:
		return &problem{ReasonFieldRange, "event_version"}
	case !domain.ValidIdentifier(*r.ConversationID):
		return &problem{ReasonFieldRange, "conversation_id"}
	case r.IDYapeAccount != nil && *r.IDYapeAccount != "" && !domain.ValidIdentifier(*r.IDYapeAccount):
		return &problem{ReasonFieldRange, "id_yape_account"}
	case utf8.RuneCountInString(*r.EventName) > maxNameLength:
		return &problem{ReasonFieldRange, "event_name"}
	}

	return nil
}

// documents reads the two JSON columns, the payload first.
func (r row) documents() (payload, metadata json.RawMessage, p *problem) {
	payload, p = document(r.Payload, "payload", ReasonPayload)
	if p != nil {
		return nil, nil, p
	}
	metadata, p = document(r.Metadata, "metadata", ReasonMetadata)
	if p != nil {
		return nil, nil, p
	}

	return payload, metadata, nil
}

// document reads one JSON column: absent or null is a missing field, anything that
// is not a JSON object or array is malformed.
func document(raw json.RawMessage, field string, malformed Reason) (json.RawMessage, *problem) {
	if doc, ok := jsonDocument(raw); ok {
		return doc, nil
	}
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, &problem{ReasonFieldMissing, field}
	}

	return nil, &problem{malformed, field}
}

// jsonDocument returns the JSON a field holds. Debezium sends JSONB as a string
// that contains JSON; an object is accepted as it is. The result must be a JSON
// object or array, so a bare string or number is rejected.
func jsonDocument(raw json.RawMessage) (json.RawMessage, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, false
	}

	if raw[0] == '"' {
		var inner string
		if err := json.Unmarshal(raw, &inner); err != nil {
			return nil, false
		}
		raw = bytes.TrimSpace([]byte(inner))
	}

	if len(raw) == 0 || (raw[0] != '{' && raw[0] != '[') || !json.Valid(raw) {
		return nil, false
	}

	return json.RawMessage(raw), true
}

func text(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
