package kafka

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rangeRecord builds a record whose numeric and name fields are the given values.
func rangeRecord(id, sequence, version, conversationID, eventName string) string {
	return fmt.Sprintf(`{"id":%s,"conversation_id":%q,"sequence":%s,"event_name":%q,"event_version":%s,`+
		`"payload":"{}","metadata":"{}","created_at":"2026-10-20T14:07:23Z"}`, id, conversationID, sequence, eventName, version)
}

func TestDecodeKeepsToWhatTheColumnsHold(t *testing.T) {
	const maxInt32 = "2147483647"
	name128 := strings.Repeat("a", 128)
	multibyte128 := strings.Repeat("ñ", 128) // 256 bytes, 128 characters: VARCHAR(128) holds it

	tests := []struct {
		name      string
		value     string
		wantField string // empty when the record is accepted
	}{
		{name: "smallestValues", value: rangeRecord("1", "1", "1", "c", "E")},
		{name: "largestSequenceAndVersion", value: rangeRecord("9223372036854775807", maxInt32, maxInt32, "c", "E")},
		{name: "namesOfExactlyTheColumnSize", value: rangeRecord("1", "1", "1", name128, name128)},
		{name: "namesWithMultibyteCharactersCountCharacters", value: rangeRecord("1", "1", "1", multibyte128, multibyte128)},

		{name: "idZero", value: rangeRecord("0", "1", "1", "c", "E"), wantField: "id"},
		{name: "idNegative", value: rangeRecord("-5", "1", "1", "c", "E"), wantField: "id"},
		{name: "sequenceZero", value: rangeRecord("1", "0", "1", "c", "E"), wantField: "sequence"},
		{name: "sequenceNegative", value: rangeRecord("1", "-1", "1", "c", "E"), wantField: "sequence"},
		{name: "sequenceJustOverInt4", value: rangeRecord("1", "2147483648", "1", "c", "E"), wantField: "sequence"},
		{name: "sequenceFarOverInt4", value: rangeRecord("1", "3000000000", "1", "c", "E"), wantField: "sequence"},
		{name: "versionZero", value: rangeRecord("1", "1", "0", "c", "E"), wantField: "event_version"},
		{name: "versionOverInt4", value: rangeRecord("1", "1", "2147483648", "c", "E"), wantField: "event_version"},
		{name: "conversationIDOneTooLong", value: rangeRecord("1", "1", "1", name128+"a", "E"), wantField: "conversation_id"},
		{name: "conversationIDOneCharacterTooLongInMultibyte", value: rangeRecord("1", "1", "1", multibyte128+"ñ", "E"), wantField: "conversation_id"},
		{name: "eventNameOneTooLong", value: rangeRecord("1", "1", "1", "c", name128+"a"), wantField: "event_name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode(record(tt.value))

			if tt.wantField == "" {
				require.NoError(t, err)
				assert.NotNil(t, got)
				return
			}
			var decodeErr *DecodeError
			require.ErrorAs(t, err, &decodeErr)
			assert.Equal(t, ReasonFieldRange, decodeErr.Reason)
			assert.Equal(t, tt.wantField, decodeErr.Field)
			assert.Nil(t, got)
			assert.NotContains(t, err.Error(), name128, "the error names the field, never the value")
		})
	}
}
