package kafka

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rangeRecord builds a record whose numeric and name fields are the given values.
func rangeRecord(sequence, version, conversationID, eventName string) string {
	return fmt.Sprintf(`{"conversation_id":%q,"sequence":%s,"event_name":%q,"event_version":%s,`+
		`"payload":"{}","metadata":"{}","occurred_at":"2026-10-20T14:07:23Z"}`, conversationID, sequence, eventName, version)
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
		{name: "smallestValues", value: rangeRecord("1", "1", "c", "E")},
		{name: "largestSequenceAndVersion", value: rangeRecord(maxInt32, maxInt32, "c", "E")},
		{name: "namesOfExactlyTheColumnSize", value: rangeRecord("1", "1", name128, name128)},
		{name: "anIDTheAPIAcceptsEndToEnd", value: rangeRecord("1", "1", "conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52", "E")},
		{name: "eventNamesWithMultibyteCharactersCountCharacters", value: rangeRecord("1", "1", "c", multibyte128)},

		{name: "sequenceZero", value: rangeRecord("0", "1", "c", "E"), wantField: "sequence"},
		{name: "sequenceNegative", value: rangeRecord("-1", "1", "c", "E"), wantField: "sequence"},
		{name: "sequenceJustOverInt4", value: rangeRecord("2147483648", "1", "c", "E"), wantField: "sequence"},
		{name: "sequenceFarOverInt4", value: rangeRecord("3000000000", "1", "c", "E"), wantField: "sequence"},
		{name: "versionZero", value: rangeRecord("1", "0", "c", "E"), wantField: "event_version"},
		{name: "versionOverInt4", value: rangeRecord("1", "2147483648", "c", "E"), wantField: "event_version"},
		{name: "conversationIDOneTooLong", value: rangeRecord("1", "1", name128+"a", "E"), wantField: "conversation_id"},
		{name: "conversationIDOneCharacterTooLongInMultibyte", value: rangeRecord("1", "1", multibyte128+"ñ", "E"), wantField: "conversation_id"},
		// The API serves only ids of this shape, so a record outside it would be stored and never found.
		{name: "conversationIDWithASpace", value: rangeRecord("1", "1", "conv 1", "E"), wantField: "conversation_id"},
		{name: "conversationIDWithANewline", value: rangeRecord("1", "1", "conv\n1", "E"), wantField: "conversation_id"},
		{name: "conversationIDWithASlash", value: rangeRecord("1", "1", "../etc", "E"), wantField: "conversation_id"},
		{name: "conversationIDWithNonASCII", value: rangeRecord("1", "1", multibyte128, "E"), wantField: "conversation_id"},
		{name: "eventNameOneTooLong", value: rangeRecord("1", "1", "c", name128+"a"), wantField: "event_name"},
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

// The account is optional, but when present it must be an id the API can ask for.
func TestDecodeChecksTheAccountIdentifier(t *testing.T) {
	body := func(account string) string {
		return `{"conversation_id":"c","sequence":1,"event_name":"E","event_version":1,"payload":"{}","metadata":"{}",` +
			`"id_yape_account":` + account + `,"occurred_at":"2026-10-20T14:07:23Z"}`
	}

	tests := []struct {
		name      string
		account   string
		want      string
		wantField string
	}{
		{name: "aUUID", account: `"896900db-eddd-4c54-99e5-5a32f0830f5e"`, want: "896900db-eddd-4c54-99e5-5a32f0830f5e"},
		{name: "absentIsAllowed", account: `null`},
		{name: "emptyIsAllowed", account: `""`},
		{name: "withASpace", account: `"acc 1"`, wantField: "id_yape_account"},
		{name: "withANewline", account: `"acc\n1"`, wantField: "id_yape_account"},
		{name: "withNonASCII", account: `"acc-ü"`, wantField: "id_yape_account"},
		{name: "oneTooLong", account: `"` + strings.Repeat("a", 129) + `"`, wantField: "id_yape_account"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode(record(body(tt.account)))

			if tt.wantField == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got.IDYapeAccount)
				return
			}
			var decodeErr *DecodeError
			require.ErrorAs(t, err, &decodeErr)
			assert.Equal(t, ReasonFieldRange, decodeErr.Reason)
			assert.Equal(t, tt.wantField, decodeErr.Field)
		})
	}
}
