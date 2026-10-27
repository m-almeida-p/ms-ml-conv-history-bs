package domain

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAConversationCursorRoundTrips(t *testing.T) {
	at := time.Date(2026, 10, 27, 14, 7, 23, 792_123_000, time.UTC) // microseconds, as Postgres keeps them

	cursor := NewConversationCursor(at, "conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52")
	parsed, err := ParseConversationCursor(cursor.String())

	require.NoError(t, err)
	assert.True(t, parsed.LastOccurredAt.Equal(at), "the instant survives to the microsecond: %s", parsed.LastOccurredAt)
	assert.Equal(t, "conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52", parsed.ConversationID)
	assert.Equal(t, time.UTC, parsed.LastOccurredAt.Location())
}

func TestAConversationCursorHasOnePlainForm(t *testing.T) {
	at := time.UnixMicro(1_792_771_559_895_000)

	assert.Equal(t, "1792771559895000~conv_a", NewConversationCursor(at, "conv_a").String())
}

func TestAConversationCursorKeepsEveryCharacterAnIDMayHold(t *testing.T) {
	cursor := NewConversationCursor(time.UnixMicro(5), "conv.A_b:c-9")

	parsed, err := ParseConversationCursor(cursor.String())

	require.NoError(t, err)
	assert.Equal(t, "conv.A_b:c-9", parsed.ConversationID)
	assert.NotContains(t, cursor.String(), "%", "nothing in a cursor needs escaping in a URL")
	assert.NotContains(t, cursor.String(), "[")
}

func TestOnlyTheFormTheServiceIssuesIsACursor(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{"empty", ""},
		{"onlyAnID", "conv_v1_a"},
		{"onlyADigits", "1792771559895000"},
		{"noSeparator", "1792771559895000conv_a"},
		{"emptyID", "1792771559895000~"},
		{"emptyInstant", "~conv_a"},
		{"theSeparatorTwice", "1792771559895000~conv~a"},
		{"instantNotANumber", "abc~conv_a"},
		{"instantWithASign", "+5~conv_a"},
		{"instantNegative", "-5~conv_a"},
		{"instantWithLeadingZeros", "05~conv_a"},
		{"instantZero", "0~conv_a"},
		{"instantWithASpace", " 5~conv_a"},
		{"instantInExponentForm", "1e6~conv_a"},
		{"instantBeyondInt64", "9223372036854775808~conv_a"},
		{"instantAfterYear9999", "253402300800000000~conv_a"},
		{"idWithASpace", "5~conv a"},
		{"idWithSQLCharacters", "5~x'; drop table--"},
		{"idWithASlash", "5~../etc"},
		{"idWithANul", "5~conv\x00a"},
		{"idTooLong", "5~" + strings.Repeat("c", 129)},
		{"veryLong", strings.Repeat("9", 5000) + "~a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseConversationCursor(tt.cursor)

			require.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}

func TestTheLargestAndSmallestInstantsAreCursors(t *testing.T) {
	for _, cursor := range []string{"1~a", "253402300799999999~a", "1~" + strings.Repeat("c", 128)} {
		parsed, err := ParseConversationCursor(cursor)

		require.NoError(t, err, cursor)
		assert.Equal(t, cursor, parsed.String())
	}
}
