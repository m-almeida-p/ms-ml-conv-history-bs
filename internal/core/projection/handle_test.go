package projection

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
)

func TestHandle(t *testing.T) {
	at := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
	event := func(name, payload string) domain.InboxEvent {
		return domain.InboxEvent{
			ConversationID: "conv", Sequence: 2, EventName: name, Payload: json.RawMessage(payload),
			IDYapeAccount: "acc-1", OccurredAt: at,
		}
	}
	storeDown := errors.New("connection reset")
	rejected := errors.Join(domain.ErrRejected, errors.New("constraint"))

	tests := []struct {
		name        string
		event       domain.InboxEvent
		wantMessage bool
		applyRes    domain.ApplyResult
		applyErr    error
		want        domain.ApplyResult
		wantErr     error
	}{
		{
			name:        "chatEventIsStoredWithItsMessage",
			event:       event("UserMessageSent", `{"text": "hola"}`),
			wantMessage: true,
			applyRes:    domain.ApplyResult{Applied: true, Projected: true},
			want:        domain.ApplyResult{Applied: true, Projected: true},
		},
		{
			name:     "otherEventIsStoredWithoutAMessage",
			event:    event("ToolActionSummarized", `{}`),
			applyRes: domain.ApplyResult{Applied: true},
			want:     domain.ApplyResult{Applied: true},
		},
		{
			name:        "repeatedEventChangesNothing",
			event:       event("UserMessageSent", `{"text": "hola"}`),
			wantMessage: true,
			applyRes:    domain.ApplyResult{},
			want:        domain.ApplyResult{},
		},
		{
			name:     "eventThatCannotProjectIsStoredAndFlagged",
			event:    event("UserMessageSent", `{"no_text": true}`),
			applyRes: domain.ApplyResult{Applied: true},
			want:     domain.ApplyResult{Applied: true, Invalid: true},
		},
		{
			name:     "repeatedInvalidEventIsNotFlaggedAgain",
			event:    event("UserMessageSent", `{"no_text": true}`),
			applyRes: domain.ApplyResult{},
			want:     domain.ApplyResult{},
		},
		{
			name:        "storeErrorIsReturnedAsIs",
			event:       event("UserMessageSent", `{"text": "hola"}`),
			wantMessage: true,
			applyErr:    storeDown,
			wantErr:     storeDown,
		},
		{
			name:        "rejectionIsKeptForTheCaller",
			event:       event("UserMessageSent", `{"text": "hola"}`),
			wantMessage: true,
			applyErr:    rejected,
			wantErr:     domain.ErrRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inbox := mocks.NewInboxRepository(t)
			inbox.On("Apply", mock.Anything, tt.event, mock.MatchedBy(func(m *domain.Message) bool {
				return (m != nil) == tt.wantMessage
			})).Return(tt.applyRes, tt.applyErr).Once()

			got, err := NewService(inbox).Handle(context.Background(), tt.event)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Equal(t, domain.ApplyResult{}, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
