package projection

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
)

func TestRebuild(t *testing.T) {
	storeDown := errors.New("store down")

	tests := []struct {
		name     string
		repoRes  domain.RebuildResult
		repoErr  error
		want     domain.RebuildResult
		wantErr  string
		wantCtxE error
	}{
		{
			name:    "returnsTheCounts",
			repoRes: domain.RebuildResult{Events: 6, Messages: 4, Invalid: 1},
			want:    domain.RebuildResult{Events: 6, Messages: 4, Invalid: 1},
		},
		{name: "repositoryErrorIsWrapped", repoErr: storeDown, wantErr: "rebuild messages from the inbox"},
		{name: "cancelledContextIsKept", repoErr: context.Canceled, wantErr: "rebuild messages from the inbox", wantCtxE: context.Canceled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inbox := mocks.NewInboxRepository(t)
			inbox.On("Rebuild", mock.Anything, mock.Anything).Return(tt.repoRes, tt.repoErr).Once()

			got, err := NewService(inbox).Rebuild(context.Background())

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.ErrorIs(t, err, tt.repoErr)
				assert.Equal(t, domain.RebuildResult{}, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRebuildReplaysThroughProject(t *testing.T) {
	inbox := mocks.NewInboxRepository(t)
	var projected *domain.Message
	inbox.On("Rebuild", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			project := args.Get(1).(func(domain.InboxEvent) (*domain.Message, error))
			var err error
			projected, err = project(domain.InboxEvent{
				EventName: "AgentMessageSent", Payload: json.RawMessage(`{"text": "hola"}`), Sequence: 3,
			})
			require.NoError(t, err)
		}).
		Return(domain.RebuildResult{}, nil).Once()

	_, err := NewService(inbox).Rebuild(context.Background())

	require.NoError(t, err)
	require.NotNil(t, projected)
	assert.Equal(t, "hola", projected.Text, "the repository replays with the service's own mapping")
	assert.Equal(t, 3, projected.Sequence)
}
