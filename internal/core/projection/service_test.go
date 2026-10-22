package projection

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

var _ service.Projection = (*Service)(nil)

func TestProject(t *testing.T) {
	occurredAt := time.Date(2026, 10, 20, 14, 13, 41, 711758000, time.UTC)

	base := func(name, payload string) domain.InboxEvent {
		return domain.InboxEvent{
			ConversationID: "conv_v1_a", Sequence: 2, EventName: name,
			Payload: json.RawMessage(payload), IDYapeAccount: "acc-1", OccurredAt: occurredAt,
		}
	}

	tests := []struct {
		name    string
		event   domain.InboxEvent
		want    *domain.Message
		wantErr string
	}{
		{
			name:  "userMessage",
			event: base("UserMessageSent", `{"text": "quiero yapear", "sequence": 1, "message_id": "m-1"}`),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleUser, AuthorID: "acc-1",
				Type: "text", Text: "quiero yapear", EventName: "UserMessageSent", IDYapeAccount: "acc-1", OccurredAt: occurredAt,
			},
		},
		{
			name: "aMessageInsideAFlowCarriesIt",
			event: func() domain.InboxEvent {
				e := base("UserMessageSent", `{"text": "20 soles"}`)
				e.Flow = "p2p"
				return e
			}(),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleUser, AuthorID: "acc-1",
				Type: "text", Text: "20 soles", EventName: "UserMessageSent", IDYapeAccount: "acc-1", Flow: "p2p", OccurredAt: occurredAt,
			},
		},
		{
			name:  "agentMessage",
			event: base("AgentMessageSent", `{"text": "¿A quién?", "sequence": 3}`),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs",
				Type: "text", Text: "¿A quién?", EventName: "AgentMessageSent", IDYapeAccount: "acc-1", OccurredAt: occurredAt,
			},
		},
		{
			name:  "guardrailResponseUsesResponseText",
			event: base("GuardrailResponseSent", `{"level": "level1", "response_text": "No puedo ayudarte", "classification": "blocked"}`),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs",
				Type: "text", Text: "No puedo ayudarte", EventName: "GuardrailResponseSent", IDYapeAccount: "acc-1", OccurredAt: occurredAt,
			},
		},
		{
			name:  "truncatedFlagIsCopied",
			event: base("AgentMessageSent", `{"text": "largo", "truncated": true}`),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs",
				Type: "text", Text: "largo", Truncated: true, EventName: "AgentMessageSent", IDYapeAccount: "acc-1", OccurredAt: occurredAt,
			},
		},
		{
			name:  "emptyTextIsKept",
			event: base("UserMessageSent", `{"text": ""}`),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleUser, AuthorID: "acc-1",
				Type: "text", Text: "", EventName: "UserMessageSent", IDYapeAccount: "acc-1", OccurredAt: occurredAt,
			},
		},
		{
			name: "agentWithoutAccountStillHasAnAuthor",
			event: func() domain.InboxEvent {
				e := base("AgentMessageSent", `{"text": "hola"}`)
				e.IDYapeAccount = ""
				return e
			}(),
			want: &domain.Message{
				ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs",
				Type: "text", Text: "hola", EventName: "AgentMessageSent", OccurredAt: occurredAt,
			},
		},
		{name: "userMessageBlockedIsSkipped", event: base("UserMessageBlocked", `{"text": "secret"}`)},
		{name: "toolActionIsSkipped", event: base("ToolActionSummarized", `{"tool_name": "draft_transfer"}`)},
		{name: "notificationIsSkipped", event: base("AgentNotificationDelivered", `{"text": "system notice"}`)},
		{name: "conversationStartedIsSkipped", event: base("ConversationStarted", `{"channel": "a2a"}`)},
		{name: "confirmationIsSkipped", event: base("ConfirmationRequested", `{"hint": "p2p_transfer"}`)},
		{name: "unknownNameIsSkipped", event: base("SomethingNew", `not even json`)},
		{name: "userTextMissing", event: base("UserMessageSent", `{"message_id": "m-1"}`), wantErr: "UserMessageSent has no text"},
		{name: "textNull", event: base("AgentMessageSent", `{"text": null}`), wantErr: "AgentMessageSent has no text"},
		{name: "guardrailWithTextOnlyIsInvalid", event: base("GuardrailResponseSent", `{"text": "wrong field"}`), wantErr: "GuardrailResponseSent has no text"},
		{name: "textNotAString", event: base("UserMessageSent", `{"text": 7}`), wantErr: "not a JSON object"},
		{name: "payloadNotJSON", event: base("UserMessageSent", `{broken`), wantErr: "not a JSON object"},
		{name: "payloadEmpty", event: base("AgentMessageSent", ``), wantErr: "not a JSON object"},
		{
			name: "userWithoutAccount",
			event: func() domain.InboxEvent {
				e := base("UserMessageSent", `{"text": "hola"}`)
				e.IDYapeAccount = ""
				return e
			}(),
			wantErr: "no account for its author",
		},
	}

	svc := NewService(nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := svc.Project(tt.event)

			if tt.wantErr != "" {
				require.ErrorIs(t, err, domain.ErrInvalidEvent)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProjectErrorNeverCarriesTheText(t *testing.T) {
	event := domain.InboxEvent{
		EventName: "UserMessageSent", Payload: json.RawMessage(`{"text": 42, "secret": "card 4111"}`), IDYapeAccount: "acc-1",
	}

	_, err := NewService(nil).Project(event)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "4111")
}
