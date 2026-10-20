package repository

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql"
)

var (
	//go:embed SQL/message_exists.sql
	sqlMessageExists string
	//go:embed SQL/conversation_exists.sql
	sqlConversationExists string
	//go:embed SQL/list_messages_newest.sql
	sqlListMessagesNewest string
	//go:embed SQL/list_messages_before.sql
	sqlListMessagesBefore string
	//go:embed SQL/list_messages_after.sql
	sqlListMessagesAfter string
	//go:embed SQL/list_conversations.sql
	sqlListConversations string
)

var _ repository.MessageRepository = (*Messages)(nil)

// Messages is the Postgres MessageRepository.
type Messages struct {
	db postgresql.DB
}

// NewMessages returns a message reader over db.
func NewMessages(db postgresql.DB) *Messages {
	return &Messages{db: db}
}

// ListMessages implements repository.MessageRepository. It reads one row more
// than a page, which tells whether more exist without a second query.
func (r *Messages) ListMessages(ctx context.Context, q domain.PageQuery) (domain.Page, error) {
	query, args, err := r.pageQuery(ctx, q)
	if err != nil {
		return domain.Page{}, err
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return domain.Page{}, dbError("list messages", err)
	}

	messages, err := scanMessages(rows)
	if err != nil {
		return domain.Page{}, err
	}

	// The newest page of a conversation with no messages means it is unknown. A
	// page past the last message is empty but valid: the cursor already proved the
	// conversation exists.
	if len(messages) == 0 && q.Direction == domain.DirectionNewest {
		return domain.Page{}, domain.ErrNotFound
	}

	hasMore := len(messages) > domain.PageSize
	if hasMore {
		messages = messages[:domain.PageSize]
	}
	// Backwards reads come newest first; a page always reads oldest to newest.
	if q.Direction != domain.DirectionAfter {
		slices.Reverse(messages)
	}

	return domain.Page{Messages: messages, HasMore: hasMore}, nil
}

// pageQuery chooses the statement and arguments that read the page q asks for.
// For a page beside a cursor it first checks the cursor.
func (r *Messages) pageQuery(ctx context.Context, q domain.PageQuery) (string, []any, error) {
	limit := domain.PageSize + 1

	switch q.Direction {
	case domain.DirectionNewest:
		return sqlListMessagesNewest, []any{q.ConversationID, limit}, nil
	case domain.DirectionBefore, domain.DirectionAfter:
		if err := r.checkCursor(ctx, q); err != nil {
			return "", nil, err
		}
		query := sqlListMessagesAfter
		if q.Direction == domain.DirectionBefore {
			query = sqlListMessagesBefore
		}

		return query, []any{q.ConversationID, q.CursorSequence, limit}, nil
	}

	return "", nil, fmt.Errorf("%w: unknown direction %q", domain.ErrInvalidRequest, q.Direction)
}

// checkCursor confirms that the cursor is a message of the conversation. One that
// is not is an unknown conversation when the conversation has no messages, and an
// invalid cursor otherwise.
func (r *Messages) checkCursor(ctx context.Context, q domain.PageQuery) error {
	var found int
	err := r.db.QueryRow(ctx, sqlMessageExists, q.ConversationID, q.CursorSequence).Scan(&found)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return dbError("find cursor", err)
	}

	var exists bool
	if err := r.db.QueryRow(ctx, sqlConversationExists, q.ConversationID).Scan(&exists); err != nil {
		return dbError("check conversation", err)
	}
	if !exists {
		return domain.ErrNotFound
	}

	return domain.ErrInvalidCursor
}

func scanMessages(rows pgx.Rows) ([]domain.Message, error) {
	defer rows.Close()

	messages := make([]domain.Message, 0, domain.PageSize+1)
	for rows.Next() {
		var (
			m       domain.Message
			role    string
			account *string
		)
		if err := rows.Scan(&m.ConversationID, &m.Sequence, &role, &m.AuthorID, &m.Type, &m.Text,
			&m.Truncated, &m.SourceEvent, &account, &m.ReceivedAt); err != nil {
			return nil, dbError("scan message", err)
		}
		m.Role, m.IDYapeAccount = domain.Role(role), value(account)
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("list messages", err)
	}

	return messages, nil
}

// ListConversations implements repository.MessageRepository.
func (r *Messages) ListConversations(ctx context.Context, idYapeAccount string) (domain.ConversationList, error) {
	rows, err := r.db.Query(ctx, sqlListConversations, idYapeAccount, domain.ConversationListSize+1)
	if err != nil {
		return domain.ConversationList{}, dbError("list conversations", err)
	}
	defer rows.Close()

	list := domain.ConversationList{Conversations: make([]domain.ConversationSummary, 0, domain.ConversationListSize+1)}
	for rows.Next() {
		var c domain.ConversationSummary
		if err := rows.Scan(&c.ConversationID, &c.MessageCount, &c.FirstReceivedAt, &c.LastReceivedAt); err != nil {
			return domain.ConversationList{}, dbError("scan conversation", err)
		}
		list.Conversations = append(list.Conversations, c)
	}
	if err := rows.Err(); err != nil {
		return domain.ConversationList{}, dbError("list conversations", err)
	}

	list.HasMore = len(list.Conversations) > domain.ConversationListSize
	if list.HasMore {
		list.Conversations = list.Conversations[:domain.ConversationListSize]
	}

	return list, nil
}
