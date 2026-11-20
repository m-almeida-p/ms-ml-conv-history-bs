package repository

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

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
	//go:embed SQL/conversation_owned.sql
	sqlConversationOwned string
	//go:embed SQL/list_messages_newest.sql
	sqlListMessagesNewest string
	//go:embed SQL/list_messages_before.sql
	sqlListMessagesBefore string
	//go:embed SQL/list_messages_after.sql
	sqlListMessagesAfter string
	//go:embed SQL/list_conversations.sql
	sqlListConversationsTemplate string
	//go:embed SQL/list_conversations_before.sql
	sqlListConversationsBeforeTemplate string
	//go:embed SQL/list_conversations_after.sql
	sqlListConversationsAfterTemplate string

	// The conversation statements carry the first message; its limit is the domain's, written into the statement once.
	sqlListConversations       = withFirstMessageLimit(sqlListConversationsTemplate)
	sqlListConversationsBefore = withFirstMessageLimit(sqlListConversationsBeforeTemplate)
	sqlListConversationsAfter  = withFirstMessageLimit(sqlListConversationsAfterTemplate)
)

// withFirstMessageLimit writes the limit of the first message into a conversation statement.
func withFirstMessageLimit(sql string) string {
	return strings.ReplaceAll(sql, "{{first_message_chars}}", strconv.Itoa(domain.FirstMessageMaxChars))
}

var _ repository.MessageRepository = (*PostgresMessageRepository)(nil)

// PostgresMessageRepository is the Postgres MessageRepository.
type PostgresMessageRepository struct {
	db postgresql.DB
}

// NewPostgresMessageRepository returns a message reader over db.
func NewPostgresMessageRepository(db postgresql.DB) *PostgresMessageRepository {
	return &PostgresMessageRepository{db: db}
}

// ListMessages implements repository.MessageRepository. It reads one row more
// than a page, which tells whether more exist without a second query.
func (r *PostgresMessageRepository) ListMessages(ctx context.Context, q domain.PageQuery) (domain.Page, error) {
	if q.Size < 1 || q.Size > domain.PageSize {
		return domain.Page{}, fmt.Errorf("list messages: the page size must be from 1 to %d", domain.PageSize)
	}

	// A conversation is read for an account, and it is the account's own or it is not found. This comes before everything else, so
	// that nothing of another account's conversation is read, not even whether a cursor is one of its messages.
	if err := r.checkOwner(ctx, q); err != nil {
		return domain.Page{}, err
	}

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

	hasMore := len(messages) > q.Size
	if hasMore {
		messages = messages[:q.Size]
	}
	// Backwards reads come newest first; a page always reads oldest to newest.
	if q.Direction != domain.DirectionAfter {
		slices.Reverse(messages)
	}

	return domain.Page{Messages: messages, HasMore: hasMore}, nil
}

// checkOwner confirms that the conversation is one of the account's. One that is not, or that does not exist, is domain.ErrNotFound:
// the two cannot be told apart.
func (r *PostgresMessageRepository) checkOwner(ctx context.Context, q domain.PageQuery) error {
	var owned bool
	if err := r.db.QueryRow(ctx, sqlConversationOwned, q.IDYapeAccount, q.ConversationID).Scan(&owned); err != nil {
		return dbError("find the owner of the conversation", err)
	}
	if !owned {
		return domain.ErrNotFound
	}

	return nil
}

// pageQuery chooses the statement and arguments that read the page q asks for.
// For a page beside a cursor it first checks the cursor.
func (r *PostgresMessageRepository) pageQuery(ctx context.Context, q domain.PageQuery) (string, []any, error) {
	limit := q.Size + 1

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

	return "", nil, fmt.Errorf("list messages: unknown direction %q", q.Direction)
}

// checkCursor confirms that the cursor is a message of the conversation. One that
// is not is an unknown conversation when the conversation has no messages, and an
// invalid cursor otherwise.
func (r *PostgresMessageRepository) checkCursor(ctx context.Context, q domain.PageQuery) error {
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
			flow    *string
		)
		if err := rows.Scan(&m.ConversationID, &m.Sequence, &role, &m.AuthorID, &m.Type, &m.Text,
			&m.Truncated, &m.EventName, &account, &flow, &m.OccurredAt); err != nil {
			return nil, dbError("scan message", err)
		}
		m.Role, m.IDYapeAccount, m.Flow = domain.Role(role), value(account), value(flow)
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("list messages", err)
	}

	return messages, nil
}

// ListConversations implements repository.MessageRepository. It reads conversation_summaries, one
// row per conversation, so a page costs the rows it returns and not the account's history. Like
// ListMessages, it reads one row more than a page, which tells whether more exist without a second
// query. A page beside a cursor starts from the position the cursor carries (the time of a
// conversation's last message, then its id as the tie-break), so the pages never overlap, and no
// lookup is needed: the cursor stays right even when the conversation it came from has moved since.
func (r *PostgresMessageRepository) ListConversations(ctx context.Context, q domain.ConversationQuery) (domain.ConversationList, error) {
	if q.Size < 1 || q.Size > domain.ConversationListSize {
		return domain.ConversationList{}, fmt.Errorf("list conversations: the page size must be from 1 to %d", domain.ConversationListSize)
	}

	query, args, err := conversationsQuery(q)
	if err != nil {
		return domain.ConversationList{}, err
	}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return domain.ConversationList{}, dbError("list conversations", err)
	}
	defer rows.Close()

	list := domain.ConversationList{Conversations: make([]domain.ConversationSummary, 0, q.Size+1)}
	for rows.Next() {
		var (
			c         domain.ConversationSummary
			first     *string
			truncated *bool
		)
		if err := rows.Scan(&c.ConversationID, &c.MessageCount, &c.FirstOccurredAt, &c.LastOccurredAt, &first, &truncated); err != nil {
			return domain.ConversationList{}, dbError("scan conversation", err)
		}
		c.FirstMessage, c.FirstMessageTruncated = value(first), truncated != nil && *truncated
		list.Conversations = append(list.Conversations, c)
	}
	if err := rows.Err(); err != nil {
		return domain.ConversationList{}, dbError("list conversations", err)
	}

	list.HasMore = len(list.Conversations) > q.Size
	if list.HasMore {
		list.Conversations = list.Conversations[:q.Size]
	}
	// A page after a cursor is read upwards from it, nearest first; a list is most recent first.
	if q.Direction == domain.DirectionAfter {
		slices.Reverse(list.Conversations)
	}

	return list, nil
}

// conversationsQuery chooses the statement and arguments that read the page q asks for.
func conversationsQuery(q domain.ConversationQuery) (string, []any, error) {
	limit := q.Size + 1

	switch q.Direction {
	case domain.DirectionNewest:
		return sqlListConversations, []any{q.IDYapeAccount, limit}, nil
	case domain.DirectionBefore:
		return sqlListConversationsBefore, []any{q.IDYapeAccount, q.Cursor.LastOccurredAt, q.Cursor.ConversationID, limit}, nil
	case domain.DirectionAfter:
		return sqlListConversationsAfter, []any{q.IDYapeAccount, q.Cursor.LastOccurredAt, q.Cursor.ConversationID, limit}, nil
	}

	return "", nil, fmt.Errorf("list conversations: unknown direction %q", q.Direction)
}

// value reads a nullable text column; NULL is the empty string.
func value(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
