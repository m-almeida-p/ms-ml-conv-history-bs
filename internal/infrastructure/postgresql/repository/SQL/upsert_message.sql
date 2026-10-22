INSERT INTO conversation_messages (
    conversation_id, sequence, role, author_id, type, text,
    truncated, event_name, id_yape_account, flow, occurred_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (conversation_id, sequence) DO UPDATE SET
    role            = EXCLUDED.role,
    author_id       = EXCLUDED.author_id,
    type            = EXCLUDED.type,
    text            = EXCLUDED.text,
    truncated       = EXCLUDED.truncated,
    event_name    = EXCLUDED.event_name,
    id_yape_account = EXCLUDED.id_yape_account,
    flow            = EXCLUDED.flow,
    occurred_at     = EXCLUDED.occurred_at
