INSERT INTO conversation_messages (
    conversation_id, sequence, role, author_id, type, text,
    truncated, source_event, id_yape_account, received_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (conversation_id, sequence) DO UPDATE SET
    role            = EXCLUDED.role,
    author_id       = EXCLUDED.author_id,
    type            = EXCLUDED.type,
    text            = EXCLUDED.text,
    truncated       = EXCLUDED.truncated,
    source_event    = EXCLUDED.source_event,
    id_yape_account = EXCLUDED.id_yape_account,
    received_at     = EXCLUDED.received_at
