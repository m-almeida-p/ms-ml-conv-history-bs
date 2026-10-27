INSERT INTO conversation_messages (
    conversation_id, sequence, role, author_id, type, text,
    truncated, event_name, id_yape_account, flow, occurred_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (conversation_id, sequence) DO NOTHING
