SELECT conversation_id, sequence, outbox_id, role, author_id, type, text, truncated, source_event, id_yape_account, received_at
FROM conversation_messages
WHERE 1=1
  AND conversation_id = $1
  AND sequence > $2
ORDER BY sequence ASC
LIMIT $3
