SELECT conversation_id, sequence, role, author_id, type, text, truncated, event_name, id_yape_account, flow, occurred_at
FROM conversation_messages
WHERE 1=1
  AND conversation_id = $1
  AND sequence > $2
ORDER BY sequence ASC
LIMIT $3
