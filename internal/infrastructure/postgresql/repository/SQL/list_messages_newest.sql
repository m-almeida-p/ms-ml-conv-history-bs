SELECT conversation_id, sequence, role, author_id, type, text, truncated, event_name, id_yape_account, flow, occurred_at
FROM conversation_messages
WHERE 1=1
  AND conversation_id = $1
  AND id_yape_account = $3
ORDER BY sequence DESC
LIMIT $2
