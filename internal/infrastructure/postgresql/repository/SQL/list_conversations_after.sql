SELECT conversation_id, message_count, first_occurred_at, last_occurred_at
FROM conversation_summaries
WHERE 1=1
  AND id_yape_account = $1
  AND (last_occurred_at, conversation_id) > ($2, $3)
ORDER BY last_occurred_at, conversation_id
LIMIT $4
