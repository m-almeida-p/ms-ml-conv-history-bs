SELECT conversation_id, count(*) AS message_count, min(occurred_at) AS first_occurred_at, max(occurred_at) AS last_occurred_at
FROM conversation_messages
WHERE 1=1
  AND id_yape_account = $1
GROUP BY conversation_id
HAVING max(occurred_at) > $2 OR (max(occurred_at) = $2 AND conversation_id < $3)
ORDER BY max(occurred_at), conversation_id DESC
LIMIT $4
