SELECT conversation_id, count(*) AS message_count, min(received_at) AS first_received_at, max(received_at) AS last_received_at
FROM conversation_messages
WHERE 1=1
  AND id_yape_account = $1
GROUP BY conversation_id
ORDER BY max(received_at) DESC, conversation_id
LIMIT $2
