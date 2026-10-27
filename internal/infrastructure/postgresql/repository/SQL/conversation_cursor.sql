SELECT max(occurred_at) AS last_occurred_at
FROM conversation_messages
WHERE 1=1
  AND id_yape_account = $1
  AND conversation_id = $2
GROUP BY conversation_id
