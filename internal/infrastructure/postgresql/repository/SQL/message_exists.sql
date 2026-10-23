SELECT 1
FROM conversation_messages
WHERE 1=1
  AND conversation_id = $1
  AND sequence = $2
