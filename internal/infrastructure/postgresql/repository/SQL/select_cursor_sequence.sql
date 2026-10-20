SELECT sequence
FROM conversation_messages
WHERE 1=1
  AND conversation_id = $1
  AND outbox_id = $2
