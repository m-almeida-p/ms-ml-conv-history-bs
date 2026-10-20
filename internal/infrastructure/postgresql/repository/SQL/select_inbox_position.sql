SELECT conversation_id, sequence
FROM conversation_inbox
WHERE 1=1
  AND outbox_id = $1
