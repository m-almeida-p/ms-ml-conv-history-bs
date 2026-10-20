SELECT conversation_id, sequence, event_name, event_version, payload, metadata,
       correlation_id, id_yape_account, actor, occurred_at, kafka_topic, kafka_partition, kafka_offset
FROM conversation_inbox
WHERE (conversation_id, sequence) > ($1, $2)
ORDER BY conversation_id, sequence
LIMIT $3
