INSERT INTO conversation_inbox (
    conversation_id, sequence, event_name, event_version, payload, metadata,
    correlation_id, id_yape_account, actor, occurred_at,
    kafka_topic, kafka_partition, kafka_offset, projected_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
ON CONFLICT (conversation_id, sequence) DO NOTHING
