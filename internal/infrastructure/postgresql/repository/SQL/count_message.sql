INSERT INTO conversation_summaries (id_yape_account, conversation_id, message_count, first_occurred_at, last_occurred_at)
VALUES ($1, $2, 1, $3, $3)
ON CONFLICT (id_yape_account, conversation_id) DO UPDATE SET
    message_count     = conversation_summaries.message_count + 1,
    first_occurred_at = LEAST(conversation_summaries.first_occurred_at, EXCLUDED.first_occurred_at),
    last_occurred_at  = GREATEST(conversation_summaries.last_occurred_at, EXCLUDED.last_occurred_at)
