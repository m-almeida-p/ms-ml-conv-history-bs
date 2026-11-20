WITH page AS (
    SELECT conversation_id, message_count, first_occurred_at, last_occurred_at
    FROM conversation_summaries
    WHERE 1=1
      AND id_yape_account = $1
      AND (last_occurred_at, conversation_id) > ($2, $3)
    ORDER BY last_occurred_at, conversation_id
    LIMIT $4
)
-- The first message is read for the rows of the page only: the LIMIT above is applied before the join.
SELECT p.conversation_id, p.message_count, p.first_occurred_at, p.last_occurred_at, f.text, f.truncated
FROM page p
LEFT JOIN LATERAL (
    SELECT left(m.text, {{first_message_chars}}) AS text,
           (m.truncated OR char_length(m.text) > {{first_message_chars}}) AS truncated
    FROM conversation_messages m
    WHERE m.conversation_id = p.conversation_id
      AND m.role = 'appUser'
    ORDER BY m.sequence
    LIMIT 1
) f ON true
ORDER BY p.last_occurred_at, p.conversation_id
