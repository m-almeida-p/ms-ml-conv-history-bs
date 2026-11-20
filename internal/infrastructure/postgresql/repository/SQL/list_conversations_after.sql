WITH page AS (
    SELECT conversation_id, id_yape_account, message_count, first_occurred_at, last_occurred_at
    FROM conversation_summaries
    WHERE 1=1
      AND id_yape_account = $1
      AND (last_occurred_at, conversation_id) > ($2, $3)
    ORDER BY last_occurred_at, conversation_id
    LIMIT $4
)
-- The first message is read for the rows of the page only: the LIMIT above is applied before the join.
-- The text is shown only when the message is of the account that asks. The lookup stays by the key of the messages, which reads one row;
-- a conversation that two accounts wrote into (the agent does not check whose a conversation is) shows no first message to the account
-- whose first one is not the first of the conversation, and never shows the other's.
SELECT p.conversation_id, p.message_count, p.first_occurred_at, p.last_occurred_at,
       CASE WHEN f.account = p.id_yape_account THEN f.text END,
       CASE WHEN f.account = p.id_yape_account THEN f.truncated END
FROM page p
LEFT JOIN LATERAL (
    SELECT left(m.text, {{first_message_chars}}) AS text,
           (m.truncated OR char_length(m.text) > {{first_message_chars}}) AS truncated,
           m.id_yape_account AS account
    FROM conversation_messages m
    WHERE m.conversation_id = p.conversation_id
      AND m.role = 'appUser'
    ORDER BY m.sequence
    LIMIT 1
) f ON true
ORDER BY p.last_occurred_at, p.conversation_id
