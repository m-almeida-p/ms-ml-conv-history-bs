SELECT id_yape_account,
       count(*)                  AS conversations,
       sum(message_count)::bigint AS messages,
       min(first_occurred_at)    AS first_occurred_at,
       max(last_occurred_at)     AS last_occurred_at
FROM conversation_summaries
GROUP BY id_yape_account
ORDER BY max(last_occurred_at) DESC, id_yape_account DESC
LIMIT $1 OFFSET $2
