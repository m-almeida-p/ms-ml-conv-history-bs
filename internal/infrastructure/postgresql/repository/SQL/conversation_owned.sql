SELECT EXISTS (
    SELECT 1
    FROM conversation_summaries
    WHERE 1=1
      AND id_yape_account = $1
      AND conversation_id = $2
)
