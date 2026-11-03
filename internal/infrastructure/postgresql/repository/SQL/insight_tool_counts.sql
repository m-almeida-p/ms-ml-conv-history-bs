-- Calls of the agent's tools for an account, by tool and outcome. The account's conversations are
-- passed as an array so the filter reaches the inbox's primary key.
SELECT coalesce(payload ->> 'tool_name', 'unknown') AS tool,
       coalesce(payload ->> 'outcome', 'unknown')   AS outcome,
       count(*)                                     AS calls
FROM conversation_inbox
WHERE conversation_id = ANY (ARRAY(SELECT conversation_id FROM conversation_summaries WHERE id_yape_account = $1))
  AND event_name = 'ToolActionSummarized'
  AND occurred_at >= $2
GROUP BY 1, 2
ORDER BY 1, 2
