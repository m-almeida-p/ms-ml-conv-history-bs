-- How the segments of one flow of an account ended. The view has no account column, so the
-- account's conversations are passed as an array: IN (SELECT ...) would make Postgres compute the
-- whole view before it filters, and the array lets the filter reach the inbox's primary key.
SELECT flow, outcome, count(*) AS segments
FROM conversation_flow_segments
WHERE conversation_id = ANY (ARRAY(SELECT conversation_id FROM conversation_summaries WHERE id_yape_account = $1))
  AND flow = $2
  AND started_at >= $3
GROUP BY flow, outcome
ORDER BY outcome
