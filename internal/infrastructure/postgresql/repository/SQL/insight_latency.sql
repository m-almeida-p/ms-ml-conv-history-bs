-- How long the segments of one flow of an account take. A segment opens with the user's request
-- (its from_sequence) and holds its card (ConfirmationRequested), its decision
-- (ConfirmationResolved) and, when the payment asked for one, PaymentChallengeIssued.
-- Segments that never reached the card are left out: there is nothing to time.
WITH seg AS (
    SELECT conversation_id, segment, from_sequence, to_sequence
    FROM conversation_flow_segments
    WHERE conversation_id = ANY (ARRAY(SELECT conversation_id FROM conversation_summaries WHERE id_yape_account = $1))
      AND flow = $2
      AND started_at >= $3
), timed AS (
    SELECT seg.conversation_id, seg.segment,
           min(i.occurred_at) FILTER (WHERE i.sequence = seg.from_sequence)     AS asked_at,
           min(i.occurred_at) FILTER (WHERE i.event_name = 'ConfirmationRequested') AS card_at,
           min(i.occurred_at) FILTER (WHERE i.event_name = 'ConfirmationResolved')  AS resolved_at,
           coalesce(bool_or(i.event_name = 'PaymentChallengeIssued'), false)        AS had_otp
    FROM seg
    JOIN conversation_inbox i
      ON i.conversation_id = seg.conversation_id
     AND i.sequence BETWEEN seg.from_sequence AND seg.to_sequence
    GROUP BY seg.conversation_id, seg.segment
), measured AS (
    SELECT had_otp, card_at, resolved_at,
           extract(epoch FROM card_at - asked_at)::float8     AS ask_to_card,
           extract(epoch FROM resolved_at - card_at)::float8  AS card_to_resolution
    FROM timed
    WHERE card_at IS NOT NULL
)
SELECT had_otp,
       count(*)                                                            AS cards,
       count(resolved_at)                                                  AS resolved,
       avg(ask_to_card)                                                    AS ask_to_card_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY ask_to_card)            AS ask_to_card_median,
       avg(card_to_resolution)                                             AS card_to_resolution_avg,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY card_to_resolution)     AS card_to_resolution_median
FROM measured
GROUP BY had_otp
ORDER BY had_otp
