-- The history schema as the tests build it: a verbatim copy of the four up migrations of
-- ms-ml-conv-history-migrations (migrations/history/conversation, 000001 to 000004), in order.
-- That repository is the source of truth; this copy only lets the tests of this service run with no other repository.
-- To refresh it: cat <that folder>/*.up.sql, keeping this header.

-- ==== 000001_create_schema.up.sql ====
-- Conversation history, part 1: the schema.
-- The tables of this flow (history/conversation) live in the schema `history`, not in `public`. Every name in the later
-- migrations is qualified with it, because migrate runs with the default search_path.
-- The version table of this folder (history_conversation_schema_migrations) stays in public: migrate creates it before the
-- first migration runs, when this schema does not exist yet.
CREATE SCHEMA IF NOT EXISTS history;

COMMENT ON SCHEMA history IS
    'Conversation history: the inbox, the chat messages, the conversation summaries and the flow-segments view. Written by ms-ml-conv-history-bs and by the Camel consumers that share its tables.';

-- ==== 000002_create_inbox_and_messages.up.sql ====
-- Conversation history, part 2: the inbox and the chat read model.
-- Every name is qualified with the schema history (created by 000001). Every statement is IF NOT EXISTS, so running it over existing tables is safe.

-- The inbox keeps every conversation_events row that reaches this service, before
-- and whether or not it becomes a chat message. It is the source a rebuild replays.
CREATE TABLE IF NOT EXISTS history.conversation_inbox (
    conversation_id VARCHAR(128) NOT NULL,
    sequence        INT          NOT NULL,
    event_name      VARCHAR(128) NOT NULL,
    event_version   INT          NOT NULL,
    payload         JSONB        NOT NULL,
    metadata        JSONB        NOT NULL,
    correlation_id  TEXT,
    id_yape_account TEXT,
    actor           TEXT CHECK (actor IN ('user', 'agent', 'system')),
    flow            TEXT,
    occurred_at     TIMESTAMPTZ  NOT NULL,
    kafka_topic     TEXT         NOT NULL,
    kafka_partition INT          NOT NULL,
    kafka_offset    BIGINT       NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    projected_at    TIMESTAMPTZ,
    CONSTRAINT conversation_inbox_stream_key PRIMARY KEY (conversation_id, sequence)
);

COMMENT ON TABLE  history.conversation_inbox IS
    'Every conversation_events row received from Kafka, stored once. Replaying it rebuilds conversation_messages.';
COMMENT ON COLUMN history.conversation_inbox.conversation_id IS
    'The conversation the event belongs to (the A2A contextId). With sequence, the dedupe key: a redelivery hits ON CONFLICT DO NOTHING.';
COMMENT ON COLUMN history.conversation_inbox.sequence IS
    'Position of the event in its conversation stream, the same value as conversation_events.sequence. Starts at 1.';
COMMENT ON COLUMN history.conversation_inbox.event_name IS
    'Name of the event, such as UserMessageSent.';
COMMENT ON COLUMN history.conversation_inbox.event_version IS
    'Shape version of the payload when it was written.';
COMMENT ON COLUMN history.conversation_inbox.payload IS
    'The event payload, parsed from the JSON string Debezium sends.';
COMMENT ON COLUMN history.conversation_inbox.metadata IS
    'The event metadata, parsed from the JSON string Debezium sends.';
COMMENT ON COLUMN history.conversation_inbox.correlation_id IS
    'Copy of the conversation_events column of the same name.';
COMMENT ON COLUMN history.conversation_inbox.id_yape_account IS
    'Copy of the conversation_events column of the same name: the customer account.';
COMMENT ON COLUMN history.conversation_inbox.actor IS
    'Who caused the fact, as a business role: user, agent or system.';
COMMENT ON COLUMN history.conversation_inbox.flow IS
    'Copy of the conversation_events column of the same name: the flow the fact took place in, such as p2p, or NULL outside any flow.';
COMMENT ON COLUMN history.conversation_inbox.occurred_at IS
    'The conversation_events occurred_at: when the fact happened, in UTC.';
COMMENT ON COLUMN history.conversation_inbox.kafka_topic IS
    'Topic the record was read from. Provenance only.';
COMMENT ON COLUMN history.conversation_inbox.kafka_partition IS
    'Partition the record was read from. Provenance only.';
COMMENT ON COLUMN history.conversation_inbox.kafka_offset IS
    'Offset of the record in its partition. Provenance only.';
COMMENT ON COLUMN history.conversation_inbox.received_at IS
    'When this service stored the event.';
COMMENT ON COLUMN history.conversation_inbox.projected_at IS
    'When the event was projected, set in the same transaction as the projection. A skipped event is stamped too.';

-- Only the chat between the user and the assistant, shaped after the Smooch message.
CREATE TABLE IF NOT EXISTS history.conversation_messages (
    conversation_id VARCHAR(128) NOT NULL,
    sequence        INT          NOT NULL,
    role            TEXT         NOT NULL CHECK (role IN ('appUser', 'appMaker')),
    author_id       TEXT         NOT NULL,
    type            TEXT         NOT NULL DEFAULT 'text',
    text            TEXT         NOT NULL,
    truncated       BOOLEAN      NOT NULL DEFAULT FALSE,
    event_name      VARCHAR(128) NOT NULL,
    id_yape_account TEXT,
    flow            TEXT,
    occurred_at     TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (conversation_id, sequence)
);

-- The conversation list asks for one account's conversations, newest first.
CREATE INDEX IF NOT EXISTS conversation_messages_account_idx
    ON history.conversation_messages (id_yape_account, occurred_at DESC);

COMMENT ON TABLE  history.conversation_messages IS
    'The chat history: one row per message between a user and the assistant. Built from conversation_inbox.';
COMMENT ON COLUMN history.conversation_messages.conversation_id IS
    'Smooch conversation._id.';
COMMENT ON COLUMN history.conversation_messages.sequence IS
    'Position of the message''s event in the conversation stream. Orders the chat; gaps are normal because other events fill them. Smooch _id, as a string in the API, unique within the conversation, and the paging cursor.';
COMMENT ON COLUMN history.conversation_messages.role IS
    'Smooch role: appUser for the customer, appMaker for the assistant and guardrail replies.';
COMMENT ON COLUMN history.conversation_messages.author_id IS
    'Smooch authorId: the customer account for appUser, ms-ml-genai-agent-bs for appMaker.';
COMMENT ON COLUMN history.conversation_messages.type IS
    'Smooch message type. Always text for now.';
COMMENT ON COLUMN history.conversation_messages.text IS
    'The message text, as the event recorded it.';
COMMENT ON COLUMN history.conversation_messages.truncated IS
    'Smooch metadata.truncated: the recorded text was cut at the agent''s limit.';
COMMENT ON COLUMN history.conversation_messages.event_name IS
    'The name of the event that produced the message, as in conversation_inbox. Smooch metadata.eventName.';
COMMENT ON COLUMN history.conversation_messages.id_yape_account IS
    'The customer account, used to list their conversations.';
COMMENT ON COLUMN history.conversation_messages.flow IS
    'The flow the message took place in, such as p2p, or NULL outside any flow. A message the user writes carries the flow they were in when they wrote it, so the message that opens a flow has none and the one that leaves it carries the flow it was in.';
COMMENT ON COLUMN history.conversation_messages.occurred_at IS
    'When the event happened, in UTC, as in the event store. Smooch received, sent as Unix seconds. Not when this service stored it: that is conversation_inbox.received_at.';

-- ==== 000003_create_flow_segments_view.up.sql ====
-- Conversation history, part 3: a conversation cut into segments of a flow.
-- Every name is qualified with the schema history (created by 000001). Every statement is repeatable, so running it over an existing view is safe.
--
-- The inbox stamps each fact with the flow that was active when it arrived. That
-- leaves two messages at the edge of a flow on the wrong side of it: the user
-- message that opens a flow arrives before the dispatcher routes it, and the one
-- that leaves a flow arrives while the flow is still active. This view reads the
-- facts again by who handled them.
--
--   * A user message (UserMessageSent) belongs to the flow of the next fact that is
--     not a user message, the one that handled it. With no next fact it keeps the
--     flow it was written in.
--   * The facts of the conversation itself (the start and the close, a blocked
--     message, the guardrail reply, a failed turn) take the flow of the fact before
--     them, so they never split a segment.
--   * A segment is a stretch of consecutive facts in one flow (or in none). It also ends
--     at a ConfirmationResolved: the first user message after one opens the next segment,
--     which is how two transfers in a row are told apart.
--   * The outcome of a segment comes from its payment facts. The decision of its last
--     ConfirmationResolved (approved, rejected or failed); abandoned, when a card was
--     offered, never resolved and another segment followed; expired, when it was the
--     last segment and ten minutes have passed (the approval's lifetime); open, while it
--     has not; none, when the segment offered no card.
--
-- The view reads two fields from the payload the inbox stored, which is never
-- upcast: confirmation_ref of any event that carries one, and decision of
-- ConfirmationResolved. A new payload version of those events needs a CASE on
-- event_version here. The agent's history_view_contract_test.go lists the versions
-- this view reads and fails when an event moves past them.
--
-- Every step is a window over (conversation_id, ...), never a correlated lookup,
-- so a filter on conversation_id reaches the inbox's primary key and one
-- conversation reads only its own rows. The first step is NOT MATERIALIZED for the
-- same reason: it is read twice, and a materialized copy would be built whole.
CREATE OR REPLACE VIEW history.conversation_flow_segments AS
WITH events AS NOT MATERIALIZED (
    SELECT conversation_id, sequence, event_name, flow, occurred_at,
           payload ->> 'confirmation_ref' AS ref,
           payload ->> 'decision'         AS decision,
           CASE
               WHEN event_name = 'UserMessageSent' THEN 'user'
               WHEN event_name IN ('ConversationStarted', 'ConversationClosed', 'UserMessageBlocked',
                                   'GuardrailResponseSent', 'AgentResponseFailed') THEN 'conversation'
               ELSE 'fact'
           END AS kind
    FROM history.conversation_inbox
),
-- Counting the facts from each row to the end of the conversation gives a user
-- message and the fact that handled it the same number, so the pair shares a group.
grouped AS (
    SELECT *,
           sum((kind = 'fact')::int) OVER (PARTITION BY conversation_id ORDER BY sequence DESC) AS facts_from_here
    FROM events
    WHERE kind <> 'conversation'
),
handled AS (
    SELECT conversation_id, sequence, event_name, kind, ref, decision, occurred_at,
           CASE
               WHEN kind = 'user' AND bool_or(kind = 'fact') OVER pair
                   THEN (array_agg(flow) FILTER (WHERE kind = 'fact') OVER pair)[1]
               ELSE flow
           END AS handled_flow,
           true AS is_handled
    FROM grouped
    WINDOW pair AS (PARTITION BY conversation_id, facts_from_here)
),
-- A fact of the conversation itself carries the flow of the handled fact before it:
-- the number of handled facts so far names the segment that fact starts.
merged AS (
    SELECT conversation_id, sequence, event_name, kind, ref, decision, occurred_at, handled_flow, is_handled
    FROM handled
    UNION ALL
    SELECT conversation_id, sequence, event_name, kind, ref, decision, occurred_at, NULL, false
    FROM events
    WHERE kind = 'conversation'
),
placed AS (
    SELECT conversation_id, sequence, event_name, kind, ref, decision, occurred_at,
           CASE
               WHEN is_handled THEN handled_flow
               ELSE max(handled_flow) FILTER (WHERE is_handled)
                    OVER (PARTITION BY conversation_id, handled_so_far)
           END AS segment_flow
    FROM (
        SELECT *, sum(is_handled::int) OVER (PARTITION BY conversation_id ORDER BY sequence) AS handled_so_far
        FROM merged
    ) m
),
counted AS (
    SELECT *,
           segment_flow IS DISTINCT FROM lag(segment_flow) OVER w
               OR row_number() OVER w = 1 AS flow_changed,
           count(*) FILTER (WHERE event_name = 'ConfirmationResolved')
               OVER (PARTITION BY conversation_id ORDER BY sequence ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS resolved_before
    FROM placed
    WINDOW w AS (PARTITION BY conversation_id ORDER BY sequence)
),
marked AS (
    SELECT *,
           flow_changed
           OR (kind = 'user' AND resolved_before > coalesce(
                  max(CASE WHEN kind = 'user' THEN resolved_before END)
                      OVER (PARTITION BY conversation_id ORDER BY sequence ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)) AS starts
    FROM counted
),
numbered AS (
    SELECT *, sum(starts::int) OVER (PARTITION BY conversation_id ORDER BY sequence) AS segment
    FROM marked
),
segments AS (
    SELECT conversation_id, segment, segment_flow AS flow,
           min(sequence) AS from_sequence, max(sequence) AS to_sequence, count(*) AS events,
           min(occurred_at) AS started_at, max(occurred_at) AS ended_at,
           (array_agg(ref ORDER BY sequence) FILTER (WHERE ref IS NOT NULL))[1] AS confirmation_ref,
           (array_agg(decision ORDER BY sequence DESC) FILTER (WHERE event_name = 'ConfirmationResolved'))[1] AS decision,
           bool_or(event_name = 'ConfirmationRequested') AS offered_a_card
    FROM numbered
    GROUP BY conversation_id, segment, segment_flow
)
SELECT conversation_id, segment::int AS segment, flow, from_sequence, to_sequence, events::int AS events,
       started_at, ended_at, confirmation_ref,
       CASE
           WHEN decision IS NOT NULL THEN decision
           WHEN NOT offered_a_card THEN 'none'
           WHEN segment < max(segment) OVER (PARTITION BY conversation_id) THEN 'abandoned'
           WHEN ended_at < now() - interval '10 minutes' THEN 'expired'
           ELSE 'open'
       END AS outcome
FROM segments;

COMMENT ON VIEW history.conversation_flow_segments IS
    'A conversation cut into segments of a flow, one row per segment, read from conversation_inbox. A user message belongs to the flow that handled it, the conversation''s own facts never split a segment, and a segment ends at its resolution. See the comment at the top of the migration for the rules.';
COMMENT ON COLUMN history.conversation_flow_segments.conversation_id IS 'The conversation.';
COMMENT ON COLUMN history.conversation_flow_segments.segment IS 'Position of the segment in the conversation, from 1.';
COMMENT ON COLUMN history.conversation_flow_segments.flow IS 'The flow of the segment, such as p2p, or NULL for a segment outside any flow.';
COMMENT ON COLUMN history.conversation_flow_segments.from_sequence IS 'Sequence of the first fact of the segment.';
COMMENT ON COLUMN history.conversation_flow_segments.to_sequence IS 'Sequence of the last fact of the segment.';
COMMENT ON COLUMN history.conversation_flow_segments.events IS 'How many facts the segment holds.';
COMMENT ON COLUMN history.conversation_flow_segments.started_at IS 'When the first fact of the segment happened.';
COMMENT ON COLUMN history.conversation_flow_segments.ended_at IS 'When the last fact of the segment happened.';
COMMENT ON COLUMN history.conversation_flow_segments.confirmation_ref IS 'Reference of the card the segment offered, from its payment facts, or NULL.';
COMMENT ON COLUMN history.conversation_flow_segments.outcome IS 'approved, rejected or failed from the resolution; abandoned, expired or open for a card never resolved; none for a segment that offered no card.';

-- ==== 000004_create_conversation_summaries.up.sql ====
-- Conversation history, part 4: one row per conversation of an account, for the conversation list.
-- Every name is qualified with the schema history (created by 000001). Every statement is repeatable, so running it over an existing table is safe.
--
-- The list of an account's conversations needs, for each one, how many messages it has and when its
-- first and last message were. Grouping conversation_messages answers that, but it reads every
-- message the account ever sent on every page, so a page costs what the account's history costs.
-- This table keeps the answer, so a page reads only the rows it returns.
--
-- The consumer keeps it, in the same transaction that writes a message: a message that is new adds
-- one to the count and widens the first and last times, and nothing else touches it. Those updates
-- commute, so events that arrive in any order, or two writers at once, end with the same row.
-- `consumer rebuild` empties it with the messages and fills it again as it replays them.
-- A message with no account belongs to no account's list and is not counted.
CREATE TABLE IF NOT EXISTS history.conversation_summaries (
    id_yape_account   TEXT         NOT NULL,
    conversation_id   VARCHAR(128) NOT NULL,
    message_count     INT          NOT NULL CHECK (message_count > 0),
    first_occurred_at TIMESTAMPTZ  NOT NULL,
    last_occurred_at  TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (id_yape_account, conversation_id),
    CHECK (first_occurred_at <= last_occurred_at)
);

-- The order of the list: most recent first, the conversation id (larger first) breaking a tie. Both
-- columns run the same way on purpose: the cursor is the pair (last_occurred_at, conversation_id), and
-- Postgres turns a comparison of two such pairs into the boundary of an index scan only when the
-- columns run in the same direction. A page then starts at its cursor and stops after the rows it returns.
CREATE INDEX IF NOT EXISTS conversation_summaries_list_idx
    ON history.conversation_summaries (id_yape_account, last_occurred_at DESC, conversation_id DESC);

COMMENT ON TABLE  history.conversation_summaries IS
    'One row per conversation of an account: its message count and the times of its first and last message. The conversation list reads it. Kept by the consumer with conversation_messages.';
COMMENT ON COLUMN history.conversation_summaries.id_yape_account IS
    'The customer account the conversation belongs to. With conversation_id, the primary key.';
COMMENT ON COLUMN history.conversation_summaries.conversation_id IS
    'The conversation (the A2A contextId).';
COMMENT ON COLUMN history.conversation_summaries.message_count IS
    'How many chat messages of this account the conversation has in conversation_messages.';
COMMENT ON COLUMN history.conversation_summaries.first_occurred_at IS
    'occurred_at of the earliest of those messages.';
COMMENT ON COLUMN history.conversation_summaries.last_occurred_at IS
    'occurred_at of the latest of those messages. Orders the conversation list and is the time part of its cursor.';

-- Fill it from the messages already stored. Running it again rewrites the same values.
INSERT INTO history.conversation_summaries (id_yape_account, conversation_id, message_count, first_occurred_at, last_occurred_at)
SELECT id_yape_account, conversation_id, count(*), min(occurred_at), max(occurred_at)
FROM history.conversation_messages
WHERE id_yape_account IS NOT NULL
GROUP BY id_yape_account, conversation_id
ON CONFLICT (id_yape_account, conversation_id) DO UPDATE SET
    message_count     = EXCLUDED.message_count,
    first_occurred_at = EXCLUDED.first_occurred_at,
    last_occurred_at  = EXCLUDED.last_occurred_at;
