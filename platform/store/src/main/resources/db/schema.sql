-- Aurora DSQL compatible: no foreign keys, no sequences, no json column types,
-- one DDL statement per transaction, secondary indexes built with CREATE INDEX ASYNC.
-- SchemaInitializer replaces /*ASYNC*/ with ASYNC on DSQL and drops it on Postgres, and /*JSON*/ with
-- jsonb on Postgres or text on DSQL (DSQL has no jsonb column type; JSON is still castable at query time).

CREATE TABLE IF NOT EXISTS processed_event (
    event_id    text PRIMARY KEY,
    received_at timestamptz NOT NULL,
    status      text NOT NULL            -- RECEIVED | DISPATCHED
);

CREATE TABLE IF NOT EXISTS shard_checkpoint (
    stream     text NOT NULL,
    shard_id   text NOT NULL,
    seq_no     text NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (stream, shard_id)
);

CREATE TABLE IF NOT EXISTS sm_state (
    workflow_id   text PRIMARY KEY,      -- smType:instanceKey
    sm_type       text NOT NULL,
    state         text NOT NULL,
    data          /*JSON*/,
    version       bigint NOT NULL,
    last_event_id text,
    updated_at    timestamptz NOT NULL
);

-- DB-truth: every event applied to an entity (accepted or rejected) with its result, written in the transition's
-- transaction. A redelivered event finds its row and gets the stored result instead of being applied again.
CREATE TABLE IF NOT EXISTS applied_event (
    workflow_id text NOT NULL,
    event_id    text NOT NULL,
    accepted    boolean NOT NULL,
    from_state  text NOT NULL,
    to_state    text NOT NULL,
    version     bigint NOT NULL,
    message     text,
    applied_at  timestamptz NOT NULL,
    PRIMARY KEY (workflow_id, event_id)
);

-- DB-truth: events an accepted transition emitted, written in its transaction, delivered to their lock chains after
-- commit (sent_at set) by the worker, or by its sweeper if the worker died first.
CREATE TABLE IF NOT EXISTS outbox (
    event_id    text PRIMARY KEY,       -- the child event id, <parentEventId>.<n>
    workflow_id text NOT NULL,          -- the emitting entity
    envelope    text NOT NULL,          -- the EventEnvelope JSON
    created_at  timestamptz NOT NULL,
    sent_at     timestamptz
);

CREATE TABLE IF NOT EXISTS event_trace (
    trace_id    text PRIMARY KEY,        -- client-generated UUID
    event_id    text NOT NULL,
    ts          timestamptz NOT NULL,
    stage       text NOT NULL,
    workflow_id text,
    lock_key    text,
    detail      text
);

CREATE INDEX /*ASYNC*/ IF NOT EXISTS event_trace_event_idx ON event_trace (event_id);

CREATE INDEX /*ASYNC*/ IF NOT EXISTS event_trace_workflow_idx ON event_trace (workflow_id);

-- Recent-window reads (dashboard flow metrics, recent events).
CREATE INDEX /*ASYNC*/ IF NOT EXISTS event_trace_ts_idx ON event_trace (ts);

CREATE INDEX /*ASYNC*/ IF NOT EXISTS processed_event_received_idx ON processed_event (received_at);

CREATE INDEX /*ASYNC*/ IF NOT EXISTS applied_event_applied_idx ON applied_event (applied_at);

CREATE INDEX /*ASYNC*/ IF NOT EXISTS outbox_created_idx ON outbox (created_at);
