-- Aurora DSQL compatible: no foreign keys, no sequences, no json column types,
-- one DDL statement per transaction, secondary indexes built with CREATE INDEX ASYNC.
-- SchemaInitializer replaces /*ASYNC*/ with ASYNC on DSQL and drops it on Postgres.

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
    data          text,
    version       bigint NOT NULL,
    last_event_id text,
    updated_at    timestamptz NOT NULL
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

CREATE INDEX /*ASYNC*/ IF NOT EXISTS processed_event_received_idx ON processed_event (received_at);
