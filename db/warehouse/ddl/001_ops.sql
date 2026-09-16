-- ops schema: platform bookkeeping. See SPEC.md §4.4, §9.4.
CREATE SCHEMA IF NOT EXISTS ops;

-- The load ledger is the SOURCE OF TRUTH for what has been consumed.
-- A ledger row is written in the SAME transaction as the data it describes, so a row being
-- visible here means that batch is fully loaded. Kafka's own committed offsets are advanced
-- afterwards and are used only for lag monitoring.
CREATE TABLE IF NOT EXISTS ops.load_ledger (
    id            bigserial   PRIMARY KEY,
    dag_run_id    text        NOT NULL,
    topic         text        NOT NULL,
    partition_id  smallint    NOT NULL,
    start_offset  bigint      NOT NULL,   -- inclusive
    end_offset    bigint      NOT NULL,   -- exclusive
    loaded_date   date        NOT NULL,   -- the single raw partition this entry wrote into
    row_count     integer     NOT NULL DEFAULT 0,
    dlq_count     integer     NOT NULL DEFAULT 0,
    erased_count  integer     NOT NULL DEFAULT 0,
    attempt       integer     NOT NULL DEFAULT 1,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT load_ledger_range_valid CHECK (end_offset >= start_offset),
    -- One entry per (run, topic, partition): this is what makes a rerun find and REPLACE
    -- its previous attempt instead of appending a second copy.
    CONSTRAINT load_ledger_run_unique UNIQUE (dag_run_id, topic, partition_id)
);

CREATE INDEX IF NOT EXISTS load_ledger_watermark_idx
    ON ops.load_ledger (topic, partition_id, end_offset DESC);

-- Messages that could not be parsed. Kept out of raw so raw stays typed and queryable,
-- but retained with the original bytes so a fix can be verified before a rerun.
CREATE TABLE IF NOT EXISTS ops.load_dlq (
    id           bigserial   PRIMARY KEY,
    ledger_id    bigint      NOT NULL REFERENCES ops.load_ledger(id) ON DELETE CASCADE,
    topic        text        NOT NULL,
    partition_id smallint    NOT NULL,
    kafka_offset bigint      NOT NULL,
    reason       text        NOT NULL,
    raw_value    text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS load_dlq_ledger_idx ON ops.load_dlq (ledger_id);

-- Erasure requests hold IDs ONLY: no PII, so the table itself never needs erasing.
-- Every load path (live, rerun, backfill) anti-joins against this, which is what stops a
-- rerun re-reading erased events from a topic whose retention has not yet expired.
CREATE TABLE IF NOT EXISTS ops.erasure_requests (
    account_id   bigint      PRIMARY KEY,
    requested_at timestamptz NOT NULL DEFAULT now(),
    purged_at    timestamptz
);
