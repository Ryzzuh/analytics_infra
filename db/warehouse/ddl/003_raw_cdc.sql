-- CDC change log as received from Debezium. See SPEC.md §4.2, §6.1.
--
-- Deviation from SPEC.md §5.2, which said `raw.cdc_<table>` per captured table: one table
-- with a `source_table` column is used instead. Per-table raw would mean DDL (and a new
-- partition set, and a new ledger stream) every time a table is added to the publication,
-- for volumes that are tiny next to product events — the whole OLTP change log here is a few
-- hundred thousand rows a year. Staging splits it back out per table.
CREATE TABLE IF NOT EXISTS raw.cdc_changes (
    loaded_date  date        NOT NULL,
    topic        text        NOT NULL,
    partition_id smallint    NOT NULL,
    kafka_offset bigint      NOT NULL,
    ledger_id    bigint      NOT NULL REFERENCES ops.load_ledger(id),
    source_table text        NOT NULL,
    op           char(1)     NOT NULL,   -- c=create u=update d=delete r=snapshot read
    pk           text        NOT NULL,
    account_id   bigint,                 -- extracted so erasure is an indexed delete
    before       jsonb,                  -- present for u/d only with REPLICA IDENTITY FULL
    after        jsonb,                  -- null for deletes
    source_lsn   bigint      NOT NULL,   -- the only total order across committed changes
    source_ts    timestamptz NOT NULL,   -- wall-clock commit time
    effective_at timestamptz NOT NULL,   -- business time; drives SCD2 (see loader/cdc.py)
    is_snapshot  boolean     NOT NULL,
    kafka_ts     timestamptz NOT NULL,
    loaded_at    timestamptz NOT NULL,
    source_path  text        NOT NULL,   -- 'live' | 'backfill'
    PRIMARY KEY (loaded_date, topic, partition_id, kafka_offset),
    CONSTRAINT cdc_op_valid CHECK (op IN ('c', 'u', 'd', 'r')),
    -- A delete with no before image means REPLICA IDENTITY was not FULL when it happened,
    -- and SCD2 cannot close the interval correctly. Refuse the row rather than model it.
    CONSTRAINT cdc_delete_has_before CHECK (op <> 'd' OR before IS NOT NULL)
) PARTITION BY RANGE (loaded_date);

-- SCD2 walks one entity's changes in LSN order, so that is the covering index.
CREATE INDEX IF NOT EXISTS cdc_changes_entity_idx
    ON raw.cdc_changes (source_table, pk, source_lsn);
CREATE INDEX IF NOT EXISTS cdc_changes_account_idx ON raw.cdc_changes (account_id);
CREATE INDEX IF NOT EXISTS cdc_changes_effective_brin
    ON raw.cdc_changes USING brin (effective_at);
CREATE INDEX IF NOT EXISTS cdc_changes_ledger_idx ON raw.cdc_changes (ledger_id);

-- Independent daily snapshot of current OLTP state, used to prove the CDC stream did not
-- drop changes (SPEC.md §6.1). Deliberately NOT derived from the change log.
CREATE TABLE IF NOT EXISTS raw.oltp_snapshots (
    snapshot_at  timestamptz NOT NULL,
    source_table text        NOT NULL,
    pk           text        NOT NULL,
    row_data     jsonb       NOT NULL,
    PRIMARY KEY (snapshot_at, source_table, pk)
);

-- Gaps in CDC coverage: written when a slot is invalidated and a re-snapshot is triggered
-- (SPEC.md §9.1). The reconciliation test reads this to explain a mismatch rather than
-- simply failing.
CREATE TABLE IF NOT EXISTS ops.cdc_gaps (
    id           bigserial   PRIMARY KEY,
    detected_at  timestamptz NOT NULL DEFAULT now(),
    reason       text        NOT NULL,
    slot_name    text,
    resnapshot_requested_at timestamptz,
    resolved_at  timestamptz
);
