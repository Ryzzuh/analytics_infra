-- raw schema: loader-owned, append-only, never modelled by dbt. See SPEC.md §5.2.
CREATE SCHEMA IF NOT EXISTS raw;

-- Partitioned by LOAD date, not event date:
--   * a rerun-replace deletes from exactly one partition (ledger entries never span days)
--   * retention drops a partition instead of deleting rows
-- Lookback scans on event_time are served by the BRIN index, which works because event_time
-- correlates with load order on the live path. The backfill loader sorts by event_time before
-- COPY to preserve that correlation in the one partition it writes (SPEC.md §5.2).
CREATE TABLE IF NOT EXISTS raw.product_events (
    loaded_date  date        NOT NULL,
    topic        text        NOT NULL,
    partition_id smallint    NOT NULL,
    kafka_offset bigint      NOT NULL,
    ledger_id    bigint      NOT NULL REFERENCES ops.load_ledger(id),
    event_id     uuid        NOT NULL,
    account_id   bigint,
    user_id      bigint,
    event_type   text        NOT NULL,
    event_time   timestamptz NOT NULL,   -- client-asserted
    received_at  timestamptz NOT NULL,   -- collector
    kafka_ts     timestamptz NOT NULL,
    loaded_at    timestamptz NOT NULL,
    source_path  text        NOT NULL,   -- 'live' | 'backfill'
    payload      jsonb       NOT NULL,
    PRIMARY KEY (loaded_date, topic, partition_id, kafka_offset)
) PARTITION BY RANGE (loaded_date);

-- account_id is extracted to a real column so an erasure purge is an indexed delete rather
-- than a JSONB scan. Partitioning does NOT help erasure: it spans every day the account was
-- active, whatever the partition key.
CREATE INDEX IF NOT EXISTS product_events_account_idx ON raw.product_events (account_id);
CREATE INDEX IF NOT EXISTS product_events_event_time_brin ON raw.product_events USING brin (event_time);
CREATE INDEX IF NOT EXISTS product_events_ledger_idx ON raw.product_events (ledger_id);
