-- Billing ingestion (SPEC.md §4.3) and reverse-ETL state (SPEC.md §8).

-- Fast path: webhooks, delivered through the broker like any other stream. Partitioned and
-- ledger-tracked exactly as product events are, so replay and rerun-replace work identically.
CREATE TABLE IF NOT EXISTS raw.billing_webhook_events (
    loaded_date         date        NOT NULL,
    topic               text        NOT NULL,
    partition_id        smallint    NOT NULL,
    kafka_offset        bigint      NOT NULL,
    ledger_id           bigint      NOT NULL REFERENCES ops.load_ledger(id),
    provider_event_id   text        NOT NULL,
    event_type          text        NOT NULL,
    account_id          bigint,
    provider_created_at timestamptz NOT NULL,
    received_at         timestamptz NOT NULL,
    kafka_ts            timestamptz NOT NULL,
    loaded_at           timestamptz NOT NULL,
    source_path         text        NOT NULL,
    payload             jsonb       NOT NULL,
    PRIMARY KEY (loaded_date, topic, partition_id, kafka_offset)
) PARTITION BY RANGE (loaded_date);

CREATE INDEX IF NOT EXISTS billing_webhook_provider_idx
    ON raw.billing_webhook_events (provider_event_id);
CREATE INDEX IF NOT EXISTS billing_webhook_account_idx
    ON raw.billing_webhook_events (account_id);
CREATE INDEX IF NOT EXISTS billing_webhook_ledger_idx
    ON raw.billing_webhook_events (ledger_id);

-- Slow path: the provider's list API, pulled daily with an overlap window.
--
-- Not partitioned, and keyed on the provider's own event id: the overlap means the same events
-- are fetched again on purpose, so the insert has to be idempotent. Without that the overlap
-- would multiply rows instead of catching gaps.
CREATE TABLE IF NOT EXISTS raw.billing_events_batch (
    provider_event_id   text        PRIMARY KEY,
    event_type          text        NOT NULL,
    account_id          bigint,
    provider_created_at timestamptz NOT NULL,
    pulled_at           timestamptz NOT NULL,
    payload             jsonb       NOT NULL
);

CREATE INDEX IF NOT EXISTS billing_batch_created_idx
    ON raw.billing_events_batch (provider_created_at);

-- Where the daily pull got to. Updated in the same transaction as the rows it describes, for
-- the same reason the load ledger is (SPEC.md §4.4).
CREATE TABLE IF NOT EXISTS ops.billing_pull_state (
    id                integer     PRIMARY KEY DEFAULT 1,
    last_created_at   timestamptz,
    last_event_id     text,
    last_pull_at      timestamptz,
    pages_last_pull   integer     NOT NULL DEFAULT 0,
    CONSTRAINT single_row CHECK (id = 1)
);
INSERT INTO ops.billing_pull_state (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Per-account reverse-ETL state (SPEC.md §8).
--
-- `payload_hash` is what makes the sync diff-based: only accounts whose payload differs from
-- the last successful send are sent again. Full-refreshing every account every run would burn
-- the destination's rate limit to say nothing.
CREATE TABLE IF NOT EXISTS ops.reverse_etl_sync_state (
    account_id        bigint      PRIMARY KEY,
    payload_hash      text        NOT NULL,
    score_version     text        NOT NULL,
    status            text        NOT NULL,   -- synced | failed | skipped_client_error
    attempts          integer     NOT NULL DEFAULT 0,
    last_attempt_at   timestamptz,
    last_success_at   timestamptz,
    last_status_code  integer,
    last_error        text,
    CONSTRAINT sync_status_valid
        CHECK (status IN ('synced', 'failed', 'skipped_client_error'))
);

CREATE INDEX IF NOT EXISTS reverse_etl_status_idx ON ops.reverse_etl_sync_state (status);
