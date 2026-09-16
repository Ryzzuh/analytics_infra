-- Observability state (SPEC.md §10).
--
-- The platform's own health lives in the warehouse, not only in Prometheus: Prometheus keeps
-- 15 days of samples and answers "is it broken now", while these tables answer "when did this
-- start, and what did it look like" long after the scrape window has rolled off.

-- Results of every data-quality check run. One row per check per run, kept as history so a
-- rising duplicate rate is visible as a trend rather than a single current value.
CREATE TABLE IF NOT EXISTS ops.dq_results (
    id           bigserial   PRIMARY KEY,
    checked_at   timestamptz NOT NULL DEFAULT now(),
    check_name   text        NOT NULL,
    target       text        NOT NULL,   -- the table, model or stream checked
    status       text        NOT NULL,   -- pass | warn | fail
    observed     numeric,
    threshold    numeric,
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT dq_status_valid CHECK (status IN ('pass', 'warn', 'fail'))
);

CREATE INDEX IF NOT EXISTS dq_results_recent_idx
    ON ops.dq_results (check_name, target, checked_at DESC);

-- Open chaos windows (SPEC.md §10.2).
--
-- Alerting reads this: an alert that fires with no window open is a genuine incident, and one
-- that fires during a window is a visitor exercising the demo. Without the distinction, either
-- the pager becomes noise or the chaos scenarios have to be kept quiet enough to be pointless.
CREATE TABLE IF NOT EXISTS ops.chaos_windows (
    id          bigserial   PRIMARY KEY,
    scenario    text        NOT NULL,
    opened_at   timestamptz NOT NULL DEFAULT now(),
    closed_at   timestamptz,
    opened_by   text        NOT NULL DEFAULT 'console',
    notes       text
);

CREATE INDEX IF NOT EXISTS chaos_windows_open_idx ON ops.chaos_windows (closed_at)
    WHERE closed_at IS NULL;

-- Freshness targets per layer, as data rather than as thresholds buried in alert rules, so the
-- Console, the checks and Prometheus all read the same numbers (SPEC.md §10.1).
CREATE TABLE IF NOT EXISTS ops.freshness_slo (
    layer          text     PRIMARY KEY,
    target_seconds integer  NOT NULL,
    description    text
);

INSERT INTO ops.freshness_slo (layer, target_seconds, description) VALUES
    ('raw_product_events', 600,   'p95 behind event time'),
    ('raw_cdc',            300,   'p95 behind commit'),
    ('raw_billing',        1800,  'webhook path'),
    ('staging',            3600,  'typed and deduplicated'),
    ('marts',              86400, 'daily build'),
    ('reverse_etl',        86400, 'delivered to the product')
ON CONFLICT (layer) DO UPDATE
    SET target_seconds = excluded.target_seconds,
        description = excluded.description;

-- Schema-drift findings (SPEC.md §7). Written by the drift detector; read by the exporter and
-- the Console. Created here so the alert that depends on it exists before the detector does,
-- rather than the alert silently referring to nothing.
CREATE TABLE IF NOT EXISTS ops.drift_findings (
    id           bigserial   PRIMARY KEY,
    detected_at  timestamptz NOT NULL DEFAULT now(),
    event_type   text        NOT NULL,
    json_path    text        NOT NULL,
    change       text        NOT NULL,   -- field_added | field_removed | type_changed
    blocking     boolean     NOT NULL,   -- does a model declare a dependency on this field?
    details      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    resolved_at  timestamptz
);

CREATE INDEX IF NOT EXISTS drift_findings_open_idx ON ops.drift_findings (resolved_at)
    WHERE resolved_at IS NULL;

-- Observed shape of each event type (SPEC.md §7).
--
-- Product events are schemaless at ingest on purpose: a new payload field must never cost a
-- client a 4xx. The cost of that choice is that nothing at the edge notices when a field
-- disappears, so the noticing happens here instead — by recording what the data actually looks
-- like and comparing it with what it looked like before.
CREATE TABLE IF NOT EXISTS ops.event_shape (
    event_type   text        NOT NULL,
    json_path    text        NOT NULL,
    json_type    text        NOT NULL,   -- string | number | boolean | object | array | null
    first_seen   timestamptz NOT NULL DEFAULT now(),
    last_seen    timestamptz NOT NULL DEFAULT now(),
    occurrences  bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (event_type, json_path, json_type)
);

CREATE INDEX IF NOT EXISTS event_shape_recent_idx ON ops.event_shape (event_type, last_seen DESC);
