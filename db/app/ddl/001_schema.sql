-- The synthetic SaaS product's OLTP database (SPEC.md §3.1).
--
-- Every table carries `effective_at`: the business time of the change, as distinct from the
-- wall-clock time it was written. During the historical replay a year of changes is written
-- in minutes, so wall-clock time is useless for history and SCD2 keys off this column.

CREATE TABLE IF NOT EXISTS plans (
    id           bigserial   PRIMARY KEY,
    code         text        NOT NULL UNIQUE,
    name         text        NOT NULL,
    monthly_price_cents integer NOT NULL,
    seat_price_cents    integer NOT NULL,
    effective_at timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    id           bigserial   PRIMARY KEY,
    company_name text        NOT NULL,
    country      text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    effective_at timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    id           bigserial   PRIMARY KEY,
    account_id   bigint      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    email        text        NOT NULL,
    role         text        NOT NULL DEFAULT 'member',
    last_login_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    effective_at timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id           bigserial   PRIMARY KEY,
    account_id   bigint      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    plan_code    text        NOT NULL REFERENCES plans(code),
    status       text        NOT NULL,  -- trial | active | past_due | cancelled
    seats        integer     NOT NULL,
    mrr_cents    integer     NOT NULL,
    started_at   timestamptz NOT NULL,
    ended_at     timestamptz,
    effective_at timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT subscription_status_valid
        CHECK (status IN ('trial', 'active', 'past_due', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS invoices (
    id           bigserial   PRIMARY KEY,
    account_id   bigint      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    provider_invoice_id text NOT NULL UNIQUE,
    amount_cents integer     NOT NULL,
    status       text        NOT NULL,  -- open | paid | payment_failed | void
    issued_at    timestamptz NOT NULL,
    paid_at      timestamptz,
    effective_at timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS support_tickets (
    id           bigserial   PRIMARY KEY,
    account_id   bigint      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    severity     text        NOT NULL,
    opened_at    timestamptz NOT NULL,
    closed_at    timestamptz,
    effective_at timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Written by reverse ETL (SPEC.md §8), and deliberately NOT part of the publication below.
-- If it were captured, every churn score the warehouse wrote would flow back into the
-- warehouse, feed the model that produced it, and close a feedback loop.
CREATE TABLE IF NOT EXISTS account_insights (
    account_id   bigint      PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    churn_score  integer     NOT NULL,
    churn_band   text        NOT NULL,
    score_version text       NOT NULL,
    reasons      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    synced_at    timestamptz NOT NULL DEFAULT now()
);

-- Debezium writes here to make the slot's confirmed LSN advance on an idle database. Without
-- heartbeats a low-traffic source can hold WAL open indefinitely: the slot only advances when
-- it sees changes, and if none of the CAPTURED tables change, it never does (SPEC.md §4.2).
CREATE TABLE IF NOT EXISTS debezium_heartbeat (
    id         integer     PRIMARY KEY DEFAULT 1,
    beat_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);
INSERT INTO debezium_heartbeat (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Debezium's signal table: an incremental snapshot is requested by inserting a row here.
-- Automated slot recovery uses this (SPEC.md §9.1).
CREATE TABLE IF NOT EXISTS debezium_signal (
    id    text  PRIMARY KEY,
    type  text  NOT NULL,
    data  text
);
