-- CDC configuration on the source database (SPEC.md §4.2).

-- REPLICA IDENTITY FULL puts the whole pre-change row into the WAL, so Debezium's `before`
-- image is complete. The default (USING INDEX on the primary key) would give only the key,
-- which means:
--   * an UPDATE cannot be diffed (what changed is unknowable), and
--   * a DELETE arrives with no values at all, so SCD2 cannot close the interval with what was
--     actually live at the time.
-- The cost is more WAL per update. That is accepted here and stated in ADR 0002, because the
-- change log IS the history in this design: it is not re-derivable later.
ALTER TABLE accounts        REPLICA IDENTITY FULL;
ALTER TABLE users           REPLICA IDENTITY FULL;
ALTER TABLE plans           REPLICA IDENTITY FULL;
ALTER TABLE subscriptions   REPLICA IDENTITY FULL;
ALTER TABLE invoices        REPLICA IDENTITY FULL;
ALTER TABLE support_tickets REPLICA IDENTITY FULL;

-- debezium_heartbeat is deliberately NOT set to FULL: it exists only so Debezium's periodic
-- write produces WAL on a published table, so the slot advances on an idle database. It holds
-- one row of no business meaning, and its before-image is never read.

-- The publication lists tables EXPLICITLY. `FOR ALL TABLES` would silently capture
-- account_insights (closing the reverse-ETL feedback loop) and would enrol every future table
-- without anyone deciding to.
DROP PUBLICATION IF EXISTS analytics_cdc;
CREATE PUBLICATION analytics_cdc FOR TABLE
    accounts,
    users,
    plans,
    subscriptions,
    invoices,
    support_tickets,
    debezium_heartbeat;

-- Server settings applied by the container command / VM config rather than here, recorded for
-- reference:
--   wal_level = logical
--   max_slot_wal_keep_size = 5GB   -- the cap that protects the shared disk (SPEC.md §9.1)
--   max_replication_slots = 4
--   max_wal_senders = 4
