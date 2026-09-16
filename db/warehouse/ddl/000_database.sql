-- Every session reads and writes in UTC.
--
-- Not cosmetic: timestamps rendered in a local session timezone shift, and a far-future SCD2
-- sentinel rendered east of UTC can roll into year 10000 and overflow client date types. The
-- platform stores timestamptz throughout, so pinning the session zone makes every client —
-- dbt, Airflow, the services, psql — agree on what it reads back.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone = ''UTC''', current_database());
END
$$;
SET timezone = 'UTC';
