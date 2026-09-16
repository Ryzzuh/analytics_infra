"""The OLTP half of the simulator, as a long-running service (SPEC.md §3.2).

`simulator.oltp.OltpWriter` has always existed and has always been tested, but nothing in the
running system ever called it: no console script, no compose service, no DAG task. The source
database therefore stayed empty, which left the entire CDC half of the platform — Debezium, the
change log, SCD2, `dim_subscription` — connected to a table nothing ever wrote to. Green tests
over a component the deployed system never invokes.

This module is the missing caller. It keeps a population of accounts alive and mutating, which
is what makes the CDC path carry anything at all.

Why a separate process from the event simulator: the two halves fail independently and are
usefully restartable on their own. The event simulator talks HTTP to the collector and needs no
database; this one talks to the source database and needs no collector. Wiring them into one
process would mean a database outage stops event ingestion, which is exactly the coupling the
architecture is meant to avoid.

The write mix is deliberate, and each part exists to exercise something downstream:

* **status transitions** produce the SCD2 versions `dim_subscription` is built from;
* **no-op updates** (an `active` row that stays `active`) are captured by CDC and must *not*
  become new SCD2 versions — the most common way a naive SCD2 implementation is wrong;
* **seat changes** move `mrr_cents` without changing status, so a version boundary depends on
  which columns are modelled rather than on the row having been touched;
* **hard deletes** are what a GDPR erasure looks like on the wire: a delete with a full before
  image plus a tombstone, which is why the source needs REPLICA IDENTITY FULL;
* **heartbeat writes** keep the replication slot's confirmed LSN moving while the captured
  tables are idle, so WAL does not accumulate on a quiet Sunday (SPEC.md §4.2).
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import time
from datetime import UTC, datetime, timedelta
from types import FrameType

import psycopg
from simulator.oltp import OltpWriter, replay_history

log = logging.getLogger("simulator.oltp")

_stop = False


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    """Stop at the end of the current tick rather than mid-transaction.

    Every write in OltpWriter commits on its own, so a hard kill cannot corrupt anything — but
    exiting cleanly keeps `docker stop` from taking the full ten-second SIGKILL timeout.
    """
    global _stop
    log.info("signal %s received; stopping after this tick", signum)
    _stop = True


def _live_subscriptions(conn: psycopg.Connection, limit: int) -> list[int]:
    """Subscription ids worth touching, newest first.

    Cancelled subscriptions are terminal in the transition table, so sampling them would burn
    the tick on rows that can never change again.
    """
    rows = conn.execute(
        "SELECT id FROM subscriptions WHERE status <> 'cancelled' ORDER BY id DESC LIMIT %s",
        (limit,),
    ).fetchall()
    return [r[0] for r in rows]


def _account_count(conn: psycopg.Connection) -> int:
    return int(conn.execute("SELECT count(*) FROM accounts").fetchone()[0])


def run(
    *,
    dsn: str,
    accounts: int,
    interval: float,
    transitions_per_tick: int,
    seat_change_rate: float,
    erasure_rate: float,
    heartbeat_seconds: float,
    history_days: int,
    seed: int,
    duration: float | None = None,
) -> dict[str, int]:
    rng = random.Random(seed)
    conn = psycopg.connect(dsn, autocommit=False)
    writer = OltpWriter(conn=conn, rng=rng)
    writer.ensure_plans()

    stats = {"signups": 0, "transitions": 0, "seat_changes": 0, "erasures": 0, "beats": 0}

    existing = _account_count(conn)
    if history_days > 0 and existing == 0:
        # Only ever on a genuinely empty database. Replaying history into a populated one would
        # date new rows in the past and interleave them with current state, which is precisely
        # the corruption SCD2's business-time keying is meant to avoid.
        end = datetime.now(UTC)
        start = end - timedelta(days=history_days)
        log.info("replaying %d days of history for %d accounts", history_days, accounts)
        replayed = replay_history(writer, accounts=accounts, start=start, end=end)
        stats["signups"] += replayed["accounts"]
        stats["transitions"] += replayed["transitions"]
        stats["seat_changes"] += replayed["seat_changes"]
        log.info("history replay complete: %s", replayed)
    elif existing < accounts:
        # Seed the population in one pass so CDC has something to carry immediately, rather
        # than waiting for the drip of one signup per tick to reach a useful size.
        log.info("seeding %d accounts (found %d)", accounts - existing, existing)
        now = datetime.now(UTC)
        for _ in range(accounts - existing):
            account_id = writer.create_account(
                effective_at=now, company=f"Account {existing + stats['signups'] + 1}"
            )
            writer.start_trial(
                account_id,
                effective_at=now,
                plan_code=rng.choices(
                    ["free", "team", "business", "enterprise"], weights=[50, 30, 15, 5]
                )[0],
            )
            stats["signups"] += 1

    log.info(
        "steady state: %d accounts, %.1fs tick, %d transitions/tick",
        _account_count(conn),
        interval,
        transitions_per_tick,
    )

    started = time.monotonic()
    last_beat = 0.0
    while not _stop:
        if duration is not None and time.monotonic() - started >= duration:
            break

        now = datetime.now(UTC)
        live = _live_subscriptions(conn, limit=max(transitions_per_tick * 20, 100))

        for subscription_id in rng.sample(live, min(transitions_per_tick, len(live))):
            # advance() returns None for a no-op update. Those are not failures — they are the
            # captured-but-not-modelled writes that SCD2 has to ignore, so they are counted
            # separately from real transitions rather than retried.
            if writer.advance(subscription_id, effective_at=now) is not None:
                stats["transitions"] += 1
            if rng.random() < seat_change_rate:
                writer.change_seats(
                    subscription_id, effective_at=now, delta=rng.choice([-2, 1, 2, 5])
                )
                stats["seat_changes"] += 1

        # Signups replace erased accounts and keep the population near target, so a long run
        # does not slowly empty the source.
        if _account_count(conn) < accounts:
            account_id = writer.create_account(
                effective_at=now, company=f"Account {rng.randrange(10**6, 10**7)}"
            )
            writer.start_trial(account_id, effective_at=now)
            stats["signups"] += 1

        if rng.random() < erasure_rate:
            row = conn.execute("SELECT id FROM accounts ORDER BY random() LIMIT 1").fetchone()
            if row is not None:
                writer.delete_account(row[0])
                stats["erasures"] += 1
                log.info("erased account %s (hard delete, cascades)", row[0])

        if time.monotonic() - last_beat >= heartbeat_seconds:
            writer.beat()
            stats["beats"] += 1
            last_beat = time.monotonic()

        time.sleep(interval)

    conn.close()
    log.info("stopped: %s", stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("APP_DSN", "postgresql://app:app@localhost:5434/app"),
        help="source (OLTP) database",
    )
    parser.add_argument("--accounts", type=int, default=int(os.environ.get("SIM_ACCOUNTS", 500)))
    parser.add_argument(
        "--interval", type=float, default=float(os.environ.get("SIM_OLTP_INTERVAL", 5))
    )
    parser.add_argument(
        "--transitions-per-tick",
        type=int,
        default=int(os.environ.get("SIM_OLTP_TRANSITIONS", 3)),
    )
    parser.add_argument(
        "--seat-change-rate",
        type=float,
        default=float(os.environ.get("SIM_OLTP_SEAT_CHANGE_RATE", 0.05)),
    )
    parser.add_argument(
        "--erasure-rate",
        type=float,
        default=float(os.environ.get("SIM_OLTP_ERASURE_RATE", 0.004)),
        help="probability per tick of a hard delete (GDPR erasure)",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=float(os.environ.get("SIM_OLTP_HEARTBEAT", 30)),
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=int(os.environ.get("SIM_OLTP_HISTORY_DAYS", 0)),
        help="on an empty database only: replay this many days of lifecycle first",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--duration", type=float, default=None, help="seconds; default runs forever"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    run(
        dsn=args.dsn,
        accounts=args.accounts,
        interval=args.interval,
        transitions_per_tick=args.transitions_per_tick,
        seat_change_rate=args.seat_change_rate,
        erasure_rate=args.erasure_rate,
        heartbeat_seconds=args.heartbeat_seconds,
        history_days=args.history_days,
        seed=args.seed,
        duration=args.duration,
    )
