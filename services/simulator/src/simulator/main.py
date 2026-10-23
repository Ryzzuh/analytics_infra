"""Synthetic SaaS product-event generator (M1 scope: product events only).

The simulator's job is not volume, it is *realistic imperfection*. Real client fleets produce
duplicates (retries after a timeout the server actually handled) and late events (phones that
were offline), so this generator produces both at configurable rates. Those are the inputs the
platform's dedup and lateness policies exist to handle, and a demo that never produces them
proves nothing.

M2 adds the OLTP writes; M4 adds historical replay via the backfill topics.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import psycopg

log = logging.getLogger("simulator")

SESSION_EVENTS = ["page_view", "feature_invoked", "report_exported", "api_called"]
PLANS = ["free", "team", "business", "enterprise"]
FEATURES = ["export", "dashboard", "share", "api", "invite", "search"]


@dataclass
class Account:
    account_id: int
    plan: str
    seats: int
    users: list[int] = field(default_factory=list)
    # Per-account activity multiplier: a few accounts are much busier than the rest, which is
    # what creates partition skew worth measuring (SPEC.md §17).
    intensity: float = 1.0


def _intensity(account_id: int, seed: int) -> float:
    """Stable per account, so a refresh does not reshuffle which accounts are busy.

    Partition skew is only worth measuring if it persists; an intensity redrawn every refresh
    would average the skew away over a long run.
    """
    return random.Random(f"{seed}:{account_id}").lognormvariate(0, 0.8)


def build_accounts(count: int, seed: int) -> list[Account]:
    """Synthetic accounts numbered 1..count.

    Only for tests and for running the generator with no source database. It invents account ids
    with no reference to what exists, which is exactly the bug `load_accounts` fixes -- events
    landed against ids that had never been created, so the warehouse saw usage predating the
    account and scored brand-new customers on a history that was not theirs.
    """
    rng = random.Random(seed)
    accounts = []
    for i in range(1, count + 1):
        plan = rng.choices(PLANS, weights=[50, 30, 15, 5])[0]
        seats = {"free": 2, "team": 8, "business": 25, "enterprise": 120}[plan]
        accounts.append(
            Account(
                account_id=i,
                plan=plan,
                seats=seats,
                users=[i * 1000 + u for u in range(seats)],
                intensity=_intensity(i, seed),
            )
        )
    return accounts


def load_accounts(conn: psycopg.Connection, seed: int) -> list[Account]:
    """The accounts that actually exist, with the plan and seat count they actually hold.

    The product stream and the OLTP source have to agree on who the customers are. While they
    did not, `events_prior` counted activity for accounts created after those events, and every
    account beyond the generator's hard-coded range received no events at all -- which the
    churn model read as dormancy and scored as critical risk.

    Cancelled accounts stay in: a churned customer's past usage is what the usage-collapse
    signal is computed from. Erased accounts disappear on their own, since the row is gone.
    """
    rows = conn.execute(
        """
        SELECT a.id, coalesce(s.plan_code, 'free'), coalesce(s.seats, 2)
        FROM accounts a
        LEFT JOIN LATERAL (
            SELECT plan_code, seats FROM subscriptions
            WHERE account_id = a.id ORDER BY effective_at DESC LIMIT 1
        ) s ON true
        ORDER BY a.id
        """
    ).fetchall()
    return [
        Account(
            account_id=row[0],
            plan=row[1],
            seats=row[2],
            users=[row[0] * 1000 + u for u in range(max(row[2], 1))],
            intensity=_intensity(row[0], seed),
        )
        for row in rows
    ]


class Emitter:
    def __init__(self, collector_url: str, batch_size: int = 50, timeout: float = 10.0):
        self._client = httpx.Client(base_url=collector_url, timeout=timeout)
        self._buffer: list[dict] = []
        self._batch_size = batch_size
        self.sent = 0
        self.failed = 0

    def emit(self, event: dict) -> None:
        self._buffer.append(event)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        batch, self._buffer = self._buffer, []
        try:
            response = self._client.post("/v1/events", json={"events": batch})
            response.raise_for_status()
            self.sent += len(batch)
        except httpx.HTTPError as exc:
            # A real client retries, which is exactly how duplicates reach the platform.
            self.failed += len(batch)
            log.warning("emit failed (%s); re-queueing %d events", exc, len(batch))
            self._buffer = batch + self._buffer


# The schema-drift chaos scenario (SPEC.md §9.2) "ships a release" by creating this file. A
# flag rather than a config change, because a release is not something the platform is told
# about — it simply starts receiving a different shape, which is the whole point.
RENAME_FLAG = "/tmp/rename_plan_field"  # noqa: S108


def plan_field_name() -> str:
    return "plan_code" if os.path.exists(RENAME_FLAG) else "plan"


def make_event(account: Account, rng: random.Random, *, event_time: datetime | None = None) -> dict:
    event_type = rng.choices(SESSION_EVENTS, weights=[60, 25, 5, 10])[0]
    return {
        "event_id": str(uuid4()),
        "account_id": account.account_id,
        "user_id": rng.choice(account.users),
        "event_type": event_type,
        "event_time": (event_time or datetime.now(UTC)).isoformat(),
        "payload": {
            "feature": rng.choice(FEATURES),
            plan_field_name(): account.plan,
            "surface": rng.choice(["web", "mobile", "api"]),
            "duration_ms": int(rng.lognormvariate(6, 1)),
        },
    }


def run(
    *,
    collector_url: str,
    accounts: int,
    rate: float,
    duplicate_rate: float,
    late_rate: float,
    late_max_days: int,
    seed: int,
    duration: float | None,
    app_dsn: str | None = None,
    refresh_seconds: float = 60.0,
) -> None:
    rng = random.Random(seed)

    conn = psycopg.connect(app_dsn, autocommit=True) if app_dsn else None
    if conn is None:
        # No source database: fall back to invented ids. Loud, because this is the configuration
        # that silently produced events for accounts that did not exist.
        log.warning("no --app-dsn: emitting for synthetic accounts 1..%d", accounts)
        population = build_accounts(accounts, seed)
    else:
        population = load_accounts(conn, seed)
    weights = [a.intensity for a in population]
    last_refresh = time.monotonic()
    emitter = Emitter(collector_url)
    started = time.monotonic()
    interval = 1.0 / rate if rate > 0 else 0.0
    next_tick = time.monotonic()

    # len(population), not the configured count: those are different numbers whenever the
    # source is the database, and printing the configured one is how a generator emitting for
    # a population it had invented still looked correct in the logs.
    log.info(
        "simulating %d accounts (%s) at %.1f events/s -> %s",
        len(population),
        "from source database" if conn is not None else "SYNTHETIC ids 1..N",
        rate,
        collector_url,
    )
    while duration is None or time.monotonic() - started < duration:
        if conn is not None and time.monotonic() - last_refresh >= refresh_seconds:
            # Signups and erasures move the population continuously, so a list read once at
            # startup goes stale the same way the hard-coded range did -- just more slowly.
            refreshed = load_accounts(conn, seed)
            last_refresh = time.monotonic()
            if refreshed:
                if len(refreshed) != len(population):
                    log.info("population %d -> %d accounts", len(population), len(refreshed))
                population = refreshed
                weights = [a.intensity for a in population]

        if not population:
            # Emitting for an account that does not exist is the defect, so wait for one.
            log.warning("no accounts in the source database; waiting")
            time.sleep(min(refresh_seconds, 5.0))
            if conn is not None:
                population = load_accounts(conn, seed)
                weights = [a.intensity for a in population]
                last_refresh = time.monotonic()
            continue

        account = rng.choices(population, weights=weights)[0]

        event_time = None
        if rng.random() < late_rate:
            # An offline device flushing its queue days later.
            event_time = datetime.now(UTC) - timedelta(
                days=rng.uniform(0.5, late_max_days), seconds=rng.uniform(0, 86400)
            )

        event = make_event(account, rng, event_time=event_time)
        emitter.emit(event)

        if rng.random() < duplicate_rate:
            # Same event_id, sent twice: the client timed out on a request the server handled.
            emitter.emit(dict(event))

        next_tick += interval
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.monotonic()  # fell behind; do not spiral

    emitter.flush()
    if conn is not None:
        conn.close()
    log.info("sent=%d failed=%d", emitter.sent, emitter.failed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--collector-url", default=os.environ.get("COLLECTOR_URL", "http://localhost:8000")
    )
    parser.add_argument("--accounts", type=int, default=int(os.environ.get("SIM_ACCOUNTS", 2000)))
    parser.add_argument("--rate", type=float, default=float(os.environ.get("SIM_RATE", 20)))
    parser.add_argument(
        "--duplicate-rate", type=float, default=float(os.environ.get("SIM_DUPLICATE_RATE", 0.004))
    )
    parser.add_argument(
        "--late-rate", type=float, default=float(os.environ.get("SIM_LATE_RATE", 0.002))
    )
    parser.add_argument("--late-max-days", type=int, default=3)
    parser.add_argument(
        "--app-dsn",
        default=os.environ.get("APP_DSN"),
        help="source database; events are emitted only for accounts that exist in it",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=float,
        default=float(os.environ.get("SIM_ACCOUNT_REFRESH", 60)),
        help="how often to re-read the account list",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--duration", type=float, default=None, help="seconds; default runs forever"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(
        collector_url=args.collector_url,
        accounts=args.accounts,
        rate=args.rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        late_max_days=args.late_max_days,
        seed=args.seed,
        duration=args.duration,
        app_dsn=args.app_dsn,
        refresh_seconds=args.refresh_seconds,
    )


if __name__ == "__main__":
    main()
