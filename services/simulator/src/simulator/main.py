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


def build_accounts(count: int, seed: int) -> list[Account]:
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
                intensity=rng.lognormvariate(0, 0.8),
            )
        )
    return accounts


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
) -> None:
    rng = random.Random(seed)
    population = build_accounts(accounts, seed)
    weights = [a.intensity for a in population]
    emitter = Emitter(collector_url)
    started = time.monotonic()
    interval = 1.0 / rate if rate > 0 else 0.0
    next_tick = time.monotonic()

    log.info("simulating %d accounts at %.1f events/s -> %s", accounts, rate, collector_url)
    while duration is None or time.monotonic() - started < duration:
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
    )


if __name__ == "__main__":
    main()
