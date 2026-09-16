"""Twelve months of history, produced into the backfill topics (SPEC.md §6.2).

Why separate topics rather than a "backfill mode" flag: a replayed event from last March and a
genuinely late event from last Tuesday are indistinguishable by timestamp. A flag would have to
be switched off afterwards, and a flag left on disables the lateness policy permanently and
silently. Separate topics make the distinction structural — it is visible in the data as
`source_path`, and the live path keeps its cutoff throughout.

Density is not uniform. Uniform 20/s for a year would be ~630M events (~300 GB in Postgres),
which does not fit the VM; uniform 1/s makes the recent weeks look dead. So history runs thin
(~1/s) and ramps to full rate over the final fortnight, which is also what a real product's
growth curve looks like.
"""

from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from loader.sink import MessageSink

from .main import Account, build_accounts, make_event

log = logging.getLogger("simulator.backfill")

EVENT_FAMILY_TOPICS = {
    "page_view": "product.session.backfill",
    "session_start": "product.session.backfill",
    "session_end": "product.session.backfill",
    "feature_invoked": "product.feature_usage.backfill",
    "report_exported": "product.feature_usage.backfill",
    "api_called": "product.feature_usage.backfill",
}


@dataclass(frozen=True)
class DensityProfile:
    """Events per second over the replayed window."""

    baseline_rate: float = 1.0
    peak_rate: float = 20.0
    ramp_days: int = 14

    def rate_at(self, when: datetime, end: datetime) -> float:
        """Baseline until the ramp window, then linear to the peak at `end`.

        `ramp_days = 0` means no ramp at all: a flat profile, which is what a catch-up wants.
        """
        if self.ramp_days <= 0:
            return self.peak_rate
        days_remaining = (end - when).total_seconds() / 86400
        if days_remaining >= self.ramp_days:
            return self.baseline_rate
        progress = 1 - (days_remaining / self.ramp_days)
        return self.baseline_rate + (self.peak_rate - self.baseline_rate) * progress


def estimate_events(profile: DensityProfile, start: datetime, end: datetime) -> int:
    """How many events a window will produce, before generating any of them.

    Worth knowing up front: at ~500 bytes a row in Postgres, the difference between a thin and
    a dense profile is the difference between a 20 GB and a 300 GB warehouse.
    """
    total = 0.0
    step = timedelta(hours=1)
    clock = start
    while clock < end:
        total += profile.rate_at(clock, end) * step.total_seconds()
        clock += step
    return int(total)


def generate_event_history(
    sink: MessageSink,
    *,
    start: datetime,
    end: datetime,
    accounts: int = 2000,
    profile: DensityProfile | None = None,
    seed: int = 42,
    step: timedelta = timedelta(hours=1),
    flush_every: int = 50_000,
    account_pool: list[Account] | None = None,
) -> dict[str, int]:
    """Produce historical product events into the backfill topics.

    Events are keyed by account, exactly as the collector keys live traffic, so per-account
    ordering holds on the backfill topics too.
    """
    profile = profile or DensityProfile()
    rng = random.Random(seed)
    population = account_pool or build_accounts(accounts, seed)
    weights = [a.intensity for a in population]

    produced = 0
    clock = start
    while clock < end:
        window_events = int(profile.rate_at(clock, end) * step.total_seconds())
        for _ in range(window_events):
            account = rng.choices(population, weights=weights)[0]
            # Spread within the step, so event_time is not a staircase of identical values.
            offset = timedelta(seconds=rng.uniform(0, step.total_seconds()))
            event = make_event(account, rng, event_time=clock + offset)
            event["received_at"] = (clock + offset + timedelta(milliseconds=40)).isoformat()
            topic = EVENT_FAMILY_TOPICS[event["event_type"]]
            sink.send(
                topic,
                json.dumps(event).encode(),
                key=str(account.account_id).encode(),
            )
            produced += 1
            if produced % flush_every == 0:
                sink.flush()
                log.info("produced %d historical events (at %s)", produced, clock.date())
        clock += step

    sink.flush()
    return {"events": produced, "accounts": len(population)}


def generate_event_id() -> str:
    return str(uuid4())


def catch_up_history(
    sink: MessageSink,
    *,
    window: tuple[datetime, datetime],
    accounts: int = 2000,
    live_rate: float = 20.0,
    seed: int = 42,
) -> dict[str, int]:
    """Fill the gap a restore leaves behind (SPEC.md §9.3).

    Restoring a snapshot taken a week ago and resuming at "now" would leave a week-shaped hole
    in every mart: no signups, no usage, renewals that never happened. The gap is replayed
    through the backfill path instead — which also means the backfill path is exercised every
    week rather than rotting between history rebuilds.

    Density is flat at the live rate, not the ramp used for deep history: the gap IS recent
    traffic, and thinning it would show up as a visible dip immediately before now.
    """
    start, end = window
    if end <= start:
        return {"events": 0, "accounts": 0}
    return generate_event_history(
        sink,
        start=start,
        end=end,
        accounts=accounts,
        profile=DensityProfile(baseline_rate=live_rate, peak_rate=live_rate, ramp_days=0),
        seed=seed,
        step=timedelta(minutes=15),
    )
