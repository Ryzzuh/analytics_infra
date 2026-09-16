"""Seed the platform's history (SPEC.md §6.2).

Product events go to the backfill topics from here. The CDC half of history is NOT generated
here: it is produced by living the OLTP lifecycle against an empty source database with
Debezium already streaming (`simulator.oltp.replay_history`), so every historical change is a
real change event with a real before-image rather than a synthetic row shaped like one.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import UTC, datetime, timedelta

from loader.kafka_sink import KafkaMessageSink

from .backfill import DensityProfile, catch_up_history, estimate_events, generate_event_history

log = logging.getLogger("simulator.history")

# Bytes per event once in Postgres, including row and index overhead (SPEC.md §6.2 sizing).
BYTES_PER_EVENT = 500


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="simulator-history", description=__doc__)
    parser.add_argument(
        "--bootstrap", default=os.environ.get("REDPANDA_BOOTSTRAP", "localhost:19092")
    )
    parser.add_argument("--accounts", type=int, default=int(os.environ.get("SIM_ACCOUNTS", 2000)))
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--baseline-rate", type=float, default=1.0)
    parser.add_argument("--peak-rate", type=float, default=20.0)
    parser.add_argument("--ramp-days", type=int, default=14)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--catch-up-from",
        help="ISO timestamp: fill the gap since a restored snapshot instead of seeding history",
    )
    parser.add_argument(
        "--estimate-only",
        action="store_true",
        help="Report how many events this would produce, and produce none",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    end = datetime.now(UTC)
    if args.catch_up_from:
        start = datetime.fromisoformat(args.catch_up_from)
        # A catch-up gap is recent traffic, so it is flat at the live rate: thinning it would
        # show up as a dip in the days immediately before now.
        profile = DensityProfile(args.peak_rate, args.peak_rate, 0)
    else:
        start = end - timedelta(days=args.days)
        profile = DensityProfile(args.baseline_rate, args.peak_rate, args.ramp_days)

    expected = estimate_events(profile, start, end)
    log.info(
        "%s: %s -> %s, ~%d events (~%.1f GB in the warehouse)",
        "catch-up" if args.catch_up_from else "history seed",
        start.date(),
        end.date(),
        expected,
        expected * BYTES_PER_EVENT / 1e9,
    )
    if args.estimate_only:
        return 0

    sink = KafkaMessageSink(args.bootstrap)
    if args.catch_up_from:
        stats = catch_up_history(
            sink,
            window=(start, end),
            accounts=args.accounts,
            live_rate=args.peak_rate,
            seed=args.seed,
        )
    else:
        stats = generate_event_history(
            sink,
            start=start,
            end=end,
            accounts=args.accounts,
            profile=profile,
            seed=args.seed,
        )
    log.info("produced %d events for %d accounts", stats["events"], stats["accounts"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
