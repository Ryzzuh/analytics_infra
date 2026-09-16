"""Run a load outside Airflow.

Useful for local iteration and for the chaos harness, which needs to kill a load at a precise
point (`--crash-before-commit`) without going through the scheduler.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime

import psycopg

from .kafka_source import KafkaMessageSource
from .run import run_load
from .targets import PRODUCT_EVENTS, billing_target, cdc_target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="loader")
    sub = parser.add_subparsers(dest="command", required=True)

    load = sub.add_parser("load", help="Load one topic once")
    load.add_argument("--topic", required=True)
    load.add_argument("--dsn", default=os.environ.get("WAREHOUSE_DSN"))
    load.add_argument(
        "--bootstrap", default=os.environ.get("REDPANDA_BOOTSTRAP", "localhost:19092")
    )
    load.add_argument("--run-id", default=f"cli-{datetime.now(UTC):%Y%m%dT%H%M%S}")
    load.add_argument("--max-records", type=int, default=50_000)
    load.add_argument("--source-path", choices=["live", "backfill"], default="live")
    load.add_argument(
        "--target",
        choices=["product_events", "cdc", "billing"],
        default="product_events",
        help="Which stream this topic carries; decides parsing and destination table",
    )
    load.add_argument(
        "--crash-before-commit",
        action="store_true",
        help="Die with the transaction open, to prove the ledger and rows commit together",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.dsn:
        parser.error("WAREHOUSE_DSN is not set and --dsn was not given")

    def crash() -> None:
        raise SystemExit("crash injected before commit")

    targets = {
        "product_events": lambda: PRODUCT_EVENTS,
        "cdc": cdc_target,
        "billing": billing_target,
    }
    target = targets[args.target]()

    source = KafkaMessageSource(args.bootstrap)
    try:
        with psycopg.connect(args.dsn, autocommit=False) as conn:
            result = run_load(
                conn,
                source,
                topic=args.topic,
                dag_run_id=args.run_id,
                target=target,
                max_records=args.max_records,
                source_path=args.source_path,
                before_commit=crash if args.crash_before_commit else None,
            )
    finally:
        source.close()

    print(
        json.dumps(
            {
                "run_id": result.dag_run_id,
                "topic": result.topic,
                "rows_loaded": result.rows_loaded,
                "dlq_rows": result.dlq_rows,
                "erased_skipped": result.erased_skipped,
                "records_skipped": result.records_skipped,
                "partitions": [
                    {
                        "partition": p.partition_id,
                        "start": p.start_offset,
                        "end": p.end_offset,
                        "rows": p.rows_loaded,
                        "replaced": p.replaced,
                        "skipped": p.skipped,
                    }
                    for p in result.partitions
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
