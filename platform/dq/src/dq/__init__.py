"""Data-quality and freshness checks (SPEC.md §10.3)."""

from .checks import (
    CheckResult,
    duplicate_rate,
    freshness,
    quarantine_rate,
    reverse_etl_health,
    run_checks,
    volume_anomaly,
)

__all__ = [
    "CheckResult",
    "duplicate_rate",
    "freshness",
    "quarantine_rate",
    "reverse_etl_health",
    "run_checks",
    "volume_anomaly",
]
