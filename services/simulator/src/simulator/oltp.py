"""The OLTP half of the simulator: subscription lifecycle writes (SPEC.md §3.2).

These writes are what Debezium captures, so this is where the CDC-shaped problems come from:
several transitions inside one day (which a daily snapshot would flatten), updates that touch
nothing we model, and hard deletes on erasure.

Every write sets `effective_at` to *business* time. In steady state that is now; during the
historical replay it is months ago, while the commit itself happens today. SCD2 keys off this
column precisely so replayed history dates correctly (SPEC.md §6.1).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from psycopg import Connection

PLANS = [
    ("free", "Free", 0, 0),
    ("team", "Team", 9900, 1500),
    ("business", "Business", 29900, 2500),
    ("enterprise", "Enterprise", 99900, 4000),
]

# Weighted transitions out of each state. Roughly: trials mostly convert, active accounts
# mostly stay, past_due either recovers or churns.
TRANSITIONS: dict[str, list[tuple[str, float]]] = {
    "trial": [("active", 0.45), ("cancelled", 0.15), ("trial", 0.40)],
    "active": [("active", 0.90), ("past_due", 0.07), ("cancelled", 0.03)],
    "past_due": [("active", 0.55), ("cancelled", 0.35), ("past_due", 0.10)],
    "cancelled": [("cancelled", 1.0)],
}


@dataclass
class OltpWriter:
    conn: Connection
    rng: random.Random

    def ensure_plans(self) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO plans (code, name, monthly_price_cents, seat_price_cents) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (code) DO NOTHING",
                PLANS,
            )
        self.conn.commit()

    def create_account(self, *, effective_at: datetime, company: str, country: str = "AU") -> int:
        account_id = self.conn.execute(
            "INSERT INTO accounts (company_name, country, created_at, effective_at) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (company, country, effective_at, effective_at),
        ).fetchone()[0]
        self.conn.commit()
        return account_id

    def start_trial(
        self, account_id: int, *, effective_at: datetime, plan_code: str = "team"
    ) -> int:
        seats = self.rng.choice([3, 5, 8, 12])
        subscription_id = self.conn.execute(
            "INSERT INTO subscriptions "
            "(account_id, plan_code, status, seats, mrr_cents, started_at, effective_at) "
            "VALUES (%s, %s, 'trial', %s, 0, %s, %s) RETURNING id",
            (account_id, plan_code, seats, effective_at, effective_at),
        ).fetchone()[0]
        self.conn.commit()
        return subscription_id

    def advance(self, subscription_id: int, *, effective_at: datetime) -> str | None:
        """Apply one lifecycle step. Returns the new status, or None if unchanged."""
        row = self.conn.execute(
            "SELECT status, plan_code, seats FROM subscriptions WHERE id = %s", (subscription_id,)
        ).fetchone()
        if row is None:
            return None
        status, plan_code, seats = row

        choices, weights = zip(*TRANSITIONS[status], strict=True)
        new_status = self.rng.choices(choices, weights=weights)[0]
        if new_status == status:
            # Not a transition, but real systems still touch the row. These updates are
            # captured by CDC and must NOT become SCD2 versions.
            self.conn.execute(
                "UPDATE subscriptions SET updated_at = %s WHERE id = %s",
                (effective_at, subscription_id),
            )
            self.conn.commit()
            return None

        price = dict((p[0], (p[2], p[3])) for p in PLANS)[plan_code]
        mrr = 0 if new_status in {"trial", "cancelled"} else price[0] + price[1] * seats
        self.conn.execute(
            "UPDATE subscriptions SET status = %s, mrr_cents = %s, ended_at = %s, "
            "effective_at = %s, updated_at = %s WHERE id = %s",
            (
                new_status,
                mrr,
                effective_at if new_status == "cancelled" else None,
                effective_at,
                effective_at,
                subscription_id,
            ),
        )
        self.conn.commit()
        return new_status

    def change_seats(self, subscription_id: int, *, effective_at: datetime, delta: int) -> None:
        self.conn.execute(
            "UPDATE subscriptions SET seats = greatest(1, seats + %s), effective_at = %s, "
            "updated_at = %s WHERE id = %s",
            (delta, effective_at, effective_at, subscription_id),
        )
        self.conn.commit()

    def delete_account(self, account_id: int) -> None:
        """A hard delete, as a GDPR erasure produces. Cascades to users and subscriptions, so
        Debezium emits a delete (with a full before image) and a tombstone for each row."""
        self.conn.execute("DELETE FROM accounts WHERE id = %s", (account_id,))
        self.conn.commit()

    def beat(self) -> None:
        """Debezium's heartbeat write: keeps the slot's confirmed LSN moving when the captured
        tables are otherwise idle (SPEC.md §4.2)."""
        self.conn.execute("UPDATE debezium_heartbeat SET beat_at = now() WHERE id = 1")
        self.conn.commit()


def replay_history(
    writer: OltpWriter,
    *,
    accounts: int,
    start: datetime,
    end: datetime,
    step: timedelta = timedelta(days=1),
) -> dict[str, int]:
    """Live a year of subscription lifecycle in minutes, dated by business time.

    This is what gives SCD2 real history: Debezium's initial snapshot would report only the
    current state of each row, so a subscription that went trial -> active -> cancelled last
    March would arrive as a single 'cancelled' row (SPEC.md §6.1).
    """
    writer.ensure_plans()
    created: list[tuple[int, int]] = []  # (account_id, subscription_id)
    stats = {"accounts": 0, "transitions": 0, "seat_changes": 0}

    signups_per_step = max(1, accounts // max(1, int((end - start) / step)))
    clock = start

    while clock < end:
        for _ in range(signups_per_step):
            if stats["accounts"] >= accounts:
                break
            account_id = writer.create_account(
                effective_at=clock, company=f"Account {stats['accounts'] + 1}"
            )
            subscription_id = writer.start_trial(
                account_id,
                effective_at=clock,
                plan_code=writer.rng.choice(["free", "team", "business", "enterprise"]),
            )
            created.append((account_id, subscription_id))
            stats["accounts"] += 1

        for _account_id, subscription_id in created:
            if writer.advance(subscription_id, effective_at=clock) is not None:
                stats["transitions"] += 1
            if writer.rng.random() < 0.02:
                writer.change_seats(
                    subscription_id, effective_at=clock, delta=writer.rng.choice([-2, 1, 2, 5])
                )
                stats["seat_changes"] += 1

        clock += step

    return stats


def now_utc() -> datetime:
    return datetime.now(UTC)
