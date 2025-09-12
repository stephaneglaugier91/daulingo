"""
Load activity events from a CSV into the provided tables using SQLAlchemy Core.

CSV schema (required columns):
  - user_id
  - occurred_at  (ISO 8601, e.g. 2025-03-11T01:50:16Z)

Behavior:
  - Inserts every row into fact_activity (occurred_at, user_id).
  - Ensures dim_user has one row per user with first_seen_date = earliest activity
    date observed across the entire CSV (safe even if the file is not sorted).
  - Works in chunks to keep memory bounded.
  - No DB/vendor-specific optimizations; pure SQLAlchemy Core + straightforward logic.
"""

import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple

from sqlalchemy import bindparam, create_engine, select, update
from sqlalchemy.engine import Connection

from daulingo.internal.tables import dim_user, fact_activity

# --------------------------- Utilities ---------------------------------------


def parse_iso_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp into an *aware* datetime.

    Python's datetime.fromisoformat doesn't accept a trailing 'Z' prior to 3.11,
    so we normalize 'Z' to '+00:00'.
    """
    s = value.strip()
    if not s:
        raise ValueError("Empty occurred_at value")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"Invalid ISO timestamp: {value!r}") from e
    if dt.tzinfo is None:
        # Assume UTC if missing timezone (keeps behavior consistent)
        from datetime import timezone

        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def read_activity_csv_in_chunks(
    csv_path: Path, *, chunk_size: int
) -> Iterator[List[Tuple[str, datetime]]]:
    """Yield lists of (user_id, occurred_at) pairs of size up to chunk_size.

    Uses csv.DictReader for resilience to column ordering.
    """
    required = {"user_id", "occurred_at"}
    buf: List[Tuple[str, datetime]] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"CSV must include columns {sorted(required)}; got {reader.fieldnames}"
            )
        for i, row in enumerate(reader, start=2):  # start=2 => account for header line
            try:
                user_id = (row.get("user_id") or "").strip()
                occurred_at_raw = (row.get("occurred_at") or "").strip()
                if not user_id:
                    raise ValueError("user_id is empty")
                occurred_at = parse_iso_ts(occurred_at_raw)
            except Exception as e:
                raise ValueError(f"Error parsing CSV at line {i}: {e}") from e

            buf.append((user_id, occurred_at))
            if len(buf) >= chunk_size:
                yield buf
                buf = []
        if buf:
            yield buf


# --------------------------- Loading logic -----------------------------------


def ensure_dim_users(
    conn: Connection, user_min_dates: Dict[str, date]
) -> Tuple[int, int]:
    """Insert missing users and fix first_seen_date for existing users if needed.

    Returns (inserted_count, updated_count).
    """
    if not user_min_dates:
        return (0, 0)

    user_ids = list(user_min_dates.keys())
    # Fetch existing users' first_seen_date
    existing: Dict[str, date] = {}
    for uid, fs in conn.execute(
        select(dim_user.c.user_id, dim_user.c.first_seen_date).where(
            dim_user.c.user_id.in_(user_ids)
        )
    ):
        existing[uid] = fs

    # Compute inserts and updates
    to_insert = [
        {"user_id": uid, "first_seen_date": user_min_dates[uid]}
        for uid in user_ids
        if uid not in existing
    ]

    to_update = [
        {"user_id": uid, "first_seen_date": user_min_dates[uid]}
        for uid, old_fs in existing.items()
        if user_min_dates[uid] < old_fs
    ]

    ins_count = 0
    upd_count = 0
    if to_insert:
        conn.execute(dim_user.insert(), to_insert)
        ins_count = len(to_insert)

    if to_update:
        # Executemany-style updates (avoid reserved param names colliding with column names)
        # SQLAlchemy reserves parameter names matching column names in UPDATE/INSERT.
        # Use distinct names for our bindparams and map the payload accordingly.
        upd_payload = [
            {"b_user_id": u["user_id"], "b_first_seen_date": u["first_seen_date"]}
            for u in to_update
        ]
        conn.execute(
            update(dim_user)
            .where(dim_user.c.user_id == bindparam("b_user_id"))
            .values(first_seen_date=bindparam("b_first_seen_date")),
            upd_payload,
        )
        upd_count = len(upd_payload)

    return (ins_count, upd_count)


def insert_fact_activity(
    conn: Connection, events: Sequence[Tuple[str, datetime]]
) -> int:
    """Bulk insert fact_activity rows for the given events. Returns inserted row count."""
    if not events:
        return 0
    payload = [{"user_id": uid, "occurred_at": ts} for (uid, ts) in events]
    conn.execute(fact_activity.insert(), payload)
    return len(payload)


# ------------------------------ CLI ------------------------------------------


def main() -> int:
    import consts

    csv_path = Path(consts.INPUTS) / "activity.csv"

    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    engine = create_engine(consts.DB_URL, echo=consts.ECHO_SQL, future=True)

    total_events, total_inserted_users, total_updated_users = 0, 0, 0

    with engine.begin() as conn:
        for chunk_idx, events in enumerate(
            read_activity_csv_in_chunks(csv_path, chunk_size=consts.CHUNK_SIZE),
            start=1,
        ):
            # Compute earliest activity date per user for this chunk
            chunk_min_date: Dict[str, date] = {}
            for uid, ts in events:
                d = ts.date()
                prev = chunk_min_date.get(uid)
                if prev is None or d < prev:
                    chunk_min_date[uid] = d

            ins_users, upd_users = ensure_dim_users(conn, chunk_min_date)
            total_inserted_users += ins_users
            total_updated_users += upd_users

            inserted = insert_fact_activity(conn, events)
            total_events += inserted

            logging.info(
                "Chunk %d: users +%d / ↑%d, events +%d",
                chunk_idx,
                ins_users,
                upd_users,
                inserted,
            )

    logging.info(
        "Done. Inserted %d events, %d new users, updated %d users.",
        total_events,
        total_inserted_users,
        total_updated_users,
    )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
