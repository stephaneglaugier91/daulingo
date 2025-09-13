import logging
from datetime import date, datetime
from typing import Dict, Sequence, Tuple

from sqlalchemy import bindparam, select, update
from sqlalchemy.engine import Connection

from daulingo.tables import dim_user, fact_activity

Event = Tuple[str, datetime]


def ingest_chunk(conn: Connection, events: Sequence[Event]) -> Tuple[int, int, int]:
    # Compute earliest activity date per user for this chunk
    chunk_min_date: Dict[str, date] = {}
    for uid, ts in events:
        d = ts.date()
        prev = chunk_min_date.get(uid)
        if prev is None or d < prev:
            chunk_min_date[uid] = d

    ins_users, upd_users = _ensure_dim_users(conn, chunk_min_date)

    inserted = _insert_fact_activity(conn, events)

    logging.info(
        f"Ingest summary: users +{ins_users} / ↑{upd_users}, events +{inserted}"
    )

    return (inserted, ins_users, upd_users)


def _ensure_dim_users(
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


def _insert_fact_activity(conn: Connection, events: Sequence[Event]) -> int:
    """Bulk insert fact_activity rows for the given events. Returns inserted row count."""
    if not events:
        return 0
    payload = [{"user_id": uid, "occurred_at": ts} for (uid, ts) in events]
    conn.execute(fact_activity.insert(), payload)
    return len(payload)
