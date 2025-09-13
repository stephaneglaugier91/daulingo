import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Set

from sqlalchemy import bindparam, delete, func, select
from sqlalchemy.engine import Connection

from daulingo.classifier import classify_state
from daulingo.tables import (
    dim_user,
    fact_activity,
    user_state_daily,
)

# ------------------------------ Helpers --------------------------------------


def daterange(start: date, end: date) -> Iterable[date]:
    """Yield each date from start to end inclusive."""
    d = start
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one


# ------------------------------ Core logic -----------------------------------


def load_users(conn: Connection, *, window_end: date) -> Dict[str, date]:
    """Return {user_id: first_seen_date} for users seen on/before window_end."""
    rows = conn.execute(
        select(dim_user.c.user_id, dim_user.c.first_seen_date).where(
            dim_user.c.first_seen_date <= bindparam("b_end")
        ),
        {"b_end": window_end},
    ).all()
    return {uid: fs for uid, fs in rows}


def load_active_dates(
    conn: Connection, *, read_from_date: date, window_end: date, user_ids: Sequence[str]
) -> Dict[str, Set[date]]:
    """Return {user_id: set({date1, date2, ...})} of distinct active dates in [read_from_date, window_end]."""
    if not user_ids:
        return {}

    # Pull distinct activity dates per user in one query.
    # Use DATE() to truncate to date (works in SQLite; SQLAlchemy will fetch as str, convert to date in Python).
    rows = conn.execute(
        select(
            fact_activity.c.user_id,
            func.date(fact_activity.c.occurred_at),  # SQLite returns 'YYYY-MM-DD'
        ).where(
            func.date(fact_activity.c.occurred_at) >= bindparam("b_from"),
            func.date(fact_activity.c.occurred_at) <= bindparam("b_end"),
            fact_activity.c.user_id.in_(user_ids),
        ),
        {"b_from": read_from_date.isoformat(), "b_end": window_end.isoformat()},
    ).all()

    out: Dict[str, Set[date]] = defaultdict(set)
    for uid, dstr in rows:
        # Convert 'YYYY-MM-DD' to date
        d = date.fromisoformat(dstr)
        out[uid].add(d)
    # Ensure all users have an entry
    for uid in user_ids:
        out.setdefault(uid, set())
    return out


def load_last_active_before_start(
    conn: Connection, *, window_start: date, user_ids: Sequence[str]
) -> Dict[str, Optional[date]]:
    """Return {user_id: last_active_date_before_start} where value may be None."""
    if not user_ids:
        return {}

    rows = conn.execute(
        select(
            fact_activity.c.user_id,
            func.max(func.date(fact_activity.c.occurred_at)),
        )
        .where(
            func.date(fact_activity.c.occurred_at) < bindparam("b_start"),
            fact_activity.c.user_id.in_(user_ids),
        )
        .group_by(fact_activity.c.user_id),
        {"b_start": window_start.isoformat()},
    ).all()

    out: Dict[str, Optional[date]] = {uid: None for uid in user_ids}
    for uid, dstr in rows:
        out[uid] = date.fromisoformat(dstr) if dstr is not None else None
    return out


def fill_last_active_dates_inplace(
    rows: List[Dict],
    *,
    users_first_seen: Dict[str, date],
    active_dates_by_user: Dict[str, Set[date]],
    last_active_before_start: Dict[str, Optional[date]],
) -> None:
    """Given the state rows (for the window), fill 'last_active_date' per user/day efficiently.

    We iterate per user chronologically carrying a rolling 'last_active' pointer.
    """
    # Organize rows by user then by as_of_date
    rows_by_user: Dict[str, List[Dict]] = defaultdict(list)
    for r in rows:
        rows_by_user[r["user_id"]].append(r)

    for uid, user_rows in rows_by_user.items():
        user_rows.sort(key=lambda r: r["as_of_date"])
        last_active = last_active_before_start.get(uid)
        active_dates = active_dates_by_user.get(uid, set())
        fs = users_first_seen[uid]

        for r in user_rows:
            d = r["as_of_date"]
            if d < fs:
                continue
            if d in active_dates:
                last_active = d
            r["last_active_date"] = last_active


def bulk_delete_range(conn: Connection, *, start: date, end: date) -> int:
    """Delete existing rows in [start, end]. Return number of rows deleted (best effort)."""
    res = conn.execute(
        delete(user_state_daily).where(
            user_state_daily.c.as_of_date >= bindparam("b_start"),
            user_state_daily.c.as_of_date <= bindparam("b_end"),
        ),
        {"b_start": start, "b_end": end},
    )
    try:
        return res.rowcount or 0
    except Exception:
        return 0


def bulk_insert_rows(
    conn: Connection, rows: Sequence[Dict], *, chunk_size: int = 50_000
) -> int:
    """Insert rows into user_state_daily in chunks. Return count inserted."""
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        if not chunk:
            continue
        conn.execute(user_state_daily.insert(), chunk)
        total += len(chunk)
    return total


# ------------------------------ Main -----------------------------------------


def compute(conn: Connection, window_start: date, window_end: date) -> int:
    """Compute and (re)fill user_state_daily for the given date window (inclusive)."""

    if window_start > window_end:
        raise ValueError("window_start must be <= window_end")
    logging.info(f"Computing user_state_daily for {window_start} .. {window_end}")

    # TODO add classifier.lookback and use it here
    read_from_date = window_start - timedelta(days=30)

    users_first_seen = load_users(conn, window_end=window_end)
    if not users_first_seen:
        logging.info(
            f"No users with first_seen_date <= {window_end}. Nothing to compute."
        )
        return 0
    user_ids = list(users_first_seen.keys())
    logging.info(f"Users considered: {len(user_ids)}")

    active_dates_by_user = load_active_dates(
        conn,
        read_from_date=read_from_date,
        window_end=window_end,
        user_ids=user_ids,
    )
    last_active_before_start = load_last_active_before_start(
        conn, window_start=window_start, user_ids=user_ids
    )

    # First pass: compute states (without last_active_date to keep it simple), skip rows before first_seen
    rows: List[Dict] = []
    for as_of in daterange(window_start, window_end):
        for uid in user_ids:
            fs = users_first_seen[uid]
            if as_of < fs:
                continue
            state = classify_state(
                as_of=as_of,
                first_seen=fs,
                active_dates=active_dates_by_user.get(uid, set()),
            )
            rows.append(
                {
                    "as_of_date": as_of,
                    "user_id": uid,
                    "state": state,
                    "last_active_date": None,  # filled below
                }
            )

    # Second pass: fill last_active_date efficiently per user
    fill_last_active_dates_inplace(
        rows,
        users_first_seen=users_first_seen,
        active_dates_by_user=active_dates_by_user,
        last_active_before_start=last_active_before_start,
    )

    # Clean existing rows in the date range, then insert
    deleted = bulk_delete_range(conn, start=window_start, end=window_end)
    logging.info(f"Deleted {deleted} existing rows in user_state_daily for the window.")

    inserted = bulk_insert_rows(conn, rows)
    logging.info(f"Inserted {inserted} rows into user_state_daily.")
