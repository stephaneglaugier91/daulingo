"""
Compute and (re)fill the `user_state_daily` table for every date from the latest
activity date back one year (inclusive).

Definitions (aligned with Duolingo Growth Model states):
- NEW:           active today AND it's the user's first-ever active day
- CURRENT:       active today AND also active in the prior 7 days (d-7..d-1)
- REACTIVATED:   active today AND also active in the prior 30 days (d-30..d-8), but
                 NOT in the prior 7 days
- RESURRECTED:   active today AND last active > 30 days ago (i.e., none in d-30..d-1)
- AT_RISK_WAU:   inactive today AND active in the prior 7 days (d-7..d-1)
- AT_RISK_MAU:   inactive today AND active in d-30..d-8 but NOT in d-7..d-1
- DORMANT:       inactive today AND none in d-30..d-1  (>=30 days inactive)

Notes:
- The script only emits rows for (user_id, as_of_date) where the user exists
  (dim_user.first_seen_date <= as_of_date). It will *not* generate rows before a
  user's first_seen_date.
- To correctly classify the first day in the window, we read activity going back
  30 days before the window start.
- Idempotent: we DELETE any existing user_state_daily rows in the target date
  range, then repopulate them.


"""

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import consts
from sqlalchemy import bindparam, create_engine, delete, func, select
from sqlalchemy.engine import Connection

from daulingo.internal.tables import (
    dim_user,
    fact_activity,
    user_state_daily,
)

# ------------------------------ Helpers --------------------------------------

State = str  # One of GROWTH_STATE_VALUES


def daterange(start: date, end: date) -> Iterable[date]:
    """Yield each date from start to end inclusive."""
    d = start
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one


def any_in_window(active_dates: Set[date], start: date, end: date) -> bool:
    """True if the set has ANY date in [start, end], where start <= end."""
    # Fast path if the window is "empty"
    if start > end:
        return False
    # Iterating one by one keeps memory predictable and is fine at <=30 checks.
    delta = (end - start).days
    for i in range(delta + 1):
        if (start + timedelta(days=i)) in active_dates:
            return True
    return False


def classify_state(
    *,
    as_of: date,
    first_seen: date,
    active_dates: Set[date],
) -> State:
    """Compute the state for a user on a given date based on activity windows.

    - `active_dates` is the set of dates on which the user had any activity.
    """
    if as_of < first_seen:
        raise ValueError("classify_state called before first_seen; caller should skip.")

    active_today = as_of in active_dates
    week_has = any_in_window(
        active_dates, as_of - timedelta(days=7), as_of - timedelta(days=1)
    )
    month_not_week_has = any_in_window(
        active_dates, as_of - timedelta(days=30), as_of - timedelta(days=8)
    )

    if active_today:
        if as_of == first_seen:
            return "NEW"
        if week_has:
            return "CURRENT"
        if month_not_week_has:
            return "REACTIVATED"
        return "RESURRECTED"

    # inactive today
    if week_has:
        return "AT_RISK_WAU"
    if month_not_week_has:
        return "AT_RISK_MAU"
    return "DORMANT"


# ------------------------------ Core logic -----------------------------------


def compute_window(conn: Connection) -> Tuple[date, date, date]:
    """Return (window_start, window_end, read_from_date).

    window_end = latest activity date
    window_start = window_end - 365 days
    read_from_date = window_start - 30 days  (for lookback classification)
    """
    max_ts: Optional[datetime] = conn.execute(
        select(func.max(fact_activity.c.occurred_at))
    ).scalar_one_or_none()

    if max_ts is None:
        raise SystemExit("No activity found in fact_activity; nothing to compute.")

    window_end = max_ts.date()
    window_start = window_end - timedelta(days=365)
    read_from_date = window_start - timedelta(days=30)
    return (window_start, window_end, read_from_date)


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


def compute_states_for_window(
    *,
    users_first_seen: Dict[str, date],
    active_dates_by_user: Dict[str, Set[date]],
    last_active_before_start: Dict[str, Optional[date]],
    window_start: date,
    window_end: date,
) -> Iterable[Dict]:
    """Yield dict rows ready for INSERT into user_state_daily for the window."""
    users = list(users_first_seen.keys())
    users.sort()

    for as_of in daterange(window_start, window_end):
        for uid in users:
            fs = users_first_seen[uid]
            if as_of < fs:
                # Don't emit rows before the user existed
                continue

            active_dates = active_dates_by_user.get(uid, set())

            state: State = classify_state(
                as_of=as_of, first_seen=fs, active_dates=active_dates
            )

            # last_active_date = as_of if active today else the max activity date < as_of
            if as_of in active_dates:
                last_active = as_of
            else:
                # Prefer using the set to find the last active quickly by scanning back up to 30 days
                # (beyond 30 days, we can still be dormant; scanning further isn't necessary for the
                # last_active_date correctness, but we can fall back to the "rolling" last_active we carry).
                # We'll carry a per-user rolling pointer initialized from last_active_before_start.
                last_active = None

            yield {
                "as_of_date": as_of,
                "user_id": uid,
                "state": state,
                # We'll fill last_active_date properly in a second pass to avoid O(365*30*U) backscans.
                "last_active_date": None,
            }


def fill_last_active_dates_inplace(
    rows: List[Dict],
    *,
    users_first_seen: Dict[str, date],
    active_dates_by_user: Dict[str, Set[date]],
    last_active_before_start: Dict[str, Optional[date]],
    window_start: date,
    window_end: date,
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


def main() -> int:
    logging.basicConfig(
        level=getattr(logging, consts.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    engine = create_engine(consts.DB_URL, echo=consts.ECHO_SQL, future=True)

    with engine.begin() as conn:
        window_start, window_end, read_from_date = compute_window(conn)
        logging.info(
            "Target window: %s .. %s (read_from=%s)",
            window_start,
            window_end,
            read_from_date,
        )

        users_first_seen = load_users(conn, window_end=window_end)
        if not users_first_seen:
            logging.info(
                "No users in dim_user with first_seen_date <= %s. Nothing to compute.",
                window_end,
            )
            return 0
        user_ids = list(users_first_seen.keys())
        logging.info("Users considered: %d", len(user_ids))

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
            window_start=window_start,
            window_end=window_end,
        )

        # Clean existing rows in the date range, then insert
        deleted = bulk_delete_range(conn, start=window_start, end=window_end)
        logging.info(
            "Deleted %d existing rows in user_state_daily for the window.", deleted
        )

        inserted = bulk_insert_rows(conn, rows)
        logging.info("Inserted %d rows into user_state_daily.", inserted)

    logging.info("Done.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
