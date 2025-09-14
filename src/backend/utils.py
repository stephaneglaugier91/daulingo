from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Set


def daterange(start: date, end: date) -> Iterable[date]:
    """Yield each date from start to end inclusive."""
    d = start
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one


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
