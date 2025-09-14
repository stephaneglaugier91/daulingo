from datetime import date, timedelta
from typing import Set


def classify_state(
    *,
    as_of: date,
    first_seen: date,
    active_dates: Set[date],
) -> str:
    """Compute the state for a user on a given date based on activity windows.

    Weekend activity (Sat/Sun) is treated as occurring on the previous Friday.
    """
    if as_of < first_seen:
        raise ValueError("classify_state called before first_seen; caller should skip.")

    # Normalize weekend activity to Friday
    adjusted_active_dates: Set[date] = set()
    for d in active_dates:
        wd = d.weekday()  # Mon=0 .. Sun=6
        if wd >= 5:  # Sat (5) or Sun (6) -> move to Friday (4)
            adjusted_active_dates.add(d - timedelta(days=wd - 4))
        else:
            adjusted_active_dates.add(d)

    active_today = as_of in adjusted_active_dates
    week_has = _any_in_window(
        adjusted_active_dates, as_of - timedelta(days=7), as_of - timedelta(days=1)
    )
    month_not_week_has = _any_in_window(
        adjusted_active_dates, as_of - timedelta(days=30), as_of - timedelta(days=8)
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


def _any_in_window(active_dates: Set[date], start: date, end: date) -> bool:
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
