"""
Generate an activity.csv with columns:
  user_id, occurred_at

- user_id is in "name.surname" form
- occurred_at is an ISO 8601 UTC timestamp (e.g., 2025-09-11T10:15:30Z)
- Defaults: 1000 users, 10000 events, random users per event
- Date range defaults to the last 365 days
"""

import csv
import logging
import random
from datetime import datetime, timedelta, timezone

import consts


def parse_iso_dt(s: str) -> datetime:
    """Parse an ISO-like date/time into a UTC-aware datetime."""
    # Allow date-only strings by appending midnight
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Support common formats loosely
        raise ValueError(f"Could not parse datetime: {s!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def isoformat_z(dt: datetime) -> str:
    """ISO 8601 with trailing Z for UTC."""
    return (
        dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def make_name(syllables, min_syl=2, max_syl=3) -> str:
    k = random.randint(min_syl, max_syl)
    name = "".join(random.choice(syllables) for _ in range(k))
    return name


def generate_unique_users(n: int) -> list[str]:
    # A set of simple syllables to synthesize pronounceable names.
    # fmt: off
    syl = [
        "al","an","ar","ba","be","bi","bo","ca","ce","ci","co","da","de","di","do","el",
        "en","er","fa","fi","fo","ga","ge","gi","go","ha","he","hi","ho","ia","ib","ic",
        "id","il","im","in","io","ir","is","it","jo","ka","ke","ki","ko","la","le","li",
        "lo","ma","me","mi","mo","na","ne","ni","no","ol","om","on","or","pa","pe","pi",
        "po","qu","ra","re","ri","ro","sa","se","si","so","ta","te","ti","to","ul","um",
        "un","ur","va","ve","vi","vo","wa","we","wi","wo","xa","xe","xi","xo","ya","ye",
        "yi","yo","za","ze","zi","zo",
    ]
    # fmt: on
    users = set()
    while len(users) < n:
        first = make_name(syl)
        last = make_name(syl)
        # ensure at least 2 characters each
        if len(first) < 2 or len(last) < 2:
            continue
        user_id = f"{first}.{last}".lower()
        users.add(user_id)
    return list(users)


def random_datetime_between(start: datetime, end: datetime) -> datetime:
    # Uniform over seconds in range
    delta = end - start
    total_seconds = int(delta.total_seconds())
    # Guard for zero/negative range
    if total_seconds <= 0:
        return start
    offset = random.randint(0, total_seconds)
    return start + timedelta(seconds=offset)


def main():
    output_file = f"{consts.INPUTS}/activity.csv"

    start_dt = consts.END_DATE - timedelta(days=365)

    end_dt = consts.END_DATE

    if end_dt < start_dt:
        raise ValueError("end date must be >= start date")

    users = generate_unique_users(consts.NUM_USERS)

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "occurred_at"])
        for _ in range(consts.NUM_EVENTS):
            u = random.choice(users)
            ts = random_datetime_between(start_dt, end_dt)
            writer.writerow([u, isoformat_z(ts)])

    logging.info(
        f"Wrote {output_file} with {consts.NUM_EVENTS} events across {consts.NUM_USERS} users."
    )
    logging.info(f"Date range: {isoformat_z(start_dt)} to {isoformat_z(end_dt)}")


if __name__ == "__main__":
    main()
