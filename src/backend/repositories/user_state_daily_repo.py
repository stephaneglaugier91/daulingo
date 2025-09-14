from datetime import date
from typing import Dict, Sequence

import pandas as pd
from sqlalchemy import String, bindparam, cast, delete, func, select

from backend.domain.tables import user_state_daily
from backend.infra.database import Database


class UserStateDailyRepo:
    """Repository for `user_state_daily` writes."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def delete_range(self, *, start: date, end: date) -> int:
        """Delete existing rows in [start, end]. Return number of rows deleted (best effort)."""
        stmt = delete(user_state_daily).where(
            user_state_daily.c.as_of_date >= bindparam("b_start"),
            user_state_daily.c.as_of_date <= bindparam("b_end"),
        )
        with self.db.get_engine().begin() as conn:
            res = conn.execute(stmt, {"b_start": start, "b_end": end})
        try:
            return res.rowcount or 0
        except Exception:
            return 0

    def bulk_insert(self, rows: Sequence[Dict], *, chunk_size: int = 50_000) -> int:
        """Insert rows into user_state_daily in chunks. Return count inserted."""
        total = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            if not chunk:
                continue
            with self.db.get_engine().begin() as conn:
                conn.execute(user_state_daily.insert(), chunk)
            total += len(chunk)
        return total

    def get_min_max_dates(self) -> tuple[date, date]:
        stmt = select(
            func.min(user_state_daily.c.as_of_date),
            func.max(user_state_daily.c.as_of_date),
        )
        with self.db.get_engine().connect() as conn:
            mn, mx = conn.execute(stmt).one()
        today = date.today()
        return (mn or today, mx or today)

    def fetch_timeseries(self, start: date, end: date) -> pd.DataFrame:
        stmt = (
            select(
                user_state_daily.c.as_of_date.label("date"),
                cast(user_state_daily.c.state, String).label("state"),
                func.count(user_state_daily.c.user_id).label("user_count"),
            )
            .where(
                user_state_daily.c.as_of_date.between(
                    bindparam("start"), bindparam("end")
                )
            )
            .group_by(user_state_daily.c.as_of_date, user_state_daily.c.state)
            .order_by(user_state_daily.c.as_of_date, user_state_daily.c.state)
        )
        with self.db.get_engine().connect() as conn:
            rows = conn.execute(stmt, {"start": start, "end": end}).all()
        df = pd.DataFrame(rows, columns=["date", "state", "user_count"])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
