from datetime import date

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import func, select

from backend.infra.database import Database
from daulingo.tables import user_state_daily


def get_min_max_dates(db: Database) -> tuple[date, date]:
    with db.get_engine().begin() as conn:
        mn, mx = conn.execute(
            select(
                func.min(user_state_daily.c.as_of_date),
                func.max(user_state_daily.c.as_of_date),
            )
        ).one()

    today = date.today()
    return (mn or today, mx or today)


def fetch_timeseries(db: Database, start: date, end: date) -> pd.DataFrame:
    stmt = (
        select(
            user_state_daily.c.as_of_date.label("date"),
            sa.cast(user_state_daily.c.state, sa.String).label("state"),
            func.count(user_state_daily.c.user_id).label("user_count"),
        )
        .where(
            user_state_daily.c.as_of_date.between(
                sa.bindparam("start"), sa.bindparam("end")
            )
        )
        .group_by(user_state_daily.c.as_of_date, user_state_daily.c.state)
        .order_by(user_state_daily.c.as_of_date, user_state_daily.c.state)
    )
    with db.get_engine().begin() as conn:
        rows = conn.execute(stmt, {"start": start, "end": end}).all()
    df = pd.DataFrame(rows, columns=["date", "state", "user_count"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df
