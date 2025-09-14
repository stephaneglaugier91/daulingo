from collections import defaultdict
from datetime import date
from typing import Dict, Optional, Sequence, Set

from sqlalchemy import bindparam, func, select

from backend.domain.tables import fact_activity
from backend.infra.database import Database


class FactActivityRepo:
    """Repository for `fact_activity` table queries used by state computation."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def active_dates_by_user(
        self,
        *,
        read_from_date: date,
        window_end: date,
        user_ids: Sequence[str],
    ) -> Dict[str, Set[date]]:
        """
        Return {user_id: set({date1, date2, ...})} of distinct active dates in
        [read_from_date, window_end].
        """
        if not user_ids:
            return {}

        stmt = select(
            fact_activity.c.user_id, func.date(fact_activity.c.occurred_at)
        ).where(
            func.date(fact_activity.c.occurred_at) >= bindparam("b_from"),
            func.date(fact_activity.c.occurred_at) <= bindparam("b_end"),
            fact_activity.c.user_id.in_(user_ids),
        )
        with self.db.get_engine().connect() as conn:
            rows = conn.execute(
                stmt,
                {"b_from": read_from_date.isoformat(), "b_end": window_end.isoformat()},
            ).all()

        out: Dict[str, Set[date]] = defaultdict(set)
        for uid, d in rows:
            if isinstance(d, str):
                d = date.fromisoformat(d)
            out[uid].add(d)
        for uid in user_ids:
            out.setdefault(uid, set())
        return out

    def last_active_before_start(
        self, *, window_start: date, user_ids: Sequence[str]
    ) -> Dict[str, Optional[date]]:
        """Return {user_id: last_active_date_before_start} (value may be None)."""
        if not user_ids:
            return {}
        stmt = (
            select(
                fact_activity.c.user_id,
                func.max(func.date(fact_activity.c.occurred_at)),
            )
            .where(
                func.date(fact_activity.c.occurred_at) < bindparam("b_start"),
                fact_activity.c.user_id.in_(user_ids),
            )
            .group_by(fact_activity.c.user_id)
        )
        with self.db.get_engine().connect() as conn:
            rows = conn.execute(stmt, {"b_start": window_start.isoformat()}).all()

        out: Dict[str, Optional[date]] = {uid: None for uid in user_ids}
        for uid, dstr in rows:
            out[uid] = date.fromisoformat(dstr) if dstr is not None else None
        return out

    def bulk_insert(self, rows: Sequence[dict]) -> int:
        """Bulk insert rows into fact_activity. Each row is a plain dict."""
        if not rows:
            return 0
        with self.db.get_engine().begin() as conn:
            conn.execute(fact_activity.insert(), rows)
        return len(rows)

    def get_min_max_dates(self) -> tuple[date, date]:
        """Return (min_date, max_date) of occurred_at in fact_activity table."""
        stmt = select(
            func.min(func.date(fact_activity.c.occurred_at)),
            func.max(func.date(fact_activity.c.occurred_at)),
        )
        with self.db.get_engine().connect() as conn:
            row = conn.execute(stmt).one()
        if row[0] is None or row[1] is None:
            raise ValueError("fact_activity table is empty")
        min_date = row[0] if isinstance(row[0], date) else date.fromisoformat(row[0])
        max_date = row[1] if isinstance(row[1], date) else date.fromisoformat(row[1])
        return min_date, max_date
