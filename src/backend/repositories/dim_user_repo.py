from datetime import date
from typing import Dict, Sequence

from sqlalchemy import bindparam, select, update

from backend.domain.tables import dim_user
from backend.infra.database import Database


class DimUserRepo:
    """Repository for `dim_user` table."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def users_on_or_before(self, *, window_end: date) -> Dict[str, date]:
        """Return {user_id: first_seen_date} for users seen on/before window_end."""
        stmt = select(dim_user.c.user_id, dim_user.c.first_seen_date).where(
            dim_user.c.first_seen_date <= bindparam("b_end")
        )
        with self.db.get_engine().connect() as conn:
            rows = conn.execute(stmt, {"b_end": window_end}).all()
        return {uid: fs for uid, fs in rows}

    def first_seen_for(self, *, user_ids: Sequence[str]) -> Dict[str, date]:
        """Return {user_id: first_seen_date} for a given set of users."""
        if not user_ids:
            return {}
        stmt = select(dim_user.c.user_id, dim_user.c.first_seen_date).where(
            dim_user.c.user_id.in_(user_ids)
        )
        with self.db.get_engine().connect() as conn:
            rows = conn.execute(stmt).all()
        return {uid: fs for uid, fs in rows}

    def insert_users(self, rows: Sequence[dict]) -> int:
        """Insert users into dim_user. Expects dicts with user_id and first_seen_date."""
        if not rows:
            return 0
        with self.db.get_engine().begin() as conn:
            conn.execute(dim_user.insert(), rows)
        return len(rows)

    def update_first_seen(self, rows: Sequence[dict]) -> int:
        """
        Bulk update first_seen_date for provided user_ids.
        Business rule (only update if earlier) is enforced by the service.
        """
        if not rows:
            return 0
        payload = [
            {"b_user_id": r["user_id"], "b_first_seen_date": r["first_seen_date"]}
            for r in rows
        ]
        stmt = (
            update(dim_user)
            .where(dim_user.c.user_id == bindparam("b_user_id"))
            .values(first_seen_date=bindparam("b_first_seen_date"))
        )
        with self.db.get_engine().begin() as conn:
            conn.execute(stmt, payload)
        return len(payload)
