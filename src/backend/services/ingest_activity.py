import logging
from datetime import date
from typing import Dict, Sequence, Tuple

from backend.domain.models import Activity
from backend.repositories.dim_user_repo import DimUserRepo
from backend.repositories.fact_activity_repo import FactActivityRepo


class ActivityIngestService:
    """
    Orchestrates ingestion of Activity events:
      - ensures users exist in dim_user and first_seen_date is the earliest
      - inserts rows into fact_activity
    """

    def __init__(
        self, *, dim_users: DimUserRepo, fact_activity: FactActivityRepo
    ) -> None:
        self.dim_users = dim_users
        self.fact_activity = fact_activity

    def ingest(self, events: Sequence[Activity]) -> Tuple[int, int, int]:
        """
        Returns (inserted_events, inserted_users, updated_users).
        Business logic lives here; repositories only execute DB operations.
        """
        # Compute earliest activity date per user for this chunk
        chunk_min_date: Dict[str, date] = {}
        for activity in events:
            d = activity.occurred_at.date()
            prev = chunk_min_date.get(activity.user_id)
            if prev is None or d < prev:
                chunk_min_date[activity.user_id] = d

        ins_users, upd_users = self._ensure_dim_users(chunk_min_date)

        # Insert activities
        payload = [a.model_dump() for a in events] if events else []
        inserted = self.fact_activity.bulk_insert(payload)

        logging.info(
            f"Ingest summary: users +{ins_users} / ↑{upd_users}, events +{inserted}"
        )
        return (inserted, ins_users, upd_users)

    def _ensure_dim_users(self, user_min_dates: Dict[str, date]) -> Tuple[int, int]:
        """Insert missing users and update first_seen_date when an earlier date is found."""
        if not user_min_dates:
            return (0, 0)

        user_ids = list(user_min_dates.keys())
        existing = self.dim_users.first_seen_for(user_ids=user_ids)

        # Decide inserts/updates here (business rules)
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

        ins_count = self.dim_users.insert_users(to_insert)
        upd_count = self.dim_users.update_first_seen(to_update)
        return (ins_count, upd_count)
