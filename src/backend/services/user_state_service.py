import logging
from datetime import date, timedelta
from typing import Dict, List

from backend.repositories.dim_user_repo import DimUserRepo
from backend.repositories.fact_activity_repo import FactActivityRepo
from backend.repositories.user_state_daily_repo import UserStateDailyRepo
from backend.services.classifier import classify_state
from backend.utils import daterange, fill_last_active_dates_inplace


class UserStateService:
    """Service layer orchestrating repositories to compute `user_state_daily`."""

    def __init__(
        self,
        *,
        dim_users: DimUserRepo,
        fact_activity: FactActivityRepo,
        user_state_daily: UserStateDailyRepo,
    ) -> None:
        self.dim_users = dim_users
        self.fact_activity = fact_activity
        self.user_state_daily = user_state_daily

    def compute(self, window_start: date, window_end: date) -> int:
        """Compute and (re)fill user_state_daily for the given date window (inclusive)."""
        if window_start > window_end:
            raise ValueError("window_start must be <= window_end")
        logging.info(f"Computing user_state_daily for {window_start} .. {window_end}")

        # TODO add classifier.lookback and use it here
        read_from_date = window_start - timedelta(days=30)

        users_first_seen = self.dim_users.users_on_or_before(window_end=window_end)
        if not users_first_seen:
            logging.info(
                f"No users with first_seen_date <= {window_end}. Nothing to compute."
            )
            return 0
        user_ids = list(users_first_seen.keys())
        logging.info(f"Users considered: {len(user_ids)}")

        active_dates_by_user = self.fact_activity.active_dates_by_user(
            read_from_date=read_from_date,
            window_end=window_end,
            user_ids=user_ids,
        )
        last_active_before_start = self.fact_activity.last_active_before_start(
            window_start=window_start, user_ids=user_ids
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
        deleted = self.user_state_daily.delete_range(start=window_start, end=window_end)
        logging.info(
            f"Deleted {deleted} existing rows in user_state_daily for the window."
        )

        inserted = self.user_state_daily.bulk_insert(rows)
        logging.info(f"Inserted {inserted} rows into user_state_daily.")
        return inserted
