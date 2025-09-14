import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

from backend.domain.tables import metadata
from backend.infra.database import Database
from backend.repositories.dim_user_repo import DimUserRepo
from backend.repositories.fact_activity_repo import FactActivityRepo
from backend.repositories.user_state_daily_repo import UserStateDailyRepo
from backend.services.ingest_activity import ActivityIngestService
from backend.services.io import read_activity_csv_in_chunks
from backend.services.user_state_service import UserStateService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> int:
    input_dir = Path("resources/")

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory not found: {input_dir.absolute()}")

    csv_files = sorted(input_dir.rglob("*.csv"))
    if not csv_files:
        raise SystemExit(f"No CSV files found in: {input_dir}")

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL not set in environment")
    engine = create_engine(db_url)
    metadata.create_all(bind=engine)
    db = Database(engine)
    activity_service = ActivityIngestService(
        dim_users=DimUserRepo(db), fact_activity=FactActivityRepo(db)
    )

    total_events, total_inserted_users, total_updated_users = 0, 0, 0

    for csv_path in csv_files:
        logging.info("Processing file %s", csv_path.name)
        for events in read_activity_csv_in_chunks(csv_path, chunk_size=10000):
            inserted, ins_users, upd_users = activity_service.ingest(events)
            total_inserted_users += ins_users
            total_updated_users += upd_users

            total_events += inserted

    logging.info(
        "Done. Inserted %d events, %d new users, updated %d users across %d files.",
        total_events,
        total_inserted_users,
        total_updated_users,
        len(csv_files),
    )

    user_state_service = UserStateService(
        dim_users=DimUserRepo(db),
        fact_activity=FactActivityRepo(db),
        user_state_daily=UserStateDailyRepo(db),
    )
    n_user_states_inserted = user_state_service.compute(
        *user_state_service.fact_activity.get_min_max_dates()
    )

    logging.info("Inserted/updated %d user_state_daily rows.", n_user_states_inserted)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
