import logging
import os
from datetime import date

from dotenv import load_dotenv
from sqlalchemy import create_engine

from daulingo.compute_user_state_daily import compute

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> None:
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL not set in environment")
    engine = create_engine(db_url)

    with engine.begin() as conn:
        compute(
            conn,
            window_start=date(2023, 1, 1),
            window_end=date(2025, 9, 12),
        )


if __name__ == "__main__":
    main()
