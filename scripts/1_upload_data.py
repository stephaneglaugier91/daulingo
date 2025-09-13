import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

from daulingo.ingest_activity import ingest_chunk
from daulingo.io import read_activity_csv_in_chunks

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

    total_events, total_inserted_users, total_updated_users = 0, 0, 0

    with engine.begin() as conn:
        for csv_path in csv_files:
            logging.info("Processing file %s", csv_path.name)
            for chunk_idx, events in enumerate(
                read_activity_csv_in_chunks(
                    csv_path,
                    chunk_size=10000,
                ),
                start=1,
            ):
                inserted, ins_users, upd_users = ingest_chunk(conn, events)
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

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
