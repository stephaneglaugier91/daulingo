import logging
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from daulingo.tables import metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL not set in environment")
    engine = create_engine(db_url, echo=True)
    metadata.create_all(engine)
