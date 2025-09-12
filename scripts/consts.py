import logging
from datetime import UTC, datetime

LOG_LEVEL = "INFO"
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

DB_URL = "sqlite:///./daulingo.db"
ECHO_SQL = False

INPUTS = "resources/inputs"
OUTPUTS = "resources/outputs"

NUM_USERS = 1000
NUM_EVENTS = 100_000

START_DATE = datetime(2024, 1, 1, tzinfo=UTC)
END_DATE = datetime(2025, 1, 1, tzinfo=UTC)

CHUNK_SIZE = 10_000
