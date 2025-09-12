from consts import DB_URL, ECHO_SQL
from sqlalchemy import create_engine

from daulingo.internal.tables import metadata

if __name__ == "__main__":
    engine = create_engine(DB_URL, echo=ECHO_SQL)
    metadata.create_all(engine)
