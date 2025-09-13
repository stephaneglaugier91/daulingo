from sqlalchemy import Engine


class Database:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def get_engine(self) -> Engine:
        return self._engine
