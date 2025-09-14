import logging

from fastapi import FastAPI
from sqlalchemy import create_engine

from backend.api.health import router as health
from backend.api.v1.compute import router as compute
from backend.api.v1.meta import router as meta
from backend.api.v1.record import router as record
from backend.api.v1.timeseries import router as ts
from backend.config import Settings
from backend.infra.database import Database

__title__ = "DAU-lingo API"
__version__ = "0.0.0-SNAPSHOT"
__author__ = "Stephane Augier"


def create_app(config: Settings) -> FastAPI:
    app = FastAPI(title=__title__, version=__version__)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    app.include_router(health)
    app.include_router(ts)
    app.include_router(meta)
    app.include_router(compute)
    app.include_router(record)

    app.state.db = Database(create_engine(config.DATABASE_URL))

    return app
