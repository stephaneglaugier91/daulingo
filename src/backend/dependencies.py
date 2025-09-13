from fastapi import Request

from backend.infra.database import Database


def get_db(request: Request) -> Database:
    return request.app.state.db
