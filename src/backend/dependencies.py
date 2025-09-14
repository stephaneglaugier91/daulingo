from fastapi import Depends, Request

from backend.infra.database import Database
from backend.repositories.dim_user_repo import DimUserRepo
from backend.repositories.fact_activity_repo import FactActivityRepo
from backend.repositories.user_state_daily_repo import UserStateDailyRepo
from backend.services.ingest_activity import ActivityIngestService
from backend.services.user_state_service import UserStateService


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_dim_user_repo(db: Database = Depends(get_db)) -> DimUserRepo:
    return DimUserRepo(db)


def get_fact_activity_repo(db: Database = Depends(get_db)) -> FactActivityRepo:
    return FactActivityRepo(db)


def get_user_state_daily_repo(
    db: Database = Depends(get_db),
) -> UserStateDailyRepo:
    return UserStateDailyRepo(db)


def get_user_state_service(
    dim_user_repo: DimUserRepo = Depends(get_dim_user_repo),
    fact_activity_repo: FactActivityRepo = Depends(get_fact_activity_repo),
    user_state_daily_repo: UserStateDailyRepo = Depends(get_user_state_daily_repo),
) -> UserStateService:
    return UserStateService(
        dim_users=dim_user_repo,
        fact_activity=fact_activity_repo,
        user_state_daily=user_state_daily_repo,
    )


def get_activity_ingest_service(
    dim_user_repo: DimUserRepo = Depends(get_dim_user_repo),
    fact_activity_repo: FactActivityRepo = Depends(get_fact_activity_repo),
) -> ActivityIngestService:
    return ActivityIngestService(
        dim_users=dim_user_repo,
        fact_activity=fact_activity_repo,
    )
