from fastapi import APIRouter, Depends

from backend.dependencies import get_user_state_daily_repo
from backend.domain.enums import STATE_ORDER
from backend.domain.models import DateRange, StatesResponse
from backend.repositories.user_state_daily_repo import UserStateDailyRepo

router = APIRouter(prefix="/v1")


@router.get("/meta/date-range", response_model=DateRange)
def date_range(db: UserStateDailyRepo = Depends(get_user_state_daily_repo)):
    mn, mx = db.get_min_max_dates()
    return {"min_date": mn, "max_date": mx}


@router.get("/states", response_model=StatesResponse)
def states():
    return {"states": STATE_ORDER}
