from fastapi import APIRouter, Depends

from backend.dependencies import get_db
from backend.domain.enums import STATE_ORDER
from backend.domain.models import DateRange, StatesResponse
from backend.infra.database import Database
from backend.repositories.user_states import get_min_max_dates

router = APIRouter(prefix="/v1")


@router.get("/meta/date-range", response_model=DateRange)
def date_range(db: Database = Depends(get_db)):
    mn, mx = get_min_max_dates(db)
    return {"min_date": mn, "max_date": mx}


@router.get("/states", response_model=StatesResponse)
def states():
    return {"states": STATE_ORDER}
