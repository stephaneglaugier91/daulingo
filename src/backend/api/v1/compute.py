from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.dependencies import get_user_state_service
from backend.services.user_state_service import UserStateService

router = APIRouter(prefix="/v1")


@router.post("/compute")
def compute(
    start_date: date | None = Query(None, description="Default: earliest date in DB"),
    end_date: date | None = Query(None, description="Default: latest date in DB"),
    service: UserStateService = Depends(get_user_state_service),
) -> dict:
    if start_date is None or end_date is None:
        earliest, latest = service.fact_activity.get_min_max_dates()
        start_date = start_date or earliest
        end_date = end_date or latest
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail=f"start_date must be <= end_date: {start_date} > {end_date}",
        )
    service.compute(start_date, end_date)
    return {"status": "Computation started"}
