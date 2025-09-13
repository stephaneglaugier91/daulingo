import io
from datetime import date

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, Response

from backend.dependencies import get_db
from backend.domain.models import TimeseriesResponse
from backend.infra.database import Database
from backend.repositories.user_states import fetch_timeseries
from backend.services.timeseries import (
    apply_weekend_filter,
    to_long_records,
    wide_pivot,
)

router = APIRouter(prefix="/v1")


@router.get("/timeseries", response_model=TimeseriesResponse)
def get_timeseries(
    start: date = Query(...),
    end: date = Query(...),
    exclude_weekends: bool = Query(True),
    capability: str | None = Query(None, description="reserved"),
    user: str | None = Query(None, description="reserved"),
    db: Database = Depends(get_db),
):
    if end < start:
        raise HTTPException(400, "end before start")
    df = fetch_timeseries(db, start, end)
    df = apply_weekend_filter(df, exclude_weekends)
    return {
        "start": start,
        "end": end,
        "exclude_weekends": exclude_weekends,
        "rows": to_long_records(df),
    }


@router.get("/timeseries.xlsx")
def get_timeseries_xlsx(
    start: date,
    end: date,
    exclude_weekends: bool = True,
    capability: str | None = None,
    user: str | None = None,
    db: Database = Depends(get_db),
):
    if end < start:
        raise HTTPException(400, "end before start")
    df = fetch_timeseries(db, start, end)
    df = apply_weekend_filter(df, exclude_weekends)
    w = wide_pivot(df)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="timeseries_long")
        w.to_excel(writer, sheet_name="timeseries_wide")
    return Response(
        content=buf.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="user_states_{start}_to_{end}.xlsx"'
        },
    )
