from datetime import date
from typing import List, Literal

from pydantic import BaseModel


class DateRange(BaseModel):
    min_date: date
    max_date: date


class StatesResponse(BaseModel):
    states: List[str]


class TimeseriesRow(BaseModel):
    date: date
    state: str
    user_count: int


class TimeseriesResponse(BaseModel):
    start: date
    end: date
    exclude_weekends: bool = True
    dimensions: List[Literal["date", "state"]] = ["date", "state"]
    metrics: List[Literal["user_count"]] = ["user_count"]
    rows: List[TimeseriesRow]
