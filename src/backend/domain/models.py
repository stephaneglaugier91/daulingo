from datetime import date, datetime
from typing import List, Literal

from pydantic import BaseModel, Field


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


class Activity(BaseModel):
    user_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        pattern=r"^[a-zA-Z]+\.[a-zA-Z]+$",
        examples=["john.doe", "alice.smith"],
    )
    occurred_at: datetime = Field(
        ..., examples=["2024-01-15T13:45:30Z", "2024-06-30T08:00:00Z"]
    )
