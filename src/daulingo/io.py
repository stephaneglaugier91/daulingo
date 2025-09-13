import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Tuple, Type

from pydantic import BaseModel, Field, ValidationError


class ActivityEvent(BaseModel):
    user_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        pattern=r"^[a-zA-Z]+\.[a-zA-Z]+$",
    )
    occurred_at: datetime


def read_activity_csv_in_chunks(
    csv_path: Path,
    *,
    chunk_size: int,
) -> Iterator[List[Tuple[str, datetime]]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    buf: List[Tuple[str, datetime]] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=2):
            try:
                ev = ActivityEvent.model_validate(row)
            except ValidationError as e:
                # raise e
                logging.debug(f"Skipping invalid row at line {i}: {e.errors()[0]}")
                continue

            buf.append((ev.user_id, ev.occurred_at))
            if len(buf) >= chunk_size:
                yield buf
                buf = []

    if buf:
        yield buf
