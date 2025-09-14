import csv
import logging
from pathlib import Path
from typing import Iterator

from pydantic import ValidationError

from backend.domain.models import Activity


def read_activity_csv_in_chunks(
    csv_path: Path,
    *,
    chunk_size: int,
    raise_on_invalid: bool = False,
) -> Iterator[list[Activity]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    buf: list[Activity] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=2):
            try:
                activity = Activity.model_validate(row)
            except ValidationError as e:
                error_msg = f"Invalid row at line {i}: {e.errors()[0]}"
                if raise_on_invalid:
                    raise ValueError(error_msg) from e
                logging.debug(error_msg)
                continue

            buf.append(activity)
            if len(buf) >= chunk_size:
                yield buf
                buf = []

    if buf:
        yield buf
