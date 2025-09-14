from datetime import date
from typing import List, Tuple
from urllib.parse import urlencode

import pandas as pd
import requests

from frontend.config import get_settings


def fetch_states() -> List[str]:
    s = get_settings()
    r = requests.get(f"{s.backend_url}/states", timeout=30)
    r.raise_for_status()
    return r.json()["states"]


def fetch_date_range() -> Tuple[date, date]:
    s = get_settings()
    r = requests.get(f"{s.backend_url}/meta/date-range", timeout=30)
    r.raise_for_status()
    j = r.json()
    return pd.to_datetime(j["min_date"]).date(), pd.to_datetime(j["max_date"]).date()


def fetch_timeseries(start: date, end: date) -> pd.DataFrame:
    s = get_settings()
    r = requests.get(
        f"{s.backend_url}/timeseries",
        params={"start": str(start), "end": str(end)},
        timeout=60,
    )
    r.raise_for_status()
    rows = r.json().get("rows", [])
    if not rows:
        return pd.DataFrame(columns=["date", "state", "user_count"])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_excel_url(start_date, end_date) -> str:
    s = get_settings()  # must return an object with .backend_url
    params = urlencode({"start": str(start_date), "end": str(end_date)})
    return f"{s.backend_url.rstrip('/')}/timeseries.xlsx?{params}"


def post_compute(start_date, end_date) -> None:
    s = get_settings()
    url = f"{s.backend_url}/compute"
    if start_date and end_date:
        url += f"?start_date={str(start_date)}&end_date={str(end_date)}"
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    print("Computation request sent for", start_date, end_date)
