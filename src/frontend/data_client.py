from datetime import date
from typing import List, Tuple

import pandas as pd
import requests
import streamlit as st

from .config import get_settings


@st.cache_data(ttl=300)
def fetch_states() -> List[str]:
    s = get_settings()
    r = requests.get(f"{s.backend_url}/states", timeout=30)
    r.raise_for_status()
    return r.json()["states"]


@st.cache_data(ttl=300)
def fetch_date_range() -> Tuple[date, date]:
    s = get_settings()
    r = requests.get(f"{s.backend_url}/meta/date-range", timeout=30)
    r.raise_for_status()
    j = r.json()
    return pd.to_datetime(j["min_date"]).date(), pd.to_datetime(j["max_date"]).date()


@st.cache_data(ttl=300)
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


def download_excel(start: date, end: date) -> bytes:
    s = get_settings()
    r = requests.get(
        f"{s.backend_url}/timeseries.xlsx",
        params={"start": str(start), "end": str(end)},
        timeout=120,
    )
    r.raise_for_status()
    return r.content
