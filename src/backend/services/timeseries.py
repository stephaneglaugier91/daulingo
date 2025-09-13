import pandas as pd

from backend.domain.enums import STATE_ORDER


def apply_weekend_filter(df: pd.DataFrame, exclude_weekends: bool) -> pd.DataFrame:
    if not exclude_weekends:
        return df
    dts = pd.to_datetime(df["date"])
    return df[~dts.dt.dayofweek.isin([5, 6])].copy()


def to_long_records(df: pd.DataFrame) -> list[dict]:
    return df.sort_values(["date", "state"]).to_dict(orient="records")


def wide_pivot(df: pd.DataFrame) -> pd.DataFrame:
    cat = pd.Categorical(df["state"], categories=STATE_ORDER, ordered=True)
    df = df.assign(state=cat).sort_values(["date", "state"])
    wide = df.pivot_table(
        index="date",
        columns="state",
        values="user_count",
        aggfunc="sum",
        fill_value=0,
        observed=False,
    )
    wide.columns.name = None
    return wide.sort_index()
