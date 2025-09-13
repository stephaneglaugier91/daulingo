import pandas as pd


def to_wide(df: pd.DataFrame, state_order: list[str]) -> pd.DataFrame:
    df = df.copy()
    df["state"] = pd.Categorical(df["state"], categories=state_order, ordered=True)
    df["state_rank"] = df["state"].cat.codes
    df = df.sort_values(["date", "state"]).reset_index(drop=True)

    wide = df.pivot_table(
        index=df["date"].dt.date,
        columns="state",
        values="user_count",
        aggfunc="sum",
        fill_value=0,
        observed=False,
    ).sort_index()
    wide.columns.name = None
    return df, wide
