"""
Read the `user_state_daily` table and generate a PNG timechart of the
daily user counts grouped by state.

Output:
  - ./user_state_daily_by_state.png

Notes:
  - Uses `consts.DB_URL` for the database connection.
  - Requires `matplotlib` and `pandas`.
  - The chart is a single stacked area chart (one figure) with default styles.
"""

import logging

import consts
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine, func, select

from daulingo.internal.tables import user_state_daily

# Reasonable default ordering for the growth states; if new/extra states are present
# they will be appended at the end in alphabetical order.
DEFAULT_STATE_ORDER = [
    "NEW",
    "CURRENT",
    "REACTIVATED",
    "RESURRECTED",
    "AT_RISK_WAU",
    "AT_RISK_MAU",
    "DORMANT",
]


def fetch_counts_df():
    engine = create_engine(consts.DB_URL, echo=consts.ECHO_SQL, future=True)
    with engine.begin() as conn:
        stmt = select(
            user_state_daily.c.as_of_date.label("as_of_date"),
            user_state_daily.c.state.label("state"),
            func.count().label("user_count"),
        ).group_by(user_state_daily.c.as_of_date, user_state_daily.c.state)
        df = pd.read_sql(stmt, conn)
    if df.empty:
        return df

    # Ensure date dtype
    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date
    return df


def build_pivot(df: pd.DataFrame) -> pd.DataFrame:
    # Pivot: rows = date, columns = state, values = count
    pivot = df.pivot_table(
        index="as_of_date",
        columns="state",
        values="user_count",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()

    # Reindex to a continuous daily range
    full_idx = pd.date_range(
        start=pivot.index.min(), end=pivot.index.max(), freq="D"
    ).date
    pivot = pivot.reindex(full_idx, fill_value=0)
    pivot.index.name = "as_of_date"

    # Order columns
    present_states = list(pivot.columns)
    ordered = [s for s in DEFAULT_STATE_ORDER if s in present_states]
    for s in sorted(set(present_states) - set(ordered)):
        ordered.append(s)
    pivot = pivot[ordered]
    return pivot


def plot_and_save(pivot: pd.DataFrame, outfile: str) -> None:
    # Single figure, default styles, no explicit colors.
    fig, ax = plt.subplots(figsize=(12, 6))

    x = pd.to_datetime(pivot.index)
    y_layers = [pivot[c].values for c in pivot.columns]

    ax.stackplot(x, y_layers, labels=list(pivot.columns))
    ax.set_title("Users by State Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Users")
    ax.legend(loc="upper left", ncols=2, fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(outfile, dpi=160)
    plt.close(fig)


def main() -> int:
    df = fetch_counts_df()
    if df.empty:
        logging.error("No data available in user_state_daily.")
        return 1

    pivot = build_pivot(df)
    outfile = f"{consts.OUTPUTS}/user_state_daily_by_state.png"
    plot_and_save(pivot, outfile)
    logging.info(f"Wrote {outfile}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
