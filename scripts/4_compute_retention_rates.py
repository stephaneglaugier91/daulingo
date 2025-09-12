#!/usr/bin/env python3
"""
4_compute_retention_rates.py

Compute daily retention rates from `user_state_daily` as described in Duolingo's
Growth Model and output both a CSV and a PNG time-series chart.

PNG now shows a 30-day moving average (configurable via MA_WINDOW_DAYS).
"""

import logging

import consts
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.sql import alias

from daulingo.internal.tables import user_state_daily

# States used in rate calcs
STATE_NEW = "NEW"
STATE_CURR = "CURRENT"
STATE_REACT = "REACTIVATED"
STATE_RESUR = "RESURRECTED"
STATE_AR_WAU = "AT_RISK_WAU"

# Moving-average window (in days/rows)
MA_WINDOW_DAYS = 30


def read_transitions_df() -> pd.DataFrame:
    """Read (t-1)->(t) transitions for each user across all days.
    Returns a DataFrame with columns: as_of_date (t), prev_state, curr_state, count.
    """
    engine = create_engine(consts.DB_URL, echo=consts.ECHO_SQL, future=True)
    with engine.begin() as conn:
        usd_prev = alias(user_state_daily, name="usd_prev")
        usd_curr = alias(user_state_daily, name="usd_curr")

        # Join consecutive days for the same user: t = d, t-1 = d-1
        stmt = (
            select(
                usd_curr.c.as_of_date.label("as_of_date"),
                usd_prev.c.state.label("prev_state"),
                usd_curr.c.state.label("curr_state"),
                func.count().label("n_users"),
            )
            .where(
                and_(
                    usd_curr.c.user_id == usd_prev.c.user_id,
                    usd_curr.c.as_of_date == func.date(usd_prev.c.as_of_date, "+1 day"),
                )
            )
            .group_by(usd_curr.c.as_of_date, usd_prev.c.state, usd_curr.c.state)
        )

        df = pd.read_sql(stmt, conn)

    if df.empty:
        return df

    # ensure python date type
    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date
    return df


def compute_rates(transitions: pd.DataFrame) -> pd.DataFrame:
    """Compute daily retention rates from the transitions table.

    Returns a dataframe indexed by date with columns:
      ["NURR","RURR","SURR","CURR","iWAURR"]
    """
    if transitions.empty:
        return transitions

    # Denominators: total users in prior state at t-1
    den = (
        transitions.groupby(["as_of_date", "prev_state"])["n_users"]
        .sum()
        .rename("den")
        .reset_index()
    )

    # Numerators for each rate = transitions prev_state -> CURRENT
    curr_trans = transitions[transitions["curr_state"] == STATE_CURR].copy()
    num = (
        curr_trans.groupby(["as_of_date", "prev_state"])["n_users"]
        .sum()
        .rename("num")
        .reset_index()
    )

    joined = pd.merge(den, num, on=["as_of_date", "prev_state"], how="left")
    joined["num"] = joined["num"].fillna(0)

    # Map prev_state -> metric name
    state_to_metric = {
        STATE_NEW: "NURR",
        STATE_REACT: "RURR",
        STATE_RESUR: "SURR",
        STATE_CURR: "CURR",
        STATE_AR_WAU: "iWAURR",
    }

    joined["metric"] = joined["prev_state"].map(state_to_metric)
    joined = joined[~joined["metric"].isna()].copy()

    joined["rate"] = joined.apply(
        lambda r: (r["num"] / r["den"]) if r["den"] > 0 else float("nan"), axis=1
    )

    # Pivot to wide format: rows=as_of_date, cols=metric
    wide = joined.pivot_table(
        index="as_of_date", columns="metric", values="rate", aggfunc="first"
    ).sort_index()

    # make the date index continuous daily (forward-looking gaps stay NaN)
    full_idx = pd.date_range(
        start=wide.index.min(), end=wide.index.max(), freq="D"
    ).date
    wide = wide.reindex(full_idx)
    wide.index.name = "as_of_date"

    return wide.reset_index()


def save_csv(df: pd.DataFrame) -> None:
    csv_path = f"{consts.OUTPUTS}/MoM_retention_rates.csv"
    df.to_csv(csv_path, index=False)
    logging.info(f"Wrote {csv_path}")


def plot_png(df: pd.DataFrame) -> None:
    """
    Plot a 30-day moving average of the retention rates.
    (CSV remains daily values; only the plot is smoothed.)
    """
    png_path = f"{consts.OUTPUTS}/MoM_retention_rates.png"

    # Ensure sorted by date and compute rolling mean over rows (daily frequency ensured earlier)
    df_ma = df.sort_values("as_of_date").copy()
    value_cols = [c for c in df_ma.columns if c != "as_of_date"]
    df_ma[value_cols] = (
        df_ma[value_cols].rolling(window=MA_WINDOW_DAYS, min_periods=1).mean()
    )

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    x = pd.to_datetime(df_ma["as_of_date"])

    for col in ["NURR", "RURR", "SURR", "CURR", "iWAURR"]:
        if col in df_ma.columns:
            ax.plot(x, df_ma[col], label=col)

    ax.set_title(f"Retention Rates ( {MA_WINDOW_DAYS}-Day Moving Average )")
    ax.set_xlabel("Date")
    ax.set_ylabel("Rate")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(png_path, dpi=160)
    plt.close(fig)
    logging.info(f"Wrote {png_path}")


def main() -> int:
    transitions = read_transitions_df()
    if transitions.empty:
        logging.error("No consecutive-day transitions found in user_state_daily.")
        return 1

    rates_daily = compute_rates(transitions)

    save_csv(rates_daily)
    plot_png(rates_daily)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
