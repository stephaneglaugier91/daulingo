import altair as alt
import pandas as pd


def area_stack_chart(df: pd.DataFrame, state_order: list[str]) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_area(opacity=0.85)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("user_count:Q", stack="zero", title="User count"),
            color=alt.Color(
                "state:N", scale=alt.Scale(domain=state_order), title="Status"
            ),
            order=alt.Order("state_rank:Q"),
            tooltip=["date:T", "state:N", "user_count:Q"],
        )
        .properties(height=780, title="Daily user counts by status")
    )
