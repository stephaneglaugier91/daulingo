import streamlit as st

from frontend.charts import area_stack_chart
from frontend.data_client import (
    fetch_date_range,
    fetch_excel_url,
    fetch_states,
    fetch_timeseries,
    post_compute,
)
from frontend.tables import to_wide


def render() -> None:
    st.set_page_config(page_title="DAU-lingo", layout="wide")
    _, col_main, _ = st.columns([1, 6, 1])
    with col_main:
        st.title("DAU-lingo: Duolingo-style usage metrics")

        col1, col2, col3, _ = st.columns([2, 2, 2, 2])

        with col3:
            refresh_clicked = st.button("Refresh", key="refresh_top")
            if refresh_clicked:
                sd = st.session_state.get("start_date")
                ed = st.session_state.get("end_date")
                if sd is None or ed is None:
                    min_d, max_d = fetch_date_range()
                    sd, ed = min_d, max_d

                with st.spinner("Refreshing data…"):
                    post_compute(sd, ed)

                st.rerun()
        with st.spinner("Loading metadata…"):
            min_date, max_date = fetch_date_range()
            state_order = fetch_states()

        with col1:
            start_date = st.date_input(
                "Start date",
                value=min_date,
                min_value=min_date,
                max_value=max_date,
                key="start_date",
            )
        with col2:
            end_date = st.date_input(
                "End date",
                value=max_date,
                min_value=start_date,
                max_value=max_date,
                key="end_date",
            )

        if start_date > end_date:
            st.warning("Start date must be before end date.")
            st.stop()

        with st.spinner("Loading data…"):
            df = fetch_timeseries(start_date, end_date)

        if df.empty:
            st.info("No data for the selected date range.")
            return

        df, wide = to_wide(df, state_order)
        chart = area_stack_chart(df, state_order)
        st.altair_chart(chart)

        link_col, _ = st.columns([1, 4])
        with link_col:
            st.link_button("Download Excel", url=fetch_excel_url(start_date, end_date))

        st.caption("Daily counts by status (wide)")
        st.dataframe(wide)
