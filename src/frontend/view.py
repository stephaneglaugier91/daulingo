import streamlit as st

from .charts import area_stack_chart
from .data_client import (
    download_excel,
    fetch_date_range,
    fetch_states,
    fetch_timeseries,
)
from .tables import to_wide


def render() -> None:
    st.set_page_config(page_title="DAU-lingo", layout="wide")
    st.title("DAU-lingo: Duolingo-style usage metrics")

    try:
        min_date, max_date = fetch_date_range()
        state_order = fetch_states()
    except Exception as e:
        st.error(f"Failed to load metadata: {e}")
        st.stop()

    col1, _ = st.columns([1, 4])
    with col1:
        date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    if start_date > end_date:
        st.warning("Start date must be before end date.")
        st.stop()

    with st.spinner("Loading data…"):
        try:
            df = fetch_timeseries(start_date, end_date)
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            st.stop()

    if df.empty:
        st.info("No data for the selected date range.")
        return

    df, wide = to_wide(df, state_order)
    chart = area_stack_chart(df, state_order)
    st.altair_chart(chart, use_container_width=True)

    try:
        xlsx_bytes = download_excel(start_date, end_date)
        st.download_button(
            "Export to Excel",
            data=xlsx_bytes,
            file_name=f"user_states_{start_date}_to_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        with st.expander("Export error"):
            st.write(str(e))

    st.caption("Daily counts by status (wide)")
    st.dataframe(wide)
