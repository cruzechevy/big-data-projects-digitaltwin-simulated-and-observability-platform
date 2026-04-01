import streamlit as st
import requests
import pandas as pd
import time

st.set_page_config(page_title="Digital Twin Dashboard", layout="wide")

st.title("🚗 Digital Twin Observability Dashboard")

API_URL = "http://api:8000"

# -----------------------------
# Metrics
# -----------------------------
st.subheader("📊 Data Quality Metrics")

try:
    metrics = requests.get(f"{API_URL}/metrics").json()
    df_metrics = pd.DataFrame(metrics)

    if not df_metrics.empty:
        col1, col2, col3 = st.columns(3)

        col1.metric("Total Records", df_metrics.iloc[-1]["total_records"])
        col2.metric("Null Engine Temp", df_metrics.iloc[-1]["null_engine_temp"])
        col3.metric("High Engine Temp", df_metrics.iloc[-1]["high_engine_temp"])

        st.dataframe(df_metrics)

    else:
        st.warning("No metrics data yet")

except:
    st.error("Metrics API not reachable")


# -----------------------------
# Vehicle Activity
# -----------------------------
st.subheader("🚗 Active Vehicles by City")

try:
    activity = requests.get(f"{API_URL}/vehicle_activity").json()
    df_activity = pd.DataFrame(activity)

    if not df_activity.empty:
        chart_data = df_activity.groupby("location")["active_vehicle_count"].sum()

        st.bar_chart(chart_data)
        st.dataframe(df_activity)

    else:
        st.warning("No activity data yet")

except:
    st.error("Activity API not reachable")


# -----------------------------
# Alerts Section
# -----------------------------
st.subheader("🚨 Alerts")

if not df_metrics.empty:
    latest = df_metrics.iloc[-1]

    if latest["high_engine_temp"] > 0:
        st.error("🚨 High engine temperature detected!")

    if latest["null_engine_temp"] > 0:
        st.warning("⚠️ Null engine_temp values present!")

# -----------------------------
# Auto Refresh
# -----------------------------
time.sleep(5)
st.rerun()