import os
import time
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# =====================================================
# CONFIG
# =====================================================
DATA_DIR = "dashboard_data"
# REFRESH_SECONDS = 3

st.set_page_config(
    page_title="Real-Time Market Dashboard",
    layout="wide"
)

st.title("📈 Real-Time Market Dashboard (Auto-refresh every 5 min)")

# =====================================================
# AUTO-REFRESH
# =====================================================
# if "last_refresh" not in st.session_state:
#     st.session_state.last_refresh = time.time()

# if time.time() - st.session_state.last_refresh > REFRESH_SECONDS:
#     st.session_state.last_refresh = time.time()
#     st.rerun()

# =====================================================
# LOAD AVAILABLE PNODES
# =====================================================
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

if not csv_files:
    st.warning("No data yet. Waiting for Kafka messages...")
    st.stop()

pnode = st.sidebar.selectbox(
    "Select PNode",
    sorted(f.replace(".csv", "") for f in csv_files)
)

metric = st.sidebar.radio(
    "Metric",
    ["lmp", "mcc", "mlc"]
)

hour_range = st.sidebar.slider(
    "Filter Hour Range",
    min_value=0,
    max_value=23,
    value=(0, 23)
)

hour_filter = list(range(hour_range[0], hour_range[1] + 1))

# =====================================================
# LOAD DATA
# =====================================================
csv_path = os.path.join(DATA_DIR, f"{pnode}.csv")
df = pd.read_csv(csv_path)

df["timestamp"] = pd.to_datetime(df["timestamp"])
df["date"] = df["timestamp"].dt.date
df = df.sort_values("timestamp")

# =====================================================
# DAY MANAGEMENT (AUTO-ADVANCE)
# =====================================================
available_days = sorted(df["date"].unique())
latest_day = available_days[-1]

if "selected_day" not in st.session_state:
    st.session_state.selected_day = latest_day

# Auto-switch when a new day appears
if st.session_state.selected_day != latest_day:
    st.session_state.selected_day = latest_day

# Sidebar day selector (manual override)
selected_day = st.sidebar.selectbox(
    "Select Day",
    options=available_days,
    index=available_days.index(st.session_state.selected_day),
    format_func=lambda d: d.strftime("%Y-%m-%d")
)

st.session_state.selected_day = selected_day

# =====================================================
# FILTER DATA
# =====================================================
df_day = df[
    (df["date"] == st.session_state.selected_day) &
    (df["hour"].isin(hour_filter))
]

if df_day.empty:
    st.warning("No data for selected filters.")
    st.stop()

latest = df_day.iloc[-1]

# =====================================================
# KPI ROW
# =====================================================
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("PNode", latest["pnode"])
c2.metric("Latest Timestamp", latest["timestamp"].strftime("%Y-%m-%d %H:%M"))
c3.metric("Latest LMP", f"{latest['lmp']:.2f}")
c4.metric("Latest MCC", f"{latest['mcc']:.2f}")
c5.metric("Latest MLC", f"{latest['mlc']:.2f}")

# =====================================================
# PLOT
# =====================================================
fig, ax = plt.subplots(figsize=(12, 4))

# Transparent background
fig.patch.set_alpha(0)
ax.set_facecolor("none")

ax.plot(
    df_day["timestamp"],
    df_day[metric],
    linewidth=2
)

ax.set_title(
    f"{pnode} – {metric.upper()} ({st.session_state.selected_day})",
    fontsize=13,
    pad=10
)

ax.set_xlabel("Timestamp", fontsize=10)
ax.set_ylabel(metric.upper(), fontsize=10)

# Dark theme styling
ax.tick_params(colors="#CCCCCC")
ax.xaxis.label.set_color("#CCCCCC")
ax.yaxis.label.set_color("#CCCCCC")
ax.title.set_color("#FFFFFF")

for spine in ax.spines.values():
    spine.set_color("#444444")

ax.grid(
    True,
    linestyle="--",
    linewidth=1,
    color="#444444",
    alpha=0.3
)

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

ax.tick_params(axis="both", labelsize=9)

st.pyplot(fig, transparent=True)

# =====================================================
# RAW DATA
# =====================================================
with st.expander("Show raw data"):
    st.dataframe(df_day.tail(300))