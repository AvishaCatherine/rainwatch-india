"""RainWatch India — Page 2: Rainfall Explorer."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import theme
from dashboard.data_loader import load_table, ready_or_stop

theme.apply_theme()
ready_or_stop()
theme.header(
    "Rainfall Explorer",
    "Filter the historical record by year, month and season — every chart recomputes from the processed Spark outputs",
)

monthly = load_table("monthly_summary")
annual = load_table("annual_summary")
seasonal = load_table("seasonal_summary")
clim = load_table("climatology")
hist = load_table("rainfall_histogram")

years = sorted(annual["year"].unique())
sel_years = st.sidebar.multiselect("Years", years, default=years, format_func=str)
months_all = list(range(1, 13))
month_names = monthly.drop_duplicates("month").set_index("month")["month_name"].to_dict()
sel_months = st.sidebar.multiselect(
    "Months", months_all, default=months_all,
    format_func=lambda m: month_names.get(m, str(m)),
)
season = st.sidebar.selectbox(
    "Season", ["All seasons", "Winter (Jan-Feb)", "Pre-monsoon (Mar-May)",
               "SW Monsoon (Jun-Sep)", "Post-monsoon (Oct-Dec)"]
)

if not sel_years or not sel_months:
    theme.note("Select at least one year and one month to explore.")
    st.stop()

m = monthly[monthly["year"].isin(sel_years) & monthly["month"].isin(sel_months)]
a = annual[annual["year"].isin(sel_years)]
s = seasonal[seasonal["year"].isin(sel_years)]
if season != "All seasons":
    s = s[s["season"] == season]
h = hist[hist["month"].isin(sel_months)]

theme.kpi_row(
    [
        ("Filtered period total", f"{m['total_mm'].sum():,.0f} mm",
         f"{len(sel_years)} yrs × {len(sel_months)} months"),
        ("Avg monthly total", f"{m['total_mm'].mean():,.1f} mm", "land-cell average"),
        ("Max monthly total", f"{m['total_mm'].max():,.1f} mm",
         f"{int(m.loc[m['total_mm'].idxmax(), 'year'])} {m.loc[m['total_mm'].idxmax(), 'month_name']}"),
        ("Heavy-rain days", f"{int(m['heavy_days'].sum()):,}", "≥ 64.5 mm/day somewhere in India"),
    ]
)

c1, c2 = st.columns([3, 2], gap="large")
with c1:
    st.subheader("Monthly totals over time")
    m_sorted = m.assign(ym=pd.to_datetime(dict(year=m.year, month=m.month, day=1)))
    fig = go.Figure()
    fig.add_scatter(x=m_sorted["ym"], y=m_sorted["total_mm"], mode="lines",
                    line=dict(color="#4DA3FF", width=2), name="Monthly total")
    fig.add_scatter(x=m_sorted["ym"], y=m_sorted["total_mm"].rolling(3, min_periods=1).mean(),
                    mode="lines", line=dict(color="#FBBF24", width=1.4, dash="dot"),
                    name="3-month mean")
    fig.update_layout(xaxis_title="", yaxis_title="Rainfall (mm)")
    theme.figure(fig, height=330)
with c2:
    st.subheader("Monthly climatology (all years)")
    sel_clim = clim[clim["month"].isin(sel_months)]
    fig2 = go.Figure()
    fig2.add_bar(x=[month_names.get(x, x) for x in sel_clim["month"]],
                 y=sel_clim["baseline_total_mm"], marker_color="#2E7FD4")
    fig2.update_layout(yaxis_title="Mean total (mm)")
    theme.figure(fig2, height=330)

c3, c4 = st.columns(2, gap="large")
with c3:
    st.subheader("Seasonal patterns")
    pivot = s.pivot_table(index="year", columns="season", values="total_mm", aggfunc="sum")
    fig3 = go.Figure()
    colors = {"Winter (Jan-Feb)": "#BAE6FD", "Pre-monsoon (Mar-May)": "#7DD3FC",
              "SW Monsoon (Jun-Sep)": "#2E7FD4", "Post-monsoon (Oct-Dec)": "#4DA3FF"}
    for season_name in pivot.columns:
        fig3.add_scatter(x=pivot.index, y=pivot[season_name], mode="lines+markers",
                         name=season_name, line=dict(color=colors.get(season_name, "#888"), width=2))
    fig3.update_layout(yaxis_title="Season total (mm)", legend=dict(orientation="h", y=1.12))
    theme.figure(fig3, height=330)
with c4:
    st.subheader("Daily-rainfall intensity distribution")
    order = ["No rain (0)", "Very light (0.1-2.4)", "Light (2.5-15.5)", "Moderate (15.6-35.5)",
             "Rather heavy (35.6-64.4)", "Heavy (64.5-115.5)", "Extremely heavy (>=115.6)"]
    agg = h.groupby("class")["n"].sum().reindex([c for c in order if c in h["class"].unique()])
    fig4 = go.Figure()
    fig4.add_bar(x=agg.index, y=agg.values, marker_color="#4DA3FF")
    fig4.update_layout(yaxis_title="Cell-days", xaxis_title="IMD daily rainfall class")
    theme.figure(fig4, height=330)

st.subheader("Year-over-year change (annual totals)")
a_sorted = a.sort_values("year")
yoy = a_sorted["total_mm"].diff()
fig5 = go.Figure()
fig5.add_bar(x=a_sorted["year"], y=yoy,
             marker_color=["#34D399" if v >= 0 else "#F87171" for v in yoy.fillna(0)])
fig5.update_layout(yaxis_title="Change vs previous year (mm)", xaxis_title="Year")
theme.figure(fig5, height=280)
theme.note(
    "Values are land-cell averages over India. A year-over-year change is a statistical "
    "difference in this dataset — it does not by itself identify a climatic cause."
)
