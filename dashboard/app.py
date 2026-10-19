"""RainWatch India — Page 1: Overview (Streamlit entry point).

Run with:  streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import theme
from dashboard.data_loader import load_manifest, load_table, ready_or_stop

theme.apply_theme()
ready_or_stop()

manifest = load_manifest()
annual = load_table("annual_summary")
monthly = load_table("monthly_summary")
clim = load_table("climatology")
anomalies = load_table("anomalies_monthly")

coverage = manifest.get("coverage", {})
validation = manifest.get("validation", {})
ingest = manifest.get("ingest", {})
partial_years = []
max_date = str(coverage.get("max_date", ""))
if max_date and max_date < f"{max_date[:4]}-12-31":
    partial_years = [int(max_date[:4])]

ann = annual.copy()
if partial_years:
    ann = ann[~ann["year"].isin(partial_years)]
wettest = ann.loc[ann["total_mm"].idxmax()]
driest = ann.loc[ann["total_mm"].idxmin()]
mean_annual = ann["total_mm"].mean()
obs_read = validation.get("n_values_all_files") or ingest.get("records_read", 0)
valid_obs = ingest.get("valid_records") or obs_read
n_monsoon_share = ann["monsoon_share_pct"].mean()

# ------------------------------------------------------------------ layout
theme.header(
    "RainWatch India — Historical Rainfall Analytics",
    "Apache Spark · NASA POWER daily precipitation (MERRA-2) · Parquet · Streamlit — "
    "an educational Big Data mini-project",
)
theme.disclaimer(
    "Historical analysis for education only — NOT an official flood-warning or "
    "disaster-forecasting service. Statistical anomalies are not disaster declarations."
)

theme.kpi_row(
    [
        ("Data coverage", f"{coverage.get('min_date','?')} → {coverage.get('max_date','?')}",
         f"{coverage.get('n_days', 0):,} days · {coverage.get('n_years', 0)} calendar years"),
        ("Observations processed", f"{obs_read:,}", f"{valid_obs:,} valid after cleaning"),
        ("Mean annual rainfall", f"{mean_annual:,.0f} mm",
         "India land-cell average, full period"),
        ("Wettest year", f"{int(wettest['year'])}", f"{wettest['total_mm']:,.0f} mm"),
        ("Driest year", f"{int(driest['year'])}", f"{driest['total_mm']:,.0f} mm"),
    ]
)

if partial_years:
    theme.note(f"Note: {partial_years[0]} is a **partial year** (data ends {max_date}); "
               f"its totals are excluded from wet/dry rankings.")

# ----------------------------------------------------------- annual trend
c1, c2 = st.columns([3, 2], gap="large")
with c1:
    st.subheader("Annual rainfall totals")
    fig = go.Figure()
    fig.add_bar(x=annual["year"], y=annual["total_mm"], name="Annual total (mm)",
                marker_color="#2E7FD4", opacity=0.9)
    fig.add_bar(x=annual["year"], y=annual["monsoon_total_mm"], name="SW Monsoon (Jun-Sep)",
                marker_color="#7DD3FC", opacity=0.85)
    fig.add_hline(y=mean_annual, line_dash="dot", line_color="#FBBF24",
                  annotation_text=f"Period mean {mean_annual:,.0f} mm",
                  annotation_font_color="#FBBF24")
    fig.update_layout(barmode="overlay", legend=dict(orientation="h", y=1.1),
                      xaxis_title="Year", yaxis_title="Rainfall (mm)")
    theme.figure(fig, height=380)

with c2:
    st.subheader("Monthly climatology")
    month_order = list(range(1, 13))
    labels = clim.sort_values("month")["month"]
    names = monthly.drop_duplicates("month").set_index("month")["month_name"]
    fig2 = go.Figure()
    fig2.add_bar(x=[names.get(m, m) for m in clim["month"]], y=clim["baseline_total_mm"],
                 marker_color="#4DA3FF",
                 hovertemplate="%{x}: %{y:.0f} mm<extra>Long-term monthly mean</extra>")
    fig2.update_layout(xaxis_title="Month", yaxis_title="Mean monthly total (mm)")
    theme.figure(fig2, height=380)

# ----------------------------------------------------------- key findings
st.subheader("Key historical findings (from this dataset)")
monsoon_avg = clim[clim["month"].isin([6, 7, 8, 9])]["baseline_total_mm"].sum()
winter_avg = clim[clim["month"].isin([1, 2])]["baseline_total_mm"].sum()
top_wet = anomalies[anomalies["category"] == "Unusually wet"].sort_values("z", ascending=False).head(3)
top_dry = anomalies[anomalies["category"] == "Unusually dry"].sort_values("z").head(3)
extremes = load_table("extremes_days")
geo = manifest.get("geography", {})

f1, f2, f3 = st.columns(3)
with f1:
    st.markdown(
        f"- The **southwest monsoon supplies on average {monsoon_avg:,.0f} mm** of the "
        f"~{clim['baseline_total_mm'].sum():,.0f} mm annual land-average total "
        f"({n_monsoon_share:.0f}% of annual rainfall in a typical year)."
        f"\n- Wettest year in the record: **{int(wettest['year'])}** ({wettest['total_mm']:,.0f} mm); "
        f"driest: **{int(driest['year'])}** ({driest['total_mm']:,.0f} mm)."
    )
with f2:
    if len(top_wet):
        wtxt = ", ".join(f"{int(r.year)} {r.month_name} (z={r.z:+.1f})" for r in top_wet.itertuples())
        st.markdown(f"- Most **unusually wet months**: {wtxt}.")
    if len(top_dry):
        dtxt = ", ".join(f"{int(r.year)} {r.month_name} (z={r.z:+.1f})" for r in top_dry.itertuples())
        st.markdown(f"- Most **unusually dry months**: {dtxt}.")
with f3:
    ex0 = extremes.iloc[0]
    st.markdown(
        f"- Highest single grid-cell rainfall in the record: "
        f"**{ex0['land_max_mm']:.0f} mm/day on {pd.Timestamp(ex0['date']).date()}**.\n"
        f"- Dataset covers **{geo.get('n_land_cells','-')} land grid cells** across "
        f"**{geo.get('n_districts_with_data','-')} districts / {geo.get('n_states_with_data','-')} states**."
    )

theme.note(
    "A wet or dry *anomaly* means the month's rainfall departed strongly from this dataset's "
    "long-term baseline — it is a statistical statement, not a claim about any disaster. "
    "Use the pages in the sidebar to explore trends, maps, districts and extremes."
)
