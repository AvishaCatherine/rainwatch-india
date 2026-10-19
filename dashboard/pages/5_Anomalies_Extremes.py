"""RainWatch India — Page 5: Anomaly and Extreme Rainfall Analysis."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yaml

from dashboard import theme
from dashboard.data_loader import load_manifest, load_table, ready_or_stop

theme.apply_theme()
ready_or_stop()
manifest = load_manifest()

PROJECT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
cfg = yaml.safe_load(PROJECT_CONFIG.read_text(encoding="utf-8"))
wet_z = float(cfg["anomalies"]["wet_z"])
dry_z = float(cfg["anomalies"]["dry_z"])
heavy_mm = float(cfg["extremes"]["heavy_mm"])
extreme_mm = float(cfg["extremes"]["extremely_heavy_mm"])

theme.header(
    "Anomaly & Extreme Rainfall Analysis",
    "Transparent statistical detection of unusually wet/dry months and historical heavy-rain days",
)

anomalies = load_table("anomalies_monthly")
clim = load_table("climatology")
extremes = load_table("extremes_days")

theme.disclaimer(
    "A statistical anomaly is a departure from this dataset's long-term monthly baseline — "
    "it is NOT a confirmed disaster. Extreme-rain days are identified by fixed thresholds "
    "(IMD classification); no official disaster status is implied."
)

theme.kpi_row(
    [
        ("Baseline period", f"{manifest['coverage']['min_date'][:4]}–{manifest['coverage']['max_date'][:4]}",
         "full available record (documented limitation)"),
        ("Wet threshold", f"z ≥ +{wet_z}", "unusually wet month"),
        ("Dry threshold", f"z ≤ {dry_z}", "unusually dry month"),
        ("Unusual months found", f"{int((anomalies['category'] != 'Normal').sum())}",
         f"of {len(anomalies)} months in record"),
    ]
)

c1, c2 = st.columns(2, gap="large")
with c1:
    st.subheader("Long-term monthly baseline (climatology)")
    month_names = anomalies.drop_duplicates("month").set_index("month")["month_name"].to_dict()
    fig = go.Figure()
    fig.add_bar(x=[month_names.get(m, m) for m in clim["month"]], y=clim["baseline_total_mm"],
                marker_color="#2E7FD4", name="Baseline monthly total")
    fig.add_scatter(x=[month_names.get(m, m) for m in clim["month"]],
                    y=clim["baseline_total_mm"] + clim["baseline_std_mm"],
                    mode="lines", line=dict(color="#7DD3FC", dash="dot"), name="+1 std dev")
    fig.add_scatter(x=[month_names.get(m, m) for m in clim["month"]],
                    y=clim["baseline_total_mm"] - clim["baseline_std_mm"],
                    mode="lines", line=dict(color="#F87171", dash="dot"), name="−1 std dev")
    fig.update_layout(yaxis_title="Land-average monthly total (mm)", height=340,
                      legend=dict(orientation="h", y=1.14))
    theme.figure(fig)
with c2:
    st.subheader("Monthly anomaly timeline (z-score)")
    ann = anomalies.assign(ym=pd.to_datetime(dict(year=anomalies.year, month=anomalies.month, day=1)))
    colors = {"Unusually wet": "#34D399", "Unusually dry": "#F87171", "Normal": "#2E4E7E"}
    fig2 = go.Figure()
    for cat, grp in ann.groupby("category"):
        fig2.add_bar(x=grp["ym"], y=grp["z"], name=cat, marker_color=colors.get(cat, "#888"))
    fig2.add_hline(y=wet_z, line_color="#34D399", line_dash="dot")
    fig2.add_hline(y=dry_z, line_color="#F87171", line_dash="dot")
    fig2.update_layout(yaxis_title="z-score", height=340, barmode="relative",
                       legend=dict(orientation="h", y=1.14))
    theme.figure(fig2)

st.subheader("Detected unusual months (inspect the underlying values)")
unusual = anomalies[anomalies["category"] != "Normal"].sort_values("z")
st.dataframe(
    unusual[["year", "month_name", "total_mm", "baseline_total_mm", "z", "anomaly_pct", "category"]]
    .rename(columns={"total_mm": "total (mm)", "baseline_total_mm": "baseline (mm)",
                     "z": "z-score", "anomaly_pct": "anomaly (%)"})
    .style.format({"total (mm)": "{:,.0f}", "baseline (mm)": "{:,.0f}",
                   "z-score": "{:+.2f}", "anomaly (%)": "{:+.1f}"}),
    use_container_width=True, height=320,
)
theme.note(
    "Formula: z = (monthly total − baseline mean) / baseline std-dev, where the baseline is "
    "the mean and std-dev of that calendar month across the full record. "
    "Wet: z ≥ +%.1f · Dry: z ≤ %.1f." % (wet_z, dry_z)
)

st.subheader("Historical extreme-rainfall days")
theme.note(
    f"Thresholds: ≥ {heavy_mm} mm/day = 'heavy' (IMD), ≥ {extreme_mm} mm/day = 'extremely heavy'. "
    "'Max cell' is the single highest ~55 km grid cell (reanalysis single-cell maxima can be noisy); "
    "'land mean' shows the India-wide context for that day."
)
sel_year = st.selectbox("Filter by year", ["All years"] + sorted(extremes["date"].dt.year.unique().tolist(), reverse=True))
frame = extremes if sel_year == "All years" else extremes[extremes["date"].dt.year == sel_year]

def fmt_districts(lst) -> str:
    try:
        items = list(lst) if lst is not None else []
        return "; ".join(f"{d['district']} ({float(d['mean_mm']):.0f} mm)" for d in items[:4])
    except (TypeError, KeyError, ValueError):
        return "—"

frame = frame.copy()
frame["Top districts (area-averaged)"] = frame["top_districts"].apply(fmt_districts)
st.dataframe(
    frame[["date", "land_max_mm", "land_mean_mm", "heavy_cells", "extreme_cells", "Top districts (area-averaged)"]]
    .rename(columns={"date": "Date", "land_max_mm": "Max cell (mm)",
                     "land_mean_mm": "Land mean (mm)", "heavy_cells": f"Cells ≥ {heavy_mm}",
                     "extreme_cells": f"Cells ≥ {extreme_mm}"})
    .assign(Date=lambda d: d["Date"].dt.strftime("%Y-%m-%d")),
    use_container_width=True, height=430,
)
