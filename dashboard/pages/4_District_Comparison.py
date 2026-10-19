"""RainWatch India — Page 4: District Comparison."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.graph_objects as go
import streamlit as st

from dashboard import theme
from dashboard.data_loader import load_table, ready_or_stop

theme.apply_theme()
ready_or_stop()
theme.header(
    "District Comparison",
    "Long-term rainfall exposure indicators per district, computed by averaging the grid cells "
    "whose centre falls inside each district",
)
theme.disclaimer(
    "These are historical *rainfall exposure* indicators — they describe where heavy rainfall "
    "is climatologically most frequent. Rainfall alone does not predict flooding, and this "
    "page is not a validated flood-risk model."
)

stats = load_table("district_stats")
district_monthly = load_table("district_monthly")
stats = stats[stats["state"] != "DATA NOT AVAILABLE"]

c1, c2 = st.columns([2, 2])
states = ["All states"] + sorted(stats["state"].dropna().unique().tolist())
sel_state = c1.selectbox("State filter", states)
top_n = c2.slider("Show top N districts", 10, 50, 15)

view = stats if sel_state == "All states" else stats[stats["state"] == sel_state]
view = view.drop_duplicates(subset=["district"], keep="first")
top = view.head(top_n)

theme.kpi_row(
    [
        ("Districts in view", f"{len(view)}", f"{sel_state}"),
        ("Wettest in view", f"{top.iloc[0]['district']}" if len(top) else "—",
         f"{top.iloc[0]['mean_annual_mm']:,.0f} mm/yr" if len(top) else ""),
        ("Most heavy-rain days", f"{view.loc[view['heavy_days_per_year'].idxmax(), 'district']}" if len(view) else "—",
         f"{view['heavy_days_per_year'].max():.1f} days/yr (≥ 64.5 mm)" if len(view) else ""),
    ]
)

tab_rank, tab_scatter, tab_series, tab_table = st.tabs(
    ["Rankings", "Variability vs total", "District time series", "Full table"]
)

with tab_rank:
    metric = st.radio(
        "Rank by",
        ["Mean annual rainfall (mm)", "Heavy-rain days per year", "Exposure index (0-100)"],
        horizontal=True,
    )
    col_map = {
        "Mean annual rainfall (mm)": ("mean_annual_mm", "mm/year"),
        "Heavy-rain days per year": ("heavy_days_per_year", "days ≥ 64.5 mm"),
        "Exposure index (0-100)": ("exposure_index", "index"),
    }
    col, unit = col_map[metric]
    data = top.sort_values(col, ascending=True)
    fig = go.Figure()
    fig.add_bar(
        y=[f"{d} ({s})" for d, s in zip(data["district"], data["state"])],
        x=data[col], orientation="h",
        marker_color="#4DA3FF",
        hovertemplate="%{y}: %{x:,.0f} " + unit + "<extra></extra>",
    )
    fig.update_layout(height=max(380, len(data) * 24), yaxis_title="", xaxis_title=unit)
    theme.figure(fig)

with tab_scatter:
    fig = go.Figure()
    fig.add_scatter(
        x=view["mean_annual_mm"], y=view["cv_annual_pct"], mode="markers",
        marker=dict(
            size=8, color=view["heavy_days_per_year"], colorscale="Blues",
            line=dict(width=0.5, color="#0A1428"),
            colorbar=dict(title="heavy days/yr", outlinecolor="#20355B", thickness=12),
        ),
        text=view["district"], customdata=view["state"],
        hovertemplate="%{text} (%{customdata})<br>Mean annual: %{x:,.0f} mm<br>"
                      "CV: %{y:.0f}%<extra></extra>",
    )
    fig.update_layout(xaxis_title="Mean annual rainfall (mm)",
                      yaxis_title="Year-to-year variability (CV, %)", height=420)
    theme.figure(fig)
    theme.note("Bottom-left = chronically dry; bottom-right = wet and reliable; "
               "top-right = wet but unpredictable year to year.")

with tab_series:
    c1, c2 = st.columns(2)
    dlist = sorted(view["district"].unique())
    sel = c1.selectbox("District", dlist,
                       index=dlist.index(top.iloc[0]["district"]) if len(top) else 0)
    series = district_monthly[district_monthly["district"] == sel]
    annual_series = series.groupby("year", as_index=False)["total_mm"].sum()
    fig = go.Figure()
    fig.add_bar(x=annual_series["year"], y=annual_series["total_mm"], marker_color="#2E7FD4",
                name="Annual total")
    fig.add_scatter(x=annual_series["year"],
                    y=annual_series["total_mm"].rolling(3, min_periods=1).mean(),
                    name="3-yr mean", line=dict(color="#FBBF24", width=2))
    fig.update_layout(height=380, yaxis_title="Rainfall (mm)", xaxis_title="Year")
    theme.figure(fig)
    if not series.empty:
        st.caption(
            f"Monthly detail for {sel}: {len(series)} district-months, "
            f"mean monthly total {series['total_mm'].mean():,.0f} mm."
        )

with tab_table:
    show = view[[
        "state", "district", "mean_annual_mm", "std_annual_mm", "cv_annual_pct",
        "heavy_days_per_year", "monsoon_share_pct", "wettest_year", "driest_year",
        "exposure_index",
    ]]
    st.dataframe(
        show.style.format({
            "mean_annual_mm": "{:,.0f}", "std_annual_mm": "{:,.0f}",
            "cv_annual_pct": "{:.0f}", "heavy_days_per_year": "{:.1f}",
            "monsoon_share_pct": "{:.0f}", "exposure_index": "{:.1f}",
        }),
        use_container_width=True, height=520,
    )

st.subheader("How these indicators are calculated")
st.markdown(
    "- **Mean annual rainfall** — average of the district's yearly totals over the full period. "
    "A district's daily rainfall = mean of the ~55 km grid cells whose centre lies inside it.\n"
    "- **Heavy-rain days per year** — days when the district's area-average rainfall is "
    "≥ 64.5 mm/day (the IMD 'heavy rainfall' threshold), averaged per year.\n"
    "- **Monsoon share** — fraction of annual rainfall falling in Jun–Sep (SW monsoon).\n"
    "- **Exposure index (0–100)** — the mean of two percentile ranks within this dataset: "
    "one for mean annual rainfall, one for heavy-rain days. It is a *relative* ranking of "
    "rainfall exposure only, not a flood-risk score."
)
