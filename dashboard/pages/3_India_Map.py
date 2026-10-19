"""RainWatch India — Page 3: India Rainfall Map."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import theme
from dashboard.data_loader import (
    load_district_geojson,
    load_manifest,
    load_table,
    ready_or_stop,
)
from dashboard.theme import MONTH_NAMES

theme.apply_theme()
ready_or_stop()
manifest = load_manifest()
geo = manifest.get("geography", {})
theme.header(
    "India Rainfall Map",
    f"Spatial distribution of rainfall on the dataset's {geo.get('n_land_cells', '-')} land grid cells "
    "(~55 km resolution) and districts",
)
theme.disclaimer(
    "Resolution notice: NASA POWER data is a satellite/model grid of ~55 km cells (0.5°). "
    "A coloured cell is an *area average*, not the rainfall at a specific street or property."
)

grid_month = load_table("grid_monthly")
grid_stats = load_table("grid_cell_stats")
district_monthly = load_table("district_monthly")

tab_month, tab_clim, tab_district = st.tabs(
    ["Monthly grid map", "Long-term annual mean", "District choropleth"]
)

# ------------------------------------------------------------ month map
with tab_month:
    years = sorted(grid_month["year"].unique())
    c1, c2, _ = st.columns([1, 1, 2])
    sel_year = c1.selectbox("Year", years, index=len(years) - 2, format_func=str)
    sel_month = c2.selectbox("Month", sorted(grid_month["month"].unique()),
                             format_func=lambda m: MONTH_NAMES.get(m, str(m)))

    frame = grid_month[(grid_month["year"] == sel_year) & (grid_month["month"] == sel_month)]
    vmax = frame["total_mm"].quantile(0.98) if len(frame) else 1
    fig = go.Figure()
    fig.add_scattergeo(
        lon=frame["lon"], lat=frame["lat"], mode="markers",
        marker=dict(
            size=9, color=frame["total_mm"], colorscale="Blues",
            cmin=0, cmax=max(vmax, 1), reversescale=False,
            line=dict(width=0.4, color="#0A1428"),
            colorbar=dict(title="mm", outlinecolor="#20355B", thickness=14),
        ),
        customdata=frame["district"],
        hovertemplate=("%{customdata}<br>%{lat:.2f}N, %{lon:.2f}E<br>"
                       "Monthly total: %{marker.color:.0f} mm<extra></extra>"),
    )
    fig.update_geos(
        projection_type="mercator", lataxis_range=[6.0, 37.5], lonaxis_range=[67.0, 98.0],
        showland=True, landcolor="#123055", showocean=True, oceancolor="#0A1428",
        showcountries=True, countrycolor="#2A4A7F", showlakes=True, lakecolor="#0A1428",
        showcoastlines=True, coastlinecolor="#2A4A7F",
    )
    fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                      title=f"Total rainfall — {MONTH_NAMES.get(sel_month, sel_month)} {sel_year}")
    theme.figure(fig)

# ---------------------------------------------------------- long-term map
with tab_clim:
    fig = go.Figure()
    fig.add_scattergeo(
        lon=grid_stats["lon"], lat=grid_stats["lat"], mode="markers",
        marker=dict(
            size=9.5, color=grid_stats["mean_annual_mm"], colorscale="Blues",
            line=dict(width=0.4, color="#0A1428"),
            colorbar=dict(title="mm/yr", outlinecolor="#20355B", thickness=14),
        ),
        customdata=list(zip(grid_stats["district"], grid_stats["monsoon_share_pct"])),
        hovertemplate=("%{customdata[0]}<br>Mean annual: %{marker.color:,.0f} mm<br>"
                       "Monsoon share: %{customdata[1]:.0f}%<extra></extra>"),
    )
    fig.update_geos(
        projection_type="mercator", lataxis_range=[6.0, 37.5], lonaxis_range=[67.0, 98.0],
        showland=True, landcolor="#123055", showocean=True, oceancolor="#0A1428",
        showcountries=True, countrycolor="#2A4A7F", showlakes=True, lakecolor="#0A1428",
        showcoastlines=True, coastlinecolor="#2A4A7F",
    )
    fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                      title="Long-term mean annual rainfall (2015–2026, land cells)")
    theme.figure(fig)

# -------------------------------------------------------- district map
with tab_district:
    years = sorted(district_monthly["year"].unique())
    c1, c2, _ = st.columns([1, 1, 2])
    sel_year = c1.selectbox("Year ", years, index=len(years) - 2, format_func=str, key="dy")
    sel_month = c2.selectbox("Month ", sorted(district_monthly["month"].unique()),
                             format_func=lambda m: MONTH_NAMES.get(m, str(m)), key="dm")

    frame = district_monthly[
        (district_monthly["year"] == sel_year) & (district_monthly["month"] == sel_month)
    ].drop_duplicates(subset=["district"], keep="last")

    try:
        gj = load_district_geojson()
        fig = go.Figure()
        fig.add_choropleth(
            geojson=gj, featureidkey="properties.district",
            locations=frame["district"], z=frame["total_mm"],
            colorscale="Blues", zmin=0,
            marker=dict(line=dict(color="#0A1428", width=0.4)),
            colorbar=dict(title="mm", outlinecolor="#20355B", thickness=14),
            hovertemplate="%{location}<br>Monthly total: %{z:.0f} mm<extra></extra>",
        )
        fig.update_geos(
            projection_type="mercator", lataxis_range=[6.0, 37.5], lonaxis_range=[67.0, 98.0],
            showland=True, landcolor="#0E2038", showocean=True, oceancolor="#0A1428",
            showcountries=True, countrycolor="#2A4A7F",
            showcoastlines=True, coastlinecolor="#2A4A7F", fitbounds=False,
        )
        fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                          title=f"District monthly rainfall — {MONTH_NAMES.get(sel_month, sel_month)} {sel_year}")
        theme.figure(fig)
        theme.note(
            "Districts smaller than one grid cell (~55 km) have no cell centroid inside them "
            "and appear blank — a documented limitation of the district aggregation method."
        )
    except FileNotFoundError:
        theme.note("District GeoJSON not found — re-run the pipeline to enable this view.")
