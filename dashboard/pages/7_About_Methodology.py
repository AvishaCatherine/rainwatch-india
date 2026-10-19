"""RainWatch India — Page 7: About & Methodology."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from dashboard import theme
from dashboard.data_loader import PROJECT_ROOT, load_manifest, ready_or_stop

theme.apply_theme()
ready_or_stop()
manifest = load_manifest()
coverage = manifest.get("coverage", {})

theme.header(
    "About & Methodology",
    "Objectives, data sources, architecture, analytical methods, limitations and how to run",
)

c1, c2 = st.columns([3, 2], gap="large")

with c1:
    st.subheader("Project objectives")
    st.markdown(
        "RainWatch India processes historical rainfall at scale with **Apache Spark**, analyses "
        "patterns across time and geography, detects statistically unusual wet/dry periods against "
        "a documented baseline, and estimates **district-level rainfall exposure** — all from a "
        "reproducible, offline pipeline. It is an academic Big Data mini-project: the goal is a "
        "genuine end-to-end demonstration of distributed processing on a real environmental "
        "dataset, not a forecast product."
    )

    st.subheader("Dataset & citation")
    st.markdown(
        f"- **Source**: NASA POWER Daily (Point) API — parameter `PRECTOTCORR` "
        f"(bias-corrected precipitation, **mm/day**), based on the MERRA-2 assimilation "
        f"(native ~0.5° × 0.625° grid).\n"
        f"- **Sampling**: a 0.5° × 0.5° lattice of {manifest.get('geography', {}).get('n_grid_points', '-')} points "
        f"covering India; {manifest.get('geography', {}).get('n_land_cells', '-')} fall on land "
        f"(centroid inside a district polygon).\n"
        f"- **Actual coverage in this build**: {coverage.get('min_date', '?')} → "
        f"{coverage.get('max_date', '?')} ({coverage.get('n_days', 0):,} days, "
        f"{coverage.get('n_years', 0)} calendar years). The latest date is read from the data, never hardcoded.\n"
        f"- **Why not GPM IMERG Final?** IMERG Final (GES DISC) requires NASA Earthdata "
        f"credentials and hundreds of GB per decade — impractical for an offline 8 GB laptop "
        f"project. Per the project's substitution rule, NASA POWER (open, credential-free) is "
        f"used and documented. See `docs/DATA_SOURCES.md` for the full history.\n"
        f"- **District boundaries**: geoBoundaries `gbOpen IND ADM2/ADM1` (open licence).\n"
        f"- **Data citation**: NASA Langley Research Center. *POWER Daily API* — "
        f"https://power.larc.nasa.gov/ (retrieved 2026-10-19)."
    )

    st.subheader("Processing architecture")
    st.markdown(
        "```\n"
        "NASA POWER API ──▶ scripts/01_download_data.py ──▶ data/raw/power/*.json  (3,538 files)\n"
        "                                                        │\n"
        "                                     scripts/02_run_pipeline.py\n"
        "                                                        │\n"
        "             ┌──────────────────────────────────────────┤\n"
        "             ▼                                          ▼\n"
        "   [1] validate (pure Python)                [2] ingest (Spark JSON reader)\n"
        "             │                                          │\n"
        "             │                              [3] clean + calendar features\n"
        "             │                                          │\n"
        "   [5] GeoPandas grid→district   ◀──────────  [4] raw Parquet (partitioned by year)\n"
        "             │                                          │\n"
        "             └──────────────┬───────────────────────────┘\n"
        "                            ▼\n"
        "             [6] Spark aggregations (13 tables)\n"
        "                            ▼\n"
        "      data/processed/*.parquet  +  outputs/pipeline_manifest.json\n"
        "                            ▼\n"
        "        Streamlit dashboard (reads Parquet only — opens in seconds)\n"
        "```"
    )

    st.subheader("How to run the project")
    st.markdown(
        "```bash\n"
        "pip install -r requirements.txt\n"
        "python scripts/00_verify_spark.py      # verify Spark + Parquet work\n"
        "python scripts/01_download_data.py     # one-time data download\n"
        "python scripts/02_run_pipeline.py      # Spark processing (~10 min)\n"
        "python scripts/03_run_benchmark.py     # optional fair benchmark\n"
        "streamlit run dashboard/app.py         # launch the dashboard\n"
        "pytest -q                              # run the test suite\n"
        "```\n"
        "Windows users: see `docs/SETUP_WINDOWS.md` — WSL Ubuntu is recommended "
        "(native Windows needs winutils.exe; the verify script detects this)."
    )

with c2:
    st.subheader("Analytical methods")
    st.markdown(
        "- **Calendar features** derived in Spark: year, month, day, IMD season "
        "(Winter / Pre-monsoon / SW Monsoon / Post-monsoon).\n"
        "- **Trends**: monthly, seasonal and annual aggregations; year-over-year differences. "
        "A statistical trend is reported as such — no causal claim is made.\n"
        "- **Anomalies**: z = (monthly total − baseline mean) / baseline std-dev, where the "
        "baseline is that calendar month's statistics over the full record. Wet: z ≥ +1.5, "
        "dry: z ≤ −1.5 (thresholds in `config/config.yaml`).\n"
        "- **Extremes**: fixed IMD thresholds — ≥ 64.5 mm/day 'heavy', ≥ 115.6 mm/day "
        "'extremely heavy' — counted per day at cell and district level.\n"
        "- **District exposure**: each ~55 km grid cell is assigned to the district containing "
        "its centroid (GeoPandas spatial join); district daily rainfall = mean of member cells. "
        "Indicators: mean annual rainfall, heavy-rain days/year, monsoon share, annual "
        "coefficient of variation, and a 0–100 *rainfall exposure index* (equal-weight percentile "
        "rank of the first two)."
    )

    st.subheader("Limitations (read before quoting numbers)")
    st.markdown(
        "- Reanalysis/model-based rainfall at ~55 km resolution: **area averages**, not "
        "point measurements; single-cell maxima can be noisy.\n"
        "- Districts smaller than one grid cell get no cells (blank on maps, absent from rankings).\n"
        "- The anomaly baseline uses the full ~12-year record, not the WMO 30-year standard.\n"
        "- 2026 is a partial year (data through " + str(coverage.get("max_date", "?")) + ").\n"
        "- Rainfall exposure ≠ flood risk: no damage or hydrological model is used.\n"
        "- Single-machine Spark (`local`) — the project demonstrates the *programming model*, "
        "not a multi-node cluster."
    )

    st.subheader("Why this is a Big Data project")
    st.markdown(
        f"- ~**15 million raw daily observations** ingested and cleaned in Spark;\n"
        f"- **Parquet columnar storage** with year partitioning and broadcast joins;\n"
        f"- distributed **group-by / window / join** operations throughout `src/analytics.py`;\n"
        f"- the identical code runs unchanged on a cluster by changing one master URL;\n"
        f"- a **fair benchmark** (`scripts/03_run_benchmark.py`) compares Spark and Pandas on "
        f"the same operation, machine and data."
    )

theme.disclaimer(
    "**Disclaimer** — RainWatch India is an educational historical-analysis tool built on public "
    "data. It is not an official flood-warning, disaster-forecasting or emergency-management "
    "service. Always follow India Meteorological Department (imdagrimet.gov.in) and NDMA "
    "guidance for real weather warnings."
)

with st.expander("Pipeline run manifest (from the actual run behind this dashboard)"):
    st.json(json.dumps({k: v for k, v in manifest.items() if k != "raw_schema"}, default=str, indent=1))

st.caption(f"Project root: `{PROJECT_ROOT}`")
