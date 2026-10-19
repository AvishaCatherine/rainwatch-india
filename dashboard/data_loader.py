"""RainWatch India — cached loaders for processed Parquet outputs.

The dashboard NEVER touches the raw dataset: it only reads the analytics
Parquet files written by the pipeline, so it opens in seconds and works
fully offline.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED = PROJECT_ROOT / "data" / "processed"
OUTPUTS = PROJECT_ROOT / "outputs"


def project_root() -> Path:
    return PROJECT_ROOT


@st.cache_data(ttl=3600, show_spinner=False)
def load_manifest() -> dict:
    """Load the pipeline manifest (processing stats from the actual run)."""
    for candidate in (PROCESSED / "pipeline_manifest.json", OUTPUTS / "pipeline_manifest.json"):
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as fh:
                return json.load(fh)
    raise FileNotFoundError(
        "pipeline_manifest.json not found. Run `python scripts/02_run_pipeline.py` first."
    )


@st.cache_data(ttl=3600, show_spinner=False)
def load_table(name: str) -> pd.DataFrame:
    """Load one processed analytics table (e.g. 'monthly_summary')."""
    path = PROCESSED / name
    if not path.exists():
        raise FileNotFoundError(
            f"Processed table '{name}' not found at {path}. "
            "Run `python scripts/02_run_pipeline.py` first."
        )
    df = pd.read_parquet(path)
    for col in ("date",):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_district_geojson() -> dict:
    path = PROCESSED / "geo" / "districts_simplified.geojson"
    if not path.exists():
        raise FileNotFoundError(f"District GeoJSON not found at {path}. Re-run the pipeline.")
    return json.loads(path.read_text(encoding="utf-8"))


def data_ready() -> bool:
    return (PROCESSED / "annual_summary").exists()


def ready_or_stop() -> None:
    """Guard: show a friendly setup screen when processed data is missing."""
    if data_ready():
        return
    st.error(
        "**Processed data not found.** This dashboard reads analytics Parquet files "
        "from `data/processed/`, which are produced by the Spark pipeline.\n\n"
        "1. Install requirements: `pip install -r requirements.txt`\n"
        "2. Download data once: `python scripts/01_download_data.py`\n"
        "3. Run the pipeline: `python scripts/02_run_pipeline.py`\n"
        "4. Restart this dashboard: `streamlit run dashboard/app.py`"
    )
    st.stop()


def monsoon_months() -> list[int]:
    return [6, 7, 8, 9]


def partial_years(manifest: dict) -> list[int]:
    """Years whose observations do not reach Dec 31 (partial coverage)."""
    end = str(manifest.get("coverage", {}).get("max_date", ""))
    if not end:
        return []
    end_year = int(end[:4])
    if end >= f"{end_year}-12-31":
        return []
    return [end_year]
