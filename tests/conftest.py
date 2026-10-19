"""Shared pytest fixtures — small SYNTHETIC fixture, clearly labelled.

No test downloads the real dataset. The synthetic NASA-POWER-shaped payloads
below are manufactured test data used ONLY to verify code behaviour; they are
never treated as real observations by the project.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    spark = (
        SparkSession.builder.appName("RainWatch-Tests")
        .master("local[1]")
        .config("spark.driver.memory", "900m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def synthetic_power_payload(lat: float, lon: float, daily: dict[str, float | None]) -> dict:
    """Build a NASA-POWER-shaped payload (SYNTHETIC data for tests only)."""
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat, 100.0]},
        "properties": {"parameter": {"PRECTOTCORR": daily}},
        "header": {
            "title": "SYNTHETIC TEST PAYLOAD (not real observations)",
            "fill_value": -999.0,
        },
    }


@pytest.fixture()
def raw_power_dir(tmp_path):
    """A tiny synthetic raw directory: 2 points x 8 days with edge-case values.

    Point A (10.0, 70.0): normal values, one -999 fill, one null, one negative.
    Point B (10.5, 70.5): two days of values.
    """
    d = tmp_path / "power"
    d.mkdir()

    daily_a = {
        "20230101": 0.0,
        "20230102": 5.5,
        "20230103": 70.0,          # heavy threshold day
        "20230104": 120.0,         # extremely heavy threshold day
        "20230105": -999.0,        # fill value -> missing
        "20230106": None,          # null
        "20230107": -3.2,          # invalid negative
        "20230108": 2.5,
    }
    daily_b = {
        "20230101": 1.0,
        "20230102": 11.0,
    }
    (d / "power_10.00_70.00.json").write_text(json.dumps(synthetic_power_payload(10.0, 70.0, daily_a)))
    (d / "power_10.50_70.50.json").write_text(json.dumps(synthetic_power_payload(10.5, 70.5, daily_b)))
    (d / "power_broken.json").write_text("{not valid json")
    return d


@pytest.fixture()
def land_pdf():
    """Tiny land-cell assignment frame matching the synthetic grid points."""
    import pandas as pd

    return pd.DataFrame(
        {
            "lat": [10.0, 10.5],
            "lon": [70.0, 70.5],
            "state": ["Test State", "Test State"],
            "district": ["District A", "District B"],
        }
    )
