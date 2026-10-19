"""RainWatch India — NASA POWER point-file I/O helpers.

These pure-Python helpers are used by the pipeline's validation stage and by
the automated tests (with a small synthetic fixture, clearly labelled — see
tests/conftest.py). The heavy lifting on the full dataset is done in Spark.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

# NASA POWER files use -999.0 as the fill / missing-value sentinel.
POWER_FILL_VALUE = -999.0


def parse_point_file(path: Path, parameter: str = "PRECTOTCORR") -> dict:
    """Parse one NASA POWER point JSON file into a plain summary dict.

    Returns a dict with:
        lat, lon, elevation_m,
        n_values, min_date, max_date, n_missing, n_null, n_negative,
        valid (bool), error (str|None)
    """
    result = {
        "file": path.name,
        "lat": None,
        "lon": None,
        "elevation_m": None,
        "n_values": 0,
        "min_date": None,
        "max_date": None,
        "n_missing": 0,
        "n_null": 0,
        "n_negative": 0,
        "valid": False,
        "error": None,
    }
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        coords = payload["geometry"]["coordinates"]
        result["lon"], result["lat"], result["elevation_m"] = coords[0], coords[1], coords[2]
        series: dict = payload["properties"]["parameter"][parameter]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    dates = sorted(series.keys())
    result["n_values"] = len(dates)
    if dates:
        result["min_date"] = _to_iso(dates[0])
        result["max_date"] = _to_iso(dates[-1])
    for key in dates:
        val = series[key]
        if val is None:
            result["n_null"] += 1
        elif val == POWER_FILL_VALUE:
            result["n_missing"] += 1
        elif val < 0:
            result["n_negative"] += 1
    result["valid"] = result["n_values"] > 0
    return result


def parse_point_payload(payload: dict, parameter: str = "PRECTOTCORR") -> list[dict]:
    """Flatten one POWER payload into daily rows: [{lat, lon, date, rainfall_mm, flag}].

    flag: 'ok' | 'missing_fill' | 'null' | 'negative'
    This is the single source of truth for how POWER values are interpreted,
    shared by the Spark ingestion path and the tests.
    """
    coords = payload["geometry"]["coordinates"]
    lon, lat = coords[0], coords[1]
    series: dict = payload["properties"]["parameter"][parameter]
    rows: list[dict] = []
    for key, val in series.items():
        if val is None:
            flag = "null"
        elif val == POWER_FILL_VALUE:
            flag = "missing_fill"
        elif val < 0:
            flag = "negative"
        else:
            flag = "ok"
        rows.append(
            {
                "lat": float(lat),
                "lon": float(lon),
                "date": _to_date(key),
                "rainfall_mm": float(val) if val is not None else None,
                "flag": flag,
            }
        )
    return rows


def _to_date(yyyymmdd: str) -> date:
    return datetime.strptime(yyyymmdd, "%Y%m%d").date()


def _to_iso(yyyymmdd: str) -> str:
    return _to_date(yyyymmdd).isoformat()


def imd_rainfall_class(mm_per_day: float, heavy: float = 64.5, extremely_heavy: float = 115.6) -> str:
    """India Meteorological Department daily rainfall classification."""
    if mm_per_day < 2.5:
        return "very light"
    if mm_per_day < 15.6:
        return "light to moderate"
    if mm_per_day < 35.5:
        return "moderate"
    if mm_per_day < 64.5:
        return "rather heavy"
    if mm_per_day < extremely_heavy:
        return "heavy"
    return "extremely heavy"
