"""Tests for POWER payload parsing, validation and classification helpers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.power_io import imd_rainfall_class, parse_point_file, parse_point_payload
from tests.conftest import synthetic_power_payload


class TestParsePointFile:
    def test_valid_file_summary(self, tmp_path):
        p = tmp_path / "power_10.00_70.00.json"
        p.write_text(json.dumps(
            synthetic_power_payload(10.0, 70.0, {"20230101": 0.0, "20230102": 12.5, "20230103": -999.0})
        ))
        rep = parse_point_file(p)
        assert rep["valid"] is True
        assert rep["lat"] == 10.0 and rep["lon"] == 70.0
        assert rep["n_values"] == 3
        assert rep["min_date"] == "2023-01-01" and rep["max_date"] == "2023-01-03"
        assert rep["n_missing"] == 1

    def test_malformed_json_is_reported_not_raised(self, tmp_path):
        p = tmp_path / "power_broken.json"
        p.write_text("{definitely not json")
        rep = parse_point_file(p)
        assert rep["valid"] is False
        assert rep["error"] is not None

    def test_missing_parameter_key(self, tmp_path):
        p = tmp_path / "power_x.json"
        p.write_text(json.dumps({"geometry": {"coordinates": [70, 10, 0]},
                                 "properties": {"parameter": {}}}))
        rep = parse_point_file(p)
        assert rep["valid"] is False


class TestParsePointPayload:
    def test_flags_are_classified(self):
        payload = synthetic_power_payload(10.0, 70.0, {
            "20230101": 3.0,     # ok
            "20230102": -999.0,  # fill
            "20230103": None,    # null
            "20230104": -1.0,    # negative
        })
        rows = parse_point_payload(payload)
        flags = {r["date"].isoformat(): r["flag"] for r in rows}
        assert flags["2023-01-01"] == "ok"
        assert flags["2023-01-02"] == "missing_fill"
        assert flags["2023-01-03"] == "null"
        assert flags["2023-01-04"] == "negative"

    def test_values_and_coords(self):
        payload = synthetic_power_payload(-10.5, 70.25, {"20230101": 7.5})
        rows = parse_point_payload(payload)
        assert len(rows) == 1
        assert rows[0]["lat"] == -10.5 and rows[0]["lon"] == 70.25
        assert rows[0]["rainfall_mm"] == 7.5


class TestImdClass:
    def test_threshold_boundaries(self):
        assert imd_rainfall_class(0.0) == "very light"
        assert imd_rainfall_class(2.4) == "very light"
        assert imd_rainfall_class(15.5) == "light to moderate"
        assert imd_rainfall_class(64.4) == "rather heavy"
        assert imd_rainfall_class(64.5) == "heavy"
        assert imd_rainfall_class(115.6) == "extremely heavy"
        assert imd_rainfall_class(900.0) == "extremely heavy"
