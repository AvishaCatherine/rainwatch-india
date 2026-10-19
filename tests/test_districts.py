"""District mapping tests with a tiny synthetic boundary file (SYNTHETIC geometry)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import districts


TWO_SQUARES_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"shapeName": "North District", "shapeID": "D1", "shapeGroup": "IND", "shapeType": "ADM2"},
            "geometry": {"type": "Polygon", "coordinates": [[
                [69.5, 9.5], [71.0, 9.5], [71.0, 10.25], [69.5, 10.25], [69.5, 9.5]
            ]]},
        },
        {
            "type": "Feature",
            "properties": {"shapeName": "South District", "shapeID": "D2", "shapeGroup": "IND", "shapeType": "ADM2"},
            "geometry": {"type": "Polygon", "coordinates": [[
                [69.5, 10.25], [71.0, 10.25], [71.0, 11.5], [69.5, 11.5], [69.5, 10.25]
            ]]},
        },
    ],
}


@pytest.fixture()
def geo_env(tmp_path, monkeypatch):
    geo_dir = tmp_path / "geo"
    geo_dir.mkdir()
    (tmp_path / "india_adm2.geojson").write_text(json.dumps(TWO_SQUARES_GEOJSON))
    monkeypatch.setattr("src.districts.resolve_path", lambda key: tmp_path)
    monkeypatch.setattr(
        "src.config.resolve_path", lambda key: tmp_path if key.startswith("paths") else __import__("src.config", fromlist=["x"]).PROJECT_ROOT
    )
    # Lattice points chosen to avoid the exact shared boundary (lat 10.25).
    cfg = {
        "dataset": {"grid": {"lat_min": 9.6, "lat_max": 11.1, "lat_step": 0.5,
                             "lon_min": 70.0, "lon_max": 70.5, "lon_step": 0.5}},
        "districts": {"simplify_tolerance": 0.01},
    }
    return tmp_path, cfg, geo_dir


class TestDistrictMapping:
    def test_cells_assigned_to_correct_district(self, geo_env):
        tmp_path, cfg, geo_dir = geo_env
        land, gdf = districts.build_land_cells(cfg)
        # Lattice: lats 9.6/10.1/10.6/11.1 x lons 70.0/70.5 -> 8 cells, all on land.
        # North square covers lat 9.5-10.25; South square covers 10.25-11.5.
        assert len(land) == 8
        north = sorted(land[land["district"] == "North District"]["lat"].unique())
        south = sorted(land[land["district"] == "South District"]["lat"].unique())
        assert north == [9.6, 10.1]
        assert south == [10.6, 11.1]
        # every land cell belongs to exactly one district
        assert land["district"].notna().all()
        assert len(land.drop_duplicates(["lat", "lon"])) == 8

    def test_dashboard_geojson_written(self, geo_env):
        tmp_path, cfg, geo_dir = geo_env
        land, gdf = districts.build_land_cells(cfg)
        out = districts.write_dashboard_geojson(gdf, geo_dir, tolerance=0.01)
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 2

    def test_missing_boundary_file_raises_helpful_error(self, tmp_path, monkeypatch):
        empty = tmp_path / "no_geo"
        empty.mkdir()
        monkeypatch.setattr("src.districts.resolve_path", lambda key: empty)
        with pytest.raises(FileNotFoundError, match="01_download_data"):
            districts.load_districts()
