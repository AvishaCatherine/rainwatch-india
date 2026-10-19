"""RainWatch India — district boundary handling and grid-to-district mapping.

Method (documented in docs/METHODOLOGY.md):
  1. Load district polygons (geoBoundaries IND ADM2 GeoJSON).
  2. Take the centre (centroid) of each NASA POWER grid cell.
  3. Spatially join cell centroids to districts: every grid cell is assigned
     to exactly the district that contains its centroid.
  4. District rainfall on a day = mean rainfall of the cells assigned to it.

This is a documented, reproducible areal-approximation. Grid cells are
~55 km wide, so small districts may contain zero or one cell centroid —
the output is called a *rainfall exposure indicator*, not an exact
district measurement. Rainfall alone does not predict flooding.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from src.config import load_config, resolve_path


def load_districts() -> gpd.GeoDataFrame:
    """Load ADM2 district polygons, keeping only the columns we need."""
    path = resolve_path("paths.raw_geo") / "india_adm2.geojson"
    if not path.exists():
        raise FileNotFoundError(
            f"District boundaries not found at {path}.\n"
            "Run scripts/01_download_data.py first (downloads geoBoundaries IND ADM2)."
        )
    gdf = gpd.read_file(path)
    keep = [c for c in ["shapeName", "shapeISO", "shapeID", "shapeGroup", "geometry"] if c in gdf.columns]
    gdf = gdf[keep].rename(columns={"shapeName": "district"})
    gdf["district"] = gdf["district"].astype(str).str.strip()
    return gdf


def load_states() -> gpd.GeoDataFrame:
    """Load ADM1 state polygons (optional; used to label grid cells with states)."""
    path = resolve_path("paths.raw_geo") / "india_adm1.geojson"
    if not path.exists():
        return None
    gdf = gpd.read_file(path)
    keep = [c for c in ["shapeName", "shapeID", "geometry"] if c in gdf.columns]
    gdf = gdf[keep].rename(columns={"shapeName": "state"})
    return gdf


def build_land_cells(cfg: dict | None = None) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    """Assign every grid point to a district (and state) by centroid-in-polygon.

    Returns:
      land_cells : DataFrame[lat, lon, state, district]  (points inside India)
      districts  : GeoDataFrame of district polygons
    """
    cfg = cfg or load_config()
    g = cfg["dataset"]["grid"]
    districts = load_districts()
    states = load_states()

    lats = _lattice(g["lat_min"], g["lat_max"], g["lat_step"])
    lons = _lattice(g["lon_min"], g["lon_max"], g["lon_step"])
    pts = pd.DataFrame([(la, lo) for la in lats for lo in lons], columns=["lat", "lon"])
    pts["geometry"] = [Point(lo, la) for lo, la in zip(pts["lon"], pts["lat"])]
    points_gdf = gpd.GeoDataFrame(pts, crs="EPSG:4326")

    joined = gpd.sjoin(points_gdf, districts[["district", "geometry"]], how="left", predicate="within")
    joined = joined.drop_duplicates(subset=["lat", "lon"])  # defensive for edge-on-boundary

    if states is not None:
        st = gpd.sjoin(points_gdf[["lat", "lon", "geometry"]], states[["state", "geometry"]], how="left", predicate="within")
        st = st.drop_duplicates(subset=["lat", "lon"])[["lat", "lon", "state"]]
        joined = joined.merge(st, on=["lat", "lon"], how="left")
    else:
        joined["state"] = None

    land = joined.dropna(subset=["district"])[["lat", "lon", "state", "district"]].copy()
    land["lat"] = land["lat"].round(2)
    land["lon"] = land["lon"].round(2)
    land = land.sort_values(["lat", "lon"]).reset_index(drop=True)
    return land, districts


def write_dashboard_geojson(districts: gpd.GeoDataFrame, out_dir: Path, tolerance: float = 0.01) -> Path:
    """Write a simplified district GeoJSON for the dashboard choropleth."""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "districts_simplified.geojson"
    simplified = districts[["district", "geometry"]].copy()
    simplified["geometry"] = simplified.geometry.simplify(tolerance, preserve_topology=True)
    simplified.to_file(out_path, driver="GeoJSON")
    return out_path


def _lattice(start: float, stop: float, step: float) -> list[float]:
    n = int(round((stop - start) / step)) + 1
    vals = [round(start + i * step, 2) for i in range(n)]
    if abs(vals[-1] - stop) > 1e-9:
        vals.append(round(stop, 2))
    return vals
