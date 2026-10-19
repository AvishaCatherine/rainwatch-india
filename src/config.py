"""RainWatch India — shared configuration loader."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Load config/config.yaml (cached)."""
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {cfg_path}\n"
            "Run the project from its root directory."
        )
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def resolve_path(key_path: str) -> Path:
    """Resolve a configured path key (e.g. 'paths.processed') to an absolute Path."""
    node: object = load_config()
    for part in key_path.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(f"Unknown config path: {key_path}")
        node = node[part]
    p = Path(str(node))
    return p if p.is_absolute() else (PROJECT_ROOT / p)


def ensure_dirs() -> None:
    """Create every configured directory if missing (idempotent)."""
    for key in ("paths.raw_power", "paths.raw_geo", "paths.processed", "paths.outputs"):
        resolve_path(key).mkdir(parents=True, exist_ok=True)


def grid_points(cfg: dict | None = None) -> list[tuple[float, float]]:
    """Return the (lat, lon) sampling lattice defined in config.yaml."""
    cfg = cfg or load_config()
    g = cfg["dataset"]["grid"]
    lats = _linspace(g["lat_min"], g["lat_max"], g["lat_step"])
    lons = _linspace(g["lon_min"], g["lon_max"], g["lon_step"])
    return [(round(la, 2), round(lo, 2)) for la in lats for lo in lons]


def _linspace(start: float, stop: float, step: float) -> list[float]:
    n = int(round((stop - start) / step)) + 1
    vals = [round(start + i * step, 2) for i in range(n)]
    if abs(vals[-1] - stop) > 1e-9:
        vals.append(round(stop, 2))
    return vals


def point_file_name(lat: float, lon: float) -> str:
    return f"power_{lat:.2f}_{lon:.2f}.json"


def point_file_path(lat: float, lon: float) -> Path:
    return resolve_path("paths.raw_power") / point_file_name(lat, lon)


def expected_env_note() -> str:
    return f"PYTHON={os.path.basename(os.sys.executable)}"  # noqa: SLF001
