#!/usr/bin/env python3
"""
RainWatch India — Step 01: download historical data (one-time setup).

Downloads, once and locally:
  1. NASA POWER daily precipitation (PRECTOTCORR, mm/day) for a regular
     lattice of points covering India, from config.dataset.start_date to
     config.dataset.end_date. The response is stored as one small JSON file
     per point, so the download is RESUMABLE — running this script again
     skips points already downloaded.
  2. District boundaries (geoBoundaries IND ADM2, simplified GeoJSON).

No credentials are required; no secrets are stored. After this script
completes, the rest of the project runs fully offline.

Run:  python scripts/01_download_data.py
"""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (  # noqa: E402
    ensure_dirs,
    grid_points,
    load_config,
    point_file_name,
    resolve_path,
)

POWER_URL = (
    "https://power.larc.nasa.gov/api/temporal/daily/point"
    "?parameters={param}&community=AG&latitude={lat}&longitude={lon}"
    "&start={start}&end={end}&format=JSON"
)


def _is_valid_point_file(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 100:
        return False
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        param = data["properties"]["parameter"]
        return any(len(v) > 0 for v in param.values())
    except (json.JSONDecodeError, KeyError, TypeError):
        return False


def fetch_point(
    lat: float,
    lon: float,
    param: str,
    start: str,
    end: str,
    out_dir: Path,
    timeout: int,
    max_retries: int,
) -> tuple[str, str]:
    """Download one grid point; returns (status, name). status in {ok, skip, fail}."""
    name = point_file_name(lat, lon)
    out_path = out_dir / name
    if _is_valid_point_file(out_path):
        return "skip", name

    url = POWER_URL.format(param=param, lat=lat, lon=lon, start=start, end=end)
    last_err = ""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": "RainWatchIndia/1.0 (academic project)"})
            if resp.status_code == 200:
                data = resp.json()
                param_block = data.get("properties", {}).get("parameter", {})
                if not param_block.get(param):
                    last_err = "empty parameter block"
                else:
                    with out_path.open("w", encoding="utf-8") as fh:
                        json.dump(data, fh, separators=(",", ":"))
                    return "ok", name
            elif resp.status_code == 429:
                last_err = "HTTP 429 rate-limited"
            else:
                last_err = f"HTTP {resp.status_code}"
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = f"{type(exc).__name__}"
        time.sleep(min(2 ** attempt, 30))  # exponential backoff, capped
    return "fail", f"{name}: {last_err}"


def download_power(cfg: dict) -> dict:
    ds = cfg["dataset"]
    dl = cfg["download"]
    out_dir = resolve_path("paths.raw_power")
    out_dir.mkdir(parents=True, exist_ok=True)

    start = ds["start_date"].replace("-", "")
    end = ds["end_date"].replace("-", "")
    param = ds["parameter"]
    points = grid_points(cfg)
    print(f"[power] {len(points)} grid points | param={param} | {start}..{end}")

    counts = {"ok": 0, "skip": 0, "fail": 0}
    failures: list[str] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=dl["concurrency"]) as pool:
        futures = {
            pool.submit(
                fetch_point,
                lat,
                lon,
                param,
                start,
                end,
                out_dir,
                dl["timeout_seconds"],
                dl["max_retries"],
            ): (lat, lon)
            for (lat, lon) in points
        }
        done = 0
        for fut in as_completed(futures):
            status, name = fut.result()
            counts[status] += 1
            done += 1
            if status == "fail":
                failures.append(name)
            if done % 250 == 0 or done == len(points):
                elapsed = time.time() - t0
                print(
                    f"[power] {done}/{len(points)} done in {elapsed:.0f}s "
                    f"(ok={counts['ok']} skip={counts['skip']} fail={counts['fail']})",
                    flush=True,
                )

    summary = {
        "source": "NASA POWER Daily Point API (MERRA-2, PRECTOTCORR mm/day)",
        "url_base": POWER_URL.split("?")[0],
        "parameter": param,
        "requested_start": ds["start_date"],
        "requested_end": ds["end_date"],
        "grid_points_total": len(points),
        "files_ok": counts["ok"],
        "files_skipped_existing": counts["skip"],
        "files_failed": counts["fail"],
        "failures": failures[:50],
        "elapsed_seconds": round(time.time() - t0, 1),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    with (out_dir / "_download_summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    if counts["fail"]:
        print(f"[power] {counts['fail']} point(s) FAILED — re-run this script to retry.")
    return summary


def _download_geo(url: str, out_path: Path, label: str) -> dict:
    """Download one GeoJSON and sanity-check that it is real JSON (not an LFS pointer)."""
    print(f"[geo] downloading {label} ...")
    resp = requests.get(url, timeout=180, headers={"User-Agent": "RainWatchIndia/1.0"})
    resp.raise_for_status()
    content = resp.content
    if content[:200].lstrip().startswith(b"version https://git-lfs") or len(content) < 500_000:
        raise RuntimeError(
            f"{label}: downloaded content is not a valid GeoJSON "
            f"({len(content)} bytes). URL may point to a Git LFS pointer."
        )
    out_path.write_bytes(content)
    size_mb = out_path.stat().st_size / 1e6
    print(f"[geo] saved {out_path.name} ({size_mb:.1f} MB)")
    return {"status": "ok", "file": str(out_path), "size_mb": round(size_mb, 1)}


def download_districts(cfg: dict) -> dict:
    out_dir = resolve_path("paths.raw_geo")
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    adm2_path = out_dir / "india_adm2.geojson"
    if adm2_path.exists() and adm2_path.stat().st_size > 500_000:
        print(f"[geo] district boundaries already present: {adm2_path.name}")
        results["districts"] = {"status": "skipped", "file": str(adm2_path)}
    else:
        results["districts"] = _download_geo(cfg["districts"]["download_url"], adm2_path, "districts (ADM2)")

    adm1_path = out_dir / "india_adm1.geojson"
    states_url = cfg["districts"].get("states_download_url")
    if states_url:
        if adm1_path.exists() and adm1_path.stat().st_size > 500_000:
            print(f"[geo] state boundaries already present: {adm1_path.name}")
            results["states"] = {"status": "skipped", "file": str(adm1_path)}
        else:
            try:
                results["states"] = _download_geo(states_url, adm1_path, "states (ADM1)")
            except Exception as exc:  # noqa: BLE001 — states are optional
                print(f"[geo] WARNING: state boundaries failed ({exc}); districts still usable.")
                results["states"] = {"status": "failed", "error": str(exc)}
    return results


def main() -> int:
    ensure_dirs()
    cfg = load_config()
    print("=" * 62)
    print("RainWatch India :: data download (one-time, then offline)")
    print("=" * 62)

    power_summary = download_power(cfg)
    try:
        geo_summary = download_districts(cfg)
    except Exception as exc:  # noqa: BLE001
        print(f"[geo] WARNING: district boundary download failed: {exc}")
        print("      District-level analytics will be unavailable;")
        print("      grid-level analytics still work. Re-run to retry.")
        geo_summary = {"districts": {"status": "failed", "error": str(exc)}}

    failed = power_summary["files_failed"]
    geo_status = geo_summary.get("districts", {}).get("status", "unknown")
    print("\nDownload finished.")
    print(f"  power points: ok={power_summary['files_ok']} "
          f"skip={power_summary['files_skipped_existing']} fail={failed}")
    print(f"  districts:    {geo_status}")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
