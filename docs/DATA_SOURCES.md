# RainWatch India — Data Sources & Coverage

## Primary dataset (used in this build)

**NASA POWER Daily (Point) API** — parameter `PRECTOTCORR`, community `AG`.

| Property | Value |
|----------|-------|
| Variable | Precipitation, bias-corrected (`PRECTOTCORR`) |
| Units | mm/day |
| Native grid | MERRA-2 assimilation, ~0.5° × 0.625° (satellite-derived meteorology reanalysis) |
| Requested grid | 0.5° × 0.5° lattice — 3,538 points, lat 6.75–36.75°N, lon 68.25–96.75°E |
| Access | Open HTTPS API, **no credentials** |
| Citation | NASA Langley Research Center. *POWER Daily API*, https://power.larc.nasa.gov/ (retrieved 2026-10-19) |
| Documentation | https://power.larc.nasa.gov/docs/methodology/meteorology/precipitation/ |

### Actual coverage of the files in this build (measured, not assumed)

| Metric | Value |
|--------|-------|
| Earliest observation | **2015-01-01** |
| Latest observation | **2026-10-16** (some points report through 2026-10-18; the last two days contain fill values for all points and are excluded by cleaning) |
| Days with valid data | 4,307 |
| Calendar years | 12 (2015 … 2026; 2026 partial) |
| Raw files | 3,538 JSON point files (~155 MB) |
| Records read | 15,245,242 |
| Valid after cleaning | 15,238,166 |
| Missing (fill −999) | 7,076 (0.046 %) |
| Null / negative | 0 / 0 |
| Land cells (inside a district polygon) | 1,171 |
| Districts with data | 573 of 735 polygons (small districts lack a cell centroid) |
| States/UTs with data | 33 |

The latest date is **computed from the files** at pipeline time (both in the validation stage and
in the manifest `coverage` block); nothing is hardcoded.

## Why NASA POWER instead of GPM IMERG Final (substitution history)

The project brief preferred **NASA GPM IMERG Final Run** (GES DISC). That path was evaluated and
set aside for documented, practical reasons:

1. **Authentication**: GES DISC requires a NASA Earthdata login plus a generated token; the brief
   forbids embedding credentials and requires the app to run offline without them.
2. **Volume**: IMERG Final daily 0.1° files are ~15–30 MB/day — hundreds of GB per decade — far
   beyond a 9 GB sandbox disk and an 8 GB laptop in the project window.
3. **Fallback rule applied**: the brief explicitly allows substituting "another legitimate public
   historical precipitation dataset" when NASA's preferred product is inaccessible. NASA POWER is
   itself an official NASA (Langley) product, is credential-free, and provides daily
   precipitation with multi-decade coverage — chosen over IMD gridded .grd files (URL endpoints
   returned 404 at build time) and CHIRPS (anonymous but ~GB-scale per year at daily resolution).

Consequences (honesty box):
- POWER precipitation is **reanalysis/assimilation-based** (MERRA-2, bias-corrected), not the
  merged satellite-gauge IMERG product; expect smoothed spatial detail vs gauge data.
- Effective resolution ~0.5° (~55 km) — documented throughout the dashboard.
- Values are **area averages**, not gauge measurements; single-cell daily maxima can be noisy.

## Auxiliary geodata

| Dataset | Role | Licence |
|---------|------|---------|
| geoBoundaries `gbOpen IND ADM2` (simplified) | 735 district polygons for spatial join + choropleth | Open (CC-BY 4.0 / ODbL-style attribution) |
| geoBoundaries `gbOpen IND ADM1` (simplified) | 36 state/UT polygons to label grid cells | same |

Source: https://www.geoboundaries.org/ (downloaded 2026-10-19). Fetched via the GitHub LFS media
endpoints; the downloader validates that it received real GeoJSON, not an LFS pointer.

## Test fixture

`tests/conftest.py` contains **synthetic** POWER-shaped payloads (clearly labelled
"SYNTHETIC TEST PAYLOAD — not real observations") used only by the pytest suite. They never
enter the analytics pipeline and are never rendered as real data anywhere.

## Reproducibility

Re-running `scripts/01_download_data.py` today re-fetches the same historical window
(`config.dataset.start_date` → `end_date`) with skip-if-exists semantics; re-running the
pipeline regenerates every number shown in the dashboard from those files. The pipeline manifest
records the exact file count, record count, missing-value count and coverage dates of each run.
