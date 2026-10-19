# RainWatch India — Architecture

## System overview

RainWatch India is a batch analytical pipeline plus a read-only dashboard. There is **no live
component by design**: every observation is downloaded once, processed once, and every
subsequent view reads precomputed Parquet.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SOURCES (one-time, internet)                                               │
│    NASA POWER Daily Point API  ──▶  data/raw/power/power_*.json  (3,538)    │
│    geoBoundaries ADM2/ADM1     ──▶  data/raw/geo/india_adm{1,2}.geojson     │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │  scripts/01_download_data.py (resumable)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PIPELINE  scripts/02_run_pipeline.py  →  src/pipeline.py                   │
│                                                                             │
│  1 validate   pure Python: parse every raw file, count values/missing,      │
│               detect malformed files              (6 s / 3,538 files)       │
│  2 ingest     Spark JSON reader with an explicit schema; explode the        │
│               parameter map → (lat, lon, date, rainfall_mm, flag)           │
│  3 clean      keep flag='ok' only (drops -999 fills, nulls, negatives);     │
│               derive year/month/day/season/month_name/is_monsoon            │
│  4 store      data/processed/raw_rainfall/ — Parquet, snappy, partitioned   │
│               by year (46 MB for 15.24 M rows)                              │
│  5 geography  GeoPandas: grid-cell centroids ⋈ district polygons →          │
│               land_cells table (lat, lon, state, district); dashboard       │
│               gets a simplified district GeoJSON                            │
│  6 analytics  Spark aggregations (src/analytics.py) → 13 Parquet tables     │
│  7 manifest   outputs/pipeline_manifest.json — every stat the dashboard     │
│               shows, measured from THIS run                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  DASHBOARD  streamlit run dashboard/app.py   (reads Parquet only)           │
│    1 Overview · 2 Explorer · 3 Map · 4 Districts · 5 Anomalies ·            │
│    6 Big Data · 7 About/Methodology                                         │
│    Streamlit cache (st.cache_data) keeps page loads fast and offline        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Module descriptions

| Module | Responsibility |
|--------|----------------|
| `src/config.py` | Loads `config/config.yaml`; resolves paths; generates the 0.5° sampling lattice; `ensure_dirs()` |
| `src/build_spark.py` | Single SparkSession factory: `local[*]`, 2 g driver, 16 shuffle partitions, quiet logs, Asia/Kolkata session TZ |
| `src/power_io.py` | Pure-Python POWER payload parsing (shared by validation stage + tests); fill-value rules; IMD rainfall classes |
| `src/pipeline.py` | Orchestrates stages 1-7; writes every output and the run manifest; `--reuse-raw` restartability |
| `src/analytics.py` | The Big Data core: `india_daily`, `monthly_summary`, `annual_summary`, `seasonal_summary`, `climatology_and_anomalies`, `extremes_days`, `district_daily/monthly/stats`, `grid_monthly`, `grid_cell_stats`, `rainfall_histogram` |
| `src/districts.py` | Loads boundaries, lattice generation, point-in-polygon spatial join, dashboard GeoJSON simplification |
| `dashboard/data_loader.py` | Cached Parquet readers + friendly "run the pipeline first" guard |
| `dashboard/theme.py` | Navy/blue design system: CSS, KPI cards, Plotly template `rainwatch` |
| `scripts/00_verify_spark.py` | The environment gate test (session → DF → Parquet → read → validate) with winutils/WSL troubleshooting hints |

## Data flow contract

- Raw JSON keys: `YYYYMMDD` strings; values mm/day; `-999.0` = fill.
- Clean table schema: `date DATE, lat DOUBLE, lon DOUBLE, rainfall_mm DOUBLE, flag STRING,
  year INT, month INT, day INT, doy INT, season STRING, month_name STRING, is_monsoon INT`.
- Land mask: broadcast-joined on `(lat, lon)` rounded to 2 decimals — exact same lattice values
  on both sides.
- All downstream tables are written by Spark as single-file (coalesced) Parquet unless marked
  otherwise; the manifest records row counts and sizes for each.

## Why Spark, where it matters

- **Ingest & clean** of 15.24 M records through a typed JSON reader with distributed parsing.
- **Broadcast joins** of the 1,171-row land mask against 15 M rows (avoids shuffling the big side).
- **Shuffle-heavy aggregations** (district×date over 2.5 M rows, window ranks for district stats).
- **Partition pruning**: the raw store is partitioned by year so future partial reprocessing
  reads only the needed years.
- Everything downstream of `write.parquet` is decoupled: the dashboard and tests never touch
  the raw files.
