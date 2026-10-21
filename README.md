# RainWatch India 🌧

**Historical Rainfall Analytics and District-Level Rainfall-Exposure Assessment Using Big Data**
*(Apache Spark · NASA POWER · Parquet · Streamlit)*

A  mini-project that processes ~15 million historical
daily rainfall observations for India with Apache Spark, analyses multi-year patterns, detects
statistically unusual wet/dry months, ranks district-level rainfall exposure, and presents
everything in a professional dark-themed interactive dashboard.

> **Disclaimer** — This is an *educational historical-analysis tool*. It is **not** an official
> flood-warning, disaster-forecasting or emergency-management service. Rainfall anomalies are
> statistical statements, not disaster declarations. Rainfall exposure is not flood risk.

---

## 1. What it does

| # | Capability | Engine |
|---|------------|--------|
| 1 | Ingests and validates 3,538 raw NASA POWER point files (2015 → 2026) | Python + Spark |
| 2 | Cleans 15,245,242 records (fill values, nulls, negatives) and derives calendar features | **PySpark** |
| 3 | Stores the analytical table as year-partitioned **Parquet** | Spark |
| 4 | Monthly / seasonal / annual aggregations, year-over-year changes | **PySpark** |
| 5 | Anomaly detection against a documented monthly climatology baseline (z-scores) | **PySpark** |
| 6 | Extreme-rainfall days with IMD thresholds (64.5 / 115.6 mm/day) | **PySpark** |
| 7 | Grid → district mapping (GeoPandas spatial join) and district exposure indicators | GeoPandas + **PySpark** |
| 8 | 7-page interactive dashboard (maps, rankings, trends, Big Data page) | Streamlit + Plotly |
| 9 | 35 automated tests + a fair Spark-vs-Pandas benchmark | pytest |

**Verified data coverage of this build (read from the data, never hardcoded):**
2015-01-01 → 2026-10-16 · 4,307 days · 12 calendar years · 15,238,166 valid observations
(0.046 % missing) · 1,171 land grid cells (~55 km resolution) · 573 districts · 33 states/UTs.

## 2. Problem statement

India's rainfall is dominated by a strong monsoon cycle with high spatial and temporal
variability. Understanding *where* and *when* heavy rainfall historically occurs, and how each
year departs from the long-term baseline, requires processing millions of gridded observations —
a classic small-scale Big Data problem: too large for naive spreadsheet handling, small enough
to demonstrate distributed processing end-to-end on one laptop.

## 3. Objectives

1. Process historical rainfall data **at scale with Apache Spark** (real transformations and
   aggregations — not Pandas in disguise).
2. Analyse rainfall patterns across **time (month, season, year)** and **geography (grid, district, state)**.
3. Identify unusually wet/dry periods with a **transparent, documented statistical baseline**.
4. Compare rainfall between years, months, and regions.
5. Estimate **district-level rainfall exposure** with a documented spatial method.
6. Present findings through an **interactive, professional dashboard**.
7. Demonstrate why distributed processing matters for large environmental datasets — including
   an *honest* benchmark.
8. Guarantee **reproducibility**: every number in the dashboard regenerates from the same source files.

## 4. Technology stack

| Layer | Technology | Role |
|-------|-----------|------|
| Processing | **Apache Spark 3.5 (PySpark)** | ingestion, cleaning, all aggregations |
| Storage | **Parquet** (Snappy, partitioned by year) + Hadoop-compatible local FS | columnar analytical store |
| Science I/O | xarray / netCDF4 | available for gridded scientific formats (NetCDF) |
| Geo | GeoPandas + geoBoundaries ADM2/ADM1 | grid→district spatial join |
| Dashboard | Streamlit + Plotly | 7-page dark-navy analytics app |
| Tests | pytest (35 tests, synthetic fixture) | CI-style verification |

The role of each technology in one sentence each (for the viva):
- **Spark** distributes the dataset across partitions and executes joins/group-bys/window
  functions in the JVM — the same code scales from `local[*]` to a cluster by changing one URL.
- **Parquet** stores data columnar + compressed so Spark reads only the columns and year
  partitions it needs (partition pruning).
- **Hadoop-compatible storage**: Spark's I/O layer is the Hadoop FileSystem API — running on the
  local filesystem here, but unchanged on HDFS/S3 in a cluster deployment.
- **Streamlit** turns the processed Parquet tables into an interactive web app in pure Python,
  re-rendering on every widget interaction.

## 5. Architecture

```
 NASA POWER API ──▶ scripts/01_download_data.py ──▶ data/raw/power/*.json (3,538 files, ~155 MB)
                                                      │
                                    scripts/02_run_pipeline.py
                                                      │
              ┌───────────────────────────────────────┤
              ▼                                       ▼
 [1] validate (pure Python)              [2] ingest (Spark JSON reader, typed schema)
              │                                       │
              │                          [3] clean + calendar features (Spark)
              │                                       │
 [5] GeoPandas grid→district  ◀─────────  [4] raw Parquet (partitioned by year, 46 MB)
              │                                       │
              └───────────────┬───────────────────────┘
                              ▼
              [6] Spark aggregations → 13 Parquet outputs (data/processed/)
                              ▼
       outputs/pipeline_manifest.json  (stats from the actual run)
                              ▼
        Streamlit dashboard (reads Parquet only — opens in seconds, fully offline)
```

Details: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · Methods: [`docs/METHODOLOGY.md`](docs/METHODOLOGY.md)
· Data provenance: [`docs/DATA_SOURCES.md`](docs/DATA_SOURCES.md) · Windows setup: [`docs/SETUP_WINDOWS.md`](docs/SETUP_WINDOWS.md)

## 6. Dataset

- **Source**: NASA POWER Daily API — `PRECTOTCORR` (bias-corrected precipitation, **mm/day**),
  MERRA-2 assimilation, native ~0.5°×0.625° grid. Open access, no credentials.
  https://power.larc.nasa.gov/
- **Sampling**: 0.5°×0.5° lattice, 3,538 points covering India (68.25–96.75°E, 6.75–36.75°N).
- **Coverage in this build**: 2015-01-01 → 2026-10-16 (verified from the files themselves).
- **District boundaries**: geoBoundaries `gbOpen IND ADM2/ADM1` (open licence).
- **Why not GPM IMERG Final?** IMERG Final requires NASA Earthdata credentials and hundreds of GB
  per decade. Per the project's substitution rule (§ "If NASA access is unavailable"), the open
  NASA POWER product was adopted and documented. Full story in `docs/DATA_SOURCES.md`.

## 7. Project layout

```
rainwatch_india/
├── config/config.yaml           # all tunables (paths, grid, thresholds, Spark)
├── data/raw/                    # downloaded source files (not shipped in the ZIP)
├── data/processed/              # Spark outputs: raw_rainfall/ + 13 analytics tables
├── src/
│   ├── config.py                # config loader, lattice generator
│   ├── build_spark.py           # SparkSession factory (8 GB-laptop sized)
│   ├── power_io.py              # POWER payload parsing/validation helpers
│   ├── pipeline.py              # 7-stage pipeline orchestration + manifest
│   ├── analytics.py             # ALL Spark aggregations
│   └── districts.py             # GeoPandas grid→district mapping
├── scripts/
│   ├── 00_verify_spark.py       # ← run FIRST: Spark+Parquet gate test
│   ├── 01_download_data.py      # one-time download (resumable, no credentials)
│   ├── 02_run_pipeline.py       # the Spark pipeline
│   └── 03_run_benchmark.py      # fair Spark vs Pandas comparison
├── dashboard/
│   ├── app.py                   # Page 1 · Overview (entry point)
│   ├── theme.py / data_loader.py
│   └── pages/  2_Explorer · 3_Map · 4_Districts · 5_Anomalies · 6_BigData · 7_About
├── tests/                       # 35 pytest tests (synthetic fixture, offline)
├── docs/                        # ARCHITECTURE · DATA_SOURCES · METHODOLOGY · SETUP_WINDOWS
└── outputs/                     # pipeline_manifest.json · benchmark_results.json
```

## 8. Installation

**Recommended on Windows: WSL Ubuntu** (avoids the winutils problem entirely) — full walk-through
in `docs/SETUP_WINDOWS.md`. On Linux/macOS or inside WSL:

```bash
sudo apt update && sudo apt install -y python3-venv openjdk-17-jre-headless
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Native Windows alternative**: JDK 17 + winutils.exe 3.3.x + `HADOOP_HOME` — step-by-step in
`docs/SETUP_WINDOWS.md` Option B.

## 9. Get the data (one-time, then fully offline)

```bash
python scripts/01_download_data.py
```
Downloads 3,538 NASA POWER point JSONs (~155 MB, ~7 min) + district GeoJSONs. Resumable: re-run
skips finished points. No credentials, no tokens, nothing secret.

## 10. Run the pipeline

```bash
python scripts/02_run_pipeline.py            # full run, ~3-4 min on 2 cores / 8 GB
python scripts/02_run_pipeline.py --reuse-raw   # restartable: reuse the raw Parquet
```
Writes `data/processed/raw_rainfall/` (partitioned by year) + 13 analytics tables +
`outputs/pipeline_manifest.json` (file counts, record counts, missing %, durations, Spark info,
schema — all from the actual run).

## 11. Start the dashboard

```bash
streamlit run dashboard/app.py
```
Opens http://localhost:8501 — Page 1 Overview, then Explorer / Map / Districts / Anomalies /
Big Data / About in the sidebar. The dashboard reads only the processed Parquet files, so it
starts in seconds **without** re-running Spark.

## 12. Run the tests

```bash
pytest -q            # 35 tests, ~25 s, fully offline (synthetic fixture only)
```

## 13. Expected output

- Pipeline console: per-stage timings + final line
  `Pipeline complete ... | coverage 2015-01-01 .. 2026-10-16 | 4307 days | 12 years`
- `outputs/pipeline_manifest.json` — every statistic the Big Data page shows.
- Dashboard: navy-themed, 7 pages, all KPIs computed from the processed tables.
- Benchmark (optional): `outputs/benchmark/benchmark_results.json` — on this laptop Pandas won
  (1.3 s vs 3.8 s on 15.2 M rows, local mode). Reported honestly, with limitations; see page 6.

## 14. Limitations & future enhancements

**Limitations**
- ~55 km reanalysis grid → area averages, not point measurements; small districts (< one cell)
  are absent from district outputs.
- Single-cell daily maxima in MERRA-2 can be noisy; district averages are the robust view.
- Anomaly baseline = full ~12-year record, not the WMO 30-year standard.
- 2026 is a partial year (data through the actual last observation, 2026-10-16).
- Rainfall exposure ≠ flood risk: no hydrology, damage records or impact labels are used.
- Spark runs in local mode on one machine — the *programming model* is distributed; the
  benchmark honestly shows Pandas can win at this scale on one box.

**Future enhancements**
- Longer record (1958→present) and a 30-year WMO climatology baseline.
- CHIRPS/IMERG integration via Earthdata credentials for finer resolution.
- Area-weighted (areal-intersection) district aggregation instead of centroid assignment.
- Spark on a real cluster / cloud object storage for genuinely bigger data.
- Statistical trend tests (Mann-Kendall) with significance reporting.

---


