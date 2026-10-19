# RainWatch India — Methodology

All methods below run in PySpark on the full 15.24 M-row analytical table unless stated
otherwise. Thresholds and switches live in `config/config.yaml`.

## 1. Calendar features

Derived once at ingest (`src/analytics.with_calendar`):

- `year`, `month`, `day`, `doy` from `date`
- **IMD climate seasons**:
  - Winter (Jan–Feb) · Pre-monsoon (Mar–May) · SW Monsoon (Jun–Sep) · Post-monsoon (Oct–Dec)
- `month_name`, `is_monsoon` (Jun–Sep flag)

## 2. Areal aggregation convention

The dataset is a lattice of point values. Unless stated otherwise, every "India" aggregate is
the **land-cell average**: for each day, the mean over the 1,171 grid cells whose centroid lies
inside an Indian district polygon. Monthly/seasonal/annual totals are the sums of those daily
land-averages — i.e. an *areal-average depth in mm*, not a volume.

## 3. Trends (statistical, not causal)

- Monthly totals per year; annual totals; season × year totals; year-over-year differences
  (Δ of annual totals).
- The dashboard labels these as statistical patterns in this dataset. **No causal attribution**
  (e.g. to climate change) is made anywhere in the project.

## 4. Rainfall anomaly detection (transparent baseline)

1. **Baseline**: for each calendar month m, the long-term monthly mean `μ_m` and standard
   deviation `σ_m` of land-average monthly totals over the **full available record**
   (2015–2026, 12 Junes, 12 Julys, …).
2. **z-score**: `z = (total − μ_m) / σ_m`.
3. **Anomaly %**: `(total − μ_m) / μ_m × 100`.
4. **Thresholds** (config): *Unusually wet* if `z ≥ +1.5`; *Unusually dry* if `z ≤ −1.5`;
   otherwise *Normal*. Months with zero σ yield null z and are reported as Normal (no crash).

Documented limitation: the WMO standard baseline is 30 years; ours is 12. A longer record would
stabilise μ and σ. The baseline period is displayed on the Anomalies page and in the manifest.

**A statistical anomaly is not a disaster declaration.** The dashboard states this on-page.

## 5. Extreme-rainfall analysis

Thresholds follow the **India Meteorological Department** daily classification:

| Class | mm/day |
|-------|--------|
| rather heavy | 35.6–64.4 |
| heavy | 64.5–115.5 |
| extremely heavy | ≥ 115.6 |

For each day we compute: max grid-cell value, land mean, counts of cells ≥ 64.5 and ≥ 115.6,
and the **top 5 districts by area-averaged rainfall** that day (from the district mapping).
The top 200 days form the extremes table. The dashboard shows single-cell maxima *and* district
averages side by side because reanalysis single-cell maxima can be noisy. No historical event is
invented and no official disaster status is claimed.

## 6. District mapping (documented spatial method)

1. Grid-cell centroids (2,342 lattice land/ocean points → 1,171 land) are spatially joined
   (point-in-polygon, GeoPandas `sjoin(..., predicate="within")`) to geoBoundaries ADM2
   district polygons; state comes from ADM1.
2. **District daily rainfall = mean of the cells assigned to that district** (unweighted).
3. District monthly totals = sum of daily district values.

Consequences: districts smaller than ~55 km may contain zero centroids (absent from outputs —
585 (state, district) pairs have data of 735 polygons); boundary cell assignment is deterministic
(a centroid belongs to exactly one polygon). This is an **approximation**, documented wherever
district numbers appear.

## 7. Rainfall exposure indicators (NOT flood risk)

Per district, over the full record:

| Indicator | Definition |
|-----------|-----------|
| mean_annual_mm | mean of yearly district totals |
| cv_annual_pct | inter-annual coefficient of variation |
| heavy_days_per_year | days with district mean ≥ 64.5 mm, averaged per year |
| monsoon_share_pct | share of annual rainfall in Jun–Sep |
| wettest/driest_year | argmax/argmin of yearly totals |
| **exposure_index** | 0–100 = mean of two percentile ranks (mean_annual_mm, heavy_days_per_year) within this dataset |

The index ranks **rainfall exposure only**. No damage records, hydrological model or impact
labels are used; rainfall alone does not predict flooding. All dashboard wording follows this.

## 8. Fair benchmark method (Spark vs Pandas)

- Same machine, same run, same raw Parquet, same operation:
  `groupBy(year, month).agg(sum, mean)` over 15,238,166 rows.
- One warm-up run per engine discarded; reported value = mean of 2 measured runs.
- Spark writes results through the **Noop sink** (forces full computation without disk I/O);
  Pandas result is fully materialised and reduced.
- Result on the build machine: Pandas 1.28 s vs Spark 3.8 s (2-core local mode) — Pandas wins
  at this scale, and the project says so. Spark's justification is horizontal scale-out and
  memory headroom, not single-box speed at 15 M rows.

## 9. Data quality rules

- POWER fill value −999.0 → `missing_fill`, dropped.
- Null values → `null`, dropped. Negative rainfall → `negative`, dropped (rainfall is ≥ 0).
- 7,076 of 15,245,242 values (0.046 %) were dropped as fill values; the rest passed.
- Malformed raw files are counted (0 in this build) in the validation stage and would surface in
  the manifest's `validation.invalid_examples`.
- The pipeline never substitutes synthetic values for failed processing — if processing fails,
  the dashboard shows a setup error instead of fake numbers.
