"""RainWatch India — main data pipeline.

Stages
  1. validate   : inspect every raw POWER point file (python, I/O bound)
  2. ingest     : Spark reads all raw JSON files -> typed DataFrame
  3. clean      : handle fill values / nulls / negatives, derive calendar fields
  4. raw parquet: write the analytical table to data/processed/raw_rainfall (by year)
  5. geography  : grid cells -> districts/states (GeoPandas), dashboard GeoJSON
  6. analytics  : Spark aggregations -> one Parquet file per dashboard topic
  7. manifest   : processing statistics from the ACTUAL run -> outputs/pipeline_manifest.json

The pipeline is restartable: raw point files are only downloaded once, and
`--reuse-raw` skips re-ingesting the raw Parquet if it already exists.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src import analytics
from src.build_spark import get_spark
from src.config import grid_points, load_config, resolve_path
from src.districts import build_land_cells, write_dashboard_geojson
from src.power_io import parse_point_file

RAW_DIR_NAME = "raw_rainfall"


# ---------------------------------------------------------------- stage 1
def validate_raw(cfg: dict) -> dict:
    """Inspect every raw point file with the shared pure-Python parser."""
    raw_dir = resolve_path("paths.raw_power")
    parameter = cfg["dataset"]["parameter"]
    files = sorted(raw_dir.glob("power_*.json"))
    reports, bad = [], []
    t0 = time.perf_counter()
    for p in files:
        rep = parse_point_file(p, parameter)
        reports.append(rep)
        if not rep["valid"]:
            bad.append(rep)

    n = len(reports)
    n_values = sum(r["n_values"] for r in reports)
    n_missing = sum(r["n_missing"] for r in reports)
    n_null = sum(r["n_null"] for r in reports)
    n_negative = sum(r["n_negative"] for r in reports)
    min_dates = [r["min_date"] for r in reports if r["min_date"]]
    max_dates = [r["max_date"] for r in reports if r["max_date"]]

    summary = {
        "n_files": n,
        "n_valid_files": sum(1 for r in reports if r["valid"]),
        "n_invalid_files": len(bad),
        "invalid_examples": [b["file"] + ": " + str(b["error"]) for b in bad[:10]],
        "n_values_all_files": n_values,
        "n_missing_fill": n_missing,
        "n_null": n_null,
        "n_negative": n_negative,
        "missing_pct": round((n_missing + n_null) / n_values * 100, 3) if n_values else None,
        "min_date_any": min(min_dates) if min_dates else None,
        "max_date_any": max(max_dates) if max_dates else None,
        "elapsed_s": round(time.perf_counter() - t0, 1),
    }
    return summary


# ---------------------------------------------------------------- stage 2-3
def power_schema(parameter: str) -> StructType:
    return StructType(
        [
            StructField(
                "geometry",
                StructType([StructField("coordinates", ArrayType(DoubleType()), True)]),
                True,
            ),
            StructField(
                "properties",
                StructType(
                    [
                        StructField(
                            "parameter",
                            StructType(
                                [StructField(parameter, MapType(StringType(), DoubleType()), True)]
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
        ]
    )


def ingest_clean(spark: SparkSession, cfg: dict) -> tuple[DataFrame, dict]:
    """Read all raw POWER JSON files with Spark; clean and derive calendar fields."""
    raw_dir = resolve_path("paths.raw_power")
    parameter = cfg["dataset"]["parameter"]

    schema = power_schema(parameter)
    typed = (
        spark.read.schema(schema)
        .json(str(raw_dir / "power_*.json"))
        .select(
            F.round(F.col("geometry.coordinates")[1], 2).alias("lat"),
            F.round(F.col("geometry.coordinates")[0], 2).alias("lon"),
            F.explode(F.col(f"properties.parameter.{parameter}")).alias("date_str", "rainfall_mm"),
        )
        .withColumn("date", F.to_date(F.col("date_str"), "yyyyMMdd"))
        .withColumn(
            "flag",
            F.when(F.col("rainfall_mm").isNull(), "null")
            .when(F.col("rainfall_mm") == -999.0, "missing_fill")
            .when(F.col("rainfall_mm") < 0, "negative")
            .otherwise("ok"),
        )
    )

    flags = {row["flag"]: row["count"] for row in typed.groupBy("flag").count().collect()}
    stats = {
        "records_read": sum(flags.values()),
        "flag_counts": flags,
        "missing_fill": flags.get("missing_fill", 0),
        "null": flags.get("null", 0),
        "negative": flags.get("negative", 0),
        "valid_records": flags.get("ok", 0),
    }
    clean = typed.filter(F.col("flag") == "ok").drop("date_str")
    clean = analytics.with_calendar(clean)
    return clean, stats


# ---------------------------------------------------------------- stage 5
def build_geography(spark: SparkSession, cfg: dict) -> tuple[DataFrame, dict]:
    """Grid -> district/state assignment (GeoPandas) + dashboard GeoJSON."""
    land_pdf, districts_gdf = build_land_cells(cfg)
    processed = resolve_path("paths.processed")
    geo_dir = processed / "geo"

    tol = cfg["districts"].get("simplify_tolerance", 0.01)
    geojson_path = write_dashboard_geojson(districts_gdf, geo_dir, tol)

    land_pdf.to_parquet(geo_dir / "land_cells.parquet", index=False)
    land = spark.createDataFrame(land_pdf)
    land.createOrReplaceTempView("land_cells")

    info = {
        "n_grid_points": len(grid_points(cfg)),
        "n_land_cells": int(land_pdf["district"].notna().sum()),
        "n_district_polygons": int(len(districts_gdf)),
        "n_districts_with_data": int(land_pdf.dropna(subset=["district"])["district"].nunique()),
        "n_states_with_data": (
            int(land_pdf.dropna(subset=["state"])["state"].nunique())
            if "state" in land_pdf and land_pdf["state"].notna().any()
            else None
        ),
        "dashboard_geojson": str(geojson_path.relative_to(geo_dir.parents[2])),
        "geojson_size_mb": round(geojson_path.stat().st_size / 1e6, 2),
    }
    return land, info


# ---------------------------------------------------------------- helpers
def _write_parquet(df: DataFrame, name: str, single: bool = True) -> int:
    out_path = resolve_path("paths.processed") / name
    target = df.coalesce(1) if single else df
    target.write.mode("overwrite").parquet(str(out_path))
    size = sum(f.stat().st_size for f in out_path.rglob("*") if f.is_file())
    return size


def _dir_rows(spark: SparkSession, name: str) -> int:
    try:
        return spark.read.parquet(str(resolve_path("paths.processed") / name)).count()
    except Exception:  # noqa: BLE001
        return -1


# ---------------------------------------------------------------- main
def run_pipeline(reuse_raw: bool = False, skip_validation: bool = False) -> dict:
    t_start = time.perf_counter()
    cfg = load_config()
    processed = resolve_path("paths.processed")
    processed.mkdir(parents=True, exist_ok=True)
    timings: dict[str, float] = {}

    # Stage 1 — validate raw files
    if skip_validation:
        validation = {"skipped": True}
    else:
        t0 = time.perf_counter()
        validation = validate_raw(cfg)
        timings["validate_raw_s"] = round(time.perf_counter() - t0, 1)
        print(f"[1/7] validated {validation['n_files']} raw files "
              f"({validation['n_invalid_files']} invalid) in {timings['validate_raw_s']}s")

    spark = get_spark("RainWatch-Pipeline")

    # Stage 2-4 — ingest, clean, write raw parquet
    raw_path = processed / RAW_DIR_NAME
    if reuse_raw and raw_path.exists():
        print("[2/7] reusing existing raw Parquet (--reuse-raw)")
        ingest_stats = {"reused": True}
        raw = spark.read.parquet(str(raw_path))
        n_years = raw.select("year").distinct().count()
    else:
        t0 = time.perf_counter()
        clean, ingest_stats = ingest_clean(spark, cfg)
        timings["ingest_clean_s"] = round(time.perf_counter() - t0, 1)
        print(f"[2/7] ingested {ingest_stats['records_read']:,} records "
              f"({ingest_stats['valid_records']:,} valid) in {timings['ingest_clean_s']}s")

        t0 = time.perf_counter()
        clean.write.mode("overwrite").partitionBy("year").parquet(str(raw_path))
        timings["raw_write_s"] = round(time.perf_counter() - t0, 1)
        raw_size_mb = sum(f.stat().st_size for f in raw_path.rglob("*") if f.is_file()) / 1e6
        ingest_stats["raw_parquet_size_mb"] = round(raw_size_mb, 1)
        print(f"[3/7] raw Parquet written ({raw_size_mb:.0f} MB) in {timings['raw_write_s']}s")
        raw = spark.read.parquet(str(raw_path))

    # Stage 5 — geography
    t0 = time.perf_counter()
    land, geo_info = build_geography(spark, cfg)
    timings["geography_s"] = round(time.perf_counter() - t0, 1)
    print(f"[4/7] geography: {geo_info['n_land_cells']} land cells / "
          f"{geo_info['n_districts_with_data']} districts in {timings['geography_s']}s")

    # Stage 6 — analytics
    ana_cfg = cfg["anomalies"]
    t0 = time.perf_counter()
    india_daily_df = analytics.india_daily(raw)
    monthly = analytics.monthly_summary(raw)
    annual = analytics.annual_summary(raw)
    seasonal = analytics.seasonal_summary(raw)
    clim, anomalies = analytics.climatology_and_anomalies(
        spark, monthly, ana_cfg["wet_z"], ana_cfg["dry_z"]
    )
    dd = analytics.district_daily(raw, land)
    dmonthly = analytics.district_monthly(dd)
    extremes = analytics.extremes_days(spark, india_daily_df, dd)
    n_years = raw.select("year").distinct().count()
    dstats = analytics.district_stats(spark, dd, n_years=n_years)
    grid_month = analytics.grid_monthly(raw, land)
    grid_stats = analytics.grid_cell_stats(raw, land)
    hist = analytics.rainfall_histogram(raw, land)
    timings["analytics_compute_s"] = round(time.perf_counter() - t0, 1)
    print(f"[5/7] analytics DAG built in {timings['analytics_compute_s']}s")

    t0 = time.perf_counter()
    outputs = {}
    outputs["india_daily"] = _write_parquet(india_daily_df, "india_daily", single=False)
    outputs["monthly_summary"] = _write_parquet(monthly, "monthly_summary")
    outputs["annual_summary"] = _write_parquet(annual, "annual_summary")
    outputs["seasonal_summary"] = _write_parquet(seasonal, "seasonal_summary")
    outputs["climatology"] = _write_parquet(clim, "climatology")
    outputs["anomalies_monthly"] = _write_parquet(anomalies, "anomalies_monthly")
    outputs["extremes_days"] = _write_parquet(extremes, "extremes_days")
    outputs["district_stats"] = _write_parquet(dstats, "district_stats")
    outputs["district_monthly"] = _write_parquet(dmonthly, "district_monthly")
    outputs["district_daily"] = _write_parquet(dd, "district_daily", single=False)
    outputs["grid_monthly"] = _write_parquet(grid_month, "grid_monthly")
    outputs["grid_cell_stats"] = _write_parquet(grid_stats, "grid_cell_stats")
    outputs["rainfall_histogram"] = _write_parquet(hist, "rainfall_histogram")
    timings["analytics_write_s"] = round(time.perf_counter() - t0, 1)
    print(f"[6/7] analytics outputs written in {timings['analytics_write_s']}s")

    # Stage 7 — manifest from the ACTUAL run
    schema_json = json.loads(raw.schema.json())
    coverage = raw.agg(
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date"),
        F.countDistinct("date").alias("n_days"),
        F.countDistinct("year").alias("n_years"),
    ).collect()[0].asDict()

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "project": cfg["project"],
        "dataset": {
            "source": "NASA POWER Daily Point API (MERRA-2, bias-corrected precipitation)",
            "source_url": "https://power.larc.nasa.gov/docs/methodology/meteorology/precipitation/",
            "parameter": cfg["dataset"]["parameter"],
            "units": "mm/day",
            "native_resolution": "~0.5 deg x 0.625 deg (MERRA-2 native grid)",
            "sampling_grid": "0.5 deg x 0.5 deg lattice over India",
            "requested_start": cfg["dataset"]["start_date"],
            "requested_end": cfg["dataset"]["end_date"],
            "substitution_note": (
                "GPM IMERG Final Run (GES DISC) requires NASA Earthdata credentials and "
                "hundreds of GB per decade; per project rules the open, credential-free "
                "NASA POWER daily product is used instead. See docs/DATA_SOURCES.md."
            ),
        },
        "coverage": coverage,
        "validation": validation,
        "ingest": ingest_stats,
        "geography": geo_info,
        "spark": {
            "version": spark.version,
            "master": spark.sparkContext.master,
            "driver_memory": spark.sparkContext._conf.get("spark.driver.memory"),
            "shuffle_partitions": spark.sparkContext._conf.get("spark.sql.shuffle.partitions"),
        },
        "outputs": {k: {"size_mb": round(v / 1e6, 2), "rows": _dir_rows(spark, k)} for k, v in outputs.items()},
        "raw_schema": schema_json,
        "timings_s": timings,
        "total_runtime_s": round(time.perf_counter() - t_start, 1),
        "disclaimer": (
            "Educational historical analysis only. Statistical rainfall anomalies are not "
            "disaster declarations; rainfall exposure indicators are not flood-risk predictions."
        ),
    }
    out_manifest = resolve_path("paths.outputs") / "pipeline_manifest.json"
    out_manifest.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    copy = processed / "pipeline_manifest.json"
    shutil.copyfile(out_manifest, copy)

    print(f"[7/7] manifest written -> {out_manifest}")
    print(f"Pipeline complete in {manifest['total_runtime_s']}s "
          f"| coverage {coverage['min_date']} .. {coverage['max_date']} "
          f"| {coverage['n_days']} days | {coverage['n_years']} years")
    return manifest


def cli() -> None:
    parser = argparse.ArgumentParser(description="Run the RainWatch India pipeline")
    parser.add_argument("--reuse-raw", action="store_true",
                        help="reuse existing raw Parquet if present (restartable)")
    parser.add_argument("--skip-validation", action="store_true",
                        help="skip the per-file raw validation stage")
    args = parser.parse_args()
    run_pipeline(reuse_raw=args.reuse_raw, skip_validation=args.skip_validation)


if __name__ == "__main__":
    cli()
