"""Tests for config helpers and Spark transformations / analytics math."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import datetime
import pandas as pd
import pytest
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import analytics
from src.config import _linspace, grid_points, load_config, point_file_name


# ------------------------------------------------------------------ config
class TestConfig:
    def test_grid_lattice_shape(self):
        cfg = load_config()
        pts = grid_points(cfg)
        g = cfg["dataset"]["grid"]
        n_lat = len(_linspace(g["lat_min"], g["lat_max"], g["lat_step"]))
        n_lon = len(_linspace(g["lon_min"], g["lon_max"], g["lon_step"]))
        assert len(pts) == n_lat * n_lon
        assert len(pts) == 3538  # the documented sampling lattice for this project

    def test_linspace_endpoints_and_step(self):
        vals = _linspace(0.0, 1.0, 0.25)
        assert vals == [0.0, 0.25, 0.5, 0.75, 1.0]

    def test_point_file_name_format(self):
        assert point_file_name(10.0, 70.0) == "power_10.00_70.00.json"


# ------------------------------------------------------------- spark tests
@pytest.fixture()
def small_clean(spark):
    """A tiny clean rainfall DF with calendar columns already derived."""
    rows = [
        (date(2023, 1, 1), 10.0, 70.0, 5.0),
        (date(2023, 1, 2), 10.0, 70.0, 15.0),
        (date(2023, 6, 15), 10.0, 70.0, 40.0),
        (date(2023, 6, 16), 10.0, 70.0, 80.0),   # heavy day
        (date(2023, 6, 16), 10.5, 70.5, 120.0),  # extremely heavy same day
        (date(2022, 7, 10), 10.0, 70.0, 10.0),
    ]
    df = spark.createDataFrame(rows, ["date", "lat", "lon", "rainfall_mm"])
    return analytics.with_calendar(df)


class TestCalendar:
    def test_calendar_fields(self, small_clean):
        r = small_clean.filter(F.col("date") == datetime.date(2023, 6, 16)).collect()[0]
        assert r["year"] == 2023 and r["month"] == 6 and r["day"] == 16
        assert r["season"] == "SW Monsoon (Jun-Sep)"
        assert r["month_name"] == "Jun" and r["is_monsoon"] == 1

    def test_winter_season(self, small_clean):
        r = small_clean.filter(F.col("date") == datetime.date(2023, 1, 1)).collect()[0]
        assert r["season"] == "Winter (Jan-Feb)" and r["is_monsoon"] == 0


class TestAggregations:
    def test_india_daily(self, small_clean):
        out = analytics.india_daily(small_clean)
        row = out.filter(F.col("date") == datetime.date(2023, 6, 16)).collect()[0]
        assert row["n_cells"] == 2
        assert abs(row["land_mean_mm"] - 100.0) < 1e-6
        assert row["land_max_mm"] == 120.0
        assert row["heavy_cells"] == 2 and row["extreme_cells"] == 1

    def test_monthly_summary_totals(self, small_clean):
        out = analytics.monthly_summary(small_clean)
        jun = out.filter("year = 2023 and month = 6").collect()[0]
        # daily land means: 40.0 then 100.0 -> monthly total 140
        assert abs(jun["total_mm"] - 140.0) < 1e-6
        assert jun["heavy_days"] == 1 and jun["extreme_days"] == 1

    def test_seasonal_summary(self, small_clean):
        out = analytics.seasonal_summary(small_clean)
        monsoon_2023 = out.filter("year = 2023 and season = 'SW Monsoon (Jun-Sep)'").collect()[0]
        assert abs(monsoon_2023["total_mm"] - 140.0) < 1e-6

    def test_anomaly_math(self, spark):
        # 24 months with a known pattern; June 2023 doubled vs June baseline
        rows = []
        for y in range(2020, 2025):
            for m in range(1, 13):
                base = 100.0 if m == 6 else 10.0
                val = 300.0 if (y == 2024 and m == 6) else base
                rows.append((y, m, val))
        schema = ["year", "month", "total_mm"]
        monthly = spark.createDataFrame(rows, schema)
        month_map = F.create_map([F.lit(x) for kv in analytics.MONTH_NAMES.items() for x in kv])
        monthly = monthly.withColumn("month_name", month_map[F.col("month")])
        clim, anom = analytics.climatology_and_anomalies(spark, monthly, 1.5, -1.5)
        clim_rows = {r["month"]: (r["baseline_total_mm"], r["n_years"]) for r in clim.collect()}
        assert clim_rows[6][0] == pytest.approx(140.0, abs=0.01)
        assert clim_rows[6][1] == 5
        jun24 = anom.filter("year = 2024 and month = 6").collect()[0]
        assert jun24["category"] == "Unusually wet"
        assert jun24["z"] == pytest.approx(1.789, abs=0.02)
        assert jun24["anomaly_pct"] == pytest.approx(114.3, abs=0.2)
        jan24 = anom.filter("year = 2024 and month = 1").collect()[0]
        assert jan24["category"] == "Normal"  # zero std-dev months must not explode

    def test_district_daily_mean(self, spark, small_clean, land_pdf):
        land = spark.createDataFrame(land_pdf)
        out = analytics.district_daily(small_clean, land)
        row = out.filter((F.col("date") == datetime.date(2023, 6, 16)) & (F.col("district") == "District A")).collect()[0]
        assert row["district_mean_mm"] == 80.0

    def test_district_stats_index_range(self, spark, small_clean, land_pdf):
        land = spark.createDataFrame(land_pdf)
        dd = analytics.district_daily(small_clean, land)
        stats = analytics.district_stats(spark, dd, n_years=2)
        pdf = stats.toPandas()
        assert len(pdf) == 2
        assert pdf["exposure_index"].between(0, 100).all()

    def test_empty_input_no_crash(self, spark):
        empty = spark.createDataFrame([], schema="date DATE, lat DOUBLE, lon DOUBLE, rainfall_mm DOUBLE")
        empty = analytics.with_calendar(empty)
        out = analytics.india_daily(empty)
        assert out.count() == 0

    def test_histogram_classes_partition_the_domain(self, spark, small_clean, land_pdf):
        land = spark.createDataFrame(land_pdf)
        hist = analytics.rainfall_histogram(small_clean, land).toPandas()
        assert hist["n"].sum() == 6  # every valid observation lands in exactly one class
