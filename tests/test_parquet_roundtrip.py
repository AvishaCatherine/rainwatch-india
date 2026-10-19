"""Parquet write/read round-trip and pipeline ingestion/validation integration tests."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipeline import ingest_clean, validate_raw, power_schema


class TestParquetRoundTrip:
    def test_write_read_validate(self, spark, tmp_path):
        from pyspark.sql import functions as F

        df = spark.createDataFrame(
            [(date(2023, 6, 1), 10.0, 70.0, 42.5), (date(2023, 6, 2), 10.5, 70.5, 3.25)],
            ["date", "lat", "lon", "rainfall_mm"],
        )
        path = tmp_path / "roundtrip.parquet"
        df.repartition(1).write.mode("overwrite").parquet(str(path))
        back = spark.read.parquet(str(path))
        agg = back.agg(F.count("*").alias("n"), F.sum("rainfall_mm").alias("total")).collect()[0]
        assert agg["n"] == 2
        assert abs(agg["total"] - 45.75) < 1e-9

    def test_partitioned_write_and_year_pruning(self, spark, tmp_path):
        df = spark.createDataFrame(
            [(2022, 1.0), (2023, 2.0)], ["year", "val"]
        )
        path = tmp_path / "partitioned.parquet"
        df.write.mode("overwrite").partitionBy("year").parquet(str(path))
        back = spark.read.parquet(str(path))
        assert back.count() == 2
        only_2022 = spark.read.parquet(str(path)).filter("year = 2022")
        assert only_2022.count() == 1
        assert only_2022.collect()[0]["val"] == 1.0


class TestIngestClean:
    def test_ingest_flags_and_cleaning(self, spark, tmp_path, monkeypatch, raw_power_dir):
        # Point the pipeline at the synthetic raw directory (no network, no real data).
        monkeypatch.setattr("src.pipeline.resolve_path", lambda key: raw_power_dir)
        clean, stats = ingest_clean(spark, {"dataset": {"parameter": "PRECTOTCORR"}})

        # 2 valid point files x 8+2 values = 10 records read (broken file contributes none)
        assert stats["records_read"] == 10
        assert stats["missing_fill"] == 1
        assert stats["null"] == 1
        assert stats["negative"] == 1
        assert stats["valid_records"] == 7

        # The clean frame must exclude fill/null/negative values
        assert clean.filter("rainfall_mm = -999.0").count() == 0
        assert clean.filter("rainfall_mm < 0").count() == 0
        assert clean.count() == 7

        # Calendar columns are present
        assert {"year", "month", "season", "is_monsoon"} <= set(clean.columns)
        assert clean.filter("year = 2023").count() == 7

    def test_power_schema_matches_payload(self):
        schema = power_schema("PRECTOTCORR")
        assert "geometry" in [f.name for f in schema.fields]
        props = [f for f in schema.fields if f.name == "properties"][0]
        assert props.dataType.fieldNames() == ["parameter"]


class TestValidateRaw:
    def test_validation_counts(self, tmp_path, raw_power_dir, monkeypatch):
        monkeypatch.setattr("src.pipeline.resolve_path", lambda key: raw_power_dir)
        cfg = {"dataset": {"parameter": "PRECTOTCORR"}}
        summary = validate_raw(cfg)
        assert summary["n_files"] == 3
        assert summary["n_valid_files"] == 2
        assert summary["n_invalid_files"] == 1
        assert summary["n_values_all_files"] == 10
        assert summary["min_date_any"] == "2015-01-01" or summary["min_date_any"] == "2023-01-01"
        # synthetic payloads all start 2023-01-01
        assert summary["min_date_any"] == "2023-01-01"
        assert summary["max_date_any"] == "2023-01-08"
        assert summary["missing_pct"] == pytest.approx(20.0, abs=0.01)  # 2/10
