"""Spark session factory shared by the pipeline, tests and benchmark."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from src.config import load_config


def get_spark(app_name: str = "RainWatchIndia") -> SparkSession:
    """Build a local SparkSession sized for an 8 GB RAM laptop.

    Env overrides:
      RAINWATCH_SPARK_MASTER      e.g. local[2]
      RAINWATCH_SPARK_DRIVER_MEM  e.g. 1500m
    """
    cfg = load_config()["spark"]
    master = os.environ.get("RAINWATCH_SPARK_MASTER", cfg.get("master", "local[*]"))
    driver_mem = os.environ.get(
        "RAINWATCH_SPARK_DRIVER_MEM", cfg.get("driver_memory", "2g")
    )
    shuffle_parts = cfg.get("shuffle_partitions", "16")

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
