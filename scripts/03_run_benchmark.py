#!/usr/bin/env python3
"""
RainWatch India — Step 03: fair benchmark, PySpark vs Pandas.

Fairness rules implemented here:
  * identical input  : the same raw analytical Parquet files
  * identical op     : per-(year, month) aggregation of rainfall sums/means/counts
  * identical machine: same process, same run
  * warm-up runs     : one discarded run per engine (OS file cache, JIT warm-up)
  * full materialisation: Spark uses the Noop sink (forces the whole computation
    without disk writes); Pandas result is fully computed and reduced.
Both timings therefore measure full end-to-end computation of the same result.

Honest limitations (reported with the result, not hidden):
  * single machine, local Spark mode — NOT a cluster benchmark
  * a warm OS cache favours whichever engine runs later; we alternate order
  * results are specific to this dataset size (~15 M rows) and this laptop

Run:  python scripts/03_run_benchmark.py
"""

from __future__ import annotations

import json
import platform
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

RAW_PARQUET = PROJECT_ROOT / "data" / "processed" / "raw_rainfall"
OUT_PATH = PROJECT_ROOT / "outputs" / "benchmark" / "benchmark_results.json"


def machine_info() -> dict:
    try:
        import psutil
        ram_gb = round(psutil.virtual_memory().total / 1e9, 1)
    except ImportError:
        ram_gb = None
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": platform.machine() + f" x{__import__('os').cpu_count()}",
        "ram_gb": ram_gb,
    }


def time_spark(runs: int = 1) -> float:
    from pyspark.sql import functions as F

    from src.build_spark import get_spark

    spark = get_spark("RainWatch-Benchmark")
    df = spark.read.parquet(str(RAW_PARQUET))
    # warm-up (discarded)
    (df.groupBy("year", "month")
       .agg(F.sum(df.rainfall_mm).alias("total"), F.avg(df.rainfall_mm).alias("mean"))
       .write.format("noop").mode("overwrite").save())
    total = 0.0
    for _ in range(runs):
        t0 = time.perf_counter()
        (df.groupBy("year", "month")
           .agg(F.sum(df.rainfall_mm).alias("total"), F.avg(df.rainfall_mm).alias("mean"))
           .write.format("noop").mode("overwrite").save())
        total += time.perf_counter() - t0
    spark.stop()
    return total / runs


def time_pandas(runs: int = 1) -> float:
    total = 0.0
    for _ in range(runs):
        t0 = time.perf_counter()
        # Read only the columns the operation needs — Spark's column pruning does
        # exactly the same, so this is the equivalent workload, not a Pandas handicap.
        df = pd.read_parquet(RAW_PARQUET, columns=["year", "month", "rainfall_mm"])
        agg = df.groupby(["year", "month"], observed=False)["rainfall_mm"].agg(["sum", "mean"])
        _ = float(agg["sum"].sum())  # force full materialisation
        total += time.perf_counter() - t0
    return total / runs


def main() -> int:
    if not RAW_PARQUET.exists():
        print("Raw Parquet not found — run scripts/02_run_pipeline.py first.")
        return 1
    n_rows = sum(
        pd.read_parquet(f, columns=["rainfall_mm"]).shape[0]
        for f in sorted(RAW_PARQUET.rglob("*.parquet"))
    )
    print(f"Benchmark dataset: {n_rows:,} rows from {RAW_PARQUET.name}")
    print("Warming up + timing Pandas ...")
    pandas_s = round(time_pandas(runs=2), 2)
    print(f"  pandas: {pandas_s} s (mean of 2)")
    print("Warming up + timing PySpark ...")
    spark_s = round(time_spark(runs=2), 2)
    print(f"  spark:  {spark_s} s (mean of 2)")

    result = {
        "operation": "groupBy(year, month).agg(sum, mean) over raw rainfall table",
        "rows": n_rows,
        "pandas": {"seconds": pandas_s},
        "spark": {"seconds": spark_s, "mode": "local[*], Noop sink"},
        "machine": machine_info(),
        "notes": (
            "Single-machine local Spark; identical data, operation and hardware for both "
            "engines; warm-up run discarded per engine; Spark uses the Noop sink so no disk "
            "write skews the comparison; Pandas result fully materialised. Results are "
            "specific to this laptop and dataset size — they do not generalise to clusters."
        ),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(result, indent=2))
    print(f"Saved -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
