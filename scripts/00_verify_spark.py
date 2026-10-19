#!/usr/bin/env python3
"""
RainWatch India — Step 00: Spark / Parquet environment verification.

This is the FIRST script you should run after installing requirements.
It performs the minimal end-to-end proof that Apache Spark works in the
current environment:

    1. Start a local Spark session.
    2. Create a small DataFrame.
    3. Write it to Parquet.
    4. Read the Parquet output back.
    5. Validate the resulting records.

If this script exits with code 0, the verified configuration is good for
the whole project. If it fails on Windows with
"HADOOP_HOME and hadoop.home.dir are unset" or a missing winutils.exe,
follow docs/SETUP_WINDOWS.md (Option A: WSL Ubuntu is recommended;
Option B documents the winutils.exe fix for native Windows).

Run:  python scripts/00_verify_spark.py
"""

import os
import shutil
import sys
import tempfile

# Keep Spark quiet and small-memory friendly (laptop with 8 GB RAM).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def main() -> int:
    print("=" * 62)
    print("RainWatch India :: Spark environment verification")
    print("=" * 62)

    print(f"Python executable : {sys.executable}")
    print(f"JAVA_HOME         : {os.environ.get('JAVA_HOME', '(not set)')}")
    hadoop_home = os.environ.get("HADOOP_HOME", "(not set)")
    print(f"HADOOP_HOME       : {hadoop_home}")
    winutils = os.environ.get("hadoop.home.dir", "(not set)")
    print(f"hadoop.home.dir   : {winutils}")
    if winutils != "(not set)":
        exe = os.path.join(winutils, "bin", "winutils.exe")
        print(f"  winutils.exe present at that path: {os.path.exists(exe)}")

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError as exc:
        print("\n[FAIL] PySpark is not installed. Run: pip install -r requirements.txt")
        print(f"       Details: {exc}")
        return 1

    tmp_dir = tempfile.mkdtemp(prefix="rainwatch_spark_check_")
    parquet_path = os.path.join(tmp_dir, "check.parquet")

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("RainWatch-Spark-Verify")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        version = spark.version
        print(f"\n[1/5] Spark session started (Spark {version}, local[2]) OK")

        df = spark.createDataFrame(
            [
                ("Mumbai", 19.07, 72.88, 2024, 7, 812.4),
                ("Chennai", 13.08, 80.27, 2024, 7, 148.2),
                ("Delhi", 28.61, 77.20, 2024, 7, 210.7),
                ("Cherrapunji", 25.30, 91.70, 2024, 7, 3210.9),
            ],
            schema=["station", "lat", "lon", "year", "month", "rainfall_mm"],
        )
        print("[2/5] Test DataFrame created (4 rows) OK")

        df.repartition(1).write.mode("overwrite").parquet(parquet_path)
        print(f"[3/5] Parquet write OK -> {parquet_path}")

        read_back = spark.read.parquet(parquet_path)
        print("[4/5] Parquet read OK")

        count = read_back.count()
        agg = read_back.agg(
            F.count("*").alias("n"),
            F.round(F.sum("rainfall_mm"), 2).alias("total_mm"),
            F.max("rainfall_mm").alias("max_mm"),
        ).collect()[0]

        expected_total = 812.4 + 148.2 + 210.7 + 3210.9
        ok = (
            count == 4
            and abs(agg["total_mm"] - expected_total) < 0.01
            and abs(agg["max_mm"] - 3210.9) < 0.01
        )

        print(f"      rows={agg['n']}, total_mm={agg['total_mm']}, max_mm={agg['max_mm']}")
        if not ok:
            print("[5/5] FAIL — round-trip validation mismatch.")
            return 1
        print("[5/5] Round-trip validation OK")

        print("\nRESULT: SUCCESS — Spark + Parquet are working in this environment.")
        print(f"        Verified with Spark {version}. Use this configuration project-wide.")
        return 0

    except Exception as exc:  # noqa: BLE001 — we want a beginner-friendly message
        print("\n[FAIL] Spark verification failed:")
        print(f"       {type(exc).__name__}: {exc}")
        msg = str(exc)
        if "HADOOP_HOME" in msg or "winutils" in msg.lower() or "hadoop.home.dir" in msg:
            print(
                "\n       This is the classic Windows/Hadoop native-binary problem.\n"
                "       Fix it with ONE of these (details in docs/SETUP_WINDOWS.md):\n"
                "         A) Recommended: run the project inside WSL Ubuntu\n"
                "            (see docs/SETUP_WINDOWS.md Option A, includes setup_wsl.sh).\n"
                "         B) Native Windows: install winutils.exe + hadoop.dll matching\n"
                "            your Hadoop 3.3.x build and set HADOOP_HOME + PATH\n"
                "            (see docs/SETUP_WINDOWS.md Option B)."
            )
        elif "Cannot find" in msg and "jvm" in msg.lower() or "JVM" in msg:
            print(
                "\n       Java is missing or not found. Install a JDK (11 or 17) and set\n"
                "       JAVA_HOME. See docs/SETUP_WINDOWS.md."
            )
        return 1

    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
