#!/usr/bin/env python3
"""
RainWatch India — Step 02: run the processing pipeline.

Reads the raw NASA POWER files downloaded by 01_download_data.py, runs all
cleaning / aggregation / analytics in Apache Spark, writes Parquet outputs to
data/processed/ and a run manifest to outputs/pipeline_manifest.json.

Run:  python scripts/02_run_pipeline.py [--reuse-raw] [--skip-validation]
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import run_pipeline  # noqa: E402

if __name__ == "__main__":
    manifest = run_pipeline(reuse_raw="--reuse-raw" in sys.argv,
                            skip_validation="--skip-validation" in sys.argv)
    raise SystemExit(0 if manifest else 1)
