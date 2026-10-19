#!/usr/bin/env bash
# ============================================================
# RainWatch India — one-shot WSL/Ubuntu setup (beginner-friendly)
# Usage:  bash scripts/setup_wsl.sh
# Idempotent: safe to run repeatedly.
# ============================================================
set -e
echo "=============================================================="
echo " RainWatch India :: WSL/Ubuntu setup"
echo "=============================================================="

# 1. System packages (Java is required by PySpark)
echo "[1/4] Installing system packages (sudo password may be asked)..."
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip unzip openjdk-17-jre-headless

# 2. Virtual environment
echo "[2/4] Creating Python virtual environment (.venv)..."
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip -q

# 3. Python dependencies
echo "[3/4] Installing Python dependencies (this can take a few minutes)..."
pip install -r requirements.txt

# 4. The gate test: Spark + Parquet MUST pass before continuing
echo "[4/4] Running the Spark verification gate test..."
python scripts/00_verify_spark.py

cat <<'DONE'

==============================================================
 Setup complete. Next steps:
   python scripts/01_download_data.py     # one-time data download (~7 min)
   python scripts/02_run_pipeline.py      # Spark processing (~3-5 min)
   streamlit run dashboard/app.py         # open the dashboard
   pytest -q                              # run the test suite
==============================================================
DONE
