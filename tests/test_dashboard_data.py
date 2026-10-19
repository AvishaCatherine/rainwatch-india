"""Dashboard data-loading tests (integration with real processed outputs; skipped
if the pipeline has not been run yet — the synthetic fixture keeps the rest of
the suite runnable without it)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PROCESSED = Path(__file__).resolve().parents[1] / "data" / "processed"
needs_pipeline = not (PROCESSED / "annual_summary").exists()

from dashboard.data_loader import load_manifest, load_table, partial_years, data_ready  # noqa: E402


@pytest.mark.skipif(needs_pipeline, reason="pipeline outputs not built yet")
class TestDashboardDataWithRealOutputs:
    def test_load_manifest(self):
        m = load_manifest()
        assert "coverage" in m and "min_date" in m["coverage"]

    def test_load_annual_summary(self):
        df = load_table("annual_summary")
        assert len(df) >= 10
        assert {"year", "total_mm", "monsoon_share_pct"} <= set(df.columns)
        assert df["total_mm"].dropna().gt(0).all()

    def test_load_district_stats(self):
        df = load_table("district_stats")
        assert df["exposure_index"].between(0, 100).all()

    def test_extremes_have_district_info(self):
        df = load_table("extremes_days")
        assert len(df) >= 100
        assert "top_districts" in df.columns

    def test_monotone_dates_in_india_daily(self):
        df = load_table("india_daily")
        dates = df["date"].sort_values()
        assert dates.is_monotonic_increasing


class TestPartialYearLogic:
    def test_full_year(self):
        m = {"coverage": {"max_date": "2025-12-31"}}
        assert partial_years(m) == []

    def test_partial_year_detected(self):
        m = {"coverage": {"max_date": "2026-10-16"}}
        assert partial_years(m) == [2026]

    def test_data_ready_flag_matches_disk(self):
        assert data_ready() == (PROCESSED / "annual_summary").exists()
