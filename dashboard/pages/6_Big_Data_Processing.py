"""RainWatch India — Page 6: Big Data Processing."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import theme
from dashboard.data_loader import OUTPUTS, PROJECT_ROOT, load_manifest, ready_or_stop

theme.apply_theme()
ready_or_stop()
manifest = load_manifest()
theme.header(
    "Big Data Processing",
    "How Apache Spark + Parquet turn 15+ million raw observations into dashboard-ready analytics",
)

coverage = manifest.get("coverage", {})
validation = manifest.get("validation", {})
ingest = manifest.get("ingest", {})
spark_info = manifest.get("spark", {})
timings = manifest.get("timings_s", {})
outputs = manifest.get("outputs", {})
geo = manifest.get("geography", {})

# ------------------------------------------------------------- stage flow
st.subheader("Pipeline stages")
stage_rows = [
    ("1 · Inspect & validate raw files", validation.get("n_files", 0),
     f"{validation.get('n_invalid_files', 0)} invalid", timings.get("validate_raw_s")),
    ("2 · Ingest raw JSON → Spark", ingest.get("records_read", 0) or validation.get("n_values_all_files", 0),
     "records read", timings.get("ingest_clean_s")),
    ("3 · Clean + calendar features", ingest.get("valid_records", 0),
     "valid observations", None),
    ("4 · Write raw Parquet (partitioned by year)", coverage.get("n_days", 0),
     f"days · {ingest.get('raw_parquet_size_mb', '?')} MB", timings.get("raw_write_s")),
    ("5 · Grid → district mapping", geo.get("n_land_cells", 0),
     f"land cells · {geo.get('n_districts_with_data', '-')} districts", timings.get("geography_s")),
    ("6 · Spark aggregations → 13 Parquet outputs", sum(v.get("rows", 0) for k, v in outputs.items() if k != "district_daily"),
     "rows written", timings.get("analytics_write_s")),
    ("7 · Run manifest", 1, "statistics from this actual run", None),
]
st.markdown(
    '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:8px;">'
    + "".join(
        f'<div style="background:#101E38;border:1px solid #20355B;border-radius:10px;'
        f'padding:8px 12px;font-size:.82rem;color:#9FB0CC;">{name}'
        f'<div style="color:#E8EFFA;font-weight:600;">{"{:,}".format(n) if isinstance(n, int) else n}'
        f' <span style="color:#9FB0CC;font-weight:400;font-size:.75rem;">{sub}</span></div></div>'
        for name, n, sub, _ in stage_rows
    )
    + "</div>",
    unsafe_allow_html=True,
)

c1, c2 = st.columns(2, gap="large")
with c1:
    st.subheader("Stage durations (seconds)")
    names, durs = [], []
    for key, label in [
        ("validate_raw_s", "Validate raw files"),
        ("ingest_clean_s", "Ingest JSON → Spark"),
        ("raw_write_s", "Write raw Parquet"),
        ("geography_s", "Geography (GeoPandas)"),
        ("analytics_compute_s", "Build analytics DAG"),
        ("analytics_write_s", "Write analytics Parquet"),
    ]:
        if timings.get(key) is not None:
            names.append(label)
            durs.append(timings[key])
    fig = go.Figure(go.Bar(x=durs, y=names, orientation="h", marker_color="#4DA3FF"))
    fig.update_layout(xaxis_title="seconds", height=280)
    theme.figure(fig)
with c2:
    st.subheader("Data volume funnel")
    read = ingest.get("records_read") or validation.get("n_values_all_files", 0)
    valid = ingest.get("valid_records", 0)
    fig2 = go.Figure(go.Funnel(
        y=["Raw JSON values read", "Valid observations (clean)", "Land-cell records (district scope)"],
        x=[read, valid, outputs.get("district_daily", {}).get("rows", 0)],
        textinfo="value", marker=dict(color=["#2E7FD4", "#4DA3FF", "#7DD3FC"]),
    ))
    fig2.update_layout(height=280, margin=dict(l=10, r=10))
    theme.figure(fig2)

# ------------------------------------------------------------- spark info
st.subheader("Spark execution information")
theme.kpi_row(
    [
        ("Spark version", f"{spark_info.get('version', '-')}", "pip-installed PySpark"),
        ("Master", f"{spark_info.get('master', '-')}", "local mode on this machine"),
        ("Driver memory", f"{spark_info.get('driver_memory', '-')}", "sized for an 8 GB laptop"),
        ("Shuffle partitions", f"{spark_info.get('shuffle_partitions', '-')}", "tuned for local[2]"),
    ]
)

c3, c4 = st.columns([2, 3], gap="large")
with c3:
    st.subheader("Source vs output sizes")
    raw_mb = ingest.get("raw_parquet_size_mb")
    raw_json_mb = None
    raw_dir = PROJECT_ROOT / "data" / "raw" / "power"
    if raw_dir.exists():
        raw_json_mb = sum(f.stat().st_size for f in raw_dir.rglob("*.json")) / 1e6
    rows = []
    if raw_json_mb:
        rows.append(("Raw NASA POWER JSON files", raw_json_mb))
    if raw_mb:
        rows.append(("Raw analytical Parquet", raw_mb))
    out_mb = sum(v.get("size_mb", 0) for v in outputs.values())
    rows.append(("Analytics Parquet outputs (13 tables)", out_mb))
    geo_dir = PROJECT_ROOT / "data" / "processed" / "geo"
    geo_mb = sum(f.stat().st_size for f in geo_dir.rglob("*") if f.is_file()) / 1e6 if geo_dir.exists() else 0.0
    rows.append(("Geo / boundary files", geo_mb))
    fig3 = go.Figure(go.Bar(
        x=[r[1] for r in rows], y=[r[0] for r in rows], orientation="h",
        marker_color="#7DD3FC",
    ))
    fig3.update_layout(xaxis_title="MB", height=250)
    theme.figure(fig3)
with c4:
    st.subheader("Processed-data schema (raw analytical table)")
    schema = manifest.get("raw_schema", {})
    fields = schema.get("fields", [])
    schema_rows = []
    for f in fields:
        name = f.get("name")
        t = f.get("type")
        if isinstance(t, dict):
            t = t.get("type", "?")
        schema_rows.append((name, t))
    st.dataframe(pd.DataFrame(schema_rows, columns=["column", "spark type"]), use_container_width=True, height=300)

st.subheader("Why Apache Spark? (and honest limitations)")
records = ingest.get("records_read") or validation.get("n_values_all_files", 0)
st.markdown(
    f"- **Scale**: the raw analytical table holds roughly **{records:,} daily observations**. "
    "Loading this into Pandas on an 8 GB laptop is possible but fragile; Spark processes it "
    "in partitions on disk-backed Parquet with shuffle-optimised aggregations.\n"
    "- **Model**: the same code scales from this laptop (`local[*]`) to a cluster by changing "
    "the master URL — the project's aggregations (group-by month/year/season/district, "
    "window ranks, joins with broadcast geography) are all distributed Spark operations.\n"
    "- **Parquet**: columnar, compressed, partition-prunable — reading one year costs a "
    "single directory scan instead of the whole file.\n"
    "- **Honesty**: for a dataset of this size on one machine, a well-tuned Pandas pipeline "
    "can be *competitive* — see the benchmark below. Spark's advantage is horizontal scale "
    "and memory headroom, not automatic speed on 15 M rows."
)

# ------------------------------------------------------------- benchmark
bench_path = OUTPUTS / "benchmark" / "benchmark_results.json"
st.subheader("Fair benchmark: Spark vs Pandas (same machine, same data, same operation)")
if bench_path.exists():
    b = json.loads(bench_path.read_text())
    fig4 = go.Figure(go.Bar(
        x=[b["spark"]["seconds"], b["pandas"]["seconds"]],
        y=["PySpark", "Pandas"], orientation="h",
        marker_color=["#2E7FD4", "#FBBF24"], text=[f"{b['spark']['seconds']} s", f"{b['pandas']['seconds']} s"],
        textposition="auto",
    ))
    fig4.update_layout(xaxis_title="seconds (lower is better)", height=200)
    theme.figure(fig4)
    st.markdown(
        f"Operation: `{b.get('operation', 'seasonal aggregation')}` over {b.get('rows', 0):,} rows — "
        f"identical data, machine and output. {b.get('notes', '')}"
    )
else:
    st.info(
        "Benchmark has not been run yet. Execute `python scripts/03_run_benchmark.py` "
        "(after the pipeline) to generate a fair same-machine comparison."
    )
