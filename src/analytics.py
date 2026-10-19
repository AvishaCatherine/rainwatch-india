"""RainWatch India — Spark analytics.

Every aggregation here is executed with Apache Spark on the full dataset.
These functions produce the dashboard-facing Parquet outputs in
data/processed/. Pandas is only used at the very edges (tiny lookup frames),
never as the main processing engine.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SEASONS = {
    1: "Winter (Jan-Feb)", 2: "Winter (Jan-Feb)",
    3: "Pre-monsoon (Mar-May)", 4: "Pre-monsoon (Mar-May)", 5: "Pre-monsoon (Mar-May)",
    6: "SW Monsoon (Jun-Sep)", 7: "SW Monsoon (Jun-Sep)",
    8: "SW Monsoon (Jun-Sep)", 9: "SW Monsoon (Jun-Sep)",
    10: "Post-monsoon (Oct-Dec)", 11: "Post-monsoon (Oct-Dec)", 12: "Post-monsoon (Oct-Dec)",
}

MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def with_calendar(df: DataFrame) -> DataFrame:
    """Derive year, month, day, season and other calendar columns from `date`."""
    month_season = F.create_map([F.lit(x) for kv in SEASONS.items() for x in kv])
    month_name = F.create_map([F.lit(x) for kv in MONTH_NAMES.items() for x in kv])
    return (
        df.withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("doy", F.dayofyear("date"))
        .withColumn("season", month_season[F.col("month")])
        .withColumn("month_name", month_name[F.col("month")])
        .withColumn("is_monsoon", F.col("month").isin(6, 7, 8, 9).cast("int"))
    )


def india_daily(clean: DataFrame) -> DataFrame:
    """Daily India (land-cell) summary: mean, max, heavy / extremely-heavy counts."""
    return (
        clean.groupBy("date")
        .agg(
            F.round(F.avg("rainfall_mm"), 4).alias("land_mean_mm"),
            F.round(F.max("rainfall_mm"), 2).alias("land_max_mm"),
            F.count(F.lit(1)).alias("n_cells"),
            F.sum(F.when(F.col("rainfall_mm") >= 64.5, 1).otherwise(0)).alias("heavy_cells"),
            F.sum(F.when(F.col("rainfall_mm") >= 115.6, 1).otherwise(0)).alias("extreme_cells"),
        )
        .withColumn("pct_heavy", F.round(F.col("heavy_cells") / F.col("n_cells") * 100, 2))
        .orderBy("date")
    )


def monthly_summary(clean: DataFrame) -> DataFrame:
    """Land-average monthly totals and related monthly stats."""
    daily = india_daily(clean)
    return (
        daily.withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.create_map([F.lit(x) for kv in MONTH_NAMES.items() for x in kv])[F.col("month")])
        .groupBy("year", "month", "month_name")
        .agg(
            F.round(F.sum("land_mean_mm"), 2).alias("total_mm"),
            F.round(F.avg("land_mean_mm"), 4).alias("mean_daily_mm"),
            F.round(F.max("land_max_mm"), 2).alias("max_cell_mm"),
            F.sum(F.when(F.col("land_max_mm") >= 64.5, 1).otherwise(0)).alias("heavy_days"),
            F.sum(F.when(F.col("land_max_mm") >= 115.6, 1).otherwise(0)).alias("extreme_days"),
        )
        .orderBy("year", "month")
    )


def annual_summary(clean: DataFrame) -> DataFrame:
    """Annual totals with monsoon share and wettest/driest month per year."""
    monthly = monthly_summary(clean)
    annual = (
        monthly.groupBy("year")
        .agg(
            F.round(F.sum("total_mm"), 1).alias("total_mm"),
            F.round(F.avg("mean_daily_mm"), 3).alias("mean_daily_mm"),
            F.round(
                F.sum(F.when(F.col("month").isin(6, 7, 8, 9), F.col("total_mm")).otherwise(0.0)), 1
            ).alias("monsoon_total_mm"),
            F.sum("heavy_days").alias("heavy_days"),
            F.sum("extreme_days").alias("extreme_days"),
        )
        .withColumn(
            "monsoon_share_pct", F.round(F.col("monsoon_total_mm") / F.col("total_mm") * 100, 1)
        )
    )
    w = Window.partitionBy("year").orderBy(F.desc("total_mm"))
    wettest = monthly.select("year", "month_name", "total_mm").withColumn("r", F.row_number().over(w))
    annual = (
        annual.join(wettest.filter("r = 1").select("year", F.col("month_name").alias("wettest_month")), "year")
        .join(wettest.filter("r = 12").select("year", F.col("month_name").alias("driest_month")), "year", "left")
    )
    return annual.orderBy("year")


def seasonal_summary(clean: DataFrame) -> DataFrame:
    """Season x year totals (IMD climate seasons)."""
    daily = india_daily(clean)
    return (
        daily.withColumn("year", F.year("date"))
        .withColumn("season", F.create_map([F.lit(x) for kv in SEASONS.items() for x in kv])[F.month("date")])
        .groupBy("year", "season")
        .agg(
            F.round(F.sum("land_mean_mm"), 2).alias("total_mm"),
            F.round(F.avg("land_mean_mm"), 4).alias("mean_daily_mm"),
        )
        .orderBy("year", "season")
    )


def climatology_and_anomalies(
    spark: SparkSession, monthly: DataFrame, wet_z: float, dry_z: float
) -> tuple[DataFrame, DataFrame]:
    """Long-term monthly baseline (climatology) and monthly anomaly table.

    Baseline = full available period (documented limitation: a longer record
    would give a more stable climatology; WMO standard is 30 years).
    """
    clim = (
        monthly.groupBy("month")
        .agg(
            F.round(F.avg("total_mm"), 2).alias("baseline_total_mm"),
            F.round(F.stddev("total_mm"), 2).alias("baseline_std_mm"),
            F.count(F.lit(1)).alias("n_years"),
        )
        .orderBy("month")
    )

    z = (F.col("total_mm") - F.col("baseline_total_mm")) / F.col("baseline_std_mm")
    anomalies = (
        monthly.join(clim, "month")
        .withColumn("z", F.round(z, 2))
        .withColumn(
            "anomaly_pct",
            F.round((F.col("total_mm") - F.col("baseline_total_mm")) / F.col("baseline_total_mm") * 100, 1),
        )
        .withColumn(
            "category",
            F.when(F.col("z") >= wet_z, "Unusually wet")
            .when(F.col("z") <= dry_z, "Unusually dry")
            .otherwise("Normal"),
        )
        .select("year", "month", "month_name", "total_mm", "baseline_total_mm",
                "baseline_std_mm", "n_years", "z", "anomaly_pct", "category")
        .orderBy("year", "month")
    )
    return clim, anomalies


def extremes_days(spark: SparkSession, india_daily_df: DataFrame, district_daily: DataFrame,
                  top_n: int = 200) -> DataFrame:
    """Highest-rainfall days with the top districts for each day."""
    top = india_daily_df.orderBy(F.desc("land_max_mm")).limit(top_n)
    top.createOrReplaceTempView("top_days")

    w = Window.partitionBy("date").orderBy(F.desc("district_mean_mm"))
    top5 = (
        district_daily.join(top.select("date"), "date")
        .withColumn("r", F.row_number().over(w))
        .filter("r <= 5")
        .groupBy("date")
        .agg(
            F.collect_list(
                F.struct(F.col("district"), F.round(F.col("district_mean_mm"), 1).alias("mean_mm"))
            ).alias("top_districts")
        )
    )
    return (
        top.join(top5, "date", "left")
        .orderBy(F.desc("land_max_mm"))
    )


def district_daily(clean: DataFrame, land_cells_spark: DataFrame) -> DataFrame:
    """Daily district rainfall = mean of member cells (areal approximation)."""
    return (
        clean.join(F.broadcast(land_cells_spark), ["lat", "lon"])
        .groupBy("date", "state", "district")
        .agg(F.round(F.avg("rainfall_mm"), 4).alias("district_mean_mm"),
             F.count(F.lit(1)).alias("n_cells"))
    )


def district_monthly(district_daily: DataFrame) -> DataFrame:
    """District x month totals — powers the district comparison time series."""
    return (
        district_daily.withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .groupBy("state", "district", "year", "month")
        .agg(
            F.round(F.sum("district_mean_mm"), 1).alias("total_mm"),
            F.round(F.avg("district_mean_mm"), 3).alias("mean_daily_mm"),
        )
        .orderBy("state", "district", "year", "month")
    )


def district_stats(spark: SparkSession, district_daily: DataFrame, n_years: int) -> DataFrame:
    """Per-district long-term indicators (documented rainfall-exposure metrics)."""
    dd = district_daily.withColumn("year", F.year("date"))

    annual = (
        dd.groupBy("state", "district", "year")
        .agg(F.round(F.sum("district_mean_mm"), 1).alias("annual_mm"))
    )
    base = (
        annual.groupBy("state", "district")
        .agg(
            F.round(F.avg("annual_mm"), 1).alias("mean_annual_mm"),
            F.round(F.stddev("annual_mm"), 1).alias("std_annual_mm"),
            F.round(F.max("annual_mm"), 1).alias("wettest_year_mm"),
            F.round(F.min("annual_mm"), 1).alias("driest_year_mm"),
            F.count(F.lit(1)).alias("n_years_obs"),
        )
        .withColumn("cv_annual_pct", F.round(F.col("std_annual_mm") / F.col("mean_annual_mm") * 100, 1))
    )

    wet_dry = (
        annual.withColumn(
            "r_desc", F.row_number().over(Window.partitionBy("state", "district").orderBy(F.desc("annual_mm")))
        )
        .withColumn(
            "r_asc", F.row_number().over(Window.partitionBy("state", "district").orderBy(F.asc("annual_mm")))
        )
    )
    wettest = wet_dry.filter("r_desc = 1").select("state", "district", F.col("year").alias("wettest_year"))
    driest = wet_dry.filter("r_asc = 1").select("state", "district", F.col("year").alias("driest_year"))

    heavy = (
        dd.groupBy("state", "district")
        .agg(F.sum(F.when(F.col("district_mean_mm") >= 64.5, 1).otherwise(0)).alias("heavy_cell_days"))
        .withColumn("heavy_days_per_year", F.round(F.col("heavy_cell_days") / n_years, 1))
    )

    monsoon = (
        dd.withColumn("m", F.month("date"))
        .groupBy("state", "district")
        .agg(
            F.round(F.sum(F.when(F.col("m").isin(6, 7, 8, 9), F.col("district_mean_mm")).otherwise(0.0)), 1)
            .alias("monsoon_mm"),
            F.round(F.sum("district_mean_mm"), 1).alias("all_mm"),
        )
        .withColumn("monsoon_share_pct", F.round(F.col("monsoon_mm") / F.col("all_mm") * 100, 1))
        .select("state", "district", "monsoon_share_pct")
    )

    stats = (
        base.join(heavy.select("state", "district", "heavy_days_per_year"), ["state", "district"])
        .join(monsoon, ["state", "district"])
        .join(wettest, ["state", "district"], "left")
        .join(driest, ["state", "district"], "left")
    )

    # Rainfall Exposure Index: equal-weight percentile rank (0-100) of
    # long-term mean annual rainfall and heavy-rain days per year.
    stats = stats.withColumn(
        "p_mean", F.percent_rank().over(Window.orderBy("mean_annual_mm"))
    ).withColumn(
        "p_heavy", F.percent_rank().over(Window.orderBy("heavy_days_per_year"))
    )
    return (
        stats.withColumn("exposure_index", F.round((F.col("p_mean") + F.col("p_heavy")) / 2 * 100, 1))
        .drop("p_mean", "p_heavy")
        .orderBy(F.desc("exposure_index"))
    )


def grid_monthly(clean: DataFrame, land_cells_spark: DataFrame) -> DataFrame:
    """Per grid-cell monthly totals (land cells) — powers the map page."""
    return (
        clean.join(F.broadcast(land_cells_spark), ["lat", "lon"])
        .groupBy("lat", "lon", "state", "district", F.year("date").alias("year"), F.month("date").alias("month"))
        .agg(F.round(F.sum("rainfall_mm"), 2).alias("total_mm"))
    )


def grid_cell_stats(clean: DataFrame, land_cells_spark: DataFrame) -> DataFrame:
    """Per-cell long-term stats: mean annual total and monsoon share."""
    cells = clean.join(F.broadcast(land_cells_spark), ["lat", "lon"])

    per_cell_annual = cells.groupBy(
        "lat", "lon", "state", "district", F.year("date").alias("year")
    ).agg(F.round(F.sum("rainfall_mm"), 2).alias("annual_mm"))

    mean_annual = per_cell_annual.groupBy("lat", "lon", "state", "district").agg(
        F.round(F.avg("annual_mm"), 1).alias("mean_annual_mm"),
        F.round(F.stddev("annual_mm"), 1).alias("std_annual_mm"),
    )

    monsoon = (
        cells.withColumn("m", F.month("date"))
        .groupBy("lat", "lon")
        .agg(
            F.round(F.sum(F.when(F.col("m").isin(6, 7, 8, 9), F.col("rainfall_mm")).otherwise(0.0)), 1)
            .alias("monsoon_mm"),
            F.round(F.sum("rainfall_mm"), 1).alias("all_mm"),
        )
        .withColumn("monsoon_share_pct", F.round(F.col("monsoon_mm") / F.col("all_mm") * 100, 1))
    )
    return mean_annual.join(monsoon, ["lat", "lon"]).drop("monsoon_mm", "all_mm")


def rainfall_histogram(clean: DataFrame, land_cells_spark: DataFrame) -> DataFrame:
    """IMD daily-rainfall class histogram over land cells (for the explorer page)."""
    df = clean.join(F.broadcast(land_cells_spark), ["lat", "lon"])
    cls = (
        F.when(F.col("rainfall_mm") < 0.1, "No rain (0)")
        .when(F.col("rainfall_mm") < 2.5, "Very light (0.1-2.4)")
        .when(F.col("rainfall_mm") < 15.6, "Light (2.5-15.5)")
        .when(F.col("rainfall_mm") < 35.5, "Moderate (15.6-35.5)")
        .when(F.col("rainfall_mm") < 64.5, "Rather heavy (35.6-64.4)")
        .when(F.col("rainfall_mm") < 115.6, "Heavy (64.5-115.5)")
        .otherwise("Extremely heavy (>=115.6)")
    )
    return (
        df.withColumn("class", cls)
        .groupBy(F.month("date").alias("month"), "class")
        .agg(F.count(F.lit(1)).alias("n"))
        .orderBy("month", "class")
    )
