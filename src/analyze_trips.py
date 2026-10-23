"""Aggregate January 2024 Jersey City trips into hourly station flows."""

from __future__ import annotations

import argparse
import csv
import glob
import sys
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logging_utils import setup_logger


def resolve_zip(path_pattern: str) -> Path:
    matches = sorted(glob.glob(path_pattern, recursive=True))
    if not matches:
        raise FileNotFoundError(f"No raw archive matches: {path_pattern}")
    if len(matches) > 1:
        raise ValueError(f"Expected one archive for the selected month, found {len(matches)}; pass --path")
    return Path(matches[0])


def write_local_csv(dataframe, destination: Path) -> None:
    """Stream an already aggregated Spark result to a single local CSV file."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            writer.writerow([value.isoformat(sep=" ") if isinstance(value, datetime) else value for value in row])


def analyze(path_pattern: str, output_dir: Path, log_file: Path) -> int:
    logger = setup_logger("analyze_trips", log_file)
    try:
        archive = resolve_zip(path_pattern)
        if not archive.is_file() or archive.stat().st_size == 0:
            raise FileNotFoundError(f"Raw archive is missing or empty: {archive}")
        with tempfile.TemporaryDirectory(prefix="stationpulse_analysis_") as temp:
            temp_path = Path(temp)
            with zipfile.ZipFile(archive) as zf:
                members = [n for n in zf.namelist() if n.lower().endswith(".csv") and not n.startswith("__MACOSX/")]
                if not members:
                    raise ValueError(f"No trip CSV found in {archive}")
                for member in members:
                    # Extract only the expected flat source CSV and prevent archive path traversal.
                    if Path(member).name != member:
                        raise ValueError(f"Unexpected nested CSV path in archive: {member}")
                    dest = temp_path / Path(member).name
                    with zf.open(member) as source, dest.open("wb") as target:
                        target.write(source.read())
            csv_paths = [str(p) for p in temp_path.glob("*.csv")]
            if not csv_paths:
                raise ValueError("Trip CSV extraction produced no files")

            from pyspark.sql import SparkSession, functions as F

            spark = (SparkSession.builder.master("local[*]")
                     .appName("StationPulse-January-2024-Analysis")
                     .config("spark.ui.enabled", "false")
                     .config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.driver.host", "127.0.0.1")
                     .config("spark.sql.shuffle.partitions", "4")
                     .getOrCreate())
            spark.sparkContext.setLogLevel("WARN")
            try:
                trips = (spark.read.option("header", True).option("inferSchema", True).csv(csv_paths)
                         .withColumn("started_at", F.to_timestamp("started_at"))
                         .withColumn("ended_at", F.to_timestamp("ended_at")))
                source_rows = trips.count()
                start_ids = trips.filter(F.col("start_station_id").isNotNull()).count()
                end_ids = trips.filter(F.col("end_station_id").isNotNull()).count()
                month_start = F.to_timestamp(F.lit("2024-01-01 00:00:00"))
                month_end = F.to_timestamp(F.lit("2024-02-01 00:00:00"))
                january_trips = trips.filter(F.col("started_at").isNotNull()
                                             & (F.col("started_at") >= month_start)
                                             & (F.col("started_at") < month_end))

                # Average departures per calendar day, including hours with zero trips.
                daily_hourly = (january_trips
                                .withColumn("service_date", F.to_date("started_at"))
                                .withColumn("hour_of_day", F.hour("started_at"))
                                .withColumn("day_type", F.when(F.dayofweek("started_at").isin(1, 7), "weekend")
                                            .otherwise("weekday"))
                                .groupBy("service_date", "day_type", "hour_of_day")
                                .agg(F.count(F.lit(1)).alias("departures")))
                total_by_type_hour = (daily_hourly.groupBy("day_type", "hour_of_day")
                                      .agg(F.sum("departures").alias("total_departures")))
                days_by_type = (january_trips.select(F.to_date("started_at").alias("service_date"))
                                .distinct()
                                .withColumn("day_type", F.when(F.dayofweek("service_date").isin(1, 7), "weekend")
                                            .otherwise("weekday"))
                                .groupBy("day_type").agg(F.count(F.lit(1)).alias("days_in_group")))
                demand_by_hour = (days_by_type.crossJoin(spark.range(24).select(F.col("id").alias("hour_of_day")))
                                  .join(total_by_type_hour, ["day_type", "hour_of_day"], "left")
                                  .fillna({"total_departures": 0})
                                  .withColumn("average_departures_per_day",
                                              F.round(F.col("total_departures") / F.col("days_in_group"), 2))
                                  .select("day_type", "hour_of_day", "days_in_group", "total_departures",
                                          "average_departures_per_day")
                                  .orderBy("day_type", "hour_of_day"))

                departures = (trips.filter(F.col("started_at").isNotNull() & F.col("start_station_id").isNotNull()
                                           & (F.col("started_at") >= month_start) & (F.col("started_at") < month_end))
                              .select(F.col("start_station_id").alias("station_id"),
                                      F.col("start_station_name").alias("station_name"),
                                      F.date_trunc("hour", "started_at").alias("hour"))
                              .groupBy("station_id", "hour")
                              .agg(F.count(F.lit(1)).alias("departures"), F.max("station_name").alias("station_name")))
                arrivals = (trips.filter(F.col("ended_at").isNotNull() & F.col("end_station_id").isNotNull()
                                         & (F.col("ended_at") >= month_start) & (F.col("ended_at") < month_end))
                            .select(F.col("end_station_id").alias("station_id"),
                                    F.col("end_station_name").alias("station_name"),
                                    F.date_trunc("hour", "ended_at").alias("hour"))
                            .groupBy("station_id", "hour")
                            .agg(F.count(F.lit(1)).alias("arrivals"), F.max("station_name").alias("arrival_station_name")))

                hourly = (departures.join(arrivals, ["station_id", "hour"], "full_outer")
                          .withColumn("station_name", F.coalesce("station_name", "arrival_station_name"))
                          .drop("arrival_station_name")
                          .fillna({"departures": 0, "arrivals": 0})
                          .withColumn("net_flow", F.col("arrivals") - F.col("departures"))
                          .select("hour", "station_id", "station_name", "departures", "arrivals", "net_flow")
                          .orderBy("hour", "station_id"))
                monthly = (hourly.groupBy("station_id", "station_name")
                           .agg(F.sum("departures").alias("departures"), F.sum("arrivals").alias("arrivals"))
                           .withColumn("net_flow", F.col("arrivals") - F.col("departures"))
                           .orderBy(F.desc("departures")))
                positive = (monthly.filter(F.col("net_flow") > 0)
                            .orderBy(F.desc("net_flow")).limit(10)
                            .withColumn("net_flow_group", F.lit("highest_net_inflow")))
                negative = (monthly.filter(F.col("net_flow") < 0)
                            .orderBy(F.asc("net_flow")).limit(10)
                            .withColumn("net_flow_group", F.lit("lowest_net_flow")))
                extremes = (positive.unionByName(negative)
                            .select("net_flow_group", "station_id", "station_name", "departures",
                                    "arrivals", "net_flow")
                            .orderBy("net_flow_group",
                                     F.when(F.col("net_flow_group") == "highest_net_inflow", -F.col("net_flow"))
                                     .otherwise(F.col("net_flow"))))

                output_dir.mkdir(parents=True, exist_ok=True)
                hourly_path = output_dir / "hourly_station_flow.csv"
                monthly_path = output_dir / "monthly_station_flow.csv"
                demand_path = output_dir / "hourly_demand_by_day_type.csv"
                extremes_path = output_dir / "station_net_flow_extremes.csv"
                # Aggregations are compact output artifacts; Spark does the heavy aggregation,
                # then the bounded result is streamed to local CSV (works without winutils).
                write_local_csv(hourly, hourly_path)
                write_local_csv(monthly, monthly_path)
                write_local_csv(demand_by_hour, demand_path)
                write_local_csv(extremes, extremes_path)
                logger.info("Source archive: %s", archive)
                logger.info("Input trip rows: %s", f"{source_rows:,}")
                logger.info("Rows with start station ID: %s", f"{start_ids:,}")
                logger.info("Rows with end station ID: %s", f"{end_ids:,}")
                logger.info("Hourly station-flow output: %s", hourly_path)
                logger.info("Monthly station-flow output: %s", monthly_path)
                logger.info("Weekday/weekend hourly-demand output: %s", demand_path)
                logger.info("Station net-flow extremes output: %s", extremes_path)
                logger.info("Average departures per day by day type and hour:")
                demand_by_hour.orderBy(F.desc("average_departures_per_day")).show(8, truncate=False)
                logger.info("Largest positive/negative monthly station net-flows:")
                extremes.show(20, truncate=False)
                logger.info("Sample monthly station totals:")
                monthly.show(10, truncate=False)
            finally:
                spark.stop()
        logger.info("January 2024 station-flow analysis completed [OK]")
        return 0
    except Exception as exc:
        logger.exception("Analysis failed: %s", exc)
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate January 2024 Jersey City Citi Bike station flows.")
    parser.add_argument("--path", default="data/raw/citibike_trips/ingest_date=*/run_*/JC-202401-citibike-tripdata.csv.zip",
                        help="Path or glob for the JC-202401 raw ZIP archive")
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed/citibike_trips/2024-01"))
    parser.add_argument("--log-file", type=Path, default=Path("logs/analyze_trips.txt"))
    args = parser.parse_args()
    raise SystemExit(analyze(args.path, args.output_dir, args.log_file))


if __name__ == "__main__":
    main()
