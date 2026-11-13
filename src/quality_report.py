import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from src.common.schemas import TRIPS_SCHEMA, build_station_dimension

def run_quality_check(df, dim_stations_df=None):
    print("=== [Quality Gate] Bắt đầu kiểm tra chất lượng dữ liệu chuyến đi ===")
    total_records = df.count()
    print(f"Tổng số bản ghi cần kiểm tra: {total_records}")

    if total_records == 0:
        return df

    null_ride_id = df.filter(col("ride_id").isNull()).count()
    null_start_station = df.filter(col("start_station_id").isNull()).count()
    null_end_station = df.filter(col("end_station_id").isNull()).count()

    distinct_rides = df.select("ride_id").distinct().count()
    duplicate_ride_id = total_records - distinct_rides

    invalid_time_count = df.filter(
        col("started_at").isNull() | col("ended_at").isNull() | (col("ended_at") <= col("started_at"))
    ).count()

    unmatched_count = 0
    if dim_stations_df is not None:
        unmatched_count = df.filter(col("start_station_id").isNotNull()).join(
            dim_stations_df,
            df["start_station_id"] == dim_stations_df["station_id"],
            "left_anti"
        ).count()

    print(f" - [Null Rate] Thiếu ride_id: {null_ride_id} ({null_ride_id/total_records:.2%})")
    print(f" - [Null Rate] Thiếu start_station_id: {null_start_station} ({null_start_station/total_records:.2%})")
    print(f" - [Null Rate] Thiếu end_station_id: {null_end_station} ({null_end_station/total_records:.2%})")
    print(f" - [Duplicate] Trùng lặp ride_id: {duplicate_ride_id} ({duplicate_ride_id/total_records:.2%})")
    print(f" - [Logic Error] Sai thời gian (ended <= started): {invalid_time_count} ({invalid_time_count/total_records:.2%})")
    if dim_stations_df is not None:
        print(f" - [Join Check] Unmatched start_station_id: {unmatched_count} ({unmatched_count/total_records:.2%})")

    checked_df = df.withColumn(
        "is_valid",
        when(col("ride_id").isNull(), lit(False))
        .when(col("started_at").isNull() | col("ended_at").isNull(), lit(False))
        .when(col("ended_at") <= col("started_at"), lit(False))
        .otherwise(lit(True))
    )

    valid_records = checked_df.filter(col("is_valid") == True).count()
    print(f"Số lượng bản ghi hợp lệ sau kiểm tra: {valid_records} / {total_records}")
    print("=== [Quality Gate] Hoàn tất kiểm tra chất lượng ===")
    return checked_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StationPulse-Dev1-QualityGate") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw_df = spark.read.csv(
        "data/JC-202401-citibike-tripdata.csv",
        header=True,
        schema=TRIPS_SCHEMA
    )
    print("--- Kiểm tra printSchema() ---")
    raw_df.printSchema()

    os.makedirs("data/dim_stations", exist_ok=True)
    dim_stations = build_station_dimension(raw_df)
    dim_stations.toPandas().to_csv("data/dim_stations/station_dimension.csv", index=False)
    print(f"Đã xuất Dimension Stations: {dim_stations.count()} trạm duy nhất.")

    run_quality_check(raw_df, dim_stations)
    spark.stop()
