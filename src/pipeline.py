
import glob
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    hour,
    dayofweek,
    unix_timestamp,
    count,
    avg,
    coalesce
)

from src.common.schemas import TRIPS_SCHEMA, STATIONS_SCHEMA


# =========================================================
# 1. KHAI BÁO ĐƯỜNG DẪN
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "citibike_trips"

STATION_PATH = (
    PROJECT_ROOT / "data" / "dim_stations" / "station_dimension.csv"
)

OUTPUT_DIR = PROJECT_ROOT / "output" / "station_flow_kpi"

PREVIEW_DIR = PROJECT_ROOT / "output" / "preview_csv"


# =========================================================
# 2. KHỞI TẠO SPARK
# =========================================================

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("CitiBike-StationPulse-Pipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# =========================================================
# 3. TÌM VÀ GIẢI NÉN FILE ZIP
# =========================================================

def extract_latest_csv():
    pattern = str(
        RAW_DIR
        / "ingest_date=*"
        / "run_*"
        / "JC-202401-citibike-tripdata.csv.zip"
    )

    zip_files = glob.glob(pattern)

    if not zip_files:
        raise FileNotFoundError(
            "Không tìm thấy file ZIP dữ liệu Trips. "
            "Hãy chạy ingest_trips.py trước."
        )

    # Chọn file ZIP được cập nhật gần nhất
    latest_zip = max(zip_files, key=os.path.getmtime)

    print(f"[INPUT] ZIP file: {latest_zip}")

    # Giải nén vào thư mục tạm
    temp_dir = tempfile.TemporaryDirectory()

    with zipfile.ZipFile(latest_zip, "r") as zip_ref:

        csv_files = [
            name for name in zip_ref.namelist()
            if name.lower().endswith(".csv")
            and not name.startswith("__MACOSX/")
        ]

        if len(csv_files) != 1:
            temp_dir.cleanup()
            raise ValueError(
                f"ZIP cần có đúng 1 file CSV dữ liệu, "
                f"nhưng tìm thấy {len(csv_files)} file."
            )

        csv_name = csv_files[0]

        csv_path = os.path.join(
            temp_dir.name,
            "JC-202401-citibike-tripdata.csv"
        )

        with zip_ref.open(csv_name) as source:
            with open(csv_path, "wb") as target:
                shutil.copyfileobj(source, target)

    print(f"[INPUT] CSV extracted: {csv_path}")

    return temp_dir, csv_path


# =========================================================
# 4. READ - ĐỌC DỮ LIỆU
# =========================================================

def read_trips(spark, csv_path):

    print("\n========== [INPUT] READ TRIPS ==========")

    trips = (
        spark.read
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(TRIPS_SCHEMA)
        .csv(csv_path)
    )

    print("[INPUT] Trips schema:")
    trips.printSchema()

    total_rows = trips.count()

    print(f"[INPUT] Total Trips: {total_rows}")

    return trips


def read_stations(spark):

    print("\n========== [INPUT] READ STATIONS ==========")

    if not STATION_PATH.exists():
        raise FileNotFoundError(
            f"Không tìm thấy Station Dimension: {STATION_PATH}"
        )

    stations = (
        spark.read
        .option("header", True)
        .schema(STATIONS_SCHEMA)
        .csv(str(STATION_PATH))
    )

    # Kiểm tra station_id bị null
    null_ids = stations.filter(
        col("station_id").isNull()
    ).count()

    # Kiểm tra station_id bị trùng
    total_stations = stations.count()

    distinct_ids = stations.select(
        "station_id"
    ).distinct().count()

    duplicate_ids = total_stations - distinct_ids

    print(f"[INPUT] Total Stations: {total_stations}")
    print(f"[INPUT] Null station IDs: {null_ids}")
    print(f"[INPUT] Duplicate station IDs: {duplicate_ids}")

    if null_ids > 0 or duplicate_ids > 0:
        raise ValueError(
            "Station Dimension có station_id null hoặc trùng lặp. "
            "Cần kiểm tra dữ liệu trước khi Join."
        )

    return stations


# =========================================================
# 5. CLEAN - LÀM SẠCH DỮ LIỆU
# =========================================================

def clean_trips(trips):

    print("\n========== [CLEAN] CLEAN TRIPS ==========")

    before = trips.count()

    # Loại bỏ bản ghi trùng ride_id
    distinct_trips = trips.dropDuplicates(["ride_id"])

    after_deduplicate = distinct_trips.count()

    duplicate_removed = before - after_deduplicate

    # Loại bỏ bản ghi thiếu thông tin bắt buộc
    # hoặc thời gian kết thúc không hợp lệ
    valid_trips = distinct_trips.filter(
        col("ride_id").isNotNull()
        & col("started_at").isNotNull()
        & col("ended_at").isNotNull()
        & (col("ended_at") > col("started_at"))
    )

    # Tính thời lượng chuyến đi (phút)
    valid_trips = valid_trips.withColumn(
        "trip_duration_min",
        (
            unix_timestamp(col("ended_at"))
            - unix_timestamp(col("started_at"))
        ) / 60.0
    )

    # Loại bỏ chuyến dưới 1 phút nếu đi và đến cùng trạm
    valid_trips = valid_trips.filter(
        ~(
            col("start_station_id").isNotNull()
            & col("end_station_id").isNotNull()
            & (col("start_station_id") == col("end_station_id"))
            & (col("trip_duration_min") < 1)
        )
    )

    after = valid_trips.count()

    print(f"[CLEAN] Rows before cleaning: {before}")
    print(f"[CLEAN] Duplicate ride_id removed: {duplicate_removed}")
    print(f"[CLEAN] Rows after cleaning: {after}")
    print(f"[CLEAN] Total rows removed: {before - after}")

    return valid_trips


# =========================================================
# 6. TRANSFORM - TẠO CÁC CỘT MỚI
# =========================================================

def transform_trips(trips):

    print("\n========== [TRANSFORM] TRANSFORM TRIPS ==========")

    # Phân loại ngày trong tuần và cuối tuần
    trips = trips.withColumn(
        "day_type",
        when(
            dayofweek(col("started_at")).isin(1, 7),
            "weekend"
        ).otherwise("weekday")
    )

    # Lấy giờ bắt đầu chuyến đi
    trips = trips.withColumn(
        "hour_of_day",
        hour(col("started_at"))
    )

    # Lấy giờ kết thúc chuyến đi để tính lượt đến
    trips = trips.withColumn(
        "end_hour_of_day",
        hour(col("ended_at"))
    )

    trips = trips.withColumn(
        "end_day_type",
        when(
            dayofweek(col("ended_at")).isin(1, 7),
            "weekend"
        ).otherwise("weekday")
    )

    print("[TRANSFORM] Created columns:")
    print("- trip_duration_min")
    print("- day_type")
    print("- hour_of_day")
    print("- end_hour_of_day")
    print("- end_day_type")

    return trips


# =========================================================
# 7. JOIN - KẾT HỢP TRIPS VỚI STATION DIMENSION
# =========================================================

def join_stations(trips, stations):

    print("\n========== [JOIN] JOIN STATION DIMENSION ==========")

    before = trips.count()

    # Chuẩn bị thông tin trạm bắt đầu
    start_stations = stations.select(
        col("station_id").alias("start_dim_id"),
        col("station_name").alias("start_dim_name"),
        col("lat").alias("start_dim_lat"),
        col("lng").alias("start_dim_lng"),
        col("capacity").alias("start_dim_capacity")
    )

    # Join trạm bắt đầu
    joined = trips.join(
        start_stations,
        trips["start_station_id"] == start_stations["start_dim_id"],
        "left"
    ).drop("start_dim_id")

    after_start_join = joined.count()

    # Chuẩn bị thông tin trạm kết thúc
    end_stations = stations.select(
        col("station_id").alias("end_dim_id"),
        col("station_name").alias("end_dim_name"),
        col("lat").alias("end_dim_lat"),
        col("lng").alias("end_dim_lng"),
        col("capacity").alias("end_dim_capacity")
    )

    # Join trạm kết thúc
    joined = joined.join(
        end_stations,
        joined["end_station_id"] == end_stations["end_dim_id"],
        "left"
    ).drop("end_dim_id")

    after_end_join = joined.count()

    # Kiểm tra số dòng trước và sau Join
    print(f"[JOIN] Rows before Join: {before}")
    print(f"[JOIN] Rows after start station Join: {after_start_join}")
    print(f"[JOIN] Rows after end station Join: {after_end_join}")

    if before != after_start_join or before != after_end_join:
        raise ValueError(
            "Số dòng thay đổi sau Join. "
            "Có thể Station Dimension chứa station_id trùng."
        )

    # Đếm số trạm bắt đầu không tìm thấy trong Dimension
    unmatched_start = joined.filter(
        col("start_station_id").isNotNull()
        & col("start_dim_name").isNull()
    ).count()

    # Đếm số trạm kết thúc không tìm thấy trong Dimension
    unmatched_end = joined.filter(
        col("end_station_id").isNotNull()
        & col("end_dim_name").isNull()
    ).count()

    print(f"[JOIN] Unmatched start stations: {unmatched_start}")
    print(f"[JOIN] Unmatched end stations: {unmatched_end}")
    print("[JOIN] Join completed successfully.")

    return joined


# =========================================================
# 8. AGGREGATE - TỔNG HỢP DỮ LIỆU
# =========================================================

def enrich_station(df, stations):

    return df.join(
        stations,
        on="station_id",
        how="left"
    )


def aggregate_trips(trips, stations):

    print("\n========== [AGG] AGGREGATE DATA ==========")

    # -----------------------------------------------------
    # 8.1. LƯỢT ĐI THEO TRẠM VÀ GIỜ
    # -----------------------------------------------------

    departures = (
        trips
        .filter(col("start_station_id").isNotNull())
        .groupBy(
            col("start_station_id").alias("station_id"),
            "hour_of_day",
            "day_type"
        )
        .agg(
            count("*").alias("total_departures")
        )
    )

    # -----------------------------------------------------
    # 8.2. LƯỢT ĐẾN THEO TRẠM VÀ GIỜ
    # -----------------------------------------------------

    arrivals = (
        trips
        .filter(col("end_station_id").isNotNull())
        .groupBy(
            col("end_station_id").alias("station_id"),
            col("end_hour_of_day").alias("hour_of_day"),
            col("end_day_type").alias("day_type")
        )
        .agg(
            count("*").alias("total_arrivals")
        )
    )

    # -----------------------------------------------------
    # 8.3. NET FLOW THEO TRẠM VÀ GIỜ
    # -----------------------------------------------------

    hourly_flow = (
        departures.alias("d")
        .join(
            arrivals.alias("a"),
            (
                (col("d.station_id") == col("a.station_id"))
                & (col("d.hour_of_day") == col("a.hour_of_day"))
                & (col("d.day_type") == col("a.day_type"))
            ),
            "full_outer"
        )
        .select(
            coalesce(
                col("d.station_id"),
                col("a.station_id")
            ).alias("station_id"),

            coalesce(
                col("d.hour_of_day"),
                col("a.hour_of_day")
            ).alias("hour_of_day"),

            coalesce(
                col("d.day_type"),
                col("a.day_type")
            ).alias("day_type"),

            coalesce(
                col("total_departures"),
                lit(0)
            ).alias("total_departures"),

            coalesce(
                col("total_arrivals"),
                lit(0)
            ).alias("total_arrivals")
        )
        .withColumn(
            "net_flow",
            col("total_arrivals") - col("total_departures")
        )
    )

    hourly_flow = enrich_station(hourly_flow, stations)

    # -----------------------------------------------------
    # 8.4. NET FLOW THEO TRẠM TRONG TOÀN THÁNG
    # -----------------------------------------------------

    monthly_departures = (
        trips
        .filter(col("start_station_id").isNotNull())
        .groupBy(
            col("start_station_id").alias("station_id")
        )
        .agg(
            count("*").alias("total_departures")
        )
    )

    monthly_arrivals = (
        trips
        .filter(col("end_station_id").isNotNull())
        .groupBy(
            col("end_station_id").alias("station_id")
        )
        .agg(
            count("*").alias("total_arrivals")
        )
    )

    monthly_flow = (
        monthly_departures.alias("d")
        .join(
            monthly_arrivals.alias("a"),
            col("d.station_id") == col("a.station_id"),
            "full_outer"
        )
        .select(
            coalesce(
                col("d.station_id"),
                col("a.station_id")
            ).alias("station_id"),

            coalesce(
                col("total_departures"),
                lit(0)
            ).alias("total_departures"),

            coalesce(
                col("total_arrivals"),
                lit(0)
            ).alias("total_arrivals")
        )
        .withColumn(
            "net_flow",
            col("total_arrivals") - col("total_departures")
        )
    )

    monthly_flow = enrich_station(monthly_flow, stations)

    # -----------------------------------------------------
    # 8.5. THỜI LƯỢNG THEO MEMBER / CASUAL
    # -----------------------------------------------------

    usage_by_member_casual = (
        trips
        .groupBy("member_casual")
        .agg(
            count("*").alias("total_trips"),
            avg("trip_duration_min").alias("avg_duration_min")
        )
    )

    print("[AGG] Hourly station flow completed.")
    print("[AGG] Monthly station flow completed.")
    print("[AGG] Member/Casual aggregation completed.")

    print("[AGG] Hourly flow sample:")
    hourly_flow.show(5, truncate=False)

    print("[AGG] Monthly flow sample:")
    monthly_flow.show(5, truncate=False)

    print("[AGG] Member/Casual sample:")
    usage_by_member_casual.show(truncate=False)

    return hourly_flow, monthly_flow, usage_by_member_casual


# =========================================================
# 9. OUTPUT - GHI PARQUET VÀ CSV PREVIEW
# =========================================================

def write_one_output(df, name):

    parquet_path = OUTPUT_DIR / name
    csv_path = PREVIEW_DIR / name

    # Ghi toàn bộ dữ liệu dạng Parquet
    (
        df.write
        .mode("overwrite")
        .parquet(str(parquet_path))
    )

    # Ghi tối đa 50 dòng để xem trước
    (
        df.limit(50)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(csv_path))
    )

    print(f"[OUTPUT] Parquet: {parquet_path}")
    print(f"[OUTPUT] CSV preview: {csv_path}")


def write_output(hourly_flow, monthly_flow, usage):

    print("\n========== [OUTPUT] WRITE DATA ==========")

    write_one_output(
        hourly_flow,
        "hourly_station_flow"
    )

    write_one_output(
        monthly_flow,
        "monthly_station_flow"
    )

    write_one_output(
        usage,
        "usage_by_member_casual"
    )

    print("[OUTPUT] All outputs written successfully.")


# =========================================================
# 10. MAIN - CHẠY TOÀN BỘ PIPELINE
# =========================================================

def main():

    spark = create_spark_session()
    temp_dir = None

    try:

        # READ
        temp_dir, csv_path = extract_latest_csv()

        trips = read_trips(spark, csv_path)

        stations = read_stations(spark)

        # CLEAN
        cleaned_trips = clean_trips(trips).cache()

        # TRANSFORM
        transformed_trips = transform_trips(cleaned_trips)

        # JOIN
        joined_trips = join_stations(
            transformed_trips,
            stations
        )

        # AGGREGATE
        hourly_flow, monthly_flow, usage = aggregate_trips(
            joined_trips,
            stations
        )

        # OUTPUT
        write_output(
            hourly_flow,
            monthly_flow,
            usage
        )

        print("\n========== PIPELINE COMPLETED ==========")

    except Exception as error:

        print("\n[ERROR] Pipeline failed:")
        print(error)

        raise

    finally:

        if temp_dir is not None:
            temp_dir.cleanup()

        spark.stop()


if __name__ == "__main__":
    main()
