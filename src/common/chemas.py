
"""
Data Schemas for Citi Bike StationPulse Pipeline
Author: DEV 1
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType
)

# 1. SCHEMA CHO BẢNG FACT TRIPS (Dữ liệu chuyến đi gốc)
# Khóa chính là ride_id (không được phép null)
TRIP_SCHEMA = StructType([
    StructField("ride_id", StringType(), False),           # Mã chuyến đi (Unique Key)
    StructField("rideable_type", StringType(), True),      # Loại xe (classic_bike, electric_bike)
    StructField("started_at", TimestampType(), True),      # Thời gian bắt đầu
    StructField("ended_at", TimestampType(), True),        # Thời gian kết thúc
    StructField("start_station_name", StringType(), True), # Tên trạm xuất phát
    StructField("start_station_id", StringType(), True),   # Mã trạm xuất phát (Foreign Key)
    StructField("end_station_name", StringType(), True),   # Tên trạm kết thúc
    StructField("end_station_id", StringType(), True),     # Mã trạm kết thúc (Foreign Key)
    StructField("start_lat", DoubleType(), True),          # Tọa độ vĩ độ trạm đi
    StructField("start_lng", DoubleType(), True),          # Tọa độ kinh độ trạm đi
    StructField("end_lat", DoubleType(), True),            # Tọa độ vĩ độ trạm đến
    StructField("end_lng", DoubleType(), True),            # Tọa độ kinh độ trạm đến
    StructField("member_casual", StringType(), True)       # Loại khách (member hay casual)
])

# 2. SCHEMA CHO BẢNG DIMENSION STATIONS (Bảng tra cứu thông tin trạm)
# Phục vụ cho bước Fact-Dimension Join bắt buộc của Checkpoint 3 (Slide 15)
STATION_DIM_SCHEMA = StructType([
    StructField("station_id", StringType(), False),        # Mã trạm (Primary Key - Unique 100%)
    StructField("station_name", StringType(), True),       # Tên trạm chuẩn hóa
    StructField("latitude", DoubleType(), True),           # Tọa độ vĩ độ
    StructField("longitude", DoubleType(), True),          # Tọa độ kinh độ
    StructField("region", StringType(), True)              # Khu vực (vd: Jersey City, Hoboken...)
])
