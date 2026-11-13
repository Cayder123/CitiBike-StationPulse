from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import col, first, lit

TRIPS_SCHEMA = StructType([
    StructField("ride_id", StringType(), nullable=False),
    StructField("rideable_type", StringType(), nullable=True),
    StructField("started_at", TimestampType(), nullable=True),
    StructField("ended_at", TimestampType(), nullable=True),
    StructField("start_station_name", StringType(), nullable=True),
    StructField("start_station_id", StringType(), nullable=True),
    StructField("end_station_name", StringType(), nullable=True),
    StructField("end_station_id", StringType(), nullable=True),
    StructField("start_lat", DoubleType(), nullable=True),
    StructField("start_lng", DoubleType(), nullable=True),
    StructField("end_lat", DoubleType(), nullable=True),
    StructField("end_lng", DoubleType(), nullable=True),
    StructField("member_casual", StringType(), nullable=True)
])

STATIONS_SCHEMA = StructType([
    StructField("station_id", StringType(), nullable=False),
    StructField("station_name", StringType(), nullable=True),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lng", DoubleType(), nullable=True),
    StructField("capacity", IntegerType(), nullable=True)
])

def build_station_dimension(trips_df):
    start_stations = trips_df.select(
        col("start_station_id").alias("station_id"),
        col("start_station_name").alias("station_name"),
        col("start_lat").alias("lat"),
        col("start_lng").alias("lng")
    ).filter(col("station_id").isNotNull())

    end_stations = trips_df.select(
        col("end_station_id").alias("station_id"),
        col("end_station_name").alias("station_name"),
        col("end_lat").alias("lat"),
        col("end_lng").alias("lng")
    ).filter(col("station_id").isNotNull())

    dim_stations = (
        start_stations.union(end_stations)
        .groupBy("station_id")
        .agg(
            first("station_name", ignorenulls=True).alias("station_name"),
            first("lat", ignorenulls=True).alias("lat"),
            first("lng", ignorenulls=True).alias("lng")
        )
        .withColumn("capacity", lit(20).cast(IntegerType()))
    )
    return dim_stations
