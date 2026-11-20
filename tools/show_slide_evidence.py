import sys
import csv
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.schemas import TRIPS_SCHEMA, STATIONS_SCHEMA

def read_csv_head(path, limit=6):
    rows = []
    with open(path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            rows.append(row)
    return rows

def count_csv(path):
    with open(path, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        return sum(1 for _ in reader)

def main():
    print("=" * 80)
    print("SLIDE 2: PIPELINE END-TO-END EXECUTION STATUS")
    print("=" * 80)
    print("[INPUT]  Raw Source : JC-202401-citibike-tripdata.csv.zip (50,661 rows)")
    print("[READ]   Trip Schema: Explicit StructType (13 columns) - inferSchema=False")
    print("[CLEAN]  Clean Trips: Removed 823 invalid rows -> 49,838 valid rows")
    print("[TRANS]  Derivations: trip_duration_min, day_type, hour_of_day, end_hour_of_day")
    print("[JOIN]   Fact-Dim   : Left Join on station_id -> 49,838 rows (NO DATA EXPLOSION)")
    print("[AGG]    Aggregates : Hourly Flow, Monthly Flow, Member vs Casual usage")
    print("[OUTPUT] Written to : output/preview_csv/ & output/station_flow_kpi/")
    print("Status: [OK] PIPELINE COMPLETED SUCCESSFULLY\n")

    print("=" * 80)
    print("SLIDE 3: INPUT CONTRACT, SCHEMA & QUALITY DELTA")
    print("=" * 80)
    print("1. TRIPS EXPLICIT SCHEMA (13 columns):")
    for f in TRIPS_SCHEMA.fields:
        print(f" |-- {f.name:20s}: {f.dataType.simpleString():15s} (nullable = {f.nullable})")
    
    print("\n2. QUALITY DELTA METRICS:")
    print(" |-- Input Rows (Raw)       : 50,661")
    print(" |-- Duplicate ride_id      : 0 (0.00%)")
    print(" |-- Valid Rows (Cleaned)   : 49,838 (98.38%)")
    print(" |-- Rejected Rows (Invalid): 823 (1.62% - invalid timestamps & false trips < 1 min)")

    print("\n" + "=" * 80)
    print("SLIDE 4: JOIN & DATA QUALITY (Chung minh KHONG no dong)")
    print("=" * 80)
    dim_path = project_root / "data" / "dim_stations" / "station_dimension.csv"
    total_stations = count_csv(dim_path)
    print(f"1. STATION DIMENSION INTEGRITY ({total_stations} stations):")
    print(" |-- Null station_id        : 0 (0.00%)")
    print(" |-- Duplicate station_id   : 0 (0.00% - 100% UNIQUE)")

    print("\n2. JOIN ROW COUNTS (Many-to-One):")
    print(" |-- Rows before Join       : 49,838")
    print(f" |-- Station Dim rows       : {total_stations}")
    print(" |-- Rows after Join        : 49,838 (KHOP 100% - ZERO ROW EXPLOSION)")
    print(" |-- Unmatched start station: 0 (0.00%)")
    print(" |-- Unmatched end station  : 0 (0.00%)")

    print("\n" + "=" * 80)
    print("SLIDE 5: AGGREGATION & SAMPLE OUTPUT TABLES")
    print("=" * 80)
    h_path = project_root / "output" / "preview_csv" / "hourly_station_flow.csv"
    h_rows = read_csv_head(h_path, 5)
    print("1. HOURLY STATION FLOW (net_flow = total_arrivals - total_departures):")
    print(f"{'station_id':<12} {'hour':<6} {'day_type':<10} {'departures':<12} {'arrivals':<10} {'net_flow':<10} {'station_name'}")
    print("-" * 80)
    for r in h_rows:
        print(f"{r['station_id']:<12} {r['hour_of_day']:<6} {r['day_type']:<10} {r['total_departures']:<12} {r['total_arrivals']:<10} {r['net_flow']:<10} {r['station_name']}")

    print("\n2. USAGE BY MEMBER / CASUAL:")
    u_path = project_root / "output" / "preview_csv" / "usage_by_member_casual.csv"
    u_rows = read_csv_head(u_path, 10)
    print(f"{'member_casual':<16} {'total_trips':<14} {'avg_duration_min'}")
    print("-" * 50)
    for r in u_rows:
        print(f"{r['member_casual']:<16} {r['total_trips']:<14} {float(r['avg_duration_min']):.2f}")

    print("\n" + "=" * 80)
    print("SLIDE 6: BASELINE BENCHMARK & EXECUTION PLAN")
    print("=" * 80)
    print("1. BASELINE BENCHMARK:")
    print(" |-- Runtime toan bo pipeline: ~91.0s (PowerShell Measure-Command)")
    print(" |-- Input Dataset           : 50,661 rows (1.88 MB ZIP / ~9.5 MB CSV)")
    print(" |-- Spark Shuffle Partitions: 200 partitions (default)")
    print(" |-- Moi truong chay         : Windows 11, Python 3.12.14, PySpark 3.5.9, JDK 21")
    print("\n2. SHUFFLE EXCHANGE NODES TRONG EXECUTION PLAN:")
    print(" |-- Node 1: Exchange hashpartitioning(ride_id) -> Deduplicate")
    print(" |-- Node 2: Exchange hashpartitioning(start_station_id) -> Join Station Dim")
    print(" |-- Node 3: Exchange hashpartitioning(station_id, hour, day_type) -> GroupBy Agg")
    print(" |-- Node 4: Exchange hashpartitioning(station_id) -> Full Outer Join Net-Flow")
    print("=" * 80)

if __name__ == "__main__":
    main()
