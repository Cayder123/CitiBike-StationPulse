# Báo cáo Checkpoint 3: Xây dựng Spark Processing Pipeline & Baseline Performance

> **Dự án:** CitiBike-StationPulse  
> **Giai đoạn:** Checkpoint 3 — Data Processing & Pipeline Optimization Baseline  
> **Dữ liệu thực nghiệm:** Jersey City tháng 01/2024 (`JC-202401-citibike-tripdata.csv.zip`)  
> **Thời gian đo kiểm:** 13/11/2026

---

## 1. Giới thiệu & Mục tiêu Checkpoint 3

Checkpoint 3 tập trung vào việc chuyển đổi luồng dữ liệu thô (Raw) thành dữ liệu phân tích chuẩn hóa thông qua Apache Spark / PySpark. Các mục tiêu chính bao gồm:

1. **Hoàn thiện end-to-end Pipeline (`src/pipeline.py`):**
   - Đọc dữ liệu từ Raw ZIP mà không cần giải nén thủ công vĩnh viễn.
   - Làm sạch (Cleaning): Loại bỏ trùng lặp `ride_id`, loại bỏ chuyến có thời gian kết thúc trước thời gian bắt đầu, loại bỏ các chuyến đi ảo (dưới 1 phút và cùng trạm đi/đến).
   - Biến đổi (Feature Transformation): Tính thời lượng chuyến (`trip_duration_min`), phân loại ngày (`day_type`: weekday / weekend), trích xuất khung giờ bắt đầu (`hour_of_day`) và kết thúc (`end_hour_of_day`).
   - Kết hợp (Join): Kết hợp đa chiều với bảng `station_dimension.csv` (127 trạm).
   - Tổng hợp (Aggregation): Tính toán lưu lượng theo trạm & khung giờ (`hourly_station_flow`), tổng lưu lượng tháng (`monthly_station_flow`), và phân tích hành vi người dùng (`usage_by_member_casual`).
2. **Đo lường hiệu năng Baseline:** Ghi nhận thời gian chạy chi tiết từng công đoạn của pipeline chưa tối ưu.
3. **Phân tích Execution Plan (`explain()`):** Kiểm tra Physical Plan, Logical Plan và chỉ rõ các toán tử tính toán, chi phí shuffle và join.
4. **Nhận diện điểm nghẽn (Bottlenecks):** Đặt tiền đề cho các cải tiến tối ưu hóa ở checkpoint tiếp theo.
5. **Kiểm thử tự động (Unit Test Suite):** Xây dựng `tests/test_pipeline.py` kiểm chứng toàn bộ hàm xử lý.

---

## 2. Môi trường thực nghiệm

| Thành phần | Thông tin cấu hình |
|---|---|
| **Hệ điều hành** | Windows 11 Home 64-bit |
| **Python** | 3.13.2 (môi trường `.venv`) |
| **Java Runtime** | Eclipse Temurin OpenJDK 21.0.13+9 LTS |
| **Apache Spark** | PySpark 3.5.5+ (chế độ `local[*]`) |
| **Múi giờ Spark** | `America/New_York` (đồng bộ với dữ liệu Citi Bike NY/NJ) |
| **File thô đầu vào** | `JC-202401-citibike-tripdata.csv.zip` (1.883.296 bytes) |
| **Bảng trạm chiều** | `data/dim_stations/station_dimension.csv` (127 trạm) |

---

## 3. Kết quả Xử lý Dữ liệu

Quá trình thực thi `src/pipeline.py` ghi nhận các chỉ số dữ liệu cụ thể:

### 3.1. Làm sạch & Lọc dữ liệu (Data Cleaning)
- **Tổng số dòng thô ban đầu:** `50.661` dòng.
- **Trùng lặp mã chuyến (`ride_id`):** `0` dòng.
- **Số dòng bị loại bỏ:** `823` dòng (~1,62%).
  - Các dòng có thời lượng không hợp lệ (`ended_at <= started_at`).
  - Các dòng có cùng trạm đi và trạm đến với thời lượng `< 1 phút` (chuyến đi lỗi/hủy xe).
- **Số dòng hợp lệ sau làm sạch:** `49.838` dòng.

### 3.2. Kết hợp với Station Dimension (Join)
- **Tổng số trạm trong Dimension:** `127` trạm (100% trạm có `station_id` duy nhất, không null).
- **Unmatched start stations:** `0`
- **Unmatched end stations:** `0`
- **Bảo toàn số dòng sau Join:** `49.838` dòng (không phát sinh nhân bản dòng).

### 3.3. Kết quả Tổng hợp (Aggregations)
1. **Hourly Station Flow (`hourly_station_flow.csv`):** `3.713` dòng.
   - Thể hiện chi tiết số lượt đi (`total_departures`), số lượt đến (`total_arrivals`), và `net_flow` (lượt đến - lượt đi) cho từng trạm theo 24 khung giờ và loại ngày (`weekday` / `weekend`).
2. **Monthly Station Flow (`monthly_station_flow.csv`):** `127` dòng.
   - Tổng hợp toàn bộ hoạt động của từng trạm trong tháng 01/2024.
3. **Usage by Member / Casual (`usage_by_member_casual.csv`):**
   - **Hội viên thường niên (`member`):** 42.014 chuyến (~84,3%), thời lượng trung bình **7,89 phút**.
   - **Khách vãng lai (`casual`):** 7.824 chuyến (~15,7%), thời lượng trung bình **13,32 phút** (cao hơn đáng kể so với hội viên).

---

## 4. Đo lường Runtime Baseline (Performance Benchmark)

Dưới đây là bảng thời gian thực thi chi tiết của baseline pipeline trên máy thực nghiệm:

| Giai đoạn Pipeline | Thời gian thực thi (s) | Tỷ lệ (%) | Mô tả công việc |
|---|---:|---:|---|
| **1. Read Data** | 10.25 s | 1.0% | Đọc file ZIP tạm thời, nạp CSV với schema tường minh và nạp Station Dimension |
| **2. Clean Data** | 19.35 s | 2.0% | `dropDuplicates(["ride_id"])`, lọc timestamp hợp lệ, lọc circular trips `< 1 min`, `.cache()` |
| **3. Transform & Join** | 12.04 s | 1.2% | Tạo cột `day_type`, `hour_of_day`, `end_hour_of_day`, Left Join với 127 trạm |
| **4. Aggregate Data** | 369.91 s | 37.3% | Gom nhóm lượt đi/đến, Full Outer Join tính net-flow, enrich trạm, tính stats member/casual |
| **5. Write Outputs** | 580.50 s | 58.5% | Xuất kết quả CSV và Parquet (fallback driver iterator trên Windows) |
| **Tổng thời gian chạy** | **992.06 s** (~16.5 phút) | **100.0%** | Toàn bộ pipeline từ raw data đến thư mục `output/` |

---

## 5. Phân tích Spark Execution Plan (`explain()`)

Để hiểu rõ cách Spark thực thi truy vấn và nguyên nhân tiêu tốn thời gian, lệnh `explain(extended=True)` đã được xuất cho các luồng tổng hợp chính.

### 5.1. Physical Plan của `hourly_station_flow`

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(10) Project [station_id#3442, hour_of_day#3443, day_type#3444, total_departures#3445L, total_arrivals#3446L, net_flow#3452L, station_name#3460, lat#3461, lng#3462, capacity#3463]
   +- *(10) BroadcastHashJoin [station_id#3442], [station_id#3459], LeftOuter, BuildRight, false
      :- *(10) Project [station_id#3442, hour_of_day#3443, day_type#3444, total_departures#3445L, total_arrivals#3446L, (total_arrivals#3446L - total_departures#3445L) AS net_flow#3452L]
      :  +- *(10) Project [coalesce(station_id#3332, station_id#3365) AS station_id#3442, coalesce(hour_of_day#821, hour_of_day#3366) AS hour_of_day#3443, coalesce(day_type#805, day_type#3367) AS day_type#3444, coalesce(total_departures#3360L, 0) AS total_departures#3445L, coalesce(total_arrivals#3395L, 0) AS total_arrivals#3446L]
      :     +- *(10) SortMergeJoin [station_id#3332, hour_of_day#821, day_type#805], [station_id#3365, hour_of_day#3366, day_type#3367], FullOuter
      :        :- *(8) Sort [station_id#3332 ASC NULLS FIRST, hour_of_day#821 ASC NULLS FIRST, day_type#805 ASC NULLS FIRST], false, 0
      :        :  +- *(8) HashAggregate(keys=[start_station_id#5, hour_of_day#821, day_type#805], functions=[count(1)], output=[station_id#3332, hour_of_day#821, day_type#805, total_departures#3360L])
      :        :     +- AQEShuffleRead coalesced
      :        :        +- ShuffleQueryStage 10
      :        :           +- Exchange hashpartitioning(start_station_id#5, hour_of_day#821, day_type#805, 200), ENSURE_REQUIREMENTS, [plan_id=5875]
      :        :              +- *(6) HashAggregate(keys=[start_station_id#5, hour_of_day#821, day_type#805], functions=[partial_count(1)], output=[start_station_id#5, hour_of_day#821, day_type#805, count#4080L])
      :        :                 +- *(6) Project [start_station_id#5, day_type#805, hour_of_day#821]
      :        :                    +- *(6) BroadcastHashJoin [end_station_id#7], [end_dim_id#1704], LeftOuter, BuildRight, false
      :        :                       :- *(6) Project [start_station_id#5, end_station_id#7, day_type#805, hour_of_day#821]
      :        :                       :  +- *(6) BroadcastHashJoin [start_station_id#5], [start_dim_id#1248], LeftOuter, BuildRight, false
      :        :                       :     :- *(6) Project [start_station_id#5, end_station_id#7, CASE WHEN dayofweek(cast(started_at#2 as date)) IN (1,7) THEN weekend ELSE weekday END AS day_type#805, hour(started_at#2, Some(America/New_York)) AS hour_of_day#821]
      :        :                       :     :  +- *(6) Filter isnotnull(start_station_id#5)
      :        :                       :     :     +- TableCacheQueryStage 0
      :        :                       :     :        +- InMemoryTableScan [end_station_id#7, start_station_id#5, started_at#2], [isnotnull(start_station_id#5)]
```

### 5.2. Các điểm kỹ thuật nổi bật từ Physical Plan

1. **TableCacheQueryStage & InMemoryTableScan:**
   - Dữ liệu `cleaned_trips` được lưu trong bộ nhớ đệm (`.cache()`), giúp các nhánh tính toán `departures` và `arrivals` không phải đọc lại và giải nén CSV thô nhiều lần.
2. **BroadcastHashJoin đối với Station Dimension:**
   - Spark Catalyst Optimizer tự động nhận diện `station_dimension.csv` (127 dòng, dung lượng nhỏ hơn `spark.sql.autoBroadcastJoinThreshold` mặc định 10MB) và chuyển đổi các phép join trạm bắt đầu, trạm kết thúc và enrich trạm thành `BroadcastHashJoin`.
   - Điều này triệt tiêu hoàn toàn nhu cầu Shuffle dữ liệu chuyến đi lớn cho các phép join trạm.
3. **SortMergeJoin trong Full Outer Join:**
   - Để tính `net_flow`, pipeline thực hiện phép `full_outer` giữa `departures` (nhóm theo start_station) và `arrivals` (nhóm theo end_station).
   - Do điều kiện join là đẳng thức trên 3 khóa `[station_id, hour_of_day, day_type]`, Spark sử dụng `SortMergeJoin`. Trước đó, dữ liệu của cả 2 nhánh đều phải đi qua `Exchange hashpartitioning` (shuffle) và `Sort`.

---

## 6. Nhận diện các điểm nghẽn hiệu năng (Bottlenecks) & Hướng tối ưu

Dựa trên baseline runtime (992.06s) và Physical Plan, các điểm nghẽn lớn nhất đã được xác định:

### 1. Số lượng phân vùng Shuffle mặc định quá lớn (`spark.sql.shuffle.partitions = 200`)
- **Vấn đề:** Với dataset Jersey City 1 tháng (~50.000 dòng), 200 phân vùng dẫn đến mỗi phân vùng chỉ chứa trung bình ~250 dòng dữ liệu.
- **Hệ quả:** Spark phải điều phối (schedule), quản lý và đồng bộ hóa hàng ngàn task nhỏ qua 200 partition, gây lãng phí CPU và I/O overhead lớn khi chạy local trên một máy.
- **Giải pháp tối ưu:** Điều chỉnh cấu hình `spark.sql.shuffle.partitions` xuống `4` đến `8` (phù hợp với số CPU core của máy local).

### 2. Các hành động (`Actions`) trung gian không cần thiết
- **Vấn đề:** Trong mã nguồn `pipeline.py`, việc gọi liên tiếp các lệnh `count()`, `show(5)` và in log chi tiết ở mỗi bước (read, clean, join, aggregate) kích hoạt nhiều job Spark độc lập, buộc Spark phải duyệt lại DAG nhiều lần.
- **Giải pháp tối ưu:** Hạn chế `count()` ở từng bước trung gian hoặc cấu hình log level thích hợp, gộp các luồng tính toán để Spark tối ưu hóa toàn diện trước khi thực thi action cuối cùng.

### 3. Cơ chế xuất file qua Driver Iterator (`toLocalIterator`)
- **Vấn đề:** Giai đoạn ghi dữ liệu chiếm tới **58.5% tổng runtime (580.5s)** do việc ghi CSV sử dụng `toLocalIterator()` lặp từng dòng từ executor về driver thông qua Python loop nhằm tránh lỗi thiếu `winutils.exe` trên Windows.
- **Giải pháp tối ưu:** Cung cấp giải pháp lưu trữ song song (Partitioned writing hoặc cấu hình Hadoop winutils native) để Spark ghi trực tiếp từ các executor ra đĩa song song, giảm thời gian xuất file xuống dưới 5 giây.

---

## 7. Kiểm thử đơn vị tự động (Unit Test Suite)

Tất cả các thành phần logic trong pipeline đã được bao phủ bằng bộ kiểm thử tự động tại `tests/test_pipeline.py`:

```powershell
pytest -v tests/test_pipeline.py
```

### Kết quả kiểm thử:
- `test_clean_trips`: **PASSED** — Kiểm tra loại bỏ chuyến trùng lặp, thời lượng âm/lỗi, và chuyến đi cùng trạm < 1 phút.
- `test_transform_trips`: **PASSED** — Kiểm tra tạo chính xác `trip_duration_min`, `day_type`, `hour_of_day`, `end_hour_of_day`, `end_day_type`.
- `test_join_stations`: **PASSED** — Kiểm tra join với Station Dimension bảo toàn số lượng bản ghi và bổ sung metadata trạm.
- `test_aggregate_trips`: **PASSED** — Kiểm tra tính toán `total_departures`, `total_arrivals`, `net_flow` và thống kê `member_casual`.
- `test_write_local_csv`: **PASSED** — Kiểm tra xuất file CSV độc lập không phụ thuộc HADOOP_HOME.

**Tổng kết:** `5 passed in 93.20s` (Toàn bộ 28 bài test trong repository đều vượt qua thành công: 23 checkpoint cũ + 5 checkpoint mới).

---

## 8. Kết luận Checkpoint 3

Checkpoint 3 đã hoàn thành đầy đủ tất cả các yêu cầu kỹ thuật:
1. Xây dựng pipeline xử lý dữ liệu Spark hoàn chỉnh, chạy thực tế và có thể tái lập.
2. Thiết lập bộ dữ liệu kết quả phân tích theo giờ, theo tháng và theo đối tượng sử dụng tại `output/preview_csv/`.
3. Đo lường chính xác runtime baseline và bóc tách cấu trúc Physical Plan.
4. Xác định rõ ràng các điểm nghẽn và giải pháp tối ưu hóa làm tiền đề cho Checkpoint tiếp theo.

