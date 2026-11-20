# NHẬT KÝ THỰC NGHIỆM TỐI ƯU HÓA PIPELINE (EXPERIMENT LOG)

- **Dự án:** CitiBike-StationPulse (Đồ án 4 - Buổi 10)
- **Repository:** https://github.com/Cayder123/CitiBike-StationPulse
- **Môi trường thực nghiệm:** Windows 11, Python 3.12.14, JDK 21.0.8, PySpark 3.5.9 (`local[*]`)
- **Quy tắc thực nghiệm:** One-Change-At-A-Time (Slide 13). Mỗi thử nghiệm chạy lặp lại 3 lần, loại trừ warm-up, báo cáo Median Runtime và độ biến động (CV%).

---

## 1. Bảng Tổng Hợp Các Thử Nghiệm

| EXP ID | Commit | Mô tả thay đổi (Lever) | Giả thuyết tối ưu (Hypothesis) | Runtime (Median) | Runtime (Mean ± Std) | Tín hiệu Execution Plan | Correctness | Quyết định |
| :--- | :--- | :--- | :--- | :---: | :---: | :--- | :---: | :---: |
| **E00** | `9cb41a4` | **Baseline:** 200 partitions, SortMerge/BHJ mặc định, không cache | Điểm chuẩn tham chiếu ban đầu | **28.68s** | 29.14s ± 0.84s (CV: 2.88%) | 200 tasks/stage, Exchange hashpartitioning | PASS (49.838 rows) | **BASELINE** |
| **E01** | `b227312` | **Lever 1:** `spark.sql.shuffle.partitions = 4` | Giảm 200 task siêu nhỏ xuống 4 task (khớp CPU core) để triệt tiêu scheduling overhead | **22.63s** *(giảm 21.1%)* | 22.57s ± 0.50s (CV: 2.22%) | Số partitions của các nút Exchange giảm từ 200 xuống 4 | PASS (49.838 rows) | **KEEP** |
| **E02** | `b227312` | **Lever 2:** Áp dụng `broadcast(stations)` | Bảng trạm chỉ 127 dòng (<10KB), broadcast sang executor giúp xóa bỏ Shuffle Exchange phía bảng trạm | **21.97s** *(giảm 23.4%)* | 21.88s ± 0.19s (CV: 0.87%) | Nút Join chuyển thành BroadcastHashJoin rõ ràng, giảm data movement | PASS (49.838 rows) | **KEEP** |
| **E03** | `b227312` | **Lever 3:** Áp dụng `cleaned_trips.cache()` (Full Optimized) | Bảng cleaned_trips được dùng cho 3 aggregation; cache trong bộ nhớ giúp tránh recompute giải nén ZIP | **20.04s** *(giảm 30.1%)* | 20.17s ± 0.24s (CV: 1.19%) | In-memory cache hit cho các nhánh aggregation tiếp theo | PASS (49.838 rows) | **KEEP (FINAL)** |

---

## 2. Chi Tiết Từng Thử Nghiệm

### Thử nghiệm E00: Baseline (Phiên bản Buổi 8 / Checkpoint 3)
* **Thiết lập:** `spark.sql.shuffle.partitions = 200`, không cấu hình cache, chạy qua `src/pipeline.py`.
* **Quan sát:** Spark chia thành 200 partition cho mỗi stage shuffle. Với tập dữ liệu 50.661 dòng (1.88 MB nén), mỗi partition chỉ chứa ~250 dòng. Chi phí khởi tạo và quản lý 200 task trên JVM cục bộ chiếm phần lớn thời gian chạy.
* **Thời gian 3 lần chạy:** `[30.11s, 28.64s, 28.68s]` $\rightarrow$ **Median: 28.68s**.

### Thử nghiệm E01: Lever 1 – Shuffle Partitions Tuning
* **Giả thuyết:** Nếu giảm `shuffle.partitions` từ 200 xuống 4 (phù hợp với số core CPU máy local), thì scheduling overhead sẽ giảm đáng kể trong khi kết quả không đổi.
* **Thay đổi:** Thêm `--partitions 4` vào SparkSession.
* **Tín hiệu:** Số task ở Stage 32, Stage 67 giảm từ 200 xuống đúng 4 task.
* **Thời gian 3 lần chạy:** `[22.05s, 22.63s, 23.04s]` $\rightarrow$ **Median: 22.63s** *(nhanh hơn 6.05 giây, giảm 21.1%)*.
* **Quyết định:** **KEEP** vì hiệu năng cải thiện rõ rệt, CV% chỉ 2.22%.

### Thử nghiệm E02: Lever 2 – Explicit Broadcast Hash Join
* **Giả thuyết:** Bảng `station_dimension.csv` chỉ có 127 dòng (dung lượng ~7.5 KB), hoàn toàn nằm trong ngưỡng broadcast an toàn (<10MB). Khi ép kiểu `broadcast(stations)`, Spark sẽ copy bảng nhỏ này tới các executor, xóa bỏ hoàn toàn Shuffle Exchange của bảng trạm.
* **Thay đổi:** Thêm cờ `--broadcast` áp dụng `trips.join(broadcast(stations), ...)`.
* **Tín hiệu:** Execution plan xuất hiện `BroadcastExchange` và `BroadcastHashJoin`.
* **Thời gian 3 lần chạy:** `[21.66s, 21.97s, 22.01s]` $\rightarrow$ **Median: 21.97s** *(nhanh hơn thêm 0.66 giây)*.
* **Quyết định:** **KEEP** vì độ lệch chuẩn cực thấp (StdDev: 0.19s, CV: 0.87%).

### Thử nghiệm E03: Lever 3 – Caching Cleaned Trips
* **Giả thuyết:** DataFrame `cleaned_trips` được tái sử dụng đồng thời cho 3 phép tính: `hourly_flow`, `monthly_flow`, và `usage_by_member_casual`. Việc gọi `.cache()` sau bước làm sạch sẽ ngăn Spark quét lại file ZIP và tính lại các biểu thức regex/timestamp.
* **Thay đổi:** Thêm cờ `--cache` kích hoạt `cleaned_trips.cache()`.
* **Thời gian 3 lần chạy:** `[20.45s, 20.03s, 20.04s]` $\rightarrow$ **Median: 20.04s** *(nhanh hơn 8.64 giây so với Baseline, giảm tổng cộng 30.1%)*.
* **Quyết định:** **KEEP** làm cấu hình tối ưu chính thức cho Đồ án 4.

---

## 3. Kiểm Chứng Tính Đúng Đắn (Correctness Verification)
Toàn bộ 4 thử nghiệm đều được kiểm chứng độc lập bằng script `src/verify_correctness.py`:
- Số dòng `valid_trips`: 49.838 dòng (khớp 100%).
- Bảng `hourly_station_flow`: 3.713 dòng; Tổng departures: 49.837; Tổng arrivals: 49.671; Net-flow checksum: -166.
- Bảng `monthly_station_flow`: 127 dòng; Tổng departures: 49.837; Tổng arrivals: 49.671.
- Bảng `usage_by_member_casual`: 2 dòng (Member: 42.014 chuyến, Casual: 7.824 chuyến).
- Kết luận: **PASS 100% - ZERO REGRESSION.**
