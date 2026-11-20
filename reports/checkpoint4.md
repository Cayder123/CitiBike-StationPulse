# BÁO CÁO TIẾN ĐỘ CHECKPOINT 4: BENCHMARK & TỐI ƯU PIPELINE

- **Tên đồ án:** CitiBike-StationPulse: Pipeline Phân Tích Lưu Lượng Trạm Citi Bike Bằng PySpark
- **Học phần:** Dữ Liệu Lớn (Big Data) — Buổi 10 • Checkpoint 4
- **Repository:** https://github.com/Cayder123/CitiBike-StationPulse
- **Mã Commit:** `b227312` (Release Tag: `checkpoint-4`)
- **Nhóm thực hiện:** Nhóm 11

---

## 1. Tóm Tắt Kết Quả Tối Ưu (Before vs After)

Tuân thủ nghiêm ngặt nguyên tắc **One-Change-At-A-Time** (Slide 13), nhóm đã thực hiện chuỗi thực nghiệm đo lường khoa học:

| Cấu hình thử nghiệm | Thời gian (Median) | Thời gian (Mean) | Độ biến động (CV%) | Tỷ lệ cải thiện | Trạng thái Correctness |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Baseline (E00)** | **28.68s** | 29.14s | 2.88% | Gốc tham chiếu | PASS (49.838 rows) |
| **Lever 1: Shuffle=4 (E01)** | **22.63s** | 22.57s | 2.22% | Nhanh hơn 21.1% | PASS (49.838 rows) |
| **Lever 2: Broadcast Join (E02)** | **21.97s** | 21.88s | 0.87% | Nhanh hơn 23.4% | PASS (49.838 rows) |
| **Lever 3: Cache (E03 - Final)** | **20.04s** | 20.17s | 1.19% | **Nhanh hơn 30.1%** | **PASS (49.838 rows)** |

> **Kết luận hiệu năng:** Pipeline tối ưu đạt thời gian trung vị **20.04 giây** so với Baseline **28.68 giây** (giảm 30.1% thời gian thực thi). Độ biến động giữa các lần chạy rất thấp ($CV \le 2.88\%$), chứng minh sự cải thiện là có ý nghĩa thực chất và ổn định.

---

## 2. Bằng Chứng Bottleneck & Kế Hoạch Thực Thi (Execution Evidence)

### 2.1. Bottleneck 1: Partitioning Overhead
- **Bằng chứng:** Trong `reports/explain-E00.txt`, Spark mặc định sử dụng 200 shuffle partitions. Trên tập dữ liệu Jersey City (50.661 dòng raw, 1.88 MB), việc chia 200 partition khiến mỗi partition chỉ có ~250 dòng. Chi phí thread scheduling và quản lý 200 task nhỏ chiếm phần lớn thời gian thực thi.
- **Xử lý:** Giảm `spark.sql.shuffle.partitions = 4` (khớp với số lõi CPU máy thực nghiệm), giảm số task từ 200 xuống 4, giúp runtime giảm ngay 6.05 giây.

### 2.2. Bottleneck 2: Shuffle Khi Join Với Bảng Nhỏ
- **Bằng chứng:** Bảng `station_dimension.csv` chỉ có 127 dòng (~7.5 KB). Mặc định Spark có thể chọn SortMergeJoin hoặc HashJoin yêu cầu Shuffle Exchange dữ liệu cả 2 phía.
- **Xử lý:** Áp dụng `broadcast(stations)` để ép kiểu sang `BroadcastHashJoin`. Trong `reports/explain-E02.txt` và `E03.txt`, nút Shuffle Exchange của bảng trạm biến mất, thay thế bằng `BroadcastExchange` siêu nhẹ.

### 2.3. Bottleneck 3: Recompute Dữ Liệu
- **Bằng chứng:** DataFrame `cleaned_trips` được rẽ nhánh phục vụ đồng thời cho 3 phép gom nhóm độc lập: `hourly_station_flow`, `monthly_station_flow`, và `usage_by_member_casual`. Nếu không cache, Spark sẽ thực thi lại toàn bộ DAG từ bước đọc ZIP.
- **Xử lý:** Gọi `cleaned_trips.cache()`, tái sử dụng dữ liệu sạch trên bộ nhớ RAM.

---

## 3. Kiểm Chứng Tính Đúng Đắn (Correctness Verification)

Mọi thử nghiệm tối ưu đều được kiểm chứng tự động bằng lệnh:
```powershell
python src/verify_correctness.py --baseline output/baseline_csv --after output/preview_csv
```

**Kết quả kiểm tra chi tiết:**
- Số dòng `valid_trips`: 49.838 dòng (không mất mát hay nổ dòng).
- Bảng `hourly_station_flow.csv`: 3.713 dòng | Departures: 49.837 | Arrivals: 49.671 | Net Flow Checksum: -166.
- Bảng `monthly_station_flow.csv`: 127 dòng | Departures: 49.837 | Arrivals: 49.671 | Net Flow Checksum: -166.
- Bảng `usage_by_member_casual.csv`: 2 dòng (Member: 42.014 chuyến | Casual: 7.824 chuyến).
- Kiểm tra từng dòng (Row-Diff): **PASS** — Toàn bộ giá trị số thực và chuỗi khớp chính xác 100%.

---

## 4. Trả Lời 6 Câu Hỏi Vấn Đáp Của Giảng Viên (Slide 25)

* **Q1: Baseline có cố định input / resources / config không?**
  - *Trả lời:* Có, cố định 100%! Cùng sử dụng snapshot `JC-202401` (50.661 dòng raw, 1.88 MB nén), chạy trên cùng một máy (Windows 11, Python 3.12, JDK 21, `local[*]`), cùng đường dẫn code từ giải nén ZIP đến xuất CSV. Chỉ thay đổi đúng 1 biến config đang kiểm tra.

* **Q2: Bottleneck này được chứng minh bằng metric nào?**
  - *Trả lời:* Được chứng minh qua số lượng Task trên mỗi Stage (200 task so với 4 task) và sự hiện diện của các nút `Exchange hashpartitioning` trong file `explain-E00.txt`.

* **Q3: Tại sao thay đổi này có thể tác động vào bottleneck?**
  - *Trả lời:* (1) Giảm `shuffle.partitions = 4` giúp cắt giảm hơn 90% chi phí quản lý task nhỏ của Spark. (2) `broadcast(stations)` sao chép trực tiếp bảng 127 trạm sang RAM của executor, xóa bỏ hoàn toàn bước Shuffle di chuyển dữ liệu qua mạng/đĩa.

* **Q4: Expected signal trên explain / UI là gì?**
  - *Trả lời:* Trong `explain-E02.txt` và `E03.txt`, nút `BroadcastHashJoin` xuất hiện thay thế cho phép nối thông thường; số lượng task trong Stage shuffle giảm từ 200 xuống 4.

* **Q5: Output sau tối ưu có đúng như trước không?**
  - *Trả lời:* Đúng tuyệt đối 100%! Script `src/verify_correctness.py` đã so sánh song song từng dòng và xác nhận không có bất kỳ sai lệch nào về số dòng, tổng lượt đi/đến hay net-flow.

* **Q6: Nếu nhanh hơn 10% nhưng run-to-run variance 20% thì kết luận gì?**
  - *Trả lời:* Nếu độ cải thiện nhỏ hơn độ biến động ngẫu nhiên thì không thể kết luận tối ưu thành công. Tuy nhiên trong đồ án của nhóm, mức cải thiện đạt **30.1%**, trong khi độ biến động ($CV$) của các lần chạy chỉ từ **0.87% đến 2.88%**, chứng minh sự cải thiện có ý nghĩa thống kê thực tế.

---

## 5. Bốn Câu Hỏi Exit Ticket Rời Lớp (Slide 33)

1. **Bottleneck chính của nhóm là gì?**  
   Overhead điều phối của 200 shuffle partitions mặc định trên tập dữ liệu nhỏ và chi phí Shuffle Exchange không cần thiết khi join với bảng Dimension chỉ có 127 trạm.
2. **Evidence nào chứng minh?**  
   Execution plan `reports/explain-E00.txt` ghi nhận 200 tasks/stage và thời gian chạy Baseline 28.68s.
3. **Thay đổi nào được KEEP / REVERT?**  
   **KEEP:** Giảm `shuffle.partitions = 4`, thêm `broadcast(stations)`, và `cleaned_trips.cache()`.
4. **Median runtime + correctness sau cùng?**  
   Median runtime sau cùng: **20.04s** (giảm 30.1% so với Baseline 28.68s). Correctness: **PASS 100%**.

---

## 6. Hướng Dẫn Tái Tạo Thử Nghiệm (Reproducibility)

Để chạy lại toàn bộ Benchmark tự động và kiểm tra tính đúng đắn:
```powershell
# 1. Chạy đo đạc tự động toàn bộ 4 kịch bản (3 lần chạy mỗi kịch bản):
python src/benchmark.py --mode all --runs 3

# 2. Kiểm chứng tính đúng đắn:
python src/verify_correctness.py --baseline output/baseline_csv --after output/preview_csv
```
