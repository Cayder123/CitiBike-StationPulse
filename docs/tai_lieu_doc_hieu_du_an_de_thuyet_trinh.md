# Đọc hiểu dự án StationPulse để thuyết trình — Checkpoint 2

> Tài liệu này diễn giải repository theo các mục checkpoint: **Environment, Data Source, Ingestion, Raw Storage, Manifest/Logs, Spark Verification, rerun/fault handling và pitch**. Nội dung dựa trên code và evidence đã ghi trong repo. Khi demo, ưu tiên kết quả terminal chạy tại chỗ; log cũ chỉ chứng minh lần chạy được ghi trong log.

## 1. Dự án làm gì?

**StationPulse** là pipeline kỹ thuật dữ liệu dùng Python và PySpark với dữ liệu chuyến Citi Bike. Phần cốt lõi Checkpoint 2 lấy một archive lịch sử, giữ bản gốc trong raw storage, lưu metadata để truy vết, rồi xác minh file và đọc CSV bằng Spark. Repo cũng có bước phân tích ban đầu cho Jersey City tháng 01/2024.

**Câu giới thiệu ngắn để nói:**

> “Nhóm em xây dựng StationPulse để kiểm tra một pipeline dữ liệu có thể chạy lại và truy vết: từ nguồn Citi Bike, tải archive nguyên bản, ghi manifest/hash, rồi dùng Spark xác minh schema và số dòng. Trên cùng archive JC tháng 01/2024, nhóm có thêm aggregation theo giờ và trạm.”

## 2. Đối chiếu yêu cầu Checkpoint 2 với dự án

| Phần yêu cầu | Thành phần trong repo | Evidence đã ghi nhận | Cách trình bày/giới hạn |
|---|---|---|---|
| Environment | `src/env_check.py`, `requirements.txt` | Lần chạy lại 2026-10-23: Python 3.12.14, Java 21.0.8, PySpark/Spark 3.5.9; `spark.range(1_000_000).count()` đạt | Spark local (`local[*]`), không phải cluster nhiều máy. Chạy lại lúc demo nếu cần chứng minh trực tiếp. |
| Data source/snapshot | `src/ingest_trips.py`, README | Citi Bike System Data, Jersey City tháng 01/2024, `JC-202401`; dry-run 2026-10-23 nhận HTTP 200 và Content-Length 1,883,296 bytes | Dry-run kiểm tra URL/metadata, không tự chứng minh đã tải file. Tải thật được chứng minh bằng Raw và manifest. |
| Ingestion | `src/ingest_trips.py` | Một lần tải thật thành công; archive 1,883,296 bytes | Luồng tải có kiểm tra, ghi file tạm/đổi tên nguyên tử và hash. Evidence live hiện có là một lần tải, không phải ba lần. |
| Raw storage | `data/raw/citibike_trips/...` | ZIP raw cục bộ, cấu trúc `ingest_date=.../run_.../` | Raw bị `.gitignore`, không nằm trên GitHub; clone repo cần chạy ingestion để tạo lại. |
| Manifest / lineage | `manifest.json`, `src/common/manifest.py` | `status=success`, snapshot, timestamp, run ID, bytes, record_count, SHA-256 và destination | Hash kiểm tra tính toàn vẹn byte; manifest ghi metadata lần chạy. `git_commit` trong manifest là commit lúc ingest cũ, không phải commit bàn giao hiện nay. |
| Logs | `logs/`, logging utilities | Environment/raw verification logs cục bộ; logs bị ignore | Mở log để xem chi tiết lần chạy đã ghi. Không commit log runtime hoặc secret. |
| Spark verification | `src/raw_verify.py` | ZIP hợp lệ; Spark đọc CSV, in schema, đếm 50,661 dòng, hiển thị 5 dòng mẫu; 23/23 tests đạt trong lần chạy ghi nhận 2026-10-23 | 50,661 là số record CSV verifier đếm được. |
| Rerun / skip | `src/ingest_trips.py`, `tests/test_rerun_policy.py` | Policy mặc định tạo run mới; test hỗ trợ helper/policy; chưa có unit test gọi trực tiếp CLI `--skip-existing`; evidence tải thật một lượt | Không nói đã chạy live ba lượt. Phân biệt policy theo code/test với evidence live. |
| Phân tích mở rộng | `src/analyze_trips.py`, `docs/analysis_questions.md` | Có output CSV tổng hợp theo trạm/giờ và nhóm ngày | Đây là phần mở rộng trên một snapshot, không thay thế artifact cốt lõi. |
| GBFS | `src/ingest_gbfs.py` | Không có snapshot GBFS JSON được lưu trong evidence hiện tại | Phần mở rộng tùy chọn; không trình bày như đã có snapshot. Một snapshot GBFS là trạng thái tại thời điểm lấy. |

## 3. Pipeline từ đầu đến cuối

```text
Citi Bike System Data
        │  JC-202401 ZIP
        ▼
src/ingest_trips.py
        ├── data/raw/.../run_<UTC>/archive.zip  (bản gốc)
        └── manifest.json                       (nguồn, bytes, hash, trạng thái...)
                    │
                    ▼
           src/raw_verify.py
       ZIP check → Spark CSV read
       schema → count → 5-row sample
                    │
                    └── src/analyze_trips.py (aggregation mở rộng)
```

**Cách giải thích:** Source cung cấp dữ liệu; ingestion tải và ghi artifact; Raw lưu byte gốc; manifest cho biết artifact đến từ đâu và các thuộc tính lần ghi nhận; verifier dùng Spark kiểm tra archive/CSV. Phân tích là bước sau đó và không sửa file Raw.

## 4. Số liệu cần nhớ

- Dataset: Jersey City, tháng 01/2024 (`JC-202401`).
- Archive: `1,883,296` bytes.
- SHA-256: `d59d149f200fb81e204099afb0f76f1b02267bb2d605e92309e6f9974b176e15`.
- Spark row count: `50,661`.
- Smoke test: `spark.range(1_000_000).count()` → 1,000,000.
- Test suite: `23/23` đạt trong lần kiểm tra ghi nhận ngày 2026-10-23.
- Commit repo hiện tại: `79b94c6` — `checkpoint: finalize ingestion and analysis evidence`.
- Raw archive và runtime logs là cục bộ/ignored; không cần tìm chúng trên GitHub.

Không cần học thuộc SHA-256; mở manifest/PowerShell để chỉ hash khớp. Nếu có commit mới, dùng commit thực tế từ `git log` thay vì hash cũ.

## 5. Lệnh demo theo thứ tự

Mở PowerShell tại `D:\CitiBike-StationPulse`.

### A. Trạng thái repo và phiên bản

```powershell
git status --short --branch
git log -1 --oneline
```

### B. Environment/Spark

```powershell
.\.venv\Scripts\python.exe src\env_check.py --log-file logs\env_check.txt
```

Lệnh in phiên bản runtime và chạy Spark smoke test. Chỉ báo PASS nếu lệnh hiện tại thành công.

### C. Source dry-run, không tải archive mới

```powershell
.\.venv\Scripts\python.exe src\ingest_trips.py --dataset jc --year 2024 --month 1 --dry-run
```

Lệnh kiểm tra URL/metadata và truy cập nguồn; không tạo raw archive.

### D. Spark Raw verification trên archive đã tải

```powershell
.\.venv\Scripts\python.exe src\raw_verify.py --path "data/raw/citibike_trips/ingest_date=*/run_*/JC-202401-citibike-tripdata.csv.zip" --log-file logs\raw_verify.txt
```

Nếu có nhiều run, mở manifest để chọn đúng ZIP và truyền path cụ thể. Không cần chạy ingestion live lại trong buổi thuyết trình.

### E. Phân tích (nếu được hỏi)

```powershell
.\.venv\Scripts\python.exe src\analyze_trips.py
```

Kết quả nằm dưới `data/processed/citibike_trips/2024-01/`; output bị ignore và tái tạo từ Raw. Chạy trước buổi demo nếu muốn mở CSV kết quả.

## 6. Giải thích phân tích và giới hạn

```text
net_flow = arrivals - departures
```

- Dương: trong dữ liệu chuyến ghi nhận, lượt đến nhiều hơn lượt đi tại trạm/khoảng thời gian đó.
- Âm: lượt đi nhiều hơn lượt đến.
- Đây là **proxy luồng chuyến**, không phải số xe tồn thật. Dữ liệu không phản ánh đầy đủ việc điều chuyển xe, sức chứa dock, xe hỏng, chuyến thiếu station ID hoặc toàn bộ xe có thể đỗ ngoài dock.
- JC tháng 01/2024 là snapshot một tháng, không đủ để kết luận mùa vụ hay xu hướng dài hạn.
- Archive có station endpoint ngoài Jersey City/Hoboken; phân tích hiện giữ mọi station ID. Đừng khẳng định đã lọc chỉ còn NJ nếu chưa thêm quy tắc lọc.
- GBFS station status phản ánh thời điểm chụp, không thể dựng lịch sử tồn xe từ một snapshot.

## 7. Kịch bản 6 slide — khoảng 4 phút

| Slide | Thời lượng | Người nói | Nội dung |
|---|---:|---|---|
| 1. Tên đề tài và mục tiêu | 20 giây | Bạn — Người 1 | StationPulse, nhóm/thành viên, repo, mục tiêu và snapshot JC-202401. |
| 2. Architecture Update | 35 giây | Bạn — Người 1 | `Source → Ingestion → Raw + Manifest → Spark Verify`; local PySpark. |
| 3. Environment | 35 giây | Người 2 | Python/Java/PySpark/Spark, smoke test một triệu dòng, kết quả mới nhất. |
| 4. Data Source & Ingestion | 45 giây | Người 3 | Citi Bike, snapshot, dry-run vs tải thật, archive bytes và run. |
| 5. Raw & Manifest / Verification | 55 giây | Người 3 rồi Người 4 | Raw path, hash/bytes/status; Spark schema/count/sample; manifest/log thật. |
| 6. Rerun, giới hạn, bước tiếp theo | 50 giây | Bạn — Người 1 | Default tạo run mới; giới hạn evidence skip; rủi ro thật và việc tiếp theo. |

Nếu mẫu slide của giảng viên dùng tên trường khác, đưa evidence tương ứng vào đúng ô. Ưu tiên sơ đồ, số liệu, ảnh terminal/manifest và lời nói ngắn.

## 8. Câu hỏi giảng viên có thể hỏi

### “Chạy ingest ba lần thì dữ liệu thế nào?”

> “Theo policy mặc định, mỗi lần chạy tạo một run partition theo timestamp riêng nên giữ artifact cũ, không ghi đè. `--skip-existing` có thể bỏ qua artifact hợp lệ cùng ngày khi metadata và hash khớp. Evidence live hiện xác nhận một lần tải thật; bộ test xác nhận policy tạo run riêng, còn nhánh CLI `--skip-existing` chưa có unit test gọi trực tiếp.”

### “Làm sao biết ZIP không hỏng hoặc bị thay đổi?”

> “Nhóm ghi kích thước và SHA-256 vào manifest, kiểm tra ZIP, rồi dùng Spark đọc CSV. Evidence hiện có là 1,883,296 bytes, hash trong manifest, và Spark đếm 50,661 dòng. Hash cho biết nội dung byte có khớp manifest đã ghi hay không; nó không tự xác nhận dữ liệu đúng về nghiệp vụ.”

### “Vì sao raw không đưa lên GitHub?”

> “Raw archive lớn và tải lại được từ nguồn chính thức. Repo lưu code và hướng dẫn tái tạo; raw, snapshot và runtime log bị ignore để repo gọn và tránh đưa artifact không cần thiết vào commit.”

### “Spark ở đây có phân tán không?”

> “Demo chạy Spark local trên một máy (`local[*]`), để kiểm chứng pipeline/DataFrame và khả năng chạy lại. Đây chưa phải triển khai cluster nhiều node.”

### “Net-flow dương có nghĩa trạm đang có nhiều xe?”

> “Không thể kết luận tồn kho hiện tại. Dương chỉ có nghĩa lượt đến ghi nhận nhiều hơn lượt đi trong khoảng tính. Điều chuyển xe, sức chứa dock và xe ngoài dịch vụ không được phản ánh đầy đủ trong trip archive.”

### “Có GBFS không?”

> “Repo có script tùy chọn để discovery và tải GBFS, nhưng evidence checkpoint hiện chưa có snapshot JSON được lưu. Phần cốt lõi nhóm xác minh là archive JC-202401; một GBFS snapshot cũng chỉ là trạng thái tại thời điểm lấy.”

### “Ai làm phần nào?”

> “Nhóm đã phân owner theo module trong bảng phân công. Em phụ trách [nêu việc thật đã làm và file/commit], bạn [tên] phụ trách [việc thật]. Đây là phần em có thể mở trực tiếp trong repo để chứng minh.”

Điền câu trả lời cuối bằng đóng góp thật của mỗi người; tài liệu không thay thế lịch sử làm việc.

## 9. Những điểm không nên nói là đã hoàn tất

- Không nói đã tải live ba lần: evidence hiện tại chỉ ghi một lần tải thật.
- Không nói đã kiểm thử riêng CLI `--skip-existing`: chưa có test trực tiếp nhánh này.
- Không nói đã lưu GBFS snapshot: hiện chưa có JSON snapshot trong evidence.
- Không nói Spark chạy trên cluster: cấu hình demo là `local[*]`.
- Không gọi net-flow là tồn kho xe hoặc dự báo thiếu xe đã được xác thực.
- Không nói `git_commit` trong manifest cũ là commit bàn giao hiện nay.

## 10. Tài liệu repo để tra cứu

- [README và lệnh chạy](../README.md)
- [Checklist evidence Checkpoint 2](evidence_checklist.md)
- [Hướng dẫn hoàn thiện Buổi 6](huong_dan_hoan_thien_buoi6.md)
- [Kiến trúc](architecture.md)
- [Câu hỏi và giới hạn phân tích](analysis_questions.md)
- [Phân công nhiệm vụ nhóm 4 người](phan_cong_nhiem_vu_nhom_4_nguoi.md)
