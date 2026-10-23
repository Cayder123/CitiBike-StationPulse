# Hướng dẫn hoàn thiện và trình bày Buổi 6

Tài liệu này chuyển rubric Checkpoint 2 thành các bước có thể chạy và kiểm tra ngay trong repository `CitiBike-StationPulse`. Mỗi tiêu chí có lệnh tạo evidence, vị trí file cần mở, cách đối chiếu và hướng xử lý khi kết quả chưa đạt.

**Phạm vi evidence Checkpoint 2:** archive Jersey City tháng 01/2024 (`JC-202401`). Không cần tải thêm tháng khác để hoàn thành checkpoint. Phần phân tích PySpark theo trạm/giờ hiện đã có trong `src/analyze_trips.py`, với bốn CSV output tại `data/processed/citibike_trips/2024-01/`. Buổi 6 không yêu cầu dashboard, ML hay ETL sâu.

## 1. Chuẩn bị trước buổi trình bày

Mở PowerShell tại repo:

```powershell
Set-Location D:\CitiBike-StationPulse
Get-Location
git status --short --branch
git log -5 --oneline --decorate
```

Kiểm tra các file cơ bản:

```powershell
Test-Path README.md
Test-Path requirements.txt
Test-Path .env.example
Test-Path src\env_check.py
Test-Path src\ingest_trips.py
Test-Path src\raw_verify.py
Test-Path docs\architecture.md
Test-Path docs\evidence_checklist.md
```

Các lệnh `Test-Path` nên trả về `True`. README phải có lệnh cài môi trường và chạy pipeline. Trước khi chốt bản nộp, xem `git diff` và thống nhất các thay đổi trong nhóm. Không commit raw data, runtime logs, `.env` thật hoặc credentials.

## 2. Environment: chạy Spark và lưu kết quả

Chạy bằng Python của virtual environment trong repo:

```powershell
.\.venv\Scripts\python.exe src\env_check.py --log-file logs\env_check.txt
```

**Mở để làm evidence:** terminal và `logs\env_check.txt`.

**Đối chiếu:**

- Có phiên bản Python, Java, PySpark và Spark. Phiên bản đã lưu là Python 3.11.9, Java 21.0.8, PySpark/Spark 3.5.9.
- Có `Smoke Test PASSED` với 1.000.000 dòng.
- Có `All Environment Checks Passed Successfully! [OK]`.
- Spark dừng sạch; lệnh trả exit code 0.

Nếu thiếu PySpark, cài vào đúng venv rồi chạy lại:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe src\env_check.py --log-file logs\env_check.txt
```

Log PASS cũ chứng minh lần chạy trước. Nếu giảng viên kiểm tra trực tiếp, chạy lại lệnh ngay trước pitch; đừng trình bày log cũ như vừa chạy.

## 3. Source: chứng minh URL và snapshot

Kiểm tra URL và metadata mà không tải archive:

```powershell
.\.venv\Scripts\python.exe src\ingest_trips.py --dataset jc --year 2024 --month 1 --dry-run --log-file logs\source_check.txt
```

**Mở để đối chiếu:** terminal, `logs\source_check.txt`, README và `manifest.json` trong raw partition.

**Kết quả cần có:** URL chứa `JC-202401-citibike-tripdata.csv.zip`; snapshot là `2024-01`; README dẫn Citi Bike System Data và điều khoản dữ liệu. `--dry-run` chỉ chứng minh kiểm tra source/metadata, không phải bằng chứng tải thật. Tải thật đã có manifest `status: success` và raw ZIP.

Không cần tải lại chỉ để có evidence. Nếu nhóm chủ động muốn tạo run mới, lệnh này sẽ tải và tạo partition mới:

```powershell
.\.venv\Scripts\python.exe src\ingest_trips.py --dataset jc --year 2024 --month 1 --log-file logs\ingest_trips.txt
```

Mặc định, lệnh không ghi đè run cũ. Có thể dùng `--skip-existing` để kiểm tra chính sách bỏ qua khi artifact cũ hợp lệ:

```powershell
.\.venv\Scripts\python.exe src\ingest_trips.py --dataset jc --year 2024 --month 1 --skip-existing --log-file logs\ingest_skip_existing.txt
```

Nhánh này chỉ tìm artifact trong partition `ingest_date` của ngày chạy hiện tại. Bộ test hiện tại xác nhận helper tạo run riêng, nhưng chưa gọi trực tiếp tùy chọn `--skip-existing`; nếu trình bày kết quả skip, chạy demo và lưu log hoặc bổ sung test trước.

Với evidence hiện có, không cần chạy live ba lần. Nếu chạy mặc định nhiều lần, mỗi lượt tạo raw partition mới.

## 4. Raw storage và manifest: đối chiếu dữ liệu với metadata

Tìm raw archive và manifest:

```powershell
$archive = Get-ChildItem .\data\raw\citibike_trips -Filter 'JC-202401-citibike-tripdata.csv.zip' -Recurse | Select-Object -First 1
$manifestPath = Join-Path $archive.DirectoryName 'manifest.json'
$manifest = Get-Content $manifestPath -Raw | ConvertFrom-Json
$sha256 = (Get-FileHash $archive.FullName -Algorithm SHA256).Hash.ToLower()

$archive | Select-Object FullName, Length
$manifest | Format-List dataset, source_url, source_snapshot, ingested_at_utc, run_id, filename, bytes, record_count, sha256, destination, status, git_commit
"File bytes match manifest: $($archive.Length -eq $manifest.bytes)"
"File SHA-256 matches manifest: $($sha256 -eq $manifest.sha256)"
```

Hai phép so sánh cuối cần là `True`. Giá trị đã ghi nhận: 1.883.296 bytes, SHA-256 `d59d149f200fb81e204099afb0f76f1b02267bb2d605e92309e6f9974b176e15`, `record_count` 50.661 và `status` `success`. Path có cấu trúc `dataset / ingest_date / run_id`; manifest nằm cạnh ZIP.

Raw được `.gitignore` loại khỏi Git. Vì thế `git status` không liệt kê ZIP là bình thường; dùng `Get-ChildItem` ở trên để chứng minh file tồn tại cục bộ. Giữ raw nguyên bản, không sửa CSV rồi gọi đó là raw.

## 5. Spark Raw Read: xác minh archive

Chạy verifier trên archive đã có:

```powershell
.\.venv\Scripts\python.exe src\raw_verify.py --path "data/raw/citibike_trips/ingest_date=*/run_*/JC-202401-citibike-tripdata.csv.zip" --log-file logs\raw_verify.txt
```

**Mở để làm evidence:** terminal và `logs\raw_verify.txt`.

**Đối chiếu:** ZIP integrity check thành công; kích thước/hash khớp manifest; Spark in schema, `Total Rows Verified: 50,661`, 5 dòng mẫu; cuối log có `Raw ZIP and All Internal CSVs Spark Verified [OK]`. Verifier giải nén CSV vào thư mục tạm, không sửa ZIP.

Nếu về sau có nhiều run khớp wildcard, truyền path ZIP cụ thể để tránh chọn nhầm archive.

## 6. Rerun policy: trả lời “chạy ba lần thì sao?”

Không cần tải live ba lần. Giải thích đúng hành vi từ code/test:

- Mặc định: mỗi lượt tạo `run_id` và partition mới, không ghi đè run cũ.
- `--skip-existing`: tìm trong partition của ngày chạy hiện tại và bỏ qua nếu manifest thành công, snapshot/file/size/hash khớp. Test suite hiện chưa kiểm tra trực tiếp nhánh CLI này.
- `tests\test_rerun_policy.py` kiểm tra việc tạo partition/run riêng, không giả lập ba lần download thật.

Nếu muốn xác nhận test hiện tại, chạy:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_rerun_policy.py -v
```

Đây là test policy, không phải ba lần tải live. Muốn chạy toàn bộ test suite:

```powershell
.\.venv\Scripts\python.exe -m pytest tests -v
```

README ghi nhận lần trước 23/23 test đạt. Chỉ báo cáo kết quả test mới nếu nhóm vừa chạy lệnh và thấy PASS.

## 7. Repo, commit và bàn giao

Kiểm tra thay đổi và whitespace:

```powershell
git status --short --branch
git diff --check
git diff --stat
```

Kiểm tra raw/log bị ignore:

```powershell
git check-ignore .\logs\env_check.txt
git check-ignore .\logs\raw_verify.txt
git check-ignore $archive.FullName
```

Các lệnh `git check-ignore` cần trả về đường dẫn bị ignore. Không dùng `git add -f` với raw/log. Trước khi nộp, kiểm tra commit cuối có chứa đúng code và tài liệu đã duyệt. Tạo tag checkpoint là tùy chọn nếu nhóm muốn nhận diện bản nộp dễ hơn.

## 8. Điền mẫu báo cáo và pitch 4 phút

Mẫu PPT yêu cầu evidence trên slide. Điền tên nhóm/thành viên, đề tài, repo và commit/tag thật. Gợi ý sáu slide:

1. **Bìa:** tên nhóm, thành viên, đề tài, repo, commit/tag.
2. **Architecture:** `Citi Bike source → ingest_trips.py → raw ZIP + manifest → raw_verify.py`; execution mode local PySpark.
3. **Environment:** output vừa chạy ở mục 2; ghi `local[*]`.
4. **Source & Ingestion:** URL, snapshot, kích thước, lệnh dry-run; manifest/log của lần tải thật.
5. **Raw & Manifest:** path thật, bytes, SHA-256, số dòng, status; kết quả Spark verify.
6. **Rerun, blocker, next action:** nói rõ run mới hay skip; nêu blocker/rủi ro thật, owner, deadline và việc kế tiếp.

Nếu không có blocker ingestion/environment đã biết, ghi rõ như vậy rồi nêu rủi ro tiếp theo. Nhóm cần tự thống nhất owner/deadline; không tự điền tên hoặc hạn chưa được phân công. Mở terminal/repo/log/manifest trong pitch vì slide không thay evidence. Mỗi thành viên nên giải thích được data flow, manifest/hash, rerun policy và trade-off của local Spark.

## 9. Exit ticket: câu trả lời lấy từ project

1. **Một lệnh kiểm tra environment:** `.\.venv\Scripts\python.exe src\env_check.py --log-file logs\env_check.txt`. Kết quả PASS, đếm 1.000.000 dòng và Spark dừng sạch.
2. **Nguồn và snapshot:** Citi Bike System Data, Jersey City `JC-202401`, tháng 01/2024; đối chiếu `source_url` và `source_snapshot` trong manifest.
3. **Chạy ingest ba lần:** mặc định có ba partition `run_id` riêng, không overwrite; với `--skip-existing`, artifact hợp lệ được skip khi manifest/hash khớp. Nêu rõ đây là policy code/test; evidence live hiện có là một lượt tải.
4. **Rủi ro trước bước tiếp theo:** phạm vi phân tích đã giới hạn ở một tháng; tiếp tục rà các station endpoint ngoài Jersey City/Hoboken và diễn giải net-flow thận trọng. Nhóm chốt owner/deadline.

## 10. Checklist trước khi nộp

- [ ] Environment check vừa chạy PASS; log và terminal mở được.
- [ ] URL, snapshot, provenance và điều khoản được ghi.
- [ ] Có ingestion script và ít nhất một lần tải thật thành công.
- [ ] Raw ZIP tồn tại, không rỗng; bytes/hash/record count khớp manifest.
- [ ] Raw verifier thành công; schema, row count, sample và log được lưu.
- [ ] Rerun behavior được giải thích đúng; không nói đã chạy ba lượt live nếu chưa chạy.
- [ ] README, cấu trúc repo, `.gitignore` và commit/tag sẵn sàng.
- [ ] Mẫu báo cáo được điền; mỗi slide có evidence.
- [ ] Blocker/rủi ro, owner, deadline và next action được thống nhất.
- [ ] Thành viên luyện data flow, trade-off và exit ticket.

**Ngoài phạm vi cốt lõi:** chưa có GBFS snapshot JSON, nhưng checklist hiện tại đánh dấu đây là phần mở rộng. Phân tích net-flow tháng 01/2024 là phần tiếp nối; đây là chỉ báo từ trip records, không phải tồn kho xe thực tế tại trạm.
