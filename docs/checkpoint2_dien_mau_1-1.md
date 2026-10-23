# Hướng dẫn điền mẫu Checkpoint 2 cho CitiBike-StationPulse

Tài liệu này đối chiếu lần lượt 6 slide trong `BigData_Buoi06_DoAn2_Checkpoint2_Report_Template.pptx` với bằng chứng có trong repo. Nội dung trong khung **Có thể điền** có thể chép vào slide. Những chỗ ghi **Nhóm tự bổ sung** cần thông tin hoặc ảnh chụp chỉ nhóm mới xác nhận được.

## Đánh giá nhanh mức độ hoàn thành dự án

- **Checkpoint 2: đủ các phần kỹ thuật cốt lõi theo tài liệu repo.** Có environment check, ingestion Citi Bike thật, raw ZIP và manifest, Spark raw verification và rerun policy mặc định. Lần chạy lại ngày 2026-10-23 đạt 23/23 test, smoke test Spark, raw verification và source dry-run. GBFS snapshot không có nhưng đây là phần mở rộng, không phải artifact cốt lõi.
- **Phân tích chuyến đi: đã có code và output cục bộ.** `src/analyze_trips.py` tạo bốn CSV cho `JC-202401`; README và `docs/analysis_questions.md` ghi các kết quả. Phạm vi hiện chỉ có một tháng dữ liệu, nên phù hợp để trình bày kết quả mô tả của snapshot này, chưa đủ để kết luận xu hướng mùa vụ hoặc dài hạn.
- **Trạng thái bàn giao:** trước commit, cần rà lại các thay đổi code/tài liệu trong working tree và xác nhận nội dung báo cáo nhóm. `git diff --check` không báo lỗi whitespace; các lần chạy kiểm tra đầy đủ đã hoàn tất bằng Python 3.12.14, Java 21.0.8 và PySpark/Spark 3.5.9.
- **Evidence phụ thuộc máy chạy.** Raw archive và bốn output phân tích đang có trong workspace, nhưng `.gitignore` cố ý loại raw, output và logs khỏi Git. Trên máy khác cần tái tạo bằng các lệnh trong README; ảnh chụp trình bày nên lấy trực tiếp từ máy có dữ liệu.

### Có nên push toàn bộ code hiện tại lên GitHub?

Nên đưa **code và tài liệu đã rà soát** lên GitHub sau khi nhóm thống nhất và commit. Không đưa dữ liệu raw, file kết quả đã sinh, logs, `.venv`, `.env` thật, token hoặc credentials. `.gitignore` hiện loại `data/raw/**`, `data/processed/**`, `data/snapshots/**` và `logs/**`.

Những thay đổi trong phiên bàn giao gồm `.gitignore`, `README.md`, các tài liệu trong `docs/` và `src/analyze_trips.py`. Các tài liệu đã được đồng bộ để phản ánh script phân tích và giới hạn của test `--skip-existing`. Sau khi nhóm rà lại, commit các file dự án rồi có thể push branch `main`; không push thư mục dữ liệu/log dù chúng đang tồn tại cục bộ.

Các lệnh kiểm tra trước khi push:

```powershell
git status --short --branch
git diff --check
git diff --stat
git check-ignore data/raw/citibike_trips/ingest_date=2026-10-21/run_20261021T014618_890189Z/JC-202401-citibike-tripdata.csv.zip
git check-ignore data/processed/citibike_trips/2024-01/hourly_station_flow.csv
git check-ignore logs/raw_verify.txt
```

Sau khi xác nhận danh sách file và commit, push bằng `git push origin main`. Hiện tại chưa có commit mới để push; các thay đổi đang chỉ ở working tree.

## Slide 1 — Trang bìa

**Có thể điền**

- Đề tài: `CitiBike-StationPulse: Ingestion và xác minh dữ liệu chuyến đi Citi Bike bằng PySpark`
- Repo: `https://github.com/Cayder123/CitiBike-StationPulse`
- Git commit/tag: lấy sau khi tạo commit bàn giao bằng `git rev-parse --short HEAD`; chỉ ghi tag nếu nhóm đã tạo tag thật.

**Nhóm tự bổ sung**

- Tên nhóm, tên các thành viên và MSSV.
- Ngày trình bày.
- Điền mã commit sau khi đã commit các file sẽ nộp. Không dùng commit cũ nếu nó chưa chứa code và tài liệu của phiên bản báo cáo.

## Slide 2 — Architecture update

**Sơ đồ có thể dựng trong slide**

`Citi Bike System Data (JC-202401 ZIP)` → `src/ingest_trips.py (Python)` → `data/raw/citibike_trips/ingest_date=.../run_.../ (ZIP nguyên bản + manifest.json)` → `src/raw_verify.py (kiểm tra ZIP, SHA-256 và đọc CSV bằng PySpark)`

Thêm nhánh `src/env_check.py` → `Spark local[*]`. Có thể thêm `src/analyze_trips.py` → `data/processed/...` và ghi “phân tích tháng 01/2024” nếu muốn thể hiện phần mở rộng đã có.

**Technology / boundary**

`Python 3.11.9; PySpark/Spark 3.5.9; chạy local[*] trên Windows. Checkpoint 2 chứng minh environment, tải một archive lịch sử, lưu raw bất biến kèm manifest và Spark verify. Không triển khai Spark cluster/cloud. GBFS được hỗ trợ trong code nhưng chưa có snapshot JSON trong evidence.`

**Trade-off có thể ghi**

`Chạy local[*] giúp nhóm tái tạo và demo mà không cần quản lý cluster; đổi lại, tài nguyên xử lý giới hạn ở một máy.`

**Cách hoàn thiện sơ đồ**: dùng các hộp và mũi tên có thể chỉnh sửa trong PowerPoint, ghi đúng tên script và thư mục ở trên. Không vẽ GBFS thành một nguồn đã ingest thành công vì chưa có snapshot evidence.

## Slide 3 — Environment evidence

**Có thể điền vào phần chữ**

- Python `3.11.9`
- Java `21.0.8`
- PySpark / Spark `3.5.9`
- Execution mode `local[*]` trên Windows
- Smoke test `spark.range(1_000_000).count()` → `1,000,000`, PASS (theo lần chạy đã ghi nhận)
- Setup command: `python -m pip install -r requirements.txt` (sau khi kích hoạt venv)

**Lệnh lấy ảnh/output mới** (PowerShell tại thư mục repo):

```powershell
.\.venv\Scripts\python.exe src\env_check.py --log-file logs\env_check.txt
```

Chụp terminal thấy đủ phiên bản, `Smoke Test PASSED`, kết quả 1.000.000 và tiến trình kết thúc thành công; hoặc mở `logs\env_check.txt` nếu file có sẵn. Tài liệu repo ghi nhận lần chạy PASS ngày 2026-10-21, nhưng log là evidence trên máy chạy và bị Git ignore. Nếu chạy lại thất bại, không dùng số PASS cũ như kết quả mới; ghi lỗi thật vào ô blocker.

## Slide 4 — Data source & ingestion

**Có thể điền vào slide**

- Source: `Citi Bike System Data — https://citibikenyc.com/system-data`
- File/snapshot: `Jersey City, 2024-01 — JC-202401-citibike-tripdata.csv.zip`
- Archive URL đã tải: `https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip`
- Quy mô archive: `1,883,296 bytes`; manifest và Spark verifier ghi nhận `50,661` dòng.
- Provenance: `Dữ liệu do Citi Bike công bố; tham khảo Data Sharing Policy tại https://citibikenyc.com/data-sharing-policy.`

**Lệnh ingestion đã dùng / để tái tạo**

```powershell
.\.venv\Scripts\python.exe src\ingest_trips.py --dataset jc --year 2024 --month 1 --log-file logs\ingest_trips.txt
```

**Result có thể ghi, với chú thích đây là lần chạy đã ghi nhận**: `Ingestion success; ZIP 1,883,296 bytes; SHA-256 d59d149f200fb81e204099afb0f76f1b02267bb2d605e92309e6f9974b176e15; manifest status=success.`

Để trình bày evidence mới, chạy lệnh trên và chụp output terminal hoặc mở `logs\ingest_trips.txt`, rồi mở `manifest.json`. Nếu chỉ chạy `--dry-run`, ghi rõ đó là kiểm tra URL/metadata; dry-run không chứng minh đã tải dữ liệu.

## Slide 5 — Raw storage & manifest

**Path convention**

`data/raw/citibike_trips/ingest_date=YYYY-MM-DD/run_<UTC timestamp>/`

**Path của run đã ghi nhận**

`data/raw/citibike_trips/ingest_date=2026-10-21/run_20261021T014618_890189Z/`

Trong đó có `JC-202401-citibike-tripdata.csv.zip` và `manifest.json`.

**Các giá trị để điền**

- Raw file size: `1,883,296 bytes`
- Source snapshot: `JC-202401`
- Record count: `50,661`
- SHA-256: `d59d149f200fb81e204099afb0f76f1b02267bb2d605e92309e6f9974b176e15`
- Status: `success`
- Raw verification: ZIP hợp lệ; Spark đọc schema, đếm `50,661` dòng, hiển thị 5 dòng mẫu; verifier kết thúc `[OK]`.

**Ảnh cần tự chụp**: mở PowerShell và chạy verifier, sau đó chụp phần có path, `Total Rows Verified: 50,661` và `[OK]`:

```powershell
.\.venv\Scripts\python.exe src\raw_verify.py --path "data/raw/citibike_trips/ingest_date=*/run_*/JC-202401-citibike-tripdata.csv.zip" --log-file logs\raw_verify.txt
```

Chụp thêm File Explorer hoặc kết quả `Get-ChildItem` để thấy ZIP và manifest cùng thư mục. Mở manifest để trích `source_url`, `ingested_at_utc`, `bytes`, `sha256`, `destination`, `status`. Không đưa toàn bộ slide bằng chữ nếu có thể thay bằng ảnh terminal/tree dễ đọc.

**Nếu không tìm thấy raw file trên máy của bạn**: dữ liệu raw và log bị `.gitignore`, nên không nằm trong Git clone. Chạy lệnh ingestion ở Slide 4 để tạo một run mới, rồi chạy verifier phía trên. Các giá trị run/hash cũ chỉ dùng khi manifest trên máy đúng là run đó.

## Slide 6 — Rerun behavior, blocker & next action

**Rerun behavior có thể điền**

`Mặc định, mỗi lần ingestion tạo run_id/partition mới và giữ run cũ, không overwrite. Với --skip-existing, bỏ qua artifact trong partition của ngày chạy khi manifest thành công và tên file, snapshot, kích thước, SHA-256 khớp. Code có nhánh skip; test suite hiện chưa gọi trực tiếp nhánh này. Evidence live chứng minh một lần tải, chưa phải ba lần tải thật.`

Không cần tải archive thật ba lần để tuyên bố đã chạy ba lần. Nếu giảng viên yêu cầu evidence ba lần live, chạy lệnh ingestion ba lần, lưu ba output, rồi xác nhận có ba `run_id` khác nhau. Nếu dùng `--skip-existing`, chạy thêm lệnh với flag này và chụp log kết quả skip. Không ghi các lần đó là đã thực hiện trước khi thật sự chạy.

**Blocker / rủi ro đề xuất để nhóm xác nhận**

`Chưa thấy blocker cho environment và ingestion trong evidence hiện có. Rủi ro trước Buổi 7: phạm vi phân tích hiện mới có JC-202401; nhóm cần xác nhận một tháng này có đủ trả lời câu hỏi dự án không.`

Điền Owner và Deadline sau khi nhóm thống nhất. Không tự điền tên hoặc hạn. Nếu lần chạy hiện tại có lỗi, thay rủi ro trên bằng lỗi cụ thể, bằng chứng lỗi, người xử lý và hạn dự kiến.

**Evidence links**

- Commit: điền kết quả của `git rev-parse --short HEAD` sau khi chốt commit bàn giao.
- Raw/manifest: đường dẫn ở Slide 5 trên máy chạy. Có thể ghi `manifest.json` và `logs/raw_verify.txt`; hai file này không được commit.
- Trước khi nộp, mở repo và log trực tiếp trong lúc pitch vì đường dẫn cục bộ không phải link dùng được trên máy khác.

**Việc trước Buổi 7 có thể ghi**

`Trình bày output phân tích PySpark đã tạo cho JC-202401; thống nhất có mở rộng thời gian/khu vực không; rà các station ID ngoài Jersey City/Hoboken trước khi kết luận.`

Phân tích net-flow đã có script `src/analyze_trips.py` và bốn output CSV hiện có trong `data/processed/citibike_trips/2024-01/`. Nếu muốn tái tạo evidence, chạy `.\.venv\Scripts\python.exe src\analyze_trips.py` và mở log `logs/analyze_trips.txt`. `arrivals - departures` là proxy luồng chuyến, không phải số xe tồn tại thực tế tại trạm.

## Các phần mình không thể tự điền hoặc tự tạo evidence thay nhóm

1. **Tên nhóm, thành viên, MSSV, owner và deadline**: cung cấp thông tin thật của nhóm rồi thay các ô trống trên slide 1 và 6.
2. **Ảnh terminal, thư mục raw và manifest trên máy nộp**: mở PowerShell tại `D:\CitiBike-StationPulse`, chạy lệnh ở slide tương ứng và chụp màn hình. Raw/log bị ignore nên mình không thể suy ra chúng hiện diện trên máy khác chỉ từ Git.
3. **Commit cuối/tag nộp**: sau khi nhóm thống nhất các thay đổi và commit, chạy `git rev-parse --short HEAD`; chỉ ghi tag nếu nhóm đã tạo tag.
4. **Ba lần ingest live**: chưa có evidence ba lần tải thật. Nếu rubric bắt buộc, chạy lệnh mặc định ba lần và chụp ba run ID; nếu không, dùng cách giải thích policy code/test như nội dung slide 6.
5. **Blocker và phạm vi nghiên cứu được nhóm chốt**: xác nhận với cả nhóm. Tài liệu hiện cho thấy không có blocker đã biết ở ingestion/environment, nhưng phạm vi đủ dùng cho câu hỏi cuối là quyết định học thuật của nhóm.

## Trình tự điền nhanh

1. Chép các đoạn **Có thể điền** vào đúng slide.
2. Dựng sơ đồ slide 2 bằng các đối tượng PowerPoint có thể chỉnh sửa.
3. Chạy environment check, ingestion (nếu máy chưa có raw) và raw verifier; chụp evidence mới.
4. Điền tên nhóm, owner, deadline và blocker/rủi ro đã được thống nhất.
5. Cập nhật commit hash sau commit cuối; kiểm tra slide không khẳng định log/run cũ là lần chạy vừa thực hiện.
