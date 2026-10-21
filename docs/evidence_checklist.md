# Checklist evidence và dàn ý trình bày Buổi 6

Checklist này dùng để chuẩn bị nghiệm thu Checkpoint 2. Trạng thái được cập nhật theo log và artifact thực tế trong thư mục dự án ngày 2026-10-21. Các mục còn thiếu được ghi rõ để nhóm hoàn tất trước khi nộp.

## 1. Bảng evidence bàn giao

| Hạng mục | Yêu cầu | Owner chính | Bằng chứng cần lưu | Trạng thái |
|---|---|---|---|---|
| Environment | Python/Java/PySpark/Spark version; `spark.range(1_000_000).count()`; exit code 0 | Nhóm | `logs/env_check.txt`: Python 3.11.9, Java 21.0.8, PySpark/Spark 3.5.9; 1,000,000 rows | Đạt — log PASS ngày 2026-10-21 |
| Source | URL chính thức, tháng/snapshot, license/terms, dung lượng và quyền truy cập | Nhóm | System Data URL, S3 archive `JC-202401`, 1,883,296 bytes; policy link trong README | Đạt — snapshot xác định, HTTP 200 |
| Ingestion | Tải tự động, timeout/retry hữu hạn, kiểm tra payload, atomic write, SHA-256 | Nhóm | Log terminal ngày 2026-10-21; manifest và checksum | Đạt — một lần tải thật |
| Raw Storage | Dữ liệu nguyên bản, file >0 byte, cấu trúc theo ngày/run, không overwrite | Nhóm | Partition `ingest_date=2026-10-21/run_20261021T014618_890189Z`; ZIP 1,883,296 bytes | Đạt — tồn tại cục bộ, được gitignore |
| Manifest | Source, UTC timestamp, bytes, row count hoặc unknown, SHA-256, run_id, destination, status | Nhóm | Manifest trong run partition; hash/bytes/rows khớp verifier | Đạt — các trường rubric và hash/bytes/rows khớp; `git_commit` lưu HEAD lúc ingest, lineage chính xác tới checkpoint chỉ là phần bổ sung |
| Logs | Log có thời gian UTC, lỗi rõ; không lộ token/header/secret | Nhóm | `logs/env_check.txt`, `logs/raw_verify.txt`; các file nằm ngoài Git | Đạt — có log thực tế; logger sanitization có test |
| Rerun safety | Giải thích hành vi khi chạy lại; mỗi lần tạo run mới hoặc skip cùng hash có kiểm tra | Nhóm | Mặc định mỗi lần chạy tạo partition riêng; unit test kiểm tra partition độc lập và skip policy | Đạt về hành vi — không cần tải cùng archive ba lần live; trong báo cáo giải thích kết quả ba lần chạy theo code/test |
| GBFS snapshot | Discovery từ endpoint chính thức, resolve version/feed thực tế, snapshot station info/status | Nhóm | Chưa có JSON snapshot; chỉ có dry-run discovery theo báo cáo trước | Chưa có snapshot — phần mở rộng, không thuộc 5 artifact cốt lõi |
| Spark Raw Read | Format hỗ trợ đọc được; `printSchema()`, `count()`, `show(5)`; Spark dừng sạch | Nhóm | `logs/raw_verify.txt`; 50,661 dòng, schema và 5 mẫu, Spark dừng sạch | Đạt — xác minh thật ngày 2026-10-21 |
| Repo & release | README, cấu trúc repo, không có Raw/log/secret, có commit hoặc tag để truy vết | Nhóm | Commit Checkpoint 2 hiện có; Raw/log không được track | Đạt ở commit cục bộ; tag riêng là tùy chọn. Bản rà soát tài liệu cần commit cùng repo trước khi nộp phiên bản cuối |

### Bằng chứng lần chạy thực tế — Jersey City, 2024-01

- Thời điểm ingest UTC: `2026-10-21T01:46:22Z`; run ID: `run_20261021T014618_890189Z`.
- Nguồn: `https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip`.
- Kích thước: `1,883,296` bytes; SHA-256: `d59d149f200fb81e204099afb0f76f1b02267bb2d605e92309e6f9974b176e15`.
- Manifest báo `status: success`, `record_count: 50661`; Spark Raw Verify cũng đếm được `50,661` dòng trong CSV và kết thúc `[OK]`.
- ZIP có 2 thành viên (CSV và metadata `__MACOSX`); verifier xác nhận archive hợp lệ và chỉ nạp thành viên CSV.
- Partition trên máy: `data/raw/citibike_trips/ingest_date=2026-10-21/run_20261021T014618_890189Z/`.
- Lần chạy ban đầu được gửi dưới dạng terminal output. Để lưu evidence, verifier đã được chạy lại ngày 2026-10-21 với `--log-file logs/raw_verify.txt`; lần chạy lưu log cũng kết thúc `[OK]`. Raw data và logs đang được `.gitignore` loại khỏi Git.
- Phạm vi bằng chứng: xác nhận một lần ingest và Spark verify; chưa có ba lần ingest live hay GBFS JSON snapshot.

### Thông tin nhóm cần chốt trước khi nộp báo cáo

- Tên nhóm và danh sách thành viên để điền trang bìa/mẫu báo cáo.
- Xác nhận JC tháng 01/2024 chỉ là snapshot minh chứng Checkpoint 2 hay cũng là phạm vi phân tích cuối; nếu làm phân tích rộng hơn, nhóm cần chốt thêm khoảng thời gian/dataset.
- Tùy chọn lineage: giữ nguyên run hiện có (manifest ghi HEAD tại thời điểm ingest) hoặc tạo run mới từ commit hiện hành nếu nhóm muốn manifest trỏ chính xác tới mã nguồn checkpoint. Các trường manifest bắt buộc của rubric đã có và khớp raw.
- Ghi blocker lớn nhất còn lại cùng người phụ trách và hạn xử lý; nếu không có blocker, nêu rõ rủi ro trước Buổi 7 thay vì tự tạo blocker.

## Exit ticket — câu trả lời dựa trên repo hiện tại

1. **Một lệnh kiểm tra môi trường:** `python src/env_check.py --log-file logs/env_check.txt`. Lệnh đã chạy thành công với Python 3.11.9, Java 21.0.8 và PySpark/Spark 3.5.9; Spark đếm đủ 1.000.000 dòng.
2. **Nguồn và snapshot:** Citi Bike System Data, archive Jersey City tháng 01/2024 (`JC-202401-citibike-tripdata.csv.zip`), 1.883.296 byte; nguồn và điều khoản được dẫn trong README.
3. **Nếu chạy ingest ba lần:** mặc định tạo ba `run_id`/partition bất biến riêng, không ghi đè; với `--skip-existing`, archive chỉ bị bỏ qua khi manifest thành công và tên file, snapshot, kích thước, SHA-256 cùng khớp. Unit test xác nhận chính sách; bằng chứng live hiện chứng minh một lần ingest.
4. **Rủi ro lớn nhất trước Buổi 7:** phạm vi phân tích hiện mới có một snapshot Jersey City tháng 01/2024. Nhóm cần xác nhận đây chỉ là dữ liệu minh chứng Checkpoint 2 hay là phạm vi phân tích; nếu phân tích rộng hơn, chốt thêm tháng/khu vực và tải dữ liệu tương ứng. Ghi người phụ trách và hạn sau khi nhóm thống nhất.

## 2. Bốn vai trò và người cung cấp evidence

- **Người 1 — Điều phối/tích hợp:** xác nhận hợp đồng chung, kiểm `.gitignore`, tích hợp các PR và tạo tag sau khi nhóm duyệt.
- **Người 2 — Environment/QA:** sở hữu smoke test, dependency và kết quả unit test; hỗ trợ Người 4 kiểm tra Spark.
- **Người 3 — Trip ingestion:** sở hữu nguồn lịch sử, download, Raw archive, manifest, checksum, logs và rerun evidence.
- **Người 4 — GBFS/Raw verify/trình bày:** sở hữu GBFS snapshot và Spark raw inspection; ghép bằng chứng đã được owner xác nhận vào slide.

## 3. Pitch tối đa 4 phút — 6 slide

1. **Bìa:** tên nhóm, đề tài, GitHub repo, commit hash/tag.
2. **Architecture Update:** một sơ đồ `Source → Ingestion → Raw Storage`; nêu trade-off công nghệ chính.
3. **Environment:** Python/Java/Spark, execution mode và smoke-test PASS.
4. **Source & Ingestion:** nguồn cụ thể, snapshot, quy mô, lệnh ingest và log thật.
5. **Raw & Manifest:** cây thư mục, dung lượng, manifest/checksum và kết quả Spark inspection.
6. **Rerun, Blocker & Next Action:** bằng chứng rerun ba lần, blocker kỹ thuật lớn nhất, owner/deadline và kế hoạch Buổi 7.

Pitch cần nêu đúng năm nội dung: Architecture Update, Environment, Data Source, Current Evidence, Blocker. Khi được hỏi, mở trực tiếp repo/log/data/manifest thật; slide không thay thế evidence.

## 4. Các giới hạn cần nói rõ

- GBFS station status là snapshot hiện tại, không phải lịch sử tồn xe. Không dùng một snapshot để khẳng định trạm từng thiếu xe trong quá khứ.
- Số chuyến đến trừ số chuyến đi có thể dùng làm net-flow proxy theo thời gian; đó không phải inventory thực tế vì không mô hình hóa điều chuyển, xe hỏng, capacity và các chuyến bị loại.
- Không commit dữ liệu Raw lớn, snapshots, logs, `.env` thật hoặc credentials lên GitHub.
- Chỉ ghi `PASS/Verified` sau khi chạy và lưu evidence tương ứng; nếu môi trường Spark còn lỗi, ghi blocker và owner xử lý.
