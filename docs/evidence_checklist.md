# Checklist evidence và dàn ý trình bày Buổi 6

Checklist này dùng để chuẩn bị nghiệm thu Checkpoint 2. Trạng thái chỉ được đổi thành Đạt khi owner có bằng chứng của lần chạy thực tế. Các mục được cập nhật dưới đây dựa trên log terminal do nhóm cung cấp ngày 2026-10-21; các mục khác vẫn cần owner xác minh.

## 1. Bảng evidence bàn giao

| Hạng mục | Yêu cầu | Owner chính | Bằng chứng cần lưu | Trạng thái |
|---|---|---|---|---|
| Environment | Python/Java/PySpark/Spark version; `spark.range(1_000_000).count()`; exit code 0 | Người 2 | `logs/env_check.txt` hoặc terminal capture từ lần chạy nộp | Chưa xác minh |
| Source | URL chính thức, tháng/snapshot, license/terms, dung lượng và quyền truy cập | Người 3 | URL trong README/manifest, thời điểm kiểm tra, thông tin snapshot | Chưa xác minh |
| Ingestion | Tải tự động, timeout/retry hữu hạn, kiểm tra payload, atomic write, SHA-256 | Người 3 | Log lần chạy 2026-10-21; manifest và checksum | Đạt — một lần tải |
| Raw Storage | Dữ liệu nguyên bản, file >0 byte, cấu trúc theo ngày/run, không overwrite | Người 3 | Partition `ingest_date=2026-10-21/run_20261021T014618_890189Z`; ZIP 1,883,296 bytes | Đạt — một partition |
| Manifest & lineage | Source, UTC timestamp, bytes, row count hoặc unknown, SHA-256, run_id, destination, status | Người 1 (schema), Người 3 (trip), Người 4 (GBFS) | Manifest trong run partition; đối chiếu hash/bytes | Chưa xác minh |
| Logs | Log có thời gian UTC, lỗi rõ; không lộ token/header/secret | Owner từng module; Người 2 review | Log console/file đã rà soát | Chưa xác minh |
| Rerun safety | Ba lần chạy không ghi đè; các run riêng hoặc skip cùng hash có log | Người 3 | Ba run directory/manifest hoặc test kết quả | Chưa xác minh |
| GBFS snapshot | Discovery từ endpoint chính thức, resolve version/feed thực tế, snapshot station info/status | Người 4 | URL/version, UTC timestamp, JSON snapshots và hash | Chưa xác minh |
| Spark Raw Read | Format hỗ trợ đọc được; `printSchema()`, `count()`, `show(5)`; Spark dừng sạch | Người 4 (Người 2 hỗ trợ) | `logs/raw_verify.txt`; 50,661 dòng, schema và 5 mẫu, Spark dừng sạch | Đạt — xác minh lại và lưu log |
| Repo & release | README, cấu trúc repo, không có Raw/log/secret, commit hash và tag | Người 1 | `git status`, commit hash, tag `checkpoint-2` | Chưa xác minh |

### Bằng chứng lần chạy thực tế — Jersey City, 2024-01

- Thời điểm ingest UTC: `2026-10-21T01:46:22Z`; run ID: `run_20261021T014618_890189Z`.
- Nguồn: `https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip`.
- Kích thước: `1,883,296` bytes; SHA-256: `d59d149f200fb81e204099afb0f76f1b02267bb2d605e92309e6f9974b176e15`.
- Manifest báo `status: success`, `record_count: 50661`; Spark Raw Verify cũng đếm được `50,661` dòng trong CSV và kết thúc `[OK]`.
- ZIP có 2 thành viên (CSV và metadata `__MACOSX`); verifier xác nhận archive hợp lệ và chỉ nạp thành viên CSV.
- Partition trên máy: `data/raw/citibike_trips/ingest_date=2026-10-21/run_20261021T014618_890189Z/`.
- Lần chạy ban đầu được gửi dưới dạng terminal output. Để lưu evidence, verifier đã được chạy lại ngày 2026-10-21 với `--log-file logs/raw_verify.txt`; lần chạy lưu log cũng kết thúc `[OK]`. Raw data và logs đang được `.gitignore` loại khỏi Git.
- Phạm vi bằng chứng: xác nhận một lần ingest và một lần Spark verify; chưa chứng minh rerun ba lần hay GBFS snapshot.

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
