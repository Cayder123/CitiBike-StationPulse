# Phân công công việc và phối hợp nhóm 4 người

## Mục tiêu chung

Nhóm hoàn thành Checkpoint 2 bằng bằng chứng chạy thực tế cho chuỗi **Environment → Source → Ingestion → Raw Storage → Verification**. Mỗi thành viên chịu trách nhiệm một phần có đầu ra kiểm tra được, đồng thời phải hiểu data flow chung để hỗ trợ demo và trả lời câu hỏi. Bản phân công này thay thế kế hoạch 5 người trước đây.

> Điền họ tên, MSSV và ngày hạn cho từng người trước khi bắt đầu. Các mốc dưới đây được ghi theo thứ tự phụ thuộc vì nhóm chưa cung cấp ngày nộp cụ thể.

## 1. Phân công chính

| Thành viên | Vai trò | Phạm vi file chính | Bàn giao / tiêu chí hoàn thành |
|---|---|---|---|
| **Người 1 — Điều phối, hợp đồng chung và tích hợp** | Quản lý backlog, quy ước repo, cấu hình và các giao diện dùng chung; tích hợp PR, chạy end-to-end, chốt phiên bản nộp. | `README.md`, `CONTRIBUTING.md`, `config/`, `src/common/`, `.gitignore`, tích hợp toàn repo. | Chốt sớm manifest schema, CLI/path contract và phiên bản môi trường; xác nhận không commit Raw, snapshots, logs hoặc secrets; README có lệnh setup/run; review PR; chạy pipeline từ README; lưu commit hash và tạo tag `checkpoint-2` sau khi nhóm xác nhận đạt. |
| **Người 2 — Môi trường Spark, dependency và QA** | Làm môi trường tái lập trên máy nhóm; xác minh Spark; phụ trách kiểm thử tự động và evidence kiểm tra. | `src/env_check.py`, `requirements.txt`, `pyproject.toml`, `tests/`, phần setup trong README (phối hợp Người 1). | Python/Java/PySpark version được ghi nhận; `python src/env_check.py` chạy smoke test `spark.range(1_000_000).count()` và exit code 0; hướng dẫn Windows PowerShell rõ ràng; unit tests không phụ thuộc mạng; ghi log evidence sau khi xác nhận thực tế. Phối hợp Người 4 kiểm tra Spark đọc Raw. |
| **Người 3 — Lịch sử chuyến đi và ingestion Raw** | Chịu trách nhiệm nguồn lịch sử Citi Bike và job tải dữ liệu an toàn, có thể chạy lại và truy vết. | `src/ingest_trips.py`, tests ingestion/rerun, phần source/ingestion trong `docs/data_dictionary.md`. | Cho phép chọn tháng/URL; stream download với timeout, retry hữu hạn, kiểm tra status/payload/kích thước; ghi `.tmp` rồi atomic rename; giữ nguyên archive/file gốc; tính bytes và SHA-256; tạo manifest theo hợp đồng chung; mỗi lần chạy tách `run_<UTC_TIMESTAMP>`; chạy thử ba lần không overwrite; xử lý ZIP có nhiều CSV nếu cần đếm records và ghi rõ cách đếm. Không tải/commit dữ liệu lớn vào repo. |
| **Người 4 — GBFS, Raw verification, kiến trúc và trình bày** | Làm snapshot feed GBFS có discovery; xác minh Raw bằng Spark; chuẩn bị sơ đồ, bằng chứng và pitch. | `src/ingest_gbfs.py`, `src/raw_verify.py`, `docs/architecture.md`, `docs/evidence_checklist.md`, `docs/analysis_questions.md`, slide. | GBFS được phát hiện từ entry point chính thức và resolve URLs/version thực tế; lưu JSON snapshot bất biến, timestamp UTC và SHA-256; `raw_verify.py` hỗ trợ format đã công bố, in `printSchema()`, `count()`, `show(5)` khi Spark đọc được; evidence checklist khớp trạng thái thật; làm 6 slide và pitch ≤4 phút; nêu rõ GBFS là snapshot hiện tại, không đại diện lịch sử tồn xe. |

### Việc chung bắt buộc

- Cả bốn thành viên chạy theo README ít nhất một lần hoặc trực tiếp tham gia walkthrough end-to-end.
- Mỗi người review tối thiểu một PR ngoài phần mình; chủ PR chịu trách nhiệm giải thích thay đổi và evidence.
- Không ghi dữ liệu giả, log giả, hash giả hoặc trạng thái “Verified” khi chưa chạy. Nếu còn blocker PySpark, ghi rõ là blocker và chuẩn bị kế hoạch khắc phục trước demo.
- Người 4 làm slide/thuyết trình nhưng vẫn phải nắm kỹ thuật GBFS/Raw verification; Người 1 tích hợp nhưng không nhận hết phần code của thành viên khác.

## 2. Phụ thuộc và quy tắc bàn giao

1. **Người 1 chốt hợp đồng chung trước khi các nhánh phát triển sâu:** cấu trúc Raw; tên tham số CLI; quy ước `run_id`; manifest schema; cách ghi log; cấu hình `config/project.example.yaml`.
2. **Người 2 xác nhận môi trường và dependency sớm.** Nếu PySpark chưa cài, mở blocker rõ ràng; Người 3 và Người 4 vẫn làm phần độc lập (logic download, manifest mock, GBFS parsing và tests) nhưng không tuyên bố Spark verification hoàn tất.
3. **Người 3 dùng `src/common/manifest.py` và hashing của Người 1** sau khi interface đã freeze; nếu cần đổi schema, đề xuất qua issue/PR và phối hợp Người 1 trước.
4. **Người 4 dùng manifest contract chung** cho snapshots GBFS và báo rõ format/đường dẫn đầu vào mà `raw_verify.py` nhận.
5. **Người 1 tích hợp sau review.** Trình tự smoke test → ingest history → kiểm manifest/hash → Spark raw verify → chạy lại ba lần → chốt evidence.
6. **Người 4 chỉ đưa vào slide số liệu đã được owner tương ứng xác minh.** Ảnh terminal, manifest, tree và commit hash lấy từ lần chạy thật; slide không thay cho việc mở repo/log/file tại bàn.

## 3. Lộ trình làm việc theo mốc

| Mốc | Người phụ trách | Việc hoàn tất |
|---|---|---|
| **A. Chốt đề tài và hợp đồng** | Cả nhóm; Người 1 điều phối | Điền tên/owner/deadline; thống nhất snapshot tháng nào; chốt cấu trúc thư mục, manifest và lệnh chạy. Xác nhận license/terms và nguồn chính thức. |
| **B. Làm song song theo module** | Người 2, 3, 4; Người 1 làm cấu hình chung | Nhánh riêng; code/test/docs trong phạm vi đã giao; mở PR nhỏ; không chỉnh file shared mà chưa phối hợp. |
| **C. Tích hợp kỹ thuật** | Người 1; cả nhóm hỗ trợ | Chạy đúng README; smoke test Spark; ingest; đối chiếu bytes/hash/manifest; kiểm tra file Raw; Spark `printSchema/count/show(5)`; rerun ba lần. |
| **D. Chuẩn bị demo** | Người 4 tổng hợp; mỗi owner cung cấp evidence | Chốt sơ đồ và 6 slide, pitch ≤4 phút; điền blocker lớn nhất, owner và deadline; tập mở repo/log/raw/manifest thật. |
| **E. Chốt Checkpoint 2** | Cả nhóm; Người 1 release | Đối checklist giảng viên; hoàn thiện báo cáo/exit ticket; commit cuối; tạo tag `checkpoint-2` và ghi hash. |

### Gợi ý quản lý deadline

Nhóm tự thay các mốc `T-...` theo lịch học: **T-5 ngày** chốt source/hợp đồng; **T-3 ngày** hoàn thành module; **T-2 ngày** tích hợp trên một máy; **T-1 ngày** chốt evidence/slide và tập pitch. Nếu nhóm còn dưới 5 ngày, gộp mốc nhưng vẫn phải ưu tiên smoke test và ingest thật trước khi đánh bóng slide.

## 4. GitHub workflow cho 4 người

- Dùng **một GitHub repo chung**: `CitiBike-StationPulse`; mỗi người làm trên branch riêng, ví dụ `feat/coordinator-integration`, `feat/spark-qa`, `feat/trips-ingestion`, `feat/gbfs-raw-verify-docs`.
- Không push trực tiếp vào `main`. Mỗi PR ghi: mục tiêu, file thay đổi, cách chạy, test/evidence và blocker còn lại; ít nhất một bạn khác review.
- Shared files gồm `requirements.txt`, `pyproject.toml`, `src/common/manifest.py`, `config/project.example.yaml`, phần cấu hình trong README. Người 1 điều phối thay đổi các file này để giảm conflict.
- Chỉ commit code, tài liệu, config mẫu, manifest mẫu không giả lập dữ liệu thật, và sample nhỏ đã được phép. `.gitignore` phải chặn `.env`, `data/raw/**`, `data/snapshots/**`, `logs/**`, `.venv/` và Spark artifacts.
- Sau khi merge, Người 1 chạy lại từ trạng thái `main`. Chỉ tạo tag `checkpoint-2` khi evidence đã được xác minh.
- Chia sẻ dataset bằng URL tải chính thức và hướng dẫn tải lại, không upload Raw lớn lên GitHub. Không commit API key/token; nếu lỡ lộ secret, phải thu hồi/rotate.

## 5. Checklist nghiệm thu theo cẩm nang Buổi 6

1. **Environment:** Python/Java/Spark versions; `env_check.py` PASS với count 1,000,000 và exit code 0; evidence ở `logs/env_check.txt` hoặc terminal capture đã kiểm tra.
2. **Source:** URL/file tháng cụ thể, nguồn gốc, snapshot period, thời điểm truy cập và điều khoản sử dụng.
3. **Ingestion:** script tham số hóa, xử lý lỗi rõ; Raw giữ nguyên bytes; không filter/chỉnh dữ liệu ở bước ingest.
4. **Raw Storage:** `data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/`; file tồn tại, kích thước >0, không ghi đè run cũ.
5. **Manifest & logs:** URL, UTC time, bytes, record count (hoặc unknown nếu chưa tính), SHA-256, destination, run_id, pipeline version, status; đối chiếu với artifact thật.
6. **Spark inspection:** `raw_verify.py` in schema/count/5 dòng mẫu trên format được hỗ trợ; lưu `logs/raw_verify.txt` sau khi chạy thực tế.
7. **Rerun/fault handling:** ba lần chạy tạo các run riêng hoặc skip cùng hash có log giải thích; nêu rõ xử lý timeout/đứt mạng/partial write.
8. **Repo/pitch:** cấu trúc repo đúng, không có Raw lớn/secrets; commit/tag; pitch tối đa 4 phút nêu Architecture Update, Environment, Data Source, Current Evidence, Blocker.

## 6. Dàn ý slide 6 trang phù hợp buổi pitch

1. **Bìa:** tên nhóm, đề tài, repo, commit/tag.
2. **Architecture Update:** một sơ đồ Source → Ingestion → Raw Storage; nêu trade-off chính.
3. **Environment Evidence:** Python/Java/Spark, execution mode, smoke-test PASS.
4. **Data Source & Ingestion:** nguồn/snapshot/quy mô; lệnh ingest và log thật.
5. **Raw Storage & Manifest:** cây thư mục, kích thước file, manifest/hash và Spark inspection.
6. **Rerun, Blocker & Next Action:** kết quả rerun ba lần, blocker + owner + deadline, chuẩn bị Buổi 7.

Bài pitch phải mở được repo/log/data thật khi được yêu cầu; không dùng slide toàn chữ để thay bằng chứng kỹ thuật.

## 7. Câu hỏi Exit Ticket và Buổi 7

Mỗi thành viên cần chuẩn bị trả lời: (1) lệnh kiểm tra environment; (2) nguồn và cách xác định snapshot/version; (3) Raw thay đổi thế nào khi chạy ingestion ba lần; (4) rủi ro kỹ thuật lớn nhất trước Buổi 7, owner và deadline.

Buổi 7 mới tập trung Spark DataFrame/Spark SQL: tổng hợp rides theo giờ/ngày/trạm, so sánh member/casual và loại xe, nối trạm nếu phù hợp, tính net-flow proxy = chuyến kết thúc − chuyến bắt đầu. Đây là proxy nhu cầu/luồng chuyến, không phải số xe tồn thực tế; GBFS chỉ là snapshot hiện thời trừ khi nhóm tự thu chuỗi snapshot theo lịch.

## 8. Thông tin nhóm điền

- Tên nhóm / đề tài: ______________________________________________
- GitHub repo: ____________________________________________________
- Ngày Checkpoint 2: ______________________________________________
- Người 1: __________________________  MSSV: _____________________
- Người 2: __________________________  MSSV: _____________________
- Người 3: __________________________  MSSV: _____________________
- Người 4: __________________________  MSSV: _____________________
