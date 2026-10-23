# Phân công nhiệm vụ nhóm 4 người — StationPulse Checkpoint 2

> **Lưu ý trước khi nộp:** Đây là phương án phân công và chia phần trả lời dựa trên cấu trúc code/evidence hiện có trong repository. Nhóm cần thay tên, MSSV, và chỉnh mô tả cho đúng phần mỗi người thực sự đã làm. Không dùng tài liệu này để nhận công thay cho thành viên khác.

## Thông tin nhóm

| Vai trò | Họ tên | MSSV | Phần chịu trách nhiệm | Bằng chứng cá nhân cần chỉ ra |
|---|---|---|---|---|
| Người 1 — Bạn, thuyết trình và tích hợp | __________________ | __________________ | Mở đầu, dẫn data flow, tích hợp repo/README và kết luận. | Commit/PR hoặc phần README/tích hợp do bạn thực hiện; mở repo và hướng dẫn demo. |
| Người 2 — Môi trường và QA | __________________ | __________________ | Python/Java/PySpark, smoke test, test suite. | `src/env_check.py`, `requirements.txt`, `tests/`; lệnh và kết quả đã chạy. |
| Người 3 — Nguồn dữ liệu và ingestion | __________________ | __________________ | Nguồn Citi Bike, tải archive, raw partition, manifest/hash. | `src/ingest_trips.py`, `src/common/`, raw ZIP/manifest cục bộ; giải thích rerun. |
| Người 4 — Spark verification và phân tích | __________________ | __________________ | Đọc Raw bằng Spark, schema/count/sample và aggregation. | `src/raw_verify.py`, `src/analyze_trips.py`, tài liệu phân tích/output cục bộ. |

## Ma trận trách nhiệm và phần trình bày

| Người | Công việc chính | Phần nói | Câu hỏi nên nhận | Trình diễn |
|---|---|---|---|---|
| **1 — Bạn (presenter/integration)** | Kết nối các phần thành pipeline; kiểm tra README, repo và trạng thái bàn giao; giới thiệu mục tiêu/phạm vi và kết luận. Chỉ ghi nhận tích hợp nếu bạn thực sự làm. | Slide 1–2 và kết luận: đề tài, mục tiêu, sơ đồ `Source → Ingestion → Raw → Verify`, giới hạn phạm vi. | Dự án giải quyết việc gì? Pipeline chạy theo thứ tự nào? Vì sao lưu Raw riêng? | Mở repo, README, sơ đồ kiến trúc; chuyển lượt cho đúng owner kỹ thuật. |
| **2 — Environment/QA** | Thiết lập môi trường có thể tái lập; chạy environment check và test. | Slide 3: Python/Java/PySpark, local mode, smoke test, trạng thái test. | PySpark chạy ở đâu? Smoke test chứng minh điều gì? Bộ test có bao nhiêu test? | Chạy `env_check.py`; nếu cần, chạy pytest hoặc mở kết quả mới nhất đã xác thực. |
| **3 — Ingestion/Raw/manifest** | Tải archive tháng đã chọn, giữ nguyên file gốc, tạo run partition/manifest, kiểm tra bytes và SHA-256. | Slide 4–5: nguồn, snapshot, dung lượng, raw path, manifest và rerun policy. | Nguồn nào? Làm sao phát hiện tải thiếu/sai? Chạy nhiều lần có ghi đè không? | Mở manifest và ZIP cục bộ; đối chiếu bytes/hash. Không cần tải lại khi thuyết trình. |
| **4 — Spark verify/analysis** | Xác minh archive bằng Spark và giải thích phần phân tích hiện có; nêu giới hạn chỉ số. | Slide 5–6: schema/count/sample, kết quả phân tích, giới hạn và bước tiếp theo. | `count()` đếm gì? Net-flow dương/âm nghĩa gì? Vì sao không phải tồn kho xe thật? | Mở `raw_verify.py`, `analyze_trips.py` và output nếu cần. |

### Quy tắc để việc phân công đáng tin

- Tên trong bảng là **owner trả lời và chuẩn bị demo**, không tự động chứng minh người đó đã viết toàn bộ file. Hãy sửa theo lịch sử commit, PR, ghi chú làm việc hoặc phần code thực tế.
- Mỗi người hiểu luồng chung và có thể trả lời ít nhất: nguồn/tháng dữ liệu, Raw/manifest/hash, cách chạy lại, và giới hạn kết quả.
- Người trình bày điều phối thời gian, không trả lời thay owner nếu câu hỏi cần chi tiết phần code của họ.
- Nếu một phần do nhiều người cùng làm, ghi đồng sở hữu và nêu việc cụ thể từng người đã thực hiện.

## Lịch tập nói gợi ý — 4 phút

| Thời gian | Người nói | Nội dung |
|---|---|---|
| 0:00–0:30 | Người 1 | Tên đề tài, mục tiêu, phạm vi JC tháng 01/2024 và pipeline tổng quan. |
| 0:30–1:00 | Người 2 | Môi trường local Spark và smoke test. |
| 1:00–1:50 | Người 3 | Source, ingestion, raw partition, manifest/hash và rerun. |
| 1:50–2:40 | Người 4 | Spark schema/count/sample; nếu cần, aggregation net-flow. |
| 2:40–3:20 | Người 1 | Evidence chính, tình trạng/bước tiếp theo và kết luận. |
| 3:20–4:00 | Cả nhóm | Dự phòng chuyển slide/câu hỏi ngắn; điều chỉnh theo thời lượng lớp. |

## Checklist trước khi trình bày

- [ ] Đã thay đủ tên, MSSV và sửa nhiệm vụ theo đóng góp thực tế.
- [ ] Mỗi owner biết mở đúng file, log, manifest hoặc output của mình.
- [ ] Chỉ nói số liệu có evidence; phân biệt lần chạy cũ và lần chạy demo hiện tại.
- [ ] Không đưa Raw lớn, `.venv`, log runtime hoặc secret lên GitHub.
- [ ] Presenter có sẵn lệnh demo trong [tài liệu đọc hiểu dự án](tai_lieu_doc_hieu_du_an_de_thuyet_trinh.md).
