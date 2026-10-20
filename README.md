# StationPulse: Phân tích nhu cầu Citi Bike và mất cân bằng xe tại trạm bằng PySpark

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5.x-orange.svg)](https://spark.apache.org/)
[![Tests Passing](https://img.shields.io/badge/Tests-11%2F11%20Passed-brightgreen.svg)]()
[![License: Academic](https://img.shields.io/badge/License-Academic%20Use%20Only-lightgrey.svg)]()

StationPulse là pipeline kỹ thuật dữ liệu học thuật được xây dựng bằng **PySpark** nhằm phân tích quy luật di chuyển, nhu cầu sử dụng hệ thống xe đạp chia sẻ **Citi Bike** (New York City & New Jersey), đồng thời đánh giá hiện tượng mất cân bằng trạm (thiếu xe / thiếu chỗ trả dock) theo thời gian.

Dự án được thiết kế cho nhóm **5 sinh viên** làm việc song song, bám sát yêu cầu đồ án:
- **Buổi 6 (Hiện tại)**: Thiết lập môi trường chuẩn, Ingestion đáng tin cậy (streaming, backoff, atomic rename, SHA-256), lưu trữ Raw bất biến theo partition, sinh Manifest truy vết nguồn gốc, chạy lại (rerun) an toàn không ghi đè, và xác minh dữ liệu thô bằng PySpark DataFrame.
- **Buổi 7 (Kế tiếp)**: Chuyển đổi và tổng hợp dữ liệu với Spark DataFrame / Spark SQL, tính toán chỉ số Net-Flow trạm theo giờ và mô hình hóa mất cân bằng trạm.

---

## 1. Phân chia công việc nhóm 5 người (5-Person Team Split)

| Thành viên | Vai trò | Trách nhiệm chính | File phụ trách | Nhánh đề xuất |
| :--- | :--- | :--- | :--- | :--- |
| **Member 1** | **Coordinator & Integration** | Quy chuẩn repo, freeze cấu hình, điều phối PR, tag checkpoint | `README.md`, `config/`, `pyproject.toml`, `src/common/` | `feat/coordinator-integration` |
| **Member 2** | **Spark Environment** | Kiểm tra môi trường (Python, Java, PySpark), smoke test, tài liệu setup | `src/env_check.py`, `requirements.txt` | `feat/spark-environment` |
| **Member 3** | **Historical Trips Ingestion** | Tải streaming dữ liệu chuyến đi, retry có backoff, ghi atomic, mã băm SHA-256, xử lý ZIP nhiều CSV | `src/ingest_trips.py`, `tests/test_rerun_policy.py` | `feat/trips-ingestion` |
| **Member 4** | **GBFS Feeds & Raw Verify** | Tự động phát hiện endpoint qua `gbfs.json`, chụp snapshot JSON, xác thực schema Spark (`printSchema`, `count`, `show`) | `src/ingest_gbfs.py`, `src/raw_verify.py` | `feat/gbfs-raw-verify` |
| **Member 5** | **Architecture & Presentation** | Sơ đồ kiến trúc, checklist bằng chứng Buổi 6, slide báo cáo (6 slide, ≤ 4 phút) | `docs/architecture.md`, `docs/evidence_checklist.md`, slide | `docs/slides-architecture` |

Chi tiết quy trình làm việc và tiêu chí nghiệm thu được lưu tại [`docs/team_work_split.md`](docs/team_work_split.md).

---

## 2. Cấu trúc thư mục (Repository Layout)

```
CitiBike-StationPulse/
├── README.md
├── CONTRIBUTING.md
├── requirements.txt
├── .env.example
├── .gitignore
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── env_check.py          # Script kiểm tra Python, Java, PySpark smoke test
│   ├── ingest_trips.py       # CLI tải lịch sử chuyến đi hàng tháng
│   ├── ingest_gbfs.py        # CLI phát hiện và chụp snapshot GBFS realtime
│   ├── raw_verify.py         # CLI xác thực tệp thô bằng Apache Spark
│   └── common/
│       ├── __init__.py
│       ├── config.py         # Quản lý cấu hình tập trung
│       ├── hashing.py        # Tính toán SHA-256 streaming
│       ├── logging_utils.py  # Structured UTC logging & khử nhạy cảm token
│       └── manifest.py       # Định nghĩa, kiểm tra và ghi manifest.json atomic
├── config/
│   └── project.example.yaml  # File cấu hình mẫu (không chứa bí mật)
├── docs/
│   ├── architecture.md       # Sơ đồ kiến trúc Mermaid & luồng dữ liệu
│   ├── data_dictionary.md    # Từ điển dữ liệu Trip CSV & GBFS JSON
│   ├── team_work_split.md    # Phân công chi tiết và quy trình PR cho 5 người
│   ├── evidence_checklist.md # Checklist bằng chứng nghiệm thu Buổi 6 & dàn ý slide
│   └── analysis_questions.md # Định hướng phân tích và công thức Net-Flow Buổi 7
├── data/
│   ├── raw/                  # Lưu trữ Trip data bất biến (được gitignore)
│   ├── snapshots/            # Lưu trữ GBFS JSON snapshots (được gitignore)
│   └── sample/               # Ghi chú dữ liệu mẫu nhỏ
├── logs/                     # File nhật ký thực thi (được gitignore)
├── metadata/                 # Ghi chú manifest & truy vết kiểm toán
└── tests/
    ├── test_hashing.py       # Unit test mã băm SHA-256
    ├── test_manifest.py      # Unit test cấu trúc manifest
    └── test_rerun_policy.py  # Unit test cách ly rerun & không ghi đè
```

---

## 3. Yêu cầu hệ thống & Hướng dẫn cài đặt (Windows PowerShell)

### 3.1 Điều kiện tiên quyết (Prerequisites)
1. **Python**: Bản 3.10 hoặc 3.11 64-bit (Đã kiểm tra tương thích trên Python 3.11.9).
2. **Java Runtime (JDK)**: Java 17 hoặc Java 21 LTS (Đã cài đặt sẵn trên máy: `Java 21.0.8 LTS`). Hãy đảm bảo biến môi trường `JAVA_HOME` đã trỏ đúng tới thư mục cài đặt Java.
3. **Apache Spark / PySpark**: Bản 3.5.x.

### 3.2 Khởi tạo môi trường ảo & cài đặt thư viện
Mở **Windows PowerShell** và thực hiện:

```powershell
# Chuyển vào thư mục dự án
Set-Location D:\CitiBike-StationPulse

# Tạo môi trường ảo Python
python -m venv .venv

# Kích hoạt virtual environment
.\.venv\Scripts\Activate.ps1

# Nâng cấp pip và cài đặt thư viện
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 4. Hướng dẫn thực thi & Bằng chứng nghiệm thu (Buổi 6)

### Bước 1: Kiểm tra môi trường (Environment Check)
Chạy script kiểm tra để xác nhận Python, Java, PySpark và thực thi smoke test Spark sinh và đếm 1,000,000 bản ghi trong bộ nhớ:

```powershell
python src/env_check.py --log-file logs/env_check.txt
```

*Lưu ý: Kết quả thực thi sẽ được xuất ra console và lưu lại tại `logs/env_check.txt`.*

### Bước 2: Chạy kiểm thử tự động (Unit Tests)
Hệ thống đi kèm bộ kiểm thử không phụ thuộc mạng (100% mocked):

```powershell
python -m pytest tests -v
```
*(Kết quả: 11/11 bài test đã vượt qua thành công bao gồm hashing, manifest schema, atomic write, zip line count, và rerun isolation).*

### Bước 3: Ingestion dữ liệu lịch sử chuyến đi (Historical Trips)
CLI hỗ trợ tham số `--dry-run` để kiểm tra kết nối và kích thước file trước khi tải thực tế:

```powershell
# Kiểm tra thử (Dry-run) tháng 01/2024 bộ dữ liệu Jersey City (nhẹ ~1.8 MB)
python src/ingest_trips.py --dataset jc --year 2024 --month 1 --dry-run

# Tải thực tế và ghi vào Raw Storage kèm Manifest
python src/ingest_trips.py --dataset jc --year 2024 --month 1
```

Dữ liệu được lưu trữ bất biến tại:
`data/raw/citibike_trips/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/JC-202401-citibike-tripdata.csv.zip`

### Bước 4: Ingestion dữ liệu GBFS Real-time (Optional Extension)
CLI tự động tìm kiếm endpoint từ `gbfs.json` và chụp snapshot 2 feed `station_information` và `station_status`:

```powershell
# Kiểm tra phát hiện feed tự động (Dry-run)
python src/ingest_gbfs.py --dry-run

# Chụp snapshot trực tiếp
python src/ingest_gbfs.py
```

Dữ liệu được lưu tại:
`data/snapshots/gbfs/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>/`

### Bước 5: Xác minh dữ liệu thô bằng PySpark (Raw Verification)
Chạy script xác minh để Spark đọc tệp thô, in schema, đếm số dòng và hiển thị 5 dòng đầu:

```powershell
# Xác thực file ZIP vừa tải (hoặc file JSON snapshot)
python src/raw_verify.py --path data/raw/citibike_trips/ingest_date=2026-10-20/run_*/JC-202401-citibike-tripdata.csv.zip --log-file logs/raw_verify.txt
```

---

## 5. Cấu trúc Manifest mẫu (`manifest.json`)

Mỗi phân vùng dữ liệu thô luôn đi kèm một file `manifest.json` nguyên tử:

```json
{
  "schema_version": "1.0.0",
  "dataset": "citibike_trips",
  "source_url": "https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip",
  "source_snapshot": "2024-01",
  "ingested_at_utc": "2026-10-20T10:35:00Z",
  "run_id": "run_20261020T103500Z",
  "filename": "JC-202401-citibike-tripdata.csv.zip",
  "bytes": 1883296,
  "record_count": 50601,
  "sha256": "3a886df43c499c8f0cf4e97a3cf5f8e5f8f81234567890abcdef1234567890ab",
  "destination": "data/raw/citibike_trips/ingest_date=2026-10-20/run_20261020T103500Z/JC-202401-citibike-tripdata.csv.zip",
  "status": "success",
  "pipeline_version": "0.1.0",
  "git_commit": "e4a2c91",
  "notes": "Internal CSV files: 1 files (JC-202401-citibike-tripdata.csv)"
}
```

---

## 6. Chính sách an toàn Rerun & Bất biến (Rerun Policy)

- **Không ghi đè (No Overwrite)**: Mỗi lần chạy script ingestion sẽ tạo một thư mục định danh thời gian duy nhất `run_<UTC_TIMESTAMP>`.
- **Bảo toàn lịch sử**: Khi chạy lại script 3 lần liên tiếp, hệ thống sẽ bảo toàn 3 thư mục độc lập với đầy đủ file artifact và file manifest riêng biệt.
- **Atomic Rename**: Tệp tải về được ghi tạm vào đuôi `.tmp` và chỉ được đổi tên chính thức khi toàn bộ dữ liệu đã được ghi và khớp kích thước. Trường hợp mất mạng hoặc lỗi giữa chừng, tệp `.tmp` sẽ được dọn dẹp sạch sẽ, không để lại file rác hay file hỏng trong thư mục Raw.

---

## 7. Giới hạn & Bản chất của GBFS Realtime (Limitations)

> [!WARNING]
> **Điểm mấu chốt về GBFS**:
> GBFS là giao diện cung cấp trạng thái **thời gian thực tức thời** (point-in-time snapshot) của hệ thống trạm, **KHÔNG PHẢI** là dữ liệu lịch sử. Việc tải một snapshot đơn lẻ không thể dùng để kết luận về tình trạng thiếu xe hay thừa xe trong quá khứ.
>
> Trong đồ án này:
> 1. Dữ liệu GBFS phục vụ mục đích mở rộng: quan sát trạng thái dock hiện tại và thông tin vị trí trạm.
> 2. Việc phân tích mất cân bằng trạm trong lịch sử sẽ được ước lượng thông qua **chỉ số Net-Flow đại diện (Proxy Imbalance Metric)** tính toán từ lịch sử các chuyến đi đón/trả tại trạm theo khung giờ (thực hiện ở Buổi 7).

---

## 8. Quyền sở hữu dữ liệu & Tuyên bố miễn trừ (Attribution & Policy)

Dự án này sử dụng dữ liệu mở công khai được cung cấp bởi hệ thống xe đạp công cộng Citi Bike (thành phố New York và New Jersey).
- **Nguồn dữ liệu chính thức**: [Citi Bike System Data](https://citibikenyc.com/system-data)
- **Chính sách chia sẻ dữ liệu**: [Citi Bike Data Sharing Policy](https://citibikenyc.com/data-sharing-policy)

**Cam kết tuân thủ chính sách**:
1. Dữ liệu chỉ được truy xuất qua các giao diện và endpoint chính thức được công bố; không thu thập trái phép (scraping).
2. Dữ liệu không được bán lại, thương mại hóa, hoặc tái phân phối như một bộ dataset độc lập.
3. Không thực hiện các thuật toán nhằm suy đoán, tái định danh hoặc can thiệp vào danh tính cá nhân của người sử dụng.
4. Dự án này phục vụ mục đích học thuật và nghiên cứu nội bộ, **không** được tài trợ, bảo trợ hay liên kết chính thức với Citi Bike, NYC Bikeshare, LLC hoặc Lyft.
