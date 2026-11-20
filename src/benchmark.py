import argparse
import csv
import os
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Cấu hình UTF-8 cho console để tránh lỗi UnicodeEncodeError trên Windows cp1258
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
os.environ["PYTHONIOENCODING"] = "utf-8"

# Tự động đảm bảo PySpark dùng đúng Python của venv
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Tự động định vị JAVA_HOME nếu terminal chưa load kịp biến môi trường
if "JAVA_HOME" not in os.environ:
    candidate_jdk = Path(os.environ.get("USERPROFILE", "C:/Users/ASUS")) / "jdk-21"
    if candidate_jdk.exists():
        subdirs = [d for d in candidate_jdk.iterdir() if d.is_dir()]
        jdk_home = subdirs[0] if subdirs else candidate_jdk
        os.environ["JAVA_HOME"] = str(jdk_home)
        bin_dir = str(jdk_home / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"

# Định nghĩa các kịch bản thử nghiệm theo luật One-Change-At-A-Time
EXPERIMENTS = {
    "E00_baseline": {
        "script": "src/pipeline.py",  # Cập nhật lại tên file pipeline gốc của nhóm nếu khác
        "args": [],
        "desc": "Baseline (200 partitions, BroadcastHashJoin default, no cache)"
    },
    "E01_partitions4": {
        "script": "src/pipeline_optimized.py",
        "args": ["--partitions", "4"],
        "desc": "Lever 1: shuffle.partitions=4"
    },
    "E02_broadcast": {
        "script": "src/pipeline_optimized.py",
        "args": ["--partitions", "4", "--broadcast"],
        "desc": "Lever 2: + explicit broadcast(stations)"
    },
    "E03_optimized": {
        "script": "src/pipeline_optimized.py",
        "args": ["--partitions", "4", "--broadcast", "--cache"],
        "desc": "Lever 3: + cleaned_trips.cache() (Full Optimized)"
    }
}

FIELDNAMES = [
    "timestamp", "experiment_id", "description", "runs", "run_times_sec",
    "median_sec", "mean_sec", "variance_sec", "stdev_sec", "cv_pct"
]

def run_single_execution(script_path, extra_args):
    """Chạy 1 lần pipeline và trả về thời gian thực thi (giây)."""
    cmd = [sys.executable, script_path] + extra_args
    start_time = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True, env=os.environ)
    elapsed = time.perf_counter() - start_time

    if result.returncode != 0:
        print(f"\n[ERROR] Lỗi khi chạy {' '.join(cmd)}:")
        print(result.stderr)
        sys.exit(1)

    return round(elapsed, 2)

def benchmark_experiment(exp_id, config, runs, warmup=False):
    print(f"\n--- Đang chạy {exp_id}: {config['desc']} ---")
    if not os.path.exists(config["script"]):
        print(f"[SKIP] Không tìm thấy file {config['script']}. Bỏ qua kịch bản này.")
        return None

    if warmup:
        print("  [Warm-up] Đang chạy nháp 1 lần để làm nóng JVM/OS cache...")
        w_time = run_single_execution(config["script"], config["args"])
        print(f"  [Warm-up] Hoàn tất trong {w_time}s (Không tính vào kết quả).")

    run_times = []
    for i in range(1, runs + 1):
        print(f"  [Run {i}/{runs}] Đang thực thi...", end=" ", flush=True)
        t = run_single_execution(config["script"], config["args"])
        run_times.append(t)
        print(f"{t}s")

    median_time = round(statistics.median(run_times), 2)
    mean_time = round(statistics.mean(run_times), 2)
    variance_time = round(statistics.variance(run_times), 4)
    stdev_time = round(statistics.stdev(run_times), 2)
    cv_percent = round((stdev_time / mean_time) * 100, 2) if mean_time > 0 else 0.0

    print(f"  => KẾT QUẢ {exp_id}: Median = {median_time}s | Mean = {mean_time}s | "
          f"Variance = {variance_time}s² | StdDev = {stdev_time}s (CV: {cv_percent}%)")

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "experiment_id": exp_id,
        "description": config["desc"],
        "runs": runs,
        "run_times_sec": str(run_times),
        "median_sec": median_time,
        "mean_sec": mean_time,
        "variance_sec": variance_time,
        "stdev_sec": stdev_time,
        "cv_pct": cv_percent
    }

def prepare_output_csv(output_path):
    """Kiểm tra schema file CSV hiện tại; nếu lệch header cũ thì backup và tạo file mới."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if not os.path.isfile(output_path):
        return False  # File chưa tồn tại -> ghi mới kèm header

    with open(output_path, mode="r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        existing_header = next(reader, None)

    if existing_header != FIELDNAMES:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(output_path)
        backup_path = f"{base}_legacy_{ts}{ext}"
        shutil.move(output_path, backup_path)
        print(f"[INFO] Phát hiện schema benchmark.csv cũ không khớp ({existing_header}).")
        print(f"[INFO] Đã sao lưu file cũ sang: {backup_path} và tạo file mới với schema chuẩn.")
        return False

    return True  # File tồn tại và khớp schema -> append

def main():
    parser = argparse.ArgumentParser(description="Benchmark Harness cho CitiBike StationPulse")
    parser.add_argument("--mode", choices=["baseline", "optimized", "all"], default="baseline",
                        help="Chọn chế độ benchmark (baseline | optimized | all)")
    parser.add_argument("--runs", type=int, default=3,
                        help="Số lần chạy lặp lại (BẮT BUỘC >= 3 theo quy tắc thực nghiệm)")
    parser.add_argument("--warmup", action="store_true", help="Chạy 1 lần warm-up trước khi đo thật")
    parser.add_argument("--output", default="reports/benchmark.csv", help="Đường dẫn lưu file CSV kết quả")
    args = parser.parse_args()

    # Ràng buộc cứng --runs >= 3
    if args.runs < 3:
        parser.error(f"--runs phải >= 3 để tính thống kê (Median, Variance) có ý nghĩa (nhận được: {args.runs}).")

    if args.mode == "baseline":
        selected_exps = ["E00_baseline"]
    elif args.mode == "optimized":
        selected_exps = ["E03_optimized"]
    else:
        selected_exps = list(EXPERIMENTS.keys())

    results = []
    for exp_id in selected_exps:
        res = benchmark_experiment(exp_id, EXPERIMENTS[exp_id], args.runs, args.warmup)
        if res:
            results.append(res)

    if results:
        can_append = prepare_output_csv(args.output)
        with open(args.output, mode="a" if can_append else "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not can_append:
                writer.writeheader()
            writer.writerows(results)
        print(f"\n[OK] Đã lưu kết quả benchmark vào {args.output}")

if __name__ == "__main__":
    main()