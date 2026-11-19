import argparse
import csv
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime

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

def run_single_execution(script_path, extra_args):
    """Chạy 1 lần pipeline và trả về thời gian thực thi (giây)."""
    cmd = [sys.executable, script_path] + extra_args
    start_time = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
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
    variance_time = round(statistics.variance(run_times), 4) if len(run_times) > 1 else 0.0
    stdev_time = round(statistics.stdev(run_times), 2) if len(run_times) > 1 else 0.0
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

def main():
    parser = argparse.ArgumentParser(description="Benchmark Harness cho CitiBike StationPulse")
    parser.add_argument("--mode", choices=["baseline", "optimized", "all"], default="baseline",
                        help="Chọn chế độ benchmark (baseline | optimized | all)")
    parser.add_argument("--runs", type=int, default=3, help="Số lần chạy lặp lại (tối thiểu 3 theo Slide 8)")
    parser.add_argument("--warmup", action="store_true", help="Chạy 1 lần warm-up trước khi đo thật")
    parser.add_argument("--output", default="reports/benchmark.csv", help="Đường dẫn lưu file CSV kết quả")
    args = parser.parse_args()

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
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        fieldnames = ["timestamp", "experiment_id", "description", "runs", "run_times_sec", 
                      "median_sec", "mean_sec", "variance_sec", "stdev_sec", "cv_pct"]
        
        file_exists = os.path.isfile(args.output)
        with open(args.output, mode="a" if file_exists else "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(results)
        print(f"\n[OK] Đã lưu kết quả benchmark vào {args.output}")

if __name__ == "__main__":
    main()