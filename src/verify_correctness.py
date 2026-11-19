import argparse
import glob
import os
import sys
import pandas as pd

EXPECTED_CLEAN_ROWS = 49838
EXPECTED_STATIONS = 127

def discover_tables(path):
    """
    Phân tách từng bảng CSV độc lập, KHÔNG gộp chung các loại CSV khác nhau.
    - Nếu path là 1 file .csv -> trả về {tên_file: df}
    - Nếu path là thư mục chứa part-*.csv của 1 bảng Spark -> gộp các part của riêng bảng đó
    - Nếu path là thư mục chứa nhiều file/thư mục bảng khác nhau -> tách riêng từng bảng theo tên
    """
    tables = {}
    if os.path.isfile(path):
        tables[os.path.basename(path)] = pd.read_csv(path)
        return tables

    if not os.path.isdir(path):
        raise FileNotFoundError(f"Không tìm thấy đường dẫn: {path}")

    # Trường hợp 1: Thư mục output trực tiếp của 1 bảng Spark (chứa part-*.csv)
    part_files = sorted(glob.glob(os.path.join(path, "part-*.csv")))
    if part_files:
        table_name = os.path.basename(os.path.normpath(path))
        tables[table_name] = pd.concat([pd.read_csv(f) for f in part_files], ignore_index=True)
        return tables

    # Trường hợp 2: Thư mục chứa nhiều file CSV đơn lẻ khác loại (vd: stations.csv, hourly.csv)
    standalone_csvs = sorted(glob.glob(os.path.join(path, "*.csv")))
    for f in standalone_csvs:
        tables[os.path.basename(f)] = pd.read_csv(f)

    # Trường hợp 3: Thư mục cha chứa nhiều thư mục con của Spark (mỗi thư mục con là 1 bảng)
    for entry in sorted(os.listdir(path)):
        sub_path = os.path.join(path, entry)
        if os.path.isdir(sub_path):
            sub_parts = sorted(glob.glob(os.path.join(sub_path, "part-*.csv")))
            if sub_parts:
                tables[entry] = pd.concat([pd.read_csv(f) for f in sub_parts], ignore_index=True)

    if not tables:
        raise FileNotFoundError(f"Không tìm thấy dữ liệu CSV hợp lệ trong: {path}")
    return tables

def calculate_metrics(df, label):
    """Trích xuất các chỉ số bất biến (invariants) cho từng bảng."""
    cols = {c.lower(): c for c in df.columns}
    
    dep_col = cols.get("departures") or cols.get("total_departures") or cols.get("departure_count")
    arr_col = cols.get("arrivals") or cols.get("total_arrivals") or cols.get("arrival_count")
    net_col = cols.get("net_flow") or cols.get("netflow")
    
    metrics = {
        "row_count": len(df),
        "columns": list(df.columns),
        "departures_sum": int(df[dep_col].sum()) if dep_col else None,
        "arrivals_sum": int(df[arr_col].sum()) if arr_col else None,
        "net_flow_sum": int(df[net_col].sum()) if net_col else None,
        "dep_col": dep_col,
        "arr_col": arr_col,
        "net_col": net_col
    }
    
    print(f"  [{label}] Rows: {metrics['row_count']} | "
          f"Departures: {metrics['departures_sum']} | "
          f"Arrivals: {metrics['arrivals_sum']} | "
          f"Net Flow Checksum: {metrics['net_flow_sum']}")
    return metrics

def main():
    parser = argparse.ArgumentParser(description="Correctness Gatekeeper - Kiểm chứng Zero Regression")
    parser.add_argument("--baseline", default="output/baseline/", help="Thư mục/file CSV output của Baseline")
    parser.add_argument("--after", default="output/optimized/", help="Thư mục/file CSV output sau tối ưu")
    parser.add_argument("--net-formula", choices=["arrivals-departures", "departures-arrivals"],
                        default="arrivals-departures", help="Công thức tính net_flow theo đúng logic pipeline")
    parser.add_argument("--strict-expected", action="store_true", 
                        help=f"Bật kiểm tra cứng số dòng sạch = {EXPECTED_CLEAN_ROWS} hoặc trạm = {EXPECTED_STATIONS}")
    args = parser.parse_args()

    # Chặn lỗi so sánh cùng 1 thư mục/file
    if os.path.abspath(args.baseline) == os.path.abspath(args.after):
        print("[FAILED] --baseline và --after đang trỏ tới cùng một đường dẫn! Phải so sánh 2 bộ output riêng biệt.")
        sys.exit(1)

    print("=== BẮT ĐẦU KIỂM CHỨNG TÍNH ĐÚNG ĐẮN (CORRECTNESS CHECK) ===")
    try:
        base_tables = discover_tables(args.baseline)
        after_tables = discover_tables(args.after)
    except Exception as e:
        print(f"[FAILED] Lỗi đọc dữ liệu: {e}")
        sys.exit(1)

    errors = []

    # Kiểm tra danh sách bảng giữa 2 bộ output
    if set(base_tables.keys()) != set(after_tables.keys()):
        errors.append(f"Danh sách file/bảng output không khớp: Baseline={list(base_tables.keys())} vs Optimized={list(after_tables.keys())}")

    common_tables = sorted(set(base_tables.keys()) & set(after_tables.keys()))
    for table_name in common_tables:
        print(f"\n>> Đang kiểm tra bảng: {table_name}")
        df_base = base_tables[table_name]
        df_after = after_tables[table_name]

        m_base = calculate_metrics(df_base, "BASELINE")
        m_after = calculate_metrics(df_after, "OPTIMIZED")

        # 1. So sánh schema và chỉ số 1-1 giữa Baseline và After
        if m_base["columns"] != m_after["columns"]:
            errors.append(f"[{table_name}] Sai lệch cấu trúc cột: {m_base['columns']} vs {m_after['columns']}")

        for key in ["row_count", "departures_sum", "arrivals_sum", "net_flow_sum"]:
            if m_base[key] != m_after[key]:
                errors.append(f"[{table_name}] Sai lệch '{key}': Baseline={m_base[key]} vs Optimized={m_after[key]}")

        # 2. Kiểm tra công thức Net Flow trên từng dòng theo đúng logic pipeline
        dep_c, arr_c, net_c = m_after["dep_col"], m_after["arr_col"], m_after["net_col"]
        if dep_c and arr_c and net_c:
            if args.net_formula == "arrivals-departures":
                expected_series = df_after[arr_c] - df_after[dep_c]
            else:
                expected_series = df_after[dep_c] - df_after[arr_c]

            mismatch_rows = (df_after[net_c] != expected_series).sum()
            if mismatch_rows > 0:
                errors.append(f"[{table_name}] Có {mismatch_rows} dòng sai công thức Net Flow ({args.net_formula})")

        # 3. Kiểm tra mốc chuẩn của dự án nếu bật cờ --strict-expected
        if args.strict_expected:
            if m_after["row_count"] not in (EXPECTED_CLEAN_ROWS, EXPECTED_STATIONS):
                errors.append(f"[{table_name}] Số dòng ({m_after['row_count']}) không khớp mốc chuẩn ({EXPECTED_CLEAN_ROWS} hoặc {EXPECTED_STATIONS})")

    print("\n" + "-" * 55)
    if errors:
        print("KẾT QUẢ: FAILED")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("KẾT QUẢ: PASS (Zero Regression - 100% khớp trên tất cả các bảng)")
        sys.exit(0)

if __name__ == "__main__":
    main()