import argparse
import glob
import os
import sys
import pandas as pd

EXPECTED_CLEAN_ROWS = 49838
EXPECTED_STATIONS = 127

def load_csv_from_dir(path):
    """Đọc file CSV từ thư mục output (hỗ trợ cả file đơn hoặc part-*.csv của Spark)."""
    if os.path.isfile(path):
        return pd.read_csv(path)
    
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Không tìm thấy file CSV nào trong thư mục: {path}")
    
    df_list = [pd.read_csv(f) for f in csv_files]
    return pd.concat(df_list, ignore_index=True)

def calculate_metrics(df, label):
    """Trích xuất các chỉ số bất biến (invariants) để đối chiếu."""
    # Tự động nhận diện tên cột phổ biến trong bài toán StationPulse
    cols = {c.lower(): c for c in df.columns}
    
    dep_col = cols.get("departures") or cols.get("total_departures") or cols.get("departure_count")
    arr_col = cols.get("arrivals") or cols.get("total_arrivals") or cols.get("arrival_count")
    net_col = cols.get("net_flow") or cols.get("netflow")
    
    metrics = {
        "row_count": len(df),
        "departures_sum": int(df[dep_col].sum()) if dep_col else None,
        "arrivals_sum": int(df[arr_col].sum()) if arr_col else None,
        "net_flow_sum": int(df[net_col].sum()) if net_col else None,
    }
    
    print(f"[{label}] Rows: {metrics['row_count']} | "
          f"Departures: {metrics['departures_sum']} | "
          f"Arrivals: {metrics['arrivals_sum']} | "
          f"Net Flow Checksum: {metrics['net_flow_sum']}")
    return metrics

def main():
    parser = argparse.ArgumentParser(description="Correctness Gatekeeper - Kiểm chứng Zero Regression")
    parser.add_argument("--baseline", default="output/preview_csv/", help="Thư mục/file CSV output của Baseline")
    parser.add_argument("--after", default="output/preview_csv/", help="Thư mục/file CSV output sau tối ưu")
    parser.add_argument("--strict-expected", action="store_true", 
                        help=f"Bật kiểm tra cứng số dòng sạch = {EXPECTED_CLEAN_ROWS} hoặc trạm = {EXPECTED_STATIONS}")
    args = parser.parse_args()

    print("=== BẮT ĐẦU KIỂM CHỨNG TÍNH ĐÚNG ĐẮN (CORRECTNESS CHECK) ===")
    try:
        df_base = load_csv_from_dir(args.baseline)
        df_after = load_csv_from_dir(args.after)
    except Exception as e:
        print(f"[FAILED] Lỗi đọc dữ liệu: {e}")
        sys.exit(1)

    m_base = calculate_metrics(df_base, "BASELINE")
    m_after = calculate_metrics(df_after, "OPTIMIZED")

    errors = []

    # 1. So sánh 1-1 giữa Baseline và After
    for key in ["row_count", "departures_sum", "arrivals_sum", "net_flow_sum"]:
        if m_base[key] != m_after[key]:
            errors.append(f"Sai lệch '{key}': Baseline={m_base[key]} vs Optimized={m_after[key]}")

    # 2. Kiểm tra tính bảo toàn dòng chảy (Tổng đi - Tổng đến == Tổng Net Flow)
    if m_after["departures_sum"] is not None and m_after["arrivals_sum"] is not None and m_after["net_flow_sum"] is not None:
        expected_net = m_after["departures_sum"] - m_after["arrivals_sum"]
        if m_after["net_flow_sum"] != expected_net:
            errors.append(f"Lỗi logic Net Flow: Departures({m_after['departures_sum']}) - Arrivals({m_after['arrivals_sum']}) != NetFlow({m_after['net_flow_sum']})")

    # 3. Kiểm tra mốc chuẩn của dự án nếu bật cờ --strict-expected
    if args.strict_expected:
        if m_after["row_count"] not in (EXPECTED_CLEAN_ROWS, EXPECTED_STATIONS):
            errors.append(f"Số dòng ({m_after['row_count']}) không khớp mốc chuẩn ({EXPECTED_CLEAN_ROWS} dòng sạch hoặc {EXPECTED_STATIONS} trạm)")

    print("-" * 55)
    if errors:
        print("KẾT QUẢ:  FAILED ")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()