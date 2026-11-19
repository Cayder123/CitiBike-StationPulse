import numpy as np
import argparse
import glob
import os
import sys
import pandas as pd
from pandas.testing import assert_frame_equal

EXPECTED_CLEAN_ROWS = 49838
EXPECTED_STATIONS = 127

def discover_tables(path):
    """
    Phân tách từng bảng CSV độc lập, KHÔNG gộp chung các loại CSV khác nhau.
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

    # Trường hợp 2: Thư mục chứa nhiều file CSV đơn lẻ khác loại
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

def compare_dataframes_row_by_row(df_base, df_after, table_name):
    """
    So sánh dữ liệu từng dòng, không phụ thuộc thứ tự dòng.
    """
    if list(df_base.columns) != list(df_after.columns):
        return f"[{table_name}] Khác cấu trúc cột."

    if len(df_base) != len(df_after):
        return f"[{table_name}] Khác số dòng."

    cols = list(df_base.columns)

    # Sắp xếp dữ liệu để loại bỏ phụ thuộc thứ tự dòng
    try:
        b_sorted = df_base.sort_values(
            by=cols, kind="mergesort", na_position="first"
        ).reset_index(drop=True)

        a_sorted = df_after.sort_values(
            by=cols, kind="mergesort", na_position="first"
        ).reset_index(drop=True)
    except Exception as e:
        return f"[{table_name}] Không thể sắp xếp dữ liệu: {e}"

    # So sánh từng cột
    row_diff = np.zeros(len(b_sorted), dtype=bool)

    for col in cols:
        b = b_sorted[col]
        a = a_sorted[col]

        if pd.api.types.is_numeric_dtype(b) and pd.api.types.is_numeric_dtype(a):
            b_values = b.to_numpy(dtype=float, na_value=np.nan)
            a_values = a.to_numpy(dtype=float, na_value=np.nan)

            equal = np.isclose(
                b_values, a_values,
                rtol=1e-5,
                atol=1e-6,
                equal_nan=True
            )
        else:
            equal = (
                b.eq(a) | (b.isna() & a.isna())
            ).to_numpy()

        row_diff |= ~equal

    diff_count = int(row_diff.sum())

    if diff_count > 0:
        first_idx = int(np.where(row_diff)[0][0])

        return (
            f"[{table_name}] Sai lệch dữ liệu ở {diff_count} dòng. "
            f"Dòng đầu tiên (index sau sort={first_idx}): "
            f"Baseline={b_sorted.iloc[first_idx].to_dict()} "
            f"vs Optimized={a_sorted.iloc[first_idx].to_dict()}"
        )

    return None
    
def check_strict_expected_for_table(table_name, row_count):
    """
    Chỉ kiểm tra mốc chuẩn cứng (49.838 hoặc 127) cho bảng cleaned trips và stations.
    Bỏ qua các bảng tổng hợp theo thời gian (hourly, monthly, daily...).
    """
    name_lower = table_name.lower()
    is_time_agg = any(k in name_lower for k in ["hour", "month", "day", "daily", "week", "trend"])
    if is_time_agg:
        return None

    if any(k in name_lower for k in ["clean", "trip"]):
        if row_count != EXPECTED_CLEAN_ROWS:
            return f"[{table_name}] Bảng cleaned trips có {row_count} dòng, không khớp mốc chuẩn {EXPECTED_CLEAN_ROWS}."
    elif "station" in name_lower:
        if row_count != EXPECTED_STATIONS:
            return f"[{table_name}] Bảng stations có {row_count} dòng, không khớp mốc chuẩn {EXPECTED_STATIONS}."

    return None

def main():
    parser = argparse.ArgumentParser(description="Correctness Gatekeeper - Kiểm chứng Zero Regression")
    parser.add_argument("--baseline", default="output/baseline/", help="Thư mục/file CSV output của Baseline")
    parser.add_argument("--after", default="output/optimized/", help="Thư mục/file CSV output sau tối ưu")
    parser.add_argument("--net-formula", choices=["arrivals-departures", "departures-arrivals"],
                        default="arrivals-departures", help="Công thức tính net_flow theo đúng logic pipeline")
    parser.add_argument("--strict-expected", action="store_true",
                        help=f"Bật kiểm tra cứng số dòng sạch = {EXPECTED_CLEAN_ROWS} (cleaned trips) và {EXPECTED_STATIONS} (stations)")
    args = parser.parse_args()

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

    if set(base_tables.keys()) != set(after_tables.keys()):
        errors.append(f"Danh sách file/bảng output không khớp: Baseline={list(base_tables.keys())} vs Optimized={list(after_tables.keys())}")

    common_tables = sorted(set(base_tables.keys()) & set(after_tables.keys()))
    for table_name in common_tables:
        print(f"\n>> Đang kiểm tra bảng: {table_name}")
        df_base = base_tables[table_name]
        df_after = after_tables[table_name]

        m_base = calculate_metrics(df_base, "BASELINE")
        m_after = calculate_metrics(df_after, "OPTIMIZED")

        # 1. So sánh schema và các chỉ số tổng hợp
        if m_base["columns"] != m_after["columns"]:
            errors.append(f"[{table_name}] Sai lệch cấu trúc cột: {m_base['columns']} vs {m_after['columns']}")

        for key in ["row_count", "departures_sum", "arrivals_sum", "net_flow_sum"]:
            if m_base[key] != m_after[key]:
                errors.append(f"[{table_name}] Sai lệch '{key}': Baseline={m_base[key]} vs Optimized={m_after[key]}")

        # 2. So sánh dữ liệu từng dòng (không phụ thuộc thứ tự dòng)
        row_diff_err = compare_dataframes_row_by_row(df_base, df_after, table_name)
        if row_diff_err:
            errors.append(row_diff_err)
        else:
            print(f"  [ROW-DIFF] Khớp 100% từng dòng và từng giá trị cột (order-independent).")

        # 3. Kiểm tra công thức Net Flow trên từng dòng
        dep_c, arr_c, net_c = m_after["dep_col"], m_after["arr_col"], m_after["net_col"]
        if dep_c and arr_c and net_c:
            if args.net_formula == "arrivals-departures":
                expected_series = df_after[arr_c] - df_after[dep_c]
            else:
                expected_series = df_after[dep_c] - df_after[arr_c]

            mismatch_rows = (~np.isclose(
                df_after[net_c].to_numpy(dtype=float),
                expected_series.to_numpy(dtype=float),
                rtol=1e-5,
                atol=1e-6,
                equal_nan=True
            )).sum()
            if mismatch_rows > 0:
                errors.append(f"[{table_name}] Có {mismatch_rows} dòng sai công thức Net Flow ({args.net_formula})")

        # 4. Kiểm tra mốc chuẩn dự án (chỉ áp dụng cho cleaned trips và stations)
        if args.strict_expected:
            strict_err = check_strict_expected_for_table(table_name, m_after["row_count"])
            if strict_err:
                errors.append(strict_err)

    print("\n" + "-" * 55)
    if errors:
        print("KẾT QUẢ: FAILED")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("KẾT QUẢ: PASS (Zero Regression - 100% khớp từng dòng trên tất cả các bảng)")
        sys.exit(0)

if __name__ == "__main__":
    main()
