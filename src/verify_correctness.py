
import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal


EXPECTED_CLEAN_ROWS = 49838
EXPECTED_STATIONS = 127


def discover_tables(path):
    """
    Tìm và đọc các bảng CSV độc lập.
    Không gộp các loại bảng khác nhau.
    """
    tables = {}

    if os.path.isfile(path):
        tables[os.path.basename(path)] = pd.read_csv(path)
        return tables

    if not os.path.isdir(path):
        raise FileNotFoundError(f"Không tìm thấy đường dẫn: {path}")

    # Trường hợp 1: Thư mục output trực tiếp của Spark
    part_files = sorted(
        glob.glob(os.path.join(path, "part-*.csv"))
    )

    if part_files:
        table_name = os.path.basename(os.path.normpath(path))

        tables[table_name] = pd.concat(
            [pd.read_csv(f) for f in part_files],
            ignore_index=True
        )

        return tables

    # Trường hợp 2: Thư mục chứa nhiều file CSV độc lập
    standalone_csvs = sorted(
        glob.glob(os.path.join(path, "*.csv"))
    )

    for file_path in standalone_csvs:
        tables[os.path.basename(file_path)] = pd.read_csv(file_path)

    # Trường hợp 3: Thư mục cha chứa nhiều bảng Spark
    for entry in sorted(os.listdir(path)):
        sub_path = os.path.join(path, entry)

        if os.path.isdir(sub_path):
            sub_parts = sorted(
                glob.glob(os.path.join(sub_path, "part-*.csv"))
            )

            if sub_parts:
                tables[entry] = pd.concat(
                    [pd.read_csv(f) for f in sub_parts],
                    ignore_index=True
                )

    if not tables:
        raise FileNotFoundError(
            f"Không tìm thấy dữ liệu CSV hợp lệ trong: {path}"
        )

    return tables


def calculate_metrics(df, label):
    """
    Tính các chỉ số tổng hợp để kiểm tra tính nhất quán dữ liệu.
    """
    cols = {c.lower(): c for c in df.columns}

    dep_col = (
        cols.get("departures")
        or cols.get("total_departures")
        or cols.get("departure_count")
    )

    arr_col = (
        cols.get("arrivals")
        or cols.get("total_arrivals")
        or cols.get("arrival_count")
    )

    net_col = (
        cols.get("net_flow")
        or cols.get("netflow")
    )

    metrics = {
        "row_count": len(df),
        "columns": list(df.columns),
        "departures_sum": df[dep_col].sum() if dep_col else None,
        "arrivals_sum": df[arr_col].sum() if arr_col else None,
        "net_flow_sum": df[net_col].sum() if net_col else None,
        "dep_col": dep_col,
        "arr_col": arr_col,
        "net_col": net_col
    }

    print(
        f"  [{label}] Rows: {metrics['row_count']} | "
        f"Departures: {metrics['departures_sum']} | "
        f"Arrivals: {metrics['arrivals_sum']} | "
        f"Net Flow Checksum: {metrics['net_flow_sum']}"
    )

    return metrics


def compare_dataframes_row_by_row(df_base, df_after, table_name):
    """
    So sánh dữ liệu từng dòng giữa Baseline và Optimized.
    Không phụ thuộc thứ tự dòng.
    Cho phép sai số float nhỏ.
    """

    # Kiểm tra cấu trúc cột
    if list(df_base.columns) != list(df_after.columns):
        return (
            f"[{table_name}] Khác cấu trúc cột: "
            f"{list(df_base.columns)} vs {list(df_after.columns)}"
        )

    # Kiểm tra số dòng
    if len(df_base) != len(df_after):
        return (
            f"[{table_name}] Khác số dòng: "
            f"{len(df_base)} vs {len(df_after)}"
        )

    cols = list(df_base.columns)

    b_norm = df_base.copy()
    a_norm = df_after.copy()

    # Làm tròn float để hạn chế sai số nhỏ
    for col in cols:
        if (
            pd.api.types.is_float_dtype(b_norm[col])
            or pd.api.types.is_float_dtype(a_norm[col])
        ):
            b_norm[col] = b_norm[col].round(6)
            a_norm[col] = a_norm[col].round(6)

    try:
        # Sắp xếp để loại bỏ ảnh hưởng của thứ tự partition Spark
        b_sorted = b_norm.sort_values(
            by=cols,
            kind="mergesort",
            na_position="first"
        ).reset_index(drop=True)

        a_sorted = a_norm.sort_values(
            by=cols,
            kind="mergesort",
            na_position="first"
        ).reset_index(drop=True)

        # So sánh toàn bộ dữ liệu
        assert_frame_equal(
            b_sorted,
            a_sorted,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
            atol=1e-6
        )

        return None

    except (AssertionError, TypeError, ValueError) as e:
        return (
            f"[{table_name}] Dữ liệu khác nhau hoặc không thể "
            f"sắp xếp để so sánh: {str(e)[:300]}"
        )


def check_strict_expected_for_table(table_name, row_count):
    """
    Kiểm tra số dòng chuẩn của cleaned trips và stations.
    Bỏ qua các bảng tổng hợp theo thời gian.
    """

    name_lower = table_name.lower()

    is_time_agg = any(
        keyword in name_lower
        for keyword in [
            "hour", "month", "day",
            "daily", "week", "trend"
        ]
    )

    if is_time_agg:
        return None

    if any(keyword in name_lower for keyword in ["clean", "trip"]):
        if row_count != EXPECTED_CLEAN_ROWS:
            return (
                f"[{table_name}] Số dòng cleaned trips = {row_count}, "
                f"không khớp mốc {EXPECTED_CLEAN_ROWS}."
            )

    elif "station" in name_lower:
        if row_count != EXPECTED_STATIONS:
            return (
                f"[{table_name}] Số dòng stations = {row_count}, "
                f"không khớp mốc {EXPECTED_STATIONS}."
            )

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Correctness Gatekeeper - Kiểm chứng tính đúng đắn"
    )

    parser.add_argument(
        "--baseline",
        default="output/baseline/",
        help="Thư mục hoặc file CSV của Baseline"
    )

    parser.add_argument(
        "--after",
        default="output/optimized/",
        help="Thư mục hoặc file CSV sau tối ưu"
    )

    parser.add_argument(
        "--net-formula",
        choices=[
            "arrivals-departures",
            "departures-arrivals"
        ],
        default="arrivals-departures",
        help="Công thức tính Net Flow"
    )

    parser.add_argument(
        "--strict-expected",
        action="store_true",
        help="Kiểm tra số dòng chuẩn của cleaned trips và stations"
    )

    args = parser.parse_args()

    # Không cho phép so sánh cùng một đường dẫn
    if os.path.abspath(args.baseline) == os.path.abspath(args.after):
        print(
            "[FAILED] Baseline và Optimized đang trỏ tới cùng "
            "một đường dẫn."
        )
        sys.exit(1)

    print("=" * 60)
    print("BẮT ĐẦU KIỂM CHỨNG TÍNH ĐÚNG ĐẮN")
    print("=" * 60)

    # Đọc dữ liệu
    try:
        base_tables = discover_tables(args.baseline)
        after_tables = discover_tables(args.after)

    except Exception as e:
        print(f"[FAILED] Lỗi đọc dữ liệu: {e}")
        sys.exit(1)

    errors = []

    # Kiểm tra danh sách bảng
    if set(base_tables.keys()) != set(after_tables.keys()):
        errors.append(
            "Danh sách bảng không khớp: "
            f"Baseline={list(base_tables.keys())} | "
            f"Optimized={list(after_tables.keys())}"
        )

    common_tables = sorted(
        set(base_tables.keys()) & set(after_tables.keys())
    )

    # Kiểm tra từng bảng
    for table_name in common_tables:

        print(f"\n>> Đang kiểm tra bảng: {table_name}")

        df_base = base_tables[table_name]
        df_after = after_tables[table_name]

        m_base = calculate_metrics(df_base, "BASELINE")
        m_after = calculate_metrics(df_after, "OPTIMIZED")

        # 1. Kiểm tra schema
        if m_base["columns"] != m_after["columns"]:
            errors.append(
                f"[{table_name}] Sai lệch cấu trúc cột."
            )

        # 2. Kiểm tra các chỉ số tổng hợp
        for key in [
            "row_count",
            "departures_sum",
            "arrivals_sum",
            "net_flow_sum"
        ]:
            if m_base[key] != m_after[key]:
                errors.append(
                    f"[{table_name}] Sai lệch {key}: "
                    f"Baseline={m_base[key]} | "
                    f"Optimized={m_after[key]}"
                )

        # 3. So sánh từng dòng
        row_diff_err = compare_dataframes_row_by_row(
            df_base,
            df_after,
            table_name
        )

        if row_diff_err:
            errors.append(row_diff_err)
            print("  [ROW-DIFF] FAILED")
        else:
            print(
                "  [ROW-DIFF] PASS - Dữ liệu khớp "
                "sau khi chuẩn hóa sai số float."
            )

        # 4. Kiểm tra công thức Net Flow
        dep_c = m_after["dep_col"]
        arr_c = m_after["arr_col"]
        net_c = m_after["net_col"]

        if dep_c and arr_c and net_c:

            if args.net_formula == "arrivals-departures":
                expected_series = (
                    df_after[arr_c] - df_after[dep_c]
                )
            else:
                expected_series = (
                    df_after[dep_c] - df_after[arr_c]
                )

            try:
                mismatch_rows = (
                    ~np.isclose(
                        df_after[net_c].astype(float),
                        expected_series.astype(float),
                        rtol=1e-5,
                        atol=1e-6,
                        equal_nan=True
                    )
                ).sum()

                if mismatch_rows > 0:
                    errors.append(
                        f"[{table_name}] Có {mismatch_rows} dòng "
                        f"sai công thức Net Flow."
                    )

            except (TypeError, ValueError) as e:
                errors.append(
                    f"[{table_name}] Không thể kiểm tra Net Flow: {e}"
                )

        # 5. Kiểm tra mốc chuẩn dự án
        if args.strict_expected:
            strict_err = check_strict_expected_for_table(
                table_name,
                m_after["row_count"]
            )

            if strict_err:
                errors.append(strict_err)

    # Tổng kết
    print("\n" + "=" * 60)

    if errors:
        print("KẾT QUẢ: FAILED")

        for error in errors:
            print(f"  - {error}")

        print(f"\nTổng số lỗi: {len(errors)}")
        sys.exit(1)

    else:
        print(
            "KẾT QUẢ: PASS "
            "(Các bảng đã kiểm tra không có sai lệch đáng kể)"
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
