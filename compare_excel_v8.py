# =============================================================================
# Excel Compare Tool – Enterprise Version
# =============================================================================
#
# PURPOSE:
# --------
# Compares two Excel files efficiently using chunking + multithreading.
# Designed for automation environments like Tosca / CI pipelines.
#
# KEY FEATURES:
# -------------
# • High-performance comparison (chunked + threaded processing)
# • Cell normalization to reduce false mismatches:
#     - Trims whitespace
#     - Collapses multiple spaces
#     - Standardizes dates/timestamps
#     - Handles NaN / None consistently
# • Supports optional row alignment using a key column (--key)
# • Detects and reports:
#     - Cell mismatches
#     - Missing rows in file1/file2
#     - Extra columns in file1/file2
# • Extra columns are treated as FAIL conditions
# Issue fixed: excel_compare_output folder not corrctly giving all files

import os
import re
import json
import math
import time
import argparse
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd


# -----------------------------
# Normalization
# -----------------------------
_WS_RE = re.compile(r"\s+")

def normalize_cell(x):
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp, datetime)):
        if x.time() == datetime.min.time():
            return x.date().isoformat()
        return x.isoformat(sep=" ")
    if isinstance(x, date):
        return x.isoformat()
    if isinstance(x, (int, np.integer)):
        return int(x)
    if isinstance(x, (float, np.floating)):
        v = float(x)
        if v == -0.0:
            v = 0.0
        return v
    s = str(x).strip()
    if s == "":
        return None
    return _WS_RE.sub(" ", s)


def normalize_df(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df.apply(lambda col: col.map(normalize_cell))


# -----------------------------
# Compare Logic
# -----------------------------
def compare_chunk(df1_chunk, df2_chunk, key_col, cols, float_tol):
    mismatches = []

    for col in cols:
        a = df1_chunk[col].to_numpy()
        b = df2_chunk[col].to_numpy()

        for i in range(len(a)):
            v1, v2 = a[i], b[i]

            if v1 is None and v2 is None:
                continue

            if isinstance(v1, float) and isinstance(v2, float):
                if math.isfinite(v1) and math.isfinite(v2):
                    if abs(v1 - v2) <= float_tol:
                        continue

            if v1 != v2:
                mismatches.append({
                    "key" if key_col else "row_index": df1_chunk.index[i],
                    "column": col,
                    "value_file1": v1,
                    "value_file2": v2,
                })

    return mismatches


def compare_dataframes(df1, df2, key_col=None,
                       float_tol=0.0, threads=4, chunk_size=5000):

    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    extra1 = sorted(list(cols1 - cols2))
    extra2 = sorted(list(cols2 - cols1))
    common_cols = [c for c in df1.columns if c in df2.columns]

    if not common_cols:
        raise ValueError("No common columns found between files.")

    result = {
        "missing_in_file2": [],
        "missing_in_file1": [],
        "extra_columns_file1": extra1,
        "extra_columns_file2": extra2,
        "mismatches": [],
        "stats": {}
    }

    if key_col:
        df1 = df1.set_index(key_col, drop=False)
        df2 = df2.set_index(key_col, drop=False)

        keys1 = set(df1.index)
        keys2 = set(df2.index)

        result["missing_in_file2"] = sorted(list(keys1 - keys2))
        result["missing_in_file1"] = sorted(list(keys2 - keys1))

        shared = sorted(list(keys1 & keys2))
        df1c = df1.loc[shared, common_cols]
        df2c = df2.loc[shared, common_cols]
    else:
        min_len = min(len(df1), len(df2))

        if len(df1) > len(df2):
            result["missing_in_file2"] = list(range(min_len, len(df1)))
        elif len(df2) > len(df1):
            result["missing_in_file1"] = list(range(min_len, len(df2)))

        df1c = df1.iloc[:min_len][common_cols]
        df2c = df2.iloc[:min_len][common_cols]

    total = len(df1c)
    ranges = [(i, min(i + chunk_size, total))
              for i in range(0, total, chunk_size)]

    start = time.time()

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [
            ex.submit(compare_chunk,
                      df1c.iloc[s:e],
                      df2c.iloc[s:e],
                      key_col,
                      common_cols,
                      float_tol)
            for s, e in ranges
        ]
        for f in as_completed(futures):
            result["mismatches"].extend(f.result())

    elapsed = time.time() - start

    result["stats"] = {
        "rows_compared": total,
        "common_columns": len(common_cols),
        "extra_columns_file1": len(extra1),
        "extra_columns_file2": len(extra2),
        "mismatch_cells": len(result["mismatches"]),
        "missing_rows_file2": len(result["missing_in_file2"]),
        "missing_rows_file1": len(result["missing_in_file1"]),
        "threads": threads,
        "chunk_size": chunk_size,
        "compare_seconds": round(elapsed, 3)
    }

    return result


# -----------------------------
# Excel Reader
# -----------------------------
def read_single_sheet(path, sheet_name=None):
    if sheet_name:
        return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    xl = pd.ExcelFile(path, engine="openpyxl")
    return pd.read_excel(xl, sheet_name=xl.sheet_names[0])


# -----------------------------
# Output Writer
# -----------------------------
def write_outputs(res, outdir, sheet_name):
    os.makedirs(outdir, exist_ok=True)

    pd.DataFrame(res["mismatches"]).to_csv(
        os.path.join(outdir, "mismatches.csv"),
        index=False, encoding="utf-8-sig"
    )

    pd.DataFrame({"missing_in_file2": res["missing_in_file2"]}).to_csv(
        os.path.join(outdir, "missing_in_file2.csv"),
        index=False, encoding="utf-8-sig"
    )

    pd.DataFrame({"missing_in_file1": res["missing_in_file1"]}).to_csv(
        os.path.join(outdir, "missing_in_file1.csv"),
        index=False, encoding="utf-8-sig"
    )

    pd.DataFrame({"extra_columns_file1": res["extra_columns_file1"]}).to_csv(
        os.path.join(outdir, "extra_columns_file1.csv"),
        index=False, encoding="utf-8-sig"
    )

    pd.DataFrame({"extra_columns_file2": res["extra_columns_file2"]}).to_csv(
        os.path.join(outdir, "extra_columns_file2.csv"),
        index=False, encoding="utf-8-sig"
    )

    summary = {
        "sheet": sheet_name if sheet_name else "Auto_Detected_First_Sheet",
        "stats": res["stats"],
        "extra_columns_file1": res["extra_columns_file1"],
        "extra_columns_file2": res["extra_columns_file2"],
        "missing_in_file2": res["missing_in_file2"],
        "missing_in_file1": res["missing_in_file1"]
    }

    summary_path = os.path.join(outdir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary_path, summary


# -----------------------------
# Tosca Output
# -----------------------------
def print_summary(summary, summary_path):
    stats = summary["stats"]

    mismatches = stats["mismatch_cells"]
    miss1 = stats["missing_rows_file1"]
    miss2 = stats["missing_rows_file2"]
    extra1 = stats["extra_columns_file1"]
    extra2 = stats["extra_columns_file2"]

    fail_reasons = []

    if mismatches: fail_reasons.append(f"mismatch_cells={mismatches}")
    if miss1: fail_reasons.append(f"missing_rows_file1={miss1}")
    if miss2: fail_reasons.append(f"missing_rows_file2={miss2}")
    if extra1: fail_reasons.append(f"extra_columns_file1={extra1}")
    if extra2: fail_reasons.append(f"extra_columns_file2={extra2}")

    result = "PASS" if not fail_reasons else "FAIL"

    print(f"SUMMARY_FILE={os.path.abspath(summary_path)}")
    print(f"RESULT={result}")
    print(f"MISMATCH_CELLS={mismatches}")
    print(f"MISSING_ROWS_FILE1={miss1}")
    print(f"MISSING_ROWS_FILE2={miss2}")
    print(f"EXTRA_COLUMNS_FILE1={extra1}")
    print(f"EXTRA_COLUMNS_FILE2={extra2}")

    if result == "FAIL":
        print("FAIL_MESSAGE=" + ", ".join(fail_reasons))

    print("----- SUMMARY_JSON_START -----")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print("----- SUMMARY_JSON_END -----")

    return 0 if result == "PASS" else 1


# -----------------------------
# MAIN
# -----------------------------
def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--input_dir")
    ap.add_argument("--file1", required=True)
    ap.add_argument("--file2", required=True)
    ap.add_argument("--sheet")
    ap.add_argument("--key")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=5000)
    ap.add_argument("--float_tol", type=float, default=0.0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = os.path.join("excel_compare_output", f"run_{timestamp}")
    ap.add_argument("--outdir", default=outdir)

    args = ap.parse_args()

    if args.input_dir:
        file1_path = os.path.join(args.input_dir, args.file1)
        file2_path = os.path.join(args.input_dir, args.file2)
    else:
        file1_path = args.file1
        file2_path = args.file2

    os.makedirs(args.outdir, exist_ok=True)

    print(f"RUN_OUTPUT_DIR={os.path.abspath(args.outdir)}")
    print(f"FILE1={os.path.abspath(file1_path)}")
    print(f"FILE2={os.path.abspath(file2_path)}")

    try:
        df1 = read_single_sheet(file1_path, args.sheet)
        df2 = read_single_sheet(file2_path, args.sheet)

        df1n = normalize_df(df1)
        df2n = normalize_df(df2)

        res = compare_dataframes(
            df1n, df2n,
            key_col=args.key,
            float_tol=args.float_tol,
            threads=args.threads,
            chunk_size=args.chunk
        )

        summary_path, summary = write_outputs(res, args.outdir, args.sheet)
        return print_summary(summary, summary_path)

    except Exception as e:
        print("ERROR=YES")
        print(f"ERROR_MESSAGE={e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

#Examples
# python compare_excel_v7.py --input_dir excelFiles --file1 S5_ColChange_Reference.xlsx --file2 S5_ColChange_Target.xlsx
# python compare_excel_v7.py --input_dir excelFiles --file1 S3_PartialMismatch_Reference.xlsx --file2 S3_PartialMismatch_Target.xlsx

# =============================================================================
# Excel Compare Tool – Enterprise Version
# =============================================================================
#
# PURPOSE:
# --------
# Compares two Excel files efficiently using chunking + multithreading.
# Designed for automation environments like Tosca / CI pipelines.
#
# KEY FEATURES:
# -------------
# • High-performance comparison (chunked + threaded processing)
# • Cell normalization to reduce false mismatches:
#     - Trims whitespace
#     - Collapses multiple spaces
#     - Standardizes dates/timestamps
#     - Handles NaN / None consistently
# • Supports optional row alignment using a key column (--key)
# • Detects and reports:
#     - Cell mismatches
#     - Missing rows in file1/file2
#     - Extra columns in file1/file2
# • Extra columns are treated as FAIL conditions
#
# INPUT OPTIONS:
# --------------
# • --input_dir   : Folder containing Excel files
# • --file1       : Reference file name (or full path)
# • --file2       : Target file name (or full path)
# • --sheet       : Optional sheet name (auto-detects first sheet if not provided)
# • --key         : Optional key column for row alignment
#
# OUTPUT:
# -------
# Creates timestamp-based output folder:
#
#   excel_compare_output/
#       run_YYYYMMDD_HHMMSS/
#
# Generates:
#   - mismatches.csv
#   - missing_in_file1.csv
#   - missing_in_file2.csv
#   - extra_columns_file1.csv
#   - extra_columns_file2.csv
#   - summary.json
#
# TOSCA OUTPUT FORMAT:
# --------------------
# Prints clean structured output:
#   SUMMARY_FILE=...
#   RESULT=PASS/FAIL
#   MISMATCH_CELLS=...
#   MISSING_ROWS_FILE1=...
#   MISSING_ROWS_FILE2=...
#   EXTRA_COLUMNS_FILE1=...
#   EXTRA_COLUMNS_FILE2=...
#   FAIL_MESSAGE=... (if applicable)
#   ----- SUMMARY_JSON_START -----
#   { full summary JSON }
#   ----- SUMMARY_JSON_END -----
#
# EXIT CODES:
# -----------
#   0 → PASS
#   1 → FAIL (logical mismatch)
#   2 → ERROR (script/config/runtime issue)
#
# =============================================================================
