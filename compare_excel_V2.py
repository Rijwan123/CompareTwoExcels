"""
Excel Comparison Utility (Data + Structure Validation)

- Compares two large Excel files (50k+ rows) efficiently
- Normalizes text, numbers, and dates to avoid false mismatches
- Supports row alignment using a key column (recommended) or row order
- Uses chunking + multithreading for faster processing
- Detects cell-level data mismatches and missing rows
- Validates structure changes: column add/remove/reorder (FAILS structure)
- Generates detailed CSV/JSON reports with PASS/FAIL status
"""

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
# Normalization helpers
# -----------------------------
_WS_RE = re.compile(r"\s+")

def normalize_cell(x):
    """
    Normalize values to reduce false mismatches:
    - trims strings, collapses whitespace
    - normalizes dates/timestamps to ISO date/time
    - converts NaN/None/empty to None
    """
    if x is None:
        return None

    # pandas missing values
    if isinstance(x, float) and np.isnan(x):
        return None
    if pd.isna(x):
        return None

    # handle pandas timestamps / python datetime / date
    if isinstance(x, (pd.Timestamp, datetime)):
        if x.time() == datetime.min.time():
            return x.date().isoformat()
        return x.isoformat(sep=" ")
    if isinstance(x, date):
        return x.isoformat()

    # numeric types
    if isinstance(x, (int, np.integer)):
        return int(x)
    if isinstance(x, (float, np.floating)):
        v = float(x)
        if v == -0.0:
            v = 0.0
        return v

    # string normalize
    s = str(x).strip()
    if s == "":
        return None
    s = _WS_RE.sub(" ", s)
    return s


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas applymap is deprecated in newer versions; use DataFrame.map if available.
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if hasattr(df, "map"):   # pandas >= 2.1
        return df.map(normalize_cell)
    return df.applymap(normalize_cell)  # fallback


# -----------------------------
# Structure validation (S5 support)
# -----------------------------
def check_structure(df1: pd.DataFrame, df2: pd.DataFrame):
    cols1 = list(df1.columns)
    cols2 = list(df2.columns)

    set1 = set(cols1)
    set2 = set(cols2)

    missing_columns = sorted(list(set1 - set2))
    extra_columns = sorted(list(set2 - set1))

    # order check only for columns present in both
    common_cols_in_ref_order = [c for c in cols1 if c in set2]
    reordered_columns = []
    for c in common_cols_in_ref_order:
        if cols1.index(c) != cols2.index(c):
            reordered_columns.append({
                "column": c,
                "reference_position": cols1.index(c),
                "target_position": cols2.index(c)
            })

    structure_fail = bool(missing_columns or extra_columns or reordered_columns)

    return {
        "structure_fail": structure_fail,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "reordered_columns": reordered_columns
    }


# -----------------------------
# Core compare logic (chunked + threaded)
# -----------------------------
def compare_chunk(df1_chunk, df2_chunk, key_col, cols_to_compare, float_tol):
    mismatches = []

    for col in cols_to_compare:
        a = df1_chunk[col].to_numpy()
        b = df2_chunk[col].to_numpy()

        for i in range(len(a)):
            v1, v2 = a[i], b[i]

            if v1 is None and v2 is None:
                continue

            # float tolerance compare
            if isinstance(v1, float) and isinstance(v2, float):
                if math.isfinite(v1) and math.isfinite(v2):
                    if abs(v1 - v2) <= float_tol:
                        continue

            if v1 != v2:
                k = df1_chunk.index[i] if key_col else df1_chunk.index[i]
                mismatches.append({
                    "key" if key_col else "row_index": k,
                    "column": col,
                    "value_file1": v1,
                    "value_file2": v2
                })

    return mismatches


def compare_dataframes(df1, df2, key_col=None, float_tol=0.0, threads=8, chunk_size=5000):
    result = {
        "missing_in_file2": [],
        "missing_in_file1": [],
        "mismatches": [],
        "stats": {}
    }

    # Compare common columns only for data mismatch
    common_cols = [c for c in df1.columns if c in df2.columns]
    if not common_cols:
        raise ValueError("No common columns found between the two files/sheets.")

    # Align by key if provided
    if key_col:
        if key_col not in df1.columns or key_col not in df2.columns:
            raise ValueError(f"Key column '{key_col}' not found in both files.")

        df1 = df1.set_index(key_col, drop=False)
        df2 = df2.set_index(key_col, drop=False)

        keys1 = set(df1.index)
        keys2 = set(df2.index)

        result["missing_in_file2"] = sorted(list(keys1 - keys2))
        result["missing_in_file1"] = sorted(list(keys2 - keys1))

        shared_keys = sorted(list(keys1 & keys2))
        df1c = df1.loc[shared_keys, common_cols]
        df2c = df2.loc[shared_keys, common_cols]
    else:
        # Row order compare
        min_len = min(len(df1), len(df2))
        if len(df1) != len(df2):
            if len(df1) > len(df2):
                result["missing_in_file2"] = list(range(min_len, len(df1)))
            else:
                result["missing_in_file1"] = list(range(min_len, len(df2)))

        df1c = df1.iloc[:min_len][common_cols].copy()
        df2c = df2.iloc[:min_len][common_cols].copy()

    total = len(df1c)
    ranges = [(i, min(i + chunk_size, total)) for i in range(0, total, chunk_size)]

    start = time.time()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [
            ex.submit(compare_chunk, df1c.iloc[s:e], df2c.iloc[s:e], key_col, common_cols, float_tol)
            for (s, e) in ranges
        ]
        for f in as_completed(futures):
            result["mismatches"].extend(f.result())

    elapsed = time.time() - start
    result["stats"] = {
        "rows_compared": total,
        "common_columns": len(common_cols),
        "mismatch_cells": len(result["mismatches"]),
        "missing_rows_file2": len(result["missing_in_file2"]),
        "missing_rows_file1": len(result["missing_in_file1"]),
        "threads": threads,
        "chunk_size": chunk_size,
        "compare_seconds": round(elapsed, 3)
    }
    return result


# -----------------------------
# Excel reading
# -----------------------------
def read_excel_fast(path, sheet_name=None):
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


# -----------------------------
# Output writer
# -----------------------------
def write_outputs(res, outdir, sheet):
    os.makedirs(outdir, exist_ok=True)

    # Timestamped mismatch file to avoid Windows lock issues
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mism_path = os.path.join(outdir, f"mismatches_{ts}.csv")

    mism_df = pd.DataFrame(res["mismatches"])
    mism_df.to_csv(mism_path, index=False, encoding="utf-8-sig")

    pd.DataFrame({"missing_in_file2": res["missing_in_file2"]}).to_csv(
        os.path.join(outdir, "missing_in_file2.csv"), index=False, encoding="utf-8-sig"
    )
    pd.DataFrame({"missing_in_file1": res["missing_in_file1"]}).to_csv(
        os.path.join(outdir, "missing_in_file1.csv"), index=False, encoding="utf-8-sig"
    )

    # structure validation
    if "structure" in res:
        with open(os.path.join(outdir, "structure_validation.json"), "w", encoding="utf-8") as f:
            json.dump(res["structure"], f, indent=2)

    # Overall status
    overall_status = "FAIL" if (
        res.get("structure", {}).get("structure_fail", False) or res["stats"]["mismatch_cells"] > 0
    ) else "PASS"

    summary = {
        "sheet": sheet,
        "overall_status": overall_status,
        "stats": res["stats"],
        "structure": res.get("structure", None)
    }
    with open(os.path.join(outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"[{sheet}] Status: {overall_status}")
    print(f" - Mismatch cells: {res['stats']['mismatch_cells']}")
    if res.get("structure", {}).get("structure_fail"):
        print(" - STRUCTURE FAIL")
        if res["structure"]["missing_columns"]:
            print("   Missing columns:", res["structure"]["missing_columns"])
        if res["structure"]["extra_columns"]:
            print("   Extra columns:", res["structure"]["extra_columns"])
        if res["structure"]["reordered_columns"]:
            print("   Reordered columns:", len(res["structure"]["reordered_columns"]))
    print(f" - Output dir: {outdir}")


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Fast Excel vs Excel comparator (threaded, structure-aware).")
    ap.add_argument("--file1", required=True, help="Path to reference Excel")
    ap.add_argument("--file2", required=True, help="Path to target Excel")
    ap.add_argument("--sheet", default=None, help="Sheet name to compare (optional). If omitted: compares common sheets by name.")
    ap.add_argument("--key", default=None, help="Key column name for row alignment (recommended), e.g. ID / EmpID")
    ap.add_argument("--threads", type=int, default=max(4, (os.cpu_count() or 8) // 2), help="Thread count")
    ap.add_argument("--chunk", type=int, default=5000, help="Chunk size per thread job")
    ap.add_argument("--float_tol", type=float, default=0.0, help="Tolerance for float compare, e.g. 0.01")
    ap.add_argument("--outdir", default="excel_compare_output", help="Output directory")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    if args.sheet:
        df1 = read_excel_fast(args.file1, sheet_name=args.sheet)
        df2 = read_excel_fast(args.file2, sheet_name=args.sheet)

        structure_result = check_structure(df1, df2)

        df1n = normalize_df(df1)
        df2n = normalize_df(df2)

        res = compare_dataframes(df1n, df2n, key_col=args.key, float_tol=args.float_tol,
                                 threads=args.threads, chunk_size=args.chunk)
        res["structure"] = structure_result

        write_outputs(res, args.outdir, sheet=args.sheet)

    else:
        xl1 = pd.ExcelFile(args.file1, engine="openpyxl")
        xl2 = pd.ExcelFile(args.file2, engine="openpyxl")

        common_sheets = [s for s in xl1.sheet_names if s in xl2.sheet_names]
        if not common_sheets:
            raise ValueError("No common sheets found. Use --sheet to compare specific sheets.")

        overall = {"sheets": {}, "overall_stats": {}}

        for sh in common_sheets:
            df1 = pd.read_excel(xl1, sheet_name=sh)
            df2 = pd.read_excel(xl2, sheet_name=sh)

            structure_result = check_structure(df1, df2)

            df1n = normalize_df(df1)
            df2n = normalize_df(df2)

            res = compare_dataframes(df1n, df2n, key_col=args.key, float_tol=args.float_tol,
                                     threads=args.threads, chunk_size=args.chunk)
            res["structure"] = structure_result

            sheet_dir = os.path.join(args.outdir, f"sheet_{sh}")
            write_outputs(res, sheet_dir, sheet=sh)

            overall["sheets"][sh] = {
                "overall_status": "FAIL" if (structure_result["structure_fail"] or res["stats"]["mismatch_cells"] > 0) else "PASS",
                "stats": res["stats"],
                "structure": structure_result
            }

        overall["overall_stats"] = {
            "common_sheets": len(common_sheets),
            "generated_at": datetime.now().isoformat(sep=" ")
        }

        with open(os.path.join(args.outdir, "overall_summary.json"), "w", encoding="utf-8") as f:
            json.dump(overall, f, indent=2)

    print(f"\nDone. Outputs saved in: {args.outdir}")


if __name__ == "__main__":
    main()
