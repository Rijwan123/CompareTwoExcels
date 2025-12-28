# Not supported -  row alignment using a key column (recommended) or row order

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
        # keep full timestamp if time exists; else keep date
        if x.time() == datetime.min.time():
            return x.date().isoformat()
        return x.isoformat(sep=" ")
    if isinstance(x, date):
        return x.isoformat()

    # numeric types: keep as is (but normalize -0.0)
    if isinstance(x, (int, np.integer)):
        return int(x)
    if isinstance(x, (float, np.floating)):
        v = float(x)
        if v == -0.0:
            v = 0.0
        return v

    # everything else -> string normalize
    s = str(x)
    s = s.strip()
    if s == "":
        return None
    s = _WS_RE.sub(" ", s)  # collapse whitespace
    return s


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure consistent column names
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # Apply normalization element-wise using vectorized approach
    # (applymap is okay for ~60k*columns; still usually fine)
    return df.applymap(normalize_cell)


# -----------------------------
# Core compare logic (chunked + threaded)
# -----------------------------
def compare_chunk(df1_chunk, df2_chunk, key_col, cols_to_compare, float_tol):
    """
    Compare two aligned chunks (same index/key) and return mismatch rows.
    Output: list of dicts describing mismatches.
    """
    mismatches = []

    # Compare cell-by-cell using numpy arrays for speed
    for col in cols_to_compare:
        a = df1_chunk[col].to_numpy()
        b = df2_chunk[col].to_numpy()

        # element-wise compare
        for i in range(len(a)):
            v1, v2 = a[i], b[i]

            if v1 is None and v2 is None:
                continue

            # float tolerance compare if both floats
            if isinstance(v1, float) and isinstance(v2, float):
                if (v1 is None and v2 is not None) or (v1 is not None and v2 is None):
                    pass
                else:
                    if math.isfinite(v1) and math.isfinite(v2):
                        if abs(v1 - v2) <= float_tol:
                            continue

            if v1 != v2:
                k = df1_chunk.index[i] if key_col else (df1_chunk.index[i])
                mismatches.append({
                    "key" if key_col else "row_index": k,
                    "column": col,
                    "value_file1": v1,
                    "value_file2": v2
                })

    return mismatches


def compare_dataframes(df1, df2, key_col=None, float_tol=0.0, threads=8, chunk_size=5000):
    """
    Compare two normalized DataFrames.
    If key_col is provided, aligns rows by key.
    Else aligns by row order (index).
    """
    result = {
        "missing_in_file2": [],
        "missing_in_file1": [],
        "mismatches": [],
        "stats": {}
    }

    # Align columns intersection only (common columns)
    common_cols = [c for c in df1.columns if c in df2.columns]
    if not common_cols:
        raise ValueError("No common columns found between the two files/sheets.")

    # If key_col provided, verify exists
    if key_col:
        if key_col not in df1.columns or key_col not in df2.columns:
            raise ValueError(f"Key column '{key_col}' not found in both files.")
        df1 = df1.set_index(key_col, drop=False)
        df2 = df2.set_index(key_col, drop=False)

        keys1 = set(df1.index)
        keys2 = set(df2.index)

        missing_in_2 = sorted(list(keys1 - keys2))
        missing_in_1 = sorted(list(keys2 - keys1))

        result["missing_in_file2"] = missing_in_2
        result["missing_in_file1"] = missing_in_1

        # Keep only shared keys for comparison
        shared_keys = sorted(list(keys1 & keys2))
        df1c = df1.loc[shared_keys, common_cols]
        df2c = df2.loc[shared_keys, common_cols]
    else:
        # Row order compare
        min_len = min(len(df1), len(df2))
        if len(df1) != len(df2):
            # extra rows considered missing
            if len(df1) > len(df2):
                result["missing_in_file2"] = list(range(min_len, len(df1)))
            else:
                result["missing_in_file1"] = list(range(min_len, len(df2)))

        df1c = df1.iloc[:min_len][common_cols].copy()
        df2c = df2.iloc[:min_len][common_cols].copy()

    cols_to_compare = common_cols

    # Chunking indices
    total = len(df1c)
    ranges = [(i, min(i + chunk_size, total)) for i in range(0, total, chunk_size)]

    start = time.time()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = []
        for (s, e) in ranges:
            futures.append(ex.submit(
                compare_chunk,
                df1c.iloc[s:e],
                df2c.iloc[s:e],
                key_col,
                cols_to_compare,
                float_tol
            ))
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
    """
    Uses pandas + openpyxl. For 50-60k rows this is typically fine.
    Tip: if file is huge with many sheets, specify sheet_name to reduce read time.
    """
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


def main():
    ap = argparse.ArgumentParser(description="Fast Excel vs Excel comparator (threaded, chunked).")
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

    # Read Excel(s)
    if args.sheet:
        df1 = read_excel_fast(args.file1, sheet_name=args.sheet)
        df2 = read_excel_fast(args.file2, sheet_name=args.sheet)

        df1n = normalize_df(df1)
        df2n = normalize_df(df2)

        res = compare_dataframes(df1n, df2n, key_col=args.key, float_tol=args.float_tol,
                                 threads=args.threads, chunk_size=args.chunk)

        # Write outputs
        write_outputs(res, args.outdir, sheet=args.sheet)

    else:
        # Compare common sheet names
        xl1 = pd.ExcelFile(args.file1, engine="openpyxl")
        xl2 = pd.ExcelFile(args.file2, engine="openpyxl")
        common_sheets = [s for s in xl1.sheet_names if s in xl2.sheet_names]

        if not common_sheets:
            raise ValueError("No common sheets found. Use --sheet to compare specific sheets.")

        overall = {"sheets": {}, "overall_stats": {}}
        for sh in common_sheets:
            df1 = pd.read_excel(xl1, sheet_name=sh)
            df2 = pd.read_excel(xl2, sheet_name=sh)

            df1n = normalize_df(df1)
            df2n = normalize_df(df2)

            res = compare_dataframes(df1n, df2n, key_col=args.key, float_tol=args.float_tol,
                                     threads=args.threads, chunk_size=args.chunk)

            overall["sheets"][sh] = res["stats"]

            # sheet outputs in subfolder
            sheet_dir = os.path.join(args.outdir, f"sheet_{sh}")
            os.makedirs(sheet_dir, exist_ok=True)
            write_outputs(res, sheet_dir, sheet=sh)

        overall["overall_stats"] = {
            "common_sheets": len(common_sheets),
            "generated_at": datetime.now().isoformat(sep=" ")
        }

        with open(os.path.join(args.outdir, "overall_summary.json"), "w", encoding="utf-8") as f:
            json.dump(overall, f, indent=2)

    print(f"\nDone. Outputs saved in: {args.outdir}")


def write_outputs(res, outdir, sheet):
    # mismatches
    mism_df = pd.DataFrame(res["mismatches"])
    mism_path = os.path.join(outdir, "mismatches.csv")
    mism_df.to_csv(mism_path, index=False, encoding="utf-8-sig")

    # missing rows
    pd.DataFrame({"missing_in_file2": res["missing_in_file2"]}).to_csv(
        os.path.join(outdir, "missing_in_file2.csv"), index=False, encoding="utf-8-sig"
    )
    pd.DataFrame({"missing_in_file1": res["missing_in_file1"]}).to_csv(
        os.path.join(outdir, "missing_in_file1.csv"), index=False, encoding="utf-8-sig"
    )

    # summary
    summary = {
        "sheet": sheet,
        "stats": res["stats"]
    }
    with open(os.path.join(outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
#python ComaperExcels.py --file1 Excel_File_1.xlsx --file2 Excel_File_2.xlsx 

