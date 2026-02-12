# Excel Compare Tool
# - Compares large Excel files using chunking + multithreading for performance
# - Normalizes values (text/date/null/whitespace) to reduce false mismatches
# - Supports row alignment using a key column (recommended) or row order
# - Detects and reports extra/missing columns in each file
# - Outputs: mismatches.csv, missing_in_file*.csv, extra_columns_file*.csv, summary.json, overall_summary.json
#
# UPDATED FOR TOSCA:
# - Prints full summary.json content (pretty JSON) to stdout with START/END markers
# - Prints RESULT=PASS/FAIL + key metrics
# - Prints FAIL_MESSAGE (single line) when FAIL (for Tosca tester visibility)
# - Exit codes: 0 PASS, 1 FAIL (logical compare fail), 2 ERROR (script/config error)
#
# NEW CHANGE (as per your latest requirement):
# - EXTRA COLUMNS should also FAIL (schema mismatch fails Tosca)

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
from datetime import datetime


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

    # everything else -> string normalize
    s = str(x).strip()
    if s == "":
        return None
    s = _WS_RE.sub(" ", s)
    return s


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize dataframe:
    - strips column names
    - normalizes each cell using normalize_cell
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df.applymap(normalize_cell)


# -----------------------------
# Core compare logic (chunked + threaded)
# -----------------------------
def compare_chunk(df1_chunk, df2_chunk, key_col, cols_to_compare, float_tol):
    """
    Compare two aligned chunks and return mismatch rows.
    Output: list of dicts describing mismatches.
    """
    mismatches = []

    for col in cols_to_compare:
        a = df1_chunk[col].to_numpy()
        b = df2_chunk[col].to_numpy()

        for i in range(len(a)):
            v1, v2 = a[i], b[i]

            if v1 is None and v2 is None:
                continue

            # float tolerance compare if both floats
            if isinstance(v1, float) and isinstance(v2, float):
                if math.isfinite(v1) and math.isfinite(v2):
                    if abs(v1 - v2) <= float_tol:
                        continue

            if v1 != v2:
                k = df1_chunk.index[i] if key_col else df1_chunk.index[i]
                mismatches.append(
                    {
                        "key" if key_col else "row_index": k,
                        "column": col,
                        "value_file1": v1,
                        "value_file2": v2,
                    }
                )

    return mismatches


def compare_dataframes(
    df1,
    df2,
    key_col=None,
    float_tol=0.0,
    threads=8,
    chunk_size=5000,
    strict_columns=False,
):
    """
    Compare two normalized DataFrames.
    If key_col is provided, aligns rows by key.
    Else aligns by row order (index).

    - Detects and reports extra columns in file1/file2
    - If strict_columns=True, raises error when extra columns exist
    """
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    extra_in_file1 = sorted(list(cols1 - cols2))
    extra_in_file2 = sorted(list(cols2 - cols1))

    common_cols = [c for c in df1.columns if c in df2.columns]

    if not common_cols:
        raise ValueError("No common columns found between the two files/sheets.")

    # keep this behavior: strict_columns can hard-stop early (script ERROR style)
    if strict_columns and (extra_in_file1 or extra_in_file2):
        raise ValueError(
            "Column mismatch detected.\n"
            f"Extra in file1: {extra_in_file1}\n"
            f"Extra in file2: {extra_in_file2}\n"
            "Re-run without --strict_columns to only report (not fail)."
        )

    result = {
        "missing_in_file2": [],
        "missing_in_file1": [],
        "extra_columns_file1": extra_in_file1,
        "extra_columns_file2": extra_in_file2,
        "mismatches": [],
        "stats": {},
    }

    # ----- Row alignment -----
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
        min_len = min(len(df1), len(df2))
        if len(df1) != len(df2):
            if len(df1) > len(df2):
                result["missing_in_file2"] = list(range(min_len, len(df1)))
            else:
                result["missing_in_file1"] = list(range(min_len, len(df2)))

        df1c = df1.iloc[:min_len][common_cols].copy()
        df2c = df2.iloc[:min_len][common_cols].copy()

    # ----- Chunking + threading -----
    total = len(df1c)
    ranges = [(i, min(i + chunk_size, total)) for i in range(0, total, chunk_size)]

    start = time.time()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = []
        for (s, e) in ranges:
            futures.append(
                ex.submit(
                    compare_chunk,
                    df1c.iloc[s:e],
                    df2c.iloc[s:e],
                    key_col,
                    common_cols,
                    float_tol,
                )
            )
        for f in as_completed(futures):
            result["mismatches"].extend(f.result())

    elapsed = time.time() - start
    result["stats"] = {
        "rows_compared": total,
        "common_columns": len(common_cols),
        "extra_columns_file1": len(extra_in_file1),
        "extra_columns_file2": len(extra_in_file2),
        "mismatch_cells": len(result["mismatches"]),
        "missing_rows_file2": len(result["missing_in_file2"]),
        "missing_rows_file1": len(result["missing_in_file1"]),
        "threads": threads,
        "chunk_size": chunk_size,
        "compare_seconds": round(elapsed, 3),
    }
    return result


# -----------------------------
# Excel reading
# -----------------------------
def read_excel_fast(path, sheet_name=None):
    return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")


def write_outputs(res, outdir, sheet):
    os.makedirs(outdir, exist_ok=True)

    # mismatches
    pd.DataFrame(res["mismatches"]).to_csv(
        os.path.join(outdir, "mismatches.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    # missing rows
    pd.DataFrame({"missing_in_file2": res["missing_in_file2"]}).to_csv(
        os.path.join(outdir, "missing_in_file2.csv"),
        index=False,
        encoding="utf-8-sig",
    )
    pd.DataFrame({"missing_in_file1": res["missing_in_file1"]}).to_csv(
        os.path.join(outdir, "missing_in_file1.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    # extra columns
    pd.DataFrame({"extra_columns_file1": res.get("extra_columns_file1", [])}).to_csv(
        os.path.join(outdir, "extra_columns_file1.csv"),
        index=False,
        encoding="utf-8-sig",
    )
    pd.DataFrame({"extra_columns_file2": res.get("extra_columns_file2", [])}).to_csv(
        os.path.join(outdir, "extra_columns_file2.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    # summary.json
    summary = {
        "sheet": sheet,
        "stats": res["stats"],
        "extra_columns_file1": res.get("extra_columns_file1", []),
        "extra_columns_file2": res.get("extra_columns_file2", []),
        "missing_in_file2": res.get("missing_in_file2", []),
        "missing_in_file1": res.get("missing_in_file1", []),
    }

    summary_path = os.path.join(outdir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary_path, summary


def print_summary_json_for_tosca(summary: dict, summary_path: str) -> int:
    """
    Prints:
      - RESULT=PASS/FAIL and key metrics
      - FAIL_MESSAGE (single line) on FAIL
      - Full summary.json content between START/END markers
    Returns exit code: 0 PASS, 1 FAIL
    """
    stats = summary.get("stats", {})

    mismatches = int(stats.get("mismatch_cells", 0) or 0)
    miss_f1 = int(stats.get("missing_rows_file1", 0) or 0)
    miss_f2 = int(stats.get("missing_rows_file2", 0) or 0)
    extra1 = int(stats.get("extra_columns_file1", 0) or 0)
    extra2 = int(stats.get("extra_columns_file2", 0) or 0)

    fail_reasons = []
    if mismatches > 0:
        fail_reasons.append(f"mismatch_cells={mismatches}")
    if miss_f1 > 0:
        fail_reasons.append(f"missing_rows_file1={miss_f1}")
    if miss_f2 > 0:
        fail_reasons.append(f"missing_rows_file2={miss_f2}")
    # NEW: extra columns should FAIL
    if extra1 > 0:
        fail_reasons.append(f"extra_columns_file1={extra1}")
    if extra2 > 0:
        fail_reasons.append(f"extra_columns_file2={extra2}")

    result = "PASS" if not fail_reasons else "FAIL"

    # easy Tosca verification lines
    print(f"SUMMARY_FILE={os.path.abspath(summary_path)}")
    print(f"RESULT={result}")
    print(f"MISMATCH_CELLS={mismatches}")
    print(f"MISSING_ROWS_FILE1={miss_f1}")
    print(f"MISSING_ROWS_FILE2={miss_f2}")
    print(f"EXTRA_COLUMNS_FILE1={extra1}")
    print(f"EXTRA_COLUMNS_FILE2={extra2}")

    # NEW: show a clear reason for tester
    if result == "FAIL":
        msg = f"Sheet='{summary.get('sheet','')}' -> " + ", ".join(fail_reasons)
        print("FAIL_MESSAGE=" + msg)

        # Optional: print the actual column names too (very helpful)
        if summary.get("extra_columns_file1"):
            print("EXTRA_COLUMNS_FILE1_LIST=" + ",".join(map(str, summary["extra_columns_file1"])))
        if summary.get("extra_columns_file2"):
            print("EXTRA_COLUMNS_FILE2_LIST=" + ",".join(map(str, summary["extra_columns_file2"])))

    # full JSON for Tosca output
    print("----- SUMMARY_JSON_START -----")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print("----- SUMMARY_JSON_END -----")

    return 0 if result == "PASS" else 1


def main():
    ap = argparse.ArgumentParser(description="Fast Excel vs Excel comparator (threaded, chunked).")
    ap.add_argument("--file1", required=True, help="Path to reference Excel")
    ap.add_argument("--file2", required=True, help="Path to target Excel")
    ap.add_argument("--sheet", default=None, help="Sheet name to compare (optional).")
    ap.add_argument("--key", default=None, help="Key column name for row alignment (recommended).")
    ap.add_argument(
        "--threads",
        type=int,
        default=max(4, (os.cpu_count() or 8) // 2),
        help="Thread count",
    )

    # ✅ UPDATED DYNAMIC OUTPUT DIRECTORY
    base_dir = "excel_compare_output"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_outdir = os.path.join(base_dir, f"run_{timestamp}")

    ap.add_argument("--chunk", type=int, default=5000, help="Chunk size per thread job")
    ap.add_argument("--float_tol", type=float, default=0.0, help="Tolerance for float compare")
    ap.add_argument("--outdir", default=default_outdir, help="Output directory")
    ap.add_argument(
        "--strict_columns",
        action="store_true",
        help="Fail comparison if extra columns exist in either file (otherwise just report).",
    )

    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    try:
        if args.sheet:
            df1 = read_excel_fast(args.file1, sheet_name=args.sheet)
            df2 = read_excel_fast(args.file2, sheet_name=args.sheet)

            df1n = normalize_df(df1)
            df2n = normalize_df(df2)

            res = compare_dataframes(
                df1n,
                df2n,
                key_col=args.key,
                float_tol=args.float_tol,
                threads=args.threads,
                chunk_size=args.chunk,
                strict_columns=args.strict_columns,
            )

            summary_path, summary = write_outputs(res, args.outdir, sheet=args.sheet)

            # print full JSON to Tosca output + return pass/fail exit code
            return print_summary_json_for_tosca(summary, summary_path)

        # else: compare all common sheets (prints a summary per sheet)
        xl1 = pd.ExcelFile(args.file1, engine="openpyxl")
        xl2 = pd.ExcelFile(args.file2, engine="openpyxl")
        common_sheets = [s for s in xl1.sheet_names if s in xl2.sheet_names]

        if not common_sheets:
            raise ValueError("No common sheets found. Use --sheet to compare specific sheets.")

        overall = {"sheets": {}, "overall_stats": {}}
        overall_fail = False

        for sh in common_sheets:
            df1 = pd.read_excel(xl1, sheet_name=sh, engine="openpyxl")
            df2 = pd.read_excel(xl2, sheet_name=sh, engine="openpyxl")

            df1n = normalize_df(df1)
            df2n = normalize_df(df2)

            res = compare_dataframes(
                df1n,
                df2n,
                key_col=args.key,
                float_tol=args.float_tol,
                threads=args.threads,
                chunk_size=args.chunk,
                strict_columns=args.strict_columns,
            )

            overall["sheets"][sh] = res["stats"]

            sheet_dir = os.path.join(args.outdir, f"sheet_{sh}")
            summary_path, summary = write_outputs(res, sheet_dir, sheet=sh)

            # print each sheet summary.json too
            print(f"\nSHEET={sh}")
            exit_code_sheet = print_summary_json_for_tosca(summary, summary_path)
            if exit_code_sheet != 0:
                overall_fail = True

        overall["overall_stats"] = {
            "common_sheets": len(common_sheets),
            "generated_at": datetime.now().isoformat(sep=" "),
        }

        with open(os.path.join(args.outdir, "overall_summary.json"), "w", encoding="utf-8") as f:
            json.dump(overall, f, indent=2)

        return 1 if overall_fail else 0

    except Exception as e:
        # Any error -> ExitCode 2 (so Tosca knows it is script/config issue)
        print("ERROR=YES")
        print(f"ERROR_MESSAGE={e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

# Example:
# python compare_excel_v6.py --file1 S5_ColChange_Reference.xlsx --file2 S5_ColChange_Target.xlsx
