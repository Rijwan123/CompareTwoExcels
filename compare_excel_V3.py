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
        return float(x)

    s = str(x).strip()
    if s == "":
        return None
    return _WS_RE.sub(" ", s)

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if hasattr(df, "map"):  # pandas >= 2.1
        return df.map(normalize_cell)
    return df.applymap(normalize_cell)

def check_structure(df1: pd.DataFrame, df2: pd.DataFrame):
    cols1 = list(df1.columns)
    cols2 = list(df2.columns)

    missing_columns = sorted(list(set(cols1) - set(cols2)))
    extra_columns = sorted(list(set(cols2) - set(cols1)))

    reordered_columns = []
    for c in cols1:
        if c in cols2 and cols1.index(c) != cols2.index(c):
            reordered_columns.append({
                "column": c,
                "reference_position": cols1.index(c),
                "target_position": cols2.index(c)
            })

    return {
        "structure_fail": bool(missing_columns or extra_columns or reordered_columns),
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "reordered_columns": reordered_columns
    }

def compare_chunk(df1c, df2c, key_col, cols, float_tol):
    mismatches = []
    for col in cols:
        a = df1c[col].to_numpy()
        b = df2c[col].to_numpy()

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
                    "type": "DATA",
                    "key" if key_col else "row_index": df1c.index[i],
                    "column": col,
                    "value_file1": v1,
                    "value_file2": v2,
                    "description": "Data mismatch"
                })
    return mismatches

def compare_dataframes(df1, df2, key_col=None, float_tol=0.0, threads=8, chunk_size=5000):
    result = {"missing_in_file2": [], "missing_in_file1": [], "mismatches": [], "stats": {}}

    common_cols = [c for c in df1.columns if c in df2.columns]
    if not common_cols:
        result["stats"] = {"rows_compared": 0, "common_columns": 0, "mismatch_cells": 0, "compare_seconds": 0.0}
        return result

    if key_col:
        if key_col not in df1.columns or key_col not in df2.columns:
            raise ValueError(f"Key column '{key_col}' not found in both files.")
        df1 = df1.set_index(key_col, drop=False)
        df2 = df2.set_index(key_col, drop=False)

        keys1, keys2 = set(df1.index), set(df2.index)
        result["missing_in_file2"] = sorted(keys1 - keys2)
        result["missing_in_file1"] = sorted(keys2 - keys1)

        shared = sorted(keys1 & keys2)
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

    ranges = [(i, min(i + chunk_size, len(df1c))) for i in range(0, len(df1c), chunk_size)]

    start = time.time()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [
            ex.submit(compare_chunk, df1c.iloc[s:e], df2c.iloc[s:e], key_col, common_cols, float_tol)
            for s, e in ranges
        ]
        for f in as_completed(futures):
            result["mismatches"].extend(f.result())

    result["stats"] = {
        "rows_compared": len(df1c),
        "common_columns": len(common_cols),
        "mismatch_cells": len(result["mismatches"]),
        "compare_seconds": round(time.time() - start, 2)
    }
    return result

def write_outputs(res, outdir, sheet_name="Sheet1"):
    os.makedirs(outdir, exist_ok=True)

    # STRUCTURE rows included into same CSV
    structure_rows = []
    st = res.get("structure", {})

    for c in st.get("missing_columns", []):
        structure_rows.append({
            "type": "STRUCTURE",
            "row_index": "",
            "column": c,
            "value_file1": "PRESENT",
            "value_file2": "REMOVED",
            "description": "Column removed from Target file"
        })

    for c in st.get("extra_columns", []):
        structure_rows.append({
            "type": "STRUCTURE",
            "row_index": "",
            "column": c,
            "value_file1": "ABSENT",
            "value_file2": "ADDED",
            "description": "Column added in Target file"
        })

    for r in st.get("reordered_columns", []):
        structure_rows.append({
            "type": "STRUCTURE",
            "row_index": "",
            "column": r["column"],
            "value_file1": f"position={r['reference_position']}",
            "value_file2": f"position={r['target_position']}",
            "description": "Column order changed"
        })

    all_rows = structure_rows + res["mismatches"]
    df_out = pd.DataFrame(all_rows)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mism_path = os.path.join(outdir, f"mismatches_{ts}.csv")
    df_out.to_csv(mism_path, index=False, encoding="utf-8-sig")

    overall_status = "FAIL" if (st.get("structure_fail") or res["stats"]["mismatch_cells"] > 0) else "PASS"

    with open(os.path.join(outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump({
            "sheet": sheet_name,
            "overall_status": overall_status,
            "stats": res["stats"],
            "structure": st,
            "output_mismatches_csv": os.path.basename(mism_path)
        }, f, indent=2)

    print(f"[{sheet_name}] RESULT: {overall_status}")
    print(f"Mismatch cells (data): {res['stats']['mismatch_cells']}")
    print(f"Structure fail: {st.get('structure_fail', False)}")
    print(f"CSV: {mism_path}")

def main():
    ap = argparse.ArgumentParser(description="Single-sheet Excel comparator (structure + data in one CSV).")
    ap.add_argument("--file1", required=True)
    ap.add_argument("--file2", required=True)
    ap.add_argument("--key", default=None)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--chunk", type=int, default=5000)
    ap.add_argument("--float_tol", type=float, default=0.0)
    ap.add_argument("--outdir", default="excel_compare_output")
    args = ap.parse_args()

    # SINGLE SHEET: always read first sheet as DataFrame
    df1 = pd.read_excel(args.file1, sheet_name=0, engine="openpyxl")
    df2 = pd.read_excel(args.file2, sheet_name=0, engine="openpyxl")

    structure = check_structure(df1, df2)

    df1n = normalize_df(df1)
    df2n = normalize_df(df2)

    res = compare_dataframes(df1n, df2n, key_col=args.key,
                             float_tol=args.float_tol,
                             threads=args.threads, chunk_size=args.chunk)

    res["structure"] = structure

    write_outputs(res, args.outdir, sheet_name="Sheet1")

if __name__ == "__main__":
    main()
