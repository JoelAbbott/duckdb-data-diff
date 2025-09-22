"""
DuckDB Data-Diff Pipeline
-------------------------
Loads datasets from datasets.yaml, stages to Parquet, validates basics,
runs presence and value diffs, and outputs CSVs + an Excel workbook
with Summary, Only-in-Left/Right, Value Differences (with flags),
and Data_Lineage.
"""

import duckdb
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timezone
import unicodedata
import re
import argparse
import sys
import shutil
import tempfile
from importlib.metadata import version, PackageNotFoundError


# ----------------------------
# Setup paths
# ----------------------------
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
STAGING = DATA / "staging"
REPORTS = DATA / "reports"


# ----------------------------
# Logging
# ----------------------------
def log(msg):
    """Prints a message with a timestamp."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ----------------------------
# UDFs / Normalizers
# ----------------------------
def unicode_clean_py(val: str) -> str:
    """Normalize text by removing accents, odd unicode, and collapsing spaces."""
    if not isinstance(val, str):
        return val
    # Decompose characters into base + combining marks
    nfkd_form = unicodedata.normalize('NFKD', val)
    val = "".join([c for c in nfkd_form if not unicodedata.combining(c)])

    val = re.sub(r"[\u200B-\u200D\u2060\ufeff]", "", val)
    val = val.replace("“", '"').replace("”", '"')
    val = val.replace("‘", "'").replace("’", "'")
    val = val.replace("–", "-").replace("—", "-")
    val = re.sub(r"\s+", " ", val).strip()
    return val

def collapse_spaces_py(val: str) -> str:
    """Collapse multiple whitespace chars into a single space."""
    if not isinstance(val, str):
        return val
    return re.sub(r"\s+", " ", val).strip()

def currency_usd_py(val) -> float | None:
    """Convert string to float, handling $, ,, and parentheses for negatives."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if not isinstance(val, str):
        return None

    val = val.strip()
    is_negative = val.startswith("(") and val.endswith(")")
    val = val.strip("() ")
    val = val.replace("$", "").replace(",", "")

    try:
        num = float(val)
        return -num if is_negative else num
    except (ValueError, TypeError):
        return None

# ----------------------------
# DuckDB Helpers
# ----------------------------
def register_udfs(con):
    """Register all Python UDFs with the DuckDB connection."""
    con.create_function("unicode_clean", unicode_clean_py, ['VARCHAR'], 'VARCHAR', null_handling='SPECIAL')
    con.create_function("collapse_spaces", collapse_spaces_py, ['VARCHAR'], 'VARCHAR', null_handling='SPECIAL')
    con.create_function("currency_usd", currency_usd_py, ['VARCHAR'], 'DOUBLE', null_handling='SPECIAL')

# ----------------------------
# Load config
# ----------------------------
def load_config():
    """Loads the datasets.yaml config file."""
    with open(ROOT / "datasets.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ----------------------------
# Stage datasets
# ----------------------------
def stage_dataset(con, name, cfg, force_pandas=False):
    """Load, normalize, and cast dataset into a Parquet file."""
    log(f"[STAGE] {name}")
    path = ROOT / cfg["path"]
    sheet = cfg.get("sheet")

    # --- Load Data ---
    if path.suffix.lower() in [".xls", ".xlsx"] and not force_pandas:
        try:
            con.execute("INSTALL excel; LOAD excel;")
            sheet_clause = f", sheet='{sheet}'" if sheet else ""
            con.execute(f"CREATE OR REPLACE TABLE {name}_raw AS SELECT * FROM read_excel('{path.as_posix()}'{sheet_clause})")
        except (duckdb.IOException, duckdb.CatalogException):
            log(f"[WARN] DuckDB excel extension failed. Falling back to pandas for {path.name}.")
            force_pandas = True

    if path.suffix.lower() == ".csv":
        con.execute(f"CREATE OR REPLACE TABLE {name}_raw AS SELECT * FROM read_csv_auto('{path.as_posix()}', header=TRUE, all_varchar=1)")

    if force_pandas and path.suffix.lower() in [".xls", ".xlsx"]:
        temp_path = STAGING / f"~{Path(tempfile.mktemp()).name}{path.suffix}"
        shutil.copyfile(path, temp_path)
        try:
            df = pd.read_excel(temp_path, sheet_name=sheet)
            con.execute(f"CREATE OR REPLACE TABLE {name}_raw AS SELECT * FROM df")
        finally:
            if temp_path.exists():
                temp_path.unlink()

    # --- Normalize and Cast ---
    # Rename columns based on map
    for src, dest in cfg.get("column_map", {}).items():
        try:
            con.execute(f'ALTER TABLE {name}_raw RENAME COLUMN "{src}" TO {dest}')
        except duckdb.CatalogException:
            log(f"[WARN] Column '{src}' not found in {name}_raw, skipping rename.")

    # Get all columns after renaming
    raw_cols = [c[0] for c in con.execute(f'DESCRIBE {name}_raw').fetchall()]

    # Build SELECT expressions
    select_exprs = []

    # Derived column: _rownum
    key_cols = cfg.get("keys", [])
    non_key_cols = sorted([c for c in raw_cols if c not in key_cols])
    order_by_cols = key_cols + non_key_cols
    if order_by_cols:
        select_exprs.append(f"row_number() OVER (ORDER BY {', '.join(order_by_cols)}) AS _rownum")
    else:
        select_exprs.append("1 AS _rownum")

    # Derived column: _last_modified
    if "last_modified" in cfg.get("dtypes", {}) and "last_modified" in raw_cols:
         select_exprs.append("TRY_CAST(last_modified AS DATE) as _last_modified")
    else:
         select_exprs.append("NULL::DATE AS _last_modified")

    # Columns from config
    final_cols = list(cfg.get("dtypes", {}).keys())
    for col in final_cols:
        if col not in raw_cols:
            select_exprs.append(f"NULL AS {col}")
            continue

        expr = f'"{col}"'
        # Apply normalizers
        for rule in cfg.get("normalizers", {}).get(col, []):
            expr = f"{rule}({expr})"

        # Apply casting
        dtype = cfg["dtypes"][col]
        if dtype == "string": expr = f"CAST({expr} AS VARCHAR)"
        elif dtype == "int64": expr = f"TRY_CAST({expr} AS BIGINT)"
        elif dtype == "float64": expr = f"TRY_CAST({expr} AS DOUBLE)"
        elif dtype == "date": expr = f"TRY_CAST({expr} AS DATE)"
        elif dtype == "currency_usd": expr = f"currency_usd({expr})"

        # Avoid re-selecting derived columns
        if col not in ["_rownum", "_last_modified"]:
            select_exprs.append(f"{expr} AS {col}")

    # Ensure key columns are selected if not already
    for key in key_cols:
        if key not in final_cols:
            select_exprs.append(f'"{key}"')

    sql = f"CREATE OR REPLACE TABLE {name} AS SELECT {', '.join(select_exprs)} FROM {name}_raw"
    con.execute(sql)

    log(f"[VALIDATE] {name}")
    out_path = STAGING / f"{name}.parquet"
    con.execute(f"COPY {name} TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    log(f"[STAGING] wrote {out_path.as_posix()}")

# ----------------------------
# Compare datasets
# ----------------------------
def compare_pair(con, name, left, right, cfg_left, cfg_right, cmp_cfg):
    """
    Build presence and value diffs for a pair and write CSV + Excel.
    Assumes staged tables {left} and {right} exist with canonical columns
    and derived columns _rownum (and optional last_modified).
    """
    keys = cmp_cfg["keys"]
    numeric_round = cmp_cfg.get("numeric_round")
    date_filter = cmp_cfg.get("date_filter")

    # ---------- presence diffs ----------
    onlyL = con.execute(f"""
        SELECT {', '.join(keys)} FROM {left}
        ANTI JOIN {right} USING ({', '.join(keys)})
        ORDER BY {', '.join(keys)}
    """).df()
    onlyR = con.execute(f"""
        SELECT {', '.join(keys)} FROM {right}
        ANTI JOIN {left} USING ({', '.join(keys)})
        ORDER BY {', '.join(keys)}
    """).df()

    # ---------- figure out comparable columns ----------
    left_cols  = [r[1] for r in con.execute(f"PRAGMA table_info({left})").fetchall()]
    right_cols = [r[1] for r in con.execute(f"PRAGMA table_info({right})").fetchall()]
    common_non_keys = [c for c in left_cols if (c in right_cols and c not in keys)]

    compare_cols = cmp_cfg.get("compare_columns") or common_non_keys
    compare_cols = [c for c in compare_cols if c in common_non_keys]
    if not compare_cols:
        raise AssertionError(
            f"No comparable columns after intersection. keys={keys}; "
            f"left-only={sorted(set(left_cols)-set(right_cols))}; "
            f"right-only={sorted(set(right_cols)-set(left_cols))}"
        )

    # ---------- helper to wrap numeric rounding ----------
    def cexpr(side, col):
        dt = (cfg_left if side == 'l' else cfg_right)["dtypes"].get(col)
        base = f"{side}.{col}"
        if numeric_round and dt in ("float64", "currency_usd"):
            return f"ROUND(CAST({base} AS DOUBLE), {numeric_round})"
        return base

    # ---------- build tall value diffs (one row per mismatching column) ----------
    diff_q = []
    for col in compare_cols:
        le = cexpr('l', col)
        re = cexpr('r', col)

        lm_left  = "l._last_modified"  if "_last_modified"  in left_cols  else "NULL::DATE"
        lm_right = "r._last_modified"  if "_last_modified"  in right_cols else "NULL::DATE"

        q = f"""
        SELECT
            {', '.join([f'l.{k}' for k in keys])},
            '{col}' AS column_name,
            CAST({le} AS VARCHAR) AS value_in_left,
            CAST({re} AS VARCHAR) AS value_in_right,
            l._rownum AS _rownum_left,
            r._rownum AS _rownum_right,
            {lm_left}  AS last_modified_left,
            {lm_right} AS last_modified_right
        FROM {left} l
        JOIN {right} r USING ({', '.join(keys)})
        WHERE {le} IS DISTINCT FROM {re}
        """
        diff_q.append(q)

    tall_sql = " UNION ALL ".join(diff_q)
    df_diffs = con.execute(tall_sql).df() if diff_q else pd.DataFrame()

    # ---------- date-based error_flag ----------
    if not df_diffs.empty:
        if date_filter:
            cutoff = pd.to_datetime(date_filter["cutoff"])
            op = date_filter["operator"]
            def flag(row):
                vals = [row.get("last_modified_left"), row.get("last_modified_right")]
                hit = False
                for v in vals:
                    try:
                        dt = pd.to_datetime(v)
                        if   op == "<"  and dt <  cutoff: hit = True
                        elif op == "<=" and dt <= cutoff: hit = True
                        elif op == ">"  and dt >  cutoff: hit = True
                        elif op == ">=" and dt >= cutoff: hit = True
                        elif op == "="  and dt == cutoff: hit = True
                    except Exception:
                        pass
                return "ERROR" if hit else "MISMATCH_UNCOUNTED"
            df_diffs["error_flag"] = df_diffs.apply(flag, axis=1)
        else:
            df_diffs["error_flag"] = "ERROR"

    # ---------- write CSVs for all diffs ----------
    (REPORTS / f"{name}__only_in_{left}.csv").write_text(onlyL.to_csv(index=False), encoding="utf-8")
    (REPORTS / f"{name}__only_in_{right}.csv").write_text(onlyR.to_csv(index=False), encoding="utf-8")

    ordered_cols = (
        keys
        + ["column_name", "value_in_left", "value_in_right",
           "_rownum_left", "_rownum_right",
           "last_modified_left", "last_modified_right",
           "error_flag"]
    )
    for c in ordered_cols:
        if c not in df_diffs.columns:
            df_diffs[c] = pd.NA
    df_diffs = df_diffs[ordered_cols]
    df_diffs.to_csv(REPORTS / f"{name}__value_differences.csv", index=False, encoding="utf-8")
    log(f"[REPORT] csv written: {REPORTS / f'{name}__only_in_{left}.csv'}")
    log(f"[REPORT] csv written: {REPORTS / f'{name}__only_in_{right}.csv'}")
    log(f"[REPORT] csv written: {REPORTS / f'{name}__value_differences.csv'}")

    # ---------- Excel workbook ----------
    excel_path = REPORTS / f"{name}__detailed_report.xlsx"
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        # Summary
        summary = pd.DataFrame([
            ["comparison", name],
            ["left", left], ["right", right],
            ["keys", ", ".join(keys)],
            ["compare_columns", ", ".join(compare_cols)],
            ["only_in_left", len(onlyL)], ["only_in_right", len(onlyR)],
            ["value_differences", len(df_diffs)],
            ["timestamp_utc", datetime.now(timezone.utc).isoformat()],
        ], columns=["Metric","Value"])
        summary.to_excel(writer, sheet_name="Summary", index=False)

        # Only-in tabs
        onlyL.to_excel(writer, sheet_name=f"Only in {left}", index=False)
        onlyR.to_excel(writer, sheet_name=f"Only in {right}", index=False)

        # Value Differences
        df_diffs.to_excel(writer, sheet_name="Value Differences", index=False)

        # Data_Lineage with versions
        def _ver(p):
            try: return version(p)
            except PackageNotFoundError: return "n/a"
        lineage_rows = [
            ["left_path",  cfg_left["path"]],
            ["left_sheet", cfg_left.get("sheet")],
            ["right_path",  cfg_right["path"]],
            ["right_sheet", cfg_right.get("sheet")],
            ["keys", ", ".join(keys)],
            ["compare_columns", ", ".join(compare_cols)],
            ["numeric_round", numeric_round],
            ["date_filter", str(date_filter)],
            ["timestamp_utc", datetime.now(timezone.utc).isoformat()],
            ["duckdb", _ver("duckdb")],
            ["pandas", _ver("pandas")],
            ["pyyaml", _ver("pyyaml")],
            ["XlsxWriter", _ver("XlsxWriter")],
            ["openpyxl", _ver("openpyxl")],
        ]
        pd.DataFrame(lineage_rows, columns=["Key","Value"]).to_excel(writer, sheet_name="Data_Lineage", index=False)

        # Formatting on Value Differences
        workbook = writer.book
        ws = writer.sheets["Value Differences"]

        # Freeze header + AutoFilter
        nrows, ncols = df_diffs.shape
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, max(nrows,1), max(ncols-1,0))

        # Conditional formatting ONLY on error_flag
        if "error_flag" in df_diffs.columns and nrows > 0:
            err_col_idx = list(df_diffs.columns).index("error_flag")
            err_fmt  = workbook.add_format({"bg_color": "#FFC7CE"})  # red
            warn_fmt = workbook.add_format({"bg_color": "#FFEB9C"})  # yellow
            ws.conditional_format(1, err_col_idx, nrows, err_col_idx,
                                  {"type":"text","criteria":"containing","value":"ERROR","format":err_fmt})
            ws.conditional_format(1, err_col_idx, nrows, err_col_idx,
                                  {"type":"text","criteria":"containing","value":"MISMATCH_UNCOUNTED","format":warn_fmt})

    log(f"[REPORT] excel written: {excel_path}")


# ----------------------------
# Main
# ----------------------------
def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Run a data-diff pipeline.")
    parser.add_argument("--pair", help="Run only a specific comparison by name.")
    parser.add_argument("--cutoff", help="Override date cutoff (YYYY-MM-DD).")
    args = parser.parse_args()

    STAGING.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    con = duckdb.connect(database=":memory:")
    register_udfs(con)

    for ds_name, ds_cfg in cfg["datasets"].items():
        stage_dataset(con, ds_name, ds_cfg)

    comparisons_to_run = cfg["comparisons"]
    if args.pair:
        comparisons_to_run = [c for c in comparisons_to_run if c["name"] == args.pair]
        if not comparisons_to_run:
            log(f"[ERROR] Comparison '{args.pair}' not found in datasets.yaml.")
            sys.exit(1)

    for cmp_cfg in comparisons_to_run:
        if args.cutoff and "date_filter" in cmp_cfg:
            cmp_cfg["date_filter"]["cutoff"] = args.cutoff
            log(f"[INFO] Using CLI cutoff date: {args.cutoff}")

        compare_pair(
            con,
            cmp_cfg["name"],
            cmp_cfg["left"],
            cmp_cfg["right"],
            cfg["datasets"][cmp_cfg["left"]],
            cfg["datasets"][cmp_cfg["right"]],
            cmp_cfg,
        )

if __name__ == "__main__":
    main()
