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
from datetime import datetime
import unicodedata
import re

# ----------------------------
# Setup paths
# ----------------------------
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW = DATA / "raw"
STAGING = DATA / "staging"
REPORTS = DATA / "reports"

# ----------------------------
# Unicode cleaner
# ----------------------------
def unicode_clean_py(val: str) -> str:
    """Normalize text by removing odd unicode and collapsing spaces."""
    if val is None:
        return None
    # Normalize to NFKC
    val = unicodedata.normalize("NFKC", str(val))
    # Replace non-breaking/zero-width spaces
    val = val.replace("\u00A0", " ").replace("\ufeff", "")
    val = re.sub(r"[\u200B-\u200D\u2060]", "", val)
    # Replace smart quotes/dashes
    val = val.replace("“", '"').replace("”", '"')
    val = val.replace("‘", "'").replace("’", "'")
    val = val.replace("–", "-").replace("—", "-")
    # Collapse whitespace
    val = re.sub(r"\s+", " ", val).strip()
    return val

# ----------------------------
# Load config
# ----------------------------
def load_config():
    with open(ROOT / "datasets.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ----------------------------
# Stage datasets
# ----------------------------
def stage_dataset(con, name, cfg):
    """Load, normalize, and cast dataset into Parquet."""
    path = ROOT / cfg["path"]
    sheet = cfg.get("sheet")

    # Load with DuckDB
    if path.suffix.lower() == ".csv":
        con.execute(f"""
            CREATE OR REPLACE TABLE {name}_raw AS
            SELECT * FROM read_csv_auto('{path.as_posix()}', header=TRUE)
        """)
    elif path.suffix.lower() in [".xls", ".xlsx"]:
        con.execute("INSTALL 'excel'; LOAD 'excel';")
        sheet_clause = f", sheet='{sheet}'" if sheet else ""
        con.execute(f"""
            CREATE OR REPLACE TABLE {name}_raw AS
            SELECT * FROM read_excel('{path.as_posix()}'{sheet_clause})
        """)
    else:
        raise ValueError(f"Unsupported file type: {path}")

    # Rename columns
    for src, dest in cfg.get("column_map", {}).items():
        try:
            con.execute(f'ALTER TABLE {name}_raw RENAME COLUMN "{src}" TO {dest}')
        except Exception:
            pass

    # Register unicode_clean as UDF
    con.create_function("unicode_clean", unicode_clean_py)

    # Apply normalizers + cast types
    select_exprs = []
    for col, dtype in cfg.get("dtypes", {}).items():
        expr = col
        if col in cfg.get("normalizers", {}):
            for rule in cfg["normalizers"][col]:
                if rule == "unicode_clean":
                    expr = f"unicode_clean({expr})"
                elif rule == "upper":
                    expr = f"upper({expr})"
                elif rule == "collapse_spaces":
                    expr = f"regexp_replace({expr}, '\\s+', ' ')"
        if dtype == "string":
            expr = f"CAST({expr} AS VARCHAR)"
        elif dtype == "int64":
            expr = f"CAST({expr} AS BIGINT)"
        elif dtype == "float64":
            expr = f"CAST({expr} AS DOUBLE)"
        elif dtype == "date":
            expr = f"TRY_CAST({expr} AS DATE)"
        elif dtype == "currency_usd":
            expr = f"CAST(regexp_replace({expr}, '[\\$,]', '') AS DOUBLE)"
        select_exprs.append(f"{expr} AS {col}")

    sql = f"""
        CREATE OR REPLACE TABLE {name} AS
        SELECT {', '.join(select_exprs)} FROM {name}_raw
    """
    con.execute(sql)

    # Export Parquet
    out = STAGING / f"{name}.parquet"
    con.execute(f"COPY {name} TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    # Validation report
    rows = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    with open(REPORTS / f"VALIDATE_{name}.txt", "w", encoding="utf-8") as f:
        f.write(f"Dataset: {name}\n")
        f.write(f"Path: {path}\n")
        f.write(f"Rows: {rows}\n")
        for col, dtype in cfg.get("dtypes", {}).items():
            if dtype == "date":
                lo, hi = con.execute(f"SELECT min({col}), max({col}) FROM {name}").fetchone()
                f.write(f"Date range {col}: {lo} → {hi}\n")

# ----------------------------
# Compare datasets
# ----------------------------
def compare_pair(con, name, left, right, cfg_left, cfg_right, cmp):
    keys = cmp["keys"]
    compare_cols = cmp.get("compare_columns")
    numeric_round = cmp.get("numeric_round")
    date_filter = cmp.get("date_filter")

    # Presence
    onlyL = con.execute(f"""
        SELECT {', '.join(keys)} FROM {cmp['left']}
        ANTI JOIN {cmp['right']} USING ({', '.join(keys)})
    """).df()
    onlyL.to_csv(REPORTS / f"{name}__only_in_{cmp['left']}.csv", index=False)

    onlyR = con.execute(f"""
        SELECT {', '.join(keys)} FROM {cmp['right']}
        ANTI JOIN {cmp['left']} USING ({', '.join(keys)})
    """).df()
    onlyR.to_csv(REPORTS / f"{name}__only_in_{cmp['right']}.csv", index=False)

    # Value differences
    all_cols = [c for c in cfg_left["dtypes"].keys() if c not in keys]
    if compare_cols:
        all_cols = [c for c in all_cols if c in compare_cols]

    diffs = []
    for col in all_cols:
        left_expr = f"l.{col}"
        right_expr = f"r.{col}"
        if cfg_left["dtypes"].get(col) in ["float64", "currency_usd"] and numeric_round:
            left_expr = f"ROUND(CAST(l.{col} AS DOUBLE), {numeric_round})"
            right_expr = f"ROUND(CAST(r.{col} AS DOUBLE), {numeric_round})"
        q = f"""
            SELECT {', '.join([f'l.{k}' for k in keys])},
                   '{col}' AS column_name,
                   CAST({left_expr} AS VARCHAR) AS value_in_left,
                   CAST({right_expr} AS VARCHAR) AS value_in_right,
                   row_number() OVER () AS _rownum
            FROM {cmp['left']} l
            JOIN {cmp['right']} r USING ({', '.join(keys)})
            WHERE {left_expr} IS DISTINCT FROM {right_expr}
        """
        diffs.append(con.execute(q).df())
    if diffs:
        df_diffs = pd.concat(diffs, ignore_index=True)
    else:
        df_diffs = pd.DataFrame(columns=keys + ["column_name","value_in_left","value_in_right","_rownum"])

    # Apply date rule
    if date_filter and not df_diffs.empty:
        cutoff = pd.to_datetime(date_filter["cutoff"])
        colL, colR = date_filter["column_left"], date_filter["column_right"]
        op = date_filter["operator"]
        def check(row):
            vL, vR = row.get(colL), row.get(colR)
            res = False
            for v in [vL, vR]:
                try:
                    dt = pd.to_datetime(v)
                    if op == "<" and dt < cutoff: res = True
                    if op == "<=" and dt <= cutoff: res = True
                    if op == ">" and dt > cutoff: res = True
                    if op == ">=" and dt >= cutoff: res = True
                    if op == "=" and dt == cutoff: res = True
                except Exception:
                    pass
            return "ERROR" if res else "MISMATCH_UNCOUNTED"
        df_diffs["error_flag"] = df_diffs.apply(check, axis=1)
    else:
        df_diffs["error_flag"] = "ERROR"
    df_diffs.to_csv(REPORTS / f"{name}__value_differences.csv", index=False)

    # Write Excel workbook
    excel_path = REPORTS / f"{name}__detailed_report.xlsx"
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        # Summary
        summary = pd.DataFrame([
            ["Left dataset", cmp["left"]],
            ["Right dataset", cmp["right"]],
            ["Rows only in left", len(onlyL)],
            ["Rows only in right", len(onlyR)],
            ["Value differences", len(df_diffs)],
            ["Keys", ", ".join(keys)],
            ["Compared columns", ", ".join(all_cols)],
            ["Run timestamp", datetime.utcnow().isoformat()]
        ], columns=["Metric","Value"])
        summary.to_excel(writer, sheet_name="Summary", index=False)

        onlyL.to_excel(writer, sheet_name=f"Only in {cmp['left']}", index=False)
        onlyR.to_excel(writer, sheet_name=f"Only in {cmp['right']}", index=False)
        df_diffs.to_excel(writer, sheet_name="Value Differences", index=False)

        # Data Lineage
        lineage = pd.DataFrame([
            ["Left path", cfg_left["path"]],
            ["Right path", cfg_right["path"]],
            ["Numeric round", numeric_round],
            ["Date filter", str(date_filter)],
        ], columns=["Key","Value"])
        lineage.to_excel(writer, sheet_name="Data_Lineage", index=False)

        # Conditional formatting for errors
        workbook  = writer.book
        ws = writer.sheets["Value Differences"]
        err_fmt = workbook.add_format({"bg_color": "#FFC7CE"})
        warn_fmt = workbook.add_format({"bg_color": "#FFEB9C"})
        if not df_diffs.empty:
            nrows, ncols = df_diffs.shape
            ws.conditional_format(1, ncols-1, nrows, ncols-1,
                                  {"type":"text","criteria":"containing","value":"ERROR","format":err_fmt})
            ws.conditional_format(1, ncols-1, nrows, ncols-1,
                                  {"type":"text","criteria":"containing","value":"MISMATCH_UNCOUNTED","format":warn_fmt})

    print(f"[REPORT] {excel_path} created")

# ----------------------------
# Main
# ----------------------------
def main():
    STAGING.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    con = duckdb.connect(database=":memory:")

    # Stage all datasets
    for name, ds in cfg["datasets"].items():
        stage_dataset(con, name, ds)

    # Run comparisons
    for cmp in cfg["comparisons"]:
        compare_pair(con, cmp["name"],
                     cmp["left"], cmp["right"],
                     cfg["datasets"][cmp["left"]], cfg["datasets"][cmp["right"]],
                     cmp)

if __name__ == "__main__":
    main()