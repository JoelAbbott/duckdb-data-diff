import os
import stat
import time
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

# Add root to sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

# Import functions from pipeline
from pipeline import (
    stage_dataset,
    compare_pair,
    load_config,
    register_udfs,
)

# ---------- helpers ----------

def _rmtree_retry(path: Path, tries: int = 8, delay: float = 0.25) -> None:
    """Robust rmtree for Windows/OneDrive locks."""
    if not path.exists():
        return

    def _onerror(func, p, exc_info):
        # try to make writable and retry the op
        try:
            os.chmod(p, stat.S_IWRITE)
        except Exception:
            pass
        try:
            func(p)
        except PermissionError:
            # let outer loop retry
            raise

    for _ in range(tries):
        try:
            shutil.rmtree(path, onerror=_onerror)
            return
        except PermissionError:
            time.sleep(delay)
    # last resort: best-effort without raising
    shutil.rmtree(path, ignore_errors=True)

# ---------- fixtures ----------

@pytest.fixture(scope="module")
def config():
    """Fixture to load the main config file."""
    return load_config()

@pytest.fixture(scope="module")
def staged_data(config):
    """Stage once for all tests in this module."""
    STAGING_DIR = ROOT / "data" / "staging"
    REPORTS_DIR = ROOT / "data" / "reports"
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    register_udfs(con)

    # Stage all datasets from config
    for name, cfg in config["datasets"].items():
        stage_dataset(con, name, cfg)

    # Yield the connection with staged data
    yield con

    # Teardown: close duckdb and clean folders with retry
    con.close()
    _rmtree_retry(STAGING_DIR)
    _rmtree_retry(REPORTS_DIR)

# ---------- tests ----------

def test_excel_report_generation(staged_data, config):
    """Test that the Excel report is generated with the correct content."""
    # Run the comparison
    cmp_cfg = config["comparisons"][0]
    name = cmp_cfg["name"]
    compare_pair(
        staged_data,
        name,
        cmp_cfg["left"],
        cmp_cfg["right"],
        config["datasets"][cmp_cfg["left"]],
        config["datasets"][cmp_cfg["right"]],
        cmp_cfg,
    )

    # Check if the Excel file exists
    report_path = ROOT / "data" / "reports" / f"{name}__detailed_report.xlsx"
    assert report_path.exists()

    # Read all sheets via a context manager to ensure the file is closed
    with pd.ExcelFile(report_path, engine="openpyxl") as xf:
        sheet_names = set(xf.sheet_names)
        expected_sheets = {
            "Summary",
            f"Only in {cmp_cfg['left']}",
            f"Only in {cmp_cfg['right']}",
            "Value Differences",
            "Data_Lineage",
        }
        assert expected_sheets.issubset(sheet_names)

        # Summary checks
        summary_df = xf.parse("Summary")
        summary_metrics = dict(zip(summary_df.Metric.astype(str), summary_df.Value))
        assert summary_metrics["comparison"] == "invoices_vs_jobs"
        assert int(summary_metrics["only_in_left"]) == 1
        assert int(summary_metrics["only_in_right"]) == 1
        assert int(summary_metrics["value_differences"]) == 3
