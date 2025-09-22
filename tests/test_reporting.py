import pytest
import pandas as pd
from pathlib import Path
import shutil
import sys

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
import duckdb

# --- Fixtures ---

@pytest.fixture(scope="module")
def config():
    """Fixture to load the main config file."""
    return load_config()

@pytest.fixture(scope="module")
def staged_data(config):
    """Fixture to run staging once for all tests in this module."""
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

    con.close()
    if STAGING_DIR.exists():
        shutil.rmtree(STAGING_DIR)
    if REPORTS_DIR.exists():
        shutil.rmtree(REPORTS_DIR)

# --- Tests ---

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

    # Read the Excel file and check its contents
    xls = pd.ExcelFile(report_path)

    # Check sheet names
    expected_sheets = [
        "Summary",
        f"Only in {cmp_cfg['left']}",
        f"Only in {cmp_cfg['right']}",
        "Value Differences",
        "Data_Lineage",
    ]
    assert all(sheet in xls.sheet_names for sheet in expected_sheets)

    # Check Summary sheet
    summary_df = pd.read_excel(xls, sheet_name="Summary")
    summary_metrics = dict(zip(summary_df.Metric, summary_df.Value))

    assert summary_metrics["comparison"] == "invoices_vs_jobs"
    assert summary_metrics["only_in_left"] == 1
    assert summary_metrics["only_in_right"] == 1
    assert summary_metrics["value_differences"] == 3
