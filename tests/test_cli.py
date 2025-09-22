import os
import stat
import time
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / "data" / "reports"
STAGING_DIR = ROOT / "data" / "staging"

def _rmtree_retry(path: Path, tries: int = 8, delay: float = 0.25) -> None:
    if not path.exists():
        return
    def _onerror(func, p, exc_info):
        try:
            os.chmod(p, stat.S_IWRITE)
        except Exception:
            pass
        try:
            func(p)
        except PermissionError:
            raise
    for _ in range(tries):
        try:
            shutil.rmtree(path, onerror=_onerror)
            return
        except PermissionError:
            time.sleep(delay)
    shutil.rmtree(path, ignore_errors=True)

@pytest.fixture(autouse=True)
def setup_teardown():
    """Create and destroy staging/reports dirs for each test (Windows-safe)."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    yield
    _rmtree_retry(STAGING_DIR)
    _rmtree_retry(REPORTS_DIR)

def _run(cmd):
    """Run a command in a subprocess and return (code, out, err)."""
    p = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        text=True,
    )
    out, err = p.communicate()
    return p.returncode, out, err

def test_cli_pair_flag():
    code, out, err = _run([sys.executable, "pipeline.py", "--pair", "invoices_vs_jobs"])
    assert code == 0, f"non-zero exit: {code}\nSTDOUT:\n{out}\nSTDERR:\n{err}"
    # artifact exists
    xlsx = REPORTS_DIR / "invoices_vs_jobs__detailed_report.xlsx"
    assert xlsx.exists()

def test_cli_cutoff_flag():
    code, out, err = _run([sys.executable, "pipeline.py", "--pair", "invoices_vs_jobs", "--cutoff", "2025-06-30"])
    assert code == 0, f"non-zero exit: {code}\nSTDOUT:\n{out}\nSTDERR:\n{err}"
    xlsx = REPORTS_DIR / "invoices_vs_jobs__detailed_report.xlsx"
    assert xlsx.exists()
