"""Isolated test for the Sail vs. PySpark benchmark example.

Unlike the numbered examples (see ``test_all.py``), this script stops the
classic Spark session and starts a Sail Connect server, so it cannot share the
session-scoped fixture. It is run in a fresh subprocess instead, and skipped
entirely when the optional ``sail`` dependency group is not installed.
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pysail", reason="requires the 'sail' dependency group")

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPT = ROOT_DIR / "examples" / "sail_vs_pyspark.py"


def test_sail_vs_pyspark_runs() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--rows", "100000", "--runs", "1"],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Both engines agree." in result.stdout
