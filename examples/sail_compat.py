"""
Sail compatibility matrix — which tutorial examples run unchanged on Sail?

`Sail <https://github.com/lakehq/sail>`_ implements the Spark Connect protocol,
so in principle the same DataFrame code that runs on classic PySpark also runs
against a Sail server. This script puts that claim to the test: it starts one
in-process Sail server and runs every numbered ``examples/NN_*.py`` against it,
reporting a pass/fail table.

The examples are untouched — they hardcode ``.master("local[*]")`` and a
driver-only Log4j ``.config(...)``. Each example is run in a subprocess whose
``SPARK_REMOTE`` points at the Sail server, with a tiny preamble that
neutralizes ``.master()`` and the driver-only config so the session is routed
to Sail instead of a local JVM. Nothing in ``examples/`` is modified.

This is a *report*, not a test: by default it exits 0 even when some examples
fail, because the failures are the interesting part. Pass ``--fail-on-error``
for CI-style behavior.

Requires the optional ``sail`` dependency group::

    uv sync --group sail
    uv run examples/sail_compat.py
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

EXAMPLES_DIR = Path(__file__).resolve().parent

# Run inside each subprocess before the example: route the hardcoded local
# session to Sail via SPARK_REMOTE by making .master() a no-op, and swallow the
# driver-only .config() (Log4j options that don't apply to a remote engine).
RUNNER_PREAMBLE = """
import sys, runpy
from pyspark.sql import SparkSession
SparkSession.Builder.master = lambda self, *a, **k: self
_orig_config = SparkSession.Builder.config
def _safe_config(self, *a, **k):
    try:
        return _orig_config(self, *a, **k)
    except Exception:
        return self
SparkSession.Builder.config = _safe_config
sys.path.insert(0, {examples!r})
runpy.run_path(sys.argv[1], run_name="__main__")
"""


def numbered_examples() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("[0-9][0-9]_*.py"))


def failure_reason(stderr: str) -> str:
    """Pull the most useful line out of a failed run's stderr.

    Sail logs INFO/WARN lines to stderr; skip those and return the last real
    line, which is normally the exception type and message.
    """
    lines = [
        line.rstrip()
        for line in stderr.splitlines()
        if line.strip() and " sail" not in line and not line.startswith("[")
    ]
    return lines[-1][:160] if lines else "(no error output captured)"


def run_example(example: Path, remote_url: str, timeout: float) -> tuple[bool, str]:
    preamble = RUNNER_PREAMBLE.format(examples=str(EXAMPLES_DIR))
    env = dict(os.environ, SPARK_REMOTE=remote_url)
    try:
        result = subprocess.run(
            [sys.executable, "-c", preamble, str(example)],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout:.0f}s"
    if result.returncode == 0:
        return True, ""
    return False, failure_reason(result.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="per-example timeout in seconds (default: 120)",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="exit non-zero if any example fails (default: always exit 0)",
    )
    args = parser.parse_args()

    from pysail.spark import SparkConnectServer

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    host, port = server.listening_address
    remote_url = f"sc://{host}:{port}"

    examples = numbered_examples()
    name_width = max(len(e.name) for e in examples)
    print(f"Running {len(examples)} examples against Sail at {remote_url}\n")

    passed = 0
    failures: list[tuple[str, str]] = []
    for example in examples:
        ok, reason = run_example(example, remote_url, args.timeout)
        mark = "PASS" if ok else "FAIL"
        detail = "" if ok else f"  {reason}"
        print(f"  [{mark}] {example.name:<{name_width}}{detail}")
        if ok:
            passed += 1
        else:
            failures.append((example.name, reason))

    server.stop()

    total = len(examples)
    print(f"\n{passed}/{total} examples run unchanged on Sail.")
    if failures:
        print("\nIncompatible:")
        for name, reason in failures:
            print(f"  - {name}: {reason}")

    if args.fail_on_error and failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
