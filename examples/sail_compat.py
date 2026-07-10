"""
Sail compatibility matrix — which tutorial examples run unchanged on Sail?

`Sail <https://github.com/lakehq/sail>`_ implements the Spark Connect protocol,
so in principle the same DataFrame code that runs on classic PySpark also runs
against a Sail server. This script puts that claim to the test: it runs every
numbered ``examples/NN_*.py`` against Sail and reports a three-way matrix:

* ``succ`` (green badge) — ran cleanly, exactly as on classic PySpark.
* ``warn`` (yellow badge) — ran and succeeded, but Sail logged a warning that
  some operation isn't supported yet and was silently ignored (e.g. a broadcast
  ``hint``). The code works, but not every optimization took effect.
* ``fail`` (red badge) — raised an error Sail (or the Spark Connect protocol)
  couldn't handle.

The markers are ASCII text badges on an ANSI-coloured background (not emoji, so
they render in terminals, asciinema recordings, and CI logs alike).

The examples are untouched — they hardcode ``.master("local[*]")`` and a
driver-only Log4j ``.config(...)``. Each example runs in its own subprocess
that starts a private in-process Sail server, points ``SPARK_REMOTE`` at it,
and neutralizes ``.master()`` and the driver-only config so the session is
routed to Sail instead of a local JVM. A per-example server is what lets each
subprocess capture Sail's own ``WARN`` log lines (they come from the server,
not the client), which is how the ⚠️ no-ops are detected. Nothing in
``examples/`` is modified.

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

# Run inside each subprocess before the example: start a private Sail server,
# route the hardcoded local session to it via SPARK_REMOTE by making .master()
# a no-op, and swallow the driver-only .config() (Log4j options that don't
# apply to a remote engine). The private server means this subprocess's stderr
# also carries Sail's own WARN lines, which flag unsupported no-ops.
RUNNER_PREAMBLE = """
import os, sys, runpy
from pyspark.sql import SparkSession
from pysail.spark import SparkConnectServer
_server = SparkConnectServer("127.0.0.1", 0)
_server.start(background=True)
_host, _port = _server.listening_address
os.environ["SPARK_REMOTE"] = f"sc://{{_host}}:{{_port}}"
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

    Sail logs timestamped INFO/WARN lines to stderr; skip those and return the
    last real line, which is normally the exception type and message.
    """
    lines = [
        line.rstrip()
        for line in stderr.splitlines()
        if line.strip() and " sail" not in line and not line.startswith("[")
    ]
    return lines[-1][:160] if lines else "(no error output captured)"


def sail_warnings(stderr: str) -> list[str]:
    """Distinct Sail WARN messages, e.g. "... is not yet supported ... no-op"."""
    messages = {
        line.split("] ", 1)[1].strip()
        for line in stderr.splitlines()
        if " WARN " in line and "sail" in line and "] " in line
    }
    return sorted(messages)


def run_example(example: Path, timeout: float) -> tuple[str, str]:
    """Run one example against a private Sail server.

    Returns ``(status, detail)`` where status is ``"ok"``, ``"warn"``, or
    ``"fail"``; detail is the warning text (warn) or the error line (fail).
    """
    preamble = RUNNER_PREAMBLE.format(examples=str(EXAMPLES_DIR))
    try:
        result = subprocess.run(
            [sys.executable, "-c", preamble, str(example)],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return "fail", f"timed out after {timeout:.0f}s"
    if result.returncode != 0:
        return "fail", failure_reason(result.stderr)
    warnings = sail_warnings(result.stderr)
    if warnings:
        return "warn", "; ".join(warnings)
    return "ok", ""


# Fixed-width text badges rendered as bold labels on a coloured background —
# ASCII only (no emoji, which show as missing-glyph boxes in asciinema and many
# terminals). Foreground/background SGR codes: black-on-green, black-on-yellow,
# white-on-red.
_LABELS = {"ok": "succ", "warn": "warn", "fail": "fail"}
_STYLES = {"ok": "30;42", "warn": "30;43", "fail": "97;41"}


def mark(status: str) -> str:
    label = _LABELS[status]
    use_color = sys.stdout.isatty() and "NO_COLOR" not in os.environ
    if use_color:
        return f"\033[1;{_STYLES[status]}m {label} \033[0m"
    return f"[{label}]"


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

    examples = numbered_examples()
    name_width = max(len(e.name) for e in examples)
    print(f"Running {len(examples)} examples against Sail\n")

    counts = {"ok": 0, "warn": 0, "fail": 0}
    warned: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []
    for example in examples:
        status, detail = run_example(example, args.timeout)
        counts[status] += 1
        suffix = f"  {detail}" if detail else ""
        print(f"  {mark(status)} {example.name:<{name_width}}{suffix}")
        if status == "warn":
            warned.append((example.name, detail))
        elif status == "fail":
            failed.append((example.name, detail))

    total = len(examples)
    print(
        f"\n{counts['ok']} clean, {counts['warn']} with no-op warnings, "
        f"{counts['fail']} incompatible (of {total})."
    )
    if warned:
        print("\nRan with unsupported no-ops:")
        for name, detail in warned:
            print(f"  - {name}: {detail}")
    if failed:
        print("\nIncompatible:")
        for name, detail in failed:
            print(f"  - {name}: {detail}")

    if args.fail_on_error and failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
