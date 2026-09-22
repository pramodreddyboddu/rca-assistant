"""Emit pytest failure details as a GitHub Actions ::error:: annotation.

The CI pytest step hides its log behind sign-in, so on failure this script
extracts the failure sections from a captured pytest output log and prints a
single URL-encoded ``::error::`` workflow command. The resulting annotation is
readable through the check-runs API without any special access.

Usage: python .github/workflows/annotate_pytest_failures.py <pytest-output-log>
"""

import re
import sys
import urllib.parse

# Stay well under the 64 KiB per-annotation limit.
MAX_CHARS = 30000


def extract_failure_details(log: str) -> str:
    """Return the FAILURES section (tracebacks + summary) if present."""
    match = re.search(r"^=+ FAILURES =+.*", log, re.M | re.S)
    if match:
        return match.group(0)
    match = re.search(r"^(ERRORS|short test summary info)\n.*", log, re.M | re.S)
    if match:
        return match.group(0)
    # Collection error / crash: fall back to the tail of the log.
    return log


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <pytest-output-log>", file=sys.stderr)
        return 2
    with open(sys.argv[1], "r", errors="replace") as fh:
        log = fh.read()
    body = extract_failure_details(log).strip()
    body = body[-MAX_CHARS:]
    message = "pytest failures (CI diagnostic):\n" + body
    # Workflow commands require URL-encoding: %0A for newlines, etc.
    encoded = urllib.parse.quote(message, safe="")
    print(f"::error::{encoded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
