"""CLI: render a markdown incident report from a run's audit trail.

Run:  python -m demo.report runs/<dirname> [--out report.md]

Prints the report to stdout unless --out is given (then it writes the file
and prints its path). If the audit chain is INVALID, a warning is printed
to stderr but the report is still generated: the invalidity itself is
evidence.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from agent.report import generate_markdown
from audit import AuditLog


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Render a markdown incident report from a run's audit trail.")
    parser.add_argument("run_dir", help="Run directory containing audit.jsonl "
                                       "(e.g. runs/20260920T170232Z)")
    parser.add_argument("--out", default=None,
                        help="Write the report to this file instead of stdout")
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir)
    audit_path = run_dir / "audit.jsonl"

    try:
        if audit_path.exists():
            ok, msg = AuditLog(audit_path).verify()
        else:
            ok, msg = False, f"audit log not found: {audit_path}"
    except Exception as exc:  # verify() promises not to raise; belt and braces
        ok, msg = False, f"verify() raised: {exc}"

    if not ok:
        print(f"WARNING: audit chain INVALID for run '{run_dir.name}': {msg}",
              file=sys.stderr)

    report = generate_markdown(run_dir)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(report, encoding="utf-8")
        print(str(out_path))
    else:
        print(report, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
