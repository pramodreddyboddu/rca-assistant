"""Sim-vs-real host metrics comparison through the same MCP authz/audit path.

Run:  python -m demo.host_compare [--log PATH]

Builds two gateways that share one audit log under runs/:

  SIM GATEWAY  - Estate (seeded simulation), include={"get_host_metrics"}
  REAL GATEWAY - LinuxHostConnector (reads the real /proc on this machine),
                 include={"get_host_metrics", "tail_log"}

Both gateways are driven with the demo read token, so token/scope checks
and audit entries are identical; the only difference is the "estate" behind
the tools. Prints a side-by-side table (left column SIMULATED, right column
REAL) and then tails a real allow-listed log.
"""

from __future__ import annotations

import argparse
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

from audit import AuditLog
from connectors.base import ConnectorError
from connectors.linux_host import LinuxHostConnector
from mcp_server import Gateway
from mcp_server.auth import SCOPES_ADMIN, TOKENS
from sim import Estate

# Demo-only bearer token (see mcp_server/auth.py). Production must use a
# real identity provider and a secret manager; never hard-code tokens.
_READ_TOKEN = next(t for t, s in TOKENS.items() if s != SCOPES_ADMIN)

_DEFAULT_LOGS = ("/var/log/syslog", "/var/log/messages")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Compare simulated vs real host metrics through the "
                    "same MCP gateway (authz + audit), then tail a real log."
    )
    p.add_argument(
        "--log",
        default=None,
        help="allow-listed log file to tail (default: first readable of "
             "/var/log/syslog, /var/log/messages)",
    )
    return p


def _banner(text: str) -> None:
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def _fmt(metrics: dict, label: str) -> list[str]:
    return [
        f"{label:<22}",
        f"  cpu_pct  {metrics['cpu_pct']:>6.1f} %",
        f"  mem_pct  {metrics['mem_pct']:>6.1f} %",
        f"  disk_pct {metrics['disk_pct']:>6.1f} %",
        f"  load1    {metrics['load1']:>6.2f}",
        f"  ts       {metrics['ts']}",
    ]


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    run_dir = Path("runs") / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-" + secrets.token_hex(4)
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    audit = AuditLog(run_dir / "audit.jsonl")

    sim_gateway = Gateway(Estate(seed=42), audit, include={"get_host_metrics"})
    connector = LinuxHostConnector()
    real_gateway = Gateway(
        connector, audit, include={"get_host_metrics", "tail_log"}
    )

    _banner("SIMULATED vs REAL HOST METRICS")
    print("Left column : SIMULATED - get_host_metrics from the seeded Estate.")
    print("Right column: REAL      - get_host_metrics from LinuxHostConnector")
    print("              (live /proc data on this machine). Both calls go")
    print("              through Gateway.call with the demo read token, so")
    print("              authz and audit entries are identical.")
    print(f"Audit log: {run_dir / 'audit.jsonl'}")

    sim = sim_gateway.call("get_host_metrics", {"host": "app01"}, _READ_TOKEN)
    real = real_gateway.call("get_host_metrics", {"host": "localhost"}, _READ_TOKEN)

    left = _fmt(sim, "SIM app01 (simulated)")
    right = _fmt(real, "REAL localhost (live)")
    print("\n" + "-" * 70)
    for l, r in zip(left, right):
        print(f"{l:<34} {r}")
    print("-" * 70)
    print("Note: the REAL column is this actual machine; the SIM column is")
    print("deterministic demo fiction (seed 42).")

    _banner("REAL LOG TAIL (allow-listed, read-only)")
    candidates = [args.log] if args.log else list(_DEFAULT_LOGS)
    for path in candidates:
        try:
            lines = real_gateway.call(
                "tail_log", {"path": path, "limit": 5}, _READ_TOKEN
            )
        except PermissionError as exc:
            # The sandbox refused: this is an audited tool_error, not a crash.
            print(f"tail_log denied for {path!r}: {exc}")
            return 0
        except ConnectorError:
            continue
        print(f"Last {len(lines)} lines of {path}:")
        for entry in lines:
            print(f"  [{entry['line_no']}] {entry['text']}")
        break
    else:
        print("no allow-listed log readable on this host")

    _banner("AUDIT TRAIL")
    summary = audit.summary()
    ok, msg = audit.verify()
    print(f"Entries : {summary['entries']}")
    print(f"Events  : {summary['events']}")
    print(f"Chain   : {'VALID' if ok else 'INVALID'} ({msg})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
