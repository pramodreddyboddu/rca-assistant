"""Polished command-line interface for the RCA assistant.

Subcommands (all imports are lazy inside the command functions so
``rca --help`` stays instant):

    rca demo                 one-command end-to-end incident run
    rca diagnose             evidence gathering + diagnosis only (no fixes)
    rca connectors           list connectors / run connector self-tests
    rca serve                start the interactive web UI
    rca incident             incident room: one real incident, replayed
                             evidence, rehearsed approval (web UI)
    rca scenarios            list the 45 simulated incident scenarios
    rca version              print the version

Every command returns a process exit code; ``main(argv)`` is the console
script entry point.
"""

from __future__ import annotations

import argparse
import sys

from rca_assistant import __version__

PROG = "rca"

_DEMO_NOTICE = (
    "NOTE: approvals are AUTO-APPROVED for this non-interactive demo run. "
    "For the interactive step-by-step approval flow, use `rca serve` "
    "(web UI) or `rca demo --interactive`."
)


def _rule(char: str = "=") -> None:
    print("\n" + char * 70)


# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------

def cmd_demo(args: argparse.Namespace) -> int:
    from demo.run_incident import main as demo_main

    _rule()
    print("RCA ASSISTANT - ONE-COMMAND DEMO")
    print(f"Scenario: {args.scenario}")
    if args.interactive:
        print("Mode    : interactive (you approve each remediation step)")
    else:
        print("Mode    : non-interactive (approvals auto-approved)")
        print()
        print(_DEMO_NOTICE)
    print("=" * 70)

    demo_argv = ["--scenario", args.scenario]
    if not args.interactive:
        demo_argv.append("--auto-approve")
    rc = demo_main(demo_argv)

    _rule()
    print("Demo run finished.")
    if rc == 0 and not args.interactive:
        print("The incident was diagnosed, the remediation plan was "
              "auto-approved,")
        print("executed, and recovery was re-verified. A hash-chained audit "
              "log was")
        print("written under runs/ in the current directory.")
    print("Next steps:")
    print("  rca diagnose --scenario disk_full   evidence + diagnosis, no fixes")
    print("  rca scenarios                       all 45 simulated incidents")
    print("  rca connectors list                 real-system connectors")
    print("  rca serve                           interactive approval web UI")
    return rc


# ---------------------------------------------------------------------------
# diagnose
# ---------------------------------------------------------------------------

def _check_scenario(name: str) -> "list[str] | None":
    """Return None when *name* is valid, else the list of valid scenarios."""
    from sim import SCENARIOS
    if name in SCENARIOS:
        return None
    return SCENARIOS


def cmd_diagnose(args: argparse.Namespace) -> int:
    valid = _check_scenario(args.scenario)
    if valid is not None:
        print(f"error: unknown scenario {args.scenario!r}", file=sys.stderr)
        print("Valid scenarios:", file=sys.stderr)
        for name in valid:
            print(f"  {name}", file=sys.stderr)
        return 2

    import tempfile
    from pathlib import Path

    from agent import RCAEngine
    from audit import AuditLog
    from mcp_server import Gateway, InProcessClient
    from mcp_server.auth import SCOPES_ADMIN, TOKENS
    from sim import Estate, inject

    # Same read-only plumbing as demo/run_incident.py, minus the plan and
    # the approval gate: this command never proposes or executes fixes.
    _read_token = next(t for t, s in TOKENS.items() if s != SCOPES_ADMIN)

    _rule()
    print("RCA DIAGNOSIS (read-only: evidence gathering + diagnosis only)")
    print(f"Scenario: {args.scenario}")
    print("=" * 70)

    tmp = tempfile.TemporaryDirectory(prefix="rca-diagnose-")
    audit = AuditLog(Path(tmp.name) / "audit.jsonl")
    estate = Estate(seed=42)
    gateway = Gateway(estate, audit)
    read_client = InProcessClient(gateway, _read_token)
    engine = RCAEngine(read_client, audit)

    alert = inject(estate, args.scenario)
    print(f"\nALERT: {alert['type']}")
    for k, v in alert.items():
        if k != "type":
            print(f"  {k}: {v}")

    diagnosis = engine.diagnose(alert)
    top = diagnosis.top
    n_others = len(diagnosis.hypotheses) - 1

    print(f"\nEvidence calls made: {len(diagnosis.evidence_calls)}")
    print(f"\nTop hypothesis: [{top.id}] {top.title}")
    print(f"  score: {top.score:.2f}"
          + (f"  ({n_others} other {'hypothesis' if n_others == 1 else 'hypotheses'}"
             f" ranked below)" if n_others else ""))
    print("  cited evidence:")
    for c in top.evidence:
        print(f"    - claim  : {c.claim}")
        print(f"      via    : {c.tool}")
        print(f"      excerpt: {c.excerpt[:150]}")

    _rule()
    print("Diagnosis complete. No remediation was proposed or executed.")
    print("To see the full flow with an approved plan, run:")
    print(f"  rca demo --scenario {args.scenario}")
    return 0


# ---------------------------------------------------------------------------
# connectors
# ---------------------------------------------------------------------------

def cmd_connectors_list(args: argparse.Namespace) -> int:
    import connectors
    from connectors.base import CONNECTORS

    items = sorted(CONNECTORS.items(), key=lambda kv: kv[0])
    width = max(len(name) for name, _ in items)
    print(f"Registered connectors ({len(items)}):\n")
    for name, (spec, _factory) in items:
        first_line = spec.description.splitlines()[0] if spec.description else ""
        print(f"  {name:<{width}}  {first_line}")
    print(f"\n{len(items)} connectors registered.")
    return 0


def cmd_connectors_self_test(args: argparse.Namespace) -> int:
    from connectors.harness import main as harness_main

    argv: list[str] = []
    if args.connector:
        argv += ["--connector", args.connector]
    if args.json:
        argv.append("--json")
    return harness_main(argv)


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------

def cmd_serve(args: argparse.Namespace) -> int:
    from demo.web import main as web_main

    argv: list[str] = []
    if args.port is not None:
        argv += ["--port", str(args.port)]
    if args.bind is not None:
        argv += ["--bind", args.bind]
    print("Starting the RCA assistant web UI "
          "(interactive approval happens here)...")
    return web_main(argv)


# ---------------------------------------------------------------------------
# incident
# ---------------------------------------------------------------------------

def cmd_incident(args: argparse.Namespace) -> int:
    from demo.web import main as web_main

    argv = ["--incident-room"]
    if args.port is not None:
        argv += ["--port", str(args.port)]
    if args.bind is not None:
        argv += ["--bind", args.bind]
    print("Starting the incident room "
          "(one real incident, replayed evidence, rehearsed approval)...")
    return web_main(argv)


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------

def cmd_scenarios(args: argparse.Namespace) -> int:
    from sim import SCENARIOS

    for name in sorted(SCENARIOS):
        print(name)
    print(f"\n{len(SCENARIOS)} scenarios. Try one:")
    print("  rca demo --scenario disk_full")
    return 0


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------

def cmd_version(args: argparse.Namespace) -> int:
    print(f"rca-assistant {__version__}")
    return 0


# ---------------------------------------------------------------------------
# parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "RCA assistant: root-cause analysis for middleware/infra ops.\n\n"
            "The 60-second path:\n"
            "  rca demo            inject an incident, diagnose it, approve\n"
            "                      and execute the fix, verify recovery.\n"
            "  rca serve           interactive web UI with per-step approval."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  rca demo --scenario kafka_lag\n"
            "  rca diagnose --scenario tomcat_oom\n"
            "  rca connectors self-test --connector linux_host\n"
            "  rca serve --port 8765\n"
            "  rca incident --port 8766\n"
        ),
    )
    sub = p.add_subparsers(dest="command", metavar="<command>")

    d = sub.add_parser("demo", help="one-command end-to-end incident demo")
    d.add_argument("--scenario", default="channel_stopped", metavar="NAME",
                   help="which incident to inject "
                        "(default: channel_stopped; see `rca scenarios`)")
    d.add_argument("--interactive", action="store_true",
                   help="prompt for approval of each remediation step "
                        "instead of auto-approving")
    d.set_defaults(func=cmd_demo)

    g = sub.add_parser("diagnose",
                       help="evidence gathering + diagnosis only (no fixes)")
    g.add_argument("--scenario", default="channel_stopped", metavar="NAME",
                   help="which incident to inject "
                        "(default: channel_stopped; see `rca scenarios`)")
    g.set_defaults(func=cmd_diagnose)

    c = sub.add_parser("connectors", help="connector registry and self-tests")
    csub = c.add_subparsers(dest="connectors_command", metavar="<command>")
    cl = csub.add_parser("list", help="list all registered connectors")
    cl.set_defaults(func=cmd_connectors_list)
    ct = csub.add_parser("self-test",
                         help="run connector contract self-tests "
                              "(static checks, no live targets)")
    ct.add_argument("--connector", default=None, metavar="NAME",
                    help="test one connector only (see `rca connectors list`)")
    ct.add_argument("--json", action="store_true",
                    help="emit machine-readable JSON results")
    ct.set_defaults(func=cmd_connectors_self_test)
    c.set_defaults(func=lambda args: (c.print_help(), 0)[1])

    s = sub.add_parser("serve", help="start the interactive web UI")
    s.add_argument("--port", type=int, default=None, metavar="N",
                   help="local port (default: 8765)")
    s.add_argument("--bind", default=None, metavar="ADDR",
                   help="bind address (default: 127.0.0.1)")
    s.set_defaults(func=cmd_serve)

    i = sub.add_parser(
        "incident",
        help="incident room: one real incident through the real pipeline "
             "over replayed fixtures, with rehearsed approval")
    i.add_argument("--port", type=int, default=None, metavar="N",
                   help="local port (default: 8765)")
    i.add_argument("--bind", default=None, metavar="ADDR",
                   help="bind address (default: 127.0.0.1)")
    i.set_defaults(func=cmd_incident)

    sc = sub.add_parser("scenarios", help="list simulated incident scenarios")
    sc.set_defaults(func=cmd_scenarios)

    v = sub.add_parser("version", help="print the version and exit")
    v.set_defaults(func=cmd_version)

    return p


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 0
    return func(args)


if __name__ == "__main__":
    raise SystemExit(main())
