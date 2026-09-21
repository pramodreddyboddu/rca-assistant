"""End-to-end reference demo: inject incident -> RCA -> plan -> approve -> fix -> verify.

Run:  python -m demo.run_incident [--scenario channel_stopped] [--auto-approve]

Flow:
  1. Build the simulated estate and inject an incident (default: receiver
     channel stopped -> queue backlog on PAYMENTS.IN).
  2. The deterministic RCA engine gathers evidence through read-only MCP
     tools and proposes the top hypothesis WITH cited evidence.
  3. The engine proposes a multi-step remediation plan (DATA only).
  4. The plan runs through the approval gate: a human approves each step
     (or rejects, which halts the plan, or approves all remaining steps).
     Default is DENY: nothing privileged happens unless explicitly approved.
  5. Each executed step is re-verified with read-only tools; after a
     completed plan the estate ticks forward and the plan's own verify
     specs are re-run to confirm recovery. The audit trail summary is
     printed at the end.

Every step is written to a hash-chained audit log under runs/.
"""

from __future__ import annotations

import argparse
import secrets
from datetime import datetime, timezone
from pathlib import Path

from agent import ApprovalGate, RCAEngine, check_verify, run_plan
from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from mcp_server.auth import SCOPES_ADMIN, TOKENS
from sim import SCENARIOS, Estate, inject

# Demo-only bearer tokens (see mcp_server/auth.py). Production must use a
# real identity provider and a secret manager; never hard-code tokens.
_READ_TOKEN = next(t for t, s in TOKENS.items() if s != SCOPES_ADMIN)
_ADMIN_TOKEN = next(t for t, s in TOKENS.items() if s == SCOPES_ADMIN)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="RCA assistant reference demo: incident -> diagnosis -> "
                    "approved remediation plan -> verified recovery."
    )
    p.add_argument("--scenario", default="channel_stopped", choices=SCENARIOS,
                   help="which incident to inject (default: channel_stopped)")
    p.add_argument("--auto-approve", action="store_true", default=False,
                   help="DEMO ONLY: approve all plan steps without prompting. "
                        "Defaults to OFF; the gate denies by default.")
    p.add_argument("--seed", type=int, default=42,
                   help="simulation seed for reproducibility")
    return p


def _banner(text: str) -> None:
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    run_dir = Path("runs") / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-" + secrets.token_hex(4)
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    audit_path = run_dir / "audit.jsonl"

    estate = Estate(seed=args.seed)
    audit = AuditLog(audit_path)
    gateway = Gateway(estate, audit)
    read_client = InProcessClient(gateway, _READ_TOKEN)
    admin_client = InProcessClient(gateway, _ADMIN_TOKEN)
    engine = RCAEngine(read_client, audit)
    gate = ApprovalGate(audit)

    _banner("RCA ASSISTANT - REFERENCE DEMO")
    print(f"Scenario : {args.scenario}")
    print(f"Audit log: {audit_path}")

    # 1. Inject the incident.
    _banner("[1/5] INCIDENT INJECTED")
    alert = inject(estate, args.scenario)
    audit.append("incident_injected", actor="demo",
                 details={"scenario": args.scenario, "alert": alert})
    print(f"ALERT: {alert['type']}")
    for k, v in alert.items():
        if k != "type":
            print(f"  {k}: {v}")

    # 2. Run the deterministic RCA engine (read-only tools only).
    _banner("[2/5] GATHERING EVIDENCE + DIAGNOSIS (read-only)")
    diagnosis = engine.diagnose(alert)
    print(f"Evidence calls made: {len(diagnosis.evidence_calls)}")
    print("\nHypotheses ranked:")
    for h in diagnosis.hypotheses:
        marker = " <-- TOP" if h is diagnosis.top else ""
        print(f"  [{h.id}] {h.title}  score={h.score:.2f}{marker}")

    top = diagnosis.top
    print(f"\nTop hypothesis: {top.title} (score {top.score:.2f})")
    print("Cited evidence:")
    for c in top.evidence:
        print(f"  - claim : {c.claim}")
        print(f"    via   : {c.tool}")
        print(f"    excerpt: {c.excerpt[:150]}")

    # 3. Propose the remediation plan (pure data).
    _banner("[3/5] PROPOSED REMEDIATION PLAN")
    plan = engine.propose_plan(top)
    if plan is None:
        print("No remediation proposed by the engine for this hypothesis.")
        print("Manual investigation required; nothing to approve.")
    else:
        print(f"Plan: {plan.title} (hypothesis {plan.hypothesis_id})")
        for step in plan.steps:
            print(f"  [{step.id}] {step.action} {step.args}")
            print(f"    rationale: {step.rationale}")
            print(f"    scope    : {step.required_scope}")
            if step.verify is not None:
                print(f"    verify   : {step.verify.tool} "
                      f"{step.verify.args} expect {step.verify.expect}")

    # 4. Run the plan through the approval gate.
    _banner("[4/5] RUNNING PLAN (approval per step)")
    result = None
    if plan is None:
        print("Skipped: no plan proposed.")
    else:
        if args.auto_approve:
            print("--auto-approve was passed: approve-all without prompting "
                  "(demo only; the flag defaults to OFF).")
            actor = "auto-approve-flag"

            def decide_fn(step, request):
                print(f"  {step.id}: {step.action} — auto-approved "
                      "(--auto-approve)")
                return "approve_all"
        else:
            actor = "human"

            def decide_fn(step, request):
                answer = input(
                    f"\nStep {step.id}: {step.action} {step.args} "
                    "— [y] approve / [n] reject / [a] approve all remaining: "
                ).strip().lower()
                if answer in ("y", "yes"):
                    return "approve"
                if answer in ("a", "all"):
                    return "approve_all"
                # default on empty/invalid: deny
                return "reject"

        result = run_plan(plan, gate=gate,
                          privileged_call=admin_client.call_tool,
                          verify_call=read_client.call_tool,
                          decide=decide_fn, audit=audit, actor=actor)

    # 5. Report the outcome; on a completed plan, tick and re-verify.
    _banner("[5/5] PLAN OUTCOME")
    if result is None:
        print("No plan ran. The incident remains open; manual "
              "investigation required.")
    else:
        print(f"Plan status: {result.status}")
        for oc in result.outcomes:
            print(f"  {oc.step_id} {oc.action}: decision={oc.decision} "
                  f"executed={oc.executed} verify_ok={oc.verify_ok}")

        if result.status == "halted_rejected":
            n_exec = sum(1 for oc in result.outcomes if oc.executed)
            print(f"\nThe plan was REJECTED at step {result.halted_at}.")
            if n_exec == 0:
                print("No privileged tool was invoked.")
            else:
                print(f"{n_exec} step(s) already executed; no further "
                      "privileged tools were invoked.")
            print("The incident remains open in the simulation.")
        elif result.status == "halted_verify_failed":
            print(f"\nVerify FAILED at step {result.halted_at} — the plan "
                  "halted with the incident left in its current state.")
        elif result.status == "halted_error":
            print(f"\nA step FAILED at {result.halted_at} — the plan halted.")
        elif result.status == "completed":
            print("\nAll steps approved, executed, and verified. "
                  "Ticking the estate to confirm recovery...")
            for _ in range(40):
                estate.tick(60)
            all_ok = True
            for step in plan.steps:
                if step.verify is None:
                    continue
                ok, _detail = check_verify(step.verify,
                                           read_client.call_tool)
                all_ok = all_ok and ok
                print(f"  post-run verify {step.id} "
                      f"({step.verify.tool} {step.verify.args}): "
                      f"{'OK' if ok else 'NOT OK'}")
            if all_ok:
                print("Recovery confirmed: all post-run verifications passed.")
            else:
                print("WARNING: some post-run verifications did not pass; "
                      "investigate.")

    # Audit summary.
    _banner("AUDIT TRAIL")
    summary = audit.summary()
    ok, msg = audit.verify()
    print(f"Entries : {summary['entries']}")
    print(f"Events  : {summary['events']}")
    print(f"Chain   : {'VALID' if ok else 'INVALID'} ({msg})")
    print(f"Log file: {audit_path}")
    print("\nDemo complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
