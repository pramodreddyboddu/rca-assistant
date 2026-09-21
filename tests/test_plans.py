"""Tests for multi-step remediation plans: the plan runner contract.

Uses a real ApprovalGate plus in-memory fakes (no estate needed): stub
privileged/verify callables, and a stub audit log. Engine integration tests
at the bottom use the real sim estate for inject -> diagnose -> propose_plan.
"""

import pytest

from agent import (ApprovalGate, PlanStep, RCAEngine, RemediationPlan,
                   VerifySpec, check_verify, run_plan)
from agent.plans import PlanResult
from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from sim import Estate, inject

READ = "demo-read-token-0001"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeAudit:
    """In-memory audit log: append() only, like the real AuditLog."""

    def __init__(self):
        self._entries = []

    def append(self, event, actor, details):
        self._entries.append({"event": event, "actor": actor,
                              "details": details})

    def events(self):
        return [e["event"] for e in self._entries]


def _two_step_plan():
    return RemediationPlan(
        hypothesis_id="h1",
        title="test plan",
        steps=(
            PlanStep(id="step-1", action="tool_one", args={"x": 1},
                     rationale="first step",
                     verify=VerifySpec(tool="read_one", args={"x": 1},
                                       expect={"ok": True})),
            PlanStep(id="step-2", action="tool_two", args={"y": 2},
                     rationale="second step",
                     verify=VerifySpec(tool="read_two", args={"y": 2},
                                       expect={"ok": True})),
        ),
    )


@pytest.fixture()
def harness():
    """Real gate, fake audit, recording privileged stub, matching verify stub."""
    audit = FakeAudit()
    gate = ApprovalGate(audit)
    privileged_calls = []

    def privileged_call(action, args):
        privileged_calls.append((action, args))
        return {"ran": action, "args": args}

    def verify_call(tool, args):
        return {"ok": True, "tool": tool}

    return gate, audit, privileged_calls, privileged_call, verify_call


def _run(harness, plan, decide, **kw):
    gate, audit, privileged_calls, privileged_call, verify_call = harness
    return run_plan(plan, gate=gate, privileged_call=privileged_call,
                    verify_call=verify_call, decide=decide, audit=audit,
                    **kw)


# ---------------------------------------------------------------------------
# Halt semantics
# ---------------------------------------------------------------------------

def test_plan_halts_on_rejection(harness):
    gate, audit, privileged_calls, _, _ = harness
    plan = _two_step_plan()
    decisions = {"step-1": "approve", "step-2": "reject"}
    result = _run(harness, plan, lambda step, req: decisions[step.id])
    assert isinstance(result, PlanResult)
    assert result.status == "halted_rejected"
    assert result.halted_at == "step-2"
    assert [c[0] for c in privileged_calls] == ["tool_one"]  # step 2 never ran
    assert [oc.step_id for oc in result.outcomes if oc.executed] == ["step-1"]
    halted = [e for e in audit._entries if e["event"] == "plan_halted"]
    assert len(halted) == 1
    assert halted[0]["details"]["reason"] == "rejected"
    assert halted[0]["details"]["at_step"] == "step-2"


def test_per_step_audit_entries(harness):
    gate, audit, privileged_calls, _, _ = harness
    result = _run(harness, _two_step_plan(),
                  lambda step, req: "approve")
    assert result.status == "completed"
    events = audit.events()
    assert events.count("plan_proposed") == 1
    assert events.count("approval_requested") == 2
    assert events.count("approval_decided") == 2
    assert events.count("plan_step_executed") == 2
    assert events.count("plan_step_verified") == 2
    assert events.count("plan_completed") == 1


def test_verify_failure_stops_plan(harness):
    gate, audit, privileged_calls, privileged_call, _ = harness

    def bad_verify(tool, args):
        return {"ok": False}  # never matches expect {"ok": True}

    result = run_plan(_two_step_plan(), gate=gate,
                      privileged_call=privileged_call, verify_call=bad_verify,
                      decide=lambda step, req: "approve", audit=audit)
    assert result.status == "halted_verify_failed"
    assert result.halted_at == "step-1"
    assert [c[0] for c in privileged_calls] == ["tool_one"]  # step 2 never ran
    assert "plan_step_verify_failed" in audit.events()
    halted = [e for e in audit._entries if e["event"] == "plan_halted"]
    assert halted[0]["details"]["reason"] == "verify_failed"


# ---------------------------------------------------------------------------
# approve_all and gate ordering
# ---------------------------------------------------------------------------

def test_approve_all_is_separately_logged(harness):
    gate, audit, privileged_calls, _, _ = harness
    result = _run(harness, _two_step_plan(),
                  lambda step, req: "approve_all")
    assert result.status == "completed"
    assert all(oc.decision == "approve_all" for oc in result.outcomes)
    events = audit.events()
    assert "approval_decided_all" in events
    assert "approval_decided" not in events  # distinct from per-step approval
    assert [c[0] for c in privileged_calls] == ["tool_one", "tool_two"]
    assert events.count("plan_step_verified") == 2
    assert events.count("plan_completed") == 1


def test_approval_bypass_denied_and_replay_denied(harness):
    gate, audit, privileged_calls, privileged_call, _ = harness
    plan = _two_step_plan()
    requests = gate.request_plan(plan)
    # Approve step 2 while step 1 is still PENDING.
    gate.decide(requests[1]["id"], True, decided_by="human")
    with pytest.raises(PermissionError):
        gate.execute(requests[1]["id"], privileged_call, "tool_two", {"y": 2})
    # Step 1 was never approved: default deny.
    with pytest.raises(PermissionError):
        gate.execute(requests[0]["id"], privileged_call, "tool_one", {"x": 1})
    gate.decide(requests[0]["id"], True, decided_by="human")
    gate.execute(requests[0]["id"], privileged_call, "tool_one", {"x": 1})
    # Replay the same approved request: denied.
    with pytest.raises(PermissionError):
        gate.execute(requests[0]["id"], privileged_call, "tool_one", {"x": 1})
    assert [c[0] for c in privileged_calls] == ["tool_one"]


def test_invalid_decide_choice_raises(harness):
    with pytest.raises(ValueError, match="decide"):
        _run(harness, _two_step_plan(), lambda step, req: "maybe")


# ---------------------------------------------------------------------------
# check_verify
# ---------------------------------------------------------------------------

def _spec(expect):
    return VerifySpec(tool="read_x", args={}, expect=expect)


def test_check_verify_equality():
    ok, detail = check_verify(_spec({"status": "RUNNING"}),
                              lambda tool, args: {"status": "RUNNING"})
    assert ok is True
    assert detail["result"]["status"] == "RUNNING"


def test_check_verify_operators():
    ok, _ = check_verify(_spec({"disk_pct": {"$lt": 90}}),
                         lambda tool, args: {"disk_pct": 50})
    assert ok is True
    ok, _ = check_verify(_spec({"mem_pct": {"$gte": 85}}),
                         lambda tool, args: {"mem_pct": 80})
    assert ok is False


def test_check_verify_tool_exception_returns_false():
    def boom(tool, args):
        raise RuntimeError("tool exploded")
    ok, detail = check_verify(_spec({"status": "RUNNING"}), boom)
    assert ok is False
    assert "error" in detail


def test_check_verify_unknown_operator_raises():
    with pytest.raises(ValueError, match="unknown verify operator"):
        check_verify(_spec({"x": {"$bogus": 1}}),
                     lambda tool, args: {"x": 1})


# ---------------------------------------------------------------------------
# Engine integration: inject -> diagnose -> propose_plan
# ---------------------------------------------------------------------------

def _engine_rig(tmp_path):
    estate = Estate(seed=42)
    audit = AuditLog(tmp_path / "audit.jsonl")
    gateway = Gateway(estate, audit)
    client = InProcessClient(gateway, READ)
    return estate, RCAEngine(client, audit), audit


def test_propose_plan_channel_stopped(tmp_path):
    estate, engine, _ = _engine_rig(tmp_path)
    alert = inject(estate, "channel_stopped")
    diagnosis = engine.diagnose(alert)
    plan = engine.propose_plan(diagnosis.top)
    assert plan is not None
    assert len(plan.steps) == 1
    assert plan.steps[0].action == "restart_channel"


def test_propose_plan_disk_full_two_steps(tmp_path):
    estate, engine, _ = _engine_rig(tmp_path)
    alert = inject(estate, "disk_full")
    diagnosis = engine.diagnose(alert)
    plan = engine.propose_plan(diagnosis.top)
    assert plan is not None
    assert [s.action for s in plan.steps] == ["archive_logs", "restart_app"]


def test_propose_plan_none_when_no_remediation(tmp_path):
    estate, engine, _ = _engine_rig(tmp_path)
    alert = inject(estate, "channel_stopped")
    diagnosis = engine.diagnose(alert)
    h2 = next(h for h in diagnosis.hypotheses if h.id == "h2")
    assert h2.plan is None
    assert engine.propose_plan(h2) is None


def test_diagnosis_audit_details_extended(tmp_path):
    estate, engine, audit = _engine_rig(tmp_path)
    alert = inject(estate, "channel_stopped")
    diagnosis = engine.diagnose(alert)
    done = [e for e in audit.entries()
            if e["event"] == "diagnosis_complete"]
    assert len(done) == 1
    details = done[0]["details"]
    assert details["top_hypothesis"] == "h1"
    assert details["top_title"] == diagnosis.top.title
    assert len(details["evidence"]) == len(diagnosis.top.evidence)
    for item in details["evidence"]:
        assert set(item) == {"claim", "tool", "excerpt"}
