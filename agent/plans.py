"""Multi-step remediation plans: ordered privileged steps, each approved and verified.

A RemediationPlan is DATA, exactly like the legacy single remediation dict:
the engine proposes it, the approval gate authorizes each step, and a runner
executes the steps in order with read-only re-verification between steps.

Contract for workstreams
------------------------
- agent/approvals.ApprovalGate MUST provide:
    request_plan(plan) -> list[request]   # one linked PENDING request per step
    approve_all(plan_id, decided_by) -> list[request]  # explicit bulk approval
    decide(request_id, approved, decided_by)           # existing, per step
    execute(request_id, fn, *args, **kwargs)          # existing + plan-order
      enforcement: a step executes only when every earlier step of the same
      plan has status EXECUTED.
- agent/remediation.py (scenario workstream) provides plan_fn helpers that
  build RemediationPlan objects from (alert, evidence).
- agent/rca.py exposes RCAEngine.propose_plan(hypothesis) -> RemediationPlan.
- Runners (demo/run_incident.py, demo/web.py) share run_plan() and
  execute_approved_step() here: no duplicated execution logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


# ---------------------------------------------------------------------------
# Data types (all JSON-serializable; safe to audit)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VerifySpec:
    """Read-only re-verification to run after a privileged step.

    `expect` is matched against the tool's result dict: a plain value means
    equality; a {"$lt"|"$lte"|"$gt"|"$gte"|"$eq": value} dict compares.
    """

    tool: str
    args: dict
    expect: dict


@dataclass(frozen=True)
class PlanStep:
    """One privileged step of a remediation plan."""

    id: str               # e.g. "step-1"; unique within the plan
    action: str           # privileged tool name, e.g. "restart_channel"
    args: dict            # tool arguments
    rationale: str        # human-readable why, shown at approval time
    verify: VerifySpec | None = None
    required_scope: str = "admin:write"


@dataclass(frozen=True)
class RemediationPlan:
    """Ordered remediation steps proposed for one hypothesis."""

    hypothesis_id: str
    title: str
    steps: tuple  # tuple[PlanStep, ...]; tuple keeps the dataclass frozen


@dataclass
class StepOutcome:
    """What happened for one step."""

    step_id: str
    action: str
    decision: str          # approved | rejected | approve_all
    executed: bool
    verify_ok: bool | None  # None when the step has no verify spec
    detail: dict = field(default_factory=dict)


@dataclass
class PlanResult:
    """Terminal state of a plan run."""

    plan_id: str
    status: str  # completed | halted_rejected | halted_verify_failed | halted_error
    outcomes: list  # list[StepOutcome]
    halted_at: str | None = None  # step id where the run stopped, if any


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

_OPERATORS = ("$lt", "$lte", "$gt", "$gte", "$eq")

_MISSING = object()


def _resolve_path(result: dict, key: str):
    """Resolve a dotted expect key against the tool result.

    Plain keys keep the exact historical behavior (top-level lookup).
    Dotted keys walk dicts and integer list indices, e.g.
    "pools.0.current_threads_busy". Missing paths return _MISSING, which
    never satisfies an expectation.
    """
    current = result
    for part in key.split("."):
        if isinstance(current, dict):
            current = current.get(part, _MISSING)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return _MISSING
        else:
            return _MISSING
        if current is _MISSING:
            return _MISSING
    return current


def _match_expect(result: dict, expect: dict) -> bool:
    """True when every expectation holds against the tool result."""
    for key, cond in expect.items():
        actual = _resolve_path(result, key)
        if isinstance(cond, dict):
            for op, val in cond.items():
                if op not in _OPERATORS:
                    raise ValueError(f"unknown verify operator: {op!r}")
                if not isinstance(actual, (int, float)) or isinstance(actual, bool):
                    return False
                if op == "$lt" and not actual < val:
                    return False
                if op == "$lte" and not actual <= val:
                    return False
                if op == "$gt" and not actual > val:
                    return False
                if op == "$gte" and not actual >= val:
                    return False
                if op == "$eq" and not actual == val:
                    return False
        elif actual != cond:
            return False
    return True


def check_verify(spec: VerifySpec,
                 verify_call: Callable[[str, dict], Any]) -> tuple[bool, dict]:
    """Run the read-only verify tool and match `expect`.

    Never raises on a mismatch or a tool error: returns (False, detail).
    Verification is evidence gathering, not execution, so a failure here
    must halt the plan loudly, never crash the runner.
    """
    try:
        result = verify_call(spec.tool, dict(spec.args))
    except Exception as exc:  # noqa: BLE001 - verification must not crash
        return False, {"error": f"{type(exc).__name__}: {exc}"}
    if not isinstance(result, dict):
        return False, {"error": "verify tool did not return a dict",
                       "result": str(result)[:200]}
    ok = _match_expect(result, spec.expect)
    return ok, {"result": result, "expect": spec.expect}


# ---------------------------------------------------------------------------
# Step execution (shared by the CLI runner and the web UI)
# ---------------------------------------------------------------------------

def execute_approved_step(*, gate: Any, request: dict, step: PlanStep,
                          privileged_call: Callable[[str, dict], Any],
                          verify_call: Callable[[str, dict], Any],
                          audit: Any) -> StepOutcome:
    """Execute one already-APPROVED step, then re-verify.

    `request` must be the gate's request record for this step (status
    APPROVED). The gate enforces default-deny and plan order; any denial
    surfaces as a non-executed outcome, never as an unhandled crash.
    """
    plan_id = request.get("plan_id")
    outcome = StepOutcome(step_id=step.id, action=step.action,
                          decision="approved", executed=False, verify_ok=None)
    try:
        result = gate.execute(request["id"], privileged_call,
                              step.action, step.args)
    except Exception as exc:  # noqa: BLE001 - denial/failure is an outcome
        audit.append("plan_step_failed", "plan-runner",
                     {"plan_id": plan_id, "step_id": step.id,
                      "error": f"{type(exc).__name__}: {exc}"})
        outcome.detail = {"error": str(exc)}
        return outcome

    outcome.executed = True
    outcome.detail = {"result": result}
    audit.append("plan_step_executed", "plan-runner",
                 {"plan_id": plan_id, "step_id": step.id,
                  "action": step.action, "request_id": request["id"]})

    if step.verify is None:
        return outcome
    ok, vdetail = check_verify(step.verify, verify_call)
    outcome.verify_ok = ok
    outcome.detail["verify"] = vdetail
    audit.append("plan_step_verified" if ok else "plan_step_verify_failed",
                 "plan-runner",
                 {"plan_id": plan_id, "step_id": step.id, "ok": ok,
                  "detail": vdetail})
    return outcome


# ---------------------------------------------------------------------------
# Plan runner (CLI; the web UI drives execute_approved_step per request)
# ---------------------------------------------------------------------------

_DECISIONS = ("approve", "reject", "approve_all")


def run_plan(plan: RemediationPlan, *, gate: Any,
             privileged_call: Callable[[str, dict], Any],
             verify_call: Callable[[str, dict], Any],
             decide: Callable[[PlanStep, dict], str],
             audit: Any, actor: str = "human") -> PlanResult:
    """Run a plan step by step: approve -> execute -> verify -> next.

    `decide(step, request)` returns "approve", "reject", or "approve_all".
    A rejection halts the plan immediately; "approve_all" is an explicit,
    separately-audited choice that approves every remaining step and then
    executes them in order, still verifying each one. Any verification
    failure halts the plan with the incident left in its current state.
    """
    requests = gate.request_plan(plan)
    plan_id = requests[0]["plan_id"]
    outcomes: list[StepOutcome] = []

    def _halt(status: str, at_step: str, reason: str) -> PlanResult:
        audit.append("plan_halted", "plan-runner",
                     {"plan_id": plan_id, "reason": reason, "at_step": at_step})
        return PlanResult(plan_id=plan_id, status=status,
                          outcomes=outcomes, halted_at=at_step)

    i = 0
    while i < len(plan.steps):
        step, req = plan.steps[i], requests[i]
        choice = decide(step, req)
        if choice not in _DECISIONS:
            raise ValueError(f"decide() returned {choice!r}; "
                             f"expected one of {_DECISIONS}")

        if choice == "reject":
            gate.decide(req["id"], False, decided_by=actor)
            outcomes.append(StepOutcome(step.id, step.action, "rejected",
                                        False, None))
            return _halt("halted_rejected", step.id, "rejected")

        if choice == "approve_all":
            gate.approve_all(plan_id, decided_by=actor)
            for j in range(i, len(plan.steps)):
                st, rq = plan.steps[j], requests[j]
                oc = execute_approved_step(
                    gate=gate, request=rq, step=st,
                    privileged_call=privileged_call, verify_call=verify_call,
                    audit=audit)
                oc.decision = "approve_all"
                outcomes.append(oc)
                if not oc.executed:
                    return _halt("halted_error", st.id, "execution_failed")
                if oc.verify_ok is False:
                    return _halt("halted_verify_failed", st.id,
                                 "verify_failed")
            audit.append("plan_completed", "plan-runner",
                         {"plan_id": plan_id, "steps": len(outcomes)})
            return PlanResult(plan_id=plan_id, status="completed",
                              outcomes=outcomes)

        gate.decide(req["id"], True, decided_by=actor)
        oc = execute_approved_step(
            gate=gate, request=req, step=step,
            privileged_call=privileged_call, verify_call=verify_call,
            audit=audit)
        outcomes.append(oc)
        if not oc.executed:
            return _halt("halted_error", step.id, "execution_failed")
        if oc.verify_ok is False:
            return _halt("halted_verify_failed", step.id, "verify_failed")
        i += 1

    audit.append("plan_completed", "plan-runner",
                 {"plan_id": plan_id, "steps": len(outcomes)})
    return PlanResult(plan_id=plan_id, status="completed", outcomes=outcomes)
