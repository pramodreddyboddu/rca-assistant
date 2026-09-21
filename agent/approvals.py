"""Human approval gate for privileged actions. Default: deny.

A privileged action runs only if a human (or an explicitly named decider) has
approved its request record. The engine in agent.rca produces remediation
dicts as DATA; this gate is the only path that may turn one into execution.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any, Callable


class ApprovalGate:
    """Tracks approval requests and enforces default-deny execution."""

    def __init__(self, audit):
        self._audit = audit
        self._requests: dict[str, dict] = {}

    def request(self, action: str, args: dict, rationale: str) -> dict:
        """Record a new approval request; status starts PENDING."""
        record = {
            "id": "APR-" + secrets.token_hex(4),
            "action": action,
            "args": dict(args),
            "rationale": rationale,
            "status": "PENDING",
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        self._requests[record["id"]] = record
        self._audit.append("approval_requested", "approval-gate",
                           {"request_id": record["id"], "action": action})
        return record

    def decide(self, request_id: str, approved: bool,
               decided_by: str = "human") -> dict:
        """Resolve a PENDING request to APPROVED or DENIED."""
        record = self._requests.get(request_id)
        if record is None:
            raise KeyError(f"unknown approval request: {request_id}")
        if record["status"] != "PENDING":
            raise ValueError(
                f"request {request_id} already decided ({record['status']})")
        record["status"] = "APPROVED" if approved else "DENIED"
        self._audit.append("approval_decided", "approval-gate",
                           {"request_id": request_id, "approved": approved,
                            "decided_by": decided_by})
        return record

    def request_plan(self, plan) -> list[dict]:
        """Create one linked PENDING request per plan step, in order.

        `plan` is an agent.plans.RemediationPlan (duck-typed: .hypothesis_id,
        .title, .steps with .id/.action/.args/.rationale). Every request
        carries plan_id + step_index so execute() can enforce plan order.
        """
        plan_id = "PLAN-" + secrets.token_hex(4)
        self._audit.append(
            "plan_proposed", "approval-gate",
            {"plan_id": plan_id, "hypothesis_id": plan.hypothesis_id,
             "title": plan.title,
             "steps": [{"id": s.id, "action": s.action, "args": dict(s.args)}
                       for s in plan.steps]})
        requests = []
        for i, step in enumerate(plan.steps):
            record = {
                "id": "APR-" + secrets.token_hex(4),
                "plan_id": plan_id,
                "step_index": i,
                "step_id": step.id,
                "action": step.action,
                "args": dict(step.args),
                "rationale": step.rationale,
                "status": "PENDING",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            self._requests[record["id"]] = record
            requests.append(record)
            self._audit.append(
                "approval_requested", "approval-gate",
                {"request_id": record["id"], "plan_id": plan_id,
                 "step_id": step.id, "action": step.action})
        return requests

    def approve_all(self, plan_id: str, decided_by: str = "human") -> list[dict]:
        """Approve every still-PENDING request of a plan, explicitly.

        This is a deliberate bulk choice (the UI/CLI offers it as a
        separate option); it is audited as its own event so an auditor can
        distinguish step-by-step approval from approve-all.
        """
        targets = [r for r in self._requests.values()
                   if r.get("plan_id") == plan_id and r["status"] == "PENDING"]
        if not targets:
            raise ValueError(f"no pending requests for plan {plan_id}")
        for record in targets:
            record["status"] = "APPROVED"
        self._audit.append(
            "approval_decided_all", "approval-gate",
            {"plan_id": plan_id, "decided_by": decided_by,
             "approved": [r["id"] for r in targets]})
        return targets

    def execute(self, request_id: str, fn: Callable, *args, **kwargs) -> Any:
        """Run fn only with a live APPROVED grant. Default: deny.

        Plan order is enforced: a step of a plan executes only when every
        earlier step of the same plan has status EXECUTED. Approving step 2
        while step 1 is still pending does NOT let step 2 run.
        """
        record = self._requests.get(request_id)
        if record is not None and record["status"] == "EXECUTED":
            raise PermissionError(f"already executed: {request_id}")
        if record is None or record["status"] != "APPROVED":
            self._audit.append("approval_denied_execution", "approval-gate",
                               {"request_id": request_id})
            raise PermissionError(
                f"default deny: no granted approval for {request_id}")
        plan_id = record.get("plan_id")
        if plan_id is not None:
            step_index = record.get("step_index", 0)
            for other in self._requests.values():
                if (other.get("plan_id") == plan_id
                        and other.get("step_index", 0) < step_index
                        and other["status"] != "EXECUTED"):
                    self._audit.append(
                        "approval_denied_execution", "approval-gate",
                        {"request_id": request_id, "plan_id": plan_id,
                         "reason": "plan_order",
                         "blocked_by": other["id"]})
                    raise PermissionError(
                        f"plan order: step {step_index} blocked until step "
                        f"{other.get('step_index')} is executed")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            record["status"] = "FAILED"
            self._audit.append("privileged_failed", "approval-gate",
                               {"request_id": request_id,
                                "action": record["action"]})
            raise
        record["status"] = "EXECUTED"
        self._audit.append("privileged_executed", "approval-gate",
                           {"request_id": request_id,
                            "action": record["action"]})
        return result
