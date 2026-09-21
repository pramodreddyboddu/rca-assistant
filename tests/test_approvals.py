"""Tests for the default-deny approval gate."""

import pytest

from agent import ApprovalGate
from audit import AuditLog


@pytest.fixture()
def gate(tmp_path):
    return ApprovalGate(AuditLog(tmp_path / "audit.jsonl"))


def test_request_starts_pending(gate):
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    assert req["status"] == "PENDING"
    assert req["id"].startswith("APR-")


def test_default_deny_without_decision(gate):
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    with pytest.raises(PermissionError, match="default deny"):
        gate.execute(req["id"], lambda: "should-not-run")


def test_deny_decision_blocks_execution(gate):
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    gate.decide(req["id"], False)
    with pytest.raises(PermissionError, match="default deny"):
        gate.execute(req["id"], lambda: "should-not-run")


def test_unknown_request_id_blocked(gate):
    with pytest.raises(Exception):
        gate.execute("APR-deadbeef", lambda: "should-not-run")


def test_approve_then_execute_runs_once(gate):
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    gate.decide(req["id"], True)
    assert gate.execute(req["id"], lambda: "ran") == "ran"
    with pytest.raises(PermissionError, match="already executed"):
        gate.execute(req["id"], lambda: "ran-again")


def test_double_decide_rejected(gate):
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    gate.decide(req["id"], True)
    with pytest.raises(ValueError):
        gate.decide(req["id"], False)


def test_failing_action_marks_failed_and_raises(gate):
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    gate.decide(req["id"], True)

    def boom():
        raise RuntimeError("tool exploded")

    with pytest.raises(RuntimeError):
        gate.execute(req["id"], boom)


def test_approval_flow_is_audited(tmp_path):
    audit = AuditLog(tmp_path / "audit.jsonl")
    gate = ApprovalGate(audit)
    req = gate.request("restart_channel", {"qmgr": "QMGR1", "channel": "X"}, "why")
    gate.decide(req["id"], True)
    gate.execute(req["id"], lambda: "ran")
    events = [e["event"] for e in audit.entries()]
    assert "approval_requested" in events
    assert "approval_decided" in events
    assert "privileged_executed" in events
