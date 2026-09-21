"""Tests for the deterministic RCA engine."""

import json

import pytest

from agent import RCAEngine
from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from sim import Estate, inject

READ = "demo-read-token-0001"


class RecordingClient:
    """Wraps a real client and records every tool call name."""

    def __init__(self, inner):
        self._inner = inner
        self.calls = []

    def call_tool(self, name, args):
        self.calls.append(name)
        return self._inner.call_tool(name, args)


@pytest.fixture()
def rig(tmp_path):
    estate = Estate(seed=42)
    audit = AuditLog(tmp_path / "audit.jsonl")
    gateway = Gateway(estate, audit)
    inner = InProcessClient(gateway, READ)
    client = RecordingClient(inner)
    engine = RCAEngine(client, audit)
    return estate, audit, client, engine


def test_channel_stopped_diagnosis(rig):
    estate, audit, client, engine = rig
    alert = inject(estate, "channel_stopped")
    d = engine.diagnose(alert)
    assert d.top.id == "h1"
    assert d.top.score == 1.0
    assert len(d.top.evidence) >= 2
    for c in d.top.evidence:
        assert len(c.excerpt) <= 160
    # remediation is data only: the privileged tool was never invoked
    assert "restart_channel" not in client.calls
    rem = d.top.remediation
    assert rem["tool"] == "restart_channel"
    assert rem["args"]["channel"] == "PAYMENTS.RCVR"
    assert rem["requires_scope"] == "admin:write"


def test_citations_come_from_real_tool_output(rig):
    estate, audit, client, engine = rig
    alert = inject(estate, "channel_stopped")
    d = engine.diagnose(alert)
    outputs = []
    for call in d.evidence_calls:
        outputs.append(json.dumps(client._inner.call_tool(call["tool"], call["args"]),
                                  sort_keys=True))
    for h in d.hypotheses:
        for c in h.evidence:
            assert any(c.excerpt in out for out in outputs), \
                f"citation not found in tool output: {c.excerpt!r}"


def test_disk_full_diagnosis(rig):
    estate, audit, client, engine = rig
    alert = inject(estate, "disk_full")
    d = engine.diagnose(alert)
    assert d.top.id == "h1"  # disk pressure
    assert d.top.remediation is None  # no safe auto-remediation proposed
    assert "restart_channel" not in client.calls


def test_kafka_lag_diagnosis(rig):
    estate, audit, client, engine = rig
    alert = inject(estate, "kafka_lag")
    d = engine.diagnose(alert)
    assert d.top.id == "h1"  # consumer group stalled, MQ side healthy
    assert d.top.remediation is None


def test_unknown_alert_type_raises(rig):
    estate, audit, client, engine = rig
    with pytest.raises(ValueError):
        engine.diagnose({"type": "alien_invasion"})


def test_degraded_mode_when_tool_denied(tmp_path):
    estate = Estate(seed=42)
    audit = AuditLog(tmp_path / "audit.jsonl")
    gateway = Gateway(estate, audit)

    class DenyingClient:
        def call_tool(self, name, args):
            if name == "read_error_log":
                raise PermissionError("denied for test")
            return gateway.call(name, args, READ)

    engine = RCAEngine(DenyingClient(), audit)
    alert = inject(estate, "channel_stopped")
    d = engine.diagnose(alert)  # must not raise
    assert "evidence_denied" in [e["event"] for e in audit.entries()]
    assert d.top is not None


def test_diagnosis_is_audited(rig):
    estate, audit, client, engine = rig
    alert = inject(estate, "channel_stopped")
    engine.diagnose(alert)
    done = [e for e in audit.entries() if e["event"] == "diagnosis_complete"]
    assert len(done) == 1
    assert done[0]["details"]["top_hypothesis"] == "h1"
