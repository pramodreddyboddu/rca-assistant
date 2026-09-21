"""Phase-4 scenario tests: Kafka config drift, Tomcat thread exhaustion,
Kafka under-replication.

Covers, for each of the three new scenarios:
  1. inject() returns a sane alert with the expected keys.
  2. The top hypothesis id is h1 with a strict score separation.
  3. plan_for() builds the expected action sequence (or None when no safe
     remediation exists), and the engine wired the same plan onto the top
     hypothesis.
  4. No privileged tool was invoked during diagnose() (engine invariant).

Plus: the new sim tool methods return the documented shapes,
restart_tomcat_app mutates sim state, and report generation renders the
new evidence without crashing.
"""

import pytest

from agent import RCAEngine, plan_for
from agent.plans import VerifySpec, check_verify
from agent.report import generate_markdown
from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from mcp_server.tools import PRIVILEGED_TOOLS
from sim import SCENARIOS, Estate, inject

READ = "demo-read-token-0001"

# New scenario -> (expected top hypothesis id, expected plan actions or None).
EXPECTATIONS = {
    "kafka_broker_config_drift": ("h1", ["restart_consumer"]),
    "tomcat_thread_exhaustion": ("h1", ["restart_tomcat_app"]),
    "kafka_under_replicated": ("h1", None),  # no safe auto-remediation
}

PRIVILEGED_NAMES = {
    "restart_channel", "archive_logs", "restart_app", "restart_consumer",
    "renew_certificate", "update_queue_config", "start_listener",
    "quarantine_message", "restart_tomcat_app",
}


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


def _diagnose(rig, scenario):
    estate, audit, client, engine = rig
    alert = inject(estate, scenario)
    diagnosis = engine.diagnose(alert)
    return estate, alert, diagnosis, client, engine


def test_new_scenarios_are_registered():
    for scenario in EXPECTATIONS:
        assert scenario in SCENARIOS


@pytest.mark.parametrize("scenario", list(EXPECTATIONS))
def test_new_scenario_alert_shape(scenario):
    alert = inject(Estate(seed=42), scenario)
    assert isinstance(alert, dict)
    assert alert["type"] in {"consumer_lag", "threadpool_saturated",
                             "kafka_under_replicated"}
    if scenario == "kafka_broker_config_drift":
        assert alert["topic"] == "orders-events"
        assert alert["group"] == "orders-consumers"
        assert alert["observed_lag"] == 95000
    elif scenario == "tomcat_thread_exhaustion":
        assert alert["pool"] == "http-nio-8080"
        assert alert["app"] == "/orders"
        assert alert["host"] == "app01"
    else:
        assert alert["topic"] == "payments-events"
        assert alert["partition"] == 2


@pytest.mark.parametrize("scenario", list(EXPECTATIONS))
def test_new_scenario_diagnosis_top_hypothesis(rig, scenario):
    estate, alert, diagnosis, client, engine = _diagnose(rig, scenario)
    expected_id, _ = EXPECTATIONS[scenario]

    assert diagnosis.top.id == expected_id
    others = [h for h in diagnosis.hypotheses if h is not diagnosis.top]
    for h in others:
        assert (h.score < diagnosis.top.score
                or (diagnosis.top.score == 1.0
                    and diagnosis.hypotheses[0] is diagnosis.top)), (
            f"{scenario}: top={diagnosis.top.id}@{diagnosis.top.score} not "
            f"separated from {h.id}@{h.score}")

    # The engine never invokes privileged tools while diagnosing.
    assert not (set(client.calls) & PRIVILEGED_NAMES), (
        f"{scenario}: privileged tool invoked during diagnose: {client.calls}")


@pytest.mark.parametrize("scenario", list(EXPECTATIONS))
def test_new_scenario_remediation_plan(rig, scenario):
    estate, alert, diagnosis, client, engine = _diagnose(rig, scenario)
    expected_id, expected_actions = EXPECTATIONS[scenario]

    plan = plan_for(diagnosis.top.id, alert, engine._evidence)
    if expected_actions is None:
        # Deliberately no safe remediation: under-replicated ISR is an
        # operator job, not an automated step.
        assert plan is None, f"{scenario}: expected no plan, got {plan}"
        assert diagnosis.top.plan is None
        return

    assert plan is not None, f"{scenario}: expected a plan for {expected_id}"
    assert isinstance(plan.steps, tuple), "plan steps must be a tuple"
    assert [s.action for s in plan.steps] == expected_actions
    assert plan.hypothesis_id == expected_id
    for step in plan.steps:
        assert step.required_scope == "admin:write"
        assert step.verify is not None

    # The engine wired the same plan onto the top hypothesis.
    assert diagnosis.top.plan is not None
    assert [s.action for s in diagnosis.top.plan.steps] == expected_actions


def test_broker_config_drift_evidence_shapes(rig):
    estate, alert, diagnosis, client, engine = _diagnose(
        rig, "kafka_broker_config_drift")
    configs = {bid: estate.get_kafka_broker_config(bid)["configs"]
               for bid in (1, 2, 3)}
    assert configs[1]["log.retention.hours"] == "168"
    assert configs[2]["log.retention.hours"] == "168"
    assert configs[3]["log.retention.hours"] == "72"
    detail = estate.get_kafka_consumer_group_detail("orders-events",
                                                    "orders-consumers")
    assert detail["topic"] == "orders-events"
    assert detail["total_lag"] == 95000
    assert len(detail["partitions"]) == 3
    assert sum(p["lag"] for p in detail["partitions"]) == 95000
    for p in detail["partitions"]:
        assert p["committed"] == p["end_offset"] - p["lag"]


def test_under_replicated_isr_shape(rig):
    estate = Estate(seed=42)
    # Healthy estate: ISR equals the replica set everywhere.
    healthy = estate.get_kafka_topic_detail("payments-events")
    for p in healthy["partitions"]:
        assert p["isr"] == p["replicas"]

    inject(estate, "kafka_under_replicated")
    degraded = estate.get_kafka_topic_detail("payments-events")
    shrunk = [p for p in degraded["partitions"]
              if len(p["isr"]) < len(p["replicas"])]
    assert len(shrunk) == 1
    assert shrunk[0]["partition"] == 2
    assert shrunk[0]["isr"] == [shrunk[0]["leader"]]

    health = estate.get_kafka_broker_health()
    assert health["reachable"] is True
    assert health["degraded"] is False
    assert health["brokers"] == [1, 2, 3]

    # The estate hook validates its inputs.
    with pytest.raises(ValueError):
        estate.degrade_partition_isr("no-such-topic", 0)
    with pytest.raises(ValueError):
        estate.degrade_partition_isr("payments-events", 99)


def test_threadpool_exhaustion_evidence_shapes(rig):
    estate = Estate(seed=42)
    before = estate.get_tomcat_threadpool("http-nio-8080")["pools"][0]
    assert before["current_threads_busy"] < before["max_threads"]

    inject(estate, "tomcat_thread_exhaustion")
    pool = estate.get_tomcat_threadpool("http-nio-8080")["pools"][0]
    assert pool["current_threads_busy"] == 200
    assert pool["current_thread_count"] == 200
    assert pool["max_threads"] == 200

    apps = estate.get_tomcat_apps()["apps"]
    assert {a["path"] for a in apps} == {"/", "/payments", "/orders"}
    orders = next(a for a in apps if a["path"] == "/orders")
    assert orders["state"] == "running"

    log = estate.read_tomcat_log("catalina", limit=30)
    assert any("currently busy" in e["message"] for e in log)

    heap = estate.get_tomcat_heap()
    assert heap["heap_used_pct"] < 85  # not a heap incident


def test_restart_tomcat_app_is_privileged_and_mutates_state(rig):
    estate, audit, client, engine = rig
    assert "restart_tomcat_app" in PRIVILEGED_TOOLS

    inject(estate, "tomcat_thread_exhaustion")
    before = estate.get_tomcat_threadpool("http-nio-8080")["pools"][0]
    assert before["current_threads_busy"] == 200

    # Sim privileged method mutates state: the http pool drains.
    result = estate.restart_tomcat_app("/orders")
    assert result["app_path"] == "/orders"
    assert result["state"] == "running"
    after = estate.get_tomcat_threadpool("http-nio-8080")["pools"][0]
    assert after["current_threads_busy"] == 8

    # Read-only routing refuses it (contract-covered; asserted here too).
    with pytest.raises(PermissionError):
        client.call_tool("restart_tomcat_app", {"app_path": "/orders"})

    with pytest.raises(ValueError):
        estate.restart_tomcat_app("/no-such-app")


def test_threadpool_verify_spec_dotted_path(rig):
    """The plan's verify spec reaches the nested busy-thread count."""
    estate, audit, client, engine = rig
    inject(estate, "tomcat_thread_exhaustion")

    spec = VerifySpec(
        tool="get_tomcat_threadpool",
        args={"pool": "http-nio-8080"},
        expect={"pools.0.current_threads_busy": {"$lt": 100}},
    )
    ok, _ = check_verify(spec, client.call_tool)
    assert ok is False  # still saturated

    estate.restart_tomcat_app("/orders")
    ok, detail = check_verify(spec, client.call_tool)
    assert ok is True
    assert detail["expect"] == {"pools.0.current_threads_busy": {"$lt": 100}}


@pytest.mark.parametrize("scenario", list(EXPECTATIONS))
def test_report_renders_new_evidence(rig, tmp_path, scenario):
    """Report generation includes the new evidence without crashing."""
    estate, alert, diagnosis, client, engine = _diagnose(rig, scenario)
    audit = AuditLog(tmp_path / "run" / "audit.jsonl")
    audit.append("incident_injected", actor="demo",
                 details={"scenario": scenario, "alert": alert})
    audit.append("diagnosis_complete", "rca-engine",
                 {"alert_type": alert["type"],
                  "top_hypothesis": diagnosis.top.id,
                  "score": diagnosis.top.score,
                  "top_title": diagnosis.top.title,
                  "evidence": [{"claim": c.claim, "tool": c.tool,
                                "excerpt": c.excerpt}
                               for c in diagnosis.top.evidence]})
    report = generate_markdown(
        tmp_path / "run",
        diagnosis={"hypotheses": [
            {"id": h.id, "title": h.title, "score": h.score,
             "evidence": [{"claim": c.claim, "tool": c.tool,
                           "excerpt": c.excerpt} for c in h.evidence]}
            for h in diagnosis.hypotheses]})
    assert scenario in report
    assert diagnosis.top.title in report
    for c in diagnosis.top.evidence:
        assert c.claim in report
