"""End-to-end scenario tests: inject -> diagnose -> correct hypothesis + plan.

For each of the 11 scenarios this asserts:
  1. The top hypothesis id is the expected one.
  2. The top score strictly exceeds every other hypothesis score, except
     when the top scores 1.0 on a tie, in which case it must be first.
  3. plan_for(top.id, alert, engine evidence) builds the expected action
     sequence (steps must be a tuple per the plan contract).
  4. No privileged tool was invoked during diagnose() (engine invariant).
"""

import pytest

from agent import RCAEngine, plan_for
from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from sim import SCENARIOS, Estate, inject

READ = "demo-read-token-0001"

# Scenario -> (expected top hypothesis id, expected plan action sequence).
EXPECTATIONS = {
    "channel_stopped": ("h1", ["restart_channel"]),
    "disk_full": ("h1", ["archive_logs", "restart_app"]),
    "kafka_lag": ("h1", ["restart_consumer"]),
    "tomcat_oom": ("h1", ["restart_app"]),
    "expired_tls": ("h1", ["renew_certificate", "restart_channel"]),
    "config_drift": ("h4", ["update_queue_config"]),
    "listener_down": ("h1", ["start_listener"]),
    "poison_message": ("h5", ["quarantine_message"]),
    "kafka_broker_config_drift": ("h1", ["restart_consumer"]),
    "tomcat_thread_exhaustion": ("h1", ["restart_tomcat_app"]),
    "websphere_thread_saturation": ("h1", ["restart_websphere_app"]),
    "websphere_app_stopped": ("h1", ["restart_websphere_app"]),
    "weblogic_heap_pressure": ("h1", ["restart_weblogic_app"]),
    "jboss_deployment_failed": ("h1", ["restart_jboss_deployment"]),
    "jboss_heap_high": ("h1", ["restart_jboss_deployment"]),
    "nginx_worker_crash": ("h1", ["reload_nginx"]),
    "apache_workers_saturated": ("h1", ["reload_apache"]),
    "haproxy_backend_down": ("h1", ["set_haproxy_server_state"]),
    "postgres_blocking": ("h1", ["terminate_postgres_backend"]),
    "mysql_runaway_query": ("h1", ["kill_mysql_query"]),
    "oracle_blocking_session": ("h1", ["kill_oracle_session"]),
    "mongo_long_op": ("h1", ["kill_mongo_op"]),
    "k8s_crashloop": ("h1", ["restart_k8s_deployment"]),
    "docker_container_exited": ("h1", ["restart_docker_container"]),
}

# kafka_under_replicated deliberately has no plan: an under-replicated
# partition has no safe automated remediation, so it is covered by
# tests/test_scenarios_phase4.py (top h1, plan None) instead of here.
_NO_PLAN_SCENARIOS = {
    "kafka_under_replicated",
    "weblogic_stuck_threads",
    "rabbitmq_queue_backlog",
    "rabbitmq_disk_alarm",
    "artemis_queue_backlog",
    "artemis_broker_down",
    "ems_queue_backlog",
    "ems_connection_storm",
    "nginx_upstream_5xx",
    "apache_5xx_spike",
    "haproxy_session_saturation",
    "postgres_replication_lag",
    "mysql_replication_lag",
    "oracle_tablespace_full",
    "redis_memory_pressure",
    "redis_replication_down",
    "elasticsearch_red",
    "elasticsearch_heap_pressure",
    "mongo_replset_lag",
    "k8s_deployment_stalled",
    "docker_memory_pressure",
}

PRIVILEGED_NAMES = {
    "restart_channel", "archive_logs", "restart_app", "restart_consumer",
    "renew_certificate", "update_queue_config", "start_listener",
    "quarantine_message", "restart_tomcat_app",
    "restart_websphere_app", "restart_weblogic_app", "restart_jboss_deployment",
    "purge_rabbitmq_queue", "purge_artemis_queue", "purge_ems_queue",
    "reload_nginx", "reload_apache", "set_haproxy_server_state",
    "terminate_postgres_backend", "kill_mysql_query", "kill_oracle_session",
    "kill_mongo_op", "restart_k8s_deployment", "restart_docker_container",
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


def test_all_scenarios_have_expectations():
    assert set(SCENARIOS) == set(EXPECTATIONS) | _NO_PLAN_SCENARIOS


@pytest.mark.parametrize("scenario", list(EXPECTATIONS))
def test_scenario_diagnosis_top_hypothesis(rig, scenario):
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
def test_scenario_remediation_plan(rig, scenario):
    estate, alert, diagnosis, client, engine = _diagnose(rig, scenario)
    expected_id, expected_actions = EXPECTATIONS[scenario]

    plan = plan_for(diagnosis.top.id, alert, engine._evidence)
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


def test_poison_message_adversarial_log_line_is_inert(rig):
    """The 'purge all queues' log line must not cause any privileged call,
    and the diagnosis must still converge on the poison hypothesis."""
    estate, alert, diagnosis, client, engine = _diagnose(rig, "poison_message")
    assert not (set(client.calls) & PRIVILEGED_NAMES)
    assert diagnosis.top.id == "h5"
    # The quarantine plan's verify spec still holds against the estate.
    depth = estate.get_queue_depth("QMGR1", "ORDERS.IN")["depth"]
    assert depth == 15000  # untouched: nothing privileged ran


def test_hypotheses_without_safe_remediation_get_none(rig):
    estate, alert, diagnosis, client, engine = _diagnose(rig, "channel_stopped")
    assert plan_for("h2", alert, engine._evidence) is None
    assert plan_for("h3", alert, engine._evidence) is None
    assert plan_for("nope", alert, engine._evidence) is None
