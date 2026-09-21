"""Wave-3 scenario tests: 34 incident scenarios for the 17 new connectors.

Covers, for each scenario:
  1. inject() returns a sane alert with the expected keys.
  2. The top hypothesis id is h1 with a strict score separation.
  3. plan_for() builds the expected action sequence (or None when no safe
     remediation exists), and the engine wired the same plan onto the top
     hypothesis.
  4. No privileged tool was invoked during diagnose() (engine invariant).

Plus: verify specs pass against real post-action sim state (failing before
the privileged mutation for the discriminating ones), the new privileged
tools refuse read-only routing, and report generation renders the new
evidence without crashing.
"""

import pytest

from agent import RCAEngine, plan_for
from agent.plans import check_verify
from agent.report import generate_markdown
from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from mcp_server.tools import PRIVILEGED_TOOLS
from sim import SCENARIOS, Estate, inject

READ = "demo-read-token-0001"

# New scenario -> (expected top hypothesis id, expected plan actions or None).
EXPECTATIONS = {
    "websphere_thread_saturation": ("h1", ["restart_websphere_app"]),
    "websphere_app_stopped": ("h1", ["restart_websphere_app"]),
    "weblogic_heap_pressure": ("h1", ["restart_weblogic_app"]),
    "weblogic_stuck_threads": ("h1", None),  # stuck threads need a code fix
    "jboss_deployment_failed": ("h1", ["restart_jboss_deployment"]),
    "jboss_heap_high": ("h1", ["restart_jboss_deployment"]),
    "rabbitmq_queue_backlog": ("h1", None),  # consumer fleet is external
    "rabbitmq_disk_alarm": ("h1", None),  # disk alarm is operator work
    "artemis_queue_backlog": ("h1", None),  # consumer fleet is external
    "artemis_broker_down": ("h1", None),  # broker restart is operator work
    "ems_queue_backlog": ("h1", None),  # consumer fleet is external
    "ems_connection_storm": ("h1", None),  # leak is in the client app
    "nginx_upstream_5xx": ("h1", None),  # fault is in the upstream app
    "nginx_worker_crash": ("h1", ["reload_nginx"]),
    "apache_workers_saturated": ("h1", ["reload_apache"]),
    "apache_5xx_spike": ("h1", None),  # fault is in the backend app
    "haproxy_backend_down": ("h1", ["set_haproxy_server_state"]),
    "haproxy_session_saturation": ("h1", None),  # maxconn tuning is operator work
    "postgres_blocking": ("h1", ["terminate_postgres_backend"]),
    "postgres_replication_lag": ("h1", None),  # standby recovery is operator work
    "mysql_runaway_query": ("h1", ["kill_mysql_query"]),
    "mysql_replication_lag": ("h1", None),  # replica recovery is operator work
    "oracle_tablespace_full": ("h1", None),  # adding datafiles is a DBA change
    "oracle_blocking_session": ("h1", ["kill_oracle_session"]),
    "redis_memory_pressure": ("h1", None),  # eviction policy is operator config
    "redis_replication_down": ("h1", None),  # replica re-sync is operator work
    "elasticsearch_red": ("h1", None),  # shard allocation is operator work
    "elasticsearch_heap_pressure": ("h1", None),  # heap sizing is operator work
    "mongo_long_op": ("h1", ["kill_mongo_op"]),
    "mongo_replset_lag": ("h1", None),  # secondary recovery is operator work
    "k8s_crashloop": ("h1", ["restart_k8s_deployment"]),
    "k8s_deployment_stalled": ("h1", None),  # stuck rollout needs a human
    "docker_container_exited": ("h1", ["restart_docker_container"]),
    "docker_memory_pressure": ("h1", None),  # restart does not fix leaks
}

# New scenario -> (expected alert type, expected alert fields).
ALERT_SHAPES = {
    "websphere_thread_saturation": (
        "websphere_thread_saturated", {"pool": "WebContainer", "app": "payments-ear"}),
    "websphere_app_stopped": ("websphere_app_stopped", {"app": "orders-ear"}),
    "weblogic_heap_pressure": ("weblogic_heap_high", {"app": "payments-app"}),
    "weblogic_stuck_threads": ("weblogic_stuck_threads", {}),
    "jboss_deployment_failed": ("jboss_deployment_failed", {"deployment": "payments.war"}),
    "jboss_heap_high": ("jboss_heap_high", {"deployment": "orders.war"}),
    "rabbitmq_queue_backlog": ("rabbitmq_queue_backlog", {"vhost": "/", "queue": "payments.in"}),
    "rabbitmq_disk_alarm": ("rabbitmq_node_resource_alarm", {"node": "rmq01"}),
    "artemis_queue_backlog": ("artemis_queue_backlog", {"queue": "PAYMENTS.IN"}),
    "artemis_broker_down": ("artemis_broker_down", {}),
    "ems_queue_backlog": ("ems_queue_backlog", {"queue": "PAYMENTS.IN"}),
    "ems_connection_storm": ("ems_connection_storm", {}),
    "nginx_upstream_5xx": ("nginx_upstream_errors", {}),
    "nginx_worker_crash": ("nginx_worker_crash", {}),
    "apache_workers_saturated": ("apache_workers_saturated", {}),
    "apache_5xx_spike": ("apache_5xx_spike", {}),
    "haproxy_backend_down": ("haproxy_backend_down", {"backend": "payments_api", "server": "pay03"}),
    "haproxy_session_saturation": ("haproxy_session_saturation", {"frontend": "https_in"}),
    "postgres_blocking": ("postgres_blocking", {"pid": 4821}),
    "postgres_replication_lag": ("postgres_replication_lag", {}),
    "mysql_runaway_query": ("mysql_runaway_query", {"process_id": 90210}),
    "mysql_replication_lag": ("mysql_replication_lag", {}),
    "oracle_tablespace_full": ("oracle_tablespace_full", {"tablespace": "USERS"}),
    "oracle_blocking_session": ("oracle_blocking", {"sid": 123, "serial": 4567}),
    "redis_memory_pressure": ("redis_memory_high", {}),
    "redis_replication_down": ("redis_replication_down", {}),
    "elasticsearch_red": ("elasticsearch_cluster_red", {}),
    "elasticsearch_heap_pressure": ("elasticsearch_heap_pressure", {}),
    "mongo_long_op": ("mongo_long_running_op", {"opid": 77123}),
    "mongo_replset_lag": ("mongo_replset_lag", {}),
    "k8s_crashloop": ("k8s_pod_crashloop", {"namespace": "payments", "deployment": "payments-api"}),
    "k8s_deployment_stalled": ("k8s_deployment_stalled", {"namespace": "payments", "deployment": "payments-api"}),
    "docker_container_exited": ("docker_container_exited", {"container": "payments-api"}),
    "docker_memory_pressure": ("docker_memory_high", {"container": "payments-api"}),
}

PLAN_SCENARIOS = [s for s, (_, actions) in EXPECTATIONS.items()
                  if actions is not None]

# Verifies that discriminate: False before the privileged mutation,
# True after. The rest are trivially true (the sim's privileged method
# does not move the incident counter), so only the post-action True is
# asserted for them.
DISCRIMINATING = {
    "websphere_app_stopped",
    "jboss_deployment_failed",
    "haproxy_backend_down",
    "postgres_blocking",
    "mysql_runaway_query",
    "oracle_blocking_session",
    "mongo_long_op",
    "docker_container_exited",
}

PRIVILEGED_NAMES = {
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


def _apply_privileged(estate, scenario, alert):
    """Perform the plan's privileged mutation directly on the sim estate."""
    if scenario in ("websphere_thread_saturation", "websphere_app_stopped"):
        return estate.restart_websphere_app(alert["app"])
    if scenario == "weblogic_heap_pressure":
        return estate.restart_weblogic_app(alert["app"])
    if scenario in ("jboss_deployment_failed", "jboss_heap_high"):
        return estate.restart_jboss_deployment(alert["deployment"])
    if scenario == "nginx_worker_crash":
        return estate.reload_nginx()
    if scenario == "apache_workers_saturated":
        return estate.reload_apache()
    if scenario == "haproxy_backend_down":
        return estate.set_haproxy_server_state(alert["backend"],
                                               alert["server"], "maint")
    if scenario == "postgres_blocking":
        return estate.terminate_postgres_backend(alert["pid"])
    if scenario == "mysql_runaway_query":
        return estate.kill_mysql_query(alert["process_id"])
    if scenario == "oracle_blocking_session":
        return estate.kill_oracle_session(alert["sid"], alert["serial"])
    if scenario == "mongo_long_op":
        return estate.kill_mongo_op(alert["opid"])
    if scenario == "k8s_crashloop":
        return estate.restart_k8s_deployment(alert["namespace"],
                                             alert["deployment"])
    if scenario == "docker_container_exited":
        return estate.restart_docker_container(alert["container"])
    raise AssertionError(f"no privileged mutation for {scenario}")


def test_new_scenarios_are_registered():
    for scenario in EXPECTATIONS:
        assert scenario in SCENARIOS
    assert set(EXPECTATIONS) == set(ALERT_SHAPES)


@pytest.mark.parametrize("scenario", list(EXPECTATIONS))
def test_new_scenario_alert_shape(scenario):
    alert = inject(Estate(seed=42), scenario)
    assert isinstance(alert, dict)
    expected_type, expected_fields = ALERT_SHAPES[scenario]
    assert alert["type"] == expected_type
    for key, value in expected_fields.items():
        assert alert[key] == value


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


@pytest.mark.parametrize("scenario", PLAN_SCENARIOS)
def test_new_scenario_verify_specs_against_post_action_state(rig, scenario):
    """Each plan's verify spec passes against the real post-action state."""
    estate, alert, diagnosis, client, engine = _diagnose(rig, scenario)
    plan = plan_for(diagnosis.top.id, alert, engine._evidence)
    assert plan is not None

    if scenario in DISCRIMINATING:
        for step in plan.steps:
            ok, _ = check_verify(step.verify, client.call_tool)
            assert ok is False, (
                f"{scenario}: verify should fail before the privileged step")

    _apply_privileged(estate, scenario, alert)

    for step in plan.steps:
        ok, detail = check_verify(step.verify, client.call_tool)
        assert ok is True, (
            f"{scenario}: verify failed after the privileged step: "
            f"{detail}")


def test_new_privileged_tools_are_gated(rig):
    """The new privileged tools exist and refuse read-only routing."""
    estate, audit, client, engine = rig
    for name in PRIVILEGED_NAMES:
        assert name in PRIVILEGED_TOOLS, f"{name} not privileged"
    inject(estate, "postgres_blocking")
    with pytest.raises(PermissionError):
        client.call_tool("terminate_postgres_backend", {"pid": 4821})
    inject(estate, "docker_container_exited")
    with pytest.raises(PermissionError):
        client.call_tool("restart_docker_container",
                         {"container": "payments-api"})


def test_websphere_pool_injection_shape():
    estate = Estate(seed=42)
    inject(estate, "websphere_thread_saturation")
    pool = estate.get_websphere_threadpool("WebContainer")["pools"][0]
    assert pool["current_threads_busy"] == 190
    assert pool["max_threads"] == 200
    assert pool["busy_pct"] == 95.0


def test_k8s_crashloop_injection_shape():
    estate = Estate(seed=42)
    inject(estate, "k8s_crashloop")
    pods = estate.get_k8s_pod_status("payments")["pods"]
    api_pod = next(p for p in pods if p["name"].startswith("payments-api-"))
    assert api_pod["phase"] == "CrashLoopBackOff"
    assert api_pod["restarts"] == 47
    events = estate.get_k8s_events("payments", limit=25)
    assert any("OOMKilled" in e["message"] for e in events)


def test_docker_log_injection_shape():
    estate = Estate(seed=42)
    inject(estate, "docker_container_exited")
    containers = estate.get_docker_containers()["containers"]
    api = next(c for c in containers if c["name"] == "payments-api")
    assert api["state"] == "exited"
    logs = estate.read_docker_logs("payments-api", limit=30)
    assert any("error" in e["message"].lower() for e in logs)


def test_haproxy_backend_injection_shape():
    estate = Estate(seed=42)
    inject(estate, "haproxy_backend_down")
    stats = estate.get_haproxy_stats()
    backend = next(b for b in stats["backends"] if b["name"] == "payments_api")
    server = next(s for s in backend["servers"] if s["name"] == "pay03")
    assert server["check_status"] == "L7STS/503"
    # The remediation drains the server: status flips to MAINT.
    estate.set_haproxy_server_state("payments_api", "pay03", "maint")
    server = next(s for s in estate.get_haproxy_stats()["backends"]
                  if s["name"] == "payments_api")["servers"][0]
    assert server["status"] == "MAINT"


def test_kill_mutations_clear_sim_rows():
    estate = Estate(seed=42)
    inject(estate, "postgres_blocking")
    assert len(estate.get_postgres_blocking()["blockers"]) == 1
    estate.terminate_postgres_backend(4821)
    assert estate.get_postgres_blocking()["blockers"] == []

    estate = Estate(seed=42)
    inject(estate, "mysql_runaway_query")
    assert len(estate.get_mysql_processlist()["processes"]) == 1
    estate.kill_mysql_query(90210)
    assert estate.get_mysql_processlist()["processes"] == []

    estate = Estate(seed=42)
    inject(estate, "oracle_blocking_session")
    assert len(estate.get_oracle_blocking()["blockers"]) == 1
    estate.kill_oracle_session(123, 4567)
    assert estate.get_oracle_blocking()["blockers"] == []

    estate = Estate(seed=42)
    inject(estate, "mongo_long_op")
    assert len(estate.get_mongo_current_ops()["ops"]) == 1
    estate.kill_mongo_op(77123)
    assert estate.get_mongo_current_ops()["ops"] == []


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
