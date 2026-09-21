"""Tests for the stdlib web UI (demo/web.py).

Drives the server over REAL HTTP (urllib) on 127.0.0.1 with an OS-assigned
port (no fixed port): scenarios list, full click-through
(inject -> diagnose -> plan -> approve -> audit valid -> report), the
reject path (no privileged execution), out-of-order/unknown request ids,
unknown runs, oversized bodies, and the sim-vs-real host compare.
"""

import json
import threading
import urllib.error
import urllib.request

import pytest

from demo.web import make_server


# ---------------------------------------------------------------------------
# HTTP client helper
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, base: str):
        self.base = base

    def _request(self, method: str, path: str, body=None):
        data = None
        headers = {}
        if body is not None:
            if isinstance(body, (dict, list)):
                data = json.dumps(body).encode("utf-8")
                headers["Content-Type"] = "application/json"
            else:
                data = body  # raw bytes (for the 413 test)
        req = urllib.request.Request(self.base + path, data=data,
                                     headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                ctype = resp.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, raw, exc.headers.get("Content-Type", "")
        return 200, raw, ctype

    def get(self, path: str):
        return self._request("GET", path)

    def post(self, path: str, body=None):
        return self._request("POST", path, body)

    def get_json(self, path: str):
        status, raw, _ = self.get(path)
        return status, json.loads(raw.decode("utf-8"))

    def post_json(self, path: str, body=None):
        status, raw, _ = self.post(path, body)
        return status, json.loads(raw.decode("utf-8"))


@pytest.fixture(scope="module")
def client():
    server = make_server(port=0)  # OS-assigned port; never a fixed one
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    assert host == "127.0.0.1", "web UI must bind loopback only"
    yield Client(f"http://{host}:{port}")
    server.shutdown()
    server.server_close()


@pytest.fixture(autouse=True)
def _isolated_runs(tmp_path, monkeypatch):
    # Web runs land under runs/; keep them out of the repo tree.
    monkeypatch.chdir(tmp_path)


def _run_through(client, scenario):
    """Create a run, diagnose it, propose a plan; return ids and payloads."""
    status, created = client.post_json("/api/runs", {"scenario": scenario})
    assert status == 200, created
    run_id = created["run_id"]
    status, diagnosis = client.post_json(f"/api/runs/{run_id}/diagnose")
    assert status == 200, diagnosis
    status, plan = client.post_json(f"/api/runs/{run_id}/plan")
    assert status == 200, plan
    return run_id, diagnosis, plan


# ---------------------------------------------------------------------------
# Static + scenarios
# ---------------------------------------------------------------------------

def test_index_serves_html(client):
    status, raw, ctype = client.get("/")
    assert status == 200
    assert "text/html" in ctype
    assert b"RCA Assistant" in raw


def test_static_assets_have_correct_content_types(client):
    status, _, ctype = client.get("/static/app.js")
    assert status == 200
    assert "javascript" in ctype
    status, _, ctype = client.get("/static/style.css")
    assert status == 200
    assert "text/css" in ctype


def test_scenarios_lists_all(client):
    status, scenarios = client.get_json("/api/scenarios")
    assert status == 200
    assert len(scenarios) == 45
    ids = {s["id"] for s in scenarios}
    assert ids == {
        "apache_5xx_spike", "apache_workers_saturated", "artemis_broker_down",
        "artemis_queue_backlog", "channel_stopped", "config_drift",
        "disk_full", "docker_container_exited", "docker_memory_pressure",
        "elasticsearch_heap_pressure", "elasticsearch_red",
        "ems_connection_storm", "ems_queue_backlog", "expired_tls",
        "haproxy_backend_down", "haproxy_session_saturation",
        "jboss_deployment_failed", "jboss_heap_high", "k8s_crashloop",
        "k8s_deployment_stalled", "kafka_broker_config_drift", "kafka_lag",
        "kafka_under_replicated", "listener_down", "mongo_long_op",
        "mongo_replset_lag", "mysql_replication_lag", "mysql_runaway_query",
        "nginx_upstream_5xx", "nginx_worker_crash", "oracle_blocking_session",
        "oracle_tablespace_full", "poison_message", "postgres_blocking",
        "postgres_replication_lag", "rabbitmq_disk_alarm",
        "rabbitmq_queue_backlog", "redis_memory_pressure",
        "redis_replication_down", "tomcat_oom", "tomcat_thread_exhaustion",
        "weblogic_heap_pressure", "weblogic_stuck_threads",
        "websphere_app_stopped", "websphere_thread_saturation",
    }
    for s in scenarios:
        assert s["title"] and s["description"]


def test_create_run_rejects_unknown_scenario(client):
    status, body = client.post_json("/api/runs", {"scenario": "nope"})
    assert status == 400
    assert "error" in body


def test_unknown_run_is_404(client):
    status, _ = client.get_json("/api/runs/abcdef0123456789/audit")
    assert status == 404
    status, _ = client.post_json("/api/runs/abcdef0123456789/diagnose")
    assert status == 404
    status, _ = client.post_json("/api/runs/abcdef0123456789/decide",
                                 {"request_id": "x", "decision": "approve"})
    assert status == 404


def test_oversized_body_is_413(client):
    status, raw, _ = client.post("/api/runs", b"x" * (1024 * 1024 + 1))
    assert status == 413


# ---------------------------------------------------------------------------
# Full click-through: approve path
# ---------------------------------------------------------------------------

def test_full_clickthrough_approve(client):
    run_id, diagnosis, plan = _run_through(client, "channel_stopped")

    assert diagnosis["top_id"] == "h1"
    assert diagnosis["evidence_calls"] > 0
    top = next(h for h in diagnosis["hypotheses"]
               if h["id"] == diagnosis["top_id"])
    assert top["title"] == "Receiver channel stopped"
    assert top["evidence"], "top hypothesis must carry cited evidence"
    for item in top["evidence"]:
        assert {"claim", "tool", "excerpt"} <= set(item)

    assert plan["plan_id"].startswith("PLAN-")
    assert len(plan["steps"]) == 1
    step = plan["steps"][0]
    assert step["action"] == "restart_channel"
    assert step["required_scope"] == "admin:write"
    assert step["verify"]["expect"] == {"status": "RUNNING"}
    pending = plan["pending_request"]
    assert pending["step_id"] == step["id"]

    status, result = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": pending["request_id"], "decision": "approve"})
    assert status == 200, result
    assert result["status"] == "completed"
    outcome = result["outcome"]
    assert outcome["executed"] is True
    assert outcome["verify_ok"] is True
    assert outcome["step_id"] == step["id"]

    status, audit = client.get_json(f"/api/runs/{run_id}/audit")
    assert status == 200
    assert audit["valid"] is True
    assert "entries verified" in audit["message"]
    events = [e["event"] for e in audit["entries"]]
    assert "incident_injected" in events
    assert "diagnosis_complete" in events
    assert "plan_proposed" in events
    assert "approval_requested" in events
    assert "approval_decided" in events
    assert "privileged_executed" in events
    assert "plan_step_executed" in events
    assert "plan_step_verified" in events
    assert "plan_completed" in events

    status, report = client.get_json(f"/api/runs/{run_id}/report")
    assert status == 200
    assert "Receiver channel stopped" in report["markdown"]
    assert "## Audit integrity" in report["markdown"]


def test_multi_step_plan_approve_all(client):
    # disk_full has a 2-step plan (archive_logs, restart_app).
    run_id, diagnosis, plan = _run_through(client, "disk_full")
    assert len(plan["steps"]) == 2

    status, result = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": plan["pending_request"]["request_id"],
         "decision": "approve_all"})
    assert status == 200, result
    assert result["status"] == "completed"
    assert len(result["outcomes"]) == 2
    for outcome in result["outcomes"]:
        assert outcome["decision"] == "approve_all"
        assert outcome["executed"] is True

    status, audit = client.get_json(f"/api/runs/{run_id}/audit")
    assert audit["valid"] is True
    events = [e["event"] for e in audit["entries"]]
    assert "approval_decided_all" in events
    assert "plan_completed" in events


def test_per_step_approve_advances_to_next_pending(client):
    # disk_full: approve step 1 -> step_completed with next pending request.
    run_id, diagnosis, plan = _run_through(client, "disk_full")
    step1, step2 = plan["steps"]

    status, result = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": plan["pending_request"]["request_id"],
         "decision": "approve"})
    assert status == 200, result
    assert result["status"] == "step_completed"
    assert result["outcome"]["step_id"] == step1["id"]
    assert result["outcome"]["verify_ok"] is True
    nxt = result["next_pending_request"]
    assert nxt["step_id"] == step2["id"]

    # Approving the already-decided step-1 request is out-of-order -> 400.
    status, body = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": plan["pending_request"]["request_id"],
         "decision": "approve"})
    assert status == 400

    # Approving step 2 now completes the plan.
    status, result = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": nxt["request_id"], "decision": "approve"})
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# Reject path: halted, nothing privileged ran
# ---------------------------------------------------------------------------

def test_reject_halts_with_no_privileged_execution(client):
    run_id, diagnosis, plan = _run_through(client, "channel_stopped")

    status, result = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": plan["pending_request"]["request_id"],
         "decision": "reject"})
    assert status == 200, result
    assert result["status"] == "halted_rejected"
    assert result["halted_at"] == plan["steps"][0]["id"]
    assert result["outcome"]["executed"] is False

    status, audit = client.get_json(f"/api/runs/{run_id}/audit")
    assert status == 200
    assert audit["valid"] is True
    events = [e["event"] for e in audit["entries"]]
    assert "plan_halted" in events
    assert "privileged_executed" not in events
    assert "plan_step_executed" not in events
    # The denial itself is audited.
    decided = [e for e in audit["entries"]
               if e["event"] == "approval_decided"]
    assert decided and decided[0]["details"]["approved"] is False


# ---------------------------------------------------------------------------
# Out-of-order decisions are refused
# ---------------------------------------------------------------------------

def test_out_of_order_and_unknown_request_ids_are_400(client):
    # expired_tls has a 2-step plan; deciding for step 2 first is 400.
    run_id, diagnosis, plan = _run_through(client, "expired_tls")
    assert len(plan["steps"]) == 2

    status, body = client.post_json(
        f"/api/runs/{run_id}/decide",
        {"request_id": "APR-00000000", "decision": "approve"})
    assert status == 400
    assert "error" in body

    # A request id from a *different* run is also not the current pending.
    other = plan["pending_request"]["request_id"]
    run_id2, _, plan2 = _run_through(client, "channel_stopped")
    status, body = client.post_json(
        f"/api/runs/{run_id2}/decide",
        {"request_id": other, "decision": "approve"})
    assert status == 400

    # Garbage decision value is 400 too.
    status, body = client.post_json(
        f"/api/runs/{run_id2}/decide",
        {"request_id": plan2["pending_request"]["request_id"],
         "decision": "maybe"})
    assert status == 400


# ---------------------------------------------------------------------------
# Host compare
# ---------------------------------------------------------------------------

def test_host_compare_sim_vs_real(client):
    status, data = client.get_json("/api/host/compare")
    assert status == 200
    assert set(data) >= {"sim", "real"}
    assert data["sim"]["host"] == "app01"
    assert data["real"]["host"] == "localhost"
    assert data["real"]["source"] == "real"
    for key in ("cpu_pct", "mem_pct", "disk_pct", "load1"):
        assert isinstance(data["sim"][key], (int, float))
        assert isinstance(data["real"][key], (int, float))
