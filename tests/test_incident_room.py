"""Tests for the incident room backend (demo/incident.py + web routes).

The incident is REAL: fixtures recorded live against QM1 (IBM MQ 10.0.0.5)
on 2026-09-21 are served by the genuine IBMQConnector read paths in
recorder replay mode. No pymqi, no network, no queue manager -- the
pymqi import is poisoned to prove the driver is never touched.

Covers: the end-to-end contract shape of POST /api/incident/run, 400 on
decide-before-run, approve/reject audit entries with rehearsal-only
execution (executed=false, never claimed otherwise), and the honest
unavailable-verify path when post-recovery fixtures were never recorded.
"""

import json
import sys
import threading
import urllib.error
import urllib.request

import pytest

from demo.web import make_server


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def replay_env(monkeypatch):
    """Replay fixtures; prove the driver is never touched on this path."""
    monkeypatch.setenv("RCA_RECORD_MODE", "replay")
    monkeypatch.setitem(sys.modules, "pymqi", None)
    # read_error_log's config check runs before the recorder; a bogus dir
    # proves the log file is never opened in replay mode.
    monkeypatch.setenv("MQ_ERROR_LOG_DIR", "/nonexistent-dir-xyz")
    # Jev must take the deterministic fallback in these tests: no key,
    # no demo client.
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delenv("RCA_JEV_DEMO", raising=False)
    return monkeypatch


@pytest.fixture()
def isolated_runs(replay_env, tmp_path):
    """Run the pipeline with runs/ landing in a temp dir."""
    replay_env.chdir(tmp_path)
    return tmp_path


class Client:
    def __init__(self, base: str):
        self.base = base

    def _request(self, method: str, path: str, body=None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data,
                                     headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return 200, resp.read(), resp.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            return (exc.code, exc.read(),
                    exc.headers.get("Content-Type", ""))

    def get(self, path: str):
        return self._request("GET", path)

    def post(self, path: str, body=None):
        return self._request("POST", path, body)

    def post_json(self, path: str, body=None):
        status, raw, _ = self.post(path, body)
        return status, json.loads(raw.decode("utf-8"))

    def get_json(self, path: str):
        status, raw, _ = self.get(path)
        return status, json.loads(raw.decode("utf-8"))


@pytest.fixture()
def client(isolated_runs):
    server = make_server(port=0, incident_room=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    assert host == "127.0.0.1", "web UI must bind loopback only"
    yield Client(f"http://{host}:{port}")
    server.shutdown()
    server.server_close()


# ---------------------------------------------------------------------------
# end-to-end: real fixtures through the real pipeline
# ---------------------------------------------------------------------------

EXPECTED_TITLE = ("Channel BATCH.CHL stopped \u2014 200 messages backlogged "
                  "on BACKLOG.Q")


def test_incident_run_contract(isolated_runs):
    from demo.incident import run_incident

    response, _audit, _plan = run_incident()

    assert response["mode"] == "replay-demo"
    inc = response["incident"]
    assert inc["title"] == EXPECTED_TITLE
    assert inc["mode"] == "replay-demo"
    assert len(inc["id"]) == 16
    assert inc["evidence_source"] == {
        "recorded_at": "2026-09-21T22:29:54Z",
        "source": "live: QM1, IBM MQ 10.0.0.5",
        "note": ("Evidence replayed from recordings. "
                 "No live queue manager was touched."),
    }

    # Evidence plan queried ONLY recorded combos, in order.
    assert [c["tool"] for c in response["evidence_calls"]] == [
        "get_channel_status", "get_queue_depth",
        "read_error_log", "get_channel_status",
    ]
    calls = {(c["tool"], json.dumps(c["args"], sort_keys=True))
             for c in response["evidence_calls"]}
    assert ("get_channel_status",
            json.dumps({"channel": "BATCH.CHL", "qmgr": "QM1"},
                       sort_keys=True)) in calls
    assert ("get_queue_depth",
            json.dumps({"qmgr": "QM1", "queue": "BACKLOG.Q"},
                       sort_keys=True)) in calls
    assert ("read_error_log",
            json.dumps({"qmgr": "QM1"}, sort_keys=True)) in calls

    diag = response["diagnosis"]
    assert diag["alert_type"] == "mq_channel_backlog"
    assert diag["top"]["id"] == "h1"
    assert diag["top"]["score"] == 1.0
    assert len(diag["hypotheses"]) == 2
    assert diag["hypotheses"][0]["score"] >= diag["hypotheses"][1]["score"]

    # Citations quote REAL fixture values, not invented ones.
    excerpts = " ".join(c["excerpt"]
                        for h in diag["hypotheses"] for c in h["evidence"])
    assert "STOPPED" in excerpts            # channel status fixture
    assert "200" in excerpts                # queue depth fixture
    assert "AMQ9533W" in excerpts           # error-log fixture
    assert "BATCH.CHL" in excerpts
    for h in diag["hypotheses"]:
        for c in h["evidence"]:
            assert len(c["excerpt"]) <= 160

    # Jev: no key -> honest deterministic fallback, never hidden.
    jev = response["jev"]
    assert jev == {
        "available": False,
        "mode": "deterministic",
        "top_probability": 1.0,
        "confidence": 1.0,
        "self_consistency": "n/a",
        "uncertain": False,
    }

    # Plan: one privileged step, approval-gated, verify attached.
    plan = response["plan"]
    assert len(plan["steps"]) == 1
    step = plan["steps"][0]
    assert step["action"] == "restart_channel"
    assert step["args"] == {"qmgr": "QM1", "channel": "BATCH.CHL"}
    assert step["verify"] == {
        "tool": "get_channel_status",
        "args": {"qmgr": "QM1", "channel": "BATCH.CHL"},
        "expect": "RUNNING",
    }
    assert step["rationale"]  # grounded in the diagnosis evidence
    assert response["pending_approval"] is True


def test_incident_audit_covers_every_phase(isolated_runs):
    from demo.incident import audit_entries_json, run_incident

    _response, audit, _plan = run_incident()
    actions = [e["action"] for e in audit_entries_json(audit)]
    assert "incident_started" in actions
    assert "tool_call" in actions            # per-tool evidence calls
    assert "diagnosis_complete" in actions
    assert "reasoning_complete" in actions   # jev phase, deterministic here
    assert "plan_proposed" in actions
    ok, msg = audit.verify()
    assert ok, msg


# ---------------------------------------------------------------------------
# HTTP: run / decide / verify / audit
# ---------------------------------------------------------------------------

def test_decide_before_run_is_400(client):
    status, body = client.post_json("/api/incident/decide",
                                    {"decision": "approve"})
    assert status == 400
    assert "run" in body["error"]


def test_decide_rejects_bad_decision(client):
    status, _ = client.post_json("/api/incident/run")
    assert status == 200
    status, body = client.post_json("/api/incident/decide",
                                    {"decision": "maybe"})
    assert status == 400


def test_approve_rehearses_never_executes(client):
    status, run = client.post_json("/api/incident/run")
    assert status == 200
    assert run["incident"]["title"] == EXPECTED_TITLE

    status, decided = client.post_json("/api/incident/decide",
                                       {"decision": "approve"})
    assert status == 200
    assert decided["mode"] == "replay-demo"
    assert decided["decision"] == "approve"
    assert decided["audit"]["action"] == "plan_approved"
    assert decided["audit"]["actor"] == "operator"

    # Rehearsal: the response must NEVER claim an execution happened.
    execution = decided["execution"]
    assert execution["executed"] is False
    assert execution["rehearsal"] is True
    assert execution["would_execute"] == {
        "action": "restart_channel",
        "args": {"qmgr": "QM1", "channel": "BATCH.CHL"},
    }
    assert "no live queue manager" in execution["note"].lower()

    # The decision is in the audit trail.
    status, audit = client.get_json("/api/incident/audit")
    assert status == 200
    actions = [e["action"] for e in audit["entries"]]
    assert "plan_approved" in actions
    assert "plan_proposed" in actions

    # Deciding twice is a 400: nothing is pending anymore.
    status, _ = client.post_json("/api/incident/decide",
                                 {"decision": "approve"})
    assert status == 400


def test_reject_is_audited(client):
    status, _ = client.post_json("/api/incident/run")
    assert status == 200
    status, decided = client.post_json("/api/incident/decide",
                                       {"decision": "reject"})
    assert status == 200
    assert decided["decision"] == "reject"
    assert decided["audit"]["action"] == "plan_rejected"
    assert decided["execution"]["executed"] is False
    assert decided["execution"]["rehearsal"] is True

    status, audit = client.get_json("/api/incident/audit")
    assert status == 200
    assert "plan_rejected" in [e["action"] for e in audit["entries"]]


def test_audit_before_run_is_404(client):
    status, body = client.get_json("/api/incident/audit")
    assert status == 404


def test_verify_with_recovery_fixtures_confirms_recovery(client):
    # Post-recovery fixtures were recorded live on QM1 (channel started,
    # queue drained): verification replays them and confirms recovery.
    status, result = client.post_json("/api/incident/verify")
    assert status == 200
    assert result["mode"] == "replay-demo"
    assert result["available"] is True
    assert result["recovered"] is True
    by_tool = {c["tool"]: c for c in result["checks"]}
    assert by_tool["get_channel_status"]["observed"] == "RUNNING"
    assert by_tool["get_channel_status"]["pass"] is True
    assert by_tool["get_queue_depth"]["observed"] == 0
    assert by_tool["get_queue_depth"]["pass"] is True


def test_verify_without_recovery_fixtures_is_honest(
        client, monkeypatch, tmp_path):
    # Point the recovery recorder at an empty fixture dir: the endpoint
    # must say plainly that nothing was recorded instead of inventing
    # a recovered state.
    import demo.incident as incident_mod
    real_recorder = incident_mod.Recorder

    def empty_recorder(tech, **kwargs):
        kwargs["fixture_root"] = tmp_path / "empty-fixtures"
        return real_recorder(tech, **kwargs)

    monkeypatch.setattr(incident_mod, "Recorder", empty_recorder)
    status, result = client.post_json("/api/incident/verify")
    assert status == 200
    assert result["mode"] == "replay-demo"
    assert result["available"] is False
    assert result["recovered"] is False
    assert result["checks"] == []
    assert "ibmmq_recovered" in result["note"]
    assert "never recorded" in result["note"]


def test_incident_page_served_or_honest_404(client):
    # The sibling worker owns demo/static/incident.html; until it lands,
    # both routes must 404 as JSON rather than 500 or HTML.
    for path in ("/", "/incident"):
        status, raw, ctype = client.get(path)
        if status == 200:
            assert "text/html" in ctype
        else:
            assert status == 404
            assert "application/json" in ctype
            assert "error" in json.loads(raw.decode("utf-8"))


def test_sim_index_unaffected_without_flag(isolated_runs):
    server = make_server(port=0)  # incident_room defaults to False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        c = Client(f"http://{host}:{port}")
        status, raw, ctype = c.get("/")
        assert status == 200
        assert "text/html" in ctype
        assert b"RCA Assistant" in raw
    finally:
        server.shutdown()
        server.server_close()
