"""Tests for the Jev reasoning layer (agent/jev.py).

Unit tests use a stubbed client only — the live TypeSafe API is never
called. They cover: the full advisory shape, deterministic fallback when
no key is configured, the disable flag, illustrative demo mode, service
errors degrading gracefully, audit hygiene (no key material), and the
/ api/jev/status route plus the diagnose payload's jev block.
"""

import json
import threading
from types import SimpleNamespace

import pytest

from agent.jev import (DemoJevClient, HttpJevClient, JevError, JevReasoner,
                       StubJevClient, self_consistent_noul)
from agent.rca import Citation, Diagnosis, Hypothesis
from demo.web import make_server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _diagnosis() -> Diagnosis:
    h1 = Hypothesis(
        id="h1", title="Channel PAYMENTS.RCVR stopped", score=1.0,
        evidence=[
            Citation(claim="Channel PAYMENTS.RCVR is STOPPED",
                     tool="get_channel_status", excerpt="status=STOPPED"),
            Citation(claim="Error log mentions channel stopped",
                     tool="read_error_log", excerpt="AMQ9513: channel stopped"),
        ],
        remediation=None)
    h2 = Hypothesis(
        id="h2", title="Deep backlog on PAYMENTS.IN", score=0.4,
        evidence=[], remediation=None)
    return Diagnosis(alert={"type": "queue_depth", "qmgr": "QM1",
                            "scenario": "channel_stopped"},
                     hypotheses=[h1, h2], top=h1, evidence_calls=[])


_STUB_ANSWERS = {
    "rank_hypotheses": {
        "type": "choice", "choice": "h1",
        "probabilities": {"h1": 0.82, "h2": 0.18}, "confidence": 0.82},
    "claim_supported_0": {"type": "noul", "noul": 0.91},
    "claim_supported_1": {"type": "noul", "noul": 0.50},
    "triage_priority": {
        "type": "choice", "choice": "P2",
        "probabilities": {"P1": 0.10, "P2": 0.70,
                          "P3": 0.15, "P4": 0.05}, "confidence": 0.70},
    "remediation_risk": {
        "type": "score", "score": 1.2, "confidence": 0.60,
        "legend": {"0": "Low", "1": "Medium", "2": "High"},
        "probabilities": {"0": 0.1, "1": 0.6, "2": 0.3}},
}


class _FakeAudit:
    def __init__(self):
        self.events = []

    def append(self, event, actor, details):
        self.events.append(
            {"event": event, "actor": actor, "details": details})
        return {}


@pytest.fixture
def clean_env(monkeypatch):
    for var in ("TYPESAFE_API_KEY", "RCA_JEV_ENABLED", "RCA_JEV_DEMO"):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# Advisory shape
# ---------------------------------------------------------------------------

def test_advise_with_stub_produces_full_advisory():
    audit = _FakeAudit()
    reasoner = JevReasoner(StubJevClient(_STUB_ANSWERS), audit=audit)
    advisory = reasoner.advise(_diagnosis())

    assert advisory.available is True
    assert advisory.mode == "jev"
    assert advisory.illustrative is False
    assert advisory.error is None

    assert len(advisory.hypothesis_ranking) == 2
    top = advisory.hypothesis_ranking[0]
    assert top.hypothesis_id == "h1"
    assert top.jev_probability == pytest.approx(0.82)
    assert top.confidence == pytest.approx(0.82)
    assert top.agrees_with_deterministic is True
    assert advisory.agreement == "agree"

    assert len(advisory.claim_support) == 2
    assert advisory.claim_support[0].verdict == "supported"
    assert advisory.claim_support[0].probability == pytest.approx(0.91)
    # 0.50 is genuinely uncertain -> partially_supported, ~zero confidence
    assert advisory.claim_support[1].verdict == "partially_supported"
    assert advisory.claim_support[1].confidence == pytest.approx(0.0)

    assert advisory.triage.priority == "P2"
    assert advisory.triage.confidence == pytest.approx(0.70)
    # Risk is scored against a real plan, not at diagnose time.
    assert advisory.remediation_risk is None

    plan = SimpleNamespace(
        title="Restart receiver channel",
        steps=[SimpleNamespace(id="s1", action="restart_channel",
                               verify="get_channel_status")])
    risk = reasoner.advise_remediation_risk(_diagnosis(), plan)
    assert risk.risk == "medium"  # 1.2 on a 0..2 scale
    assert risk.risk_score == pytest.approx(1.2)

    as_dict = advisory.to_dict()
    assert as_dict["available"] is True
    assert as_dict["triage"]["priority"] == "P2"
    assert as_dict["remediation_risk"] is None


def test_disagreement_is_reported_not_hidden():
    answers = dict(_STUB_ANSWERS)
    answers["rank_hypotheses"] = {
        "type": "choice", "choice": "h2",
        "probabilities": {"h1": 0.3, "h2": 0.7}, "confidence": 0.7}
    advisory = JevReasoner(StubJevClient(answers)).advise(_diagnosis())
    assert advisory.agreement == "disagree"
    assert advisory.hypothesis_ranking[0].hypothesis_id == "h2"
    assert advisory.hypothesis_ranking[0].agrees_with_deterministic is False
    # The deterministic top still decides; advisory is advisory-only.
    assert _diagnosis().top.id == "h1"


def test_claim_verdict_boundaries():
    reasoner = JevReasoner(StubJevClient({
        "rank_hypotheses": {"type": "choice", "choice": "h1",
                            "probabilities": {"h1": 1.0}, "confidence": 1.0},
        "claim_supported_0": {"type": "noul", "noul": 0.10},
    }))
    advisory = reasoner.advise(_diagnosis())
    assert advisory.claim_support[0].verdict == "unsupported"


def test_questions_are_typed_narrow_and_bounded():
    client = StubJevClient(_STUB_ANSWERS)
    JevReasoner(client).advise(_diagnosis())
    assert len(client.seen) == 2
    state, questions = client.seen[0]
    kinds = {q["type"] for q in questions.values()}
    assert kinds <= {"noul", "choice", "score"}
    # Risk is not scored at diagnose time: there is no plan to judge yet.
    assert "remediation_risk" not in questions
    assert set(questions) == set(_STUB_ANSWERS) - {"remediation_risk"}
    # Bounded payload: few hypotheses, short excerpts.
    assert len(state["hypotheses"]) <= 5
    for hyp in state["hypotheses"]:
        for ev in hyp["evidence"]:
            assert len(ev["excerpt"]) <= 160
    # The second batched call is the GAP-2 self-consistency re-ask of the
    # final root-cause verdict ONLY: exactly three Noul paraphrasings in
    # one request; everything else stays single-shot.
    _, sc_questions = client.seen[1]
    assert set(sc_questions) == {"sc_p1", "sc_p2", "sc_p3"}
    assert {q["type"] for q in sc_questions.values()} == {"noul"}


def test_remediation_risk_is_scored_against_the_plan_only():
    client = StubJevClient(_STUB_ANSWERS)
    reasoner = JevReasoner(client)
    plan = SimpleNamespace(
        title="Restart receiver channel",
        steps=[SimpleNamespace(id="s1", action="restart_channel",
                               verify="get_channel_status")])
    risk = reasoner.advise_remediation_risk(_diagnosis(), plan)
    assert risk is not None
    assert risk.risk == "medium"
    # The targeted call asks exactly one narrow question, with the plan in
    # the state so Jev judges real steps, not a guess.
    state, questions = client.seen[0]
    assert set(questions) == {"remediation_risk"}
    assert questions["remediation_risk"]["type"] == "score"
    assert state["plan"]["title"] == "Restart receiver channel"
    assert state["plan"]["steps"][0]["action"] == "restart_channel"


def test_remediation_risk_degrades_without_jev(clean_env):
    reasoner = JevReasoner.from_env()  # no key: deterministic fallback
    plan = SimpleNamespace(title="t", steps=[])
    assert reasoner.advise_remediation_risk(_diagnosis(), plan) is None


# ---------------------------------------------------------------------------
# Modes: Jev on by default, deterministic fallback
# ---------------------------------------------------------------------------

def test_no_key_means_deterministic_fallback(clean_env):
    reasoner = JevReasoner.from_env()
    assert reasoner.enabled is False
    status = reasoner.status()
    assert status["mode"] == "deterministic"
    assert "TYPESAFE_API_KEY" in status["message"]

    advisory = reasoner.advise(_diagnosis())
    assert advisory.available is False
    assert advisory.mode == "deterministic"
    assert advisory.hypothesis_ranking == []


def test_key_means_jev_on_by_default(clean_env, monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    reasoner = JevReasoner.from_env()
    assert reasoner.enabled is True
    status = reasoner.status()
    assert status["mode"] == "jev"
    assert status["configured"] is True


def test_disable_flag_forces_deterministic(clean_env, monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setenv("RCA_JEV_ENABLED", "0")
    reasoner = JevReasoner.from_env()
    assert reasoner.enabled is False
    assert reasoner.status()["mode"] == "deterministic"


def test_demo_mode_is_labeled_illustrative(clean_env, monkeypatch):
    monkeypatch.setenv("RCA_JEV_DEMO", "1")
    reasoner = JevReasoner.from_env()
    assert reasoner.enabled is True
    status = reasoner.status()
    assert status["mode"] == "jev"
    assert status["illustrative"] is True

    advisory = reasoner.advise(_diagnosis())
    assert advisory.available is True
    assert advisory.illustrative is True
    assert advisory.to_dict()["illustrative"] is True
    # Derived from the diagnosis state, not hardcoded to one scenario.
    assert advisory.hypothesis_ranking[0].hypothesis_id == "h1"


# ---------------------------------------------------------------------------
# Failure handling + key hygiene
# ---------------------------------------------------------------------------

def test_service_error_degrades_to_unavailable():
    bad = HttpJevClient("k", api_url="http://127.0.0.1:1/", timeout_s=2)
    with pytest.raises(JevError):
        bad.ask({}, {})
    advisory = JevReasoner(bad).advise(_diagnosis())
    assert advisory.available is False
    assert advisory.error is not None
    assert advisory.mode == "jev"  # configured, but the service failed


def test_key_never_appears_in_audit_or_advisory(clean_env, monkeypatch):
    canary = "sk-canary-9f8e7d6c5b4a"
    monkeypatch.setenv("TYPESAFE_API_KEY", canary)
    audit = _FakeAudit()
    advisory = JevReasoner(StubJevClient(_STUB_ANSWERS),
                           audit=audit).advise(_diagnosis())
    blob = json.dumps(audit.events) + json.dumps(advisory.to_dict())
    assert canary not in blob
    jev_events = [e for e in audit.events
                  if e["event"] == "jev_advisory"]
    assert len(jev_events) == 1
    details = jev_events[0]["details"]
    assert "questions_sha" in details  # hash, not the request body
    assert details["mode"] == "jev"


def test_http_client_sends_bearer_and_model():
    seen = {}

    class _Transport(HttpJevClient):
        def ask(self, state, questions):  # noqa: D102 - test double
            seen["questions"] = questions
            return {}

    client = _Transport("sekret")
    assert client._api_key == "sekret"  # held in memory only
    client.ask({"a": 1}, {"q": {"type": "noul"}})
    assert "q" in seen["questions"]
    assert isinstance(client, HttpJevClient)


# ---------------------------------------------------------------------------
# GAP 2: self-consistency on the final root-cause verdict
# ---------------------------------------------------------------------------

def _sc_stub(*probs, key="sc"):
    """Stub client scripted with Noul answers for the sc trio."""
    answers = {f"{key}_p{i}": {"type": "noul", "noul": p}
               for i, p in enumerate(probs, 1)}
    return StubJevClient(answers)


def _answers_with_sc(sc_probs):
    answers = dict(_STUB_ANSWERS)
    for i, p in enumerate(sc_probs, 1):
        answers[f"sc_p{i}"] = {"type": "noul", "noul": p}
    return answers


def test_self_consistent_noul_agree_returns_yes():
    result = self_consistent_noul(
        _sc_stub(0.80, 0.85, 0.75), {}, "Hypothesis h1 is the root cause.")
    assert result["verdict"] == "yes"
    assert result["scores"] == pytest.approx([0.80, 0.85, 0.75])
    assert result["spread"] == pytest.approx(0.10)
    assert "error" not in result


def test_self_consistent_noul_split_returns_uncertain():
    result = self_consistent_noul(
        _sc_stub(0.80, 0.20, 0.60), {}, "claim")
    assert result["verdict"] == "uncertain"
    assert result["spread"] == pytest.approx(0.60)


def test_self_consistent_noul_all_no_returns_no():
    result = self_consistent_noul(
        _sc_stub(0.20, 0.30, 0.15), {}, "claim")
    assert result["verdict"] == "no"


def test_self_consistent_noul_boundary_half_is_uncertain():
    # "yes" needs ALL probabilities strictly above 0.5.
    result = self_consistent_noul(
        _sc_stub(0.80, 0.50, 0.70), {}, "claim")
    assert result["verdict"] == "uncertain"


def test_self_consistent_noul_wide_spread_is_uncertain():
    # All above 0.5 but spread 0.30 >= 0.25 -> genuine disagreement.
    result = self_consistent_noul(
        _sc_stub(0.90, 0.60, 0.95), {}, "claim")
    assert result["verdict"] == "uncertain"
    assert result["spread"] == pytest.approx(0.35)


def test_self_consistent_noul_client_error_never_raises():
    class _Boom:
        def ask(self, state, questions):
            raise JevError("network down")

    result = self_consistent_noul(_Boom(), {}, "claim")
    assert result["verdict"] == "uncertain"
    assert result["scores"] == []
    assert result["spread"] == 1.0
    assert "error" in result


def test_self_consistent_noul_missing_answers_are_uncertain():
    result = self_consistent_noul(StubJevClient({}), {}, "claim")
    assert result["verdict"] == "uncertain"
    assert result["scores"] == []
    assert result["spread"] == 1.0
    assert "error" in result


def test_self_consistent_noul_default_paraphrases_and_keys():
    client = _sc_stub(0.7, 0.7, 0.7)
    self_consistent_noul(client, {"alert": {}}, "the sky is green")
    state, questions = client.seen[0]
    assert state == {"alert": {}}
    assert set(questions) == {"sc_p1", "sc_p2", "sc_p3"}
    assert all(q["type"] == "noul" for q in questions.values())
    instrs = [questions[f"sc_p{i}"]["instructions"] for i in (1, 2, 3)]
    assert instrs[0] == "the sky is green"
    assert instrs[1] == ("Judge whether the following claim is true: "
                        "the sky is green")
    assert instrs[2] == ("Is the following statement accurate? "
                        "the sky is green")


def test_self_consistent_noul_custom_paraphrases_and_key():
    client = _sc_stub(0.7, 0.7, 0.7, key="final")
    result = self_consistent_noul(
        client, {}, "claim",
        paraphrases=["a", "b", "c"], question_key="final")
    assert result["verdict"] == "yes"
    _, questions = client.seen[0]
    assert set(questions) == {"final_p1", "final_p2", "final_p3"}


def test_final_verdict_agreement_marks_consistent_keeps_confidence():
    advisory = JevReasoner(
        StubJevClient(_answers_with_sc([0.80, 0.85, 0.78]))
    ).advise(_diagnosis())
    top = advisory.hypothesis_ranking[0]
    assert top.hypothesis_id == "h1"
    assert top.self_consistency == "consistent"
    assert top.jev_probability == pytest.approx(0.82)
    assert top.confidence == pytest.approx(0.82)
    as_dict = advisory.to_dict()
    assert as_dict["hypothesis_ranking"][0]["self_consistency"] == "consistent"


def test_final_verdict_disagreement_flags_uncertain_and_caps_confidence():
    advisory = JevReasoner(
        StubJevClient(_answers_with_sc([0.80, 0.20, 0.60]))
    ).advise(_diagnosis())
    top = advisory.hypothesis_ranking[0]
    # Surfaced honestly: the single-shot probability is preserved but the
    # confidence is capped at the genuine-uncertainty boundary and the
    # verdict is marked uncertain for the UI/report.
    assert top.self_consistency == "uncertain"
    assert top.confidence == pytest.approx(0.5)
    assert top.jev_probability == pytest.approx(0.82)
    as_dict = advisory.to_dict()
    assert as_dict["hypothesis_ranking"][0]["self_consistency"] == "uncertain"
    assert as_dict["hypothesis_ranking"][0]["confidence"] == pytest.approx(0.5)


def test_final_verdict_all_no_flags_uncertain():
    advisory = JevReasoner(
        StubJevClient(_answers_with_sc([0.20, 0.30, 0.15]))
    ).advise(_diagnosis())
    top = advisory.hypothesis_ranking[0]
    assert top.self_consistency == "uncertain"
    assert top.confidence == pytest.approx(0.5)


def test_final_verdict_check_unavailable_keeps_single_shot_verdict():
    # The stub never answers the sc trio (like the demo client): the
    # single-shot verdict stands, marked "n/a" — absence of a second
    # opinion is not a disagreement.
    audit = _FakeAudit()
    advisory = JevReasoner(
        StubJevClient(_STUB_ANSWERS), audit=audit).advise(_diagnosis())
    top = advisory.hypothesis_ranking[0]
    assert top.self_consistency == "n/a"
    assert top.confidence == pytest.approx(0.82)
    jev_events = [e for e in audit.events
                  if e["event"] == "jev_advisory"]
    assert jev_events[0]["details"]["self_consistency"]["verdict"] == "uncertain"
    assert "error" not in jev_events[0]["details"]["self_consistency"]


def test_final_verdict_consistency_never_raises_into_diagnosis():
    class _Flaky:
        def __init__(self):
            self.calls = 0

        def ask(self, state, questions):
            self.calls += 1
            if self.calls == 1:
                return dict(_STUB_ANSWERS)
            raise JevError("flaked on the re-ask")

    advisory = JevReasoner(_Flaky()).advise(_diagnosis())
    assert advisory.available is True
    top = advisory.hypothesis_ranking[0]
    assert top.self_consistency == "n/a"
    assert top.confidence == pytest.approx(0.82)


# ---------------------------------------------------------------------------
# Web wiring
# ---------------------------------------------------------------------------

class _Client:
    def __init__(self, base):
        self.base = base

    def get_json(self, path):
        import urllib.request
        with urllib.request.urlopen(self.base + path,
                                     timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def post_json(self, path, body=None):
        import urllib.request
        data = json.dumps(body or {}).encode("utf-8")
        req = urllib.request.Request(
            self.base + path, data=data,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))


@pytest.fixture(scope="module")
def web_client():
    server = make_server(port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield _Client(f"http://{host}:{port}")
    server.shutdown()
    server.server_close()


@pytest.fixture(autouse=True)
def _isolated_runs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("TYPESAFE_API_KEY", "RCA_JEV_ENABLED", "RCA_JEV_DEMO"):
        monkeypatch.delenv(var, raising=False)


def test_jev_status_route_reports_deterministic_without_key(web_client):
    status, data = web_client.get_json("/api/jev/status")
    assert status == 200
    assert data["mode"] == "deterministic"
    assert data["enabled"] is False


def test_diagnose_payload_carries_jev_block(web_client):
    status, created = web_client.post_json(
        "/api/runs", {"scenario": "channel_stopped"})
    assert status == 200
    status, diagnosis = web_client.post_json(
        f"/api/runs/{created['run_id']}/diagnose")
    assert status == 200
    jev = diagnosis["jev"]
    assert jev["mode"] == "deterministic"
    assert jev["available"] is False
    # Existing fields untouched.
    assert diagnosis["top_id"] == "h1"


def test_demo_mode_flows_through_diagnose(web_client, monkeypatch):
    monkeypatch.setenv("RCA_JEV_DEMO", "1")
    status, data = web_client.get_json("/api/jev/status")
    assert status == 200
    assert data["illustrative"] is True
    status, created = web_client.post_json(
        "/api/runs", {"scenario": "poison_message"})
    assert status == 200
    status, diagnosis = web_client.post_json(
        f"/api/runs/{created['run_id']}/diagnose")
    assert status == 200
    jev = diagnosis["jev"]
    assert jev["available"] is True
    assert jev["illustrative"] is True
    assert jev["hypothesis_ranking"], "ranking must be visible by default"
    assert jev["triage"]["priority"] in ("P1", "P2", "P3", "P4")
    # Risk is scored against the plan, not at diagnose time.
    assert jev["remediation_risk"] is None

    status, plan = web_client.post_json(
        f"/api/runs/{created['run_id']}/plan")
    assert status == 200
    assert plan["steps"], "poison_message should propose a plan"
    risk = plan["remediation_risk"]
    assert risk["risk"] in ("low", "medium", "high")
    assert risk["confidence"] >= 0.0
