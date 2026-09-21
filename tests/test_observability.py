"""Tests for demo.observability: metrics exposition, healthz, redaction,
JSON logging, run history, and the web.py wiring contract."""

import io
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from demo import observability
from demo.observability import (
    MetricsRegistry,
    RunHistory,
    get_healthz,
    get_logger,
    handle_healthz,
    handle_metrics,
    note_jev_judgment,
    redact,
)


@pytest.fixture()
def fresh_registry():
    reg = MetricsRegistry()
    reg.define("rca_runs_total", "counter", "Runs.", ("scenario", "status"))
    reg.define("rca_jev_enabled", "gauge", "Jev enabled.")
    return reg


# ----------------------------------------------------------------- metrics ---

def test_exposition_has_help_and_type(fresh_registry):
    fresh_registry.inc("rca_runs_total", {"scenario": "mq-backlog", "status": "done"})
    fresh_registry.set("rca_jev_enabled", value=1.0)
    out = fresh_registry.exposition()
    assert "# HELP rca_runs_total" in out
    assert "# TYPE rca_runs_total counter" in out
    assert "# TYPE rca_jev_enabled gauge" in out


def test_exposition_label_rendering(fresh_registry):
    fresh_registry.inc("rca_runs_total", {"scenario": "mq-backlog", "status": "done"})
    fresh_registry.inc("rca_runs_total", {"scenario": "mq-backlog", "status": "done"})
    out = fresh_registry.exposition()
    line = [ln for ln in out.splitlines()
            if ln.startswith("rca_runs_total{")][0]
    assert line == 'rca_runs_total{scenario="mq-backlog",status="done"} 2'


def test_exposition_escapes_label_values(fresh_registry):
    fresh_registry.inc("rca_runs_total", {"scenario": 'a"b\\c', "status": "done"})
    out = fresh_registry.exposition()
    assert 'scenario="a\\"b\\\\c"' in out


def test_exposition_gauge_without_labels(fresh_registry):
    fresh_registry.set("rca_jev_enabled", value=0.0)
    out = fresh_registry.exposition()
    assert "rca_jev_enabled 0" in out


def test_module_registry_exposes_required_metrics():
    required = [
        "rca_runs_total", "rca_approvals_total", "rca_privileged_actions_total",
        "rca_audit_entries_total", "rca_jev_judgments_total",
        "rca_jev_enabled", "rca_uptime_seconds",
    ]
    for name in required:
        assert name in observability.registry._meta, name
    out = observability.registry.exposition()
    assert "# TYPE rca_jev_judgments_total counter" in out
    assert "# TYPE rca_uptime_seconds gauge" in out


def test_inc_rejects_unknown_counter(fresh_registry):
    with pytest.raises(KeyError):
        fresh_registry.inc("nope_total")


def test_inc_rejects_bad_labels(fresh_registry):
    with pytest.raises(ValueError):
        fresh_registry.inc("rca_runs_total", {"bogus": "x"})


# ----------------------------------------------------------------- healthz ---

def test_healthz_shape():
    jev = {"enabled": True, "mode": "jev", "key_configured": True}
    body = get_healthz(jev)
    assert body["status"] == "ok"
    assert body["version"] == observability.VERSION == "0.3.0"
    assert isinstance(body["uptime_s"], int) and body["uptime_s"] >= 0
    assert body["jev"] == jev


def test_handle_healthz_contract():
    status, body = handle_healthz({"enabled": False})
    assert status == 200
    assert body["status"] == "ok"
    assert body["jev"]["enabled"] is False


def test_handle_metrics_contract():
    status, body, content_type = handle_metrics()
    assert status == 200
    assert content_type == "text/plain; version=0.0.4"
    assert "# TYPE rca_runs_total counter" in body
    assert body.endswith("\n")


# ----------------------------------------------------------------- redact ----

def test_redact_nested_dicts_and_lists():
    payload = {
        "username": "pramod",
        "password": "hunter2",
        "nested": {"API_KEY": "sk-secret", "list": [
            {"authorization": "Bearer abc", "ok": 1},
            "plain",
        ]},
        "TYPESAFE_API_KEY": "sk-live-should-never-leak",
    }
    out = redact(payload)
    assert out["password"] == "***REDACTED***"
    assert out["nested"]["API_KEY"] == "***REDACTED***"
    assert out["nested"]["list"][0]["authorization"] == "***REDACTED***"
    assert out["nested"]["list"][0]["ok"] == 1
    assert out["nested"]["list"][1] == "plain"
    assert out["username"] == "pramod"
    assert out["TYPESAFE_API_KEY"] == "***REDACTED***"
    # input untouched
    assert payload["password"] == "hunter2"


def test_redact_case_insensitive_secret_token():
    out = redact({"Secret": "x", "authToken": "y", "safe": "z"})
    assert out == {"Secret": "***REDACTED***",
                   "authToken": "***REDACTED***",
                   "safe": "z"}


def test_redact_passthrough_scalars():
    assert redact(42) == 42
    assert redact("hello") == "hello"
    assert redact(None) is None


# ----------------------------------------------------------------- logging ---

def _read_json_line(buf: io.StringIO) -> dict:
    buf.seek(0)
    line = buf.readline()
    assert line, "no log output captured"
    return json.loads(line)


def test_logger_emits_json_on_stderr():
    buf = io.StringIO()
    logger = get_logger("rca.test.observability.json", stream=buf)
    logger.info("run finished", scenario="mq-backlog", steps=7)
    rec = _read_json_line(buf)
    assert rec["level"] == "INFO"
    assert rec["logger"] == "rca.test.observability.json"
    assert rec["msg"] == "run finished"
    assert rec["scenario"] == "mq-backlog"
    assert rec["steps"] == 7
    assert "ts" in rec  # ISO-8601 timestamp present
    assert isinstance(rec["ts"], str)


def test_logger_auto_redacts_fields():
    buf = io.StringIO()
    logger = get_logger("rca.test.observability.redact", stream=buf)
    logger.warning("connecting", api_key="sk-live-123", host="example.com")
    rec = _read_json_line(buf)
    assert rec["api_key"] == "***REDACTED***"
    assert rec["host"] == "example.com"
    assert "sk-live-123" not in buf.getvalue()


def test_logger_never_logs_raw_typesafe_key():
    buf = io.StringIO()
    logger = get_logger("rca.test.observability.key", stream=buf)
    logger.info("jev client init", TYPESAFE_API_KEY="ts1-abc-def")
    rec = _read_json_line(buf)
    assert rec["TYPESAFE_API_KEY"] == "***REDACTED***"
    assert "ts1-abc-def" not in buf.getvalue()


def test_logger_redacts_nested_extra_fields():
    buf = io.StringIO()
    logger = get_logger("rca.test.observability.nested", stream=buf)
    logger.error("boom", config={"password": "p@ss", "retries": 3})
    rec = _read_json_line(buf)
    assert rec["config"] == {"password": "***REDACTED***", "retries": 3}


# ------------------------------------------------------------- run history ---

def test_run_history_round_trip_newest_first(tmp_path):
    hist = RunHistory(path=str(tmp_path / "history.jsonl"))
    assert hist.list() == []  # missing file -> empty
    hist.record({"run_id": "r1", "scenario": "mq-backlog",
                 "top_hypothesis": "consumer-stall", "jev_mode": "jev",
                 "plan_status": "approved", "steps": 5})
    hist.record({"run_id": "r2", "scenario": "kafka-lag",
                 "top_hypothesis": "broker-down", "jev_mode": "deterministic",
                 "plan_status": "draft", "steps": 3})
    entries = hist.list(limit=50)
    assert [e["run_id"] for e in entries] == ["r2", "r1"]
    assert all("ts" in e for e in entries)
    assert entries[0]["scenario"] == "kafka-lag"
    # limit respected
    assert len(hist.list(limit=1)) == 1
    assert hist.list(limit=1)[0]["run_id"] == "r2"


def test_run_history_skips_corrupt_lines(tmp_path):
    path = tmp_path / "history.jsonl"
    path.write_text('{"run_id": "good"}\nnot-json\n{"run_id": "alsogood"}\n')
    hist = RunHistory(path=str(path))
    assert [e["run_id"] for e in hist.list()] == ["alsogood", "good"]


# -------------------------------------------------------- wiring: jev note ---

def test_note_jev_judgment_increments_counter():
    reg = observability.registry
    before = dict(reg._counters["rca_jev_judgments_total"])
    note_jev_judgment("hypothesis_rank", "ok")
    note_jev_judgment("evidence_verify", "error")
    after = reg._counters["rca_jev_judgments_total"]
    assert after.get(("hypothesis_rank", "ok"), 0.0) == \
        before.get(("hypothesis_rank", "ok"), 0.0) + 1.0
    assert after.get(("evidence_verify", "error"), 0.0) == \
        before.get(("evidence_verify", "error"), 0.0) + 1.0


def test_note_jev_judgment_validates():
    with pytest.raises(ValueError):
        note_jev_judgment("bogus_kind", "ok")
    with pytest.raises(ValueError):
        note_jev_judgment("triage", "bogus_result")


def test_note_jev_judgment_visible_in_exposition():
    note_jev_judgment("triage", "unavailable")
    out = observability.registry.exposition()
    assert 'rca_jev_judgments_total{kind="triage",result="unavailable"}' in out
