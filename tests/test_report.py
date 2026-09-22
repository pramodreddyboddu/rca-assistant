"""Tests for the incident report export (agent/report.py + demo/report.py).

Synthetic audit logs are built directly with AuditLog.append so these tests
do not depend on the engine or CLI flow: later refactors of those cannot
break the report contract.
"""

import json

from agent.report import generate_markdown
from audit import AuditLog


def _new_log(tmp_path):
    """Fresh run dir with a new audit log at <dir>/audit.jsonl."""
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    return run_dir, AuditLog(run_dir / "audit.jsonl")


def test_multi_step_flow(tmp_path):
    run_dir, log = _new_log(tmp_path)
    log.append("incident_injected", "demo", {
        "scenario": "kafka_lag",
        "alert": {"type": "consumer_lag", "topic": "payments-events",
                  "group": "payments-consumers"},
    })
    log.append("diagnosis_complete", "rca-engine", {
        "alert_type": "consumer_lag",
        "top_hypothesis": "h2",
        "score": 0.9,
        "title": "Consumer group stalled",
        "evidence": [
            {"claim": "consumer lag is growing", "source": "get_kafka_consumer_lag"},
            {"claim": "no consumer errors in logs", "source": "read_error_log"},
        ],
    })
    log.append("plan_proposed", "rca-engine", {
        "steps": [
            {"id": "s1", "action": "restart_connector"},
            {"id": "s2", "action": "verify_lag"},
        ],
    })
    log.append("approval_requested", "approval-gate",
               {"request_id": "APR-1", "action": "restart_connector"})
    log.append("approval_requested", "approval-gate",
               {"request_id": "APR-2", "action": "verify_lag"})
    log.append("approval_decided", "approval-gate",
               {"request_id": "APR-1", "approved": True, "decided_by": "human"})
    log.append("approval_decided_all", "approval-gate",
               {"request_ids": ["APR-1", "APR-2"], "approved": True,
                "decided_by": "human"})
    log.append("plan_step_executed", "plan-runner",
               {"step_id": "s1", "action": "restart_connector"})
    log.append("plan_step_executed", "plan-runner",
               {"step_id": "s2", "action": "verify_lag"})
    log.append("plan_step_verified", "plan-runner", {"step_id": "s2", "ok": True})
    log.append("plan_completed", "plan-runner", {"steps": 2})

    md = generate_markdown(run_dir)

    assert "Consumer group stalled" in md          # hypothesis title
    assert "APR-1" in md                            # an approval request id
    assert "approve-all" in md                      # approve-all explicitly noted
    assert "verified: OK" in md                     # verification-ok line
    assert "VALID" in md                            # audit integrity
    assert "# Incident report" in md
    assert "## Timeline" in md and "plan_completed" in md


def test_legacy_single_action_flow_denied(tmp_path):
    run_dir, log = _new_log(tmp_path)
    log.append("incident_injected", "demo", {
        "scenario": "channel_stopped",
        "alert": {"type": "queue_backlog", "qmgr": "QMGR1",
                  "queue": "PAYMENTS.IN", "observed_depth": 48500},
    })
    log.append("diagnosis_complete", "rca-engine",
               {"alert_type": "queue_backlog", "top_hypothesis": "h1", "score": 1.0})
    log.append("approval_requested", "approval-gate",
               {"request_id": "APR-x", "action": "restart_channel"})
    log.append("approval_decided", "approval-gate",
               {"request_id": "APR-x", "approved": False, "decided_by": "human"})

    md = generate_markdown(run_dir)

    assert "DENIED" in md or "denied" in md
    assert "No actions taken" in md
    assert "VALID" in md
    # Legacy flow: no plan sections break the render.
    assert "No diagnosis recorded." not in md  # diagnosis IS present
    assert "step-by-step" in md  # no approve-all used


def test_tampered_log_still_renders_invalid(tmp_path):
    run_dir, log = _new_log(tmp_path)
    log.append("incident_injected", "demo", {"scenario": "kafka_lag"})
    path = run_dir / "audit.jsonl"

    # Flip one byte inside the stored hash of the last entry: the JSON stays
    # valid, so entries() still parses, but the chain no longer verifies.
    lines = path.read_text(encoding="utf-8").splitlines()
    entry = json.loads(lines[-1])
    old_hash = entry["hash"]
    entry["hash"] = ("0" if old_hash[0] != "0" else "1") + old_hash[1:]
    assert entry["hash"] != old_hash
    lines[-1] = json.dumps(entry)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    md = generate_markdown(run_dir)  # must not raise
    assert "INVALID" in md
    assert "# Incident report" in md


def test_missing_audit_log_renders_gracefully(tmp_path):
    run_dir = tmp_path / "empty-run"
    run_dir.mkdir()
    md = generate_markdown(run_dir)  # no audit.jsonl at all
    assert "INVALID" in md
    assert "No incident recorded." in md
    assert "No actions taken recorded." in md


def test_malformed_details_never_crash(tmp_path):
    run_dir, log = _new_log(tmp_path)
    # details may be a non-dict JSON value; report must not crash.
    log.append("weird_event", "someone", ["not", "a", "dict"])
    log.append("diagnosis_complete", "rca-engine", {})
    md = generate_markdown(run_dir)
    assert "weird_event" in md  # unknown event type rendered generically
    assert "VALID" in md


def test_diagnosis_param_enriches_missing_title(tmp_path):
    run_dir, log = _new_log(tmp_path)
    log.append("diagnosis_complete", "rca-engine",
               {"alert_type": "queue_backlog", "top_hypothesis": "h1",
                "score": 1.0})
    md = generate_markdown(run_dir, diagnosis={
        "hypotheses": [{"id": "h1", "title": "Receiver channel stopped",
                        "evidence": [{"claim": "channel in STOPPED state",
                                      "source": "get_channel_status"}]}],
    })
    assert "Receiver channel stopped" in md
    assert "get_channel_status" in md


def test_cli_writes_file_and_warns_on_invalid(tmp_path, capsys):
    from demo.report import main

    run_dir, log = _new_log(tmp_path)
    log.append("incident_injected", "demo", {"scenario": "kafka_lag"})
    path = run_dir / "audit.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    entry = json.loads(lines[-1])
    # Flip the first hex char to a *different* value: replacing it with "f"
    # is a no-op when the hash already starts with "f" (1/16 of runs),
    # which would leave the chain VALID and make this test flaky.
    first = entry["hash"][0]
    entry["hash"] = ("0" if first != "0" else "1") + entry["hash"][1:]
    lines[-1] = json.dumps(entry)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    out = tmp_path / "report.md"
    rc = main([str(run_dir), "--out", str(out)])
    assert rc == 0
    assert out.exists()
    assert "INVALID" in out.read_text(encoding="utf-8")
    captured = capsys.readouterr()
    assert "WARNING" in captured.err and "INVALID" in captured.err
    assert str(out) in captured.out


def test_cli_stdout_mode(tmp_path, capsys):
    from demo.report import main

    run_dir, log = _new_log(tmp_path)
    log.append("incident_injected", "demo", {"scenario": "kafka_lag"})
    rc = main([str(run_dir)])
    assert rc == 0
    captured = capsys.readouterr()
    assert "# Incident report" in captured.out
    assert captured.err == ""  # valid chain: no stderr warning
