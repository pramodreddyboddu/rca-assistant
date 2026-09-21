"""Incident room: one REAL incident through the REAL pipeline.

The incident is fixed data, not a simulation: queue manager QM1, channel
BATCH.CHL stopped, 200 messages backlogged on BACKLOG.Q -- exactly what
was recorded live against QM1 (IBM MQ 10.0.0.5) on 2026-09-21 and
committed under tests/fixtures/recorded/ibmmq/.

One run flows through the genuine production path:

    RecordedMQEstate (demo/live_estate.py: the real IBMQConnector read
      paths in recorder replay mode)
      -> build_tool_defs(include={read tools})   (mcp_server/tools.py)
      -> Gateway + InProcessClient               (authz + audit, same as prod)
      -> RCAEngine.diagnose(alert)               (agent/rca.py, read-only)
      -> JevReasoner.advise(diagnosis)           (agent/jev.py; advisory only)
      -> remediation plan                        (pure data, never executed)

Approval is REHEARSED, never executed: deciding "approve" records the
decision in the audit log and returns what WOULD run, with
executed=false. Nothing in this module can start a channel, clear a
queue, or open a driver session.

Verification replays a SECOND recorder pointed at
tests/fixtures/recorded/ibmmq_recovered/ (post-recovery state). If those
fixtures are absent the endpoint says so plainly -- it never invents a
recovered state.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from pathlib import Path

from agent import JevReasoner, RCAEngine
from audit import AuditLog
from connectors import ibmmq as _ibmmq
from connectors.recording import Recorder, RecordingError
from demo.live_estate import RecordedMQEstate
from mcp_server import Gateway, InProcessClient
from mcp_server.auth import SCOPES_ADMIN, TOKENS

MODE = "replay-demo"

EVIDENCE_SOURCE = {
    "recorded_at": "2026-09-21T22:29:54Z",
    "source": "live: QM1, IBM MQ 10.0.0.5",
    "note": ("Evidence replayed from recordings. "
             "No live queue manager was touched."),
}

ALERT = {
    "type": "mq_channel_backlog",
    "qmgr": "QM1",
    "channel": "BATCH.CHL",
    "queue": "BACKLOG.Q",
}

INCIDENT_TITLE = (
    "Channel BATCH.CHL stopped \u2014 200 messages backlogged on BACKLOG.Q"
)

# Read-only MQ tools registered for the incident gateway. Privileged tools
# (restart_channel, ...) are deliberately NOT registered: the engine is
# read-only and approval is rehearsed, so no privileged tool may exist on
# this path at all.
_READ_TOOLS = {"get_queue_depth", "get_channel_status",
               "read_error_log", "get_config"}

_READ_TOKEN = next(t for t, s in TOKENS.items() if s != SCOPES_ADMIN)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def _jev_json(advisory, diagnosis) -> dict | None:
    """Contract-shaped Jev block. Deterministic fallback when unavailable.

    JevReasoner.advise never raises for service problems: without a key it
    returns available=False in deterministic mode, which we surface
    honestly rather than hiding.
    """
    if (advisory is not None and advisory.available
            and advisory.hypothesis_ranking):
        top = advisory.hypothesis_ranking[0]
        return {
            "available": True,
            "mode": "live" if advisory.mode == "jev" else "deterministic",
            "top_probability": round(top.jev_probability, 3),
            "confidence": round(top.confidence, 3),
            "self_consistency": top.self_consistency,
            "uncertain": (top.self_consistency == "uncertain"
                          or top.confidence <= 0.5),
        }
    score = round(diagnosis.top.score, 3)
    return {
        "available": False,
        "mode": "deterministic",
        "top_probability": score,
        "confidence": score,
        "self_consistency": "n/a",
        "uncertain": score <= 0.5,
    }


def _build_plan(alert: dict, diagnosis) -> dict:
    """Remediation plan as pure data, shaped to the frozen API contract.

    The rationale quotes the diagnosis's own evidence claims, so it stays
    grounded in what the tools actually returned.
    """
    claims = "; ".join(c.claim for c in diagnosis.top.evidence)
    step = {
        "id": "step-1",
        "action": "restart_channel",
        "args": {"qmgr": alert["qmgr"], "channel": alert["channel"]},
        "rationale": (
            f"Top hypothesis '{diagnosis.top.title}' is fully supported by "
            f"evidence: {claims}. Restarting {alert['channel']} should "
            "resume message flow and drain the backlog. Privileged action: "
            "requires operator approval; demo mode only rehearses it, "
            "never executes."
        ),
        "verify": {
            "tool": "get_channel_status",
            "args": {"qmgr": alert["qmgr"], "channel": alert["channel"]},
            "expect": "RUNNING",
        },
    }
    return {"steps": [step]}


def run_incident() -> tuple[dict, AuditLog, dict]:
    """Run the full incident pipeline.

    Returns (contract response, audit log, plan record). The audit log is
    written under runs/<ts>-incident-<id>/audit.jsonl and carries every
    phase: incident_started, per-tool tool_call entries, diagnosis_complete,
    reasoning_complete (plus jev_advisory when Jev itself ran), and
    plan_proposed.
    """
    incident_id = secrets.token_hex(8)
    started_at = _now_iso()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    audit = AuditLog(Path("runs") / f"{ts}-incident-{incident_id}"
                     / "audit.jsonl")
    audit.append("incident_started", "incident-room",
                 {"incident_id": incident_id, "alert": dict(ALERT),
                  "mode": MODE, "evidence_source": dict(EVIDENCE_SOURCE)})

    estate = RecordedMQEstate()
    gateway = Gateway(estate, audit, include=_READ_TOOLS)
    client = InProcessClient(gateway, _READ_TOKEN)

    engine = RCAEngine(client, audit)
    diagnosis = engine.diagnose(dict(ALERT))

    advisory = JevReasoner.from_env(audit=audit).advise(diagnosis)
    audit.append("reasoning_complete", "incident-room",
                 {"incident_id": incident_id,
                  "jev_available": advisory.available,
                  "mode": ("live" if advisory.available
                           and advisory.mode == "jev"
                           else "deterministic")})

    plan = _build_plan(ALERT, diagnosis)
    audit.append("plan_proposed", "incident-room",
                 {"incident_id": incident_id,
                  "hypothesis_id": diagnosis.top.id,
                  "hypothesis_title": diagnosis.top.title,
                  "action": plan["steps"][0]["action"]})

    hypotheses = [
        {"id": h.id, "title": h.title, "score": round(h.score, 3),
         "evidence": [{"claim": c.claim, "tool": c.tool,
                       "excerpt": c.excerpt} for c in h.evidence]}
        for h in diagnosis.hypotheses
    ]
    response = {
        "mode": MODE,
        "incident": {
            "id": incident_id,
            "title": INCIDENT_TITLE,
            "mode": MODE,
            "evidence_source": dict(EVIDENCE_SOURCE),
            "started_at": started_at,
        },
        "evidence_calls": diagnosis.evidence_calls,
        "diagnosis": {
            "alert_type": ALERT["type"],
            "top": {"id": diagnosis.top.id,
                    "title": diagnosis.top.title,
                    "score": round(diagnosis.top.score, 3)},
            "hypotheses": hypotheses,
        },
        "jev": _jev_json(advisory, diagnosis),
        "plan": plan,
        "pending_approval": True,
    }
    plan_record = {"incident_id": incident_id, "plan": plan,
                   "audit": audit, "decided": False}
    return response, audit, plan_record


# ---------------------------------------------------------------------------
# Decide (rehearsal only)
# ---------------------------------------------------------------------------

def decide_incident(record: dict, decision: str) -> dict:
    """Record an approval decision. The fix is REHEARSED, never executed.

    Raises ValueError for anything but approve/reject; the caller maps that
    to a 400.
    """
    if decision not in ("approve", "reject"):
        raise ValueError(
            f"unknown decision: {decision!r}; expected 'approve' or 'reject'")
    step = record["plan"]["steps"][0]
    audit = record["audit"]
    action = "plan_approved" if decision == "approve" else "plan_rejected"
    entry = audit.append(action, "operator", {
        "incident_id": record["incident_id"],
        "decision": decision,
        "step": {"action": step["action"], "args": step["args"]},
        "execution": "rehearsal",
    })
    record["decided"] = True
    return {
        "mode": MODE,
        "evidence_source": dict(EVIDENCE_SOURCE),
        "decision": decision,
        "audit": {"ts": entry["ts"], "actor": "operator",
                  "action": action, "details": entry["details"]},
        "execution": {
            "executed": False,
            "rehearsal": True,
            "would_execute": {"action": step["action"],
                             "args": step["args"]},
            "note": ("Demo mode: no live queue manager. In production this "
                     "approval would execute restart_channel on QM1 via the "
                     "admin credential."),
        },
    }


# ---------------------------------------------------------------------------
# Verify (replays post-recovery fixtures; never invents a recovered state)
# ---------------------------------------------------------------------------

def _never_live():
    """The live thunk for verify calls: replay must never execute it."""
    raise AssertionError(
        "verify runs in replay mode; the live driver must not be called")


def _verify_checks() -> list[dict]:
    """Run the verify tools through the ibmmq_recovered recorder.

    Raises RecordingError when the post-recovery fixtures are absent --
    the caller turns that into an honest "not available" answer.
    """
    rec = Recorder("ibmmq_recovered", default_mode="replay")
    cmqc, cfc = _ibmmq._pcf_attrs()

    raw_status = rec.call(
        "inquire_channel_status",
        {"op": "MQCMD_INQUIRE_CHANNEL_STATUS",
         "qmgr": "QM1", "channel": "BATCH.CHL"},
        _never_live,
    )
    code = int(raw_status[0][cfc.MQIACH_CHANNEL_STATUS])
    status = _ibmmq._CHANNEL_STATUS_NAMES.get(code, f"UNKNOWN({code})")

    raw_depth = rec.call(
        "inquire_q",
        {"op": "MQCMD_INQUIRE_Q", "qmgr": "QM1", "queue": "BACKLOG.Q"},
        _never_live,
    )
    depth = int(raw_depth[0][cmqc.MQIA_CURRENT_Q_DEPTH])

    return [
        {"tool": "get_channel_status",
         "args": {"qmgr": "QM1", "channel": "BATCH.CHL"},
         "observed": status, "expected": "RUNNING",
         "pass": status == "RUNNING"},
        {"tool": "get_queue_depth",
         "args": {"qmgr": "QM1", "queue": "BACKLOG.Q"},
         "observed": depth, "expected": 0,
         "pass": depth == 0},
    ]


def verify_incident() -> dict:
    """Verify recovery against post-recovery recordings.

    Honest when the recordings are missing: recovered=false with a note
    that says why, instead of a fabricated recovered state.
    """
    try:
        checks = _verify_checks()
    except RecordingError:
        return {
            "mode": MODE,
            "evidence_source": dict(EVIDENCE_SOURCE),
            "available": False,
            "recovered": False,
            "checks": [],
            "note": ("No post-recovery recordings available: "
                     "tests/fixtures/recorded/ibmmq_recovered/ is empty or "
                     "missing. The recovery step (start BATCH.CHL, drain "
                     "BACKLOG.Q) was never recorded into these fixtures, so "
                     "there is nothing to replay. Verification will replay "
                     "real post-recovery evidence once it is recorded."),
        }
    recovered = all(c["pass"] for c in checks)
    return {
        "mode": MODE,
        "evidence_source": dict(EVIDENCE_SOURCE),
        "available": True,
        "recovered": recovered,
        "checks": checks,
        "note": ("Verification replayed from post-recovery recordings "
                 "(channel started and queue drained live on QM1, "
                 "2026-09-21)."),
    }


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

def audit_entries_json(audit: AuditLog) -> list[dict]:
    """Audit entries in the contract shape (event -> action)."""
    return [{"ts": e["ts"], "actor": e["actor"], "action": e["event"],
             "details": e["details"]}
            for e in audit.entries()]
