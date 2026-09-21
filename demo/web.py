"""Local web UI for the RCA assistant reference demo.

Run:  python -m demo.web [--port 8765]   then open http://127.0.0.1:8765

This is the same demo as demo/run_incident.py behind a browser UI for
screen-sharing customer discovery calls: inject an incident, run the
deterministic RCA engine over read-only MCP tools, propose a remediation
plan, approve each privileged step (or reject / approve all remaining),
and download the markdown incident report.

BINDING
-------
The server binds 127.0.0.1 ONLY, never 0.0.0.0. The UI has no
authentication and drives privileged-action approvals, so it must not be
reachable from the network: loopback-only keeps it a local demo tool. If
you ever need remote access, put it behind an authenticated reverse proxy
and real identity tokens -- do not just rebind to 0.0.0.0.

SAFETY
------
- Reuses the EXACT code paths the CLI uses: RCAEngine.diagnose(),
  RCAEngine.propose_plan(), ApprovalGate.request_plan/decide/approve_all,
  and agent.plans.execute_approved_step(). No duplicated logic.
- The engine still never touches privileged tools; approvals are explicit
  per step; the gate still defaults to deny. The UI cannot approve
  out-of-order: the API 400s any decision whose request id is not the
  current pending step's request.
- Stdlib only (http.server, json, urllib-style parsing, secrets).
- Every exception in a handler becomes a 500 {"error": msg}; tracebacks
  are never sent to the browser.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import threading
import urllib.parse
from dataclasses import asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from agent import (ApprovalGate, RCAEngine, StepOutcome,
                   execute_approved_step, JevReasoner)
from agent.report import generate_markdown
from audit import AuditLog
from connectors.linux_host import LinuxHostConnector
from mcp_server import Gateway, InProcessClient
from mcp_server.auth import SCOPES_ADMIN, TOKENS
from sim import SCENARIOS, Estate, inject

try:  # observability workstream may not have landed yet
    from demo.observability import (handle_healthz, handle_metrics,
                                    note_approval, note_jev_judgment,
                                    note_privileged_action, note_run,
                                    set_jev_enabled)
    _OBSERVABILITY = True
except ImportError:  # pragma: no cover - until the module lands
    _OBSERVABILITY = False

    def note_jev_judgment(kind: str, result: str) -> None:
        pass

    def set_jev_enabled(enabled: bool) -> None:
        pass

    def note_run(scenario: str, status: str) -> None:
        pass

    def note_approval(decision: str) -> None:
        pass

    def note_privileged_action(tool: str) -> None:
        pass


def _note_jev_metrics(advisory) -> None:
    """Feed per-kind Jev judgment outcomes to the metrics registry."""
    if not advisory.available:
        note_jev_judgment("hypothesis_rank", "unavailable")
        note_jev_judgment("evidence_verify", "unavailable")
        note_jev_judgment("triage", "unavailable")
        note_jev_judgment("risk", "unavailable")
        return
    result = "error" if advisory.error else "ok"
    note_jev_judgment("hypothesis_rank", result)
    note_jev_judgment("evidence_verify", result)
    note_jev_judgment("triage", result)
    note_jev_judgment("risk", result)

# Demo-only bearer tokens (see mcp_server/auth.py), same selection as
# demo/run_incident.py. Production must use a real identity provider.
_READ_TOKEN = next(t for t, s in TOKENS.items() if s != SCOPES_ADMIN)
_ADMIN_TOKEN = next(t for t, s in TOKENS.items() if s == SCOPES_ADMIN)

_BIND_HOST = "127.0.0.1"  # loopback only; see module docstring
_DEFAULT_PORT = 8765
_MAX_BODY = 1024 * 1024  # 1 MB; larger request bodies get 413


def _bind_host() -> str:
    """Bind address: RCA_BIND env or 127.0.0.1.

    0.0.0.0 is honored ONLY for containerized runs (Docker): inside a
    container the container network is the trust boundary and the compose
    file controls port publishing. On a bare host the default stays
    loopback-only because this UI has no authentication and drives
    privileged-action approvals.
    """
    return os.environ.get("RCA_BIND", _BIND_HOST)


def _default_port() -> int:
    try:
        return int(os.environ.get("RCA_PORT", _DEFAULT_PORT))
    except ValueError:
        return _DEFAULT_PORT

_STATIC_DIR = Path(__file__).with_name("static")
_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
}

_RUN_ID_RE = r"[0-9a-f]{16}"
_API_RUN_RE = re.compile(rf"^/api/runs/({_RUN_ID_RE})/(diagnose|plan|decide|audit|report)$")

_DECISIONS = ("approve", "reject", "approve_all")

_SCENARIO_DESCRIPTIONS = {
    "channel_stopped": "Receiver channel PAYMENTS.RCVR stopped; backlog of "
                       "48,500 messages building on PAYMENTS.IN.",
    "disk_full": "App host app01 at 97% disk; payments-api cannot write logs.",
    "kafka_lag": "Kafka consumer group payments-consumers stalled at "
                 "250,000 lag on payments-events.",
    "tomcat_oom": "payments-api throwing OutOfMemoryError; app01 memory at 96%.",
    "expired_tls": "TLS certificate expired for channel PAYMENTS.RCVR "
                   "(channel RETRYING).",
    "config_drift": "Queue PAYMENTS.IN MAXDEPTH lowered to 5,000, below its "
                    "baseline.",
    "listener_down": "MQ listener TCP.LISTENER stopped; inbound connections "
                     "refused.",
    "poison_message": "A poison message keeps backing out on ORDERS.IN, "
                      "blocking the queue.",
    "kafka_broker_config_drift": "Broker 3 log.retention.hours drifted to "
                                 "72h vs 168h; orders-events lag rising.",
    "tomcat_thread_exhaustion": "http-nio-8080 thread pool saturated "
                                "(200/200); /orders slowed.",
    "kafka_under_replicated": "payments-events partition 2 under-replicated: "
                              "ISR smaller than the replica set.",
    "websphere_thread_saturation": "WebSphere WebContainer thread pool "
                                   "saturated on payments-ear.",
    "websphere_app_stopped": "WebSphere app orders-ear stopped; requests "
                             "failing.",
    "weblogic_heap_pressure": "WebLogic JVM heap high on payments-app; GC "
                              "pressure rising.",
    "weblogic_stuck_threads": "Stuck threads detected on WebLogic "
                              "payments-app.",
    "jboss_deployment_failed": "JBoss deployment payments.war failed to start.",
    "jboss_heap_high": "JBoss heap high on orders.war; frequent GC.",
    "rabbitmq_queue_backlog": "RabbitMQ queue payments.in (vhost /) backing "
                              "up.",
    "rabbitmq_disk_alarm": "RabbitMQ node rmq01 raised a disk resource alarm.",
    "artemis_queue_backlog": "Artemis queue PAYMENTS.IN backing up.",
    "artemis_broker_down": "ActiveMQ Artemis broker down; producers "
                           "failing.",
    "ems_queue_backlog": "TIBCO EMS queue PAYMENTS.IN backing up.",
    "ems_connection_storm": "EMS connection storm: thousands of new "
                            "connections per minute.",
    "nginx_upstream_5xx": "Nginx upstream returning 5xx errors.",
    "nginx_worker_crash": "Nginx worker processes crashing; connections "
                          "dropping.",
    "apache_workers_saturated": "Apache HTTPD worker slots saturated; "
                                "requests queueing.",
    "apache_5xx_spike": "Apache backend 5xx error spike.",
    "haproxy_backend_down": "HAProxy backend payments_api server pay03 down.",
    "haproxy_session_saturation": "HAProxy frontend https_in session limit "
                                  "reached.",
    "postgres_blocking": "Postgres blocking session (pid 4821) holding locks.",
    "postgres_replication_lag": "Postgres standby replication lag high.",
    "mysql_runaway_query": "MySQL runaway query (process 90210) consuming "
                           "resources.",
    "mysql_replication_lag": "MySQL replica replication lag high.",
    "oracle_tablespace_full": "Oracle tablespace USERS nearly full.",
    "oracle_blocking_session": "Oracle blocking session (sid 123) holding "
                               "locks.",
    "redis_memory_pressure": "Redis memory usage high; eviction imminent.",
    "redis_replication_down": "Redis replica replication down.",
    "elasticsearch_red": "Elasticsearch cluster health RED.",
    "elasticsearch_heap_pressure": "Elasticsearch JVM heap pressure high.",
    "mongo_long_op": "MongoDB long-running operation (opid 77123).",
    "mongo_replset_lag": "MongoDB replica set secondary lag high.",
    "k8s_crashloop": "Kubernetes deployment payments-api CrashLoopBackOff.",
    "k8s_deployment_stalled": "Kubernetes deployment payments-api rollout "
                              "stalled.",
    "docker_container_exited": "Docker container payments-api exited.",
    "docker_memory_pressure": "Docker container payments-api memory pressure "
                              "high.",
}


# ---------------------------------------------------------------------------
# Errors and serialization helpers
# ---------------------------------------------------------------------------

class _HTTPError(Exception):
    """A handler failure that maps to an HTTP status + JSON error body."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _hypothesis_to_json(h) -> dict:
    return {
        "id": h.id,
        "title": h.title,
        "score": h.score,
        "evidence": [
            {"claim": c.claim, "tool": c.tool, "excerpt": c.excerpt}
            for c in h.evidence
        ],
    }


def _step_to_json(step) -> dict:
    verify = step.verify
    return {
        "id": step.id,
        "action": step.action,
        "args": step.args,
        "rationale": step.rationale,
        "verify": ({"tool": verify.tool, "args": verify.args,
                    "expect": verify.expect}
                   if verify is not None else None),
        "required_scope": step.required_scope,
    }


def _outcome_to_json(outcome: StepOutcome) -> dict:
    return asdict(outcome)


# ---------------------------------------------------------------------------
# App state (one per server instance; all mutation under the lock)
# ---------------------------------------------------------------------------

class _AppState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.runs: dict[str, dict] = {}
        self._host_audit: AuditLog | None = None

    def host_audit(self) -> AuditLog:
        """Lazy audit log shared by /api/host/compare calls."""
        if self._host_audit is None:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self._host_audit = AuditLog(
                Path("runs") / f"{ts}-host" / "audit.jsonl")
        return self._host_audit


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def _handler_class(app: _AppState):
    """Build the request-handler class bound to one app state."""

    class WebHandler(BaseHTTPRequestHandler):
        server_version = "RCAWebUI/1.0"

        # -- plumbing ------------------------------------------------
        def log_message(self, fmt, *args):  # noqa: D102 - stdlib hook
            pass  # keep the demo quiet; audit log is the record

        def _send_json(self, status: int, obj: dict) -> None:
            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, status: int, body: str,
                       content_type: str = "text/plain; charset=utf-8") -> None:
            raw = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_static(self, name: str) -> None:
            path = _STATIC_DIR / name
            if not path.is_file():
                self._send_json(404, {"error": "not found"})
                return
            data = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type",
                             _CONTENT_TYPES.get(path.suffix,
                                                "application/octet-stream"))
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _drain_body(self, length: int) -> None:
            """Read and discard an oversized request body in chunks.

            Draining (instead of closing mid-upload) lets the client finish
            its send and actually read the 413 response instead of seeing a
            reset connection. Chunks keep memory bounded; the connection
            closes after the response either way.
            """
            remaining = length
            while remaining > 0:
                chunk = self.rfile.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)

        def _read_body(self) -> bytes:
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            if length > _MAX_BODY:
                self._drain_body(length)
                raise _HTTPError(413, "request body too large (max 1 MB)")
            if length < 0:
                length = 0
            return self.rfile.read(length) if length else b""

        def _json_body(self) -> dict:
            raw = self._read_body()
            if not raw:
                raise _HTTPError(400, "missing JSON request body")
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                raise _HTTPError(400, "request body is not valid JSON")
            if not isinstance(data, dict):
                raise _HTTPError(400, "request body must be a JSON object")
            return data

        def _get_run(self, run_id: str) -> dict:
            run = app.runs.get(run_id)
            if run is None:
                raise _HTTPError(404, f"unknown run: {run_id}")
            return run

        def _current_pending(self, run: dict) -> tuple[object, dict]:
            """(current PlanStep, its approval request) or raise 400."""
            plan = run["plan"]
            idx = run["current_index"]
            if plan is None or idx >= len(run["requests"]):
                raise _HTTPError(400, "no pending plan step for this run")
            return plan.steps[idx], run["requests"][idx]

        def _halt_plan(self, run: dict, step_id: str, status: str,
                       reason: str) -> None:
            """Mirror agent.plans.run_plan's halt bookkeeping."""
            run["audit"].append(
                "plan_halted", "plan-runner",
                {"plan_id": run["plan_id"], "reason": reason,
                 "at_step": step_id})
            run["status"] = status

        # -- API: run lifecycle --------------------------------------
        def _api_create_run(self) -> None:
            data = self._json_body()
            scenario = data.get("scenario")
            if scenario not in SCENARIOS:
                raise _HTTPError(
                    400, f"unknown scenario: {scenario!r}; "
                         f"expected one of {SCENARIOS}")

            run_id = secrets.token_hex(8)
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            run_dir = Path("runs") / f"{ts}-{run_id}"

            # Same construction as demo/run_incident.py main().
            estate = Estate(seed=42)
            audit = AuditLog(run_dir / "audit.jsonl")
            gateway = Gateway(estate, audit)
            read_client = InProcessClient(gateway, _READ_TOKEN)
            admin_client = InProcessClient(gateway, _ADMIN_TOKEN)
            engine = RCAEngine(read_client, audit)
            gate = ApprovalGate(audit)

            alert = inject(estate, scenario)
            audit.append("incident_injected", actor="demo",
                         details={"scenario": scenario, "alert": alert})
            note_run(scenario, "injected")

            app.runs[run_id] = {
                "run_id": run_id,
                "run_dir": run_dir,
                "scenario": scenario,
                "alert": alert,
                "estate": estate,
                "audit": audit,
                "gateway": gateway,
                "read_client": read_client,
                "admin_client": admin_client,
                "engine": engine,
                "gate": gate,
                "diagnosis": None,
                "plan": None,
                "plan_id": None,
                "requests": [],
                "current_index": 0,
                "outcomes": [],
                "status": "injected",
            }
            self._send_json(200, {"run_id": run_id, "alert": alert,
                                  "scenario": scenario})

        def _api_diagnose(self, run: dict) -> None:
            engine = run["engine"]
            diagnosis = engine.diagnose(run["alert"])
            run["diagnosis"] = diagnosis
            # Jev is the DEFAULT reasoning experience when a key is
            # configured; otherwise the deterministic engine stands alone
            # and the advisory carries the "deterministic mode" label.
            # Advisory only: it never changes the diagnosis or the plan.
            reasoner = JevReasoner.from_env(audit=run["audit"])
            advisory = reasoner.advise(diagnosis)
            run["jev_advisory"] = advisory
            _note_jev_metrics(advisory)
            set_jev_enabled(advisory.available and advisory.mode == "jev")
            run["status"] = "diagnosed"
            self._send_json(200, {
                "hypotheses": [_hypothesis_to_json(h)
                               for h in diagnosis.hypotheses],
                "top_id": diagnosis.top.id,
                "evidence_calls": len(diagnosis.evidence_calls),
                "jev": advisory.to_dict(),
            })

        def _api_plan(self, run: dict) -> None:
            diagnosis = run["diagnosis"]
            if diagnosis is None:
                raise _HTTPError(400, "diagnose the run before proposing a plan")
            plan = run["engine"].propose_plan(diagnosis.top)
            if plan is None:
                run["status"] = "no_plan"
                self._send_json(200, {
                    "plan": None,
                    "note": ("No safe remediation proposed for this "
                             "hypothesis; manual investigation required."),
                })
                return
            gate = run["gate"]
            requests = gate.request_plan(plan)
            plan_id = requests[0]["plan_id"]
            run["plan"] = plan
            run["plan_id"] = plan_id
            run["requests"] = requests
            run["current_index"] = 0
            run["outcomes"] = []
            run["status"] = "plan_proposed"
            # Remediation risk is only meaningful against a real plan, so it
            # is scored here — not at diagnose time. Advisory only: it never
            # changes the plan or the approval gate.
            risk = JevReasoner.from_env(
                audit=run["audit"]).advise_remediation_risk(diagnosis, plan)
            advisory = run.get("jev_advisory")
            if advisory is not None and risk is not None:
                advisory.remediation_risk = risk
            self._send_json(200, {
                "plan_id": plan_id,
                "title": plan.title,
                "hypothesis_id": plan.hypothesis_id,
                "steps": [_step_to_json(s) for s in plan.steps],
                "remediation_risk": (
                    None if risk is None else {
                        "risk": risk.risk,
                        "risk_score": round(risk.risk_score, 3),
                        "confidence": round(risk.confidence, 3)}),
                "pending_request": {
                    "request_id": requests[0]["id"],
                    "step_id": requests[0]["step_id"],
                },
            })

        def _api_decide(self, run: dict) -> None:
            data = self._json_body()
            request_id = data.get("request_id")
            decision = data.get("decision")
            if decision not in _DECISIONS:
                raise _HTTPError(
                    400, f"unknown decision: {decision!r}; "
                         f"expected one of {list(_DECISIONS)}")

            step, request = self._current_pending(run)
            if request["id"] != request_id:
                # The gate itself would refuse out-of-order execution; the
                # API refuses out-of-order *decisions* one step earlier.
                raise _HTTPError(
                    400, "request is not the current pending step's request")

            gate = run["gate"]
            audit = run["audit"]

            if decision == "reject":
                gate.decide(request["id"], False, decided_by="human-ui")
                note_approval("reject")
                outcome = StepOutcome(step_id=step.id, action=step.action,
                                      decision="rejected", executed=False,
                                      verify_ok=None)
                run["outcomes"].append(outcome)
                self._halt_plan(run, step.id, "halted_rejected", "rejected")
                self._send_json(200, {
                    "status": "halted_rejected",
                    "halted_at": step.id,
                    "outcome": _outcome_to_json(outcome),
                })
                return

            if decision == "approve_all":
                gate.approve_all(run["plan_id"], decided_by="human-ui")
                note_approval("approve_all")
                plan = run["plan"]
                for i in range(run["current_index"], len(plan.steps)):
                    st, rq = plan.steps[i], run["requests"][i]
                    oc = execute_approved_step(
                        gate=gate, request=rq, step=st,
                        privileged_call=run["admin_client"].call_tool,
                        verify_call=run["read_client"].call_tool,
                        audit=audit)
                    oc.decision = "approve_all"
                    if oc.executed:
                        note_privileged_action(st.action)
                    run["outcomes"].append(oc)
                    if not oc.executed:
                        self._halt_plan(run, st.id, "halted_error",
                                        "execution_failed")
                        self._send_json(200, {
                            "status": "halted_error",
                            "halted_at": st.id,
                            "outcome": _outcome_to_json(oc),
                        })
                        return
                    if oc.verify_ok is False:
                        self._halt_plan(run, st.id, "halted_verify_failed",
                                        "verify_failed")
                        self._send_json(200, {
                            "status": "halted_verify_failed",
                            "halted_at": st.id,
                            "outcome": _outcome_to_json(oc),
                        })
                        return
                run["current_index"] = len(plan.steps)
                audit.append("plan_completed", "plan-runner",
                             {"plan_id": run["plan_id"],
                              "steps": len(run["outcomes"])})
                run["status"] = "completed"
                self._send_json(200, {
                    "status": "completed",
                    "outcomes": [_outcome_to_json(o)
                                 for o in run["outcomes"]],
                })
                return

            # decision == "approve": one step at a time.
            gate.decide(request["id"], True, decided_by="human-ui")
            note_approval("approve")
            outcome = execute_approved_step(
                gate=gate, request=request, step=step,
                privileged_call=run["admin_client"].call_tool,
                verify_call=run["read_client"].call_tool,
                audit=audit)
            if outcome.executed:
                note_privileged_action(step.action)
            run["outcomes"].append(outcome)
            if not outcome.executed:
                self._halt_plan(run, step.id, "halted_error",
                                "execution_failed")
                self._send_json(200, {
                    "status": "halted_error",
                    "halted_at": step.id,
                    "outcome": _outcome_to_json(outcome),
                })
                return
            if outcome.verify_ok is False:
                self._halt_plan(run, step.id, "halted_verify_failed",
                                "verify_failed")
                self._send_json(200, {
                    "status": "halted_verify_failed",
                    "halted_at": step.id,
                    "outcome": _outcome_to_json(outcome),
                })
                return
            run["current_index"] += 1
            if run["current_index"] < len(run["requests"]):
                nxt = run["requests"][run["current_index"]]
                run["status"] = "awaiting_approval"
                self._send_json(200, {
                    "status": "step_completed",
                    "outcome": _outcome_to_json(outcome),
                    "next_pending_request": {
                        "request_id": nxt["id"],
                        "step_id": nxt["step_id"],
                    },
                })
                return
            audit.append("plan_completed", "plan-runner",
                         {"plan_id": run["plan_id"],
                          "steps": len(run["outcomes"])})
            run["status"] = "completed"
            self._send_json(200, {
                "status": "completed",
                "outcome": _outcome_to_json(outcome),
            })

        # -- API: read-only ------------------------------------------
        def _api_scenarios(self) -> None:
            self._send_json(200, [
                {"id": sid, "title": sid.replace("_", " ").title(),
                 "description": _SCENARIO_DESCRIPTIONS.get(sid, "")}
                for sid in SCENARIOS
            ])

        def _api_audit(self, run: dict) -> None:
            audit = run["audit"]
            entries = audit.entries()
            ok, msg = audit.verify()
            self._send_json(200, {"entries": entries, "valid": ok,
                                  "message": msg})

        def _api_report(self, run: dict) -> None:
            diagnosis = None
            d = run["diagnosis"]
            if d is not None:
                # agent.report.generate_markdown accepts an optional
                # diagnosis dict to enrich the Diagnosis section (hypothesis
                # titles, cited evidence) when the log itself only carries
                # ids. Same helper shape as the API's diagnose response.
                diagnosis = {
                    "hypotheses": [
                        {"id": h.id, "title": h.title, "score": h.score,
                         "evidence": [
                             {"claim": c.claim, "tool": c.tool,
                              "excerpt": c.excerpt} for c in h.evidence]}
                        for h in d.hypotheses
                    ]
                }
            self._send_json(
                200, {"markdown": generate_markdown(run["run_dir"],
                                                   diagnosis=diagnosis)})

        def _api_host_compare(self) -> None:
            # Same two include-filtered gateways as demo/host_compare.py,
            # same demo read token; the sim and real hosts differ, the
            # authz/audit path does not.
            audit = app.host_audit()
            sim_gateway = Gateway(Estate(seed=42), audit,
                                  include={"get_host_metrics"})
            real_gateway = Gateway(LinuxHostConnector(), audit,
                                   include={"get_host_metrics"})
            sim = sim_gateway.call("get_host_metrics", {"host": "app01"},
                                   _READ_TOKEN)
            real = real_gateway.call("get_host_metrics", {"host": "localhost"},
                                     _READ_TOKEN)
            self._send_json(200, {"sim": sim, "real": real})

        # -- routing -------------------------------------------------
        def _dispatch(self) -> None:
            path = urllib.parse.urlsplit(self.path).path
            if self.command == "GET":
                if path == "/":
                    self._send_static("index.html")
                    return
                if path in ("/static/app.js", "/static/style.css"):
                    self._send_static(path.rsplit("/", 1)[1])
                    return
                if path == "/healthz" and _OBSERVABILITY:
                    status, body = handle_healthz(JevReasoner.from_env().status())
                    self._send_json(status, body)
                    return
                if path == "/metrics" and _OBSERVABILITY:
                    status, body, content_type = handle_metrics()
                    self._send_text(status, body, content_type)
                    return
                with app.lock:
                    if path == "/api/scenarios":
                        self._api_scenarios()
                        return
                    if path == "/api/jev/status":
                        self._send_json(200, JevReasoner.from_env().status())
                        return
                    if path == "/api/host/compare":
                        self._api_host_compare()
                        return
                    m = _API_RUN_RE.match(path)
                    if m:
                        run_id, action = m.group(1), m.group(2)
                        run = self._get_run(run_id)
                        if action == "audit":
                            self._api_audit(run)
                        elif action == "report":
                            self._api_report(run)
                        else:
                            self._send_json(
                                405, {"error": f"{action} requires POST"})
                        return
                self._send_json(404, {"error": "not found"})
                return
            if self.command == "POST":
                if path == "/api/runs":
                    with app.lock:
                        self._api_create_run()
                    return
                m = _API_RUN_RE.match(path)
                if m:
                    run_id, action = m.group(1), m.group(2)
                    with app.lock:
                        run = self._get_run(run_id)
                        if action == "diagnose":
                            self._api_diagnose(run)
                        elif action == "plan":
                            self._api_plan(run)
                        elif action == "decide":
                            self._api_decide(run)
                        else:
                            self._send_json(
                                405, {"error": f"{action} requires GET"})
                    return
                self._send_json(404, {"error": "not found"})
                return
            self._send_json(405, {"error": "method not allowed"})

        def do_GET(self):  # noqa: D102 - stdlib hook
            try:
                self._dispatch()
            except _HTTPError as exc:
                self._send_json(exc.status, {"error": exc.message})
            except Exception as exc:  # never leak a traceback
                self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})

        def do_POST(self):  # noqa: D102 - stdlib hook
            try:
                self._dispatch()
            except _HTTPError as exc:
                self._send_json(exc.status, {"error": exc.message})
            except Exception as exc:  # never leak a traceback
                self._send_json(500, {"error": f"{type(exc).__name__}: {exc}"})

    return WebHandler


# ---------------------------------------------------------------------------
# Server factory + entry point
# ---------------------------------------------------------------------------

def make_server(port: int = _DEFAULT_PORT,
               bind: str | None = None) -> ThreadingHTTPServer:
    """Build the demo web server. Binds 127.0.0.1 unless overridden.

    Binding to 0.0.0.0 would expose an unauthenticated UI that drives
    privileged-action approvals to the whole network; loopback-only keeps
    it a local screen-sharing tool. Pass bind="0.0.0.0" ONLY for
    containerized runs (see _bind_host); on a bare host, put remote access
    behind an authenticated reverse proxy and real identity tokens --
    do not just rebind to 0.0.0.0. Port 0 asks the OS for a free port
    (used by the test suite).
    """
    app = _AppState()
    handler = _handler_class(app)
    server = ThreadingHTTPServer((bind or _bind_host(), port), handler)
    return server


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="RCA assistant web demo UI (localhost only).")
    p.add_argument("--port", type=int, default=_default_port(),
                   help="local port to listen on (default: 8765, RCA_PORT)")
    p.add_argument("--bind", type=str, default=None,
                   help="bind address (default: 127.0.0.1, RCA_BIND; "
                        "use 0.0.0.0 only inside containers)")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    server = make_server(port=args.port, bind=args.bind)
    print("RCA assistant web demo")
    print(f"Open: http://127.0.0.1:{server.server_address[1]}")
    print(f"(bound to {server.server_address[0]}; Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
