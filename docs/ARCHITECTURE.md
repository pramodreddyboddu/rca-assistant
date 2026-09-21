# Architecture

## Components

| Package | Responsibility | Key API |
|---|---|---|
| `sim/` | Deterministic in-memory model of an IBM MQ / Kafka / Tomcat / Linux landscape. `Estate` holds state; `inject()` mutates it into one of 11 incident scenarios. | `Estate(seed)`, `inject(estate, scenario)`, `SCENARIOS` |
| `mcp_server/` | MCP tool surface with security policy. `tools.py` defines 90 tool definitions (66 read-only, 24 privileged). `auth.py` verifies demo Bearer <redacted> to scope sets. `gateway.py` is the single policy enforcement point: token verification, scope check, audit, dispatch. `client.py` is the in-process client. `server.py` exposes the same 90 tools over stdio for external MCP clients. | `Gateway(estate, audit).call(name, args, token)`, `InProcessClient(gateway, token)`, `verify_token(token)`, `TOOL_NAMES` |
| `agent/` | Deterministic root-cause analysis engine plus the human approval gate, with the Jev reasoning layer on by default. `rca.py` (`RCAEngine`) runs evidence plans through read-only tools, scores hypotheses (fraction of matched checks, 0.0-1.0), and returns remediation as **data** (never executed there): a `RemediationPlan` of ordered steps via `RCAEngine.propose_plan()`. `jev.py` (`JevReasoner`) is the default reasoning experience when `TYPESAFE_API_KEY` is set: one batched TypeSafe System One call per diagnosis producing typed hypothesis ranking, per-claim evidence-support verification, triage priority (P1–P4), and remediation risk class (low/medium/high) — advisory only, never overriding approvals, default-deny, audit, or verification; without a key the engine runs alone, labeled "deterministic mode" (see ADR 0009). `plans.py` defines the plan data types (`RemediationPlan`, `PlanStep`, `VerifySpec`), `check_verify()` for read-only re-verification, and `run_plan()` / `execute_approved_step()` which share execution logic between runners. `remediation.py` builds plans from (alert, evidence) for the supported (alert type, hypothesis) pairs, returning `None` where no safe remediation exists. `approvals.py` (`ApprovalGate`) is the only path that turns a plan step into execution, and only on a live `APPROVED` grant; default is deny. | `RCAEngine(client, audit).diagnose(alert) -> Diagnosis`, `RCAEngine.propose_plan(hypothesis) -> RemediationPlan | None`, `JevReasoner.from_env(audit).advise(diagnosis) -> JevAdvisory`, `JevReasoner.from_env().status()`, `plan_for(hypothesis_id, alert, evidence)`, `run_plan(plan, gate=gate, privileged_call=..., verify_call=..., decide=..., audit=audit)`, `ApprovalGate(audit)`: `request_plan` / `approve_all` / `decide` / `execute` |
| `audit/` | Append-only, hash-chained JSONL log. Each entry: `seq, ts, event, actor, details, prev, hash`. Detects tampering on `verify()`. Stdlib only. | `AuditLog(path).append/verify/summary/entries` |
| `demo/` | Wires everything together for one incident run: `demo/run_incident.py` (`--scenario`, `--auto-approve`, `--seed`). Builds the estate, two clients (read token for the engine, admin token for approved fixes only), the engine, the gate, and the audit log under `runs/`. Drives the plan flow: inject -> diagnose -> propose plan -> approve each step -> execute + verify -> recovery check. | `python -m demo.run_incident [--scenario ...] [--auto-approve] [--seed ...]` |
| `connectors/` | Four registered real-connector clients (`CONNECTORS`: `ibmmq`, `kafka`, `tomcat`, `linux-host`) over the `Connector` protocol (`connect`, `close`, `read`, `act`); see ADR 0008 for the sandboxing argument and `docs/REAL_CONNECTOR_READINESS.md` for the honest fake-transport-tested-only status. `connectors/harness.py` is the customer onboarding self-test: `.venv/bin/python -m connectors.harness [--connector NAME] [--json]` runs 10 static contract checks per connector with probe (non-secret) values — construction, registration, capabilities, read/act isolation, ctor-secret-params, repr hygiene, driver-absent connect, instance secret scan, privileged refusal, result-shape boundary — exit 0 pass / 1 fail / 2 unknown. The harness never contacts a live target or resolves a real secret; see ADR 0010. | `Connector`, `ConnectorError`, `CONNECTORS`, `run_checks()` |

## Data flow of a demo run

```
human operator
     |
     v
 demo/run_incident.py
     |  builds Estate(seed), AuditLog, Gateway,
     |  InProcessClient(read token), InProcessClient(admin token),
     |  RCAEngine, ApprovalGate
     v
 inject(estate, scenario) ---------------------> Estate (sim/)
     |  alert dict                                   |  state mutated
     v                                              v
 RCAEngine.diagnose(alert) --read-only tool calls--> Gateway.call(name, args, read token)
     |                                              |  token -> scopes -> tool.scope ok?
     |                                              |  audit: tool_call / evidence_denied / scope_denied
     |                                              v  dispatch to Estate method
     |  Diagnosis: hypotheses sorted by score,      v
     |  top with citations + plan DATA             Estate read method (data only)
     v
 JevReasoner.advise(diagnosis) --one batched System One call--> api.typesafe.ai
     |  (only when TYPESAFE_API_KEY is set; otherwise the flow continues
     |  in "deterministic mode" and this step is skipped)
     |  JevAdvisory: hypothesis confidence, per-claim evidence support,
     |  triage priority, remediation risk -- ADVISORY ONLY, never changes
     |  the diagnosis; audited as jev_advisory (hashes + bounded scalars,
     |  never the key)
     v
 RCAEngine.propose_plan(top) -> RemediationPlan (or None: manual investigation)
     v
 ApprovalGate.request_plan(plan)               (audited: plan_proposed,
     |                                           one approval_requested per step)
     v
 per step: decide(step, request) -> approve | reject | approve_all
     |
     +-- "approve"  --> ApprovalGate.decide(id, True, decided_by)
     |                     (audited: approval_decided)
     +-- "reject"   --> ApprovalGate.decide(id, False, decided_by)
     |                     plan HALTS: halted_rejected, incident left open
     +-- "approve_all" --> ApprovalGate.approve_all(plan_id, decided_by)
                           (audited: approval_decided_all -- a distinct event,
                           so an auditor can tell bulk approval from
                           step-by-step approval)
     v
 ApprovalGate.execute(id, admin_client.call_tool, action, args)
   per step, in plan order: a step executes only after every earlier step
   has status EXECUTED (plan-order enforcement; default-deny otherwise)
     |
     v
 Gateway.call(action, args, admin token)
   |  scope admin:write required (audited: scope_denied if missing)
   v  Estate privileged method (mutates state)
 check_verify(step.verify, read_client.call_tool)   (read-only re-verification)
     |
     +-- ok --> next step
     +-- FAIL -> plan HALTS: halted_verify_failed, incident left in its
                 current state (earlier steps stay executed; loudly, not crash)
     v
 estate.tick(40 x 60s); plan's verify specs re-run via check_verify
     v
 AuditLog.verify() -> "ok: N entries verified"

## Remediation plans

Most real incidents need more than one privileged action (renew a
certificate, *then* restart the channel; free disk, *then* restart the app).
A `RemediationPlan` (`agent/plans.py`) is an ordered tuple of `PlanStep`
records proposed for one hypothesis:

- **Plan data.** Each `PlanStep` is pure data: `id` (e.g. `step-1`, unique
  within the plan), `action` (a privileged tool name), `args`, a human-readable
  `rationale` shown at approval time, an optional `VerifySpec`, and
  `required_scope` (always `admin:write` in the reference demo). Like the
  legacy single remediation dict, a plan names privileged tools but never
  executes them: `diagnose()` and the plan builders in `agent/remediation.py`
  have no code path that passes a plan action to a tool client.
- **Per-step approval.** `ApprovalGate.request_plan()` creates one linked
  `PENDING` request per step, each tagged with `plan_id` and `step_index`.
  `run_plan()` asks `decide(step, request)` for each step in order: `approve`
  continues, `reject` halts the plan immediately. Every decision is audited.
- **Explicit approve-all.** `decide` may also return `approve_all`, which the
  gate executes as `approve_all(plan_id, decided_by)`: it approves every
  still-`PENDING` request of the plan in one explicit choice. It is audited
  as `approval_decided_all`, a distinct event from the per-step
  `approval_decided`, so bulk approval is never confused with step-by-step
  approval in the audit trail. Steps still execute in order and are still
  individually verified.
- **Re-verification.** A `VerifySpec` is data: a read-only `tool`, its `args`,
  and an `expect` dict matched against the tool's result (plain value means
  equality; `{"$lt"|"$lte"|"$gt"|"$gte"|"$eq": value}` compares numerics;
  dotted keys like `pools.0.current_threads_busy` resolve nested/list
  positions). After each executed step, `check_verify()` runs the spec's
  read-only tool and reports `(ok, detail)`; it never raises on a mismatch
  or a tool error, because verification is evidence gathering, not
  execution.
- **Halt semantics.** A plan run ends in exactly one terminal state:
  `completed` (all steps approved, executed, verified), `halted_rejected`
  (a step was rejected; nothing after it runs), `halted_verify_failed` (a
  post-step verification failed; the incident is left in its current state),
  or `halted_error` (a step could not execute, e.g. gate denial). Every halt
  is audited with `plan_halted` carrying the reason and the step it stopped
  at. A rejected or halted plan leaves the incident open for manual
  investigation rather than proceeding on assumptions.
- **Plan order enforcement.** `ApprovalGate.execute()` refuses to run a step
  until every earlier step of the same plan has status `EXECUTED`. Approving
  step 2 while step 1 is still pending does not let step 2 run, and executing
  an already-`EXECUTED` request is a replay that raises `PermissionError`.

`run_plan()` and `execute_approved_step()` in `agent/plans.py` hold the only
copy of this execution logic; both the CLI (`demo/run_incident.py`) and any
future runner (e.g. a web UI) share it instead of duplicating it.
```

## Why the in-process client exercises the same authz/audit path

Both entry points call `Gateway.call()` directly:

- The demo's `InProcessClient.call_tool(name, args)` is a thin wrapper that
  passes the client's fixed token to `gateway.call()`.
- `server.py`'s stdio MCP tools read the caller token from the `RCA_TOKEN`
  env var at call time and pass it to the same `gateway.call()`.

The Gateway performs token verification, scope authorization, audit logging
(`auth_denied`, `scope_denied`, `tool_error`, `tool_call`), and dispatch in
one place, so authz and auditing cannot drift between transports. The demo
uses the in-process client for speed and determinism (no subprocess, no
serialization round-trips); `server.py` exists so external MCP clients can
use the stdio transport against the identical policy point.

## Package dependency direction

```
demo      -> agent, audit, mcp_server, sim
agent     -> (receives client + audit as parameters; no privileged tool paths)
mcp_server-> (receives estate and audit as parameters; dispatches to estate
           methods; appends audit events via the passed AuditLog)
audit     -> stdlib only
sim       -> stdlib only
connectors-> mcp_server (TOOL_NAMES / PRIVILEGED_TOOLS only), stdlib otherwise
```

Note: `connectors/` is not in the demo run's critical path above — it is
the production integration seam. `demo/` wires the simulated estate, not
the real connectors; the harness (`connectors/harness.py`) and the pytest
suite (`tests/test_connector_contract.py`, per-connector modules) are the
checks that keep the seam honest.

Invariants that matter:

- **Nothing imports `demo`.** The demo is a consumer of the packages, not a
  dependency of any of them.
- **`agent` never imports privileged tool paths.** `rca.py` constructs the
  remediation dict as data (e.g. `{"tool": "restart_channel", ...}`) and never
  calls it; `remediation.py` and `propose_plan()` likewise build
  `RemediationPlan` data without executing it. `approvals.py` executes a
  callable only with a live `APPROVED` grant; the demo passes
  `admin_client.call_tool` into `execute()`, so the privileged call still
  goes through the Gateway's scope check.
- **`mcp_server` does not import `agent`, and `audit` imports nothing** but
  the standard library, so the audit trail has no circular dependencies.
- **`connectors` imports `mcp_server.tools` for the shared `TOOL_NAMES` /
  `PRIVILEGED_TOOLS` tables only** — the same names the gateway enforces,
  so a connector can never claim a capability the gateway does not know.
  The harness (`connectors/harness.py`) exercises this seam deliberately:
  it asserts every connector capability is within `TOOL_NAMES` and that no
  read routing table admits a privileged tool.
