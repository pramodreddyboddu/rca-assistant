# ADR 0007: Web demo UI on stdlib http.server + vanilla JS

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The CLI demo (`demo/run_incident.py`) works for engineers but is awkward
for customer discovery calls: a terminal transcript does not show well on
a shared screen, and typing approval answers at a prompt breaks the flow
of a conversation. The team wanted a click-through UI for the same
incident -> diagnose -> approve -> verify flow. The natural instinct is a
web framework (Flask/FastAPI) plus a JS framework (React/Vue), but the
reference demo has a hard "zero new dependencies" posture everywhere else
(the engine, the MCP server path used by the demo, and the audit log are
all stdlib), and the UI is a demo aid, not the product.

## Decision

Build the UI on **stdlib `http.server` (ThreadingHTTPServer) + vanilla
JS**, in one new module `demo/web.py` plus three static files
(`demo/static/index.html`, `app.js`, `style.css`):

1. **Zero dependencies, zero build.** The whole UI runs with the same
   Python 3.12 the demo already requires. No `pip install`, no npm, no
   bundler, no framework to audit or keep patched. It runs anywhere the
   CLI demo runs, including air-gapped customer laptops.

2. **The server reuses the exact CLI code paths.** `POST /api/runs`
   builds `Estate` / `AuditLog` / `Gateway` / `InProcessClient` /
   `RCAEngine` / `ApprovalGate` exactly like `demo/run_incident.py`;
   `/diagnose` calls `engine.diagnose()`, `/plan` calls
   `engine.propose_plan()` + `gate.request_plan()`, and `/decide` calls
   `gate.decide()` / `gate.approve_all()` plus the shared
   `agent.plans.execute_approved_step()`. There is one copy of the
   approval/execution logic; the UI cannot diverge from the CLI.

3. **The gate's guarantees hold over HTTP.** The API refuses any decision
   whose request id is not the *current* pending step's request (400),
   so out-of-order approval is impossible through the UI too. All
   exceptions become `{"error": msg}` with the right status; tracebacks
   never reach the browser.

4. **Loopback-only by construction.** `make_server()` has no host
   parameter: it always binds 127.0.0.1. The UI has no authentication
   and drives privileged-action approvals, so exposing it to a network
   would be a real hole. If remote access is ever needed, the documented
   path is an authenticated reverse proxy, not rebinding to 0.0.0.0.

## Consequences

- **Auditable surface:** ~600 lines of server code a reviewer can read in
  one sitting, vs. a framework's worth of behavior to trust.
- **No real-time push:** the UI polls/refreshes on each user action
  (fetch after every click); there is no websocket/SSE. For a
  human-driven click-through this is fine and keeps the server trivial.
- **In-memory runs:** runs live in a dict keyed by
  `secrets.token_hex(8)`; restarting the server drops them (the
  hash-chained audit logs persist under `runs/`). A production UI would
  need durable run storage and real auth; this one is explicitly a
  screen-sharing aid, and the docs say so.
- **Downgrade:** if the UI ever becomes a burden, deleting `demo/web.py`
  and `demo/static/` returns the repo to the CLI-only demo with zero
  leftover dependencies.
