# RCA Assistant: Reference Demo

A deterministic reference demo of an RCA (root cause analysis) assistant for
middleware and infrastructure operations (IBM MQ, Kafka, Tomcat, Linux hosts).

The demo injects an incident into a **simulated** middleware estate, runs a
deterministic diagnosis engine that gathers evidence through **read-only MCP
tools**, proposes the top hypothesis with cited evidence, and, if a fix is
proposed, asks a human for approval before executing the single **privileged**
tool (`restart_channel`). Every step is written to a hash-chained audit log.
Nothing runs against real infrastructure; this is a pattern reference, not a
production system.

## Quickstart (5 minutes)

Run these from the repo root. Requires Python 3.12.

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install -e .
.venv/bin/python -m demo.run_incident            # interactive approval prompt
printf 'n\n' | .venv/bin/python -m demo.run_incident   # reject path
.venv/bin/python -m demo.run_incident --auto-approve   # approve path
.venv/bin/python -m pytest tests/ -q
```

The test suite has 1247 tests covering the sim, tools, engine, approvals,
audit, adversarial cases, and the demo run itself.

## Web UI

For screen-sharing demos, the same incident flow runs behind a local web
UI (stdlib only, no dependencies, no build step):

```bash
.venv/bin/python -m demo.web            # serves http://127.0.0.1:8765
.venv/bin/python -m demo.web --port 9000
```

Open the URL and click through: **Inject** a scenario → **Diagnose** (top
hypothesis with cited evidence) → **Propose plan** → **Approve** / **Reject**
/ **Approve all remaining** each privileged step → **Download incident
report** (.md). The right-hand panel shows the live audit trail (with
hash-chain status) and a sim-vs-real host metrics compare.

The server binds **127.0.0.1 only** and has no authentication: it is a
localhost demo tool, not a network service. Never rebind it to 0.0.0.0;
if you need remote access, put it behind an authenticated reverse proxy
(see `docs/RUNBOOK.md`).

## What you will see

The demo runs in five stages:

1. **Incident injected** - a receiver channel is stopped on `QMGR1` and a
   backlog builds on queue `PAYMENTS.IN`; the alert is printed.
2. **Evidence + diagnosis (read-only)** - the engine queries only read-only
   tools (`get_queue_depth`, `get_channel_status`, `read_error_log`,
   `get_kafka_consumer_lag`) and ranks hypotheses by score.
3. **Approval gate** - the proposed fix (`restart_channel`, scope
   `admin:write`) is presented with rationale. Default is **deny**: the gate
   asks `Approve this privileged action? [y/N]`, and anything but `y` rejects.
4. **Fix executed or not applied** - on approval the privileged tool runs
   through the same gateway; on rejection nothing privileged happens.
5. **Recovery verified** - the sim clock advances, the channel status and
   queue depth are re-read, and the result is printed.

The run ends with an audit-trail summary and a hash-chain verification.

## Repo layout

```
rca-assistant/
  demo/          entry point (demo/run_incident.py)
  sim/           deterministic simulated estate (Estate) + fault injection
  mcp_server/    MCP server: tool defs, bearer-token auth, Gateway (policy point),
                 in-process client, stdio server
  agent/         deterministic RCA engine (rca.py) + human approval gate (approvals.py)
  audit/         hash-chained JSONL audit log
  connectors/    interface stub where real middleware connectors would plug in
  docs/          this documentation set
  runs/          audit logs from demo runs (created at runtime)
  tests/         test stub
```

## Demo-only auth warning

Authentication is two hard-coded bearer tokens in `mcp_server/auth.py`. This
is a deliberate demo simplification. Do not reuse it anywhere real. Production
must use a real identity provider (OAuth/OIDC) or a secret manager, with
short-lived scoped tokens, hashed storage, and rotation. See `docs/SECURITY.md`.

## Further reading

- `docs/GETTING_STARTED.md` - install, five-minute path, troubleshooting (start here)
- `docs/ARCHITECTURE.md` - components, data flow, dependency direction
- `docs/TOOLS.md` - reference for all 90 tools
- `docs/SECURITY.md` - threat model, trust boundaries, auth warnings
- `docs/RUNBOOK.md` - operating the demo: scenarios, audit verification, troubleshooting
- `docs/CHANGELOG.md` - release history
- `docs/adr/` - architecture decision records (why a simulated estate, why a
  deterministic engine, why hash-chained audit, why MIT, why an in-process client)
