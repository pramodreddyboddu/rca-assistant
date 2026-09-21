# Runbook: operating the demo

## Running each scenario

All commands run from the repo root with the venv from the quickstart.

Default scenario (receiver channel stopped, queue backlog on `PAYMENTS.IN`).
The engine proposes `restart_channel` and the approval gate prompts you:

```bash
.venv/bin/python -m demo.run_incident
```

Disk-full scenario (app host `app01` at 97% disk). No privileged remediation
is proposed, so there is no approval prompt; the engine ranks "Disk pressure
on app host" and the run reports that manual investigation is required:

```bash
.venv/bin/python -m demo.run_incident --scenario disk_full
```

Kafka-lag scenario (consumer group stalled at 250000 lag). Like `disk_full`,
no remediation is proposed and no prompt appears:

```bash
.venv/bin/python -m demo.run_incident --scenario kafka_lag
```

Other flags:

```bash
.venv/bin/python -m demo.run_incident --scenario channel_stopped --auto-approve
# approve the proposed fix without prompting (demo only; defaults to OFF)

.venv/bin/python -m demo.run_incident --seed 7
# reproducibility: same seed + same scenario = same estate state
```

To exercise the reject path non-interactively:

```bash
printf 'n\n' | .venv/bin/python -m demo.run_incident
```

Invalid choice of `--scenario` fails fast with `ValueError: unknown scenario`.

## Where audit logs land

Each run creates a timestamped directory:

```
runs/<YYYYMMDDTHHMMSSZ>/audit.jsonl
```

Example: `runs/20260920T170000Z/audit.jsonl`. The path is printed at the
start of the run ("Audit log: ..."). The stdio server (`mcp_server/server.py`)
defaults to `runs/rca-audit.jsonl`, overridable with the `RCA_AUDIT_PATH`
environment variable.

## Verifying an audit chain by hand

```bash
.venv/bin/python -c "from audit import AuditLog; print(AuditLog('runs/<TIMESTAMP>/audit.jsonl').verify())"
```

A healthy log prints `(True, 'ok: N entries verified')`. If tampered, you get
`(False, <reason>)` naming the failing `seq` and cause (hash mismatch,
prev-link mismatch, or seq out of order). `verify()` never raises on corrupt
content; always check the boolean.

To inspect events, read the JSONL directly (one JSON object per line, field
order `seq, ts, event, actor, details, prev, hash`) or use
`AuditLog(path).summary()` for entry counts per event.

## Token rotation

- **Demo:** the tokens live in `mcp_server/auth.py` (`TOKENS`). To rotate,
  edit that dict (and update the `_READ_TOKEN` / `_ADMIN_TOKEN` selection in
  `demo/run_incident.py` if you change which entries carry which scopes).
  There is no revocation list; tokens are static by design in the demo.
- **Production:** tokens must come from a real identity provider
  (OAuth/OIDC) or a secret manager: short-lived, scoped, per-user tokens,
  hashes stored instead of plaintext, regular rotation, and revocation
  support. Never commit tokens to source control.

## Incident response if the demo misbehaves

1. Nothing in the demo touches real infrastructure; the blast radius is the
   in-memory sim. A bad run cannot affect production systems.
2. The approval gate defaults to deny. If a fix was approved and the estate
   ends in an odd state, just re-run the demo: each run builds a fresh
   `Estate` from the seed.
3. If the audit log shows `scope_denied` or `auth_denied` events, check which
   token the caller used and whether it carries the tool's required scope
   (`diagnostics:read` vs `admin:write`).
4. `approval_denied_execution` in the log means `execute()` was called without
   a live `APPROVED` grant; this is the gate working as designed.

## Troubleshooting

- `ModuleNotFoundError: No module named 'sim'` (or `agent`, `mcp_server`,
  `audit`): you forgot `pip install -e .`. Run the quickstart install steps.
- `ModuleNotFoundError: No module named 'mcp'` when running the stdio
  server: install the SDK with `.venv/bin/pip install -r requirements.txt`.
  (The demo path `demo/run_incident.py` does not need the MCP SDK.)
- `KeyError: unknown tool: <name>`: the tool name is not one of the 18 in
  `mcp_server/tools.py` (`TOOL_NAMES`); check spelling.
- `PermissionError: token lacks required scope 'admin:write'`: you called
  a privileged tool with the read token. The demo only calls privileged
  tools via the admin client after approval.
- `ValueError: unknown scenario: ...`: `--scenario` must be one of the 8
  in `sim` (`SCENARIOS`).
- Test failures: `tests/` holds the full suite (run
  `.venv/bin/python -m pytest tests/ -q`). A failure names the exact
  behavior that broke; check the most recent code change against the
  documented behavior above.
- The interactive prompt appears only when the engine proposes a
  remediation. That happens for `channel_stopped` (top hypothesis "Receiver
  channel stopped"); `disk_full` and `kafka_lag` propose no remediation, so
  no prompt is expected for those scenarios.

### Jev troubleshooting

- UI shows "Deterministic mode": no `TYPESAFE_API_KEY` in the environment
  (or `RCA_JEV_ENABLED=0`). Set the key and restart; confirm
  `GET /api/jev/status` reports `"mode": "jev"`.
- Diagnosis is slow (~20s) then falls back: the TypeSafe API is unreachable
  or timing out. The advisory carries `available: false` and the
  deterministic diagnosis stands. Check outbound HTTPS to
  `api.typesafe.ai` and the key's validity (a 401 means a bad key — the
  app keeps working in deterministic mode).
- Jev metrics: `GET /metrics` exposes
  `rca_jev_judgments_total{kind,result}` and the `rca_jev_enabled` gauge;
  `GET /healthz` includes the Jev status block.
- The key never appears in logs or the audit trail by design. If you need
  to verify which key is active, check the environment of the running
  process — not the app.

## Real Linux host connector

The first real (non-simulated) connector in `connectors/`: `LinuxHostConnector`
(`connectors/linux_host.py`, `name = "linux-host"`). It reads live diagnostics
from the machine it runs on and exposes them through the *same* MCP gateway as
the sim — same token verification, scope checks, and hash-chained audit.

### What it reads

- `get_host_metrics(host)`: real CPU (two `/proc/stat` samples ~0.1s apart,
  clamped 0-100), memory from `/proc/meminfo` (`MemTotal`/`MemAvailable`),
  disk from `shutil.disk_usage("/")`, and 1-minute load from `/proc/loadavg`.
  The result carries `"source": "real"` so live data can never be confused
  with sim fiction. Any `host` that is not this machine (`localhost` or the
  machine's own hostname) raises `ConnectorError` — this connector only
  reads its own host. Missing `/proc` files raise `ConnectorError` with a
  clear message, never a raw traceback.
- `tail_log(path, limit)`: last `limit` lines of an allow-listed log file,
  returned as `[{"line_no": n, "text": line}]` with absolute line numbers.
  Content is returned verbatim and never interpreted.

### Allow-list policy

The connector constructor takes the policy:
`LinuxHostConnector(allowed_log_paths=("/var/log/",))`. A path is served
only if `os.path.realpath(path)` (symlinks fully resolved) equals an
allow-listed file or sits under an allow-listed directory. Anything else
raises `PermissionError`, which the gateway audits as `tool_error`.
Additional caps: `limit` is clamped to 200 lines, and a single read stops
at 256KB. To allow a specific file or tree, pass it explicitly, e.g.
`allowed_log_paths=("/var/log/myapp/", "/var/log/messages")`.

### Read-only guarantee

Read-only by construction: the class defines no method that writes,
deletes, kills, restarts, executes, or otherwise mutates anything; it never
shells out (no `os.system`/`subprocess`/popen); metrics come from `/proc`
(which cannot alter system state). See the sandbox contract in the module
docstring and ADR 0008.

### The host_compare command

```bash
.venv/bin/python -m demo.host_compare [--log PATH]
```

Builds two gateways sharing one audit log under `runs/`: the sim estate
(`include={"get_host_metrics"}`) and the real connector
(`include={"get_host_metrics", "tail_log"}`), both driven with the demo
read token. Prints a side-by-side table — left column is SIMULATED
(seed 42 `app01`), right column is REAL (live `/proc` on this machine) —
then tails a real allow-listed log. Default log: the first readable of
`/var/log/syslog`, `/var/log/messages`; on hosts without either, it prints
"no allow-listed log readable on this host" and exits 0. Note the `include`
parameter: it is how a connector that implements only part of the tool
surface plugs into the gateway without exposing tools it cannot serve.

## Web demo UI

```bash
.venv/bin/python -m demo.web            # http://127.0.0.1:8765
.venv/bin/python -m demo.web --port 9000
```

The browser UI walks the same incident flow as the CLI
(inject -> diagnose -> propose plan -> approve each step -> download the
markdown incident report), plus a live audit-trail viewer and a sim-vs-real
host compare panel. It is intended for screen-sharing discovery calls.

- **Localhost only, by construction.** The server binds `127.0.0.1` and
  `make_server()` takes no host parameter, so it cannot be rebound to
  `0.0.0.0` by accident. There is no authentication, and the UI drives
  privileged-action approvals: treat it as a local demo tool only. If you
  need remote access, terminate TLS and authenticate at a reverse proxy in
  front of it — do not just expose the port.
- **Changing the port:** `--port` (default 8765). The startup line prints
  the actual URL.
- **Runs:** in-memory per server process, keyed by a random `run_id`;
  restarting the server drops them. The hash-chained audit logs persist
  under `runs/<utc-ts>-<run_id>/audit.jsonl` and stay verifiable by hand
  (see "Verifying an audit chain by hand" above).
- **The API refuses out-of-order decisions** (400) and unknown runs (404);
  request bodies are capped at 1 MB (413). Unexpected errors return
  `{"error": msg}` without a traceback.
