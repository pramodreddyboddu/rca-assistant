# Getting Started with the RCA Assistant

The RCA assistant is a reference tool for root cause analysis on middleware
and infrastructure (IBM MQ, Kafka, Tomcat, Linux, and more). It injects an
incident into a simulated estate, gathers evidence with read-only tools,
proposes the most likely root cause with citations, and asks a human to
approve any fix before running it. Every step goes into a hash-chained
audit log.

This guide takes you from a fresh machine to your first diagnosis in about
five minutes. It assumes you are comfortable with a terminal.

## Prerequisites

- **Python 3.12 or newer.** Check with:
  ```bash
  python3 --version
  ```
  On some systems you may need to install a newer Python first (for
  example via `pyenv install 3.12.12` or your OS package manager).
- `pip` (it comes with Python). A virtual environment is a good idea:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  ```

## Install

Straight from the release tag:

```bash
pip install git+https://github.com/pramodreddyboddu/rca-assistant.git@v0.7.0
```

Or download the prebuilt wheel (`rca_assistant-0.7.0-py3-none-any.whl`) from the [v0.7.0 release page](https://github.com/pramodreddyboddu/rca-assistant/releases/tag/v0.7.0) and `pip install` it.

Verify it worked:

```bash
rca version
```

(The `rca` command set ships with the 0.7.0 package. If you installed from
the source checkout instead of the wheel, the same flows exist as
`python -m demo.run_incident`, `python -m demo.web`, and
`connectors/harness.py`; see `docs/RUNBOOK.md`.)

## The five-minute path

### 1. Run the full demo: `rca demo`

```bash
rca demo
```

This runs the complete reference flow in about a minute:

1. **Incident injection**: a receiver channel is stopped and a backlog
   builds on a queue.
2. **Diagnosis**: the engine queries only read-only tools (queue depth,
   channel status, error logs, consumer lag) and ranks hypotheses.
3. **Root cause proposal**: the top hypothesis, with cited evidence.
4. **Approval gate**: the proposed remediation plan is shown step by
   step. The default is **deny**: each privileged step asks
   `Approve this privileged action? [y/N]`, and anything but `y` rejects it.
5. **Execution and verified recovery**: approved steps run, then the
   estate is re-read to confirm the problem is actually gone.
6. **Audit summary**: hash-chain verification of the full trail.

Say `n` to a step to watch the reject path: nothing privileged happens,
and the run reports the plan as not applied.

### 2. Diagnose a specific scenario: `rca diagnose --scenario kafka_lag`

```bash
rca diagnose --scenario kafka_lag
```

This runs the diagnose part only for one incident type. There are 45
built-in scenarios covering the 21 supported technologies. List them with:

```bash
rca scenarios
```

Try a couple: `kafka_lag`, `tomcat_thread_exhaustion`, `channel_stopped`.

### 3. See it in the browser: `rca serve`

```bash
rca serve
```

Open **http://127.0.0.1:8765** and click through: **Inject** a scenario,
**Diagnose** (top hypothesis with cited evidence), **Propose plan**,
**Approve** / **Reject** each privileged step, **Download incident report**.

The server binds to localhost only and has no login: it is a local demo
tool, not a network service. Never expose it to the open internet without
a reverse proxy and access control.

### 4. Check the connectors: `rca connectors self-test`

```bash
rca connectors list
rca connectors self-test
```

`self-test` runs each connector's contract checks (connect, read-only
queries, policy, audit) against its fake transport. This is the onboarding
check you would run on a new connector before pointing anything at it.

## What to expect from the output

A diagnosis reads like this:

- **Alert**: what triggered the investigation (e.g. backlog on
  `PAYMENTS.IN`, consumer lag on a topic).
- **Evidence**: the read-only observations the engine gathered, each one
  sourced to a specific tool call.
- **Hypotheses**: ranked candidates with scores, top pick first.
- **Root cause**: the winning hypothesis plus the evidence that supports
  it, and confidence where the reasoning layer is active.
- **Proposed plan**: the remediation steps with their scope, then the
  approval prompt for each privileged step.
- **Verification**: post-fix re-reads confirming recovery.
- **Audit summary**: where the log went, and that the hash chain verified.

## Concepts in thirty seconds

- **Simulated estate vs real connectors.** Everything in the demo runs
  against a deterministic simulation, not your systems. The package also
  ships 21 real connector clients (IBM MQ, Kafka, Tomcat, WebSphere,
  PostgreSQL, and others), but those are fake-transport tested only. No
  live validation has been done, and you should not point them at
  production yet. See `docs/REAL_CONNECTOR_READINESS.md`.
- **Read-only evidence.** Diagnosis is allowed to look, never to touch.
  Fixes go through separate privileged tools.
- **Approval gates.** The gate defaults to deny. Nothing privileged runs
  without an explicit human yes. `--auto-approve` exists for scripted
  demos only.
- **Audit logs.** Each run writes to `runs/<timestamp>-<id>/audit.jsonl`
  in the directory you ran the command from, with a hash chain linking
  every entry.

## Troubleshooting

**Wrong Python version.** The package needs 3.12 or newer. Run
`python3 --version`. If yours is older, install a newer Python
(`pyenv`, your distro packages, or python.org) and try again.

**pip install failures.** Check you can reach github.com from the
machine. Upgrade pip first (`pip install --upgrade pip`). If the wheel
URL fails, try the `git+https` form, and make sure you are in a venv if
your distro blocks system-wide pip installs.

**Port already in use for `rca serve`.** Either stop whatever owns the
port, or serve on another one:

```bash
rca serve --port 9000
```

then open http://127.0.0.1:9000.

**Where do runs and audit logs go?** Under `runs/<timestamp>-<id>/`
relative to the directory you ran the command from. Each run directory
has `audit.jsonl` (the hash-chained log) and the incident report.

**"Demo tokens are demo-only."** Authentication in `mcp_server/auth.py`
uses hard-coded bearer tokens so the demo works with zero setup. Do not
reuse this anywhere real. Production must use a real identity provider
(OAuth/OIDC) or a secret manager, with short-lived scoped tokens, hashed
storage, and rotation. See `docs/SECURITY.md`.

**Running the test suite.** From a source checkout:

```bash
python -m pytest tests/ -q
```

## Next steps

- `docs/ARCHITECTURE.md`: how the pieces fit together (sim, MCP server,
  engine, approval gate, audit).
- `docs/TECHNOLOGY_COVERAGE.md`: the 21 technologies and what each
  connector can do.
- `docs/REAL_CONNECTOR_READINESS.md`: what it takes to point this at
  real infrastructure, and what is not proven yet.
- `docs/RUNBOOK.md`: operating the demo: scenarios, audit verification,
  troubleshooting.
- `docs/SECURITY.md`: threat model, trust boundaries, auth warnings.
- `docs/adr/`: architecture decision records: why a simulated estate,
  why a deterministic engine, why hash-chained audit.
