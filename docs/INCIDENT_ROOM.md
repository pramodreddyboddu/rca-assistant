# Incident Room

One REAL incident, through the REAL pipeline, with REAL evidence.

The incident room (`rca incident`) replays a single incident recorded live
against queue manager **QM1 (IBM MQ 10.0.0.5)** on 2026-09-21:

- Channel `BATCH.CHL` was stopped
- 200 messages were backlogged on `BACKLOG.Q`
- The error log recorded `AMQ9533W: Channel 'BATCH.CHL' is not currently
  active`

The evidence was recorded by the production `IBMQConnector` read paths in
recorder record mode and committed as fixtures under
`tests/fixtures/recorded/ibmmq/`. The incident room serves those
fixtures back through the *same* connector read paths in replay mode --
the pipeline cannot tell the recordings from a live queue manager, and
that is the point: the demo proves the genuine `IBMQConnector` response
shaping, not a hand-crafted fixture.

## What is real, and what is not

- **Real:** the evidence (recorded live on QM1), the connector read path,
  the include-filtered gateway (authz + audit, same as production), the
  `RCAEngine` diagnosis, the Jev advisory when a key is available, and
  the remediation plan construction.
- **Replayed:** the MQ transport. `RCA_RECORD_MODE=replay` is set for the
  room process only; the connector's production default stays
  `passthrough`.
- **Rehearsed, never executed:** approval. Approving the plan records the
  decision in the audit log and returns what *would* run
  (`executed: false, rehearsal: true`). No channel is ever started, no
  queue drained, no privileged tool even registered on this path.
- **Honest:** verification. `POST /api/incident/verify` replays a second
  recorder pointed at `tests/fixtures/recorded/ibmmq_recovered/`
  (post-recovery state: `BATCH.CHL` RUNNING, `BACKLOG.Q` depth 0 --
  recorded live on QM1 on 2026-09-21 right after the real recovery:
  starting the channel and draining the queue). If those fixtures were
  absent, the endpoint would say so plainly instead of inventing a
  recovered state.

## Running it

```bash
rca incident              # http://127.0.0.1:8765 (incident page at /)
rca incident --port 8766  # alternate port; loopback binding is default
```

The incident page is at `/`; `/incident` is the same page (404 JSON
until the frontend lands). Existing behavior is untouched: `rca serve`
still serves the sim index at `/`.

## API

All incident responses carry `mode: "replay-demo"` and the evidence
source block:

```json
"evidence_source": {
  "recorded_at": "2026-09-21T22:29:54Z",
  "source": "live: QM1, IBM MQ 10.0.0.5",
  "note": "Evidence replayed from recordings. No live queue manager was touched."
}
```

- `POST /api/incident/run` -- runs the full pipeline: four evidence
  calls (two channel statuses, queue depth, error log), diagnosis (two
  hypotheses; top hypothesis scores 1.0), Jev advisory (deterministic
  fallback with `available: false` when no key is configured), and the
  remediation plan (one privileged step `restart_channel`, verify attached).
  Writes an audit log under `runs/<ts>-incident-<id>/audit.jsonl`.
- `POST /api/incident/decide` -- `{"decision": "approve" | "reject"}`.
  Rehearsal only. 400 unless a run is pending and the decision is valid.
- `POST /api/incident/verify` -- replays the post-recovery fixtures
  (channel RUNNING, queue depth 0); `recovered: true` when the checks
  pass. With the fixtures absent it would answer `recovered: false`
  with an honest note instead of inventing a state.
- `GET /api/incident/audit` -- the run's audit entries in contract shape
  (`action`, not `event`).

## Implementation notes

- Orchestration lives in `demo/incident.py`; `demo/web.py` adds the
  routes and the `--incident-room` flag (extended, not forked).
- `demo/live_estate.py` wraps `IBMQConnector` in replay mode; its
  `RecordedMQEstate.__init__` sets `RCA_RECORD_MODE=replay` for the
  process (the connector's `default_mode="passthrough"` is untouched --
  see `demo/live_estate.py` for the caveat on process-wide scope).
- The incident gateway registers **only** the four read tools
  (`get_queue_depth`, `get_channel_status`, `read_error_log`,
  `get_config`). Privileged tools are deliberately absent.
- A Jev advisory is optional and advisory-only: with a key it runs live
  through `agent/jev.py` (never raises for service problems); without one
  the response says `available: false, mode: deterministic`. Jev never
  overrides the approval controls.
- The diagnosis of the recorded incident lives in `agent/rca.py` as the
  `mq_channel_backlog` alert type.
