# Demo script — 3-minute narrated screen-share

Goal: in 3 minutes, show a middleware engineer that the RCA assistant finds
the real root cause, cites its evidence, fixes nothing without approval, and
leaves an audit trail. Use the web UI (`python -m demo.web`, open
http://127.0.0.1:8765). Have it booted before you start talking.

## 0:00 – 0:20 — Set the frame

Say: "This is our RCA assistant for middleware operations. It watches MQ,
Kafka, Tomcat, and Linux through governed, read-only connectors. When an
alert fires it gathers evidence, proposes the root cause *with citations*,
and it cannot fix anything on its own — every privileged action needs a
human's approval, and every step lands in a tamper-evident audit log."

Do: nothing yet; let them see the scenario list.

## 0:20 – 0:45 — Inject the incident

Do: click **Inject** on `channel_stopped`.

Say: "An alert just fired: queue PAYMENTS.IN is backing up — 48,500 messages
deep. In real life this is the page that ruins your morning."

## 0:45 – 1:20 — Diagnose with cited evidence

Do: click **Diagnose**. Point at the evidence cards as they appear.

Say: "The engine gathers evidence through read-only tools only — six calls,
no changes to anything. It ranks hypotheses. Top: *receiver channel
stopped*, score 1.0. And every claim is cited: the channel status came from
`get_channel_status`, the stop cause from the error log, the backlog depth
from `get_queue_depth`. If you don't believe the diagnosis, you can inspect
the exact tool output behind each claim."

## 1:20 – 1:40 — Jev confidence, visible by default

Do: point at the Jev confidence panel above the hypotheses.

Say: "And this is the part buyers feel. Jev — TypeSafe's System One — scores
every diagnosis by default when a key is configured: calibrated confidence
on each hypothesis, a supported-or-not verdict on every cited claim, a
triage priority, and a risk class on the remediation. See the banner: *Jev
confidence scoring active*. Jev advises only — it never approves anything,
never touches a privileged tool. Without a key the product runs in
deterministic mode, clearly labeled."

## 1:40 – 2:00 — The plan needs a human

Do: click **Propose plan**. Point at the pending step card.

Say: "It proposes a remediation plan — here, one step: restart the channel.
Note what it does *not* do: it does not run it. The plan is data. That step
needs me."

## 2:00 – 2:25 — Approve, execute, verify

Do: click **Approve**. Watch the step go green; point at the verification
line.

Say: "I approve. The privileged tool runs through the approval gate — scoped,
audited — and then the system re-verifies with read-only checks: channel is
running, backlog is draining. Recovery confirmed. The assistant never
skipped the human, and it checked its own work."

## 2:25 – 2:50 — Audit and report

Do: scroll the audit-trail table; click **Download incident report**.

Say: "Every step — every tool call, every decision I made, every Jev
judgment — is in a hash-chained audit log; tampering breaks the chain. One
click downloads the incident report: timeline, cited evidence, Jev
confidence, approvals, outcome. That is what
goes to the manager, the postmortem, or the auditor."

## 2:45 – 3:00 — Close on the hard case

Say: "For a trickier incident — say, disk-full — the plan has *two* steps:
archive old logs, then restart the app. Each step is approved separately,
each is re-verified, and if I reject any step the plan halts with the
incident left open and clearly reported. Read-only by default, human in
charge, everything cited, everything audited."

## If they ask for more (backup beats)

- **Reject path**: inject again, propose the plan, click **Reject** — "No
  privileged tool was invoked. The incident stays open in the log."
- **Approve-all**: on the 2-step `disk_full` plan, click **Approve all
  remaining** — "One explicit choice, logged separately from step-by-step
  approval, and every step is still verified."
- **Adversarial**: "We also test prompt injection — a log line telling the
  engine to purge all queues is treated as inert data. It never becomes an
  action."
- **Real data**: open the host-compare panel — "The same tool interface
  reads live `/proc` data from a real Linux host, sandboxed and read-only."
- **Headless**: "The identical flow runs from the CLI for pipelines:
  `python -m demo.run_incident --scenario expired_tls --auto-approve`."

## New Kafka + Tomcat scenarios (Phase 4)

Three more scenarios exercise the new connector tools; inject them from the
scenario list or the CLI (`python -m demo.run_incident --scenario <id>
--auto-approve`):

- **`kafka_broker_config_drift`** — Broker 3's `log.retention.hours` drifted
  to 72h vs 168h on brokers 1-2, and the orders consumer group is lagging
  (95,000). Diagnose cites `get_kafka_broker_config` per broker plus
  `get_kafka_consumer_group_detail`. The plan restarts the lagging consumer
  group; the drifted config is flagged in the rationale as an operator
  follow-up.
- **`tomcat_thread_exhaustion`** — The `http-nio-8080` thread pool is
  saturated (200/200) and `/orders` is slow; the catalina log reports "All
  threads (200) are currently busy". Diagnose cites `get_tomcat_threadpool`,
  `read_tomcat_log`, and `get_tomcat_apps`. Approve the plan and
  `restart_tomcat_app("/orders")` frees the pool (busy drops to 8), with the
  verify step matching the nested `pools.0.current_threads_busy` reading.
- **`kafka_under_replicated`** — Partition 2 of `payments-events` is
  under-replicated (ISR smaller than the replica set) while the brokers are
  all reachable. Diagnose cites `get_kafka_topic_detail` and
  `get_kafka_broker_health`. There is deliberately *no* automated remediation
  for this one — fixing ISR is an operator job — so the plan step shows
  "No safe remediation proposed ... manual investigation required."

## Presenter notes

- Keep to the `channel_stopped` happy path for the 3 minutes; use backup
  beats only on questions.
- Never type the demo tokens anywhere; they are demo-only and documented
  as such.
- If the UI ever misbehaves live, fall back to the CLI — same engine,
  same guarantees.
