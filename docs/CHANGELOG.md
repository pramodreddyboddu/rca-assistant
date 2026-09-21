# Changelog

## 0.7.0 (2026-09-21)

The product release: real installable packaging, a company-grade README,
a getting-started guide, CI, issue/PR templates, and a security policy.

- Packaging: the project is now a real Python package named
  `rca-assistant` (previously `rca-assistant-demo`). `pip install
  rca-assistant` installs everything; the wheel ships the demo web UI
  (`demo/static`) so the bundled browser interface works out of the box.
  Two console entry points are registered: `rca` and `rca-assistant`,
  both backed by `rca_assistant.cli:main`. Requires Python 3.12+; the
  only runtime dependency is the official MCP Python SDK (`mcp>=2,<3`) —
  every vendor driver (kafka-python, pymqi, psycopg, pymysql, oracledb,
  redis-py, pymongo, the Kubernetes client) stays lazily imported, so
  installing never pulls in a driver you do not use.
- README: rewritten as a company-grade front door — what the product
  is, who it is for, how it works (secure read-only MCP connectors ->
  cited root causes -> human approves every fix), supported
  technologies, and install instructions for the real package.
- docs/GETTING_STARTED.md: a step-by-step guide from install to first
  incident walkthrough — sim scenarios, the demo web UI, the connector
  self-test harness, and pointing a connector at a real system.
- Continuous integration: a GitHub Actions workflow runs the full pytest
  suite (1247 tests) on Python 3.12 for every push and pull request, plus
  a build check that `python -m build` produces an installable wheel.
- Contribution surface: issue templates (bug report, feature request),
  a pull request template, and `SECURITY.md` with the vulnerability
  reporting policy.
- The stale `rca_assistant_demo.egg-info/` build artifact is removed;
  the build now derives metadata solely from `pyproject.toml`.

## 0.6.0 (2026-09-21)

The 21-technology catalog: 17 new connectors, 63 new tools, 34 new
incident scenarios.

- Connectors: 4 -> 21. New: IBM WebSphere Application Server (Jolokia
  JMX-HTTP, stdlib `urllib`, no driver needed), Oracle WebLogic Server
  (Management REST API over stdlib `urllib`, HTTP Basic, `X-Requested-By`
  CSRF header), JBoss EAP / WildFly (HTTP management API, HTTP Digest
  auth, stdlib `urllib`), RabbitMQ (Management HTTP API over stdlib
  `urllib`), ActiveMQ Artemis (Jolokia JMX-HTTP, stdlib `urllib`),
  TIBCO EMS (vendor `tibemsadmin` CLI over stdlib `subprocess`, fixed
  argv, no shell), Nginx (`stub_status` page over stdlib `urllib`),
  Apache HTTPD (`mod_status` machine-readable page over stdlib
  `urllib`, privileged reload via `apachectl`), HAProxy (stats HTTP CSV
  over stdlib `urllib`; server-state acts on the runtime API), PostgreSQL
  (`psycopg`, lazily imported), MySQL (`pymysql`, lazily imported,
  parameterized queries only), Oracle Database (`oracledb` THIN mode,
  lazily imported), Redis (`redis-py`, lazily imported, short-lived
  per-call clients), Elasticsearch (REST API over stdlib `urllib`),
  MongoDB (`pymongo`, lazily imported, read-only commands except
  privileged `killOp`), Kubernetes (official `kubernetes` client,
  lazily imported, kubeconfig or in-cluster auth, no tokens inlined),
  Docker (Engine API over the Unix socket, stdlib `socket` +
  `http.client`, no driver). Every connector follows the same guardrails
  as Kafka/Tomcat: guarded transport boundary, separate read vs
  privileged credentials, secrets never stored on the instance, never in
  `repr`/exceptions/logs, and the connector self-test harness covers all
  21 (201 passed, 0 failed, 9 informational skips).
- Tool surface: 27 -> 90. 63 new read tools and 15 new privileged tools
  (`restart_websphere_app`, `restart_weblogic_app`,
  `restart_jboss_deployment`, `purge_rabbitmq_queue`, `purge_artemis_queue`,
  `purge_ems_queue`, `reload_nginx`, `reload_apache`,
  `set_haproxy_server_state`, `terminate_postgres_backend`,
  `kill_mysql_query`, `kill_oracle_session`, `kill_mongo_op`,
  `restart_k8s_deployment`, `restart_docker_container`), added to
  `PRIVILEGED_TOOLS` (24 total). Per-technology tool counts:
  WebSphere 5, WebLogic 5, JBoss 5, RabbitMQ 4, Artemis 3, EMS 3,
  Nginx 3, Apache 3, HAProxy 2, PostgreSQL 4, MySQL 4, Oracle DB 4,
  Redis 3, Elasticsearch 3, MongoDB 4, Kubernetes 4, Docker 4.
- Scenarios: 11 -> 45. 34 new wave-3 scenarios, 2 per technology:
  `websphere_thread_saturation`, `websphere_app_stopped`,
  `weblogic_heap_pressure`, `weblogic_stuck_threads`,
  `jboss_deployment_failed`, `jboss_heap_high`,
  `rabbitmq_queue_backlog`, `rabbitmq_disk_alarm`,
  `artemis_queue_backlog`, `artemis_broker_down`,
  `ems_queue_backlog`, `ems_connection_storm`,
  `nginx_upstream_5xx`, `nginx_worker_crash`,
  `apache_workers_saturated`, `apache_5xx_spike`,
  `haproxy_backend_down`, `haproxy_session_saturation`,
  `postgres_blocking`, `postgres_replication_lag`,
  `mysql_runaway_query`, `mysql_replication_lag`,
  `oracle_tablespace_full`, `oracle_blocking_session`,
  `redis_memory_pressure`, `redis_replication_down`,
  `elasticsearch_red`, `elasticsearch_heap_pressure`,
  `mongo_long_op`, `mongo_replset_lag`,
  `k8s_crashloop`, `k8s_deployment_stalled`,
  `docker_container_exited`, `docker_memory_pressure`.
  14 new remediation plans (all safe single-action plans gated by the
  approval gate); 20 deliberately plan-less (e.g. stuck threads,
  queue backlogs with external consumers, disk/tableSpace alarms,
  replication lag, under-replicated ISR) — operator work with no safe
  automated remediation, so the engine proposes nothing and the incident
  stays open for manual investigation.
- Verification evidence: 1247 tests pass, 0 fail (was 343 in 0.4.0).
  Wave-3 coverage in `tests/test_scenarios_tranches.py`: injection shape,
  top-h1 score separation, plan-vs-None, engine plan wiring, privileged
  tools never fired during diagnose, discriminating verify specs
  (`tests/test_scenarios.py` now covers all 45 scenarios end to end).
  The web demo's scenario list carries descriptions for all 45
  scenarios.
- Connector status, stated plainly: all 17 new connectors are real
  clients but **fake-transport/fake-driver tested only** — canned
  driver/HTTP responses, no live middleware touched. Live validation
  against real systems still needs a customer pilot (see
  `docs/REAL_CONNECTOR_READINESS.md`, and the new
  `docs/TECHNOLOGY_COVERAGE.md` coverage map).
- Governance unchanged: approval gate, hash-chained audit, default-deny,
  and the Jev reasoning layer are untouched.

## 0.4.0 (2026-09-21)

Real Kafka and Tomcat connectors, connector self-test harness, three new
scenarios.

- Scenarios: 8 -> 11. New: `kafka_broker_config_drift` (broker 3's
  `log.retention.hours` diverges 72h vs 168h across brokers 1-2 with
  per-partition consumer lag; plan restarts the lagging consumer group and
  flags the drifted config as an operator follow-up),
  `tomcat_thread_exhaustion` (the `http-nio-8080` pool is 200/200 busy with
  catalina log evidence; the plan runs privileged `restart_tomcat_app` and
  verifies the nested `pools.0.current_threads_busy` reading), and
  `kafka_under_replicated` (under-replicated ISR on `payments-events`
  partition 2; deliberately NO remediation plan — fixing ISR is an operator
  job — so the engine proposes nothing and the incident stays open for
  manual investigation). `agent/rca.py` handles the three new alert types
  with evidence gathering only, never privileged paths.
- Tool surface: 18 -> 27. New read tools: `get_kafka_consumer_group_detail`,
  `get_kafka_topic_detail`, `get_kafka_broker_config`, `get_kafka_broker_health`,
  `get_tomcat_heap`, `get_tomcat_threadpool`, `get_tomcat_apps`,
  `read_tomcat_log`. New privileged tool: `restart_tomcat_app` (added to
  `PRIVILEGED_TOOLS`).
- Real Kafka connector (`connectors/kafka.py`): live diagnostics through
  the real `kafka-python` client, imported lazily so the module loads with
  the driver absent. Per-partition lag detail, topic topology, broker
  config, and broker health. Honest limitation: kafka-python's public
  surface exposes no cluster/broker listing, so `get_kafka_broker_health`
  degrades to reachable + latency, and leader/replica/ISR fields fall back
  to empty where the driver's public `describe_topics` is unavailable; no
  private driver internals are touched. `restart_consumer` stays
  hook-driven (Kafka has no remote restart API); the hook signature is now
  `hook(topic, group, user, password)`, passing the separately-resolved
  privileged identity so the hook can authenticate to the customer's own
  consumer supervisor.
- Real Tomcat connector (`connectors/tomcat.py`): Jolokia JMX-HTTP is the
  primary transport (stdlib `urllib`, no driver needed); the Tomcat Manager
  text API is the alternate transport for app state and the required path
  for privileged app restart. Separate `TOMCAT_READ_*` / `TOMCAT_ADMIN_*`
  credential pairs; secrets resolve per request and are never stored on the
  instance, never appear in `repr`/exceptions/logs. `read_tomcat_log` is
  co-located best-effort: it tails files on the connector's own disk, so
  remote log access needs a log shipper (out of scope).
- Connector self-test/contract harness (`connectors/harness.py`, ADR 0010):
  a runnable onboarding check with no pytest dependency —
  `.venv/bin/python -m connectors.harness [--connector NAME] [--json]`,
  exit 0 pass / 1 fail / 2 unknown, `--json` for CI. Ten static contract
  checks per connector: construction, registration, capabilities,
  read/act isolation, ctor-secret-params, repr hygiene, driver-absent
  connect, instance secret scan, privileged refusal, result-shape boundary.
  All four connectors pass: 37 passed, 0 failed, 3 informational skips
  (per-connector informational checks documented in the output). Explicit
  boundary: the harness checks the static contract only — it never contacts
  a live target, resolves a real secret, or requires the vendor driver.
  Live validation stays the pytest suite's job plus a customer pilot.
- Dotted-path verification: `agent/plans.py` `check_verify()` now resolves
  nested `expect` keys (e.g. `pools.0.current_threads_busy`) against tool
  results, alongside the existing plain and `$lt/$lte/$gt/$gte/$eq` matches.
- Connector status, stated plainly: Kafka and Tomcat are real clients but
  **fake-transport tested only** — canned driver/HTTP responses, no live
  middleware touched. Live validation against real systems still needs a
  customer pilot (see `docs/REAL_CONNECTOR_READINESS.md`).
- Verification evidence: 343 tests pass, 0 fail (was 238 in 0.3.0).
- Governance unchanged: approval gate, hash-chained audit, default-deny,
  and the Jev reasoning layer are untouched.

## 0.3.0 (2026-09-21)

Jev-based reasoning, on by default.

- Jev reasoning layer (`agent/jev.py`, ADR 0009): TypeSafe System One is
  the default reasoning experience when `TYPESAFE_API_KEY` is configured.
  One batched call per diagnosis yields typed hypothesis ranking with
  calibrated probabilities, per-claim evidence-support verification
  (`supported` / `partially_supported` / `unsupported`), triage priority
  (P1–P4), and remediation risk class (low/medium/high). Jev advises only:
  it never overrides human approval, default-deny, the audit trail, or
  verification; when it disagrees with the deterministic top hypothesis
  the UI shows the disagreement and the deterministic result still
  decides. Service errors and timeouts degrade to the deterministic
  engine, never crash the run.
- Deterministic mode: with no key (or `RCA_JEV_ENABLED=0`) the engine runs
  alone and the UI/report label it "deterministic mode — set
  TYPESAFE_API_KEY for Jev confidence scoring". `GET /api/jev/status`
  reports the mode for the UI banner.
- Visible confidence: the web UI shows a Jev status banner, per-hypothesis
  confidence bars, evidence-support verdicts on each cited claim,
  agreement/disagreement with the deterministic top, triage priority, and
  remediation risk. Remediation risk is scored at plan time
  (`advise_remediation_risk`) — never at diagnose time, where there are no
  steps to judge; the plan view shows a "Jev remediation risk" panel for
  the exact proposed plan. The markdown incident report gains a "Jev
  confidence" section (folding in the plan-time risk). The diagnose API
  response carries a `jev` block; the plan response carries
  `remediation_risk`.
- Removed the obsolete unwired `LLMHook` stub from `agent/rca.py`; the Jev
  layer is the wired reasoning integration. Key hygiene: from the
  environment only, never logged/audited/returned; audit carries question
  hashes and bounded scalars. `StubJevClient` for tests (live API never
  called in the suite); `DemoJevClient` for clearly-labeled illustrative
  values in the hosted demo.
- `RCA_BIND`/`RCA_PORT` env config for the web server (`--bind`/`--port`
  flags); default stays loopback-only, `0.0.0.0` honored only for
  containerized runs.

## 0.2.0 (2026-09-20)

Phase 2: the complete reference demo — full incident flow, screen-shareable.

- Scenario breadth: 3 -> 8 fault-injection scenarios (`channel_stopped`,
  `disk_full`, `kafka_lag`, plus new `tomcat_oom`, `expired_tls`,
  `config_drift`, `listener_down`, `poison_message`, the last with an inert
  adversarial "purge all queues" log line the engine must ignore). New
  deterministic estate surface: MQ listeners, per-channel TLS certificates,
  host log archiving, app restart, Kafka consumer restart, queue MAXDEPTH
  updates with a `baseline_max_depth` reference for drift detection, and
  poison-message quarantine to SYSTEM.DLQ.
- MCP tools: 7 -> 18 (9 new read-only/diagnostic tools and 7 new privileged
  tools under `admin:write`, including `read_app_log`, `get_listener_status`,
  `get_cert_status`, `archive_logs`, `restart_app`, `restart_consumer`,
  `renew_certificate`, `update_queue_config`, `start_listener`,
  `quarantine_message`, `tail_log`). `Gateway` accepts an `include` filter so
  a connector exposes only the tools its backend supports.
- Multi-step remediation plans (`agent/plans.py`): ordered privileged steps
  proposed as data by the engine (`agent/remediation.py`,
  `RCAEngine.propose_plan()`), each step approved separately through the
  approval gate (default deny, plan-order enforced), each step re-verified
  with read-only checks before the next runs. "Approve all remaining" is an
  explicit, separately-audited choice; a rejection or a failed verification
  halts the plan with the incident left open and reported. The CLI demo
  (`demo/run_incident.py`) now runs the full plan flow with per-step
  `[y]es / [n]o / [a]ll-remaining` prompts.
- Web demo UI (`demo/web.py`, stdlib `http.server` + vanilla JS, zero new
  dependencies): `python -m demo.web` -> http://127.0.0.1:8765 (loopback
  only). Scenario injection, live RCA timeline, evidence cards with cited
  claims, per-step approve/reject/approve-all, audit-trail viewer, one-click
  markdown incident report download, and a sim-vs-real host compare panel.
  Reuses the exact engine/gateway/audit code paths as the CLI.
- Incident report export (`agent/report.py`, `demo/report.py`):
  `python -m demo.report runs/<run> [--out report.md]` renders timeline,
  cited evidence, hypothesis, approvals (step-by-step vs approve-all),
  actions, outcome, and audit-chain validity from the tamper-evident log.
- First real connector (`connectors/linux_host.py`): reads live CPU/memory/
  disk/load from `/proc` plus allow-list-sandboxed log tailing, through the
  same MCP authz/audit path. Read-only by construction, no shelling out.
  `python -m demo.host_compare` shows sim vs real side by side.
- `diagnosis_complete` audit details now carry the top hypothesis title and
  cited evidence, making the audit trail self-contained for reporting.
- Docs: new ADRs 0006 (multi-step plans), 0007 (stdlib web UI), 0008 (real
  connector sandboxing); new `docs/DEMO_SCRIPT.md` 3-minute narrated demo
  script; TOOLS, ARCHITECTURE, RUNBOOK, README updated throughout.

## 0.1.0 (2026-09-20)

Initial reference demo.

- Deterministic simulated middleware estate (`sim/`): IBM MQ queues and
  channels, Kafka consumer lag, Tomcat app logs, Linux host metrics, with a
  seedable clock for reproducible runs.
- Fault injection (`sim/incidents.py`) for three scenarios:
  `channel_stopped`, `disk_full`, `kafka_lag`, plus an adversarial
  `poison_log` option for the prompt-injection test.
- MCP server (`mcp_server/`) with 7 tools (6 read-only with
  `diagnostics:read`, plus privileged `restart_channel` with `admin:write`),
  demo bearer-token auth, and a single `Gateway` policy point for token
  verification, scope authorization, audit, and dispatch.
- Deterministic RCA engine (`agent/rca.py`) that gathers evidence via
  read-only tools, scores hypotheses as fraction of matched checks, cites
  evidence, and emits remediation as data only.
- Human approval gate (`agent/approvals.py`): default deny; privileged
  execution only on a live `APPROVED` grant.
- Hash-chained JSONL audit log (`audit/`) with tamper detection via
  `AuditLog.verify()`.
- Connector seam (`connectors/`) defining the `Connector` protocol where real
  middleware drivers would plug in; no drivers shipped.
- End-to-end demo CLI (`demo/run_incident.py`) with `--scenario`,
  `--auto-approve`, and `--seed` flags, plus stdio MCP server
  (`mcp_server/server.py`) for external MCP clients.
- Docs-as-code documentation set (`docs/`), architecture decision records
  (`docs/adr/`), MIT license.
