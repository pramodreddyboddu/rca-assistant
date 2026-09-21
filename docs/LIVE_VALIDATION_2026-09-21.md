# Live Validation — 2026-09-21 (dev machine)

First run of the RCA Assistant's **real connector clients against real services**.
Prior to this, every connector was exercised only through fake-transport /
fake-driver tests. On 2026-09-21 we stood up real services on a dev machine
(localhost only, throwaway test data, weak local-only credentials) and called
the real connector classes directly — `connect()` plus public read methods —
bypassing the static contract harness, which never contacts a live target.

Raw per-connector result files: `hidden_files/live-validation/*.json`.

## Per-connector results

| Connector | Service (version) | What was seeded | Result |
|---|---|---|---|
| `redis` | Redis 7.0.15 (apt) | keys incl. `mq:queue:depth:orders.in`=42, a 3-field hash, a 2-item list | **4/4 PASS** — INFO, replication, slowlog, seed round-trip all live |
| `postgres` | PostgreSQL 16.15 (apt) | database `rcatest`, table `orders` with 5 rows, throwaway read role | **4/4 PASS** — health, blocking, replication, 5-row query through the read role |
| `nginx` | nginx 1.24.0 (apt) on :8080 | static page, `stub_status` at `/nginx_status`, 20 seeded curl hits (incl. 404s) | **2/2 PASS** — live counters tracked traffic exactly (2 → 25); access log parsed the seeded hits |
| `kafka` | Apache Kafka 3.9.1 (KRaft single broker, PLAINTEXT, :9092) | topic `rca-test`, 10 messages | **6/7 PASS** — connect, topic detail (end_offset=10 matches seeds), consumer-group lag (lag=10), broker health, unknown-topic error path clean. **1 FAIL:** `get_kafka_broker_config` — the connector probes for `ConfigResource` at the wrong import location for kafka-python 3.x and expects the old futures-dict API; live read worked once corrected at runtime. Repo fake-transport tests mock the old API, so they cannot catch this. **Bug filed below.** |
| `tomcat` | Apache Tomcat 10.1.46 (tarball), OpenJDK 21, :8081, Jolokia 2.6.3 agent | manager-script user, Jolokia war deployed | **6/8 PASS** — both transports connect, 6 real apps listed, real heap (20.5% of 384MB), catalina + access log tails parse. **2 FAIL (real bugs):** `get_tomcat_threadpool` — bulk full-attribute Jolokia GET breaks because one attribute getter (`NioEndpoint.getDeferAccept`) throws on Tomcat 10.1.46; fix = read the 3–4 needed attributes individually. `get_tomcat_apps` (jolokia) — connector fully URL-quotes mbean ObjectNames (`%2F%2F`), which Tomcat rejects with 400; fix = POST-body reads as `_jolokia_search` already does. |
| `linux_host` | this machine (2 CPU, ~8GB RAM) | temp log file with 5 lines | **5/5 PASS** — real metrics (cpu 5.3%, mem 67.8%, disk 3.0%, load1 0.98, all plausible/nonzero), log tail returns last lines with correct line numbers. Security boundaries held: out-of-allow-list path → `PermissionError`; remote hostname → `ConnectorError`. A seeded prompt-injection line in the log was returned as inert text, never acted on. |

Totals: **29 checks, 26 pass, 3 fail** across 6 connectors. RabbitMQ was
attempted as a stretch goal and skipped with reason (Erlang runtime + broker
would not fit the RAM budget without risking the running validations).
IBM MQ / WebSphere / Oracle DB were not attempted (licensed or heavyweight) —
recorded as out of scope, not failures.

## Genuine bugs found (all fixed and re-validated live — see below)

1. **Kafka `get_kafka_broker_config`** — wrong `ConfigResource` import path for
   kafka-python 3.x; expected the old futures-dict from `describe_configs()`.
   *Fixed:* nested-dict result parsing with legacy fallback; also passes
   `config_filter="all"` after live re-validation showed the driver's
   default `"modified"` filter returns zero rows on a fresh broker.
2. **Tomcat `get_tomcat_threadpool`** — bulk full-attribute Jolokia GET read is
   fragile; now reads the needed attributes individually via POST-body.
3. **Tomcat `get_tomcat_apps` (jolokia)** — over-URL-quoted mbean ObjectNames
   rejected by Tomcat; now uses POST-body reads. Live re-validation exposed
   one more layer: a full-mbean read 500s when any attribute getter throws
   (`DirectJDKLog.isFatalEnabled`), and a multi-attribute read 404s when any
   one attribute is absent — so the method now reads `stateName` alone and
   falls back to `state` only when the mbean lacks it.

All three were invisible to the fake-transport test suite by construction —
this is exactly what live validation is for. Recommended follow-up: add a
live-validation CI lane that runs these checks against containerized services.

## Post-fix live re-validation (2026-09-21 ~4:35 PM Central)

Each fixed method was re-run against a freshly started local service:

- **Kafka `get_kafka_broker_config(1)`** — PASS: 314 configs, `log.dirs`
  matches the live broker, unknown broker 99 raises a clean `ConnectorError`.
  Sanity: `get_kafka_topic_detail("rca-test")` end_offset=10 after re-seed.
- **Tomcat `get_tomcat_threadpool`** — PASS: `http-nio-8081` busy=1,
  count=10, max=200 (real values).
- **Tomcat `get_tomcat_apps` (jolokia)** — PASS: all 6 deployed apps listed,
  every state `running`, real per-app session counts.
- **Tomcat `get_tomcat_heap`** — PASS sanity: real heap reading (0.6% used).

Full suite in a clean venv (no vendor drivers installed): **1283 passed,
0 failed**. Connector harness: **201 passed, 0 failed, 9 skipped** across
21 connectors. All services were stopped after re-validation.

## What this proves — and what it does not

**Proves:**
- The real client code paths (lazy driver imports, connect, read dispatch,
  result shaping, credential-env plumbing) work against real OSS services:
  Redis 7.0.15, PostgreSQL 16.15, nginx 1.24.0, Kafka 3.9.1 (KRaft),
  Tomcat 10.1.46, and the local Linux host.
- Read-only isolation holds under live use: no privileged/act method was
  called during any validation.
- The validation itself has teeth: it found 3 real bugs the test suite missed.

**Does NOT prove:**
- Behavior against customer environments (TLS, auth schemes, network
  policies, proxies, hardened configs).
- IBM MQ, WebSphere, WebLogic, Oracle DB, or any licensed/heavyweight target —
  none was available on this machine.
- Clustered / production topologies (multi-broker Kafka, MQ multi-instance,
  replicated Postgres, nginx fleets).
- Performance or reliability under incident-scale load.

## Addendum: IBM MQ live-grounding + record/replay (2026-09-21 ~10:30 PM Central)

After the earlier session, IBM MQ became the **7th live-validated
technology** — and the first with permanent record/replay fixtures.

**Live session.** Real IBM MQ 10.0.0.5, queue manager `QM1`, plain TCP on
port 1414 (throwaway developer queue manager, simplified local auth).
Seeded incident state: `APP.Q1` depth 60, `APP.Q2` depth 0,
`BACKLOG.Q` depth 200, `EMPTY.Q` depth 0, `BATCH.CHL` STOPPED,
`LISTENER.TCP` RUNNING. **18/18 functional checks passed**, plus two
deliberate error-path probes (unknown queue -> clean `ConnectorError`;
wrong queue-manager name rejected). Privileged paths were genuinely
exercised: queue MAXDEPTH 5000 -> 1000 -> 5000, empty and non-empty
quarantine paths, stopped-channel restart, listener start from STOPPED and
from RUNNING, read session restored after privileged sessions.

**Seven real connector defects found and fixed:** (1) PCF queue/channel
change commands needed object-type parameters; (2) correct listener command
is `MQCMD_START_CHANNEL_LISTENER`; (3) running listener status code is 2,
stopped listeners need the defined-object fallback; (4) restarting an
already-inactive channel must tolerate reason 4064; (5) starting an
already-running listener must tolerate reason 3249; (6) `pymqi` allows one
live connection per thread, so privileged sessions must disconnect and
later restore the read connection; (7) certificate-related PCF byte strings
now decode to clean text, blank becoming `None`.

**Record/replay.** Nine real fixtures live under
`tests/fixtures/recorded/ibmmq/`: 3x `inquire_q` (`APP.Q1` depth 60,
`APP.Q2` depth 0, `BACKLOG.Q` depth 200), 2x `inquire_channel_status`
(`APP.SVRCONN` RUNNING, `BATCH.CHL` STOPPED), 2x `inquire_listener_status`
(`LISTENER.TCP` RUNNING, plus the recorded 2085 no-instance outcome for the
stopped listener), 1x `inquire_listener` (stopped-listener definition
fallback), 1x `read_error_log` (2,206 real AMQERR01.LOG lines). The
connector's PCF read paths are now wired through
`connectors/recording.py` (`Recorder("ibmmq", default_mode="passthrough")`):
production default always hits the driver (behavior unchanged);
`RCA_RECORD_MODE=replay` serves the fixtures with zero driver installed;
`RCA_RECORD_MODE=live` re-records them; missing fixtures fail closed.
Privileged actions never go through the recorder.
`tests/test_ibmmq_replay.py` replays the fixtures end to end with the
`pymqi` import poisoned: **11/11 pass**, proving the incident shape (depth
200 backlog, STOPPED channel) and the connector's response shaping without
any driver, network, or queue manager.

**Honest limits.** TLS was not exercised. Customer authentication, network
policy, clustering, and production topology remain unproven. `restart_channel`
reports hardcoded `RUNNING` without polling post-start status. MQ was stopped
cleanly after validation but remains installed and defined for re-recording.

With this addendum, the live-validation count is **7 technologies**
(Redis, PostgreSQL, nginx, Kafka, Tomcat, Linux host, IBM MQ).
