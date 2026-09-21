# Real Connector Readiness

What a customer provides so the RCA assistant can talk to their **real** middleware estate instead of the simulated estate.
21 technologies, 90 tools, 45 scenarios (see
`docs/TECHNOLOGY_COVERAGE.md` for the full map). One section per
connector: exactly what we need from you, what we will never ask for,
and a validation checklist that separates what is already proven
in our suite from what still needs a live pilot in your environment.

> Standing rule, all connectors: **we never take raw credentials.** You
> deliver credentials through YOUR vault (HashiCorp Vault, CyberArk,
> AWS Secrets Manager, Azure Key Vault, ...). Our connector receives a
> provider callable or env-var names that resolve at connect time. The
> values never appear in our code, configs, logs, audit entries, or
> exception messages. If a credential ever shows up in a log, that is a
> bug, not a feature.

---

## 0. Customer onboarding self-test (runs before the pilot)

Before any pilot, run the connector self-test harness against your
configuration — no live target, no real credentials, no vendor driver
required:

```
.venv/bin/python -m connectors.harness [--connector NAME] [--json]
```

Exit 0 means every check passed (informational skips allowed); 1 means a
check failed; `--json` emits machine-readable results for CI. The harness
builds every registered connector with probe (non-secret) values and runs
ten static contract checks: construction, registration, capabilities,
read/act dispatch isolation, ctor-secret-params, repr hygiene,
driver-absent connect, instance secret scan, privileged refusal, and
result-shape boundary. See ADR 0010 for the full design.

The boundary to hold in mind: the harness checks the **static contract
only** — it never contacts a live target, never resolves a real secret,
and never requires the vendor driver. A green harness means "safe to wire
into your config and proceed to pilot", not "proven against production".
The "Requires live pilot" rows below are what the harness cannot see.

---

## 1. IBM MQ connector (`ibmmq`)

Driver: `pymqi` + the IBM MQ client libraries on the connector host.

### What you provide

| Item | Details |
|---|---|
| Host / port | Queue-manager host(s) and listener port (default 1414) reachable from the connector host. |
| Queue manager + channel | Queue manager name and a client (SVRCONN) channel dedicated to this integration. |
| TLS | If you require TLS: the cipher spec for the channel and a key repository (`.kdb`) path the connector host can read. Plain TCP is supported only where your policy allows it. |
| Read service account | An MQ user with **read-only** authority: inquire on queues/channels/listeners, browse where quarantine needs it, nothing else. Default env refs: `MQ_READ_USER` / `MQ_READ_PASSWORD`. |
| Privileged service account | A **separate** MQ user authorized for the four privileged actions only: stop/start channel, alter queue MAXDEPTH, start listener, get+put for DLQ quarantine. Default env refs: `MQ_ADMIN_USER` / `MQ_ADMIN_PASSWORD`. Privileged calls open their own session under this identity; if it is absent, privileged actions refuse rather than reusing the read account. |
| Error-log access | A path (or `MQ_ERROR_LOG_DIR`) to the queue manager's `AMQERR01.LOG` directory readable by the connector host. The connector tails it; entries are parsed best-effort into `{ts, severity, code, message}`. |
| Dead-letter queue | Name of the DLQ for poison-message quarantine (default `SYSTEM.DEAD.LETTER.QUEUE`). |
| Credential delivery | YOUR vault. Give us a provider callable or the env-var names above; we resolve at connect time and never persist or log the values. |

### Network / firewall

- TCP from the connector host to each queue-manager host:port (typically 1414, or your TLS port).
- If the connector runs off-host from the queue manager, also allow read access to the `AMQERR01.LOG` directory (NFS/SMB mount or a log-ship sidecar; no agent runs on your MQ host).
- No inbound ports needed on your side; the connector only dials out.

### Validation checklist

| Check | Status |
|---|---|
| Module imports cleanly without pymqi; driver-missing error carries install instructions | Verified with fakes in our suite |
| Queue depth / channel status / listener status / config via PCF inquire | Verified with fakes in our suite |
| Error-log tail + AMQERR parse | Verified with fakes in our suite |
| Cert status shape (cipher/label/peer from PCF; expiry only when the driver exposes it) | Verified with fakes in our suite |
| Privileged: channel restart, MAXDEPTH change, listener start, DLQ quarantine | Verified with fakes in our suite |
| Separate privileged credential enforced; refused act() leaves the read session untouched | Verified with fakes in our suite |
| No secret in repr / exceptions / results | Verified with fakes in our suite |
| **Live PCF against your queue manager** (depth values, channel status codes, cert attributes) | Requires live pilot |
| **TLS handshake with your cipher + key repository** | Requires live pilot |
| **AMQERR01.LOG format variants in your MQ version** | Requires live pilot |
| **Privileged actions under your admin account** (stop/start channel, alter queue, DLQ move) | Requires live pilot |

---

## 2. Kafka connector (`kafka`)

Driver: `kafka-python`, imported lazily (module loads without it; driver
paths raise with install instructions). Status: **real client,
fake-transport tested** — per-partition lag detail, topic topology, broker
config, and broker health read through the driver's public API; the
restart path is hook-driven by design (Kafka has no remote "restart a
consumer group" API).

Honest limitations, by driver surface: kafka-python's public API exposes
no cluster/broker listing, so `get_kafka_broker_health` degrades to
reachable + latency; leader/replica/ISR fields in `get_kafka_topic_detail`
fall back to empty where the driver's public `describe_topics` is
unavailable. No private (`_`-prefixed) driver internals are touched.

### What you provide

| Item | Details |
|---|---|
| Bootstrap servers | `host:port` list reachable from the connector host. |
| Security | `security_protocol` (`SASL_SSL` default, `PLAINTEXT` only where your policy allows), `sasl_mechanism` (`PLAIN` default). |
| Service account | A Kafka user with describe/read on the topics and consumer groups being diagnosed. Default env refs: `KAFKA_USER` / `KAFKA_PASSWORD`. SASL is only configured when the protocol uses it. |
| Restart hook (privileged) | A callable `hook(topic, group, user, password) -> dict` wired to YOUR consumer supervisor (systemd unit bounce, Kubernetes rollout restart, etc.). The connector passes the separately-resolved privileged identity (`KAFKA_ADMIN_USER` / `KAFKA_ADMIN_PASSWORD` or your privileged provider) so the hook can authenticate to your supervisor. Without a hook, `restart_consumer` raises instead of pretending to restart something. |
| Credential delivery | YOUR vault, same rule as MQ: provider callable or env-var names, resolved at use time, never logged. |

### Network / firewall

- TCP from the connector host to the bootstrap servers and the brokers they advertise (Kafka clients connect to advertised listeners, so firewall rules must cover the broker addresses, not just the bootstrap host).

### Validation checklist

| Check | Status |
|---|---|
| Module imports cleanly without kafka-python; driver-missing error carries install instructions | Verified with fakes in our suite |
| Per-partition lag, topic detail, broker config, broker health via public driver API | Verified with fakes in our suite |
| Restart hook receives the separately-resolved privileged identity; absent hook or absent privileged credential refuses | Verified with fakes in our suite |
| No secret in repr / exceptions / results | Verified with fakes in our suite |
| Connector harness contract checks (10 checks) | Green (run `.venv/bin/python -m connectors.harness --connector kafka`) |
| **Consumer-lag math against a real cluster** (end offsets vs committed offsets, incl. SASL) | Requires live pilot |
| **Broker config/health fields against your client version** (esp. `describe_configs` / `describe_topics` availability) | Requires live pilot |
| **Restart hook against your consumer supervisor** | Requires live pilot |
| **Advertised-listener reachability through your firewall** | Requires live pilot |

---

## 3. Tomcat connector (`tomcat`)

Transport: Jolokia JMX-HTTP primary (stdlib `urllib`, **no driver** —
plain HTTP to `http://host:port/jolokia/`); Tomcat Manager text API
alternate for app deployment state, and the required path for privileged
app restart (stop + start). Status: **real client, fake-transport
tested** — canned Jolokia JSON and Manager text responses, no live Tomcat
touched.

Honest limitations: the Manager text API does not expose heap or
thread-pool data, so those reads raise `ConnectorError` in manager mode
(documented limitation, not a bug). `read_tomcat_log` tails files on the
connector's own disk (catalina.out, localhost access logs): it assumes the
connector runs co-located with Tomcat (same host or shared log volume).
Remote log access needs a log shipper, which is out of scope for this
connector.

### What you provide

| Item | Details |
|---|---|
| Jolokia base URL | `http://host:port/jolokia/` reachable from the connector host, for heap / thread-pool / app reads. |
| Manager text URL | `http://host:port/manager/text/` reachable from the connector host — required only if you want privileged app restart. |
| Read service account | A Tomcat user with read access to the Jolokia agent and (optionally) the Manager text API. Default env refs: `TOMCAT_READ_USER` / `TOMCAT_READ_PASSWORD`. |
| Privileged service account | A **separate** Tomcat user with Manager-script rights for app stop/start only. Default env refs: `TOMCAT_ADMIN_USER` / `TOMCAT_ADMIN_PASSWORD`. `restart_tomcat_app` resolves this pair at act time and refuses when it is absent rather than reusing the read account. |
| Log access | The connector must run co-located with Tomcat (or on a host sharing its log volume) for `read_tomcat_log`. |
| Credential delivery | YOUR vault, same rule as MQ: provider callable or env-var names, resolved per request, never stored on the instance, never in `repr` / exceptions / logs. |

### Network / firewall

- TCP from the connector host to the Tomcat HTTP port(s) (Jolokia and Manager URLs).

### Validation checklist

| Check | Status |
|---|---|
| Jolokia transport fails safe (all urllib/socket failures wrapped in `ConnectorError`) | Verified with fakes in our suite |
| Heap / thread-pool / app reads against canned Jolokia JSON; manager-mode limitation documented | Verified with fakes in our suite |
| Privileged restart refuses without the separate admin credential | Verified with fakes in our suite |
| No secret in repr / exceptions / results | Verified with fakes in our suite |
| Connector harness contract checks (10 checks) | Green (run `.venv/bin/python -m connectors.harness --connector tomcat`) |
| **Jolokia agent presence and agent version on your Tomcat** (MBean attribute names can differ) | Requires live pilot |
| **Manager text API restart (stop + start) under your admin account** | Requires live pilot |
| **Log file locations and formats on your Tomcat** | Requires live pilot |

---

## 4. Linux host connector (`linux-host`)

No driver, no credentials: it reads `/proc` and allow-listed log files on
the machine it runs on, and only that machine. Deploy one connector
instance per host you want diagnosed (or point the allow-list at your
centralized log mount). Read-only by construction; there is no privileged
surface to credential.

| Check | Status |
|---|---|
| Real `/proc` metrics, allow-listed log tails, gateway authz/audit wiring | Verified against the live local host in our suite |
| **Log allow-list paths in your environment** | Requires live pilot (config, not code) |

---

## 5. Wave-3 connectors (17 new, v0.6.0)

All 17 follow the same standing rules (we never take raw credentials;
separate read vs privileged service accounts; fake-transport/fake-driver
tested in our suite; the per-connector pilot is live validation). The
harness covers every one: `.venv/bin/python -m connectors.harness
--connector NAME`.

| Connector | Driver / transport | What you provide | Privileged tools |
|---|---|---|---|
| `websphere` (WebSphere) | Jolokia JMX-HTTP agent on the JVM; stdlib `urllib`, no driver | Jolokia URL reachable from the connector host; read and admin service accounts | `restart_websphere_app` |
| `weblogic` (WebLogic) | Management REST API; stdlib `urllib` | REST endpoint (`management/wls/latest`); read and admin service accounts | `restart_weblogic_app` |
| `jboss` (JBoss EAP / WildFly) | HTTP management API (`:9990/management`); stdlib `urllib` | management endpoint; read and admin service accounts | `restart_jboss_deployment` |
| `rabbitmq` | Management HTTP API (`:15672/api`); stdlib `urllib` | management endpoint; read and admin users | `purge_rabbitmq_queue` |
| `artemis` (ActiveMQ Artemis) | Jolokia JMX-HTTP; stdlib `urllib` | Jolokia URL (`console/jolokia` on stock console); read and admin accounts | `purge_artemis_queue` |
| `tibco_ems` (TIBCO EMS) | vendor `tibemsadmin` CLI over subprocess | `tibemsadmin` binary on the connector host; server URL, read and admin users | `purge_ems_queue` |
| `nginx` | `stub_status` page; stdlib `urllib` | stub_status page enabled; log file paths | `reload_nginx` |
| `apache` (Apache HTTPD) | `mod_status` page; stdlib `urllib` | `?auto` mod_status endpoint; log paths; `apachectl` on the connector host (co-located) | `reload_apache` |
| `haproxy` | stats HTTP CSV; runtime API; stdlib `urllib` | stats endpoint; runtime API socket or admin user | `set_haproxy_server_state` |
| `postgres` (PostgreSQL) | `psycopg` (lazy) | host/port/database; read and privileged roles | `terminate_postgres_backend` |
| `mysql` (MySQL) | `pymysql` (lazy) | host/port; read and privileged users | `kill_mysql_query` |
| `oracle_db` (Oracle DB) | `oracledb` THIN mode (lazy, pure Python) | connect string; read and privileged users | `kill_oracle_session` |
| `redis` | `redis-py` (lazy) | host/port; read user (no privileged surface) | none |
| `elasticsearch` | REST API; stdlib `urllib` | cluster endpoint; read user (no privileged surface) | none |
| `mongodb` | `pymongo` (lazy) | connection string; read and admin users | `kill_mongo_op` |
| `kubernetes` | official `kubernetes` client (lazy) | kubeconfig file (or in-cluster identity); RBAC roles for reads and for rollout restart | `restart_k8s_deployment` |
| `docker` | Engine API over the Unix socket; stdlib only | `/var/run/docker.sock` mounted for the connector host | `restart_docker_container` |

Validation for all 17: reads match your own tooling first, then
privileged actions only through the approval gate with the hash-chained
audit trail — same pilot runbook as sections 1-3.

---

## Pilot runbook (all connectors)

1. Run the onboarding self-test: `.venv/bin/python -m connectors.harness [--connector NAME]` — fix anything red before touching your environment.
2. You provision the service accounts and network paths above; we get vault
   references, never secrets.
3. We run the read surface first (`get_queue_depth`, `get_channel_status`,
   `get_kafka_consumer_lag`, `get_kafka_broker_health`, `get_tomcat_heap`,
   `get_host_metrics`) and compare against your own tooling (MQ Explorer /
   `runmqsc`, Kafka consumer-group CLI, Tomcat manager, `top`).
4. Only after reads match do we exercise privileged actions, each through
   the approval gate with the hash-chained audit trail recording who
   approved what.
5. Anything in the "Requires live pilot" column above that misbehaves is
   fixed in the connector and re-validated before sign-off.
