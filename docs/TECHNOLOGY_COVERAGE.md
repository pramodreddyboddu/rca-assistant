# Technology Coverage

Coverage map for the 21 technologies in the v0.7.0 catalog: what the
connector speaks, how many MCP tools and incident scenarios back it, and
the honest testing status.

**Status key:** every connector is a real client implementation but
*fake-transport / fake-driver tested only* — the pytest suite and the
connector self-test harness use canned driver/HTTP responses and never
touch live middleware. *Live validation* (a connector green against a
real system) is the customer's self-test step in a pilot, via
`python -m connectors.harness --connector NAME`.

| Technology | Connector | Transport | Tools | Scenarios | Status |
|---|---|---|---|---|---|
| IBM MQ | `connectors/ibmmq.py` | PCF over lazily-imported `pymqi` | 13 (6 read + 7 privileged) | 5 | fake-driver tested; live validation = customer pilot |
| Kafka | `connectors/kafka.py` | lazily-imported `kafka-python`; privileged restart via customer hook only | 6 (5 read + 1 privileged) | 3 | fake-driver tested; live validation = customer pilot |
| Apache Tomcat | `connectors/tomcat.py` | Jolokia JMX-HTTP + Manager text API (stdlib `urllib`) | 5 (4 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| Linux host | `connectors/linux_host.py` | local `/proc` + allow-listed log reads (read-only by construction) | 3 read (no privileged) | 1 | live local reads |
| IBM WebSphere | `connectors/websphere.py` | Jolokia JMX-HTTP (stdlib `urllib`, no driver) | 5 (4 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| Oracle WebLogic | `connectors/weblogic.py` | Management REST API (stdlib `urllib`, Basic auth, `X-Requested-By`) | 5 (4 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| JBoss EAP / WildFly | `connectors/jboss.py` | HTTP management API (stdlib `urllib`, HTTP Digest auth) | 5 (4 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| RabbitMQ | `connectors/rabbitmq.py` | Management HTTP API (stdlib `urllib`) | 4 (3 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| ActiveMQ Artemis | `connectors/artemis.py` | Jolokia JMX-HTTP (stdlib `urllib`) | 3 (2 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| TIBCO EMS | `connectors/tibco_ems.py` | vendor `tibemsadmin` CLI (stdlib `subprocess`, fixed argv, no shell) | 3 (2 read + 1 privileged) | 2 | fake-CLI tested; live validation = customer pilot |
| Nginx | `connectors/nginx.py` | `stub_status` page (stdlib `urllib`) | 3 (2 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| Apache HTTPD | `connectors/apache.py` | `mod_status` page (stdlib `urllib`); privileged reload via `apachectl` | 3 (2 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| HAProxy | `connectors/haproxy.py` | stats HTTP CSV (stdlib `urllib`); runtime API for server-state acts | 2 (1 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |
| PostgreSQL | `connectors/postgres.py` | lazily-imported `psycopg`; per-call connections, no pool | 4 (3 read + 1 privileged) | 2 | fake-driver tested; live validation = customer pilot |
| MySQL | `connectors/mysql.py` | lazily-imported `pymysql`; parameterized queries only | 4 (3 read + 1 privileged) | 2 | fake-driver tested; live validation = customer pilot |
| Oracle DB | `connectors/oracle_db.py` | lazily-imported `oracledb` THIN mode (pure Python, no client libs) | 4 (3 read + 1 privileged) | 2 | fake-driver tested; live validation = customer pilot |
| Redis | `connectors/redis.py` | lazily-imported `redis-py`; short-lived per-call clients | 3 read (no privileged) | 2 | fake-driver tested; live validation = customer pilot |
| Elasticsearch | `connectors/elasticsearch.py` | REST API (stdlib `urllib`, no client) | 3 read (no privileged) | 2 | fake-transport tested; live validation = customer pilot |
| MongoDB | `connectors/mongodb.py` | lazily-imported `pymongo`; read-only commands except privileged `killOp` | 4 (3 read + 1 privileged) | 2 | fake-driver tested; live validation = customer pilot |
| Kubernetes | `connectors/kubernetes.py` | official `kubernetes` client (lazy); kubeconfig or in-cluster auth, no inlined tokens | 4 (3 read + 1 privileged) | 2 | fake-driver tested; live validation = customer pilot |
| Docker | `connectors/docker.py` | Engine API over the Unix socket (stdlib `socket` + `http.client`) | 4 (3 read + 1 privileged) | 2 | fake-transport tested; live validation = customer pilot |

**Totals: 90 MCP tools (66 read, 24 privileged), 45 incident scenarios
(34 new, 14 new remediation plans), 21 technologies.**

Run the per-connector onboarding check any time (no live target needed):

```bash
.venv/bin/python -m connectors.harness [--connector NAME] [--json]
```
