# Tools reference

All 90 tools registered by the MCP server (`mcp_server/tools.py`,
`TOOL_NAMES`). Every call goes through `Gateway.call()`, which verifies the
token, checks the tool's required scope, audits the outcome, then dispatches.

Scopes: `diagnostics:read` (demo token `demo-read-token-0001`),
`admin:write` (demo token `demo-admin-token-0002`, which also holds
`diagnostics:read`).

Missing scope -> `PermissionError` (audited as `scope_denied`).
Invalid token -> `PermissionError` (audited as `auth_denied`).
Unknown tool name -> `KeyError` (audited as `tool_error`).

## get_queue_depth

- **Description:** Current depth and status of an IBM MQ queue.
- **Required scope:** `diagnostics:read`
- **Inputs:** `qmgr` (string, required), `queue` (string, required)
- **Output (dict):** `{qmgr, queue, depth, max_depth, ts}`
- **Errors:** `ValueError` for an unknown queue manager or unknown queue;
  `ValueError` if a required argument is missing.

## get_channel_status

- **Description:** Current status of an IBM MQ channel.
- **Required scope:** `diagnostics:read`
- **Inputs:** `qmgr` (string, required), `channel` (string, required)
- **Output (dict):** `{qmgr, channel, status, ts}` where `status` is one of
  `RUNNING`, `STOPPED`, `RETRYING`
- **Errors:** `ValueError` for an unknown queue manager or unknown channel;
  `ValueError` if a required argument is missing. (Note: the RCA engine
  swallows `ValueError` for this tool when probing its candidate channel
  list, so an unknown candidate is treated as "no answer", not a failure.)

## read_error_log

- **Description:** Recent entries from a queue manager error log.
- **Required scope:** `diagnostics:read`
- **Inputs:** `qmgr` (string, required), `limit` (integer, optional, default 50)
- **Output (list):** entries shaped `{ts, severity, code, message}`, most
  recent last
- **Errors:** `ValueError` for an unknown queue manager; `ValueError` if
  `qmgr` is missing.

## get_kafka_consumer_lag

- **Description:** Consumer lag for a Kafka topic/group.
- **Required scope:** `diagnostics:read`
- **Inputs:** `topic` (string, required), `group` (string, required)
- **Output (dict):** `{topic, group, lag, ts}`
- **Errors:** `ValueError` for an unknown topic/group pair; `ValueError` if a
  required argument is missing.

## get_host_metrics

- **Description:** CPU, memory, and disk metrics for a host.
- **Required scope:** `diagnostics:read`
- **Inputs:** `host` (string, required)
- **Output (dict):** `{host, cpu_pct, mem_pct, disk_pct, load1, ts}`
- **Errors:** `ValueError` for an unknown host; `ValueError` if `host` is
  missing.

## get_config

- **Description:** Configuration of an MQ object (queue, channel, ...).
- **Required scope:** `diagnostics:read`
- **Inputs:** `qmgr` (string, required), `object_type` (string, required,
  `"channel"` or `"queue"`), `name` (string, required)
- **Output (dict):** for a channel:
  `{qmgr, object_type, name, status, feeds_queue, ts}`; for a queue:
  `{qmgr, object_type, name, max_depth, baseline_depth, baseline_max_depth,
  ts}` (`baseline_max_depth` is the MAXDEPTH the queue was created with,
  used to detect config drift)
- **Errors:** `ValueError` for an unknown queue manager, unknown
  channel/queue name, or an unsupported `object_type`; `ValueError` if a
  required argument is missing.

## restart_channel (PRIVILEGED)

- **Description:** PRIVILEGED: restart an MQ channel (mutates state). Sets the
  channel to `RUNNING` and appends a log entry recording the previous status.
- **Required scope:** `admin:write` (the read token is denied with
  `PermissionError`)
- **Inputs:** `qmgr` (string, required), `channel` (string, required)
- **Output (dict):** `{qmgr, channel, previous_status, status, ts}`
- **Errors:** `ValueError` for an unknown queue manager or unknown channel;
  `ValueError` if a required argument is missing; `PermissionError` without
  the `admin:write` scope.

In the demo this tool is only reachable through `ApprovalGate.execute()` with
a live `APPROVED` grant. The RCA engine names it in remediation **data** but
never calls it.

## read_app_log

- **Description:** Recent entries from an application log.
- **Required scope:** `diagnostics:read`
- **Inputs:** `app` (string, required), `limit` (integer, optional, default 50)
- **Output (list):** entries shaped `{ts, level, logger, message}`, most
  recent last
- **Errors:** `ValueError` for an unknown app; `ValueError` if `app` is
  missing.

## get_listener_status

- **Description:** Current status of an MQ listener.
- **Required scope:** `diagnostics:read`
- **Inputs:** `name` (string, required)
- **Output (dict):** `{name, status, ts}` where `status` is `RUNNING` or
  `STOPPED`
- **Errors:** `ValueError` for an unknown listener; `ValueError` if `name`
  is missing.

## get_cert_status

- **Description:** TLS certificate status for an MQ channel.
- **Required scope:** `diagnostics:read`
- **Inputs:** `qmgr` (string, required), `channel` (string, required)
- **Output (dict):** `{qmgr, channel, valid, expires, ts}`
- **Errors:** `ValueError` for an unknown queue manager or a channel with no
  recorded certificate; `ValueError` if a required argument is missing.

## archive_logs (PRIVILEGED)

- **Description:** PRIVILEGED: archive logs on a host, freeing disk (mutates
  state). Disk usage drops to 45% and a log entry is recorded.
- **Required scope:** `admin:write`
- **Inputs:** `host` (string, required)
- **Output (dict):** `{host, disk_pct, ts}`
- **Errors:** `ValueError` for an unknown host; `PermissionError` without
  the `admin:write` scope.

## restart_app (PRIVILEGED)

- **Description:** PRIVILEGED: restart an application (mutates state).
  Memory on the app host drops to 60%, any OOM flag clears, and a restart
  entry is logged.
- **Required scope:** `admin:write`
- **Inputs:** `app` (string, required)
- **Output (dict):** `{app, status, ts}` (`status` is `RUNNING`)
- **Errors:** `ValueError` for an unknown app; `PermissionError` without
  the `admin:write` scope.

## restart_consumer (PRIVILEGED)

- **Description:** PRIVILEGED: restart a Kafka consumer group (mutates
  state). Lag drops back to the group's baseline.
- **Required scope:** `admin:write`
- **Inputs:** `topic` (string, required), `group` (string, required)
- **Output (dict):** `{topic, group, lag, ts}`
- **Errors:** `ValueError` for an unknown topic/group pair; `PermissionError`
  without the `admin:write` scope.

## renew_certificate (PRIVILEGED)

- **Description:** PRIVILEGED: renew the TLS certificate for an MQ channel
  (mutates state). Sets `valid` to true with a new expiry and logs the
  renewal.
- **Required scope:** `admin:write`
- **Inputs:** `qmgr` (string, required), `channel` (string, required)
- **Output (dict):** `{qmgr, channel, valid, expires, ts}`
- **Errors:** `ValueError` for an unknown queue manager or a channel with no
  recorded certificate; `PermissionError` without the `admin:write` scope.

## update_queue_config (PRIVILEGED)

- **Description:** PRIVILEGED: change an MQ queue's MAXDEPTH (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `qmgr` (string, required), `queue` (string, required),
  `max_depth` (integer, required)
- **Output (dict):** `{qmgr, queue, old_max_depth, max_depth, ts}`
- **Errors:** `ValueError` for an unknown queue manager or queue;
  `PermissionError` without the `admin:write` scope.

## start_listener (PRIVILEGED)

- **Description:** PRIVILEGED: start an MQ listener (mutates state). Sets the
  listener to `RUNNING` and appends a log entry recording the previous
  status.
- **Required scope:** `admin:write`
- **Inputs:** `name` (string, required)
- **Output (dict):** `{name, previous_status, status, ts}`
- **Errors:** `ValueError` for an unknown listener; `PermissionError`
  without the `admin:write` scope.

## quarantine_message (PRIVILEGED)

- **Description:** PRIVILEGED: quarantine a poison message to SYSTEM.DLQ
  (mutates state). If a poison message is recorded for the queue, the queue
  drains to its depth baseline, SYSTEM.DLQ gains one message, the poison
  flag clears, and the quarantine is logged.
- **Required scope:** `admin:write`
- **Inputs:** `qmgr` (string, required), `queue` (string, required)
- **Output (dict):** `{qmgr, queue, quarantined, ts}`
- **Errors:** `ValueError` for an unknown queue manager or queue;
  `PermissionError` without the `admin:write` scope.

The privileged tools above are only reachable through `ApprovalGate`
(request per step or per plan) with a live `APPROVED` grant. The RCA
engine names them in `RemediationPlan` **data** but never calls them.

## tail_log

- **Description:** Read-only tail of a host log file. Only estates that
  expose an allow-listed log reader support it: the real
  `LinuxHostConnector` (`connectors/linux_host.py`). The simulated estate
  has no `tail_log`, so sim gateways simply never register it (via the
  `include` parameter on `Gateway` / `build_tool_defs`).
- **Required scope:** `diagnostics:read`
- **Inputs:** `path` (string, required), `limit` (integer, optional,
  default 50, capped at 200)
- **Output (list):** `[{"line_no": n, "text": line}, ...]` — line numbers
  are absolute within the file; content is returned verbatim and never
  interpreted (hostile log text is inert data).
- **Errors:** `PermissionError` if the path is not on the connector's
  allow-list (audited as `tool_error` — that is the sandbox working);
  `ConnectorError` if the allow-listed file cannot be read;
  `ValueError` if a required argument is missing.

## get_kafka_consumer_group_detail

- **Description:** Per-partition consumer lag detail for a Kafka
  topic/group: end offsets vs committed offsets per partition.
- **Required scope:** `diagnostics:read`
- **Inputs:** `topic` (string, required), `group` (string, required)
- **Output (dict):** `{topic, group, partitions: [{partition, end_offset,
  committed, lag}, ...], total_lag, ts}`
- **Errors:** `ValueError` for an unknown topic/group pair or an unknown
  topic; `ValueError` if a required argument is missing.

## get_kafka_topic_detail

- **Description:** Partition/leader/replica/ISR detail for a Kafka topic.
- **Required scope:** `diagnostics:read`
- **Inputs:** `topic` (string, required)
- **Output (dict):** `{topic, partitions: [{partition, leader, replicas,
  isr, end_offset}, ...], ts}` — `isr` smaller than `replicas` flags an
  under-replicated partition.
- **Errors:** `ValueError` for an unknown topic; `ValueError` if a
  required argument is missing.

## get_kafka_broker_config

- **Description:** Configuration of one Kafka broker (compare across
  brokers to spot drift).
- **Required scope:** `diagnostics:read`
- **Inputs:** `broker_id` (integer, required)
- **Output (dict):** `{broker_id, configs: {...}, ts}`
- **Errors:** `ValueError` for an unknown broker id; `ValueError` if a
  required argument is missing.

## get_kafka_broker_health

- **Description:** Reachability and health of the Kafka cluster/brokers.
- **Required scope:** `diagnostics:read`
- **Inputs:** none
- **Output (dict):** `{bootstrap_servers, reachable, latency_ms, brokers,
  controller, degraded, degradation, ts}`
- **Errors:** none (read-only status read).

## get_tomcat_heap

- **Description:** JVM heap and non-heap memory usage of a Tomcat server.
- **Required scope:** `diagnostics:read`
- **Inputs:** none
- **Output (dict):** `{host, port, heap_init_bytes, heap_used_bytes,
  heap_committed_bytes, heap_max_bytes, heap_used_pct, non_heap_*_bytes,
  ts}` — `heap_used_pct` is None when max <= 0.
- **Errors:** none (read-only status read).

## get_tomcat_threadpool

- **Description:** Tomcat connector thread-pool usage (busy/total/max
  threads), optionally for one pool by exact name.
- **Required scope:** `diagnostics:read`
- **Inputs:** `pool` (string, optional) — exact pool name filter
- **Output (dict):** `{host, port, pools: [{name, current_threads_busy,
  current_thread_count, max_threads, busy_pct}, ...], ts}`
- **Errors:** `ValueError` for an unknown pool name.

## get_tomcat_apps

- **Description:** Deployed Tomcat web applications and their state.
- **Required scope:** `diagnostics:read`
- **Inputs:** none
- **Output (dict):** `{host, port, apps: [{path, state, sessions}, ...],
  ts}` — `state` is one of running/stopped/failed/...;
  `sessions` is None when not readable.
- **Errors:** none (read-only status read).

## read_tomcat_log

- **Description:** Tail a Tomcat log (`catalina` = catalina.out,
  `access` = newest localhost access log). Real-connector note: co-located
  best-effort — tails files on the connector's own disk.
- **Required scope:** `diagnostics:read`
- **Inputs:** `log` (string, optional, default "catalina"), `limit`
  (integer, optional, default 50)
- **Output (list):** `[{ts, severity, message}, ...]` — returned verbatim,
  never interpreted (hostile log text is inert data).
- **Errors:** `ValueError` for an unknown log name.

## restart_tomcat_app (PRIVILEGED)

- **Description:** PRIVILEGED: restart a Tomcat web application (mutates
  state). In the simulated estate a restart also frees busy threads on the
  `http-nio-8080` pool.
- **Required scope:** `admin:write`
- **Inputs:** `app_path` (string, required)
- **Output (dict):** `{app_path, previous_state, state, ts}`
- **Errors:** `ValueError` for an unknown app path; `PermissionError`
  without the `admin:write` scope.

## New in v0.6.0 (63 tools)

The 17 new connectors add 63 MCP tools (48 read, 15 privileged).

### WebSphere

#### get_websphere_heap

- **Description:** JVM heap and non-heap memory usage of a WebSphere server.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_websphere_threadpool

- **Description:** WebSphere thread-pool usage (busy/total/max threads), optionally for one pool.
- **Required scope:** `diagnostics:read`
- **Inputs:** `pool` (string, optional)

#### get_websphere_apps

- **Description:** Deployed WebSphere applications and their state.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### read_websphere_log

- **Description:** Tail a WebSphere log (SystemOut by default).
- **Required scope:** `diagnostics:read`
- **Inputs:** `log` (string, optional), `limit` (integer, optional)

#### restart_websphere_app (PRIVILEGED)

- **Description:** PRIVILEGED: restart a WebSphere application (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `app_name` (string, required)

### WebLogic

#### get_weblogic_heap

- **Description:** JVM heap and non-heap memory usage of a WebLogic server.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_weblogic_threadpool

- **Description:** WebLogic thread-pool usage (busy/total/max threads).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_weblogic_apps

- **Description:** Deployed WebLogic applications and their state.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### read_weblogic_log

- **Description:** Tail a WebLogic server log.
- **Required scope:** `diagnostics:read`
- **Inputs:** `log` (string, optional), `limit` (integer, optional)

#### restart_weblogic_app (PRIVILEGED)

- **Description:** PRIVILEGED: restart a WebLogic application (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `app_name` (string, required)

### JBoss EAP / WildFly

#### get_jboss_heap

- **Description:** JVM heap and non-heap memory usage of a JBoss/WildFly server.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_jboss_threadpool

- **Description:** JBoss/WildFly thread-pool usage (busy/total/max threads), optionally for one pool.
- **Required scope:** `diagnostics:read`
- **Inputs:** `pool` (string, optional)

#### get_jboss_deployments

- **Description:** JBoss/WildFly deployments and their state.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### read_jboss_log

- **Description:** Tail a JBoss/WildFly server log.
- **Required scope:** `diagnostics:read`
- **Inputs:** `log` (string, optional), `limit` (integer, optional)

#### restart_jboss_deployment (PRIVILEGED)

- **Description:** PRIVILEGED: restart a JBoss/WildFly deployment (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `deployment` (string, required)

### RabbitMQ

#### get_rabbitmq_queues

- **Description:** RabbitMQ queues with depth and consumer counts for a vhost.
- **Required scope:** `diagnostics:read`
- **Inputs:** `vhost` (string, optional)

#### get_rabbitmq_nodes

- **Description:** RabbitMQ cluster nodes and their health.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_rabbitmq_connections

- **Description:** RabbitMQ client connections and their state.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### purge_rabbitmq_queue (PRIVILEGED)

- **Description:** PRIVILEGED: purge all messages in a RabbitMQ queue (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `vhost` (string, required), `queue` (string, required)

### ActiveMQ Artemis

#### get_artemis_queues

- **Description:** ActiveMQ Artemis queues with depth and consumer counts.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_artemis_broker

- **Description:** ActiveMQ Artemis broker health and status.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### purge_artemis_queue (PRIVILEGED)

- **Description:** PRIVILEGED: purge all messages in an ActiveMQ Artemis queue (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `queue` (string, required)

### TIBCO EMS

#### get_ems_queues

- **Description:** TIBCO EMS queues with depth and consumer counts.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_ems_server

- **Description:** TIBCO EMS server health and status.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### purge_ems_queue (PRIVILEGED)

- **Description:** PRIVILEGED: purge all messages in a TIBCO EMS queue (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `queue` (string, required)

### Nginx

#### get_nginx_status

- **Description:** NGINX stub status (active connections, accepted requests).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### read_nginx_log

- **Description:** Tail an NGINX log (access by default).
- **Required scope:** `diagnostics:read`
- **Inputs:** `log` (string, optional), `limit` (integer, optional)

#### reload_nginx (PRIVILEGED)

- **Description:** PRIVILEGED: reload the NGINX configuration (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** (none)

### Apache HTTPD

#### get_apache_status

- **Description:** Apache HTTPD server status (workers, requests).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### read_apache_log

- **Description:** Tail an Apache HTTPD log (access by default).
- **Required scope:** `diagnostics:read`
- **Inputs:** `log` (string, optional), `limit` (integer, optional)

#### reload_apache (PRIVILEGED)

- **Description:** PRIVILEGED: reload the Apache HTTPD configuration (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** (none)

### HAProxy

#### get_haproxy_stats

- **Description:** HAProxy frontend/backend/server statistics.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### set_haproxy_server_state (PRIVILEGED)

- **Description:** PRIVILEGED: set an HAProxy backend server's state (e.g. ready, maint, drain) (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `backend` (string, required), `server` (string, required), `state` (string, required)

### PostgreSQL

#### get_postgres_health

- **Description:** PostgreSQL health (up, connections, slow queries).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_postgres_blocking

- **Description:** PostgreSQL sessions currently blocked on locks.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_postgres_replication

- **Description:** PostgreSQL replication status (primary/replica lag).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### terminate_postgres_backend (PRIVILEGED)

- **Description:** PRIVILEGED: terminate a PostgreSQL backend by PID (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `pid` (integer, required)

### MySQL

#### get_mysql_health

- **Description:** MySQL health (up, connections, slow queries).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_mysql_processlist

- **Description:** MySQL processlist (running queries and sessions).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_mysql_replication

- **Description:** MySQL replication status (replica lag, IO/SQL thread state).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### kill_mysql_query (PRIVILEGED)

- **Description:** PRIVILEGED: kill a MySQL query/process by ID (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `process_id` (integer, required)

### Oracle DB

#### get_oracle_health

- **Description:** Oracle database health (up, sessions, wait events).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_oracle_tablespaces

- **Description:** Oracle tablespace usage (size, used, free).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_oracle_blocking

- **Description:** Oracle sessions currently blocked on locks.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### kill_oracle_session (PRIVILEGED)

- **Description:** PRIVILEGED: kill an Oracle session by SID and serial# (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `sid` (integer, required), `serial` (integer, required)

### Redis

#### get_redis_info

- **Description:** Redis INFO (memory, clients, keyspace).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_redis_replication

- **Description:** Redis replication status (role, replicas, lag).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_redis_slowlog

- **Description:** Recent Redis SLOWLOG entries.
- **Required scope:** `diagnostics:read`
- **Inputs:** `limit` (integer, optional)

### Elasticsearch

#### get_elasticsearch_cluster_health

- **Description:** Elasticsearch cluster health (status, shards).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_elasticsearch_nodes

- **Description:** Elasticsearch nodes and their stats.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_elasticsearch_indices

- **Description:** Elasticsearch indices with health and size.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

### MongoDB

#### get_mongo_health

- **Description:** MongoDB server health (up, connections).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_mongo_replset

- **Description:** MongoDB replica set status (primary, members).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_mongo_current_ops

- **Description:** MongoDB in-flight operations (currentOp).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### kill_mongo_op (PRIVILEGED)

- **Description:** PRIVILEGED: kill a MongoDB operation by opid (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `opid` (string, required)

### Kubernetes

#### get_k8s_pod_status

- **Description:** Kubernetes pod statuses in a namespace.
- **Required scope:** `diagnostics:read`
- **Inputs:** `namespace` (string, required)

#### get_k8s_deployments

- **Description:** Kubernetes deployments and rollout status in a namespace.
- **Required scope:** `diagnostics:read`
- **Inputs:** `namespace` (string, required)

#### get_k8s_events

- **Description:** Recent Kubernetes events in a namespace.
- **Required scope:** `diagnostics:read`
- **Inputs:** `namespace` (string, required), `limit` (integer, optional)

#### restart_k8s_deployment (PRIVILEGED)

- **Description:** PRIVILEGED: restart a Kubernetes deployment (rollout restart) (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `namespace` (string, required), `deployment` (string, required)

### Docker

#### get_docker_containers

- **Description:** Docker containers and their state.
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### get_docker_stats

- **Description:** Docker container resource stats (CPU, memory).
- **Required scope:** `diagnostics:read`
- **Inputs:** (none)

#### read_docker_logs

- **Description:** Tail a Docker container's logs.
- **Required scope:** `diagnostics:read`
- **Inputs:** `container` (string, required), `limit` (integer, optional)

#### restart_docker_container (PRIVILEGED)

- **Description:** PRIVILEGED: restart a Docker container (mutates state).
- **Required scope:** `admin:write`
- **Inputs:** `container` (string, required)

