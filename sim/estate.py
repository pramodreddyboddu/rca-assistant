"""Simulated middleware estate for the RCA assistant demo.

A deterministic, in-memory model of a 21-technology middleware estate:
IBM MQ, Kafka, Linux hosts, Tomcat, WebSphere, WebLogic, JBoss/WildFly,
RabbitMQ, ActiveMQ Artemis, TIBCO EMS, Nginx, Apache HTTPD, HAProxy,
PostgreSQL, MySQL, Oracle Database, Redis, Elasticsearch, MongoDB,
Kubernetes, and Docker. Everything is stdlib-only; timestamps come from
an internal simulation clock so runs are reproducible from the seed.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta


_EPOCH = datetime(2026, 9, 20, 6, 0, 0)


class Estate:
    """A deterministic simulated estate covering 21 technologies.

    IBM MQ, Kafka, Linux hosts, Tomcat, WebSphere, WebLogic, JBoss,
    RabbitMQ, ActiveMQ Artemis, TIBCO EMS, Nginx, Apache HTTPD, HAProxy,
    PostgreSQL, MySQL, Oracle Database, Redis, Elasticsearch, MongoDB,
    Kubernetes, and Docker.

    State is derived from ``seed`` so identical seeds produce identical
    estates. All timestamps are ISO strings from an internal clock that
    advances with :meth:`tick`.
    """

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self._rng = random.Random(seed)
        self._clock = _EPOCH

        self._queues: dict[str, dict[str, dict]] = {
            "QMGR1": {
                "PAYMENTS.IN": {"depth": 1200, "max_depth": 50000, "baseline": 1200,
                                "baseline_max_depth": 50000},
                "PAYMENTS.OUT": {"depth": 45, "max_depth": 50000, "baseline": 45,
                                 "baseline_max_depth": 50000},
                "ORDERS.IN": {"depth": 300, "max_depth": 20000, "baseline": 300,
                              "baseline_max_depth": 20000},
                "SYSTEM.DLQ": {"depth": 2, "max_depth": 5000, "baseline": 2,
                               "baseline_max_depth": 5000},
            }
        }
        self._channels: dict[str, dict[str, str]] = {
            "QMGR1": {
                "PAYMENTS.RCVR": "RUNNING",
                "PAYMENTS.SDR": "RUNNING",
                "ORDERS.RCVR": "RUNNING",
            }
        }
        # Inbound channels map onto the queue they feed.
        self._channel_feeds: dict[tuple[str, str], str] = {}
        self._channel_feeds[("QMGR1", "PAYMENTS.RCVR")] = "PAYMENTS.IN"
        self._channel_feeds[("QMGR1", "ORDERS.RCVR")] = "ORDERS.IN"

        self._mq_error_log: dict[str, list[dict]] = {"QMGR1": []}
        self._seed_mq_log("QMGR1")

        self._hosts: dict[str, dict] = {
            "app01": {"cpu_pct": 34, "mem_pct": 61, "disk_pct": 58, "load1": 1.4},
            "mq01": {"cpu_pct": 22, "mem_pct": 48, "disk_pct": 41, "load1": 0.8},
        }

        self._kafka_lag: dict[tuple[str, str], dict] = {
            ("payments-events", "payments-consumers"): {"lag": 120, "baseline": 120},
            ("orders-events", "orders-consumers"): {"lag": 35, "baseline": 35},
        }

        # Kafka topic topology: partitions, replication, per-broker config.
        self._kafka_topics: dict[str, dict] = {
            "payments-events": {"partitions": 6, "replication_factor": 3},
            "orders-events": {"partitions": 3, "replication_factor": 3},
        }
        self._kafka_brokers: dict[int, dict] = {
            1: {"log.retention.hours": "168", "num.network.threads": "3",
                "log.segment.bytes": "1073741824"},
            2: {"log.retention.hours": "168", "num.network.threads": "3",
                "log.segment.bytes": "1073741824"},
            3: {"log.retention.hours": "72", "num.network.threads": "3",
                "log.segment.bytes": "1073741824"},
        }
        # ISR overrides per (topic, partition): empty in a healthy estate;
        # degrade_partition_isr() installs one to simulate under-replication.
        self._kafka_isr: dict[tuple[str, int], list[int]] = {}

        self._kafka_controller: int = 1

        # Tomcat state on app01.
        self._tomcat: dict = {
            "host": "app01",
            "port": 8080,
            "heap": {
                "init_bytes": 268435456,
                "used_bytes": 402653184,
                "committed_bytes": 536870912,
                "max_bytes": 1073741824,
                "non_heap_init_bytes": 25559168,
                "non_heap_used_bytes": 89128960,
                "non_heap_committed_bytes": 100663296,
                "non_heap_max_bytes": -1,
            },
            "pools": {
                "http-nio-8080": {"busy": 12, "count": 48, "max": 200},
                "ajp-nio-8009": {"busy": 0, "count": 10, "max": 200},
            },
            "apps": [
                {"path": "/", "state": "running", "sessions": 14},
                {"path": "/payments", "state": "running", "sessions": 231},
                {"path": "/orders", "state": "running", "sessions": 87},
            ],
        }

        # WebSphere Application Server on was01.
        self._websphere: dict = {
            "host": "was01",
            "port": 9080,
            "heap": {
                "init_bytes": 268435456,
                "used_bytes": 612368384,
                "committed_bytes": 805306368,
                "max_bytes": 1610612736,
            },
            "pools": {
                "Default": {"busy": 4, "count": 20, "max": 50},
                "WebContainer": {"busy": 23, "count": 64, "max": 100},
            },
            "apps": [
                {"name": "orders-ear", "state": "running", "sessions": None},
                {"name": "payments-ear", "state": "running",
                 "sessions": None},
            ],
        }

        # WebLogic Server on wls01. The REST API exposes only current /
        # free / max heap, so init/committed stay None; the self-tuning
        # pool exposes no max.
        self._weblogic: dict = {
            "host": "wls01",
            "port": 7001,
            "heap": {
                "used_bytes": 1228931072,
                "max_bytes": 1610612736,
            },
            "pools": [
                {"name": "weblogic.kernel.Default (self-tuning)",
                 "busy": 31, "count": 64},
            ],
            "apps": [
                {"name": "billing-app", "state": "running",
                 "sessions": 412},
                {"name": "ledger-app", "state": "running", "sessions": 96},
            ],
        }

        # JBoss EAP / WildFly on jboss01.
        self._jboss: dict = {
            "host": "jboss01",
            "port": 9990,
            "heap": {
                "init_bytes": 268435456,
                "used_bytes": 447283200,
                "committed_bytes": 536870912,
                "max_bytes": 1073741824,
            },
            "pools": {
                "default": {"busy": 18, "count": 40, "max": 200},
                "io": {"busy": 6, "count": 16, "max": 64},
            },
            "deployments": [
                {"name": "orders.war", "enabled": True, "status": "OK"},
                {"name": "payments.war", "enabled": True, "status": "OK"},
            ],
        }

        # RabbitMQ on rmq01. Queues keyed by vhost.
        self._rabbitmq: dict = {
            "host": "rmq01",
            "port": 15672,
            "queues": {
                "/": {
                    "orders.q": {"messages": 12, "messages_ready": 12,
                                 "messages_unacknowledged": 0,
                                 "consumers": 2, "state": "running"},
                    "payments.q": {"messages": 842, "messages_ready": 800,
                                   "messages_unacknowledged": 42,
                                   "consumers": 3, "state": "running"},
                },
            },
            "nodes": [
                {"name": "rabbit@rmq01", "running": True,
                 "mem_used_bytes": 2147483648,
                 "mem_limit_bytes": 8589934592,
                 "disk_free_bytes": 53687091200,
                 "fd_used": 128, "fd_total": 1024},
            ],
            "connections": [
                {"name": "10.0.0.21:52341 -> 10.0.0.50:5672",
                 "user": "payments", "vhost": "/", "state": "running",
                 "channels": 4},
            ],
        }

        # ActiveMQ Artemis on artemis01.
        self._artemis: dict = {
            "host": "artemis01",
            "port": 8161,
            "version": "2.31.2",
            "started": True,
            "queues": {
                "DLQ": {"message_count": 3, "delivering_count": 0,
                        "consumer_count": 0},
                "payments.orders": {"message_count": 214,
                                    "delivering_count": 12,
                                    "consumer_count": 2},
            },
        }

        # TIBCO EMS on ems01 (tcp://ems01:7222).
        self._ems: dict = {
            "host": "ems01",
            "server": "tcp://ems01:7222",
            "state": "active",
            "connections": 37,
            "version": "10.2.1",
            "queues": {
                "queue.orders.in": {"pending_messages": 7, "consumers": 1,
                                    "state": "active"},
                "queue.payments.in": {"pending_messages": 96,
                                      "consumers": 2, "state": "active"},
            },
        }

        # Nginx on web01.
        self._nginx: dict = {
            "host": "web01",
            "port": 80,
            "active_connections": 137,
            "accepts": 1048576,
            "handled": 1048576,
            "requests": 2097152,
            "reading": 2,
            "writing": 9,
            "waiting": 126,
        }

        # Apache HTTPD on web02.
        self._apache: dict = {
            "host": "web02",
            "port": 80,
            "total_accesses": 524288,
            "total_kbytes": 8388608,
            "uptime_secs": 86400,
            "busy_workers": 24,
            "idle_workers": 76,
        }

        # HAProxy on lb01. Each server tracks an admin_state in
        # {"ready", "drain", "maint"}; stats map it to UP/DRAIN/MAINT.
        self._haproxy: dict = {
            "host": "lb01",
            "port": 8404,
            "frontends": [
                {"name": "fe_payments", "status": "OPEN",
                 "current_sessions": 214},
            ],
            "backends": [
                {"name": "be_payments", "status": "UP", "servers": [
                    {"name": "pay01", "admin_state": "ready",
                     "current_sessions": 108, "check_status": "L7OK"},
                    {"name": "pay02", "admin_state": "ready",
                     "current_sessions": 106, "check_status": "L7OK"},
                ]},
            ],
        }

        # PostgreSQL on pg01.
        self._postgres: dict = {
            "host": "pg01",
            "port": 5432,
            "database": "payments",
            "version": "PostgreSQL 15.4 on x86_64-pc-linux-gnu",
            "up": True,
            "connections_used": 87,
            "connections_max": 200,
            "blockers": [
                {"pid": 4821, "usename": "payments_app",
                 "wait_seconds": 184,
                 "query_snippet": "UPDATE ledger SET balance = "
                                  "balance - 100 WHERE account_id = 42",
                 "locktype": "tuple"},
            ],
            "role": "primary",
            "replay_lag_bytes": None,
            "replay_lag_seconds": None,
        }

        # MySQL on my01.
        self._mysql: dict = {
            "host": "my01",
            "port": 3306,
            "version": "8.0.34",
            "uptime_secs": 172800,
            "threads_connected": 42,
            "max_connections": 151,
            "slow_queries": 17,
            "processes": [
                {"id": 99182, "user": "etl", "db": "warehouse",
                 "command": "Query", "time_secs": 372,
                 "state": "Sending data",
                 "query_snippet": "SELECT * FROM fact_orders WHERE "
                                  "created_at > '2026-09-01'"},
                {"id": 99201, "user": "app", "db": "shop",
                 "command": "Sleep", "time_secs": 12, "state": "",
                 "query_snippet": None},
            ],
            "role": "replica",
            "io_running": True,
            "sql_running": True,
            "seconds_behind_source": 0,
        }

        # Oracle Database on ora01.
        self._oracle: dict = {
            "host": "ora01",
            "port": 1521,
            "service": "ORCLPDB1",
            "version": "Oracle Database 19c Enterprise Edition "
                       "Release 19.0.0.0.0",
            "open_mode": "READ WRITE",
            "sessions_used": 214,
            "sessions_max": 500,
            "tablespaces": [
                {"name": "SYSTEM", "used_pct": 41.2, "used_mb": 842,
                 "max_mb": 2048},
                {"name": "USERS", "used_pct": 68.4, "used_mb": 7014,
                 "max_mb": 10240},
            ],
            "blockers": [
                {"sid": 142, "serial": 9017, "username": "APP_USER",
                 "wait_seconds": 96,
                 "sql_snippet": "UPDATE accounts SET balance = "
                                "balance + 50 WHERE id = 7"},
            ],
        }

        # Redis on redis01.
        self._redis: dict = {
            "host": "redis01",
            "port": 6379,
            "version": "7.2.3",
            "uptime_secs": 259200,
            "used_memory_bytes": 1073741824,
            "maxmemory_bytes": 4294967296,
            "connected_clients": 58,
            "blocked_clients": 2,
            "keyspace_hits": 987654,
            "keyspace_misses": 12345,
            "role": "master",
            "connected_replicas": 1,
            "master_link_status": None,
            "slowlog": [
                {"id": 1284, "ts": self._now(), "duration_us": 41203,
                 "command_snippet": "KEYS user:session:*"},
                {"id": 1283, "ts": self._now(), "duration_us": 18750,
                 "command_snippet": "HGETALL order:99182"},
            ],
        }

        # Elasticsearch cluster es-prod.
        self._es: dict = {
            "cluster_name": "es-prod",
            "status": "green",
            "number_of_nodes": 3,
            "active_shards": 42,
            "unassigned_shards": 0,
            "relocating_shards": 0,
            "nodes": [
                {"name": "es-node-1", "heap_used_pct": 61, "cpu_pct": 24,
                 "disk_used_pct": 38},
                {"name": "es-node-2", "heap_used_pct": 58, "cpu_pct": 21,
                 "disk_used_pct": 36},
                {"name": "es-node-3", "heap_used_pct": 63, "cpu_pct": 27,
                 "disk_used_pct": 41},
            ],
            "indices": [
                {"name": "orders-2026.09", "health": "green",
                 "docs_count": 524288, "store_size_bytes": 1073741824},
                {"name": "payments-2026.09", "health": "green",
                 "docs_count": 1048576, "store_size_bytes": 2147483648},
            ],
        }

        # MongoDB on mongo01.
        self._mongo: dict = {
            "host": "mongo01",
            "port": 27017,
            "version": "7.0.8",
            "uptime_secs": 345600,
            "connections_current": 73,
            "connections_available": 51127,
            "replset": {
                "set_name": "rs0",
                "my_state": 1,
                "members": [
                    {"name": "mongo01:27017", "state": 1, "health": 1,
                     "lag_seconds": 0.0},
                    {"name": "mongo02:27017", "state": 2, "health": 1,
                     "lag_seconds": 1.2},
                    {"name": "mongo03:27017", "state": 2, "health": 1,
                     "lag_seconds": 0.8},
                ],
            },
            "ops": [
                {"opid": 881234, "secs_running": 214, "op": "query",
                 "ns": "shop.orders",
                 "query_snippet": '{ find: "orders", filter: { status: '
                                  '"PENDING" } }'},
            ],
        }

        # Kubernetes: one namespace, "payments".
        self._k8s: dict = {
            "namespaces": {
                "payments": {
                    "pods": [
                        {"name": "payments-api-7d9f6c4b8-x2v4n",
                         "phase": "Running", "restarts": 0,
                         "ready": "1/1", "node": "node-1"},
                        {"name": "payments-worker-5b8d2f1a9-q7m3p",
                         "phase": "Running", "restarts": 2,
                         "ready": "1/1", "node": "node-2"},
                    ],
                    "deployments": [
                        {"name": "payments-api", "replicas_desired": 3,
                         "replicas_ready": 3, "replicas_unavailable": 0},
                        {"name": "payments-worker", "replicas_desired": 2,
                         "replicas_ready": 2, "replicas_unavailable": 0},
                    ],
                    "events": [
                        {"ts": self._now(), "type": "Warning",
                         "reason": "BackOff",
                         "object": "Pod/payments-worker-5b8d2f1a9-q7m3p",
                         "message": "Back-off restarting failed "
                                    "container"},
                        {"ts": self._now(), "type": "Normal",
                         "reason": "Scheduled",
                         "object": "Pod/payments-api-7d9f6c4b8-x2v4n",
                         "message": "Successfully assigned payments/"
                                    "payments-api-7d9f6c4b8-x2v4n to "
                                    "node-1"},
                    ],
                },
            },
        }

        # Docker host docker01.
        self._docker: dict = {
            "host": "docker01",
            "containers": [
                {"id": "a1b2c3d4e5f6", "name": "payments-api",
                 "image": "acme/payments-api:1.4.2", "state": "running",
                 "status": "Up 3 days"},
                {"id": "b2c3d4e5f6a7", "name": "redis-cache",
                 "image": "redis:7.2", "state": "running",
                 "status": "Up 3 days"},
            ],
            "stats": {
                "payments-api": {"cpu_pct": 12.4,
                                 "mem_used_bytes": 268435456,
                                 "mem_limit_bytes": 1073741824},
                "redis-cache": {"cpu_pct": 3.1,
                                "mem_used_bytes": 134217728,
                                "mem_limit_bytes": 536870912},
            },
            "logs": {
                "payments-api": [
                    {"ts": self._now(),
                     "message": "INFO  Started payments-api on :8443"},
                    {"ts": self._now(),
                     "message": "INFO  Health check OK: queue depth "
                                "nominal"},
                ],
                "redis-cache": [
                    {"ts": self._now(),
                     "message": "1:C 20 Sep 2026 06:00:01.000 * Ready to "
                                "accept connections"},
                ],
            },
        }

        # Listeners (one per queue manager host in this demo).
        self._listeners: dict[str, str] = {"TCP.LISTENER": "RUNNING"}

        # TLS certificates per (qmgr, channel) pair.
        self._certs: dict[tuple[str, str], dict] = {
            ("QMGR1", "PAYMENTS.RCVR"): {"valid": True, "expires": "2027-06-01"},
        }

        # Poison-message state: {"qmgr", "queue", "msgid"} or None.
        self._poison: dict | None = None

        # JVM out-of-memory flag, set by the tomcat_oom fault injector.
        self._oom: dict | None = None

        self._app_logs: dict[str, list[dict]] = {"payments-api": []}
        self._seed_app_log("payments-api")

    # -- helpers -----------------------------------------------------------
    def _now(self) -> str:
        return self._clock.isoformat(timespec="seconds")

    def _require_qmgr(self, qmgr: str) -> None:
        if qmgr not in self._queues:
            raise ValueError(f"unknown queue manager: {qmgr!r}")

    def _seed_mq_log(self, qmgr: str) -> None:
        ts = self._now()
        self._mq_error_log[qmgr] = [
            {
                "ts": ts,
                "severity": "INFO",
                "code": "AMQ5967",
                "message": f"Queue manager '{qmgr}' started on host mq01.",
            },
            {
                "ts": ts,
                "severity": "INFO",
                "code": "AMQ9202",
                "message": "Channel 'PAYMENTS.RCVR' started inbound communication.",
            },
        ]

    def _seed_app_log(self, app: str) -> None:
        ts = self._now()
        self._app_logs[app] = [
            {
                "ts": ts,
                "level": "INFO",
                "logger": "payments-api",
                "message": "Payments API started on port 8443 (health: OK).",
            },
            {
                "ts": ts,
                "level": "INFO",
                "logger": "payments-consumer",
                "message": "Subscribed to topic payments-events, group payments-consumers.",
            },
        ]

    # -- IBM MQ-like surface -----------------------------------------------
    def get_queue_depth(self, qmgr: str, queue: str) -> dict:
        """Return current depth of a queue; ValueError if unknown."""
        self._require_qmgr(qmgr)
        if queue not in self._queues[qmgr]:
            raise ValueError(f"unknown queue: {queue!r} on {qmgr!r}")
        q = self._queues[qmgr][queue]
        return {
            "qmgr": qmgr,
            "queue": queue,
            "depth": q["depth"],
            "max_depth": q["max_depth"],
            "ts": self._now(),
        }

    def get_channel_status(self, qmgr: str, channel: str) -> dict:
        """Return channel status; one of RUNNING/STOPPED/RETRYING."""
        self._require_qmgr(qmgr)
        if channel not in self._channels[qmgr]:
            raise ValueError(f"unknown channel: {channel!r} on {qmgr!r}")
        return {
            "qmgr": qmgr,
            "channel": channel,
            "status": self._channels[qmgr][channel],
            "ts": self._now(),
        }

    def list_queues(self, qmgr: str) -> list[str]:
        self._require_qmgr(qmgr)
        return sorted(self._queues[qmgr])

    def list_channels(self, qmgr: str) -> list[str]:
        self._require_qmgr(qmgr)
        return sorted(self._channels[qmgr])

    def read_error_log(self, qmgr: str, limit: int = 50) -> list[dict]:
        """Most recent error-log entries, each {ts, severity, code, message}."""
        self._require_qmgr(qmgr)
        return list(self._mq_error_log[qmgr][-limit:])

    def get_config(self, qmgr: str, object_type: str, name: str) -> dict:
        """Return a lightweight config view for a queue or channel."""
        self._require_qmgr(qmgr)
        if object_type == "channel":
            if name not in self._channels[qmgr]:
                raise ValueError(f"unknown channel: {name!r} on {qmgr!r}")
            return {
                "qmgr": qmgr,
                "object_type": "channel",
                "name": name,
                "status": self._channels[qmgr][name],
                "feeds_queue": self._channel_feeds.get((qmgr, name)),
                "ts": self._now(),
            }
        if object_type == "queue":
            if name not in self._queues[qmgr]:
                raise ValueError(f"unknown queue: {name!r} on {qmgr!r}")
            q = self._queues[qmgr][name]
            return {
                "qmgr": qmgr,
                "object_type": "queue",
                "name": name,
                "max_depth": q["max_depth"],
                "baseline_depth": q["baseline"],
                "baseline_max_depth": q["baseline_max_depth"],
                "ts": self._now(),
            }
        raise ValueError(f"unsupported object_type: {object_type!r}")

    def restart_channel(self, qmgr: str, channel: str) -> dict:
        """Set a channel RUNNING and log the action."""
        self._require_qmgr(qmgr)
        if channel not in self._channels[qmgr]:
            raise ValueError(f"unknown channel: {channel!r} on {qmgr!r}")
        previous = self._channels[qmgr][channel]
        self._channels[qmgr][channel] = "RUNNING"
        self._mq_error_log[qmgr].append(
            {
                "ts": self._now(),
                "severity": "INFO",
                "code": "AMQ9202",
                "message": f"Channel '{channel}' started (was {previous}).",
            }
        )
        return {
            "qmgr": qmgr,
            "channel": channel,
            "previous_status": previous,
            "status": "RUNNING",
            "ts": self._now(),
        }

    def update_queue_config(self, qmgr: str, queue: str, max_depth: int) -> dict:
        """PRIVILEGED: set a queue's MAXDEPTH; ValueError if unknown."""
        self._require_qmgr(qmgr)
        if queue not in self._queues[qmgr]:
            raise ValueError(f"unknown queue: {queue!r} on {qmgr!r}")
        old = self._queues[qmgr][queue]["max_depth"]
        self._queues[qmgr][queue]["max_depth"] = int(max_depth)
        self._mq_error_log[qmgr].append(
            {
                "ts": self._now(),
                "severity": "INFO",
                "code": "AMQ5967",
                "message": f"Queue '{queue}' MAXDEPTH changed from {old} to "
                f"{int(max_depth)} on {qmgr}.",
            }
        )
        return {
            "qmgr": qmgr,
            "queue": queue,
            "old_max_depth": old,
            "max_depth": int(max_depth),
            "ts": self._now(),
        }

    def quarantine_message(self, qmgr: str, queue: str) -> dict:
        """PRIVILEGED: move the poison message to SYSTEM.DLQ; ValueError if unknown.

        If the recorded poison message belongs to this queue, the queue drains
        back to its depth baseline, SYSTEM.DLQ gains one message, and the
        poison flag clears. The return shape is identical either way.
        """
        self._require_qmgr(qmgr)
        if queue not in self._queues[qmgr]:
            raise ValueError(f"unknown queue: {queue!r} on {qmgr!r}")
        if (self._poison is not None
                and self._poison.get("queue") == queue
                and self._poison.get("qmgr", qmgr) == qmgr):
            q = self._queues[qmgr][queue]
            q["depth"] = q["baseline"]
            self._queues[qmgr]["SYSTEM.DLQ"]["depth"] += 1
            self._poison = None
            self._app_logs["payments-api"].append(
                {
                    "ts": self._now(),
                    "level": "INFO",
                    "logger": "payments-api",
                    "message": f"Poison message quarantined to SYSTEM.DLQ "
                    f"from queue '{queue}'.",
                }
            )
        return {
            "qmgr": qmgr,
            "queue": queue,
            "quarantined": True,
            "ts": self._now(),
        }

    # -- listener surface ---------------------------------------------------
    def get_listener_status(self, name: str) -> dict:
        """Return listener status; ValueError if unknown."""
        if name not in self._listeners:
            raise ValueError(f"unknown listener: {name!r}")
        return {"name": name, "status": self._listeners[name], "ts": self._now()}

    def start_listener(self, name: str) -> dict:
        """PRIVILEGED: set a listener RUNNING and log the action."""
        if name not in self._listeners:
            raise ValueError(f"unknown listener: {name!r}")
        previous = self._listeners[name]
        self._listeners[name] = "RUNNING"
        self._mq_error_log["QMGR1"].append(
            {
                "ts": self._now(),
                "severity": "INFO",
                "code": "AMQ9202",
                "message": f"Listener '{name}' started (was {previous}).",
            }
        )
        return {
            "name": name,
            "previous_status": previous,
            "status": "RUNNING",
            "ts": self._now(),
        }

    # -- certificate surface -------------------------------------------------
    def get_cert_status(self, qmgr: str, channel: str) -> dict:
        """Return TLS certificate status for a channel; ValueError if unknown."""
        self._require_qmgr(qmgr)
        key = (qmgr, channel)
        if key not in self._certs:
            raise ValueError(f"unknown certificate for channel {channel!r} on {qmgr!r}")
        c = self._certs[key]
        return {
            "qmgr": qmgr,
            "channel": channel,
            "valid": c["valid"],
            "expires": c["expires"],
            "ts": self._now(),
        }

    def renew_certificate(self, qmgr: str, channel: str) -> dict:
        """PRIVILEGED: renew a channel's TLS certificate; ValueError if unknown."""
        self._require_qmgr(qmgr)
        key = (qmgr, channel)
        if key not in self._certs:
            raise ValueError(f"unknown certificate for channel {channel!r} on {qmgr!r}")
        self._certs[key] = {"valid": True, "expires": "2028-06-01"}
        self._mq_error_log[qmgr].append(
            {
                "ts": self._now(),
                "severity": "INFO",
                "code": "AMQ9777",
                "message": f"Certificate for channel '{channel}' renewed; "
                "new expiry 2028-06-01.",
            }
        )
        return {
            "qmgr": qmgr,
            "channel": channel,
            "valid": True,
            "expires": "2028-06-01",
            "ts": self._now(),
        }

    # -- Kafka-like surface -------------------------------------------------
    def get_kafka_consumer_lag(self, topic: str, group: str) -> dict:
        key = (topic, group)
        if key not in self._kafka_lag:
            raise ValueError(f"unknown consumer group {group!r} on topic {topic!r}")
        return {
            "topic": topic,
            "group": group,
            "lag": self._kafka_lag[key]["lag"],
            "ts": self._now(),
        }

    def restart_consumer(self, topic: str, group: str) -> dict:
        """PRIVILEGED: restart a consumer group (lag drops to baseline)."""
        key = (topic, group)
        if key not in self._kafka_lag:
            raise ValueError(f"unknown consumer group {group!r} on topic {topic!r}")
        self._kafka_lag[key]["lag"] = self._kafka_lag[key]["baseline"]
        return {
            "topic": topic,
            "group": group,
            "lag": self._kafka_lag[key]["baseline"],
            "ts": self._now(),
        }

    def get_kafka_consumer_group_detail(self, topic: str, group: str) -> dict:
        """Per-partition lag detail for a consumer group (simulated)."""
        key = (topic, group)
        if key not in self._kafka_lag:
            raise ValueError(f"unknown consumer group {group!r} on topic {topic!r}")
        if topic not in self._kafka_topics:
            raise ValueError(f"unknown topic: {topic!r}")
        total = self._kafka_lag[key]["lag"]
        nparts = self._kafka_topics[topic]["partitions"]
        per, rem = divmod(total, nparts)
        partitions = []
        for p in range(nparts):
            lag = per + (1 if p < rem else 0)
            end_offset = 100_000 + p * 1_000
            partitions.append({
                "partition": p,
                "end_offset": end_offset,
                "committed": end_offset - lag,
                "lag": lag,
            })
        return {
            "topic": topic,
            "group": group,
            "partitions": partitions,
            "total_lag": total,
            "ts": self._now(),
        }

    def get_kafka_topic_detail(self, topic: str) -> dict:
        """Partition/leader/replica/ISR detail for a topic (simulated)."""
        if topic not in self._kafka_topics:
            raise ValueError(f"unknown topic: {topic!r}")
        topo = self._kafka_topics[topic]
        nparts = topo["partitions"]
        repl = topo["replication_factor"]
        broker_ids = sorted(self._kafka_brokers)
        partitions = []
        for p in range(nparts):
            leader = broker_ids[p % len(broker_ids)]
            replicas = [broker_ids[(p + i) % len(broker_ids)] for i in range(repl)]
            partitions.append({
                "partition": p,
                "leader": leader,
                "replicas": replicas,
                # ISR equals the replica set unless an override was
                # installed (see degrade_partition_isr); with no overrides
                # this returns exactly the historical healthy topology.
                "isr": list(self._kafka_isr.get((topic, p), replicas)),
                "end_offset": 100_000 + p * 1_000,
            })
        return {"topic": topic, "partitions": partitions, "ts": self._now()}

    def degrade_partition_isr(self, topic: str, partition: int) -> dict:
        """Shrink a partition's ISR to its leader only.

        Simulates an under-replicated partition: replicas stay assigned but
        only the leader is in sync. ValueError on unknown topic/partition.
        """
        if topic not in self._kafka_topics:
            raise ValueError(f"unknown topic: {topic!r}")
        nparts = self._kafka_topics[topic]["partitions"]
        if not 0 <= partition < nparts:
            raise ValueError(
                f"unknown partition {partition!r} on topic {topic!r}")
        broker_ids = sorted(self._kafka_brokers)
        leader = broker_ids[partition % len(broker_ids)]
        self._kafka_isr[(topic, partition)] = [leader]
        return {
            "topic": topic,
            "partition": partition,
            "leader": leader,
            "isr": [leader],
            "ts": self._now(),
        }

    def get_kafka_broker_config(self, broker_id: int) -> dict:
        """Broker configuration (simulated)."""
        if broker_id not in self._kafka_brokers:
            raise ValueError(f"unknown broker id: {broker_id!r}")
        return {
            "broker_id": broker_id,
            "configs": dict(self._kafka_brokers[broker_id]),
            "ts": self._now(),
        }

    def get_kafka_broker_health(self) -> dict:
        """Cluster reachability and broker health (simulated)."""
        return {
            "bootstrap_servers": "kafka01:9092",
            "reachable": True,
            "latency_ms": 3.2,
            "brokers": sorted(self._kafka_brokers),
            "controller": self._kafka_controller,
            "degraded": False,
            "degradation": None,
            "ts": self._now(),
        }

    def get_tomcat_heap(self) -> dict:
        """JVM heap and non-heap usage (simulated)."""
        t = self._tomcat
        h = t["heap"]
        used_pct = (h["used_bytes"] / h["max_bytes"] * 100.0
                    if h["max_bytes"] > 0 else None)
        return {
            "host": t["host"],
            "port": t["port"],
            "heap_init_bytes": h["init_bytes"],
            "heap_used_bytes": h["used_bytes"],
            "heap_committed_bytes": h["committed_bytes"],
            "heap_max_bytes": h["max_bytes"],
            "heap_used_pct": used_pct,
            "non_heap_init_bytes": h["non_heap_init_bytes"],
            "non_heap_used_bytes": h["non_heap_used_bytes"],
            "non_heap_committed_bytes": h["non_heap_committed_bytes"],
            "non_heap_max_bytes": h["non_heap_max_bytes"],
            "ts": self._now(),
        }

    def get_tomcat_threadpool(self, pool: str | None = None) -> dict:
        """Thread-pool usage (simulated)."""
        pools = self._tomcat["pools"]
        if pool is not None:
            if pool not in pools:
                raise ValueError(f"unknown thread pool: {pool!r}")
            names = [pool]
        else:
            names = sorted(pools)
        out = []
        for name in names:
            p = pools[name]
            busy_pct = (p["busy"] / p["max"] * 100.0) if p["max"] > 0 else None
            out.append({
                "name": name,
                "current_threads_busy": p["busy"],
                "current_thread_count": p["count"],
                "max_threads": p["max"],
                "busy_pct": busy_pct,
            })
        return {
            "host": self._tomcat["host"],
            "port": self._tomcat["port"],
            "pools": out,
            "ts": self._now(),
        }

    def get_tomcat_apps(self) -> dict:
        """Deployed web apps and their state (simulated)."""
        return {
            "host": self._tomcat["host"],
            "port": self._tomcat["port"],
            "apps": [dict(a) for a in self._tomcat["apps"]],
            "ts": self._now(),
        }

    def read_tomcat_log(self, log: str = "catalina", limit: int = 50) -> list[dict]:
        """Tail a Tomcat log (simulated catalina/access entries)."""
        if log not in ("catalina", "access"):
            raise ValueError(f"unknown tomcat log: {log!r}")
        if log == "catalina":
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": "Server startup in 2341 ms"},
                {"ts": self._now(), "severity": "WARNING",
                 "message": "The web application [/orders] appears to have "
                            "started a thread but has failed to stop it."},
                {"ts": self._now(), "severity": "SEVERE",
                 "message": "All threads (200) are currently busy, waiting. "
                            "Increase maxThreads or investigate stuck threads."},
            ]
        else:
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": '10.0.0.5 - - "POST /payments/charge HTTP/1.1" 200 412'},
                {"ts": self._now(), "severity": "INFO",
                 "message": '10.0.0.5 - - "GET /orders/status HTTP/1.1" 503 0'},
            ]
        return entries[-max(1, int(limit)):]

    def restart_tomcat_app(self, app_path: str) -> dict:
        """PRIVILEGED: restart a Tomcat web app (simulated)."""
        for app in self._tomcat["apps"]:
            if app["path"] == app_path:
                previous = app["state"]
                app["state"] = "running"
                # A restart frees busy threads on the http pool.
                self._tomcat["pools"]["http-nio-8080"]["busy"] = 8
                return {
                    "app_path": app_path,
                    "previous_state": previous,
                    "state": "running",
                    "ts": self._now(),
                }
        raise ValueError(f"unknown app path: {app_path!r}")

    # -- Tomcat-like surface ------------------------------------------------
    def read_app_log(self, app: str, limit: int = 50) -> list[dict]:
        """Most recent app log entries, each {ts, level, logger, message}."""
        if app not in self._app_logs:
            raise ValueError(f"unknown app: {app!r}")
        return list(self._app_logs[app][-limit:])

    def restart_app(self, app: str) -> dict:
        """PRIVILEGED: restart an app; memory on app01 drops, OOM flag clears."""
        if app not in self._app_logs:
            raise ValueError(f"unknown app: {app!r}")
        self._hosts["app01"]["mem_pct"] = 60
        self._oom = None
        self._app_logs[app].append(
            {
                "ts": self._now(),
                "level": "INFO",
                "logger": app,
                "message": "Payments API restarted; JVM heap reset.",
            }
        )
        return {"app": app, "status": "RUNNING", "ts": self._now()}

    # -- WebSphere-like surface ----------------------------------------------
    def get_websphere_heap(self) -> dict:
        """JVM heap usage on was01 (simulated)."""
        w = self._websphere
        h = w["heap"]
        used_pct = (h["used_bytes"] / h["max_bytes"] * 100.0
                    if h["max_bytes"] > 0 else None)
        return {
            "host": w["host"],
            "port": w["port"],
            "heap_init_bytes": h["init_bytes"],
            "heap_used_bytes": h["used_bytes"],
            "heap_committed_bytes": h["committed_bytes"],
            "heap_max_bytes": h["max_bytes"],
            "heap_used_pct": used_pct,
            "ts": self._now(),
        }

    def get_websphere_threadpool(self, pool: str | None = None) -> dict:
        """WebSphere thread-pool stats; ValueError on unknown pool name."""
        w = self._websphere
        pools = w["pools"]
        if pool is not None:
            if pool not in pools:
                raise ValueError(f"unknown websphere thread pool: {pool!r}")
            names = [pool]
        else:
            names = sorted(pools)
        out = []
        for name in names:
            p = pools[name]
            busy_pct = (p["busy"] / p["max"] * 100.0) if p["max"] > 0 else None
            out.append({
                "name": name,
                "current_threads_busy": p["busy"],
                "current_thread_count": p["count"],
                "max_threads": p["max"],
                "busy_pct": busy_pct,
            })
        return {
            "host": w["host"],
            "port": w["port"],
            "pools": out,
            "ts": self._now(),
        }

    def get_websphere_apps(self) -> dict:
        """Deployed WebSphere enterprise apps (simulated)."""
        w = self._websphere
        return {
            "host": w["host"],
            "port": w["port"],
            "apps": [dict(a) for a in w["apps"]],
            "ts": self._now(),
        }

    def read_websphere_log(self, log: str = "systemout",
                           limit: int = 50) -> list[dict]:
        """Tail a WebSphere log (simulated SystemOut/SystemErr entries)."""
        if log not in ("systemout", "systemerr"):
            raise ValueError(f"unknown websphere log: {log!r}")
        if log == "systemout":
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": "Server started: server1 on node wasNode01"},
                {"ts": self._now(), "severity": "WARNING",
                 "message": "Thread pool WebContainer is 82% utilized; "
                            "consider increasing maximumPoolSize"},
            ]
        else:
            entries = [
                {"ts": self._now(), "severity": "ERROR",
                 "message": "NullPointerException in payments-ear "
                            "ChargeServlet.doPost"},
            ]
        return entries[-max(1, int(limit)):]

    def restart_websphere_app(self, app_name: str) -> dict:
        """PRIVILEGED: restart a WebSphere app (simulated stop + start)."""
        for app in self._websphere["apps"]:
            if app["name"] == app_name:
                previous = app["state"]
                app["state"] = "running"
                return {
                    "app_name": app_name,
                    "previous_state": previous,
                    "state": "running",
                    "ts": self._now(),
                }
        raise ValueError(f"unknown websphere app: {app_name!r}")

    # -- WebLogic-like surface -----------------------------------------------
    def get_weblogic_heap(self) -> dict:
        """JVM heap on wls01; init/committed are None (REST limitation)."""
        w = self._weblogic
        h = w["heap"]
        used_pct = (h["used_bytes"] / h["max_bytes"] * 100.0
                    if h["max_bytes"] > 0 else None)
        return {
            "host": w["host"],
            "port": w["port"],
            "heap_init_bytes": None,
            "heap_used_bytes": h["used_bytes"],
            "heap_committed_bytes": None,
            "heap_max_bytes": h["max_bytes"],
            "heap_used_pct": used_pct,
            "ts": self._now(),
        }

    def get_weblogic_threadpool(self) -> dict:
        """WebLogic self-tuning thread pool (no exposed max)."""
        w = self._weblogic
        out = []
        for p in w["pools"]:
            out.append({
                "name": p["name"],
                "current_threads_busy": p["busy"],
                "current_thread_count": p["count"],
                "max_threads": None,
                "busy_pct": None,
            })
        return {
            "host": w["host"],
            "port": w["port"],
            "pools": out,
            "ts": self._now(),
        }

    def get_weblogic_apps(self) -> dict:
        """Deployed WebLogic apps (simulated)."""
        w = self._weblogic
        return {
            "host": w["host"],
            "port": w["port"],
            "apps": [dict(a) for a in w["apps"]],
            "ts": self._now(),
        }

    def read_weblogic_log(self, log: str = "server",
                          limit: int = 50) -> list[dict]:
        """Tail a WebLogic log (simulated server/access entries)."""
        if log not in ("server", "access"):
            raise ValueError(f"unknown weblogic log: {log!r}")
        if log == "server":
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": "Server started in RUNNING mode"},
                {"ts": self._now(), "severity": "WARNING",
                 "message": "ExecuteThread pool utilization above 80%"},
            ]
        else:
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": '10.0.0.7 - - "POST /billing/charge '
                            'HTTP/1.1" 200 512'},
            ]
        return entries[-max(1, int(limit)):]

    def restart_weblogic_app(self, app_name: str) -> dict:
        """PRIVILEGED: restart a WebLogic app (simulated)."""
        for app in self._weblogic["apps"]:
            if app["name"] == app_name:
                previous = app["state"]
                app["state"] = "running"
                return {
                    "app_name": app_name,
                    "previous_state": previous,
                    "state": "running",
                    "ts": self._now(),
                }
        raise ValueError(f"unknown weblogic app: {app_name!r}")

    # -- JBoss/WildFly-like surface ------------------------------------------
    def get_jboss_heap(self) -> dict:
        """JVM heap usage on jboss01 (simulated)."""
        j = self._jboss
        h = j["heap"]
        used_pct = (h["used_bytes"] / h["max_bytes"] * 100.0
                    if h["max_bytes"] > 0 else None)
        return {
            "host": j["host"],
            "port": j["port"],
            "heap_init_bytes": h["init_bytes"],
            "heap_used_bytes": h["used_bytes"],
            "heap_committed_bytes": h["committed_bytes"],
            "heap_max_bytes": h["max_bytes"],
            "heap_used_pct": used_pct,
            "ts": self._now(),
        }

    def get_jboss_threadpool(self, pool: str | None = None) -> dict:
        """JBoss worker stats; ValueError on unknown pool name."""
        j = self._jboss
        pools = j["pools"]
        if pool is not None:
            if pool not in pools:
                raise ValueError(f"unknown jboss thread pool: {pool!r}")
            names = [pool]
        else:
            names = sorted(pools)
        out = []
        for name in names:
            p = pools[name]
            busy_pct = (p["busy"] / p["max"] * 100.0) if p["max"] > 0 else None
            out.append({
                "name": name,
                "current_threads_busy": p["busy"],
                "current_thread_count": p["count"],
                "max_threads": p["max"],
                "busy_pct": busy_pct,
            })
        return {
            "host": j["host"],
            "port": j["port"],
            "pools": out,
            "ts": self._now(),
        }

    def get_jboss_deployments(self) -> dict:
        """Deployed JBoss artifacts (simulated)."""
        j = self._jboss
        return {
            "host": j["host"],
            "port": j["port"],
            "deployments": [dict(d) for d in j["deployments"]],
            "ts": self._now(),
        }

    def read_jboss_log(self, log: str = "server",
                       limit: int = 50) -> list[dict]:
        """Tail a JBoss log (simulated server.log / boot.log entries)."""
        if log not in ("server", "boot"):
            raise ValueError(f"unknown jboss log: {log!r}")
        if log == "server":
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": "WFLYSRV0025: WildFly Full 27.0.1.Final "
                            "started"},
                {"ts": self._now(), "severity": "ERROR",
                 "message": "WFLYEJB0034: EJB invocation failed on "
                            "payments-ejb ChargeBean"},
            ]
        else:
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": "WFLYSRV0239: Bootstrapping complete"},
            ]
        return entries[-max(1, int(limit)):]

    def restart_jboss_deployment(self, deployment: str) -> dict:
        """PRIVILEGED: restart a JBoss deployment (simulated)."""
        for dep in self._jboss["deployments"]:
            if dep["name"] == deployment:
                previous = "running" if dep["enabled"] else "stopped"
                dep["enabled"] = True
                dep["status"] = "OK"
                return {
                    "deployment": deployment,
                    "previous_state": previous,
                    "state": "running",
                    "ts": self._now(),
                }
        raise ValueError(f"unknown jboss deployment: {deployment!r}")

    # -- RabbitMQ-like surface -----------------------------------------------
    def get_rabbitmq_queues(self, vhost: str = "/") -> dict:
        """Queues on a vhost, sorted by name; ValueError on unknown vhost."""
        r = self._rabbitmq
        if vhost not in r["queues"]:
            raise ValueError(f"unknown rabbitmq vhost: {vhost!r}")
        queues = [{"name": name, **q}
                  for name, q in sorted(r["queues"][vhost].items())]
        return {
            "host": r["host"],
            "port": r["port"],
            "vhost": vhost,
            "queues": queues,
            "ts": self._now(),
        }

    def get_rabbitmq_nodes(self) -> dict:
        """Cluster nodes (simulated)."""
        r = self._rabbitmq
        nodes = []
        for n in r["nodes"]:
            limit = n["mem_limit_bytes"]
            used_pct = (n["mem_used_bytes"] / limit * 100.0
                        if limit else None)
            nodes.append({
                "name": n["name"],
                "running": n["running"],
                "mem_used_bytes": n["mem_used_bytes"],
                "mem_limit_bytes": n["mem_limit_bytes"],
                "mem_used_pct": used_pct,
                "disk_free_bytes": n["disk_free_bytes"],
                "fd_used": n["fd_used"],
                "fd_total": n["fd_total"],
            })
        return {
            "host": r["host"],
            "port": r["port"],
            "nodes": nodes,
            "ts": self._now(),
        }

    def get_rabbitmq_connections(self) -> dict:
        """Client connections (simulated)."""
        r = self._rabbitmq
        return {
            "host": r["host"],
            "port": r["port"],
            "connections": [dict(c) for c in r["connections"]],
            "ts": self._now(),
        }

    def purge_rabbitmq_queue(self, vhost: str, queue: str) -> dict:
        """PRIVILEGED: discard every ready message in a queue (simulated)."""
        r = self._rabbitmq
        if vhost not in r["queues"] or queue not in r["queues"][vhost]:
            raise ValueError(
                f"unknown rabbitmq queue: {queue!r} on vhost {vhost!r}")
        q = r["queues"][vhost][queue]
        purged = q["messages"]
        q["messages"] = 0
        q["messages_ready"] = 0
        q["messages_unacknowledged"] = 0
        return {
            "vhost": vhost,
            "queue": queue,
            "messages_purged": purged,
            "ts": self._now(),
        }

    # -- ActiveMQ Artemis-like surface ----------------------------------------
    def get_artemis_queues(self) -> dict:
        """Queues on artemis01 (simulated)."""
        a = self._artemis
        queues = [{"name": name, **q}
                  for name, q in sorted(a["queues"].items())]
        return {
            "host": a["host"],
            "port": a["port"],
            "queues": queues,
            "ts": self._now(),
        }

    def get_artemis_broker(self) -> dict:
        """Broker summary (simulated)."""
        a = self._artemis
        total = sum(q["message_count"] for q in a["queues"].values())
        return {
            "host": a["host"],
            "port": a["port"],
            "version": a["version"],
            "started": a["started"],
            "total_message_count": total,
            "ts": self._now(),
        }

    def purge_artemis_queue(self, queue: str) -> dict:
        """PRIVILEGED: removeAllMessages on a queue (simulated)."""
        a = self._artemis
        if queue not in a["queues"]:
            raise ValueError(f"unknown artemis queue: {queue!r}")
        q = a["queues"][queue]
        purged = q["message_count"]
        q["message_count"] = 0
        q["delivering_count"] = 0
        return {
            "queue": queue,
            "messages_purged": purged,
            "ts": self._now(),
        }

    # -- TIBCO EMS-like surface ------------------------------------------------
    def get_ems_queues(self) -> dict:
        """EMS queues (simulated tibemsadmin output)."""
        e = self._ems
        queues = [{"name": name, **q}
                  for name, q in sorted(e["queues"].items())]
        return {
            "host": e["host"],
            "queues": queues,
            "ts": self._now(),
        }

    def get_ems_server(self) -> dict:
        """EMS server summary (simulated)."""
        e = self._ems
        return {
            "host": e["host"],
            "server": e["server"],
            "state": e["state"],
            "connections": e["connections"],
            "version": e["version"],
            "ts": self._now(),
        }

    def purge_ems_queue(self, queue: str) -> dict:
        """PRIVILEGED: purge an EMS queue (simulated)."""
        e = self._ems
        if queue not in e["queues"]:
            raise ValueError(f"unknown ems queue: {queue!r}")
        q = e["queues"][queue]
        purged = q["pending_messages"]
        q["pending_messages"] = 0
        return {
            "queue": queue,
            "messages_purged": purged,
            "ts": self._now(),
        }

    # -- Nginx-like surface -----------------------------------------------------
    def get_nginx_status(self) -> dict:
        """stub_status counters on web01 (simulated)."""
        n = self._nginx
        return {
            "host": n["host"],
            "port": n["port"],
            "active_connections": n["active_connections"],
            "accepts": n["accepts"],
            "handled": n["handled"],
            "requests": n["requests"],
            "reading": n["reading"],
            "writing": n["writing"],
            "waiting": n["waiting"],
            "ts": self._now(),
        }

    def read_nginx_log(self, log: str = "access",
                       limit: int = 50) -> list[dict]:
        """Tail an Nginx log (simulated access.log / error.log entries)."""
        if log not in ("access", "error"):
            raise ValueError(f"unknown nginx log: {log!r}")
        if log == "access":
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": '10.0.0.9 - - "GET /api/orders HTTP/1.1" '
                            "200 2314"},
                {"ts": self._now(), "severity": "INFO",
                 "message": '10.0.0.9 - - "POST /api/pay HTTP/1.1" 502 0'},
            ]
        else:
            entries = [
                {"ts": self._now(), "severity": "ERROR",
                 "message": "connect() failed (111: Connection refused) "
                            "while connecting to upstream, upstream: "
                            '"10.0.0.31:8080"'},
            ]
        return entries[-max(1, int(limit)):]

    def reload_nginx(self) -> dict:
        """PRIVILEGED: reload nginx config (simulated ``nginx -s reload``)."""
        return {"state": "reloaded", "ts": self._now()}

    # -- Apache HTTPD-like surface ----------------------------------------------
    def get_apache_status(self) -> dict:
        """mod_status counters on web02 (simulated)."""
        a = self._apache
        return {
            "host": a["host"],
            "port": a["port"],
            "total_accesses": a["total_accesses"],
            "total_kbytes": a["total_kbytes"],
            "uptime_secs": a["uptime_secs"],
            "busy_workers": a["busy_workers"],
            "idle_workers": a["idle_workers"],
            "ts": self._now(),
        }

    def read_apache_log(self, log: str = "access",
                        limit: int = 50) -> list[dict]:
        """Tail an Apache log (simulated access_log / error_log entries)."""
        if log not in ("access", "error"):
            raise ValueError(f"unknown apache log: {log!r}")
        if log == "access":
            entries = [
                {"ts": self._now(), "severity": "INFO",
                 "message": '10.0.0.10 - - "GET /static/app.js HTTP/1.1" '
                            "200 48210"},
            ]
        else:
            entries = [
                {"ts": self._now(), "severity": "WARNING",
                 "message": "[proxy:error] AH00940: worker for "
                            "(http://10.0.0.31:8080/) failed"},
            ]
        return entries[-max(1, int(limit)):]

    def reload_apache(self) -> dict:
        """PRIVILEGED: graceful apache reload (simulated)."""
        return {"state": "reloaded", "ts": self._now()}

    # -- HAProxy-like surface -----------------------------------------------------
    def get_haproxy_stats(self) -> dict:
        """Frontend/backend/server stats on lb01 (simulated stats CSV)."""
        h = self._haproxy
        frontends = [dict(f) for f in h["frontends"]]
        backends = []
        for b in h["backends"]:
            servers = []
            for s in b["servers"]:
                admin = s["admin_state"]
                status = {"ready": "UP", "drain": "DRAIN",
                          "maint": "MAINT"}.get(admin, "UP")
                servers.append({
                    "name": s["name"],
                    "status": status,
                    "current_sessions": s["current_sessions"],
                    "check_status": s["check_status"],
                })
            backends.append({
                "name": b["name"],
                "status": b["status"],
                "servers": servers,
            })
        return {
            "host": h["host"],
            "port": h["port"],
            "frontends": frontends,
            "backends": backends,
            "ts": self._now(),
        }

    def set_haproxy_server_state(self, backend: str, server: str,
                                 state: str) -> dict:
        """PRIVILEGED: set an HAProxy server admin state (simulated)."""
        if state not in ("ready", "drain", "maint"):
            raise ValueError(
                f"unknown haproxy server state: {state!r}; expected one of "
                "'ready', 'drain', 'maint'")
        h = self._haproxy
        for b in h["backends"]:
            if b["name"] != backend:
                continue
            for s in b["servers"]:
                if s["name"] != server:
                    continue
                previous = s["admin_state"]
                s["admin_state"] = state
                return {
                    "backend": backend,
                    "server": server,
                    "previous_state": previous,
                    "state": state,
                    "ts": self._now(),
                }
            raise ValueError(
                f"unknown haproxy server: {server!r} in backend "
                f"{backend!r}")
        raise ValueError(f"unknown haproxy backend: {backend!r}")

    # -- PostgreSQL-like surface --------------------------------------------------
    def get_postgres_health(self) -> dict:
        """Postgres server health on pg01 (simulated)."""
        p = self._postgres
        maxc = p["connections_max"]
        pct = (p["connections_used"] / maxc * 100.0) if maxc > 0 else None
        return {
            "host": p["host"],
            "port": p["port"],
            "database": p["database"],
            "version": p["version"],
            "up": p["up"],
            "connections_used": p["connections_used"],
            "connections_max": p["connections_max"],
            "connections_pct": pct,
            "ts": self._now(),
        }

    def get_postgres_blocking(self) -> dict:
        """Sessions waiting on locks on pg01 (simulated)."""
        p = self._postgres
        return {
            "host": p["host"],
            "blockers": [dict(b) for b in p["blockers"]],
            "ts": self._now(),
        }

    def get_postgres_replication(self) -> dict:
        """Replication role/lag on pg01 (simulated)."""
        p = self._postgres
        return {
            "host": p["host"],
            "role": p["role"],
            "replay_lag_bytes": p["replay_lag_bytes"],
            "replay_lag_seconds": p["replay_lag_seconds"],
            "ts": self._now(),
        }

    def terminate_postgres_backend(self, pid: int) -> dict:
        """PRIVILEGED: terminate a backend; the blocker row clears."""
        p = self._postgres
        for i, b in enumerate(p["blockers"]):
            if b["pid"] == int(pid):
                del p["blockers"][i]
                return {"pid": int(pid), "terminated": True,
                        "ts": self._now()}
        raise ValueError(f"unknown postgres backend pid: {pid!r}")

    # -- MySQL-like surface ---------------------------------------------------------
    def get_mysql_health(self) -> dict:
        """MySQL server health on my01 (simulated)."""
        m = self._mysql
        maxc = m["max_connections"]
        pct = (m["threads_connected"] / maxc * 100.0) if maxc > 0 else None
        return {
            "host": m["host"],
            "port": m["port"],
            "version": m["version"],
            "uptime_secs": m["uptime_secs"],
            "threads_connected": m["threads_connected"],
            "max_connections": m["max_connections"],
            "connections_pct": pct,
            "slow_queries": m["slow_queries"],
            "ts": self._now(),
        }

    def get_mysql_processlist(self) -> dict:
        """SHOW PROCESSLIST on my01 (simulated)."""
        m = self._mysql
        return {
            "host": m["host"],
            "processes": [dict(p) for p in m["processes"]],
            "ts": self._now(),
        }

    def get_mysql_replication(self) -> dict:
        """Replica status on my01 (simulated)."""
        m = self._mysql
        return {
            "host": m["host"],
            "role": m["role"],
            "io_running": m["io_running"],
            "sql_running": m["sql_running"],
            "seconds_behind_source": m["seconds_behind_source"],
            "ts": self._now(),
        }

    def kill_mysql_query(self, process_id: int) -> dict:
        """PRIVILEGED: KILL a query; the process row clears."""
        m = self._mysql
        for i, p in enumerate(m["processes"]):
            if p["id"] == int(process_id):
                del m["processes"][i]
                return {"process_id": int(process_id), "killed": True,
                        "ts": self._now()}
        raise ValueError(f"unknown mysql process id: {process_id!r}")

    # -- Oracle Database-like surface ---------------------------------------------------
    def get_oracle_health(self) -> dict:
        """Oracle instance health on ora01 (simulated)."""
        o = self._oracle
        maxs = o["sessions_max"]
        pct = (o["sessions_used"] / maxs * 100.0
               if maxs is not None and maxs > 0 else None)
        return {
            "host": o["host"],
            "port": o["port"],
            "service": o["service"],
            "version": o["version"],
            "open_mode": o["open_mode"],
            "sessions_used": o["sessions_used"],
            "sessions_max": o["sessions_max"],
            "sessions_pct": pct,
            "ts": self._now(),
        }

    def get_oracle_tablespaces(self) -> dict:
        """Tablespace usage on ora01 (simulated)."""
        o = self._oracle
        return {
            "host": o["host"],
            "tablespaces": [dict(t) for t in o["tablespaces"]],
            "ts": self._now(),
        }

    def get_oracle_blocking(self) -> dict:
        """Blocking sessions on ora01 (simulated)."""
        o = self._oracle
        return {
            "host": o["host"],
            "blockers": [dict(b) for b in o["blockers"]],
            "ts": self._now(),
        }

    def kill_oracle_session(self, sid: int, serial: int) -> dict:
        """PRIVILEGED: kill a session; the blocker row clears."""
        o = self._oracle
        for i, b in enumerate(o["blockers"]):
            if b["sid"] == int(sid) and b["serial"] == int(serial):
                del o["blockers"][i]
                return {"sid": int(sid), "serial": int(serial),
                        "killed": True, "ts": self._now()}
        raise ValueError(
            f"unknown oracle session: sid={sid!r}, serial={serial!r}")

    # -- Redis-like surface -----------------------------------------------------------
    def get_redis_info(self) -> dict:
        """INFO summary on redis01 (simulated)."""
        r = self._redis
        maxmem = r["maxmemory_bytes"]
        mem_pct = (r["used_memory_bytes"] / maxmem * 100.0
                   if maxmem else None)
        hits, misses = r["keyspace_hits"], r["keyspace_misses"]
        hit_rate = (hits / (hits + misses) * 100.0
                    if (hits + misses) > 0 else None)
        return {
            "host": r["host"],
            "port": r["port"],
            "version": r["version"],
            "uptime_secs": r["uptime_secs"],
            "used_memory_bytes": r["used_memory_bytes"],
            "maxmemory_bytes": r["maxmemory_bytes"],
            "mem_used_pct": mem_pct,
            "connected_clients": r["connected_clients"],
            "blocked_clients": r["blocked_clients"],
            "hit_rate_pct": hit_rate,
            "ts": self._now(),
        }

    def get_redis_replication(self) -> dict:
        """Replication role on redis01 (simulated)."""
        r = self._redis
        return {
            "host": r["host"],
            "role": r["role"],
            "connected_replicas": r["connected_replicas"],
            "master_link_status": r["master_link_status"],
            "ts": self._now(),
        }

    def get_redis_slowlog(self, limit: int = 25) -> list[dict]:
        """SLOWLOG entries on redis01 (simulated); limit clamped [1, 500]."""
        n = max(1, min(500, int(limit)))
        return [dict(e) for e in self._redis["slowlog"][:n]]

    # -- Elasticsearch-like surface ---------------------------------------------------------
    def get_elasticsearch_cluster_health(self) -> dict:
        """Cluster health of es-prod (simulated)."""
        e = self._es
        return {
            "cluster_name": e["cluster_name"],
            "status": e["status"],
            "number_of_nodes": e["number_of_nodes"],
            "active_shards": e["active_shards"],
            "unassigned_shards": e["unassigned_shards"],
            "relocating_shards": e["relocating_shards"],
            "ts": self._now(),
        }

    def get_elasticsearch_nodes(self) -> dict:
        """Node stats, sorted by name (simulated)."""
        e = self._es
        nodes = sorted((dict(n) for n in e["nodes"]),
                       key=lambda n: n["name"])
        return {"nodes": nodes, "ts": self._now()}

    def get_elasticsearch_indices(self) -> dict:
        """Index stats, sorted by name (simulated)."""
        e = self._es
        indices = sorted((dict(i) for i in e["indices"]),
                         key=lambda i: i["name"])
        return {"indices": indices, "ts": self._now()}

    # -- MongoDB-like surface ---------------------------------------------------------------
    def get_mongo_health(self) -> dict:
        """serverStatus summary on mongo01 (simulated)."""
        m = self._mongo
        return {
            "host": m["host"],
            "port": m["port"],
            "version": m["version"],
            "uptime_secs": m["uptime_secs"],
            "connections_current": m["connections_current"],
            "connections_available": m["connections_available"],
            "ts": self._now(),
        }

    def get_mongo_replset(self) -> dict:
        """Replica set status (simulated)."""
        m = self._mongo
        rs = m["replset"]
        return {
            "set_name": rs["set_name"],
            "my_state": rs["my_state"],
            "members": [dict(x) for x in rs["members"]],
            "ts": self._now(),
        }

    def get_mongo_current_ops(self) -> dict:
        """currentOp output on mongo01 (simulated)."""
        m = self._mongo
        return {
            "ops": [dict(o) for o in m["ops"]],
            "ts": self._now(),
        }

    def kill_mongo_op(self, opid) -> dict:
        """PRIVILEGED: kill an op; the op row clears."""
        m = self._mongo
        for i, o in enumerate(m["ops"]):
            if o["opid"] == opid:
                del m["ops"][i]
                return {"opid": opid, "killed": True, "ts": self._now()}
        raise ValueError(f"unknown mongo opid: {opid!r}")

    # -- Kubernetes-like surface ----------------------------------------------------------------
    def _require_k8s_namespace(self, namespace: str) -> dict:
        ns = self._k8s["namespaces"].get(namespace)
        if ns is None:
            raise ValueError(
                f"unknown kubernetes namespace: {namespace!r}")
        return ns

    def get_k8s_pod_status(self, namespace: str) -> dict:
        """Pod phases in a namespace (simulated)."""
        ns = self._require_k8s_namespace(namespace)
        return {
            "namespace": namespace,
            "pods": [dict(p) for p in ns["pods"]],
            "ts": self._now(),
        }

    def get_k8s_deployments(self, namespace: str) -> dict:
        """Deployments in a namespace (simulated)."""
        ns = self._require_k8s_namespace(namespace)
        return {
            "namespace": namespace,
            "deployments": [dict(d) for d in ns["deployments"]],
            "ts": self._now(),
        }

    def get_k8s_events(self, namespace: str,
                       limit: int = 25) -> list[dict]:
        """Newest-first events in a namespace (simulated)."""
        ns = self._require_k8s_namespace(namespace)
        return [dict(e) for e in ns["events"][:max(1, int(limit))]]

    def restart_k8s_deployment(self, namespace: str,
                               deployment: str) -> dict:
        """PRIVILEGED: rollout restart of a deployment (simulated)."""
        ns = self._require_k8s_namespace(namespace)
        for d in ns["deployments"]:
            if d["name"] == deployment:
                d["restarted_at"] = self._now()
                return {
                    "namespace": namespace,
                    "deployment": deployment,
                    "state": "restarted",
                    "ts": self._now(),
                }
        raise ValueError(
            f"unknown kubernetes deployment: {deployment!r} in namespace "
            f"{namespace!r}")

    # -- Docker-like surface ----------------------------------------------------------------------
    def _find_docker_container(self, container: str) -> dict:
        for c in self._docker["containers"]:
            if c["name"] == container or c["id"].startswith(container):
                return c
        raise ValueError(f"unknown docker container: {container!r}")

    def get_docker_containers(self) -> dict:
        """Containers on the docker host (simulated)."""
        return {
            "containers": [dict(c) for c in self._docker["containers"]],
            "ts": self._now(),
        }

    def get_docker_stats(self) -> dict:
        """One-shot container stats (simulated)."""
        stats = []
        for name, s in self._docker["stats"].items():
            limit = s["mem_limit_bytes"]
            mem_pct = (s["mem_used_bytes"] / limit * 100.0
                       if limit else None)
            stats.append({
                "name": name,
                "cpu_pct": s["cpu_pct"],
                "mem_used_bytes": s["mem_used_bytes"],
                "mem_limit_bytes": s["mem_limit_bytes"],
                "mem_pct": mem_pct,
            })
        return {"stats": stats, "ts": self._now()}

    def read_docker_logs(self, container: str,
                         limit: int = 50) -> list[dict]:
        """Tail a container's logs by name or id prefix (simulated)."""
        c = self._find_docker_container(container)
        entries = self._docker["logs"].get(c["name"], [])
        return [dict(e) for e in entries[-max(1, int(limit)):]]

    def restart_docker_container(self, container: str) -> dict:
        """PRIVILEGED: restart a container (simulated)."""
        c = self._find_docker_container(container)
        previous = c["state"]
        c["state"] = "running"
        c["status"] = "Up 1 second"
        return {
            "container": c["name"],
            "previous_state": previous,
            "state": "running",
            "ts": self._now(),
        }

    # -- Linux-like surface -------------------------------------------------
    def get_host_metrics(self, host: str) -> dict:
        if host not in self._hosts:
            raise ValueError(f"unknown host: {host!r}")
        h = self._hosts[host]
        return {
            "host": host,
            "cpu_pct": h["cpu_pct"],
            "mem_pct": h["mem_pct"],
            "disk_pct": h["disk_pct"],
            "load1": h["load1"],
            "ts": self._now(),
        }

    def archive_logs(self, host: str) -> dict:
        """PRIVILEGED: archive logs on a host, freeing disk to 45%."""
        if host not in self._hosts:
            raise ValueError(f"unknown host: {host!r}")
        self._hosts[host]["disk_pct"] = 45
        self._app_logs["payments-api"].append(
            {
                "ts": self._now(),
                "level": "INFO",
                "logger": "payments-api",
                "message": f"Log archive completed on host '{host}'; "
                "disk usage now 45%.",
            }
        )
        return {"host": host, "disk_pct": 45, "ts": self._now()}

    # -- time ----------------------------------------------------------------
    def tick(self, seconds: int = 60) -> None:
        """Advance the sim clock; evolve stopped-channel backlogs and drains."""
        self._clock += timedelta(seconds=seconds)
        ts = self._now()

        for (qmgr, channel), queue in self._channel_feeds.items():
            q = self._queues[qmgr][queue]
            status = self._channels[qmgr][channel]
            poisoned = (self._poison is not None
                        and self._poison.get("queue") == queue
                        and self._poison.get("qmgr", qmgr) == qmgr)
            if status in ("STOPPED", "RETRYING"):
                q["depth"] = min(q["max_depth"], q["depth"] + 200)
            elif status == "RUNNING" and not poisoned and q["depth"] > q["baseline"]:
                q["depth"] = max(q["baseline"], q["depth"] - 1500)

        for key, entry in self._kafka_lag.items():
            if entry["lag"] > entry["baseline"]:
                entry["lag"] = max(entry["baseline"], entry["lag"] - 400)

        if self._rng.random() < 0.3:
            self._app_logs["payments-api"].append(
                {
                    "ts": ts,
                    "level": "INFO",
                    "logger": "payments-api",
                    "message": "Health check OK: processed batch of payments events.",
                }
            )
