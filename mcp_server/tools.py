"""Tool definitions for the RCA assistant MCP server.

Each ToolDef pairs a name, required scope, input schema, and a handler.
Handlers take (estate, args) and return plain JSON-serializable
dicts/lists. The simulated Estate is built in a parallel workstream, so
nothing here imports it; handlers receive it as a parameter and are
written against its documented interface.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ToolDef:
    """One MCP tool: scope gate, description, schema, and dispatcher."""

    name: str
    scope: str
    description: str
    input_schema: dict
    handler: Callable[[Any, dict], Any]


PRIVILEGED_TOOLS = frozenset({
    "restart_channel",
    "archive_logs",
    "restart_app",
    "restart_consumer",
    "renew_certificate",
    "update_queue_config",
    "start_listener",
    "quarantine_message",
    "restart_tomcat_app",
    "restart_websphere_app",
    "restart_weblogic_app",
    "restart_jboss_deployment",
    "purge_rabbitmq_queue",
    "purge_artemis_queue",
    "purge_ems_queue",
    "reload_nginx",
    "reload_apache",
    "set_haproxy_server_state",
    "terminate_postgres_backend",
    "kill_mysql_query",
    "kill_oracle_session",
    "kill_mongo_op",
    "restart_k8s_deployment",
    "restart_docker_container",
})


def _require(args: dict, *names: str) -> None:
    missing = [n for n in names if n not in args or args[n] is None]
    if missing:
        raise ValueError(f"missing required argument(s): {', '.join(missing)}")


def _limit(args: dict) -> int:
    return int(args.get("limit", 50))


def _h_queue_depth(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "queue")
    return estate.get_queue_depth(args["qmgr"], args["queue"])


def _h_channel_status(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "channel")
    return estate.get_channel_status(args["qmgr"], args["channel"])


def _h_error_log(estate: Any, args: dict) -> list:
    _require(args, "qmgr")
    return estate.read_error_log(args["qmgr"], limit=_limit(args))


def _h_kafka_lag(estate: Any, args: dict) -> dict:
    _require(args, "topic", "group")
    return estate.get_kafka_consumer_lag(args["topic"], args["group"])


def _h_host_metrics(estate: Any, args: dict) -> dict:
    _require(args, "host")
    return estate.get_host_metrics(args["host"])


def _h_config(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "object_type", "name")
    return estate.get_config(args["qmgr"], args["object_type"], args["name"])


def _h_restart_channel(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "channel")
    return estate.restart_channel(args["qmgr"], args["channel"])


def _h_app_log(estate: Any, args: dict) -> list:
    _require(args, "app")
    return estate.read_app_log(args["app"], limit=_limit(args))


def _h_listener_status(estate: Any, args: dict) -> dict:
    _require(args, "name")
    return estate.get_listener_status(args["name"])


def _h_cert_status(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "channel")
    return estate.get_cert_status(args["qmgr"], args["channel"])


def _h_archive_logs(estate: Any, args: dict) -> dict:
    _require(args, "host")
    return estate.archive_logs(args["host"])


def _h_restart_app(estate: Any, args: dict) -> dict:
    _require(args, "app")
    return estate.restart_app(args["app"])


def _h_restart_consumer(estate: Any, args: dict) -> dict:
    _require(args, "topic", "group")
    return estate.restart_consumer(args["topic"], args["group"])


def _h_renew_cert(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "channel")
    return estate.renew_certificate(args["qmgr"], args["channel"])


def _h_update_queue_config(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "queue", "max_depth")
    return estate.update_queue_config(args["qmgr"], args["queue"],
                                      int(args["max_depth"]))


def _h_start_listener(estate: Any, args: dict) -> dict:
    _require(args, "name")
    return estate.start_listener(args["name"])


def _h_quarantine(estate: Any, args: dict) -> dict:
    _require(args, "qmgr", "queue")
    return estate.quarantine_message(args["qmgr"], args["queue"])


def _h_tail_log(estate: Any, args: dict) -> list:
    _require(args, "path")
    return estate.tail_log(args["path"], int(args.get("limit", 50)))


def _h_kafka_group_detail(estate: Any, args: dict) -> dict:
    _require(args, "topic", "group")
    return estate.get_kafka_consumer_group_detail(args["topic"], args["group"])


def _h_kafka_topic_detail(estate: Any, args: dict) -> dict:
    _require(args, "topic")
    return estate.get_kafka_topic_detail(args["topic"])


def _h_kafka_broker_config(estate: Any, args: dict) -> dict:
    _require(args, "broker_id")
    return estate.get_kafka_broker_config(int(args["broker_id"]))


def _h_kafka_broker_health(estate: Any, args: dict) -> dict:
    return estate.get_kafka_broker_health()


def _h_tomcat_heap(estate: Any, args: dict) -> dict:
    return estate.get_tomcat_heap()


def _h_tomcat_threadpool(estate: Any, args: dict) -> dict:
    return estate.get_tomcat_threadpool(args.get("pool"))


def _h_tomcat_apps(estate: Any, args: dict) -> dict:
    return estate.get_tomcat_apps()


def _h_tomcat_log(estate: Any, args: dict) -> list:
    return estate.read_tomcat_log(args.get("log", "catalina"),
                                  int(args.get("limit", 50)))


def _h_restart_tomcat_app(estate: Any, args: dict) -> dict:
    _require(args, "app_path")
    return estate.restart_tomcat_app(args["app_path"])


def _h_websphere_heap(estate: Any, args: dict) -> dict:
    return estate.get_websphere_heap()


def _h_websphere_threadpool(estate: Any, args: dict) -> dict:
    return estate.get_websphere_threadpool(args.get("pool"))


def _h_websphere_apps(estate: Any, args: dict) -> dict:
    return estate.get_websphere_apps()


def _h_websphere_log(estate: Any, args: dict) -> list:
    return estate.read_websphere_log(args.get("log", "systemout"),
                                     int(args.get("limit", 50)))


def _h_weblogic_heap(estate: Any, args: dict) -> dict:
    return estate.get_weblogic_heap()


def _h_weblogic_threadpool(estate: Any, args: dict) -> dict:
    return estate.get_weblogic_threadpool()


def _h_weblogic_apps(estate: Any, args: dict) -> dict:
    return estate.get_weblogic_apps()


def _h_weblogic_log(estate: Any, args: dict) -> list:
    return estate.read_weblogic_log(args.get("log", "server"),
                                    int(args.get("limit", 50)))


def _h_jboss_heap(estate: Any, args: dict) -> dict:
    return estate.get_jboss_heap()


def _h_jboss_threadpool(estate: Any, args: dict) -> dict:
    return estate.get_jboss_threadpool(args.get("pool"))


def _h_jboss_deployments(estate: Any, args: dict) -> dict:
    return estate.get_jboss_deployments()


def _h_jboss_log(estate: Any, args: dict) -> list:
    return estate.read_jboss_log(args.get("log", "server"),
                                 int(args.get("limit", 50)))


def _h_rabbitmq_queues(estate: Any, args: dict) -> dict:
    return estate.get_rabbitmq_queues(args.get("vhost", "/"))


def _h_rabbitmq_nodes(estate: Any, args: dict) -> dict:
    return estate.get_rabbitmq_nodes()


def _h_rabbitmq_connections(estate: Any, args: dict) -> dict:
    return estate.get_rabbitmq_connections()


def _h_artemis_queues(estate: Any, args: dict) -> dict:
    return estate.get_artemis_queues()


def _h_artemis_broker(estate: Any, args: dict) -> dict:
    return estate.get_artemis_broker()


def _h_ems_queues(estate: Any, args: dict) -> dict:
    return estate.get_ems_queues()


def _h_ems_server(estate: Any, args: dict) -> dict:
    return estate.get_ems_server()


def _h_nginx_status(estate: Any, args: dict) -> dict:
    return estate.get_nginx_status()


def _h_nginx_log(estate: Any, args: dict) -> list:
    return estate.read_nginx_log(args.get("log", "access"),
                                 int(args.get("limit", 50)))


def _h_apache_status(estate: Any, args: dict) -> dict:
    return estate.get_apache_status()


def _h_apache_log(estate: Any, args: dict) -> list:
    return estate.read_apache_log(args.get("log", "access"),
                                  int(args.get("limit", 50)))


def _h_haproxy_stats(estate: Any, args: dict) -> dict:
    return estate.get_haproxy_stats()


def _h_postgres_health(estate: Any, args: dict) -> dict:
    return estate.get_postgres_health()


def _h_postgres_blocking(estate: Any, args: dict) -> dict:
    return estate.get_postgres_blocking()


def _h_postgres_replication(estate: Any, args: dict) -> dict:
    return estate.get_postgres_replication()


def _h_mysql_health(estate: Any, args: dict) -> dict:
    return estate.get_mysql_health()


def _h_mysql_processlist(estate: Any, args: dict) -> dict:
    return estate.get_mysql_processlist()


def _h_mysql_replication(estate: Any, args: dict) -> dict:
    return estate.get_mysql_replication()


def _h_oracle_health(estate: Any, args: dict) -> dict:
    return estate.get_oracle_health()


def _h_oracle_tablespaces(estate: Any, args: dict) -> dict:
    return estate.get_oracle_tablespaces()


def _h_oracle_blocking(estate: Any, args: dict) -> dict:
    return estate.get_oracle_blocking()


def _h_redis_info(estate: Any, args: dict) -> dict:
    return estate.get_redis_info()


def _h_redis_replication(estate: Any, args: dict) -> dict:
    return estate.get_redis_replication()


def _h_redis_slowlog(estate: Any, args: dict) -> list:
    return estate.get_redis_slowlog(int(args.get("limit", 50)))


def _h_elasticsearch_cluster_health(estate: Any, args: dict) -> dict:
    return estate.get_elasticsearch_cluster_health()


def _h_elasticsearch_nodes(estate: Any, args: dict) -> dict:
    return estate.get_elasticsearch_nodes()


def _h_elasticsearch_indices(estate: Any, args: dict) -> dict:
    return estate.get_elasticsearch_indices()


def _h_mongo_health(estate: Any, args: dict) -> dict:
    return estate.get_mongo_health()


def _h_mongo_replset(estate: Any, args: dict) -> dict:
    return estate.get_mongo_replset()


def _h_mongo_current_ops(estate: Any, args: dict) -> dict:
    return estate.get_mongo_current_ops()


def _h_k8s_pod_status(estate: Any, args: dict) -> dict:
    _require(args, "namespace")
    return estate.get_k8s_pod_status(args["namespace"])


def _h_k8s_deployments(estate: Any, args: dict) -> dict:
    _require(args, "namespace")
    return estate.get_k8s_deployments(args["namespace"])


def _h_k8s_events(estate: Any, args: dict) -> list:
    _require(args, "namespace")
    return estate.get_k8s_events(args["namespace"], int(args.get("limit", 50)))


def _h_docker_containers(estate: Any, args: dict) -> dict:
    return estate.get_docker_containers()


def _h_docker_stats(estate: Any, args: dict) -> dict:
    return estate.get_docker_stats()


def _h_docker_logs(estate: Any, args: dict) -> list:
    _require(args, "container")
    return estate.read_docker_logs(args["container"], int(args.get("limit", 50)))


def _h_restart_websphere_app(estate: Any, args: dict) -> dict:
    _require(args, "app_name")
    return estate.restart_websphere_app(args["app_name"])


def _h_restart_weblogic_app(estate: Any, args: dict) -> dict:
    _require(args, "app_name")
    return estate.restart_weblogic_app(args["app_name"])


def _h_restart_jboss_deployment(estate: Any, args: dict) -> dict:
    _require(args, "deployment")
    return estate.restart_jboss_deployment(args["deployment"])


def _h_purge_rabbitmq_queue(estate: Any, args: dict) -> dict:
    _require(args, "vhost", "queue")
    return estate.purge_rabbitmq_queue(args["vhost"], args["queue"])


def _h_purge_artemis_queue(estate: Any, args: dict) -> dict:
    _require(args, "queue")
    return estate.purge_artemis_queue(args["queue"])


def _h_purge_ems_queue(estate: Any, args: dict) -> dict:
    _require(args, "queue")
    return estate.purge_ems_queue(args["queue"])


def _h_reload_nginx(estate: Any, args: dict) -> dict:
    return estate.reload_nginx()


def _h_reload_apache(estate: Any, args: dict) -> dict:
    return estate.reload_apache()


def _h_set_haproxy_server_state(estate: Any, args: dict) -> dict:
    _require(args, "backend", "server", "state")
    return estate.set_haproxy_server_state(args["backend"], args["server"],
                                           args["state"])


def _h_terminate_postgres_backend(estate: Any, args: dict) -> dict:
    _require(args, "pid")
    return estate.terminate_postgres_backend(int(args["pid"]))


def _h_kill_mysql_query(estate: Any, args: dict) -> dict:
    _require(args, "process_id")
    return estate.kill_mysql_query(int(args["process_id"]))


def _h_kill_oracle_session(estate: Any, args: dict) -> dict:
    _require(args, "sid", "serial")
    return estate.kill_oracle_session(int(args["sid"]), int(args["serial"]))


def _h_kill_mongo_op(estate: Any, args: dict) -> dict:
    _require(args, "opid")
    return estate.kill_mongo_op(args["opid"])


def _h_restart_k8s_deployment(estate: Any, args: dict) -> dict:
    _require(args, "namespace", "deployment")
    return estate.restart_k8s_deployment(args["namespace"], args["deployment"])


def _h_restart_docker_container(estate: Any, args: dict) -> dict:
    _require(args, "container")
    return estate.restart_docker_container(args["container"])


def _schema(*required: str, properties: dict) -> dict:
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


def build_tool_defs(estate: Any, include: set[str] | None = None) -> list[ToolDef]:
    """Return the 90 ToolDefs bound against the given estate instance.

    The estate argument is accepted for signature symmetry with other
    packages; handlers receive the estate at call time from the Gateway.

    When `include` is given, only the named tools are registered. That is
    how connectors that implement a subset of the surface (e.g. the
    LinuxHostConnector, which has no MQ/Kafka methods) plug into the same
    authz/audit path: the gateway registers just the tools the estate
    supports, so every registered handler can be dispatched safely.
    """
    _ = estate
    defs = [
        ToolDef(
            name="get_queue_depth",
            scope="diagnostics:read",
            description="Current depth and status of an IBM MQ queue.",
            input_schema=_schema(
                "qmgr", "queue",
                properties={"qmgr": {"type": "string"}, "queue": {"type": "string"}},
            ),
            handler=_h_queue_depth,
        ),
        ToolDef(
            name="get_channel_status",
            scope="diagnostics:read",
            description="Current status of an IBM MQ channel.",
            input_schema=_schema(
                "qmgr", "channel",
                properties={"qmgr": {"type": "string"}, "channel": {"type": "string"}},
            ),
            handler=_h_channel_status,
        ),
        ToolDef(
            name="read_error_log",
            scope="diagnostics:read",
            description="Recent entries from a queue manager error log.",
            input_schema=_schema(
                "qmgr",
                properties={
                    "qmgr": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_error_log,
        ),
        ToolDef(
            name="get_kafka_consumer_lag",
            scope="diagnostics:read",
            description="Consumer lag for a Kafka topic/group.",
            input_schema=_schema(
                "topic", "group",
                properties={"topic": {"type": "string"}, "group": {"type": "string"}},
            ),
            handler=_h_kafka_lag,
        ),
        ToolDef(
            name="get_host_metrics",
            scope="diagnostics:read",
            description="CPU, memory, and disk metrics for a host.",
            input_schema=_schema(
                "host", properties={"host": {"type": "string"}}
            ),
            handler=_h_host_metrics,
        ),
        ToolDef(
            name="get_config",
            scope="diagnostics:read",
            description="Configuration of an MQ object (queue, channel, ...).",
            input_schema=_schema(
                "qmgr", "object_type", "name",
                properties={
                    "qmgr": {"type": "string"},
                    "object_type": {"type": "string"},
                    "name": {"type": "string"},
                },
            ),
            handler=_h_config,
        ),
        ToolDef(
            name="restart_channel",
            scope="admin:write",
            description="PRIVILEGED: restart an MQ channel (mutates state).",
            input_schema=_schema(
                "qmgr", "channel",
                properties={"qmgr": {"type": "string"}, "channel": {"type": "string"}},
            ),
            handler=_h_restart_channel,
        ),
        ToolDef(
            name="read_app_log",
            scope="diagnostics:read",
            description="Recent entries from an application log.",
            input_schema=_schema(
                "app",
                properties={
                    "app": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_app_log,
        ),
        ToolDef(
            name="get_listener_status",
            scope="diagnostics:read",
            description="Current status of an MQ listener.",
            input_schema=_schema(
                "name", properties={"name": {"type": "string"}}
            ),
            handler=_h_listener_status,
        ),
        ToolDef(
            name="get_cert_status",
            scope="diagnostics:read",
            description="TLS certificate status for an MQ channel.",
            input_schema=_schema(
                "qmgr", "channel",
                properties={"qmgr": {"type": "string"}, "channel": {"type": "string"}},
            ),
            handler=_h_cert_status,
        ),
        ToolDef(
            name="archive_logs",
            scope="admin:write",
            description="PRIVILEGED: archive logs on a host, freeing disk (mutates state).",
            input_schema=_schema(
                "host", properties={"host": {"type": "string"}}
            ),
            handler=_h_archive_logs,
        ),
        ToolDef(
            name="restart_app",
            scope="admin:write",
            description="PRIVILEGED: restart an application (mutates state).",
            input_schema=_schema(
                "app", properties={"app": {"type": "string"}}
            ),
            handler=_h_restart_app,
        ),
        ToolDef(
            name="restart_consumer",
            scope="admin:write",
            description="PRIVILEGED: restart a Kafka consumer group (mutates state).",
            input_schema=_schema(
                "topic", "group",
                properties={"topic": {"type": "string"}, "group": {"type": "string"}},
            ),
            handler=_h_restart_consumer,
        ),
        ToolDef(
            name="renew_certificate",
            scope="admin:write",
            description="PRIVILEGED: renew the TLS certificate for an MQ channel "
            "(mutates state).",
            input_schema=_schema(
                "qmgr", "channel",
                properties={"qmgr": {"type": "string"}, "channel": {"type": "string"}},
            ),
            handler=_h_renew_cert,
        ),
        ToolDef(
            name="update_queue_config",
            scope="admin:write",
            description="PRIVILEGED: change an MQ queue's MAXDEPTH (mutates state).",
            input_schema=_schema(
                "qmgr", "queue", "max_depth",
                properties={
                    "qmgr": {"type": "string"},
                    "queue": {"type": "string"},
                    "max_depth": {"type": "integer"},
                },
            ),
            handler=_h_update_queue_config,
        ),
        ToolDef(
            name="start_listener",
            scope="admin:write",
            description="PRIVILEGED: start an MQ listener (mutates state).",
            input_schema=_schema(
                "name", properties={"name": {"type": "string"}}
            ),
            handler=_h_start_listener,
        ),
        ToolDef(
            name="quarantine_message",
            scope="admin:write",
            description="PRIVILEGED: quarantine a poison message to SYSTEM.DLQ "
            "(mutates state).",
            input_schema=_schema(
                "qmgr", "queue",
                properties={"qmgr": {"type": "string"}, "queue": {"type": "string"}},
            ),
            handler=_h_quarantine,
        ),
        ToolDef(
            name="tail_log",
            scope="diagnostics:read",
            description="Read-only tail of a host log file. Only estates that "
            "expose an allow-listed log reader (e.g. the real "
            "LinuxHostConnector) support it; the simulated estate does not, "
            "so sim gateways simply never register it.",
            input_schema=_schema(
                "path",
                properties={
                    "path": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_tail_log,
        ),
        ToolDef(
            name="get_kafka_consumer_group_detail",
            scope="diagnostics:read",
            description="Per-partition consumer lag detail for a Kafka "
            "topic/group.",
            input_schema=_schema(
                "topic", "group",
                properties={"topic": {"type": "string"}, "group": {"type": "string"}},
            ),
            handler=_h_kafka_group_detail,
        ),
        ToolDef(
            name="get_kafka_topic_detail",
            scope="diagnostics:read",
            description="Partition/leader/replica/ISR detail for a Kafka topic.",
            input_schema=_schema(
                "topic", properties={"topic": {"type": "string"}}
            ),
            handler=_h_kafka_topic_detail,
        ),
        ToolDef(
            name="get_kafka_broker_config",
            scope="diagnostics:read",
            description="Configuration of a Kafka broker.",
            input_schema=_schema(
                "broker_id", properties={"broker_id": {"type": "integer"}}
            ),
            handler=_h_kafka_broker_config,
        ),
        ToolDef(
            name="get_kafka_broker_health",
            scope="diagnostics:read",
            description="Reachability and health of the Kafka cluster/brokers.",
            input_schema=_schema(properties={}),
            handler=_h_kafka_broker_health,
        ),
        ToolDef(
            name="get_tomcat_heap",
            scope="diagnostics:read",
            description="JVM heap and non-heap memory usage of a Tomcat server.",
            input_schema=_schema(properties={}),
            handler=_h_tomcat_heap,
        ),
        ToolDef(
            name="get_tomcat_threadpool",
            scope="diagnostics:read",
            description="Tomcat connector thread-pool usage (busy/total/max "
            "threads), optionally for one pool.",
            input_schema=_schema(
                properties={"pool": {"type": "string"}},
            ),
            handler=_h_tomcat_threadpool,
        ),
        ToolDef(
            name="get_tomcat_apps",
            scope="diagnostics:read",
            description="Deployed Tomcat web applications and their state.",
            input_schema=_schema(properties={}),
            handler=_h_tomcat_apps,
        ),
        ToolDef(
            name="read_tomcat_log",
            scope="diagnostics:read",
            description="Tail a Tomcat log (catalina or access).",
            input_schema=_schema(
                properties={
                    "log": {"type": "string", "default": "catalina"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_tomcat_log,
        ),
        ToolDef(
            name="restart_tomcat_app",
            scope="admin:write",
            description="PRIVILEGED: restart a Tomcat web application "
            "(mutates state).",
            input_schema=_schema(
                "app_path", properties={"app_path": {"type": "string"}}
            ),
            handler=_h_restart_tomcat_app,
        ),
        ToolDef(
            name="get_websphere_heap",
            scope="diagnostics:read",
            description="JVM heap and non-heap memory usage of a WebSphere "
            "server.",
            input_schema=_schema(properties={}),
            handler=_h_websphere_heap,
        ),
        ToolDef(
            name="get_websphere_threadpool",
            scope="diagnostics:read",
            description="WebSphere thread-pool usage (busy/total/max "
            "threads), optionally for one pool.",
            input_schema=_schema(
                properties={"pool": {"type": "string"}},
            ),
            handler=_h_websphere_threadpool,
        ),
        ToolDef(
            name="get_websphere_apps",
            scope="diagnostics:read",
            description="Deployed WebSphere applications and their state.",
            input_schema=_schema(properties={}),
            handler=_h_websphere_apps,
        ),
        ToolDef(
            name="read_websphere_log",
            scope="diagnostics:read",
            description="Tail a WebSphere log (SystemOut by default).",
            input_schema=_schema(
                properties={
                    "log": {"type": "string", "default": "systemout"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_websphere_log,
        ),
        ToolDef(
            name="get_weblogic_heap",
            scope="diagnostics:read",
            description="JVM heap and non-heap memory usage of a WebLogic "
            "server.",
            input_schema=_schema(properties={}),
            handler=_h_weblogic_heap,
        ),
        ToolDef(
            name="get_weblogic_threadpool",
            scope="diagnostics:read",
            description="WebLogic thread-pool usage (busy/total/max threads).",
            input_schema=_schema(properties={}),
            handler=_h_weblogic_threadpool,
        ),
        ToolDef(
            name="get_weblogic_apps",
            scope="diagnostics:read",
            description="Deployed WebLogic applications and their state.",
            input_schema=_schema(properties={}),
            handler=_h_weblogic_apps,
        ),
        ToolDef(
            name="read_weblogic_log",
            scope="diagnostics:read",
            description="Tail a WebLogic server log.",
            input_schema=_schema(
                properties={
                    "log": {"type": "string", "default": "server"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_weblogic_log,
        ),
        ToolDef(
            name="get_jboss_heap",
            scope="diagnostics:read",
            description="JVM heap and non-heap memory usage of a JBoss/WildFly "
            "server.",
            input_schema=_schema(properties={}),
            handler=_h_jboss_heap,
        ),
        ToolDef(
            name="get_jboss_threadpool",
            scope="diagnostics:read",
            description="JBoss/WildFly thread-pool usage (busy/total/max "
            "threads), optionally for one pool.",
            input_schema=_schema(
                properties={"pool": {"type": "string"}},
            ),
            handler=_h_jboss_threadpool,
        ),
        ToolDef(
            name="get_jboss_deployments",
            scope="diagnostics:read",
            description="JBoss/WildFly deployments and their state.",
            input_schema=_schema(properties={}),
            handler=_h_jboss_deployments,
        ),
        ToolDef(
            name="read_jboss_log",
            scope="diagnostics:read",
            description="Tail a JBoss/WildFly server log.",
            input_schema=_schema(
                properties={
                    "log": {"type": "string", "default": "server"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_jboss_log,
        ),
        ToolDef(
            name="get_rabbitmq_queues",
            scope="diagnostics:read",
            description="RabbitMQ queues with depth and consumer counts for "
            "a vhost.",
            input_schema=_schema(
                properties={"vhost": {"type": "string", "default": "/"}},
            ),
            handler=_h_rabbitmq_queues,
        ),
        ToolDef(
            name="get_rabbitmq_nodes",
            scope="diagnostics:read",
            description="RabbitMQ cluster nodes and their health.",
            input_schema=_schema(properties={}),
            handler=_h_rabbitmq_nodes,
        ),
        ToolDef(
            name="get_rabbitmq_connections",
            scope="diagnostics:read",
            description="RabbitMQ client connections and their state.",
            input_schema=_schema(properties={}),
            handler=_h_rabbitmq_connections,
        ),
        ToolDef(
            name="get_artemis_queues",
            scope="diagnostics:read",
            description="ActiveMQ Artemis queues with depth and consumer "
            "counts.",
            input_schema=_schema(properties={}),
            handler=_h_artemis_queues,
        ),
        ToolDef(
            name="get_artemis_broker",
            scope="diagnostics:read",
            description="ActiveMQ Artemis broker health and status.",
            input_schema=_schema(properties={}),
            handler=_h_artemis_broker,
        ),
        ToolDef(
            name="get_ems_queues",
            scope="diagnostics:read",
            description="TIBCO EMS queues with depth and consumer counts.",
            input_schema=_schema(properties={}),
            handler=_h_ems_queues,
        ),
        ToolDef(
            name="get_ems_server",
            scope="diagnostics:read",
            description="TIBCO EMS server health and status.",
            input_schema=_schema(properties={}),
            handler=_h_ems_server,
        ),
        ToolDef(
            name="get_nginx_status",
            scope="diagnostics:read",
            description="NGINX stub status (active connections, accepted "
            "requests).",
            input_schema=_schema(properties={}),
            handler=_h_nginx_status,
        ),
        ToolDef(
            name="read_nginx_log",
            scope="diagnostics:read",
            description="Tail an NGINX log (access by default).",
            input_schema=_schema(
                properties={
                    "log": {"type": "string", "default": "access"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_nginx_log,
        ),
        ToolDef(
            name="get_apache_status",
            scope="diagnostics:read",
            description="Apache HTTPD server status (workers, requests).",
            input_schema=_schema(properties={}),
            handler=_h_apache_status,
        ),
        ToolDef(
            name="read_apache_log",
            scope="diagnostics:read",
            description="Tail an Apache HTTPD log (access by default).",
            input_schema=_schema(
                properties={
                    "log": {"type": "string", "default": "access"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_apache_log,
        ),
        ToolDef(
            name="get_haproxy_stats",
            scope="diagnostics:read",
            description="HAProxy frontend/backend/server statistics.",
            input_schema=_schema(properties={}),
            handler=_h_haproxy_stats,
        ),
        ToolDef(
            name="get_postgres_health",
            scope="diagnostics:read",
            description="PostgreSQL health (up, connections, slow queries).",
            input_schema=_schema(properties={}),
            handler=_h_postgres_health,
        ),
        ToolDef(
            name="get_postgres_blocking",
            scope="diagnostics:read",
            description="PostgreSQL sessions currently blocked on locks.",
            input_schema=_schema(properties={}),
            handler=_h_postgres_blocking,
        ),
        ToolDef(
            name="get_postgres_replication",
            scope="diagnostics:read",
            description="PostgreSQL replication status (primary/replica lag).",
            input_schema=_schema(properties={}),
            handler=_h_postgres_replication,
        ),
        ToolDef(
            name="get_mysql_health",
            scope="diagnostics:read",
            description="MySQL health (up, connections, slow queries).",
            input_schema=_schema(properties={}),
            handler=_h_mysql_health,
        ),
        ToolDef(
            name="get_mysql_processlist",
            scope="diagnostics:read",
            description="MySQL processlist (running queries and sessions).",
            input_schema=_schema(properties={}),
            handler=_h_mysql_processlist,
        ),
        ToolDef(
            name="get_mysql_replication",
            scope="diagnostics:read",
            description="MySQL replication status (replica lag, IO/SQL "
            "thread state).",
            input_schema=_schema(properties={}),
            handler=_h_mysql_replication,
        ),
        ToolDef(
            name="get_oracle_health",
            scope="diagnostics:read",
            description="Oracle database health (up, sessions, wait events).",
            input_schema=_schema(properties={}),
            handler=_h_oracle_health,
        ),
        ToolDef(
            name="get_oracle_tablespaces",
            scope="diagnostics:read",
            description="Oracle tablespace usage (size, used, free).",
            input_schema=_schema(properties={}),
            handler=_h_oracle_tablespaces,
        ),
        ToolDef(
            name="get_oracle_blocking",
            scope="diagnostics:read",
            description="Oracle sessions currently blocked on locks.",
            input_schema=_schema(properties={}),
            handler=_h_oracle_blocking,
        ),
        ToolDef(
            name="get_redis_info",
            scope="diagnostics:read",
            description="Redis INFO (memory, clients, keyspace).",
            input_schema=_schema(properties={}),
            handler=_h_redis_info,
        ),
        ToolDef(
            name="get_redis_replication",
            scope="diagnostics:read",
            description="Redis replication status (role, replicas, lag).",
            input_schema=_schema(properties={}),
            handler=_h_redis_replication,
        ),
        ToolDef(
            name="get_redis_slowlog",
            scope="diagnostics:read",
            description="Recent Redis SLOWLOG entries.",
            input_schema=_schema(
                properties={"limit": {"type": "integer", "default": 50}},
            ),
            handler=_h_redis_slowlog,
        ),
        ToolDef(
            name="get_elasticsearch_cluster_health",
            scope="diagnostics:read",
            description="Elasticsearch cluster health (status, shards).",
            input_schema=_schema(properties={}),
            handler=_h_elasticsearch_cluster_health,
        ),
        ToolDef(
            name="get_elasticsearch_nodes",
            scope="diagnostics:read",
            description="Elasticsearch nodes and their stats.",
            input_schema=_schema(properties={}),
            handler=_h_elasticsearch_nodes,
        ),
        ToolDef(
            name="get_elasticsearch_indices",
            scope="diagnostics:read",
            description="Elasticsearch indices with health and size.",
            input_schema=_schema(properties={}),
            handler=_h_elasticsearch_indices,
        ),
        ToolDef(
            name="get_mongo_health",
            scope="diagnostics:read",
            description="MongoDB server health (up, connections).",
            input_schema=_schema(properties={}),
            handler=_h_mongo_health,
        ),
        ToolDef(
            name="get_mongo_replset",
            scope="diagnostics:read",
            description="MongoDB replica set status (primary, members).",
            input_schema=_schema(properties={}),
            handler=_h_mongo_replset,
        ),
        ToolDef(
            name="get_mongo_current_ops",
            scope="diagnostics:read",
            description="MongoDB in-flight operations (currentOp).",
            input_schema=_schema(properties={}),
            handler=_h_mongo_current_ops,
        ),
        ToolDef(
            name="get_k8s_pod_status",
            scope="diagnostics:read",
            description="Kubernetes pod statuses in a namespace.",
            input_schema=_schema(
                "namespace", properties={"namespace": {"type": "string"}}
            ),
            handler=_h_k8s_pod_status,
        ),
        ToolDef(
            name="get_k8s_deployments",
            scope="diagnostics:read",
            description="Kubernetes deployments and rollout status in a "
            "namespace.",
            input_schema=_schema(
                "namespace", properties={"namespace": {"type": "string"}}
            ),
            handler=_h_k8s_deployments,
        ),
        ToolDef(
            name="get_k8s_events",
            scope="diagnostics:read",
            description="Recent Kubernetes events in a namespace.",
            input_schema=_schema(
                "namespace",
                properties={
                    "namespace": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_k8s_events,
        ),
        ToolDef(
            name="get_docker_containers",
            scope="diagnostics:read",
            description="Docker containers and their state.",
            input_schema=_schema(properties={}),
            handler=_h_docker_containers,
        ),
        ToolDef(
            name="get_docker_stats",
            scope="diagnostics:read",
            description="Docker container resource stats (CPU, memory).",
            input_schema=_schema(properties={}),
            handler=_h_docker_stats,
        ),
        ToolDef(
            name="read_docker_logs",
            scope="diagnostics:read",
            description="Tail a Docker container's logs.",
            input_schema=_schema(
                "container",
                properties={
                    "container": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            ),
            handler=_h_docker_logs,
        ),
        ToolDef(
            name="restart_websphere_app",
            scope="admin:write",
            description="PRIVILEGED: restart a WebSphere application "
            "(mutates state).",
            input_schema=_schema(
                "app_name", properties={"app_name": {"type": "string"}}
            ),
            handler=_h_restart_websphere_app,
        ),
        ToolDef(
            name="restart_weblogic_app",
            scope="admin:write",
            description="PRIVILEGED: restart a WebLogic application "
            "(mutates state).",
            input_schema=_schema(
                "app_name", properties={"app_name": {"type": "string"}}
            ),
            handler=_h_restart_weblogic_app,
        ),
        ToolDef(
            name="restart_jboss_deployment",
            scope="admin:write",
            description="PRIVILEGED: restart a JBoss/WildFly deployment "
            "(mutates state).",
            input_schema=_schema(
                "deployment", properties={"deployment": {"type": "string"}}
            ),
            handler=_h_restart_jboss_deployment,
        ),
        ToolDef(
            name="purge_rabbitmq_queue",
            scope="admin:write",
            description="PRIVILEGED: purge all messages in a RabbitMQ queue "
            "(mutates state).",
            input_schema=_schema(
                "vhost", "queue",
                properties={"vhost": {"type": "string"}, "queue": {"type": "string"}},
            ),
            handler=_h_purge_rabbitmq_queue,
        ),
        ToolDef(
            name="purge_artemis_queue",
            scope="admin:write",
            description="PRIVILEGED: purge all messages in an ActiveMQ "
            "Artemis queue (mutates state).",
            input_schema=_schema(
                "queue", properties={"queue": {"type": "string"}}
            ),
            handler=_h_purge_artemis_queue,
        ),
        ToolDef(
            name="purge_ems_queue",
            scope="admin:write",
            description="PRIVILEGED: purge all messages in a TIBCO EMS queue "
            "(mutates state).",
            input_schema=_schema(
                "queue", properties={"queue": {"type": "string"}}
            ),
            handler=_h_purge_ems_queue,
        ),
        ToolDef(
            name="reload_nginx",
            scope="admin:write",
            description="PRIVILEGED: reload the NGINX configuration "
            "(mutates state).",
            input_schema=_schema(properties={}),
            handler=_h_reload_nginx,
        ),
        ToolDef(
            name="reload_apache",
            scope="admin:write",
            description="PRIVILEGED: reload the Apache HTTPD configuration "
            "(mutates state).",
            input_schema=_schema(properties={}),
            handler=_h_reload_apache,
        ),
        ToolDef(
            name="set_haproxy_server_state",
            scope="admin:write",
            description="PRIVILEGED: set an HAProxy backend server's state "
            "(e.g. ready, maint, drain) (mutates state).",
            input_schema=_schema(
                "backend", "server", "state",
                properties={
                    "backend": {"type": "string"},
                    "server": {"type": "string"},
                    "state": {"type": "string"},
                },
            ),
            handler=_h_set_haproxy_server_state,
        ),
        ToolDef(
            name="terminate_postgres_backend",
            scope="admin:write",
            description="PRIVILEGED: terminate a PostgreSQL backend by PID "
            "(mutates state).",
            input_schema=_schema(
                "pid", properties={"pid": {"type": "integer"}}
            ),
            handler=_h_terminate_postgres_backend,
        ),
        ToolDef(
            name="kill_mysql_query",
            scope="admin:write",
            description="PRIVILEGED: kill a MySQL query/process by ID "
            "(mutates state).",
            input_schema=_schema(
                "process_id", properties={"process_id": {"type": "integer"}}
            ),
            handler=_h_kill_mysql_query,
        ),
        ToolDef(
            name="kill_oracle_session",
            scope="admin:write",
            description="PRIVILEGED: kill an Oracle session by SID and "
            "serial# (mutates state).",
            input_schema=_schema(
                "sid", "serial",
                properties={"sid": {"type": "integer"}, "serial": {"type": "integer"}},
            ),
            handler=_h_kill_oracle_session,
        ),
        ToolDef(
            name="kill_mongo_op",
            scope="admin:write",
            description="PRIVILEGED: kill a MongoDB operation by opid "
            "(mutates state).",
            input_schema=_schema(
                "opid", properties={"opid": {"type": "string"}}
            ),
            handler=_h_kill_mongo_op,
        ),
        ToolDef(
            name="restart_k8s_deployment",
            scope="admin:write",
            description="PRIVILEGED: restart a Kubernetes deployment "
            "(rollout restart) (mutates state).",
            input_schema=_schema(
                "namespace", "deployment",
                properties={
                    "namespace": {"type": "string"},
                    "deployment": {"type": "string"},
                },
            ),
            handler=_h_restart_k8s_deployment,
        ),
        ToolDef(
            name="restart_docker_container",
            scope="admin:write",
            description="PRIVILEGED: restart a Docker container "
            "(mutates state).",
            input_schema=_schema(
                "container", properties={"container": {"type": "string"}}
            ),
            handler=_h_restart_docker_container,
        ),
    ]
    if include is not None:
        wanted = set(include)
        defs = [d for d in defs if d.name in wanted]
    return defs


TOOL_NAMES = [d.name for d in build_tool_defs(None)]
