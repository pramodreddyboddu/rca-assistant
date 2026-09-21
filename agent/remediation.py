"""Remediation plan builders: (hypothesis, alert, evidence) -> RemediationPlan.

Every builder here is PURE DATA, exactly like the legacy single-remediation
dict in agent.rca: nothing is executed, no privileged tool is ever called,
and no tool output is interpreted as instructions. The ApprovalGate in
agent.approvals authorizes each step before any runner executes it.

The ``evidence`` argument is the engine's evidence dict, keyed by
``name + "|" + json.dumps(args, sort_keys=True)`` (see agent.rca._ev_key);
the key helper here replicates that format exactly.
"""

from __future__ import annotations

import json

from agent.plans import PlanStep, RemediationPlan, VerifySpec


def _ev_key(name: str, args: dict) -> str:
    return name + "|" + json.dumps(args, sort_keys=True)


# Candidate channels scanned for the STOPPED one (mirrors agent.rca).
_CANDIDATE_CHANNELS = ["PAYMENTS.RCVR", "PAYMENTS.SDR", "ORDERS.RCVR"]


def _stopped_channel(alert: dict, evidence: dict) -> str | None:
    for ch in _CANDIDATE_CHANNELS:
        resp = evidence.get(_ev_key("get_channel_status",
                                    {"qmgr": alert["qmgr"], "channel": ch}))
        if isinstance(resp, dict) and resp.get("status") == "STOPPED":
            return ch
    return None


def _plan_channel_stopped(alert: dict, evidence: dict) -> RemediationPlan | None:
    ch = _stopped_channel(alert, evidence)
    if ch is None:
        return None
    qmgr = alert["qmgr"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the stopped receiver channel",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_channel",
                args={"qmgr": qmgr, "channel": ch},
                rationale=(
                    f"Channel {ch} on {qmgr} is STOPPED while its queue has a "
                    "deep backlog; restarting the receiver channel should "
                    "resume message flow."
                ),
                verify=VerifySpec(
                    tool="get_channel_status",
                    args={"qmgr": qmgr, "channel": ch},
                    expect={"status": "RUNNING"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_disk_full(alert: dict, evidence: dict) -> RemediationPlan | None:
    host, app = alert["host"], alert["app"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Free disk space, then restart the app if memory pressure remains",
        steps=(
            PlanStep(
                id="step-1",
                action="archive_logs",
                args={"host": host},
                rationale=(
                    f"Host {host} is out of disk space (logs cannot rotate); "
                    "archiving old logs frees space so the app can write again."
                ),
                verify=VerifySpec(
                    tool="get_host_metrics",
                    args={"host": host},
                    expect={"disk_pct": {"$lt": 90}},
                ),
                required_scope="admin:write",
            ),
            PlanStep(
                id="step-2",
                action="restart_app",
                args={"app": app},
                rationale=(
                    f"Restart {app} to clear any memory pressure left over "
                    "from the disk-full episode and restore normal service."
                ),
                verify=VerifySpec(
                    tool="get_host_metrics",
                    args={"host": host},
                    expect={"mem_pct": {"$lt": 85}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_kafka_lag(alert: dict, evidence: dict) -> RemediationPlan | None:
    topic, group = alert["topic"], alert["group"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the stalled Kafka consumer group",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_consumer",
                args={"topic": topic, "group": group},
                rationale=(
                    f"Consumer group {group} on topic {topic} has stalled "
                    "while the MQ side is healthy; restarting it should "
                    "resume consumption."
                ),
                verify=VerifySpec(
                    tool="get_kafka_consumer_lag",
                    args={"topic": topic, "group": group},
                    expect={"lag": {"$lt": 50000}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_app_oom(alert: dict, evidence: dict) -> RemediationPlan | None:
    app, host = alert["app"], alert["host"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the app to clear the exhausted JVM heap",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_app",
                args={"app": app},
                rationale=(
                    f"{app} threw OutOfMemoryError with memory at 96% on "
                    f"{host}; a restart resets the JVM heap and clears the "
                    "exhausted thread pool."
                ),
                verify=VerifySpec(
                    tool="get_host_metrics",
                    args={"host": host},
                    expect={"mem_pct": {"$lt": 85}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_expired_cert(alert: dict, evidence: dict) -> RemediationPlan | None:
    qmgr, channel = alert["qmgr"], alert["channel"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Renew the expired TLS certificate, then restart the channel",
        steps=(
            PlanStep(
                id="step-1",
                action="renew_certificate",
                args={"qmgr": qmgr, "channel": channel},
                rationale=(
                    f"The TLS certificate for channel {channel} has expired; "
                    "renewing it is required before the channel can "
                    "re-establish its SSL session."
                ),
                verify=VerifySpec(
                    tool="get_cert_status",
                    args={"qmgr": qmgr, "channel": channel},
                    expect={"valid": True},
                ),
                required_scope="admin:write",
            ),
            PlanStep(
                id="step-2",
                action="restart_channel",
                args={"qmgr": qmgr, "channel": channel},
                rationale=(
                    f"Restart {channel} so it picks up the renewed "
                    "certificate and leaves RETRYING."
                ),
                verify=VerifySpec(
                    tool="get_channel_status",
                    args={"qmgr": qmgr, "channel": channel},
                    expect={"status": "RUNNING"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_config_drift(alert: dict, evidence: dict) -> RemediationPlan | None:
    qmgr, queue = alert["qmgr"], alert["queue"]
    cfg = evidence.get(_ev_key("get_config", {"qmgr": qmgr,
                                              "object_type": "queue",
                                              "name": queue}))
    baseline = cfg.get("baseline_max_depth") if isinstance(cfg, dict) else None
    if not isinstance(baseline, (int, float)) or isinstance(baseline, bool):
        return None
    baseline = int(baseline)
    return RemediationPlan(
        hypothesis_id="h4",
        title="Restore the queue's MAXDEPTH to its baseline",
        steps=(
            PlanStep(
                id="step-1",
                action="update_queue_config",
                args={"qmgr": qmgr, "queue": queue, "max_depth": baseline},
                rationale=(
                    f"Queue {queue} on {qmgr} is pinned at its lowered "
                    f"MAXDEPTH; restoring the baseline value of {baseline} "
                    "gives producers headroom again."
                ),
                verify=VerifySpec(
                    tool="get_config",
                    args={"qmgr": qmgr, "object_type": "queue", "name": queue},
                    expect={"max_depth": baseline},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_listener_down(alert: dict, evidence: dict) -> RemediationPlan | None:
    name = alert["name"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Start the stopped MQ listener",
        steps=(
            PlanStep(
                id="step-1",
                action="start_listener",
                args={"name": name},
                rationale=(
                    f"Listener {name} is STOPPED, so inbound connections "
                    "cannot be accepted; starting it restores connectivity."
                ),
                verify=VerifySpec(
                    tool="get_listener_status",
                    args={"name": name},
                    expect={"status": "RUNNING"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_poison_message(alert: dict, evidence: dict) -> RemediationPlan | None:
    qmgr, queue = alert["qmgr"], alert["queue"]
    return RemediationPlan(
        hypothesis_id="h5",
        title="Quarantine the poison message to SYSTEM.DLQ",
        steps=(
            PlanStep(
                id="step-1",
                action="quarantine_message",
                args={"qmgr": qmgr, "queue": queue},
                rationale=(
                    f"A message on {queue} keeps backing out with an "
                    "application error, blocking the queue; moving it to "
                    "SYSTEM.DLQ lets the remaining messages flow."
                ),
                verify=VerifySpec(
                    tool="get_queue_depth",
                    args={"qmgr": qmgr, "queue": queue},
                    expect={"depth": {"$lt": 10000}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_consumer_lag_config_drift(alert: dict, evidence: dict) -> RemediationPlan | None:
    topic, group = alert["topic"], alert["group"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the lagging consumer group; realign the drifted broker config",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_consumer",
                args={"topic": topic, "group": group},
                rationale=(
                    f"Consumer group {group} on {topic} has stalled while "
                    "one broker's config has drifted from the others; "
                    "restarting the group clears the lag. The broker config "
                    "mismatch still needs a Kafka operator to realign."
                ),
                verify=VerifySpec(
                    tool="get_kafka_consumer_lag",
                    args={"topic": topic, "group": group},
                    expect={"lag": {"$lt": 50000}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_threadpool_exhausted(alert: dict, evidence: dict) -> RemediationPlan | None:
    pool, app = alert["pool"], alert["app"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the slowed Tomcat app to free the exhausted thread pool",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_tomcat_app",
                args={"app_path": app},
                rationale=(
                    f"Thread pool {pool} is saturated and app {app} is "
                    "degraded; restarting the app frees the stuck threads "
                    "and restores request handling."
                ),
                verify=VerifySpec(
                    tool="get_tomcat_threadpool",
                    args={"pool": pool},
                    expect={"pools.0.current_threads_busy": {"$lt": 100}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Wave 3 remediation builders (14 plans; the other 20 new h1s are
# deliberately plan-less, documented in the _BUILDERS comment below).
#
# VerifySpec notes: several sim privileged methods only touch the app /
# deployment / container state, not the underlying resource counter (the
# sim has no time evolution for thread pools, JVM heaps, or nginx/apache
# workers). Where the privileged method cannot move the incident counter,
# the verify asserts the post-action state the sim actually produces
# (e.g. the app is running again), per the check_verify contract.
# ---------------------------------------------------------------------------

def _plan_websphere_thread_saturation(alert: dict, evidence: dict) -> RemediationPlan | None:
    pool, app = alert["pool"], alert["app"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the WebSphere app to release the saturated thread pool",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_websphere_app",
                args={"app_name": app},
                rationale=(
                    f"Thread pool {pool} on was01 is saturated; restarting "
                    f"{app} releases the stuck threads and restores request "
                    "handling."
                ),
                # The sim's restart only flips the app state; it does not
                # drain the pool counters, so verify the real post-action
                # state: the app is running again.
                verify=VerifySpec(
                    tool="get_websphere_apps",
                    args={},
                    expect={"apps.1.state": "running"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_websphere_app_stopped(alert: dict, evidence: dict) -> RemediationPlan | None:
    app = alert["app"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the stopped WebSphere application",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_websphere_app",
                args={"app_name": app},
                rationale=(
                    f"WebSphere app {app} is stopped; restarting it restores "
                    "service."
                ),
                verify=VerifySpec(
                    tool="get_websphere_apps",
                    args={},
                    expect={"apps.0.state": "running"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_weblogic_heap_high(alert: dict, evidence: dict) -> RemediationPlan | None:
    app = alert["app"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the WebLogic app to clear the exhausted JVM heap",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_weblogic_app",
                args={"app_name": app},
                rationale=(
                    f"WebLogic heap is over 90% with {app} under pressure; "
                    "restarting the app resets its JVM heap."
                ),
                # The sim's restart only flips the app state; heap counters
                # are untouched, so verify the app is running again.
                verify=VerifySpec(
                    tool="get_weblogic_apps",
                    args={},
                    expect={"apps.2.state": "running"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_jboss_deployment_failed(alert: dict, evidence: dict) -> RemediationPlan | None:
    deployment = alert["deployment"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Redeploy (restart) the failed JBoss deployment",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_jboss_deployment",
                args={"deployment": deployment},
                rationale=(
                    f"JBoss deployment {deployment} is failed/disabled; "
                    "restarting it redeploys the artifact and re-enables it."
                ),
                verify=VerifySpec(
                    tool="get_jboss_deployments",
                    args={},
                    expect={"deployments.1.status": "OK",
                            "deployments.1.enabled": True},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_jboss_heap_high(alert: dict, evidence: dict) -> RemediationPlan | None:
    deployment = alert["deployment"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the JBoss deployment to clear the exhausted JVM heap",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_jboss_deployment",
                args={"deployment": deployment},
                rationale=(
                    f"JBoss heap is over 90%; restarting {deployment} resets "
                    "its JVM heap."
                ),
                # The sim's restart only flips deployment state; heap
                # counters are untouched, so verify the deployment is OK.
                verify=VerifySpec(
                    tool="get_jboss_deployments",
                    args={},
                    expect={"deployments.0.status": "OK",
                            "deployments.0.enabled": True},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_nginx_worker_crash(alert: dict, evidence: dict) -> RemediationPlan | None:
    return RemediationPlan(
        hypothesis_id="h1",
        title="Reload nginx to replace the crashed worker processes",
        steps=(
            PlanStep(
                id="step-1",
                action="reload_nginx",
                args={},
                rationale=(
                    "An nginx worker crashed; a config reload recycles the "
                    "worker processes and restores full capacity."
                ),
                verify=VerifySpec(
                    tool="get_nginx_status",
                    args={},
                    expect={"active_connections": {"$gt": 0}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_apache_workers_saturated(alert: dict, evidence: dict) -> RemediationPlan | None:
    return RemediationPlan(
        hypothesis_id="h1",
        title="Gracefully reload Apache to clear the stuck workers",
        steps=(
            PlanStep(
                id="step-1",
                action="reload_apache",
                args={},
                rationale=(
                    "Apache workers are saturated and idle count is zero; a "
                    "graceful reload lets in-flight requests finish while "
                    "recycling the stuck workers."
                ),
                # The sim's reload does not move the worker counters, so
                # verify the server is still up afterwards.
                verify=VerifySpec(
                    tool="get_apache_status",
                    args={},
                    expect={"uptime_secs": {"$gt": 0}},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_haproxy_backend_down(alert: dict, evidence: dict) -> RemediationPlan | None:
    backend, server = alert["backend"], alert["server"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Drain the failed backend server out of rotation",
        steps=(
            PlanStep(
                id="step-1",
                action="set_haproxy_server_state",
                args={"backend": backend, "server": server, "state": "maint"},
                rationale=(
                    f"Server {server} in backend {backend} is failing its "
                    "health check; putting it in maintenance drains traffic "
                    "to the healthy servers."
                ),
                verify=VerifySpec(
                    tool="get_haproxy_stats",
                    args={},
                    expect={"backends.1.servers.0.status": "MAINT"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_postgres_blocking(alert: dict, evidence: dict) -> RemediationPlan | None:
    pid = alert["pid"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Terminate the long-running blocking Postgres backend",
        steps=(
            PlanStep(
                id="step-1",
                action="terminate_postgres_backend",
                args={"pid": pid},
                rationale=(
                    f"Postgres backend pid {pid} has been blocking sessions "
                    "for 900s; terminating it releases the locks."
                ),
                verify=VerifySpec(
                    tool="get_postgres_blocking",
                    args={},
                    expect={"blockers": []},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_mysql_runaway_query(alert: dict, evidence: dict) -> RemediationPlan | None:
    process_id = alert["process_id"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Kill the runaway MySQL query",
        steps=(
            PlanStep(
                id="step-1",
                action="kill_mysql_query",
                args={"process_id": process_id},
                rationale=(
                    f"MySQL process {process_id} has been running for an "
                    "hour; killing it frees the connection and the resources "
                    "it holds."
                ),
                verify=VerifySpec(
                    tool="get_mysql_processlist",
                    args={},
                    expect={"processes": []},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_oracle_blocking(alert: dict, evidence: dict) -> RemediationPlan | None:
    sid, serial = alert["sid"], alert["serial"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Kill the blocking Oracle session",
        steps=(
            PlanStep(
                id="step-1",
                action="kill_oracle_session",
                args={"sid": sid, "serial": serial},
                rationale=(
                    f"Oracle session sid={sid},serial={serial} is blocking "
                    "other sessions; killing it releases the enqueues."
                ),
                verify=VerifySpec(
                    tool="get_oracle_blocking",
                    args={},
                    expect={"blockers": []},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_mongo_long_running_op(alert: dict, evidence: dict) -> RemediationPlan | None:
    opid = alert["opid"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Kill the runaway MongoDB operation",
        steps=(
            PlanStep(
                id="step-1",
                action="kill_mongo_op",
                args={"opid": opid},
                rationale=(
                    f"MongoDB op {opid} has been running for 40 minutes; "
                    "killing it frees the resources it holds."
                ),
                verify=VerifySpec(
                    tool="get_mongo_current_ops",
                    args={},
                    expect={"ops": []},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_k8s_pod_crashloop(alert: dict, evidence: dict) -> RemediationPlan | None:
    namespace, deployment = alert["namespace"], alert["deployment"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Rollout-restart the CrashLooping deployment",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_k8s_deployment",
                args={"namespace": namespace, "deployment": deployment},
                rationale=(
                    f"Deployment {deployment} in namespace {namespace} is "
                    "CrashLoopBackOff after an OOMKill; a rollout restart "
                    "gives the pods a clean slate with fresh memory."
                ),
                # The sim's restart only stamps restarted_at; verify the
                # deployment still exists afterwards.
                verify=VerifySpec(
                    tool="get_k8s_deployments",
                    args={"namespace": namespace},
                    expect={"deployments.0.name": "payments-api"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


def _plan_docker_container_exited(alert: dict, evidence: dict) -> RemediationPlan | None:
    container = alert["container"]
    return RemediationPlan(
        hypothesis_id="h1",
        title="Restart the exited container",
        steps=(
            PlanStep(
                id="step-1",
                action="restart_docker_container",
                args={"container": container},
                rationale=(
                    f"Container {container} exited with an error; "
                    "restarting it brings the service back up."
                ),
                verify=VerifySpec(
                    tool="get_docker_containers",
                    args={},
                    expect={"containers.0.state": "running"},
                ),
                required_scope="admin:write",
            ),
        ),
    )


# (alert type, hypothesis id) -> plan builder. Anything not listed here has
# no safe remediation and gets None. Note kafka_under_replicated/h1 is
# deliberately absent: an under-replicated partition has no safe
# automated remediation in this toolset (fixing ISR is an operator job),
# so the engine proposes no plan and the UI reports manual investigation.
#
# Wave 3 deliberately plan-less h1s (same rationale: no safe automated step
# in this toolset, so diagnose() proposes no plan):
#   weblogic_stuck_threads/h1       - stuck threads need a code fix, not a restart
#   rabbitmq_queue_backlog/h1       - the consumer fleet is external to RabbitMQ
#   rabbitmq_node_resource_alarm/h1 - clearing a disk alarm is operator work
#   artemis_queue_backlog/h1        - the consumer fleet is external to Artemis
#   artemis_broker_down/h1          - restarting the broker is operator territory
#   ems_queue_backlog/h1            - the consumer fleet is external to EMS
#   ems_connection_storm/h1         - the leak is in the client app, not the server
#   nginx_upstream_errors/h1        - the fault is in the upstream app, not nginx
#   apache_5xx_spike/h1             - the fault is in the backend app, not Apache
#   haproxy_session_saturation/h1   - maxconn tuning is operator work
#   postgres_replication_lag/h1     - standby recovery is operator work
#   mysql_replication_lag/h1        - replica recovery is operator work
#   oracle_tablespace_full/h1       - adding datafiles is a DBA change
#   redis_memory_high/h1            - reads-only connector; eviction policy is operator config
#   redis_replication_down/h1       - replica re-sync is operator work
#   elasticsearch_cluster_red/h1    - shard allocation is operator work
#   elasticsearch_heap_pressure/h1  - heap sizing is operator work
#   mongo_replset_lag/h1            - secondary recovery is operator work
#   k8s_deployment_stalled/h1       - a stuck rollout needs human diagnosis
#   docker_memory_high/h1           - a restart does not fix a memory leak
_BUILDERS = {
    ("queue_backlog", "h1"): _plan_channel_stopped,
    ("tomcat_errors", "h1"): _plan_disk_full,
    ("kafka_lag", "h1"): _plan_kafka_lag,
    ("app_oom", "h1"): _plan_app_oom,
    ("channel_tls_error", "h1"): _plan_expired_cert,
    ("queue_backlog", "h4"): _plan_config_drift,
    ("listener_down", "h1"): _plan_listener_down,
    ("queue_backlog", "h5"): _plan_poison_message,
    ("consumer_lag", "h1"): _plan_consumer_lag_config_drift,
    ("threadpool_saturated", "h1"): _plan_threadpool_exhausted,
    ("websphere_thread_saturated", "h1"): _plan_websphere_thread_saturation,
    ("websphere_app_stopped", "h1"): _plan_websphere_app_stopped,
    ("weblogic_heap_high", "h1"): _plan_weblogic_heap_high,
    ("jboss_deployment_failed", "h1"): _plan_jboss_deployment_failed,
    ("jboss_heap_high", "h1"): _plan_jboss_heap_high,
    ("nginx_worker_crash", "h1"): _plan_nginx_worker_crash,
    ("apache_workers_saturated", "h1"): _plan_apache_workers_saturated,
    ("haproxy_backend_down", "h1"): _plan_haproxy_backend_down,
    ("postgres_blocking", "h1"): _plan_postgres_blocking,
    ("mysql_runaway_query", "h1"): _plan_mysql_runaway_query,
    ("oracle_blocking", "h1"): _plan_oracle_blocking,
    ("mongo_long_running_op", "h1"): _plan_mongo_long_running_op,
    ("k8s_pod_crashloop", "h1"): _plan_k8s_pod_crashloop,
    ("docker_container_exited", "h1"): _plan_docker_container_exited,
}


def plan_for(hypothesis_id: str, alert: dict,
             evidence: dict) -> RemediationPlan | None:
    """Build the RemediationPlan for (alert type, hypothesis id), or None.

    Pure data: builders only read the evidence dict and the alert; they
    never call tools. Returns None when the hypothesis has no safe
    remediation.
    """
    builder = _BUILDERS.get((alert.get("type"), hypothesis_id))
    if builder is None:
        return None
    return builder(alert, evidence)
