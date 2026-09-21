"""Tests for MCP tool authz: scopes, tokens, arg validation, audit of denials."""

import pytest

from audit import AuditLog
from mcp_server import Gateway, InProcessClient
from mcp_server.tools import PRIVILEGED_TOOLS, TOOL_NAMES
from sim import Estate

READ = "demo-read-token-0001"
ADMIN = "demo-admin-token-0002"


@pytest.fixture()
def stack(tmp_path):
    estate = Estate(seed=42)
    audit = AuditLog(tmp_path / "audit.jsonl")
    gateway = Gateway(estate, audit)
    return estate, audit, gateway


def test_all_tools_registered():
    # Deliberate change (phase 2: scenario breadth): 7 -> 17 tools, 1 -> 8
    # privileged. See docs/CHANGELOG.d/phase2-scenarios.md.
    # Deliberate change (real Linux connector): 17 -> 18 tools with
    # tail_log (diagnostics:read). See docs/CHANGELOG.d/phase2-connector.md.
    # Deliberate change (phase 4: connector depth): 18 -> 27 tools with 4
    # Kafka reads (consumer-group detail, topic detail, broker config,
    # broker health) and 5 Tomcat tools (heap, threadpool, apps, log,
    # restart_tomcat_app privileged).
    # Deliberate change (new connector surface): 27 -> 90 tools with 63
    # new connector tools (48 reads, 15 privileged).
    assert len(TOOL_NAMES) == 90
    assert set(PRIVILEGED_TOOLS) == {
        "restart_channel", "archive_logs", "restart_app", "restart_consumer",
        "renew_certificate", "update_queue_config", "start_listener",
        "quarantine_message", "restart_tomcat_app",
        "restart_websphere_app", "restart_weblogic_app",
        "restart_jboss_deployment", "purge_rabbitmq_queue",
        "purge_artemis_queue", "purge_ems_queue", "reload_nginx",
        "reload_apache", "set_haproxy_server_state",
        "terminate_postgres_backend", "kill_mysql_query",
        "kill_oracle_session", "kill_mongo_op", "restart_k8s_deployment",
        "restart_docker_container",
    }
    for name in ("get_queue_depth", "get_channel_status", "read_error_log",
                 "get_kafka_consumer_lag", "get_host_metrics", "get_config",
                 "restart_channel", "read_app_log", "get_listener_status",
                 "get_cert_status", "archive_logs", "restart_app",
                 "restart_consumer", "renew_certificate", "update_queue_config",
                 "start_listener", "quarantine_message", "tail_log",
                 "get_kafka_consumer_group_detail", "get_kafka_topic_detail",
                 "get_kafka_broker_config", "get_kafka_broker_health",
                 "get_tomcat_heap", "get_tomcat_threadpool", "get_tomcat_apps",
                 "read_tomcat_log", "restart_tomcat_app"):
        assert name in TOOL_NAMES


def test_read_tools_work_with_read_token(stack):
    estate, audit, gateway = stack
    client = InProcessClient(gateway, READ)
    d = client.call_tool("get_queue_depth", {"qmgr": "QMGR1", "queue": "PAYMENTS.IN"})
    assert d["depth"] == 1200
    s = client.call_tool("get_channel_status", {"qmgr": "QMGR1", "channel": "PAYMENTS.RCVR"})
    assert s["status"] == "RUNNING"
    assert isinstance(client.call_tool("read_error_log", {"qmgr": "QMGR1"}), list)
    lag = client.call_tool("get_kafka_consumer_lag",
                           {"topic": "payments-events", "group": "payments-consumers"})
    assert lag["lag"] >= 0
    m = client.call_tool("get_host_metrics", {"host": "app01"})
    assert "disk_pct" in m
    cfg = client.call_tool("get_config", {"qmgr": "QMGR1", "object_type": "channel",
                                          "name": "PAYMENTS.RCVR"})
    assert cfg  # non-empty config


def test_privileged_tool_denied_for_read_token(stack):
    estate, audit, gateway = stack
    client = InProcessClient(gateway, READ)
    with pytest.raises(PermissionError):
        client.call_tool("restart_channel", {"qmgr": "QMGR1", "channel": "PAYMENTS.RCVR"})
    events = [e["event"] for e in audit.entries()]
    assert "scope_denied" in events
    denied = [e for e in audit.entries() if e["event"] == "scope_denied"][0]
    assert denied["details"]["required_scope"] == "admin:write"


def test_privileged_tool_works_with_admin_token(stack):
    estate, audit, gateway = stack
    client = InProcessClient(gateway, ADMIN)
    estate.get_channel_status("QMGR1", "PAYMENTS.RCVR")  # baseline
    from sim import inject
    inject(estate, "channel_stopped")
    out = client.call_tool("restart_channel", {"qmgr": "QMGR1", "channel": "PAYMENTS.RCVR"})
    assert out["status"] == "RUNNING"
    assert out["previous_status"] == "STOPPED"


def test_invalid_token_denied_and_audited(stack):
    estate, audit, gateway = stack
    client = InProcessClient(gateway, "bogus-token")
    with pytest.raises(PermissionError):
        client.call_tool("get_queue_depth", {"qmgr": "QMGR1", "queue": "PAYMENTS.IN"})
    assert "auth_denied" in [e["event"] for e in audit.entries()]


def test_empty_token_denied(stack):
    estate, audit, gateway = stack
    with pytest.raises(PermissionError):
        gateway.call("get_queue_depth", {"qmgr": "QMGR1", "queue": "PAYMENTS.IN"}, "")


def test_raw_tokens_never_in_audit_log(stack, tmp_path):
    estate, audit, gateway = stack
    InProcessClient(gateway, READ).call_tool(
        "get_queue_depth", {"qmgr": "QMGR1", "queue": "PAYMENTS.IN"})
    try:
        InProcessClient(gateway, READ).call_tool(
            "restart_channel", {"qmgr": "QMGR1", "channel": "PAYMENTS.RCVR"})
    except PermissionError:
        pass
    raw = (tmp_path / "audit.jsonl").read_text()
    assert READ not in raw and ADMIN not in raw


def test_unknown_tool_and_bad_args(stack):
    estate, audit, gateway = stack
    client = InProcessClient(gateway, READ)
    with pytest.raises(Exception):
        client.call_tool("drop_database", {})
    with pytest.raises(ValueError):
        client.call_tool("get_queue_depth", {"qmgr": "QMGR1", "queue": "NOPE.Q"})
    with pytest.raises(ValueError):
        client.call_tool("get_queue_depth", {"qmgr": "QMGR1"})  # missing arg
