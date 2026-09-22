"""Contract tests for the connector framework (connectors/).

Every registered connector must satisfy the Connector ABC: its
capabilities() lists only real tool names it truly implements, the
read()/act() routing can never let a read call reach act-path code, and
unreachable targets raise ConnectorError (never raw driver exceptions).
No network, no real credentials, no drivers required.
"""

import json
import os
import sys

import pytest

from connectors import (
    CONNECTORS,
    Connector,
    ConnectorError,
    ConnectorSpec,
)
from mcp_server.tools import PRIVILEGED_TOOLS, TOOL_NAMES


def _build(name):
    """Construct each registered connector without network or secrets."""
    if name == "ibmmq":
        from connectors.ibmmq import IBMQConnector
        return IBMQConnector(
            qmgr="QM1", channel="SVRCONN.CH", host="mq01.example",
            credential_provider=lambda: ("rca_reader", "REDACTED"),
        )
    if name == "kafka":
        from connectors.kafka import KafkaConnector
        return KafkaConnector(bootstrap_servers="kafka01.example:9092")
    if name == "tomcat":
        from connectors.tomcat import TomcatConnector
        return TomcatConnector(host="tomcat01.example", port=8080)
    if name == "linux-host":
        from connectors.linux_host import LinuxHostConnector
        return LinuxHostConnector()
    if name == "websphere":
        from connectors.websphere import WebSphereConnector
        return WebSphereConnector(host="probe.invalid")
    if name == "weblogic":
        from connectors.weblogic import WebLogicConnector
        return WebLogicConnector(host="probe.invalid")
    if name == "jboss":
        from connectors.jboss import JBossConnector
        return JBossConnector(host="probe.invalid")
    if name == "rabbitmq":
        from connectors.rabbitmq import RabbitMQConnector
        return RabbitMQConnector(host="probe.invalid")
    if name == "artemis":
        from connectors.artemis import ArtemisConnector
        return ArtemisConnector(host="probe.invalid")
    if name == "tibco_ems":
        from connectors.tibco_ems import TibcoEmsConnector
        return TibcoEmsConnector(host="probe.invalid")
    if name == "nginx":
        from connectors.nginx import NginxConnector
        return NginxConnector(host="probe.invalid")
    if name == "apache":
        from connectors.apache import ApacheConnector
        return ApacheConnector(host="probe.invalid")
    if name == "haproxy":
        from connectors.haproxy import HAProxyConnector
        return HAProxyConnector(host="probe.invalid")
    if name == "postgres":
        from connectors.postgres import PostgresConnector
        return PostgresConnector(host="probe.invalid")
    if name == "mysql":
        from connectors.mysql import MySQLConnector
        return MySQLConnector(host="probe.invalid")
    if name == "oracle_db":
        from connectors.oracle_db import OracleDbConnector
        return OracleDbConnector(host="probe.invalid")
    if name == "redis":
        from connectors.redis import RedisConnector
        return RedisConnector(host="probe.invalid")
    if name == "elasticsearch":
        from connectors.elasticsearch import ElasticsearchConnector
        return ElasticsearchConnector(host="probe.invalid")
    if name == "mongodb":
        from connectors.mongodb import MongoDbConnector
        return MongoDbConnector(host="probe.invalid")
    if name == "kubernetes":
        from connectors.kubernetes import KubernetesConnector
        return KubernetesConnector()
    if name == "docker":
        from connectors.docker import DockerConnector
        # Deliberately unreachable socket path, mirroring the probe.invalid
        # convention used for the host-based connectors above: the default
        # /var/run/docker.sock exists on Docker hosts (e.g. CI runners),
        # which would make connect() succeed and break the unreachable-target
        # contract this fixture is built to exercise.
        return DockerConnector(socket_path="/nonexistent/docker.sock")
    raise AssertionError(f"unknown connector {name}")


@pytest.fixture(params=sorted(CONNECTORS))
def connector(request):
    return request.param, _build(request.param)


# ------------------------------------------------- registration & spec


def test_registry_covers_known_connectors():
    assert set(CONNECTORS) == {
        "ibmmq", "kafka", "linux-host", "tomcat",
        "websphere", "weblogic", "jboss",
        "rabbitmq", "artemis", "tibco_ems",
        "nginx", "apache", "haproxy",
        "postgres", "mysql", "oracle_db",
        "redis", "elasticsearch", "mongodb",
        "kubernetes", "docker",
    }


def test_registry_entries_are_spec_plus_factory(connector):
    name, _ = connector
    spec, factory = CONNECTORS[name]
    assert isinstance(spec, ConnectorSpec)
    assert spec.name == name
    assert issubclass(factory, Connector)


def test_spec_names_are_sane_and_credential_refs_hold_no_secrets(connector):
    name, _ = connector
    spec, _ = CONNECTORS[name]
    assert spec.display_name and spec.description
    for ref in spec.credential_refs:
        assert ref == ref.strip() and " " not in ref
        assert "password_value" not in ref.lower()
    # required config keys are identifiers, not secret material
    for key in spec.required_config + spec.optional_config:
        assert key and " " not in key


def test_instances_satisfy_abc_and_contract(connector):
    _, inst = connector
    assert isinstance(inst, Connector)
    inst.check_contract()


# ------------------------------------------------- capabilities


def test_capabilities_subset_of_tool_names(connector):
    _, inst = connector
    caps = set(inst.capabilities())
    assert caps, "capabilities() must not be empty"
    assert caps <= set(TOOL_NAMES), f"unknown tools: {caps - set(TOOL_NAMES)}"


def test_capabilities_only_list_implemented_tools(connector):
    _, inst = connector
    for tool in inst.capabilities():
        assert callable(getattr(inst, tool, None)), (
            f"capability {tool!r} has no implementation"
        )


def test_expected_capabilities_per_connector():
    assert set(_build("ibmmq").capabilities()) == {
        "get_queue_depth", "get_channel_status", "read_error_log",
        "get_listener_status", "get_cert_status", "get_config",
        "restart_channel", "update_queue_config", "start_listener",
        "quarantine_message",
    }
    assert set(_build("kafka").capabilities()) == {
        "get_kafka_consumer_lag", "get_kafka_consumer_group_detail",
        "get_kafka_topic_detail", "get_kafka_broker_config",
        "get_kafka_broker_health", "restart_consumer",
    }
    assert set(_build("tomcat").capabilities()) == {
        "get_tomcat_heap", "get_tomcat_threadpool", "get_tomcat_apps",
        "read_tomcat_log", "restart_tomcat_app",
    }
    assert set(_build("linux-host").capabilities()) == {
        "get_host_metrics", "tail_log",
    }


# ------------------------------------------------- read/act routing


def test_read_routing_matches_privileged_set(connector):
    _, inst = connector
    assert inst._read_tools == set(inst.capabilities()) - PRIVILEGED_TOOLS
    assert inst._act_tools == set(inst.capabilities()) & PRIVILEGED_TOOLS
    assert inst._read_tools.isdisjoint(PRIVILEGED_TOOLS)


def test_read_rejects_privileged_and_unknown_names(connector):
    _, inst = connector
    for name in list(PRIVILEGED_TOOLS) + ["no_such_tool"]:
        with pytest.raises(ConnectorError):
            inst.read(name, {})


def test_act_rejects_read_only_and_unknown_names(connector):
    _, inst = connector
    read_tools = set(TOOL_NAMES) - PRIVILEGED_TOOLS
    for name in list(read_tools) + ["no_such_tool"]:
        with pytest.raises(ConnectorError):
            inst.act(name, {})


def test_read_path_never_invokes_act_path_code(connector, monkeypatch):
    """Even if an act method is spied on, read() must not reach it."""
    name, inst = connector
    called = []
    for tool in inst._act_tools:
        monkeypatch.setattr(
            inst, tool,
            lambda *a, _t=tool, **k: called.append(_t) or {"spy": True},
        )
    for tool in inst._act_tools:
        with pytest.raises(ConnectorError):
            inst.read(tool, {})
    assert called == [], f"read() reached act-path code: {called}"


def test_act_path_never_invokes_read_path_code(connector, monkeypatch):
    name, inst = connector
    called = []
    for tool in inst._read_tools:
        monkeypatch.setattr(
            inst, tool,
            lambda *a, _t=tool, **k: called.append(_t) or {"spy": True},
        )
    for tool in inst._read_tools:
        with pytest.raises(ConnectorError):
            inst.act(tool, {})
    assert called == [], f"act() reached read-path code: {called}"


# ------------------------------------------------- credential hygiene


def test_repr_contains_no_secret_references(connector):
    _, inst = connector
    text = repr(inst)
    assert "REDACTED" not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()


def test_no_raw_password_constructor_argument(connector):
    """Constructors take credential references (providers / env-var NAMES),
    never a raw secret value. ``password_env``-style names are references
    and are allowed; bare ``password``/``secret``/``token`` params are not.
    """
    name, _ = connector
    _, factory = CONNECTORS[name]
    import inspect
    params = inspect.signature(factory.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned, (
            f"{name}.__init__ takes a raw secret argument {pname!r}"
        )


# ------------------------------------------------- unreachable targets


# Driver module each driver-based connector lazily imports in connect().
# Connectors absent from this map are driverless by design (stdlib
# urllib, local CLI, or unix-socket transports).
_DRIVER_MODULES = {
    "ibmmq": "pymqi",
    "kafka": "kafka",
    "postgres": "psycopg",
    "mysql": "pymysql",
    "oracle_db": "oracledb",
    "redis": "redis",
    "mongodb": "pymongo",
    "kubernetes": "kubernetes",
}


def test_connect_without_driver_raises_connector_error(connector, monkeypatch):
    """With no driver installed, connect() must raise ConnectorError."""
    name, inst = connector
    if name == "linux-host":
        inst.connect()  # no driver needed; must not raise
        inst.close()
        return
    if name not in _DRIVER_MODULES:
        # No third-party driver (stdlib urllib / local CLI / unix-socket
        # transports); connect() must still raise ConnectorError -- here
        # via missing credentials, a missing CLI binary, or an
        # unreachable probe target.
        with pytest.raises(ConnectorError):
            inst.connect()
        return
    driver = _DRIVER_MODULES[name]
    monkeypatch.delitem(sys.modules, driver, raising=False)
    with pytest.raises(ConnectorError) as excinfo:
        inst.connect()
    assert driver in str(excinfo.value)


def test_ibmmq_connect_unreachable_target(monkeypatch):
    """Driver present but target unreachable -> ConnectorError, no secrets."""
    from connectors.ibmmq import IBMQConnector

    secret = "s3cr3t-unreachable-probe"
    conn = IBMQConnector(
        qmgr="QM1", channel="CH", host="unreachable.invalid",
        credential_provider=lambda: ("rca_reader", secret),
    )

    import types
    fake = types.ModuleType("pymqi")

    class Boom(Exception):
        pass

    class FakeQM:
        def connect(self, *a, **k):
            raise Boom("connection refused")

    fake.QueueManager = lambda name: FakeQM()  # noqa: E731
    fake.MQMIError = Boom
    fake.CMQC = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "pymqi", fake)

    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert secret not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()


# ------------------------------------------------- JSON-serializability guard


def test_connector_results_must_be_json_serializable(connector):
    """Smoke: any dict/list a connector returns must survive json.dumps.

    (Concrete shape tests live in the per-connector test modules.)
    """
    _, inst = connector
    if isinstance(inst, Connector):
        json.dumps({"name": inst.name, "caps": sorted(inst.capabilities())})


def test_env_placeholders_stay_redacted(monkeypatch):
    """The suite itself must never need a real secret in the environment."""
    for var in ("MQ_READ_USER", "MQ_READ_PASSWORD", "KAFKA_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    from connectors.ibmmq import IBMQConnector
    conn = IBMQConnector(qmgr="QM1", channel="CH", host="mq01.example")
    with pytest.raises(ConnectorError):
        # No provider and no env -> clean refusal, no secret involved.
        conn._resolve_read_credentials()
    assert os.environ.get("MQ_READ_PASSWORD") is None
