"""Tests for the real JBoss EAP / WildFly connector (connectors/jboss.py).

No live server is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``post(url, payload, auth)``) is replaced with a ``FakeTransport``
returning canned WildFly management-API ``result`` payloads.
Placeholders like "REDACTED-fake" stand in for secrets, and hygiene
tests assert they never leak into repr, exceptions, or logs.

The ``connectors.jboss`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown, so running this file together
with tests/test_connector_contract.py does not change the registry that
the contract tests pin.
"""

import importlib
import json
import urllib.error

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

SERVER_LOG_LINES = [
    "14:33:20,123 INFO [org.jboss.as] (Controller Boot Thread) "
    "WFLYSRV0025: WildFly Full 23.0.0.Final (WildFly Core 15.0.0.Final) "
    "started in 4231ms",
    "14:34:01,456 ERROR [org.jboss.as.server.deployment] (MSC service "
    "thread 1-6) WFLYSRV0021: Deploy of deployment \"orders.war\" was rolled "
    "back",
    "this line has no timestamp or level prefix",
]


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def jboss_mod():
    """Import connectors.jboss lazily; deregister it afterwards.

    "jboss" is not part of the registry that
    tests/test_connector_contract.py pins, so the teardown removes the
    registration (mirroring tests/test_artemis.py) rather than keeping
    it like the pinned tomcat connector does.
    """
    had = "jboss" in CONNECTORS
    mod = importlib.import_module("connectors.jboss")
    yield mod
    if not had:
        CONNECTORS.pop("jboss", None)


@pytest.fixture(scope="module")
def JC(jboss_mod):
    return jboss_mod.JBossConnector


class FakeTransport:
    """Canned WildFly management-API responses for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, dict, str | None]] = []
        self.fail_with: Exception | None = None
        #: Flip to False to simulate a server without read-log-file.
        self.log_op_supported = True
        self.redeploy_outcome = "success"

    def _record(self, url, payload, auth):
        user = auth[0] if auth else None
        self.calls.append((payload.get("operation"), url, user))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the single boundary method the connector uses -----------------

    def post(self, url, payload, auth):
        self._record(url, payload, auth)
        op = payload.get("operation")
        address = payload.get("address", [])
        if op == "read-attribute" and payload.get("name") == "server-state":
            return "running"
        if op == "read-resource":
            if address == [{"core-service": "platform-mbean"},
                           {"type": "memory"}]:
                return {"heap-memory-usage": {
                    "init": 268435456, "used": 536870912,
                    "committed": 1073741824, "max": 2147483648,
                }}
            if len(address) == 2 and address[0] == {"subsystem": "io"}:
                worker = address[1].get("worker")
                if worker == "default":
                    return {"busy-task-thread-count": 7,
                            "core-pool-size": 10,
                            "max-pool-size": 128,
                            "queue-size": 3}
                if worker == "undertow-worker":
                    return {"busy-task-thread-count": 0,
                            "core-pool-size": 4,
                            "max-pool-size": 64,
                            "queue-size": 0}
        if op == "read-children-resources":
            child_type = payload.get("child-type")
            if child_type == "deployment" and address == []:
                return {
                    "orders.war": {"enabled": True, "status": "OK"},
                    "reports.ear": {"enabled": False, "status": "OK"},
                }
            if child_type == "worker" and address == [{"subsystem": "io"}]:
                return {"default": {}, "undertow-worker": {}}
        if op == "read-log-file":
            if not self.log_op_supported:
                raise ConnectorError(
                    "jboss: 'read-log-file' failed: WFLYCTL0030: No resource "
                    "definition is registered for address")
            return SERVER_LOG_LINES
        if op == "redeploy":
            if self.redeploy_outcome != "success":
                raise ConnectorError(
                    "jboss: 'redeploy' failed: deployment failed")
            return None
        raise AssertionError(f"FakeTransport: unexpected op {payload!r}")


@pytest.fixture
def conn(JC):
    """A connected connector backed by FakeTransport."""
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(jboss_mod):
    assert "jboss" in CONNECTORS
    spec, factory = CONNECTORS["jboss"]
    assert spec.name == "jboss"
    assert factory is jboss_mod.JBossConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_jboss_heap",
        "get_jboss_threadpool",
        "get_jboss_deployments",
        "read_jboss_log",
        "restart_jboss_deployment",
    }


def test_constructor_takes_no_raw_secret(JC):
    import inspect
    params = inspect.signature(JC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


def test_constructor_defaults(JC):
    c = JC(host="h")
    assert c._port == 9990
    assert c._mgmt_url == "http://h:9990/management"
    assert "password" not in repr(c).lower()


# ------------------------------------------------------------- connect


def test_connect_probes_server_state(JC):
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "read-attribute"
    assert fake.calls[0][1] == "http://jboss01.example:9990/management"
    assert fake.calls[0][2] == "rca_reader"  # read user, not admin


def test_connect_uses_env_credentials(JC, monkeypatch):
    monkeypatch.setenv("JBOSS_READ_USER", "env_reader")
    monkeypatch.setenv("JBOSS_READ_PASSWORD", "env-pass-fake")
    c = JC(host="jboss01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(JC):
    """A dead management endpoint must surface ConnectorError, never raw."""
    c = JC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_unexpected_server_state(JC):
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))

    class WeirdState(FakeTransport):
        def post(self, url, payload, auth):
            if payload.get("operation") == "read-attribute":
                return "banana"
            return super().post(url, payload, auth)

    c._transport = WeirdState()
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(JC):
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_jboss_heap()
    with pytest.raises(ConnectorError):
        c.get_jboss_deployments()


def test_empty_credential_provider_refuses(JC):
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- heap


def test_get_jboss_heap_shape_and_math(conn):
    heap = conn.get_jboss_heap()
    assert heap["host"] == "jboss01.example"
    assert heap["port"] == 9990
    assert heap["heap_init_bytes"] == 268435456
    assert heap["heap_used_bytes"] == 536870912
    assert heap["heap_max_bytes"] == 2147483648
    assert heap["heap_used_pct"] == 25.0  # 512MiB / 2GiB
    assert heap["heap_committed_bytes"] == 1073741824
    assert heap["ts"].endswith("Z")
    json.dumps(heap)  # JSON-serializable


# ------------------------------------------------------------- thread pools


def test_get_jboss_threadpool_reports_busy_threads(conn):
    result = conn.get_jboss_threadpool()
    pools = {p["name"]: p for p in result["pools"]}
    assert set(pools) == {"default", "undertow-worker"}
    default = pools["default"]
    assert default["current_threads_busy"] == 7
    assert default["current_thread_count"] == 10
    assert default["max_threads"] == 128
    assert default["busy_pct"] == round(7 / 128 * 100, 1)
    assert pools["undertow-worker"]["busy_pct"] == 0.0
    assert result["host"] == "jboss01.example"
    assert result["port"] == 9990
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_jboss_threadpool_filter_by_name(conn):
    result = conn.get_jboss_threadpool(pool="undertow-worker")
    assert [p["name"] for p in result["pools"]] == ["undertow-worker"]


def test_get_jboss_threadpool_unknown_pool(conn):
    with pytest.raises(ConnectorError):
        conn.get_jboss_threadpool(pool="no-such-worker")


# ------------------------------------------------------------- deployments


def test_get_jboss_deployments_shape(conn):
    result = conn.get_jboss_deployments()
    deps = {d["name"]: d for d in result["deployments"]}
    assert deps["orders.war"] == {
        "name": "orders.war", "enabled": True, "status": "OK"}
    assert deps["reports.ear"] == {
        "name": "reports.ear", "enabled": False, "status": "OK"}
    assert result["host"] == "jboss01.example"
    assert result["ts"].endswith("Z")
    json.dumps(result)


# ------------------------------------------------------------- log tailing


def test_read_jboss_log_parses_wildfly_lines(conn):
    entries = conn.read_jboss_log(limit=10)
    assert len(entries) == 3
    assert entries[0]["severity"] == "INFO"
    assert entries[0]["ts"] == "14:33:20,123"
    assert "started in 4231ms" in entries[0]["message"]
    assert entries[1]["severity"] == "ERROR"
    assert "orders.war" in entries[1]["message"]
    assert entries[2]["severity"] == "INFO"  # unparseable line kept
    assert entries[2]["ts"] is None
    json.dumps(entries)


def test_read_jboss_log_unknown_log_name(conn):
    with pytest.raises(ConnectorError):
        conn.read_jboss_log(log="gc")


def test_read_jboss_log_falls_back_to_co_located_file(JC, tmp_path):
    log_dir = tmp_path / "log"
    log_dir.mkdir()
    (log_dir / "server.log").write_text(
        "14:33:20,123 INFO [org.jboss.as] server started\n")
    c = JC(host="jboss01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.log_op_supported = False  # server without read-log-file
    c._transport = fake
    c.connect()
    entries = c.read_jboss_log()
    assert len(entries) == 1
    assert entries[0]["severity"] == "INFO"
    assert "server started" in entries[0]["message"]


def test_read_jboss_log_no_op_no_log_dir_refuses(JC, monkeypatch):
    monkeypatch.delenv("JBOSS_LOG_DIR", raising=False)
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.log_op_supported = False
    c._transport = fake
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.read_jboss_log()
    assert FAKE_SECRET not in str(excinfo.value)


# ------------------------------------------------------------- privileged redeploy


def _admin_conn(JC):
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("jboss_admin",
                                                   FAKE_ADMIN_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake
    return c


def test_restart_jboss_deployment_redeploy(JC):
    c = _admin_conn(JC)
    result = c.restart_jboss_deployment("orders.war")
    assert result == {
        "deployment": "orders.war",
        "previous_state": "OK",
        "state": "running",
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    redeploys = [call for call in c._fake.calls
                 if call[0] == "redeploy"]
    assert len(redeploys) == 1
    op, url, user = redeploys[0]
    assert url == "http://jboss01.example:9990/management"
    assert user == "jboss_admin"  # admin identity, not the read user
    assert all(call[2] != "rca_reader" or call[0] != "redeploy"
               for call in c._fake.calls)
    json.dumps(result)


def test_restart_jboss_deployment_refuses_without_admin_credential(
        JC, monkeypatch):
    monkeypatch.delenv("JBOSS_ADMIN_USER", raising=False)
    monkeypatch.delenv("JBOSS_ADMIN_PASSWORD", raising=False)
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.restart_jboss_deployment("orders.war")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_restart_jboss_deployment_unknown_deployment(JC):
    c = _admin_conn(JC)
    with pytest.raises(ConnectorError):
        c.restart_jboss_deployment("no-such.war")


def test_restart_jboss_deployment_rejects_bad_name(JC):
    c = _admin_conn(JC)
    with pytest.raises(ConnectorError):
        c.restart_jboss_deployment("../etc/passwd")


def test_restart_jboss_deployment_op_failure_raises(JC):
    c = _admin_conn(JC)
    c._fake.redeploy_outcome = "failed"
    with pytest.raises(ConnectorError):
        c.restart_jboss_deployment("orders.war")


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(JC):
    c = JC(host="jboss01.example", log_dir="/var/log/jboss",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("jboss_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "jboss01.example" in text


def test_close_is_safe_when_not_connected(JC):
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    heap = conn.read("get_jboss_heap", {})
    assert heap["heap_used_bytes"] == 536870912
    deps = conn.read("get_jboss_deployments", {})
    assert len(deps["deployments"]) == 2
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})


def test_management_envelope_failure_wraps(JC):
    """A transport-level ConnectorError from a failed envelope is reused."""
    c = JC(host="jboss01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))

    class Broken(FakeTransport):
        def post(self, url, payload, auth):
            raise ConnectorError("jboss: 'read-attribute' failed: WFLYCTL0062")

    c._transport = Broken()
    with pytest.raises(ConnectorError):
        c.connect()
