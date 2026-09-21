"""Tests for the real RabbitMQ connector (connectors/rabbitmq.py).

No live RabbitMQ is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``get_json`` / ``delete``) is replaced with a ``FakeTransport`` returning
canned Management API JSON responses. Placeholders like "REDACTED-fake"
stand in for secrets, and hygiene tests assert they never leak into repr,
exceptions, or logs.

The ``connectors.rabbitmq`` module is imported lazily inside a
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

OVERVIEW = {
    "rabbitmq_version": "3.12.0",
    "cluster_name": "rabbit@mq01",
    "management_version": "3.12.0",
}

QUEUE_LIST = [
    {
        "name": "orders", "vhost": "/",
        "messages": 42, "messages_ready": 40, "messages_unacknowledged": 2,
        "consumers": 3, "state": "running",
    },
    {
        "name": "audit", "vhost": "/",
        "messages": 0, "messages_ready": 0, "messages_unacknowledged": 0,
        "consumers": 0, "state": "idle",
    },
]

QUEUE_DETAIL_ORDERS = {
    "name": "orders", "vhost": "/",
    "messages": 42, "messages_ready": 40, "messages_unacknowledged": 2,
    "consumers": 3, "state": "running",
}

NODE_LIST = [
    {
        "name": "rabbit@mq01", "running": True,
        "mem_used": 171798692, "mem_limit": 1717986918,  # 10.0%
        "disk_free": 53687091200, "fd_used": 48, "fd_total": 1024,
    },
    {
        "name": "rabbit@mq02", "running": True,
        "mem_used": 134217728, "mem_limit": False,  # no limit configured
        "disk_free": 42949672960, "fd_used": 32, "fd_total": 1024,
    },
]

CONNECTION_LIST = [
    {
        "name": "10.0.0.5:54321 -> 10.0.0.9:5672",
        "user": "app_svc", "vhost": "/", "state": "running", "channels": 2,
    },
    {
        "name": "10.0.0.6:54322 -> 10.0.0.9:5672",
        "user": "rca_reader", "vhost": "/", "state": "running", "channels": 1,
    },
]


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def rabbitmq_mod():
    """Import connectors.rabbitmq lazily; restore its registration after.

    The module is self-registering on import; the teardown re-registers
    rather than removing, so later test modules still see the
    package-level registration.
    """
    mod = importlib.import_module("connectors.rabbitmq")
    yield mod
    CONNECTORS["rabbitmq"] = (
        mod.RabbitMQConnector.SPEC, mod.RabbitMQConnector)


@pytest.fixture(scope="module")
def RQ(rabbitmq_mod):
    return rabbitmq_mod.RabbitMQConnector


class FakeTransport:
    """Canned Management API responses for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_with: Exception | None = None

    def _record(self, method, url, auth):
        user = auth[0] if auth else None
        self.calls.append((method, url, user))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the two boundary methods the connector uses -------------------

    def get_json(self, url, auth):
        self._record("get_json", url, auth)
        if url.endswith("/api/overview"):
            return OVERVIEW
        if "/api/queues/" in url:
            parts = url.split("/api/queues/", 1)[1].split("/")
            if len(parts) == 1:
                return QUEUE_LIST
            queue = parts[-1]
            if queue == "orders":
                return QUEUE_DETAIL_ORDERS
            # A missing queue: the real API answers 404, which the
            # connector's transport wraps as ConnectorError.
            raise ConnectorError(
                f"rabbitmq: GET {url} failed: HTTP Error 404: Not Found")
        if url.endswith("/api/nodes"):
            return NODE_LIST
        if url.endswith("/api/connections"):
            return CONNECTION_LIST
        raise AssertionError(f"FakeTransport: unexpected get_json {url}")

    def delete(self, url, auth):
        self._record("delete", url, auth)
        if url.endswith("/contents"):
            return None
        raise AssertionError(f"FakeTransport: unexpected delete {url}")


@pytest.fixture
def conn(RQ):
    """A connected connector backed by FakeTransport."""
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(rabbitmq_mod):
    assert "rabbitmq" in CONNECTORS
    spec, factory = CONNECTORS["rabbitmq"]
    assert spec.name == "rabbitmq"
    assert factory is rabbitmq_mod.RabbitMQConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_rabbitmq_queues",
        "get_rabbitmq_nodes",
        "get_rabbitmq_connections",
        "purge_rabbitmq_queue",
    }


def test_constructor_takes_no_raw_secret(RQ):
    import inspect
    params = inspect.signature(RQ.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


def test_default_port_is_management_port(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    assert c._port == 15672
    assert repr(c) == "RabbitMQConnector(host='mq01.example', port=15672)"


# ------------------------------------------------------------- connect


def test_connect_probes_overview(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_json"
    assert fake.calls[0][1].endswith("/api/overview")
    assert fake.calls[0][2] == "rca_reader"  # read user, not admin


def test_connect_uses_env_credentials(RQ, monkeypatch):
    monkeypatch.setenv("RABBITMQ_READ_USER", "env_reader")
    monkeypatch.setenv("RABBITMQ_READ_PASSWORD", "env-pass-fake")
    c = RQ(host="mq01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(RQ):
    """A dead broker must surface ConnectorError, never a raw URLError."""
    c = RQ(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_non_management_response(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))

    class WrongService(FakeTransport):
        def get_json(self, url, auth):
            self._record("get_json", url, auth)
            return {"not": "rabbitmq"}  # some other HTTP service

    c._transport = WrongService()
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_rabbitmq_queues()
    with pytest.raises(ConnectorError):
        c.get_rabbitmq_nodes()
    with pytest.raises(ConnectorError):
        c.get_rabbitmq_connections()
    with pytest.raises(ConnectorError):
        c.purge_rabbitmq_queue("/", "orders")


def test_empty_credential_provider_refuses(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- queues


def test_get_rabbitmq_queues_shape(conn):
    result = conn.get_rabbitmq_queues()
    assert result["host"] == "mq01.example"
    assert result["port"] == 15672
    assert result["vhost"] == "/"
    assert [q["name"] for q in result["queues"]] == ["audit", "orders"]
    orders = result["queues"][1]
    assert orders == {
        "name": "orders",
        "messages": 42,
        "messages_ready": 40,
        "messages_unacknowledged": 2,
        "consumers": 3,
        "state": "running",
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)  # JSON-serializable


def test_get_rabbitmq_queues_vhost_is_url_encoded(conn):
    conn.get_rabbitmq_queues(vhost="/")
    assert conn._fake.calls[-1][1].endswith("/api/queues/%2F")
    assert conn.get_rabbitmq_queues()["vhost"] == "/"


# ------------------------------------------------------------- nodes


def test_get_rabbitmq_nodes_shape_and_math(conn):
    result = conn.get_rabbitmq_nodes()
    assert result["host"] == "mq01.example"
    assert result["port"] == 15672
    nodes = {n["name"]: n for n in result["nodes"]}
    assert set(nodes) == {"rabbit@mq01", "rabbit@mq02"}
    mq01 = nodes["rabbit@mq01"]
    assert mq01["running"] is True
    assert mq01["mem_used_bytes"] == 171798692
    assert mq01["mem_limit_bytes"] == 1717986918
    assert mq01["mem_used_pct"] == 10.0
    assert mq01["disk_free_bytes"] == 53687091200
    assert mq01["fd_used"] == 48
    assert mq01["fd_total"] == 1024
    # No memory limit configured (mem_limit: false) -> None, not a crash.
    mq02 = nodes["rabbit@mq02"]
    assert mq02["mem_limit_bytes"] is None
    assert mq02["mem_used_pct"] is None
    json.dumps(result)


# ------------------------------------------------------------- connections


def test_get_rabbitmq_connections_shape(conn):
    result = conn.get_rabbitmq_connections()
    assert result["host"] == "mq01.example"
    conns = result["connections"]
    assert len(conns) == 2
    assert conns[0] == {
        "name": "10.0.0.5:54321 -> 10.0.0.9:5672",
        "user": "app_svc",
        "vhost": "/",
        "state": "running",
        "channels": 2,
    }
    json.dumps(result)


# ------------------------------------------------------------- privileged purge


def _admin_conn(RQ, **kwargs):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("rmq_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake
    return c


def test_purge_rabbitmq_queue_reports_purged_count(RQ):
    c = _admin_conn(RQ)
    result = c.purge_rabbitmq_queue("/", "orders")
    assert result == {
        "vhost": "/",
        "queue": "orders",
        "messages_purged": 42,  # pre-purge count from the queue detail
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    urls = [call[1] for call in c._fake.calls]
    assert urls[-2].endswith("/api/queues/%2F/orders")
    assert urls[-1].endswith("/api/queues/%2F/orders/contents")
    # Privileged calls used the admin identity, not the read user.
    admin_calls = [call for call in c._fake.calls if call[2] == "rmq_admin"]
    assert len(admin_calls) == 2
    assert all("rca_reader" != call[2] for call in admin_calls)
    json.dumps(result)


def test_purge_rabbitmq_queue_refuses_without_admin_credential(RQ, monkeypatch):
    monkeypatch.delenv("RABBITMQ_ADMIN_USER", raising=False)
    monkeypatch.delenv("RABBITMQ_ADMIN_PASSWORD", raising=False)
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_rabbitmq_queue("/", "orders")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_purge_rabbitmq_queue_rejects_empty_params(RQ):
    c = _admin_conn(RQ)
    with pytest.raises(ConnectorError):
        c.purge_rabbitmq_queue("/", "")
    with pytest.raises(ConnectorError):
        c.purge_rabbitmq_queue("", "orders")


def test_purge_rabbitmq_queue_unknown_queue(RQ):
    c = _admin_conn(RQ)
    with pytest.raises(ConnectorError):
        c.purge_rabbitmq_queue("/", "no-such-queue")


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("rmq_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "mq01.example" in text


def test_close_is_safe_when_not_connected(RQ):
    c = RQ(host="mq01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    queues = conn.read("get_rabbitmq_queues", {"vhost": "/"})
    assert [q["name"] for q in queues["queues"]] == ["audit", "orders"]
    nodes = conn.read("get_rabbitmq_nodes", {})
    assert len(nodes["nodes"]) == 2
    conns = conn.read("get_rabbitmq_connections", {})
    assert len(conns["connections"]) == 2
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
    with pytest.raises(ConnectorError):
        conn.act("get_rabbitmq_queues", {})
