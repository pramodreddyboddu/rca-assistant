"""Tests for the real ActiveMQ Artemis connector (connectors/artemis.py).

No live Artemis is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``get_json`` / ``post_json`` / ``get_text``) is replaced with a
``FakeTransport`` returning canned Jolokia JSON. Placeholders like
"REDACTED-fake" stand in for secrets, and hygiene tests assert they never
leak into repr, exceptions, or logs.

The ``connectors.artemis`` module is imported lazily inside a
module-scoped fixture (never at module top). Unlike tomcat/kafka, artemis
is not imported by ``connectors/__init__.py`` (a parallel workstream owns
the registry wiring), so the teardown DEREGISTERS it from the global
``CONNECTORS`` registry -- leaving "artemis" registered would break the
pinned registry assertions in tests/test_connector_contract.py and
tests/test_contract_harness.py.
"""

import importlib
import json
import urllib.error
import urllib.parse

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

ORDERS_MBEAN = (
    'org.apache.activemq.artemis:broker="mybroker",component=addresses,'
    'address="orders",subcomponent=queues,routing-type="anycast",'
    'queue="orders"'
)
ALERTS_MBEAN = (
    'org.apache.activemq.artemis:broker="mybroker",component=addresses,'
    'address="alerts",subcomponent=queues,routing-type="multicast",'
    'queue="alerts"'
)
SERVER_MBEAN = 'org.apache.activemq.artemis:broker="mybroker"'
ADDRESS_MBEAN = (
    'org.apache.activemq.artemis:broker="mybroker",component=addresses,'
    'address="orders"'
)


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def artemis_mod():
    """Import connectors.artemis lazily; deregister it afterwards."""
    had = "artemis" in CONNECTORS
    mod = importlib.import_module("connectors.artemis")
    yield mod
    if not had:
        CONNECTORS.pop("artemis", None)


@pytest.fixture(scope="module")
def AC(artemis_mod):
    return artemis_mod.ArtemisConnector


class FakeTransport:
    """Canned Jolokia JSON for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.searches: list[dict] = []
        self.fail_with: Exception | None = None
        #: when True, the alerts queue read reports name "orders" too,
        #: so purge must refuse the ambiguous match.
        self.duplicate_queue_name = False

    def _record(self, method, url, auth):
        user = auth[0] if auth else None
        self.calls.append((method, url, user))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the three boundary methods the connector uses -----------------

    def get_json(self, url, auth):
        self._record("get_json", url, auth)
        if url.endswith("/version"):
            return {"value": {"agent": "1.7.2", "info": {"product": "artemis"}}}
        if "/read/" in url:
            decoded = urllib.parse.unquote(url)
            if 'queue="orders"' in decoded:
                return {"value": {
                    "name": "orders", "messageCount": 150,
                    "deliveringCount": 4, "consumerCount": 2}}
            if 'queue="alerts"' in decoded:
                return {"value": {
                    "name": "orders" if self.duplicate_queue_name else "alerts",
                    "messageCount": 0, "deliveringCount": 0,
                    "consumerCount": 1}}
            if decoded.endswith("/version,started,totalMessageCount"):
                return {"value": {
                    "version": "2.33.0", "started": True,
                    "totalMessageCount": 154}}
        raise AssertionError(f"FakeTransport: unexpected get_json {url}")

    def post_json(self, url, payload, auth):
        self._record("post_json", url, auth)
        if payload.get("type") == "search":
            self.searches.append(payload)
            pattern = payload.get("mbean", "")
            if "subcomponent=queues" in pattern:
                return {"value": [ORDERS_MBEAN, ALERTS_MBEAN]}
            if pattern == "org.apache.activemq.artemis:broker=*":
                # Broker search also matches address MBeans on a real
                # broker; the connector must filter to the broker-only
                # MBean (ActiveMQServerControl).
                return {"value": [ADDRESS_MBEAN, SERVER_MBEAN]}
            raise AssertionError(
                f"FakeTransport: unexpected search {pattern!r}")
        if payload.get("type") == "exec":
            if payload.get("operation") == "removeAllMessages":
                return {"value": 154, "status": 200}
            raise AssertionError(
                f"FakeTransport: unexpected exec "
                f"{payload.get('operation')!r}")
        raise AssertionError(f"FakeTransport: unexpected post {payload!r}")

    def get_text(self, url, auth):
        self._record("get_text", url, auth)
        raise AssertionError(f"FakeTransport: unexpected get_text {url}")


@pytest.fixture
def conn(AC):
    """A connected connector backed by FakeTransport."""
    c = AC(host="artemis01.example", port=8161,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(artemis_mod):
    assert "artemis" in CONNECTORS
    spec, factory = CONNECTORS["artemis"]
    assert spec.name == "artemis"
    assert factory is artemis_mod.ArtemisConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_artemis_queues",
        "get_artemis_broker",
        "purge_artemis_queue",
    }


def test_constructor_takes_no_raw_secret(AC):
    import inspect
    params = inspect.signature(AC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned
    # env-name / provider params are the sanctioned shape
    assert "password_env" in params
    assert "privileged_password_env" in params
    assert "privileged_credential_provider" in params


def test_constructor_defaults(AC):
    c = AC(host="h",
           credential_provider=lambda: ("u", "p-fake"))
    assert c._port == 8161
    assert c._jolokia_base == "http://h:8161/jolokia"
    assert c._broker is None
    c = AC(host="h", jolokia_path="console/jolokia",
           credential_provider=lambda: ("u", "p-fake"))
    assert c._jolokia_base == "http://h:8161/console/jolokia"


# ------------------------------------------------------------- connect


def test_connect_probes_jolokia_version(AC):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_json"
    assert fake.calls[0][1].endswith("/version")
    assert fake.calls[0][2] == "rca_reader"  # read user, not admin


def test_connect_uses_env_credentials(AC, monkeypatch):
    monkeypatch.setenv("ARTEMIS_READ_USER", "env_reader")
    monkeypatch.setenv("ARTEMIS_READ_PASSWORD", "env-pass-fake")
    c = AC(host="artemis01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(AC):
    """A dead agent must surface ConnectorError, never a raw URLError."""
    c = AC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_reads_require_connect(AC):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_artemis_queues()
    with pytest.raises(ConnectorError):
        c.get_artemis_broker()
    with pytest.raises(ConnectorError):
        c.purge_artemis_queue("orders")


def test_empty_credential_provider_refuses(AC):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- queues


def test_get_artemis_queues_shape(conn):
    result = conn.get_artemis_queues()
    assert result["host"] == "artemis01.example"
    assert result["port"] == 8161
    queues = {q["name"]: q for q in result["queues"]}
    assert set(queues) == {"orders", "alerts"}
    assert queues["orders"] == {
        "name": "orders", "message_count": 150,
        "delivering_count": 4, "consumer_count": 2,
    }
    assert queues["alerts"]["message_count"] == 0
    assert queues["alerts"]["consumer_count"] == 1
    assert result["ts"].endswith("Z")
    json.dumps(result)  # JSON-serializable


def test_get_artemis_queues_searches_queue_mbeans(conn):
    conn.get_artemis_queues()
    assert any("subcomponent=queues" in p.get("mbean", "")
               for p in conn._fake.searches)


def test_get_artemis_queues_empty_when_no_queues(AC):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()

    def _post(url, payload, auth):
        fake._record("post_json", url, auth)
        return {"value": []}
    fake.post_json = _post
    c._transport = fake
    c.connect()
    assert c.get_artemis_queues()["queues"] == []


# ------------------------------------------------------------- broker


def test_get_artemis_broker_shape(conn):
    result = conn.get_artemis_broker()
    assert result == {
        "host": "artemis01.example",
        "port": 8161,
        "version": "2.33.0",
        "started": True,
        "total_message_count": 154,
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_artemis_broker_reads_server_control_attrs(conn):
    conn.get_artemis_broker()
    attr_calls = [call[1] for call in conn._fake.calls
                  if call[0] == "get_json" and "/read/" in call[1]]
    assert any("version,started,totalMessageCount" in call
               for call in attr_calls)


def test_broker_name_pins_mbean(AC):
    """A pinned broker= name is used verbatim, not searched."""
    c = AC(host="artemis01.example", broker="pinned-broker",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c.get_artemis_broker()
    searches = [call for call in fake.calls
                if call[0] == "post_json"
                and call[1].endswith("jolokia")]
    assert not searches  # no search round-trip with a pinned broker
    reads = [urllib.parse.unquote(call[1]) for call in fake.calls
             if call[0] == "get_json" and "/read/" in call[1]]
    assert any('broker="pinned-broker"' in call for call in reads)


def test_broker_search_without_server_mbean_raises(AC):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()

    def _post(url, payload, auth):
        fake._record("post_json", url, auth)
        return {"value": [ADDRESS_MBEAN]}  # no broker-only MBean
    fake.post_json = _post
    c._transport = fake
    c.connect()
    with pytest.raises(ConnectorError):
        c.get_artemis_broker()


# ------------------------------------------------------------- privileged purge


def _admin_conn(AC, **kwargs):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("artemis_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake
    return c


def test_purge_artemis_queue_shape(AC):
    c = _admin_conn(AC)
    result = c.purge_artemis_queue("orders")
    assert result == {
        "queue": "orders",
        "messages_purged": 154,
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_purge_artemis_queue_execs_remove_all_messages(AC):
    c = _admin_conn(AC)
    c.purge_artemis_queue("orders")
    execs = [call for call in c._fake.calls if call[0] == "post_json"
             and call[2] == "artemis_admin"]
    assert execs  # the exec ran under the admin identity, not the reader
    admin_calls = [call for call in c._fake.calls if call[2] == "artemis_admin"]
    assert all("rca_reader" != call[2] for call in admin_calls)


def test_purge_artemis_queue_refuses_without_admin_credential(AC, monkeypatch):
    monkeypatch.delenv("ARTEMIS_ADMIN_USER", raising=False)
    monkeypatch.delenv("ARTEMIS_ADMIN_PASSWORD", raising=False)
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_artemis_queue("orders")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_purge_artemis_queue_unknown_queue(AC):
    c = _admin_conn(AC)
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_artemis_queue("no-such-queue")
    assert "no-such-queue" in str(excinfo.value)


def test_purge_artemis_queue_refuses_ambiguous_name(AC):
    c = _admin_conn(AC)
    c._fake.duplicate_queue_name = True
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_artemis_queue("orders")
    assert "ambiguous" in str(excinfo.value).lower()


def test_purge_artemis_queue_via_act_routing(AC):
    """The approval-gated act() path reaches purge with admin creds."""
    c = _admin_conn(AC)
    result = c.act("purge_artemis_queue", {"queue": "orders"})
    assert result["messages_purged"] == 154


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(AC):
    c = AC(host="artemis01.example", port=8161, broker="mybroker",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("artemis_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "artemis01.example" in text


def test_close_is_safe_when_not_connected(AC):
    c = AC(host="artemis01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    broker = conn.read("get_artemis_broker", {})
    assert broker["version"] == "2.33.0"
    queues = conn.read("get_artemis_queues", {})
    assert len(queues["queues"]) == 2
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
    # purge is privileged: unreachable through read()
    with pytest.raises(ConnectorError):
        conn.read("purge_artemis_queue", {"queue": "orders"})
    # ... and read tools are unreachable through act()
    with pytest.raises(ConnectorError):
        conn.act("get_artemis_queues", {})
