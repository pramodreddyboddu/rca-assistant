"""Tests for the real MongoDB connector (connectors/mongodb.py).

pymongo is NOT installed here and no live mongod is touched. A fake
``pymongo`` module is injected into ``sys.modules`` (removed after each
test by monkeypatch), modelling the public driver surface the connector
uses: MongoClient, db.command("ping"/"serverStatus"/"replSetGetStatus"/
"currentOp"/"killOp"), and errors.OperationFailure with a ``code``
attribute. Placeholders like "s3cr3t-fake-..." stand in for secrets, and
the hygiene tests assert they never leak into repr, exceptions, or the
instance.

The ``connectors.mongodb`` module is imported lazily inside a
module-scoped fixture (never at module top) and its registration is
removed from the global ``CONNECTORS`` registry on teardown, so running
this file together with tests/test_connector_contract.py does not change
the registry that the contract tests pin.
"""

import importlib
import json
import subprocess
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError
from mcp_server.tools import PRIVILEGED_TOOLS, TOOL_NAMES

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"


# ------------------------------------------------------------- fake pymongo


def _base_state():
    primary_optime = datetime(2026, 9, 21, 15, 0, 0, tzinfo=timezone.utc)
    long_value = "x" * 500
    return {
        "serverStatus": {
            "version": "7.0.14",
            "uptime": 86461,
            "connections": {"current": 42, "available": 51158},
            "ok": 1,
        },
        "replSetStatus": {
            "set": "rs0",
            "myState": 1,
            "members": [
                {"_id": 0, "name": "mongo01:27017", "state": 1, "health": 1,
                 "optimeDate": primary_optime},
                {"_id": 1, "name": "mongo02:27017", "state": 2, "health": 1,
                 "optimeDate": primary_optime - timedelta(seconds=3)},
                {"_id": 2, "name": "mongo03:27017", "state": 2, "health": 1,
                 "optimeDate": primary_optime - timedelta(seconds=17)},
            ],
            "ok": 1,
        },
        "currentOp": {
            "inprog": [
                {"opid": 12345, "secs_running": 12, "op": "query",
                 "ns": "orders.customers",
                 "command": {"find": "customers",
                             "filter": {"region": "TX"}}},
                {"opid": 12346, "secs_running": 0, "op": "command",
                 "ns": "admin.$cmd",
                 "command": {"ping": 1}},
                {"opid": "shard-op-7", "secs_running": 300, "op": "query",
                 "ns": "logs.events",
                 "query": {"payload": long_value}},
            ],
            "ok": 1,
        },
    }


def _make_fake_pymongo(state, fail_connect=False):
    """Build a fake `pymongo` module modelling the driver's public API.

    FakeMongoClient records constructor kwargs EXCEPT the password value
    (``got_password`` just flags it was passed). FakeDatabase.command
    serves canned serverStatus / replSetGetStatus / currentOp responses,
    records killOp targets, and raises FakeOperationFailure(code=93) for
    replSetGetStatus when the state has no replica-set status (standalone
    node).
    """
    mod = types.ModuleType("pymongo")

    class FakePyMongoError(Exception):
        pass

    class FakeOperationFailure(FakePyMongoError):
        def __init__(self, msg, code=None, details=None):
            super().__init__(msg)
            self.code = code
            self.details = details

    class FakeConnectionFailure(FakePyMongoError):
        pass

    mod.errors = types.SimpleNamespace(
        PyMongoError=FakePyMongoError,
        OperationFailure=FakeOperationFailure,
        ConnectionFailure=FakeConnectionFailure,
    )

    class FakeDatabase:
        def __init__(self, client, name):
            self._client = client
            self._name = name

        def command(self, cmd, value=None, **kwargs):
            if cmd == "ping":
                if self._client._unreachable:
                    raise FakeConnectionFailure("connection refused")
                return {"ok": 1}
            if cmd == "serverStatus":
                return state["serverStatus"]
            if cmd == "replSetGetStatus":
                rs = state.get("replSetStatus")
                if rs is None:
                    raise FakeOperationFailure(
                        "not running with --replSet", code=93)
                return rs
            if cmd == "currentOp":
                state.setdefault("currentOp_filters", []).append(value)
                return state["currentOp"]
            if cmd == "killOp":
                state.setdefault("killed", []).append(kwargs.get("op"))
                return {"ok": 1}
            raise AssertionError(f"FakeDatabase: unexpected command {cmd!r}")

    class FakeMongoClient:
        instances = []

        def __init__(self, host=None, port=None, username=None,
                     password=None, authSource=None,
                     serverSelectionTimeoutMS=None, **kwargs):
            self.host = host
            self.port = port
            self.username = username
            self.authSource = authSource
            self.serverSelectionTimeoutMS = serverSelectionTimeoutMS
            # Deliberately records everything EXCEPT the password value.
            self.got_password = bool(password)
            self._unreachable = fail_connect or "unreachable" in str(host)
            if self._unreachable:
                raise FakeConnectionFailure("connection refused")
            self.closed = False
            FakeMongoClient.instances.append(self)

        def __getitem__(self, name):
            return FakeDatabase(self, name)

        @property
        def admin(self):
            return self["admin"]

        def close(self):
            self.closed = True

    FakeMongoClient.instances.clear()
    mod.MongoClient = FakeMongoClient
    mod._fake_client_cls = FakeMongoClient
    return mod


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def mongo_mod():
    """Import connectors.mongodb lazily; undo its registration afterwards.

    connectors/__init__ does NOT import this module (it is wired by the
    coordinator), so the teardown pops our registration to leave the
    pinned registry in tests/test_connector_contract.py untouched.
    """
    had = "mongodb" in CONNECTORS
    mod = importlib.import_module("connectors.mongodb")
    yield mod
    if not had:
        CONNECTORS.pop("mongodb", None)


@pytest.fixture(scope="module")
def MC(mongo_mod):
    return mongo_mod.MongoDbConnector


@pytest.fixture
def fake_pymongo(monkeypatch):
    """Install the fake pymongo module for one test."""
    state = _base_state()
    mod = _make_fake_pymongo(state)
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    return mod, state


@pytest.fixture
def conn(MC, fake_pymongo):
    """A connected connector backed by the fake driver."""
    mod, state = fake_pymongo
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    c._fake_mod = mod  # test introspection only; never a real attribute
    c._fake_state = state
    return c


def _client_cls(fake_pymongo):
    mod, _ = fake_pymongo
    return mod._fake_client_cls


# ------------------------------------------------------------- registration


def test_module_registers_connector(mongo_mod):
    assert "mongodb" in CONNECTORS
    spec, factory = CONNECTORS["mongodb"]
    assert spec.name == "mongodb"
    assert factory is mongo_mod.MongoDbConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description
    assert spec.credential_refs == ("MONGO_READ_USER", "MONGO_READ_PASSWORD")


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_mongo_health",
        "get_mongo_replset",
        "get_mongo_current_ops",
        "kill_mongo_op",
    }


def test_constructor_takes_no_raw_secret(MC):
    import inspect
    params = inspect.signature(MC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials", "credential"}
    for pname in params:
        assert pname.lower() not in banned


def test_constructor_defaults(MC):
    c = MC(host="mongo01.example")
    text = repr(c)
    assert "27017" in text
    assert "'admin'" in text or '"admin"' in text


def test_module_imports_without_driver():
    """The module must import cleanly when pymongo is absent (lazy driver)."""
    code = (
        "import sys; sys.modules.pop('pymongo', None); "
        "import connectors.mongodb; print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, cwd=".")
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_driver_absent_connect_raises_with_install_guidance(MC, monkeypatch):
    monkeypatch.delitem(sys.modules, "pymongo", raising=False)
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "pip install pymongo" in str(excinfo.value)
    assert FAKE_SECRET not in str(excinfo.value)


# ------------------------------------------------------------- connect


def test_connect_pings_with_read_credential(MC, fake_pymongo):
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    cls = _client_cls(fake_pymongo)
    assert len(cls.instances) == 1
    client = cls.instances[0]
    assert client.host == "mongo01.example"
    assert client.port == 27017
    assert client.username == "rca_reader"  # read user, not admin
    assert client.got_password  # password reached the driver
    assert client.authSource == "admin"
    c.close()
    assert client.closed


def test_connect_uses_env_credentials(MC, fake_pymongo, monkeypatch):
    monkeypatch.setenv("MONGO_READ_USER", "env_reader")
    monkeypatch.setenv("MONGO_READ_PASSWORD", "env-pass-fake")
    c = MC(host="mongo01.example")
    c.connect()  # must not raise
    cls = _client_cls(fake_pymongo)
    assert cls.instances[0].username == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(MC, monkeypatch):
    """A dead mongod must surface ConnectorError, never a raw driver error."""
    state = _base_state()
    mod = _make_fake_pymongo(state, fail_connect=True)
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    c = MC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, mod.errors.PyMongoError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_empty_credential_provider_refuses(MC, fake_pymongo):
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("", ""))
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(MC, fake_pymongo):
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError):
        c.get_mongo_health()
    with pytest.raises(ConnectorError):
        c.get_mongo_replset()
    with pytest.raises(ConnectorError):
        c.get_mongo_current_ops()
    with pytest.raises(ConnectorError):
        c.kill_mongo_op(123)


# ------------------------------------------------------------- health


def test_get_mongo_health_shape(conn):
    health = conn.get_mongo_health()
    assert health == {
        "host": "mongo01.example",
        "port": 27017,
        "version": "7.0.14",
        "uptime_secs": 86461,
        "connections_current": 42,
        "connections_available": 51158,
        "ts": health["ts"],
    }
    assert health["ts"].endswith("Z")
    json.dumps(health)  # JSON-serializable


def test_get_mongo_health_missing_fields_degrade_to_none(MC, monkeypatch):
    state = _base_state()
    state["serverStatus"] = {"ok": 1}
    mod = _make_fake_pymongo(state)
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    health = c.get_mongo_health()
    assert health["version"] is None
    assert health["uptime_secs"] is None
    assert health["connections_current"] is None
    json.dumps(health)


# ------------------------------------------------------------- replica set


def test_get_mongo_replset_shape_and_lag(conn):
    rs = conn.get_mongo_replset()
    assert rs["set_name"] == "rs0"
    assert rs["my_state"] == 1
    members = {m["name"]: m for m in rs["members"]}
    assert set(members) == {"mongo01:27017", "mongo02:27017", "mongo03:27017"}
    primary = members["mongo01:27017"]
    assert primary["state"] == 1
    assert primary["health"] == 1
    assert primary["lag_seconds"] == 0.0
    assert members["mongo02:27017"]["lag_seconds"] == 3.0
    assert members["mongo03:27017"]["lag_seconds"] == 17.0
    assert members["mongo02:27017"]["state"] == 2
    assert rs["ts"].endswith("Z")
    json.dumps(rs)


def test_get_mongo_replset_not_a_replset_raises_clear_error(MC, monkeypatch):
    state = _base_state()
    del state["replSetStatus"]  # standalone node
    mod = _make_fake_pymongo(state)
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.get_mongo_replset()
    assert "--replSet" in str(excinfo.value)


def test_get_mongo_replset_unknown_optime_is_none(MC, monkeypatch):
    """Members without an optimeDate, or with no primary, get lag None."""
    state = _base_state()
    for m in state["replSetStatus"]["members"]:
        m.pop("optimeDate", None)
    mod = _make_fake_pymongo(state)
    monkeypatch.setitem(sys.modules, "pymongo", mod)
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    rs = c.get_mongo_replset()
    lags = {m["name"]: m["lag_seconds"] for m in rs["members"]}
    # Primary's own lag is defined as 0.0; others are unknown.
    assert lags == {"mongo01:27017": 0.0,
                    "mongo02:27017": None,
                    "mongo03:27017": None}
    json.dumps(rs)


# ------------------------------------------------------------- current ops


def test_get_mongo_current_ops_shape(conn):
    result = conn.get_mongo_current_ops()
    ops = {o["opid"]: o for o in result["ops"]}
    assert set(ops) == {12345, 12346, "shard-op-7"}
    first = ops[12345]
    assert first["secs_running"] == 12
    assert first["op"] == "query"
    assert first["ns"] == "orders.customers"
    assert "customers" in first["query_snippet"]
    assert len(first["query_snippet"]) <= 200
    # query_snippet is truncated to 200 chars for long payloads
    long_op = ops["shard-op-7"]
    assert len(long_op["query_snippet"]) == 200
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_mongo_current_ops_uses_active_filter(conn):
    conn.get_mongo_current_ops()
    assert conn._fake_state["currentOp_filters"] == [{"active": True}]


# ------------------------------------------------------------- privileged kill


def _admin_conn(MC, fake_pymongo, **kwargs):
    mod, state = fake_pymongo
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("mongo_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    c.connect()
    c._fake_state = state  # test introspection only; never a real attribute
    return c


def test_kill_mongo_op_uses_separate_admin_identity(MC, fake_pymongo):
    c = _admin_conn(MC, fake_pymongo)
    result = c.kill_mongo_op(12345)
    assert result == {
        "opid": 12345,
        "killed": True,
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)
    # The kill went through a SECOND client built with the admin identity.
    cls = _client_cls(fake_pymongo)
    assert len(cls.instances) == 2
    read_client, admin_client = cls.instances
    assert read_client.username == "rca_reader"
    assert admin_client.username == "mongo_admin"  # NOT the read user
    assert admin_client.got_password
    assert admin_client.closed  # short-lived: closed after the kill
    assert c._fake_state["killed"] == [12345]


def test_kill_mongo_op_accepts_string_opid(MC, fake_pymongo):
    c = _admin_conn(MC, fake_pymongo)
    result = c.kill_mongo_op("shard-op-7")
    assert result["opid"] == "shard-op-7"
    assert result["killed"] is True
    assert c._fake_state["killed"] == ["shard-op-7"]


def test_kill_mongo_op_refuses_without_admin_credential(MC, fake_pymongo,
                                                       monkeypatch):
    monkeypatch.delenv("MONGO_ADMIN_USER", raising=False)
    monkeypatch.delenv("MONGO_ADMIN_PASSWORD", raising=False)
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    c._fake_state = fake_pymongo[1]  # test introspection only
    with pytest.raises(ConnectorError) as excinfo:
        c.kill_mongo_op(12345)
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)
    # No kill was attempted and no admin client was built.
    cls = _client_cls(fake_pymongo)
    assert len(cls.instances) == 1  # the read client from connect() only
    assert "killed" not in c._fake_state


def test_kill_mongo_op_validates_opid(MC, fake_pymongo):
    c = _admin_conn(MC, fake_pymongo)
    for bad in (None, "", "   ", 1.5, True, False, {"opid": 1}, ["x"]):
        with pytest.raises(ConnectorError):
            c.kill_mongo_op(bad)
    # No kill attempted for any invalid opid.
    assert "killed" not in c._fake_state


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(MC):
    c = MC(host="mongo01.example", port=27018, database="ops",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("mongo_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "mongo01.example" in text
    assert "27018" in text


def test_secrets_never_stored_on_instance(conn):
    for attr, value in vars(conn).items():
        assert FAKE_SECRET not in str(value), f"secret in self.{attr}"


def test_close_is_safe_when_not_connected(MC):
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    health = conn.read("get_mongo_health", {})
    assert health["version"] == "7.0.14"
    ops = conn.read("get_mongo_current_ops", {})
    assert len(ops["ops"]) == 3
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})


def test_privileged_tool_routing_matches_registry(conn):
    """kill_mongo_op routes through act() exactly when the gateway marks
    it privileged; read() must never reach it in that case."""
    if "kill_mongo_op" not in PRIVILEGED_TOOLS:
        pytest.skip("kill_mongo_op not yet added to PRIVILEGED_TOOLS "
                    "by the tools.py workstream")
    with pytest.raises(ConnectorError):
        conn.read("kill_mongo_op", {"opid": 123})
    with pytest.raises(ConnectorError):
        conn.act("get_mongo_health", {})


def test_check_contract_once_tool_names_registered(MC, fake_pymongo):
    """Full contract check when the coordinator has registered the names."""
    c = MC(host="mongo01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    missing = set(c.capabilities()) - set(TOOL_NAMES)
    if missing:
        pytest.skip(f"tool names not yet in mcp_server.tools.TOOL_NAMES: "
                    f"{sorted(missing)}")
    c.check_contract()
