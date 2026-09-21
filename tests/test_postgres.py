"""Tests for the real PostgreSQL connector (connectors/postgres.py).

The real ``psycopg`` driver is NOT installed here and no live PostgreSQL
is touched. A fake ``psycopg`` module is injected into ``sys.modules``
(via monkeypatch, reverted after each test) with canned cursor results
keyed on the constant SQL each read issues. Placeholders like
"REDACTED-fake" stand in for secrets, and hygiene tests assert they never
leak into repr, exceptions, or logs.

The ``connectors.postgres`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown, so running this file together
with tests/test_connector_contract.py does not change the registry that
the contract tests pin.
"""

import importlib
import json
import subprocess
import sys
import types
from pathlib import Path

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

REPO = Path(__file__).resolve().parents[1]

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

LONG_QUERY = "SELECT * FROM very_large_table WHERE " + "x = 1 AND " * 100


class FakePgError(Exception):
    """Stands in for psycopg.Error in the fake driver."""


def _default_state():
    return {
        "version": "PostgreSQL 16.2 on x86_64-pc-linux-gnu",
        "connections_used": 42,
        "max_connections": "100",
        "blockers": [],
        "in_recovery": False,
        "lag_bytes": None,
        "lag_seconds": None,
        "fail_connect": False,
        "fail_query": False,
    }


def _response(state, sql, single):
    """Canned result keyed on the constant SQL the connector issues."""
    if "pg_terminate_backend" in sql:
        return {"pg_terminate_backend": True}
    if "pg_is_in_recovery" in sql:
        return {"in_recovery": state["in_recovery"]}
    if "pg_wal_lsn_diff" in sql:
        return {"lag_bytes": state["lag_bytes"]}
    if "pg_last_xact_replay_timestamp" in sql:
        return {"lag_seconds": state["lag_seconds"]}
    if "version()" in sql:
        return {"version": state["version"]}
    if "pg_stat_activity" in sql and "count(*)" in sql:
        return {"connections_used": state["connections_used"]}
    if "max_connections" in sql:
        return {"max_connections": state["max_connections"]}
    if "wait_event_type" in sql:
        return [dict(b) for b in state["blockers"]]
    raise AssertionError(f"fake psycopg: unexpected SQL: {sql!r}")


def _make_fake_psycopg(state):
    """Build a fake ``psycopg`` module modelling psycopg3's used surface.

    connect(**kwargs) (context-managed), connection.cursor()
    (context-managed), cursor.execute(sql, params), fetchone(), fetchall().
    The fake records connect kwargs EXCEPT the password value (mirroring
    the hygiene assertion that the password value is passed but not kept).
    """
    mod = types.ModuleType("psycopg")
    mod.rows = types.SimpleNamespace(dict_row=object())
    mod.Error = FakePgError

    class FakeCursor:
        def __init__(self):
            self.executed: list[tuple[str, object]] = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            if state["fail_query"]:
                raise FakePgError("query exploded")
            self._sql = sql

        def fetchone(self):
            return _response(state, self._sql, single=True)

        def fetchall(self):
            return _response(state, self._sql, single=False)

    class FakeConnection:
        instances = []

        def __init__(self, kwargs):
            self._kwargs = kwargs
            # Deliberately records everything EXCEPT the password value.
            self.recorded = {
                k: v for k, v in kwargs.items() if k != "password"
            }
            self.got_password = bool(kwargs.get("password"))
            self.closed = False
            self._cursor = FakeCursor()
            FakeConnection.instances.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.closed = True
            return False

        def cursor(self):
            return self._cursor

        def close(self):
            self.closed = True

    def connect(**kwargs):
        if state["fail_connect"] or "unreachable" in str(
                kwargs.get("host", "")):
            raise FakePgError("connection refused")
        return FakeConnection(kwargs)

    FakeConnection.instances.clear()
    mod.connect = connect
    mod.FakeConnection = FakeConnection
    return mod


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def postgres_mod():
    """Import connectors.postgres lazily; deregister on teardown.

    The contract tests pin the registry to the four package-imported
    connectors, so this module's registration must not leak.
    """
    had = "postgres" in CONNECTORS
    mod = importlib.import_module("connectors.postgres")
    yield mod
    if not had:
        CONNECTORS.pop("postgres", None)


@pytest.fixture(scope="module")
def PC(postgres_mod):
    return postgres_mod.PostgresConnector


@pytest.fixture
def fake_state(monkeypatch):
    """Install the fake psycopg driver; return the mutable canned state."""
    state = _default_state()
    fake = _make_fake_psycopg(state)
    monkeypatch.setitem(sys.modules, "psycopg", fake)
    return state


@pytest.fixture
def conn(PC, fake_state):
    """A connected read-credential connector backed by the fake driver."""
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    return c


def _admin_conn(PC, fake_state, **kwargs):
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("pg_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    c.connect()
    return c


def _fake_connections():
    return sys.modules["psycopg"].FakeConnection.instances


# ------------------------------------------------------------- registration


def test_module_registers_connector(postgres_mod):
    assert "postgres" in CONNECTORS
    spec, factory = CONNECTORS["postgres"]
    assert spec.name == "postgres"
    assert factory is postgres_mod.PostgresConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description
    assert spec.credential_refs == ("POSTGRES_READ_USER",
                                    "POSTGRES_READ_PASSWORD")


def test_module_imports_cleanly_without_driver():
    """Subprocess import with no psycopg installed must succeed (lazy)."""
    code = ("import connectors.postgres; "
            "from connectors import CONNECTORS; "
            "print('postgres' in CONNECTORS)")
    out = subprocess.run([sys.executable, "-c", code], cwd=REPO,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "True" in out.stdout


def test_driver_absent_raises_install_guidance(PC, monkeypatch):
    monkeypatch.delitem(sys.modules, "psycopg", raising=False)
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "install" in str(excinfo.value).lower()
    assert "psycopg" in str(excinfo.value)
    assert FAKE_SECRET not in str(excinfo.value)


def test_capabilities_exact_set(PC):
    c = PC(host="pg01.example")
    assert set(c.capabilities()) == {
        "get_postgres_health",
        "get_postgres_blocking",
        "get_postgres_replication",
        "terminate_postgres_backend",
    }


def test_constructor_defaults(PC):
    c = PC(host="pg01.example")
    assert c._port == 5432
    assert c._database == "postgres"


def test_database_env_fallback(PC, monkeypatch):
    monkeypatch.setenv("POSTGRES_DATABASE", "appdb")
    c = PC(host="pg01.example")
    assert c._database == "appdb"


def test_constructor_takes_no_raw_secret(PC):
    import inspect
    params = inspect.signature(PC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


# ------------------------------------------------------------- connect


def test_connect_probe_opens_and_closes(PC, fake_state):
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    conns = _fake_connections()
    assert len(conns) == 1
    assert conns[0].closed  # probe connection released immediately
    assert conns[0].recorded["host"] == "pg01.example"
    assert conns[0].recorded["port"] == 5432
    assert conns[0].recorded["dbname"] == "postgres"
    assert conns[0].recorded["user"] == "rca_reader"
    assert conns[0].got_password  # password passed to the driver...
    assert "password" not in conns[0].recorded  # ...but not recorded


def test_connect_uses_env_credentials(PC, fake_state, monkeypatch):
    monkeypatch.setenv("POSTGRES_READ_USER", "env_reader")
    monkeypatch.setenv("POSTGRES_READ_PASSWORD", "env-pass-fake")
    c = PC(host="pg01.example")
    c.connect()  # must not raise
    assert _fake_connections()[-1].recorded["user"] == "env_reader"


def test_connect_failure_wraps_driver_error(PC, fake_state):
    """A dead server must surface ConnectorError, never a raw driver error."""
    fake_state["fail_connect"] = True
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, FakePgError)
    assert "pg01.example" in str(excinfo.value)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_unreachable_host_wrapped(PC, fake_state):
    c = PC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError):
        c.connect()


def test_empty_credential_provider_refuses(PC, fake_state):
    c = PC(host="pg01.example",
           credential_provider=lambda: ("", ""))
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(PC, fake_state):
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    with pytest.raises(ConnectorError):
        c.get_postgres_health()
    with pytest.raises(ConnectorError):
        c.get_postgres_blocking()
    with pytest.raises(ConnectorError):
        c.get_postgres_replication()
    with pytest.raises(ConnectorError):
        c.terminate_postgres_backend(123)


# ------------------------------------------------------------- health


def test_get_postgres_health_shape_and_math(conn):
    health = conn.get_postgres_health()
    assert health["host"] == "pg01.example"
    assert health["port"] == 5432
    assert health["database"] == "postgres"
    assert health["version"] == "PostgreSQL 16.2 on x86_64-pc-linux-gnu"
    assert health["up"] is True
    assert health["connections_used"] == 42
    assert health["connections_max"] == 100
    assert health["connections_pct"] == 42.0
    assert health["ts"].endswith("Z")
    json.dumps(health)  # JSON-serializable


def test_get_postgres_health_pct_none_when_max_zero(conn, fake_state):
    fake_state["max_connections"] = "0"
    health = conn.get_postgres_health()
    assert health["connections_max"] == 0
    assert health["connections_pct"] is None


def test_get_postgres_health_query_failure_raises(conn, fake_state):
    fake_state["fail_query"] = True
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_postgres_health()
    assert FAKE_SECRET not in str(excinfo.value)


# ------------------------------------------------------------- blocking


def test_get_postgres_blocking_shapes_and_truncates(conn, fake_state):
    fake_state["blockers"] = [
        {"pid": 4242, "usename": "app", "wait_seconds": 12.5,
         "query": LONG_QUERY, "locktype": "relation"},
        {"pid": 4243, "usename": "etl", "wait_seconds": 0.0,
         "query": "SELECT 1", "locktype": "transactionid"},
    ]
    result = conn.get_postgres_blocking()
    assert result["host"] == "pg01.example"
    assert len(result["blockers"]) == 2
    first, second = result["blockers"]
    assert first["pid"] == 4242
    assert first["usename"] == "app"
    assert first["wait_seconds"] == 12.5
    assert first["locktype"] == "relation"
    assert len(first["query_snippet"]) == 200  # truncated to the boundary
    assert first["query_snippet"] == LONG_QUERY[:200]
    assert second["query_snippet"] == "SELECT 1"  # short queries untouched
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_postgres_blocking_empty(conn):
    result = conn.get_postgres_blocking()
    assert result["blockers"] == []


# ------------------------------------------------------------- replication


def test_get_postgres_replication_primary(conn):
    result = conn.get_postgres_replication()
    assert result == {
        "host": "pg01.example",
        "role": "primary",
        "replay_lag_bytes": None,
        "replay_lag_seconds": None,
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_postgres_replication_standby(conn, fake_state):
    fake_state["in_recovery"] = True
    fake_state["lag_bytes"] = 1048576
    fake_state["lag_seconds"] = 1.5
    result = conn.get_postgres_replication()
    assert result["host"] == "pg01.example"
    assert result["role"] == "standby"
    assert result["replay_lag_bytes"] == 1048576
    assert result["replay_lag_seconds"] == 1.5
    json.dumps(result)


# ------------------------------------------------------------- privileged terminate


def test_terminate_postgres_backend_success(PC, fake_state):
    c = _admin_conn(PC, fake_state)
    before = len(_fake_connections())
    result = c.terminate_postgres_backend(4242)
    assert result == {"pid": 4242, "terminated": True, "ts": result["ts"]}
    assert result["ts"].endswith("Z")
    # The terminate connection ran as the ADMIN identity, not the reader.
    term_conn = _fake_connections()[before]
    assert term_conn.recorded["user"] == "pg_admin"
    assert term_conn.closed  # per-call connection released
    # The pid was bound as a parameter, never interpolated into SQL.
    sql, params = term_conn._cursor.executed[0]
    assert params == (4242,)
    assert "4242" not in sql
    json.dumps(result)


def test_terminate_validates_pid(PC, fake_state):
    c = _admin_conn(PC, fake_state)
    for bad in (0, -1, -4242, "4242", None, True, 4.2):
        with pytest.raises(ConnectorError):
            c.terminate_postgres_backend(bad)


def test_terminate_refuses_without_admin_credential(PC, fake_state,
                                                   monkeypatch):
    monkeypatch.delenv("POSTGRES_ADMIN_USER", raising=False)
    monkeypatch.delenv("POSTGRES_ADMIN_PASSWORD", raising=False)
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.terminate_postgres_backend(4242)
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_terminate_failure_wraps_driver_error(PC, fake_state):
    fake_state["fail_query"] = True
    c = _admin_conn(PC, fake_state)
    with pytest.raises(ConnectorError) as excinfo:
        c.terminate_postgres_backend(4242)
    assert not isinstance(excinfo.value, FakePgError)
    assert FAKE_ADMIN_SECRET not in str(excinfo.value)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(PC):
    c = PC(host="pg01.example", port=5433, database="appdb",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("pg_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "pg01.example" in text


def test_no_instance_attribute_holds_secrets(PC, fake_state):
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("pg_admin",
                                                   FAKE_ADMIN_SECRET))
    c.connect()
    c.terminate_postgres_backend(4242)
    leaked = [attr for attr, value in vars(c).items()
              if FAKE_SECRET in str(value)
              or FAKE_ADMIN_SECRET in str(value)]
    assert leaked == []


def test_close_is_safe_anytime(PC, fake_state):
    c = PC(host="pg01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # not connected: must not raise
    c.connect()
    c.close()
    c.close()


def test_read_act_routing_dispatches_by_name(PC, fake_state, monkeypatch):
    # The gateway's privileged set is owned by the tools workstream
    # (terminate_postgres_backend lands there with the other privileged
    # tools); pin it here so the routing contract is provable in isolation.
    import connectors.base as base
    monkeypatch.setattr(base, "PRIVILEGED_TOOLS",
                        base.PRIVILEGED_TOOLS | {"terminate_postgres_backend"})
    c = _admin_conn(PC, fake_state)
    health = c.read("get_postgres_health", {})
    assert health["connections_used"] == 42
    result = c.act("terminate_postgres_backend", {"pid": 99})
    assert result["terminated"] is True
    with pytest.raises(ConnectorError):
        c.read("terminate_postgres_backend", {"pid": 99})  # not a read
    with pytest.raises(ConnectorError):
        c.act("get_postgres_health", {})  # not a privileged act
    with pytest.raises(ConnectorError):
        c.read("no_such_tool", {})
