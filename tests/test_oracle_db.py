"""Tests for the real Oracle Database connector (connectors/oracle_db.py).

oracledb is NOT installed here and no live database is touched. A fake
``oracledb`` module is injected into ``sys.modules`` (and removed after
each test by monkeypatch), covering health, tablespaces, blocking
sessions, the ORA-00942 grants path, the privileged kill, and credential
hygiene. No test fixture contains a real secret: placeholders like
"REDACTED-fake" stand in, and the hygiene tests assert they never leak
into repr, exceptions, or logs.

The ``connectors.oracle_db`` module is imported lazily inside a
module-scoped fixture (never at module top) and REMOVED from the global
``CONNECTORS`` registry on teardown (``connectors/__init__`` does not
import it), so running this file together with
tests/test_connector_contract.py does not change the registry that the
contract tests pin.
"""

import importlib
import json
import subprocess
import sys
import types

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

LONG_SQL = (
    "SELECT /*+ FULL(orders) */ order_id, customer_id, total, status, "
    "created_at, shipped_at, billing_address, shipping_address "
    "FROM sales.orders WHERE status = 'PENDING' AND created_at > "
    "SYSDATE - 7 AND region IN ('US-EAST', 'US-WEST', 'EU-CENTRAL') "
    "ORDER BY created_at DESC, order_id ASC"
)  # > 200 chars: exercises sql_snippet truncation


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def oracle_mod():
    """Import connectors.oracle_db lazily; undo its registration afterwards.

    Unlike kafka/tomcat, connectors/__init__ does not import oracle_db,
    so teardown pops the entry this import added rather than
    re-registering it.
    """
    already = "oracle_db" in CONNECTORS
    mod = importlib.import_module("connectors.oracle_db")
    yield mod
    if not already:
        CONNECTORS.pop("oracle_db", None)


@pytest.fixture(scope="module")
def OC(oracle_mod):
    return oracle_mod.OracleDbConnector


# ------------------------------------------------------------- fake oracledb


def _make_fake_oracledb(state):
    """Build a fake `oracledb` module modelling the thin-driver surface the
    connector uses: oracledb.connect(**kwargs) -> connection with
    .cursor() -> cursor with .execute(sql, params), .fetchall(),
    .fetchone(), .description, .close()."""
    mod = types.ModuleType("oracledb")

    class FakeDatabaseError(Exception):
        pass

    class FakeCursor:
        instances = []

        def __init__(self, conn):
            self._conn = conn
            self.executed = []  # (sql, params) pairs
            self._rows = []
            self._description = []
            self.closed = False
            FakeCursor.instances.append(self)

        @property
        def description(self):
            return self._description

        def _respond(self, sql):
            up = sql.upper()
            if "FROM DUAL" in up:
                return [(1,)], [("1",)]
            if "V$VERSION" in up:
                return [(
                    "Oracle Database 19c Enterprise Edition Release "
                    "19.0.0.0.0 - Production",
                )], [("BANNER",)]
            if "V$DATABASE" in up:
                return [("READ WRITE",)], [("OPEN_MODE",)]
            if "V$RESOURCE_LIMIT" in up:
                return [(150, state.get("sessions_limit", "300")),
                        ], [("CURRENT_UTILIZATION",), ("LIMIT_VALUE",)]
            if "DBA_DATA_FILES" in up:
                if state.get("tablespace_ora_00942"):
                    raise FakeDatabaseError(
                        "ORA-00942: table or view does not exist")
                return [
                    ("SYSAUX", 45.2, 920.5, 2048.0),
                    ("USERS", 78.9, 1612.3, 2048.0),
                ], [("NAME",), ("USED_PCT",), ("USED_MB",), ("MAX_MB",)]
            if "V$SESSION" in up:
                return [
                    (123, 456, "APP_USER", 42, LONG_SQL),
                    (124, 789, "REPORT_USER", 7, None),
                ], [("SID",), ("SERIAL_NO",), ("USERNAME",),
                    ("WAIT_SECONDS",), ("SQL_TEXT",)]
            if up.startswith("ALTER SYSTEM KILL SESSION"):
                state.setdefault("kills", []).append(sql)
                return [], []
            raise AssertionError(f"FakeCursor: unexpected SQL {sql!r}")

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            rows, desc = self._respond(sql)
            self._rows = rows
            self._description = desc

        def fetchall(self):
            return list(self._rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def close(self):
            self.closed = True

    class FakeConnection:
        instances = []

        def __init__(self, **kwargs):
            # Deliberately records everything EXCEPT the password value.
            self.kwargs = {k: v for k, v in kwargs.items()
                           if k != "password"}
            self.got_password = bool(kwargs.get("password"))
            self.closed = False
            FakeConnection.instances.append(self)

        def cursor(self):
            return FakeCursor(self)

        def close(self):
            self.closed = True

    def connect(**kwargs):
        if state.get("fail_connect") or "unreachable" in kwargs.get("host",
                                                                    ""):
            raise FakeDatabaseError(
                "DPY-6005: cannot connect to database (CONNECTION_ID=fake)")
        return FakeConnection(**kwargs)

    mod.connect = connect
    mod.DatabaseError = FakeDatabaseError
    mod.FakeCursor = FakeCursor
    mod.FakeConnection = FakeConnection
    return mod


@pytest.fixture()
def fake_oracledb(monkeypatch):
    """Inject the fake oracledb module; return its backing state dict."""
    state = {}
    monkeypatch.setitem(sys.modules, "oracledb",
                        _make_fake_oracledb(state))
    return state


@pytest.fixture()
def failing_oracledb(monkeypatch):
    state = {"fail_connect": True}
    monkeypatch.setitem(sys.modules, "oracledb",
                        _make_fake_oracledb(state))
    return state


def _conn(OC, **kwargs):
    kwargs.setdefault("credential_provider",
                      lambda: ("rca_reader", FAKE_SECRET))
    kwargs.setdefault("privileged_credential_provider",
                      lambda: ("rca_admin", FAKE_ADMIN_SECRET))
    return OC(host="oradb01.example", **kwargs)


def _connected(OC, **kwargs):
    """A connected connector backed by the injected fake driver.

    The caller must use the fake_oracledb / failing_oracledb fixture.
    """
    c = _conn(OC, **kwargs)
    c.connect()
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(oracle_mod):
    assert "oracle_db" in CONNECTORS
    spec, factory = CONNECTORS["oracle_db"]
    assert spec.name == "oracle_db"
    assert factory is oracle_mod.OracleDbConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description
    assert "ORACLE_READ_USER" in spec.credential_refs


def test_capabilities_exact_set(OC):
    c = _conn(OC)
    assert set(c.capabilities()) == {
        "get_oracle_health",
        "get_oracle_tablespaces",
        "get_oracle_blocking",
        "kill_oracle_session",
    }


def test_constructor_defaults(OC):
    c = OC(host="oradb01.example")
    assert c._port == 1521
    assert c._service == "ORCLPDB1"
    assert c._connect_timeout == 10


def test_constructor_takes_no_raw_secret(OC):
    import inspect
    params = inspect.signature(OC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


# ------------------------------------------------------------- guarded import


def test_module_imports_cleanly_without_oracledb():
    """The driver must never be needed at import time (subprocess proof)."""
    code = (
        "import sys; "
        "assert 'oracledb' not in sys.modules, 'oracledb unexpectedly "
        "present'; "
        "from connectors.oracle_db import OracleDbConnector; "
        "assert 'oracledb' not in sys.modules, 'import pulled in oracledb'; "
        "print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_driver_absent_gives_install_instructions(OC, monkeypatch):
    monkeypatch.delitem(sys.modules, "oracledb", raising=False)
    c = _conn(OC)
    c._connected = True  # past the gating; the driver path is under test
    with pytest.raises(ConnectorError) as excinfo:
        c.get_oracle_health()
    assert "install oracledb" in str(excinfo.value)
    assert "pip install oracledb" in str(excinfo.value)


# ------------------------------------------------------------- connect


def test_connect_probes_and_succeeds(OC, fake_oracledb):
    c = _conn(OC)
    c.connect()  # must not raise
    conn = sys.modules["oracledb"].FakeConnection.instances[-1]
    assert conn.kwargs["user"] == "rca_reader"
    assert conn.got_password  # passed through, never recorded
    assert conn.kwargs["host"] == "oradb01.example"
    assert conn.kwargs["port"] == 1521
    assert conn.kwargs["service_name"] == "ORCLPDB1"
    assert conn.kwargs["tcp_connect_timeout"] == 10
    assert "REDACTED" not in json.dumps(conn.kwargs)
    assert FAKE_SECRET not in json.dumps(conn.kwargs)
    # the probe query ran
    probe = sys.modules["oracledb"].FakeCursor.instances[0].executed[0][0]
    assert "FROM dual" in probe


def test_connect_uses_env_credentials(OC, fake_oracledb, monkeypatch):
    monkeypatch.setenv("ORACLE_READ_USER", "env_reader")
    monkeypatch.setenv("ORACLE_READ_PASSWORD", "env-pass-fake")
    c = OC(host="oradb01.example")
    c.connect()
    conn = sys.modules["oracledb"].FakeConnection.instances[-1]
    assert conn.kwargs["user"] == "env_reader"


def test_connect_unreachable_target_is_connector_error(OC, failing_oracledb):
    c = _conn(OC)
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert FAKE_SECRET not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()


def test_connect_empty_credential_provider_refuses(OC, fake_oracledb):
    c = OC(host="oradb01.example",
           credential_provider=lambda: ("", ""))
    with pytest.raises(ConnectorError):
        c.connect()


def test_close_safe_when_not_connected(OC):
    c = _conn(OC)
    c.close()  # must not raise
    c.close()


def test_reads_require_connect(OC, fake_oracledb):
    c = _conn(OC)  # not connected
    with pytest.raises(ConnectorError):
        c.get_oracle_health()
    with pytest.raises(ConnectorError):
        c.get_oracle_tablespaces()
    with pytest.raises(ConnectorError):
        c.get_oracle_blocking()
    with pytest.raises(ConnectorError):
        c.kill_oracle_session(123, 456)


# ------------------------------------------------------------- health


def test_get_oracle_health_shape_and_math(OC, fake_oracledb):
    c = _connected(OC)
    h = c.get_oracle_health()
    assert h["host"] == "oradb01.example"
    assert h["port"] == 1521
    assert h["service"] == "ORCLPDB1"
    assert h["version"].startswith("Oracle Database 19c")
    assert h["open_mode"] == "READ WRITE"
    assert h["sessions_used"] == 150
    assert h["sessions_max"] == 300
    assert h["sessions_pct"] == 50.0  # 150 / 300
    assert h["ts"].endswith("Z")
    json.dumps(h)  # JSON-serializable


def test_get_oracle_health_unlimited_sessions(OC, fake_oracledb):
    fake_oracledb["sessions_limit"] = "UNLIMITED"
    c = _connected(OC)
    h = c.get_oracle_health()
    assert h["sessions_used"] == 150
    assert h["sessions_max"] is None
    assert h["sessions_pct"] is None
    json.dumps(h)


def test_get_oracle_health_reads_use_no_binds_or_params(OC, fake_oracledb):
    """Bind discipline: the fake records params; health passes none."""
    c = _connected(OC)
    c.get_oracle_health()
    cursors = sys.modules["oracledb"].FakeCursor.instances
    selects = [e for cur in cursors for e in cur.executed
               if e[0].upper().startswith("SELECT")]
    assert selects, "expected SELECT statements to run"
    for sql, params in selects:
        assert not params, f"unexpected params on {sql!r}"


# ------------------------------------------------------------- tablespaces


def test_get_oracle_tablespaces_shape(OC, fake_oracledb):
    c = _connected(OC)
    t = c.get_oracle_tablespaces()
    assert t["host"] == "oradb01.example"
    ts = {row["name"]: row for row in t["tablespaces"]}
    assert set(ts) == {"SYSAUX", "USERS"}
    assert ts["USERS"] == {
        "name": "USERS", "used_pct": 78.9,
        "used_mb": 1612.3, "max_mb": 2048.0,
    }
    assert ts["SYSAUX"]["used_pct"] == 45.2
    assert t["ts"].endswith("Z")
    json.dumps(t)


def test_get_oracle_tablespaces_ora_00942_gives_grants_guidance(
        OC, fake_oracledb):
    """A read account without DBA-view grants gets a clear message, not a
    raw ORA-00942."""
    fake_oracledb["tablespace_ora_00942"] = True
    c = _connected(OC)
    with pytest.raises(ConnectorError) as excinfo:
        c.get_oracle_tablespaces()
    text = str(excinfo.value)
    assert "ORA-00942" in text
    assert "dba_data_files" in text
    assert "dba_free_space" in text
    assert "grant" in text.lower()


# ------------------------------------------------------------- blocking


def test_get_oracle_blocking_shape_and_snippet_truncation(OC, fake_oracledb):
    c = _connected(OC)
    b = c.get_oracle_blocking()
    assert b["host"] == "oradb01.example"
    assert len(b["blockers"]) == 2
    first, second = b["blockers"]
    assert first["sid"] == 123
    assert first["serial"] == 456
    assert first["username"] == "APP_USER"
    assert first["wait_seconds"] == 42
    assert len(first["sql_snippet"]) == 200  # truncated to 200 chars
    assert first["sql_snippet"] == " ".join(LONG_SQL.split())[:200]
    assert "\n" not in first["sql_snippet"]
    # session with no current SQL -> empty snippet, not None
    assert second["sid"] == 124
    assert second["serial"] == 789
    assert second["sql_snippet"] == ""
    assert second["wait_seconds"] == 7
    assert b["ts"].endswith("Z")
    json.dumps(b)


def test_get_oracle_blocking_empty(OC, monkeypatch):
    """No blocked sessions -> empty blockers list, still a valid shape."""
    state = {}
    mod = _make_fake_oracledb(state)

    orig_respond = mod.FakeCursor._respond

    def _no_blockers(self, sql):
        if "V$SESSION" in sql.upper():
            return [], [("SID",), ("SERIAL_NO",), ("USERNAME",),
                        ("WAIT_SECONDS",), ("SQL_TEXT",)]
        return orig_respond(self, sql)

    mod.FakeCursor._respond = _no_blockers
    monkeypatch.setitem(sys.modules, "oracledb", mod)
    c = _connected(OC)
    b = c.get_oracle_blocking()
    assert b["blockers"] == []
    json.dumps(b)


# ------------------------------------------------------------- privileged kill


def test_kill_oracle_session_shape_and_statement(OC, fake_oracledb):
    c = _connected(OC)
    result = c.kill_oracle_session(123, 456)
    assert result == {
        "sid": 123,
        "serial": 456,
        "killed": True,
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    assert fake_oracledb["kills"] == ["ALTER SYSTEM KILL SESSION '123,456'"]
    json.dumps(result)


def test_kill_uses_separate_admin_connection(OC, fake_oracledb):
    """The kill runs on its own admin connection; the read connection is
    untouched and the read credential never reaches the admin path."""
    c = _connected(OC)
    n_before = len(sys.modules["oracledb"].FakeConnection.instances)
    c.kill_oracle_session(123, 456)
    conns = sys.modules["oracledb"].FakeConnection.instances
    assert len(conns) == n_before + 1
    admin = conns[-1]
    assert admin.kwargs["user"] == "rca_admin"
    assert admin.got_password
    assert admin.closed  # short-lived: closed after the kill
    assert "rca_reader" not in json.dumps(admin.kwargs)
    assert FAKE_ADMIN_SECRET not in json.dumps(admin.kwargs)


def test_kill_validates_sid_serial(OC, fake_oracledb):
    c = _connected(OC)
    for bad in (0, -1, "abc", None, 1.5, True, "12; DROP TABLE t"):
        with pytest.raises(ConnectorError):
            c.kill_oracle_session(bad, 456)
        with pytest.raises(ConnectorError):
            c.kill_oracle_session(123, bad)
    # string digits are accepted and normalized
    result = c.kill_oracle_session("123", "456")
    assert result["sid"] == 123 and result["serial"] == 456
    assert fake_oracledb["kills"] == ["ALTER SYSTEM KILL SESSION '123,456'"]


def test_kill_refuses_without_privileged_credential(OC, fake_oracledb,
                                                   monkeypatch):
    """The privileged path must not silently reuse the read credential."""
    for var in ("ORACLE_ADMIN_USER", "ORACLE_ADMIN_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    c = OC(host="oradb01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    n_before = len(sys.modules["oracledb"].FakeConnection.instances)
    with pytest.raises(ConnectorError) as excinfo:
        c.kill_oracle_session(123, 456)
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)
    # no admin connection was even attempted
    assert len(sys.modules["oracledb"].FakeConnection.instances) == n_before


def test_kill_privileged_env_fallback(OC, fake_oracledb, monkeypatch):
    monkeypatch.setenv("ORACLE_ADMIN_USER", "env-admin")
    monkeypatch.setenv("ORACLE_ADMIN_PASSWORD", "REDACTED-fake-env-admin")
    c = OC(host="oradb01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.connect()
    result = c.kill_oracle_session(9, 10)
    admin = sys.modules["oracledb"].FakeConnection.instances[-1]
    assert admin.kwargs["user"] == "env-admin"
    assert admin.got_password
    assert result["killed"] is True
    assert fake_oracledb["kills"] == ["ALTER SYSTEM KILL SESSION '9,10'"]


def test_kill_failure_wrapped_no_secrets(OC, fake_oracledb):
    """A failed kill (e.g. no such session) surfaces ConnectorError, and the
    admin password never appears in the message."""
    state = fake_oracledb

    class BoomCursor(sys.modules["oracledb"].FakeCursor):
        def execute(self, sql, params=None):
            if sql.upper().startswith("ALTER SYSTEM"):
                raise sys.modules["oracledb"].DatabaseError(
                    "ORA-00030: User session ID does not exist.")
            super().execute(sql, params)

    orig_cursor = sys.modules["oracledb"].FakeConnection.cursor
    sys.modules["oracledb"].FakeConnection.cursor = (
        lambda self: BoomCursor(self))
    try:
        c = _connected(OC)
        with pytest.raises(ConnectorError) as excinfo:
            c.kill_oracle_session(999, 1)
    finally:
        sys.modules["oracledb"].FakeConnection.cursor = orig_cursor
    text = str(excinfo.value)
    assert "ORA-00030" in text
    assert FAKE_ADMIN_SECRET not in text
    assert "traceback" not in text.lower()


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(OC):
    c = OC(host="oradb01.example", port=1522, service="PRODPDB",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("rca_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "oradb01.example" in text


def test_no_secrets_on_instance_attrs(OC, fake_oracledb):
    c = _connected(OC)
    for attr, value in vars(c).items():
        assert FAKE_SECRET not in str(value), f"{attr} holds the secret"
        assert FAKE_ADMIN_SECRET not in str(value), f"{attr} holds the secret"


def test_read_routing_dispatches_by_name(OC, fake_oracledb):
    c = _connected(OC)
    health = c.read("get_oracle_health", {})
    assert health["sessions_pct"] == 50.0
    blockers = c.read("get_oracle_blocking", {})
    assert len(blockers["blockers"]) == 2
    with pytest.raises(ConnectorError):
        c.read("no_such_tool", {})


def test_read_rejects_privileged_action(OC, fake_oracledb):
    c = _connected(OC)
    with pytest.raises(ConnectorError):
        c.read("kill_oracle_session", {"sid": 123, "serial": 456})


def test_act_rejects_read_tools(OC, fake_oracledb):
    c = _connected(OC)
    for tool in ("get_oracle_health", "get_oracle_tablespaces",
                 "get_oracle_blocking"):
        with pytest.raises(ConnectorError):
            c.act(tool, {})


def test_act_dispatches_privileged_kill(OC, fake_oracledb):
    c = _connected(OC)
    result = c.act("kill_oracle_session", {"sid": 5, "serial": 6})
    assert result == {"sid": 5, "serial": 6, "killed": True,
                      "ts": result["ts"]}
    assert fake_oracledb["kills"] == ["ALTER SYSTEM KILL SESSION '5,6'"]


def test_all_results_json_serializable(OC, fake_oracledb):
    c = _connected(OC)
    results = [
        c.get_oracle_health(),
        c.get_oracle_tablespaces(),
        c.get_oracle_blocking(),
        c.kill_oracle_session(123, 456),
    ]
    for r in results:
        json.dumps(r)
