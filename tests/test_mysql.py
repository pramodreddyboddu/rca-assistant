"""Tests for the real MySQL connector (connectors/mysql.py).

pymysql is NOT installed here and no live MySQL is touched. A fake
``pymysql`` module is injected into ``sys.modules`` (and removed after
each test by monkeypatch), modelling pymysql's PUBLIC API:
``pymysql.connect(**kwargs)``, connection ``.cursor(...)`` context
managers, ``pymysql.cursors.DictCursor``, and ``cursor.execute(sql,
params)`` / ``fetchall()``. It covers health, processlist (own-row skip,
200-char snippet truncation, empty list), replication (replica role,
primary role, SLAVE fallback + spelling normalization), the privileged
KILL QUERY path, and credential hygiene. No test fixture contains a real
secret: placeholders like "REDACTED-fake" stand in, and the hygiene tests
assert they never leak into repr, exceptions, or logs.

The ``connectors.mysql`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown -- the same pattern as
tests/test_tomcat.py -- so collecting/running this file does not change
the registry that tests/test_connector_contract.py pins.
"""

import importlib
import json
import subprocess
import sys
import types

import pytest

from connectors.base import CONNECTORS, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def mysql_mod():
    """Import connectors.mysql lazily; deregister it afterwards.

    The module self-registers on import; this package's ``__init__`` does
    not import it (gateway wiring lands separately), so the teardown pops
    the entry again to keep the global registry pinned for the contract
    tests.
    """
    had = "mysql" in CONNECTORS
    mod = importlib.import_module("connectors.mysql")
    yield mod
    if not had:
        CONNECTORS.pop("mysql", None)


@pytest.fixture(scope="module")
def MC(mysql_mod):
    return mysql_mod.MySQLConnector


# ------------------------------------------------------------- fake pymysql


class FakeMySQLError(Exception):
    pass


def _make_fake_pymysql(state, fail_connect=False):
    """Build a fake `pymysql` module modelling the PUBLIC API we use.

    ``pymysql.connect(**kwargs)`` returns a FakeConnection whose
    ``.cursor(...)`` is a context manager exposing ``execute(sql,
    params)`` and ``fetchall()``. Canned rows are chosen by SQL prefix
    from ``state``; every (sql, params) pair is recorded on the
    connection so tests can prove queries were parameterized, never
    string-formatted.
    """
    mod = types.ModuleType("pymysql")
    mod.cursors = types.SimpleNamespace(DictCursor=type("DictCursor", (), {}))

    def _rows_for(sql, params):
        s = sql.strip().upper()
        if s.startswith("SELECT VERSION()"):
            return [{"VERSION()": state["version"]}]
        if s.startswith("SHOW GLOBAL STATUS"):
            return [{"Variable_name": k, "Value": str(v)}
                    for k, v in state["status"].items()]
        if s.startswith("SHOW GLOBAL VARIABLES"):
            return [{"Variable_name": "max_connections",
                     "Value": str(state["max_connections"])}]
        if s.startswith("SHOW FULL PROCESSLIST"):
            return [dict(r) for r in state["processlist"]]
        if s.startswith("SELECT CONNECTION_ID()"):
            return [{"CONNECTION_ID()": state["connection_id"]}]
        if s.startswith("SHOW REPLICA STATUS"):
            if state.get("replica_unsupported"):
                raise FakeMySQLError(
                    1064, "You have an error in your SQL syntax near "
                          "'REPLICA STATUS'")
            return [dict(r) for r in state["replica_status"]]
        if s.startswith("SHOW SLAVE STATUS"):
            return [dict(r) for r in state["slave_status"]]
        if s.startswith("KILL QUERY"):
            return []
        raise AssertionError(f"fake pymysql: unexpected SQL {sql!r}")

    class FakeCursor:
        def __init__(self, conn):
            self._conn = conn
            self._sql = None
            self._params = None

        def execute(self, sql, params=None):
            self._sql = sql
            self._params = params
            self._conn.queries.append((sql, params))

        def fetchall(self):
            return _rows_for(self._sql, self._params)

        def fetchone(self):
            rows = _rows_for(self._sql, self._params)
            return rows[0] if rows else None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class FakeConnection:
        instances = []

        def __init__(self, **kwargs):
            self._kwargs = kwargs
            # Deliberately records everything EXCEPT the password value.
            self.recorded = {k: v for k, v in kwargs.items()
                             if k != "password"}
            self.got_password = bool(kwargs.get("password"))
            self.closed = False
            self.queries: list = []
            FakeConnection.instances.append(self)

        def cursor(self, cursorclass=None):
            return FakeCursor(self)

        def close(self):
            self.closed = True

    def fake_connect(**kwargs):
        if fail_connect or "unreachable" in str(kwargs.get("host", "")):
            raise FakeMySQLError(2003, "Can't connect to MySQL server")
        return FakeConnection(**kwargs)

    mod.connect = fake_connect
    mod.MySQLError = FakeMySQLError
    mod.FakeConnection = FakeConnection
    return mod


def _fresh_state():
    long_query = ("SELECT * FROM orders WHERE customer_id = 12345 AND "
                  "status = 'PENDING' " + "x" * 300)
    return {
        "version": "8.0.36",
        "status": {"Uptime": "86400", "Threads_connected": "17",
                   "Slow_queries": "5"},
        "max_connections": 151,
        "connection_id": 42,
        "processlist": [
            {"Id": 42, "User": "rca_reader", "Host": "10.0.0.9:51234",
             "db": None, "Command": "Query", "Time": 0,
             "State": "executing", "Info": "SHOW FULL PROCESSLIST"},
            {"Id": 7, "User": "app", "Host": "10.0.0.5:33060",
             "db": "orders", "Command": "Query", "Time": 125,
             "State": "Sending data", "Info": long_query},
            {"Id": 9, "User": "etl", "Host": "10.0.0.6:44010",
             "db": "warehouse", "Command": "Sleep", "Time": 3600,
             "State": "", "Info": None},
        ],
        "replica_status": [
            {"Replica_IO_Running": "Yes", "Replica_SQL_Running": "Yes",
             "Seconds_Behind_Source": 3, "Source_Host": "db-primary-1"},
        ],
        "slave_status": [],
        "replica_unsupported": False,
    }


@pytest.fixture()
def fake_pymysql(monkeypatch):
    """Inject the fake pymysql module and return its backing state."""
    state = _fresh_state()
    monkeypatch.setitem(sys.modules, "pymysql", _make_fake_pymysql(state))
    return state


@pytest.fixture()
def failing_pymysql(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "pymysql",
        _make_fake_pymysql(state, fail_connect=True))
    return state


def _conn(MC, **kwargs):
    kwargs.setdefault("credential_provider",
                      lambda: ("rca_reader", FAKE_SECRET))
    kwargs.setdefault("privileged_credential_provider",
                      lambda: ("rca_admin", FAKE_ADMIN_SECRET))
    return MC(host="db01.example", **kwargs)


def _connected(MC, **kwargs):
    """A connector whose connect() succeeded against the fake driver."""
    conn = _conn(MC, **kwargs)
    conn.connect()
    return conn


def _conns():
    return sys.modules["pymysql"].FakeConnection.instances


# ------------------------------------------------------------- guarded import


def test_module_imports_cleanly_without_pymysql():
    """The driver must never be needed at import time (subprocess proof)."""
    code = (
        "import sys; "
        "assert 'pymysql' not in sys.modules, 'pymysql unexpectedly present'; "
        "from connectors.mysql import MySQLConnector; "
        "assert 'pymysql' not in sys.modules, 'import pulled in pymysql'; "
        "print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_driver_absent_gives_install_instructions(MC, monkeypatch):
    monkeypatch.delitem(sys.modules, "pymysql", raising=False)
    conn = _conn(MC)
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert "pymysql" in str(excinfo.value)
    assert "pip install pymysql" in str(excinfo.value)


def test_no_driver_import_at_module_top():
    """pymysql must only be imported lazily (inside _driver), never at
    module top."""
    import ast
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "connectors" / "mysql.py"
           ).read_text(encoding="utf-8")
    tree = ast.parse(src)
    top_imports = set()
    for node in tree.body:  # module top level only; lazy imports allowed
        if isinstance(node, ast.Import):
            top_imports.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            top_imports.add((node.module or "").split(".")[0])
    assert "pymysql" not in top_imports


# ------------------------------------------------------------- registration


def test_module_registers_connector(mysql_mod, MC):
    assert "mysql" in CONNECTORS
    spec, factory = CONNECTORS["mysql"]
    assert spec.name == "mysql"
    assert factory is MC
    assert spec.display_name and spec.description


def test_capabilities_exact_set(MC, fake_pymysql):
    conn = _conn(MC)
    assert set(conn.capabilities()) == {
        "get_mysql_health",
        "get_mysql_processlist",
        "get_mysql_replication",
        "kill_mysql_query",
    }


def test_constructor_takes_no_raw_secret(MC):
    import inspect
    params = inspect.signature(MC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned, (
            f"MySQLConnector.__init__ takes a raw secret argument {pname!r}"
        )


# ------------------------------------------------------------- connect


def test_connect_probes_with_version(MC, fake_pymysql):
    conn = _conn(MC)
    conn.connect()
    read_conn = _conns()[-1]
    assert read_conn.recorded["host"] == "db01.example"
    assert read_conn.recorded["port"] == 3306
    assert read_conn.recorded["user"] == "rca_reader"
    assert read_conn.got_password  # passed through, never recorded
    assert FAKE_SECRET not in json.dumps(read_conn.recorded)
    assert ("SELECT VERSION()", ()) in read_conn.queries  # probe ran
    assert not read_conn.closed


def test_connect_uses_env_credentials(MC, fake_pymysql, monkeypatch):
    monkeypatch.setenv("MYSQL_READ_USER", "env_reader")
    monkeypatch.setenv("MYSQL_READ_PASSWORD", "env-pass-fake")
    conn = MC(host="db01.example")
    conn.connect()  # must not raise
    assert _conns()[-1].recorded["user"] == "env_reader"


def test_connect_unreachable_target_is_connector_error(MC, failing_pymysql):
    conn = MC(
        host="unreachable.invalid",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert FAKE_SECRET not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()
    assert not isinstance(excinfo.value, FakeMySQLError)


def test_connect_failure_leaves_no_half_open_connection(MC, failing_pymysql):
    conn = MC(
        host="db01.example",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
    )
    with pytest.raises(ConnectorError):
        conn.connect()
    assert conn._connection is None


def test_close_safe_when_not_connected(MC, fake_pymysql):
    conn = _conn(MC)
    conn.close()  # must not raise
    conn.close()


def test_close_closes_read_connection(MC, fake_pymysql):
    conn = _connected(MC)
    read_conn = _conns()[-1]
    conn.close()
    assert read_conn.closed


def test_reads_require_connect(MC, fake_pymysql):
    conn = _conn(MC)
    with pytest.raises(ConnectorError):
        conn.get_mysql_health()
    with pytest.raises(ConnectorError):
        conn.get_mysql_processlist()
    with pytest.raises(ConnectorError):
        conn.get_mysql_replication()
    with pytest.raises(ConnectorError):
        conn.kill_mysql_query(7)


def test_empty_credential_provider_refuses(MC, fake_pymysql):
    conn = MC(host="db01.example",
              credential_provider=lambda: ("", ""))
    with pytest.raises(ConnectorError):
        conn.connect()


# ------------------------------------------------------------- health


def test_get_mysql_health_shape_and_math(MC, fake_pymysql):
    conn = _connected(MC)
    d = conn.get_mysql_health()
    assert set(d) == {"host", "port", "version", "uptime_secs",
                      "threads_connected", "max_connections",
                      "connections_pct", "slow_queries", "ts"}
    assert d["host"] == "db01.example"
    assert d["port"] == 3306
    assert d["version"] == "8.0.36"
    assert d["uptime_secs"] == 86400
    assert d["threads_connected"] == 17
    assert d["max_connections"] == 151
    assert d["connections_pct"] == 11.3  # 17/151*100 rounded to 1dp
    assert d["slow_queries"] == 5
    assert d["ts"].endswith("Z")
    json.dumps(d)


def test_get_mysql_health_zero_max_connections(MC, fake_pymysql):
    state = fake_pymysql
    state["max_connections"] = 0
    conn = _connected(MC)
    d = conn.get_mysql_health()
    assert d["max_connections"] == 0
    assert d["connections_pct"] is None


# ------------------------------------------------------------- processlist


def test_get_mysql_processlist_shape_skips_own_row(MC, fake_pymysql):
    conn = _connected(MC)
    d = conn.get_mysql_processlist()
    assert set(d) == {"host", "processes", "ts"}
    assert d["host"] == "db01.example"
    procs = {p["id"]: p for p in d["processes"]}
    # own session (Id 42) is skipped; 7 and 9 remain
    assert sorted(procs) == [7, 9]
    assert procs[7] == {
        "id": 7, "user": "app", "db": "orders", "command": "Query",
        "time_secs": 125, "state": "Sending data",
        "query_snippet": procs[7]["query_snippet"],
    }
    assert procs[9]["query_snippet"] is None  # Sleep, no statement
    assert procs[9]["time_secs"] == 3600
    json.dumps(d)


def test_get_mysql_processlist_truncates_query_snippet(MC, fake_pymysql):
    conn = _connected(MC)
    d = conn.get_mysql_processlist()
    snippet = {p["id"]: p for p in d["processes"]}[7]["query_snippet"]
    assert len(snippet) == 200
    assert snippet == fake_pymysql["processlist"][1]["Info"][:200]


def test_get_mysql_processlist_empty(MC, fake_pymysql):
    state = fake_pymysql
    state["processlist"] = []
    conn = _connected(MC)
    d = conn.get_mysql_processlist()
    assert d["processes"] == []
    json.dumps(d)


# ------------------------------------------------------------- replication


def test_get_mysql_replication_replica(MC, fake_pymysql):
    conn = _connected(MC)
    d = conn.get_mysql_replication()
    assert set(d) == {"host", "role", "io_running", "sql_running",
                      "seconds_behind_source", "ts"}
    assert d["host"] == "db01.example"
    assert d["role"] == "replica"
    assert d["io_running"] == "Yes"
    assert d["sql_running"] == "Yes"
    assert d["seconds_behind_source"] == 3
    json.dumps(d)


def test_get_mysql_replication_primary_when_no_rows(MC, fake_pymysql):
    state = fake_pymysql
    state["replica_status"] = []
    conn = _connected(MC)
    d = conn.get_mysql_replication()
    assert d["role"] == "primary"
    assert d["io_running"] is None
    assert d["sql_running"] is None
    assert d["seconds_behind_source"] is None
    json.dumps(d)


def test_get_mysql_replication_falls_back_to_slave_status(MC, fake_pymysql):
    """Old servers reject REPLICA syntax: fall back to SLAVE STATUS and
    normalize the old column spellings."""
    state = fake_pymysql
    state["replica_unsupported"] = True
    state["slave_status"] = [
        {"Slave_IO_Running": "Yes", "Slave_SQL_Running": "No",
         "Seconds_Behind_Master": 12},
    ]
    conn = _connected(MC)
    d = conn.get_mysql_replication()
    assert d["role"] == "replica"
    assert d["io_running"] == "Yes"
    assert d["sql_running"] == "No"
    assert d["seconds_behind_source"] == 12
    read_conn = _conns()[-1]
    issued = [q[0] for q in read_conn.queries]
    assert "SHOW REPLICA STATUS" in issued
    assert "SHOW SLAVE STATUS" in issued
    json.dumps(d)


def test_get_mysql_replication_null_lag_stays_none(MC, fake_pymysql):
    state = fake_pymysql
    state["replica_status"] = [
        {"Replica_IO_Running": "Yes", "Replica_SQL_Running": "No",
         "Seconds_Behind_Source": None},
    ]
    conn = _connected(MC)
    d = conn.get_mysql_replication()
    assert d["role"] == "replica"
    assert d["seconds_behind_source"] is None
    json.dumps(d)


# ------------------------------------------------------------- privileged kill


def test_kill_mysql_query_parameterized_and_shaped(MC, fake_pymysql):
    conn = _connected(MC)
    d = conn.kill_mysql_query(7)
    assert set(d) == {"process_id", "killed", "ts"}
    assert d == {"process_id": 7, "killed": True, "ts": d["ts"]}
    assert d["ts"].endswith("Z")
    json.dumps(d)
    # The KILL went out on a SEPARATE admin connection, parameterized.
    admin_conn = _conns()[-1]
    assert admin_conn.recorded["user"] == "rca_admin"
    assert admin_conn.got_password
    assert FAKE_ADMIN_SECRET not in json.dumps(admin_conn.recorded)
    assert admin_conn.queries == [("KILL QUERY %s", (7,))]
    # The id was never interpolated into the SQL text.
    sql, params = admin_conn.queries[0]
    assert sql == "KILL QUERY %s"
    assert params == (7,)
    assert "7" not in sql.replace("%s", "")
    # The admin connection is closed again before returning.
    assert admin_conn.closed
    # The read connection never issued a KILL.
    read_conn = _conns()[0]
    assert all(not q[0].startswith("KILL") for q in read_conn.queries)


def test_kill_mysql_query_id_validation(MC, fake_pymysql):
    conn = _connected(MC)
    for bad in (0, -1, -999, "7", 7.5, None, True, False):
        with pytest.raises(ConnectorError) as excinfo:
            conn.kill_mysql_query(bad)
        assert FAKE_ADMIN_SECRET not in str(excinfo.value)
    # No KILL reached any connection for the invalid ids.
    assert all(not q[0].startswith("KILL")
               for c in _conns() for q in c.queries)


def test_kill_mysql_query_refuses_without_privileged_credential(
        MC, fake_pymysql, monkeypatch):
    """The privileged path must not silently reuse the read credential."""
    for var in ("MYSQL_ADMIN_USER", "MYSQL_ADMIN_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    conn = MC(
        host="db01.example",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
    )
    conn.connect()
    before = len(_conns())
    with pytest.raises(ConnectorError) as excinfo:
        conn.kill_mysql_query(7)
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)
    assert FAKE_ADMIN_SECRET not in str(excinfo.value)
    # No admin connection was even opened.
    assert len(_conns()) == before


def test_kill_mysql_query_privileged_env_fallback(
        MC, fake_pymysql, monkeypatch):
    monkeypatch.setenv("MYSQL_ADMIN_USER", "env-admin")
    monkeypatch.setenv("MYSQL_ADMIN_PASSWORD", "REDACTED-fake-env-admin")
    conn = MC(
        host="db01.example",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
    )
    conn.connect()
    d = conn.kill_mysql_query(9)
    assert d["killed"] is True and d["process_id"] == 9
    admin_conn = _conns()[-1]
    assert admin_conn.recorded["user"] == "env-admin"
    assert admin_conn.got_password
    assert "REDACTED-fake-env-admin" not in json.dumps(admin_conn.recorded)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(MC, fake_pymysql):
    conn = _conn(MC)
    text = repr(conn)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "db01.example" in text


def test_no_instance_attribute_holds_secrets(MC, fake_pymysql):
    conn = _connected(MC)
    for attr, value in vars(conn).items():
        text = str(value)
        assert FAKE_SECRET not in text, f"{attr} leaks the read secret"
        assert FAKE_ADMIN_SECRET not in text, f"{attr} leaks the admin secret"


def test_read_rejects_privileged_action(MC, fake_pymysql):
    """read() must never reach the KILL path: kill_mysql_query is a
    privileged tool, so read() refuses it by name before any method is
    resolved."""
    from mcp_server.tools import PRIVILEGED_TOOLS
    assert "kill_mysql_query" in PRIVILEGED_TOOLS  # gateway wiring
    conn = _conn(MC)
    with pytest.raises(ConnectorError):
        conn.read("kill_mysql_query", {"process_id": 7})


def test_act_dispatches_kill(MC, fake_pymysql):
    """act() routes the privileged KILL by name once the gateway marks it
    privileged."""
    from mcp_server.tools import PRIVILEGED_TOOLS
    assert "kill_mysql_query" in PRIVILEGED_TOOLS  # gateway wiring
    conn = _connected(MC)
    d = conn.act("kill_mysql_query", {"process_id": 7})
    assert d["killed"] is True and d["process_id"] == 7


def test_read_dispatch_reaches_read_tools(MC, fake_pymysql):
    conn = _connected(MC)
    assert conn.read("get_mysql_health", {})["version"] == "8.0.36"
    assert len(conn.read("get_mysql_processlist", {})["processes"]) == 2
    assert conn.read("get_mysql_replication", {})["role"] == "replica"
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})


def test_all_results_json_serializable(MC, fake_pymysql):
    conn = _connected(MC)
    results = [
        conn.get_mysql_health(),
        conn.get_mysql_processlist(),
        conn.get_mysql_replication(),
        conn.kill_mysql_query(7),
    ]
    for r in results:
        json.dumps(r)
