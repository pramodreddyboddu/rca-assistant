"""Tests for the TIBCO EMS connector (connectors/tibco_ems.py).

Fake-executor only: the connector's subprocess boundary (``conn._executor``,
called as ``executor(argv, timeout)``) is replaced with a ``FakeExecutor``
returning canned ``tibemsadmin`` output. No real EMS server, no real
credentials, and no ``tibemsadmin`` binary anywhere here.

The ``connectors.tibco_ems`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown, so running this file together
with tests/test_connector_contract.py does not change the registry that
the contract tests pin (exactly {"ibmmq", "kafka", "linux-host",
"tomcat"}).
"""

import importlib
import json
import shutil

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

SHOW_SERVER = """\
Server:                         EMS-SERVER-01
State:                          active
Connections:                    14
Version:                        10.2.0
"""

SHOW_QUEUES = """\
tibemsadmin> show queues
Queue Name                Pending Msgs   Consumers   State
------------------------- -------------- ----------- -------
queue.orders              128            4           enabled
queue.audit               1,024          1           enabled
queue.dead                0              0           disabled
this line is not a queue row
Queue Count: 3
"""

PURGE_OUTPUT = "Purged 42 messages from queue 'queue.orders'"


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def ems_mod():
    """Import connectors.tibco_ems lazily; restore the registry afterwards."""
    already = "tibco_ems" in CONNECTORS
    mod = importlib.import_module("connectors.tibco_ems")
    yield mod
    if not already:
        CONNECTORS.pop("tibco_ems", None)


@pytest.fixture(scope="module")
def TC(ems_mod):
    return ems_mod.TibcoEmsConnector


@pytest.fixture(scope="module")
def CliResult(ems_mod):
    return ems_mod.CliResult


class FakeExecutor:
    """Canned tibemsadmin output for the subprocess boundary."""

    def __init__(self, CliResult):
        self._CliResult = CliResult
        self.calls: list[tuple[list[str], int]] = []
        self.show_server = SHOW_SERVER
        self.show_queues = SHOW_QUEUES
        self.purge_output = PURGE_OUTPUT
        #: Raise this instead of returning, to test failure wrapping.
        self.fail_with: Exception | None = None
        #: Return this (returncode, stderr) to test non-zero exits.
        self.fail_rc: tuple[int, str] | None = None

    def __call__(self, argv, timeout):
        self.calls.append((list(argv), timeout))
        if self.fail_with is not None:
            raise self.fail_with
        if self.fail_rc is not None:
            code, err = self.fail_rc
            return self._CliResult(code, "", err)
        if argv[-2:] == ["show", "server"]:
            return self._CliResult(0, self.show_server, "")
        if argv[-2:] == ["show", "queues"]:
            return self._CliResult(0, self.show_queues, "")
        if argv[-3:-1] == ["purge", "queue"]:
            return self._CliResult(0, self.purge_output, "")
        raise AssertionError(f"FakeExecutor: unexpected argv {argv}")


def _fake_binary(tmp_path) -> str:
    """A real, executable file standing in for tibemsadmin on PATH."""
    path = tmp_path / "tibemsadmin"
    path.write_text("#!/bin/sh\nexit 0\n")
    path.chmod(0o755)
    return str(path)


@pytest.fixture
def conn(TC, CliResult, tmp_path):
    """A connected connector backed by FakeExecutor."""
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    c._executor = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(ems_mod):
    assert "tibco_ems" in CONNECTORS
    spec, factory = CONNECTORS["tibco_ems"]
    assert spec.name == "tibco_ems"
    assert factory is ems_mod.TibcoEmsConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description
    assert spec.credential_refs == ("EMS_READ_USER", "EMS_READ_PASSWORD")


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_ems_queues",
        "get_ems_server",
        "purge_ems_queue",
    }


def test_constructor_takes_no_raw_secret(TC):
    import inspect
    params = inspect.signature(TC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


def test_default_executor_is_real_subprocess_runner(TC, ems_mod):
    c = TC(host="ems01.example",
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    assert c._executor is ems_mod._real_executor


# ------------------------------------------------------------- connect


def test_connect_probes_show_server_with_read_identity(TC, CliResult,
                                                       tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    c._executor = fake
    c.connect()
    argv = fake.calls[0][0]
    assert argv[0].endswith("tibemsadmin")
    assert argv[-2:] == ["show", "server"]
    assert argv[argv.index("-server") + 1] == "tcp://ems01.example:7222"
    assert argv[argv.index("-user") + 1] == "ems_reader"  # read user
    assert fake.calls[0][1] == 30  # default timeout


def test_connect_uses_env_credentials(TC, CliResult, tmp_path, monkeypatch):
    monkeypatch.setenv("EMS_READ_USER", "env_reader")
    monkeypatch.setenv("EMS_READ_PASSWORD", "env-pass-fake")
    c = TC(host="ems01.example", tibemsadmin_path=_fake_binary(tmp_path))
    fake = FakeExecutor(CliResult)
    c._executor = fake
    c.connect()  # must not raise
    argv = fake.calls[0][0]
    assert argv[argv.index("-user") + 1] == "env_reader"


def test_connect_refuses_missing_explicit_binary(TC, CliResult):
    c = TC(host="ems01.example",
           tibemsadmin_path="/nonexistent/tibemsadmin",
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    c._executor = FakeExecutor(CliResult)
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "tibemsadmin" in str(excinfo.value)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_refuses_when_binary_not_on_path(TC, CliResult, monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    c = TC(host="ems01.example",
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    c._executor = FakeExecutor(CliResult)
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "PATH" in str(excinfo.value)
    assert "install" in str(excinfo.value).lower()


def test_connect_wraps_nonzero_exit(TC, CliResult, tmp_path):
    """A failing CLI surfaces ConnectorError; stderr is suppressed."""
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    fake.fail_rc = (3, "Authentication failed for 'ems_reader'")
    c._executor = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "exit code 3" in str(excinfo.value)
    assert FAKE_SECRET not in str(excinfo.value)
    assert "Authentication failed" not in str(excinfo.value)


def test_connect_wraps_executor_exception_without_secrets(TC, CliResult,
                                                          tmp_path):
    """Even a hostile executor exception cannot leak argv/credentials."""
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    fake.fail_with = RuntimeError(f"boom {FAKE_SECRET}")
    c._executor = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_unparseable_probe(TC, CliResult, tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    fake.show_server = "tibemsadmin: command not recognized\n"
    c._executor = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "unparseable" in str(excinfo.value)


def test_connect_empty_credential_provider_refuses(TC, CliResult, tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("", ""))
    c._executor = FakeExecutor(CliResult)
    with pytest.raises(ConnectorError):
        c.connect()


def test_timeout_reaches_executor(TC, CliResult, tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path), timeout=5,
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    c._executor = fake
    c.connect()
    assert fake.calls[0][1] == 5


def test_reads_require_connect(TC, CliResult, tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    c._executor = FakeExecutor(CliResult)
    with pytest.raises(ConnectorError):
        c.get_ems_queues()
    with pytest.raises(ConnectorError):
        c.get_ems_server()
    with pytest.raises(ConnectorError):
        c.purge_ems_queue("queue.orders")


# ------------------------------------------------------------- queue reads


def test_get_ems_queues_shape_and_parsing(conn):
    result = conn.get_ems_queues()
    assert result["host"] == "ems01.example"
    queues = {q["name"]: q for q in result["queues"]}
    assert set(queues) == {"queue.orders", "queue.audit", "queue.dead"}
    assert queues["queue.orders"] == {
        "name": "queue.orders", "pending_messages": 128,
        "consumers": 4, "state": "enabled",
    }
    # Comma-separated thousands are normalized, not dropped.
    assert queues["queue.audit"]["pending_messages"] == 1024
    assert queues["queue.dead"]["state"] == "disabled"
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_ems_queues_skips_garbage_never_crashes(TC, CliResult, tmp_path):
    """Preamble, garbage rows, and footers are skipped; header located by
    column, so the parse never raises on real-world CLI noise."""
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    fake.show_queues = (
        "TIBCO Enterprise Message Service\n"
        "tibemsadmin> show queues\n"
        "Queue Name         Pending Msgs  Consumers  State\n"
        "------------------ ------------- ---------- -----\n"
        "q.good             7             2          enabled\n"
        "not a queue row at all\n"
        "q.bad              N/A           x          ???\n"
        "\n"
    )
    c._executor = fake
    c.connect()
    result = c.get_ems_queues()
    assert [q["name"] for q in result["queues"]] == ["q.good"]
    assert result["queues"][0]["pending_messages"] == 7
    json.dumps(result)


def test_get_ems_queues_empty_when_no_table(TC, CliResult, tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    fake.show_queues = "No queues.\n"
    c._executor = fake
    c.connect()
    assert c.get_ems_queues()["queues"] == []


def test_get_ems_queues_sends_show_queues(conn):
    conn.get_ems_queues()
    argv = conn._fake.calls[-1][0]
    assert argv[-2:] == ["show", "queues"]
    assert argv[argv.index("-user") + 1] == "ems_reader"


# ------------------------------------------------------------- server read


def test_get_ems_server_shape(conn):
    server = conn.get_ems_server()
    assert server == {
        "host": "ems01.example",
        "server": "EMS-SERVER-01",
        "state": "active",
        "connections": 14,
        "version": "10.2.0",
        "ts": server["ts"],
    }
    assert server["ts"].endswith("Z")
    json.dumps(server)


def test_get_ems_server_missing_connections_is_none(TC, CliResult, tmp_path):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    fake = FakeExecutor(CliResult)
    fake.show_server = "Server:  EMS-X\nState:   active\nVersion: 9.0\n"
    c._executor = fake
    c.connect()
    server = c.get_ems_server()
    assert server["connections"] is None
    assert server["server"] == "EMS-X"
    assert server["version"] == "9.0"


# ------------------------------------------------------------- privileged purge


def _admin_conn(TC, CliResult, tmp_path, **kwargs):
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("ems_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    fake = FakeExecutor(CliResult)
    c._executor = fake
    c.connect()
    c._fake = fake
    return c


def test_purge_ems_queue_shape_and_argv(TC, CliResult, tmp_path):
    c = _admin_conn(TC, CliResult, tmp_path)
    result = c.purge_ems_queue("queue.orders")
    assert result == {
        "queue": "queue.orders",
        "messages_purged": 42,
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    argv = c._fake.calls[-1][0]
    # Single argv elements: `purge queue <name>`, no shell involved.
    assert argv[-3:] == ["purge", "queue", "queue.orders"]
    # Privileged call used the admin identity, not the read user.
    assert argv[argv.index("-user") + 1] == "ems_admin"
    assert argv[argv.index("-password") + 1] == FAKE_ADMIN_SECRET
    json.dumps(result)


def test_purge_refuses_without_admin_credential(TC, CliResult, tmp_path,
                                                monkeypatch):
    monkeypatch.delenv("EMS_ADMIN_USER", raising=False)
    monkeypatch.delenv("EMS_ADMIN_PASSWORD", raising=False)
    c = TC(host="ems01.example",
           tibemsadmin_path=_fake_binary(tmp_path),
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    c._executor = FakeExecutor(CliResult)
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_ems_queue("queue.orders")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_purge_refuses_when_binary_missing(TC, CliResult, tmp_path):
    c = _admin_conn(TC, CliResult, tmp_path)
    c._tibemsadmin_path = "/nonexistent/tibemsadmin"  # binary vanished
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_ems_queue("queue.orders")
    assert "tibemsadmin" in str(excinfo.value)
    assert FAKE_ADMIN_SECRET not in str(excinfo.value)


def test_purge_rejects_empty_queue(TC, CliResult, tmp_path):
    c = _admin_conn(TC, CliResult, tmp_path)
    with pytest.raises(ConnectorError):
        c.purge_ems_queue("   ")


def test_purge_without_count_reports_none(TC, CliResult, tmp_path):
    c = _admin_conn(TC, CliResult, tmp_path)
    c._fake.purge_output = "OK"
    result = c.purge_ems_queue("queue.orders")
    assert result["messages_purged"] is None
    assert result["queue"] == "queue.orders"


def test_purge_nonzero_exit_wraps(TC, CliResult, tmp_path):
    c = _admin_conn(TC, CliResult, tmp_path)
    c._fake.fail_rc = (1, "Queue 'queue.orders' does not exist")
    with pytest.raises(ConnectorError) as excinfo:
        c.purge_ems_queue("queue.orders")
    assert "exit code 1" in str(excinfo.value)
    assert FAKE_ADMIN_SECRET not in str(excinfo.value)


# ------------------------------------------------------------- hygiene & routing


def test_credentials_travel_in_argv_never_in_errors(conn):
    """The documented transport (argv -user/-password) is explicit here;
    the guarantee is that they never reach error text."""
    argv = conn._fake.calls[0][0]
    assert argv[argv.index("-password") + 1] == FAKE_SECRET
    assert argv[0].endswith("tibemsadmin")


def test_repr_contains_no_secrets(TC):
    c = TC(host="ems01.example",
           credential_provider=lambda: ("ems_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("ems_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "ems01.example" in text


def test_instance_attrs_hold_no_secrets(TC, CliResult, tmp_path):
    c = _admin_conn(TC, CliResult, tmp_path)  # connected, creds resolved
    for attr, value in vars(c).items():
        try:
            text = str(value)
        except Exception:
            continue
        assert FAKE_SECRET not in text, attr
        assert FAKE_ADMIN_SECRET not in text, attr


def test_close_is_safe_when_not_connected(TC):
    c = TC(host="ems01.example",
           credential_provider=lambda: ("ems_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    result = conn.read("get_ems_queues", {})
    assert len(result["queues"]) == 3
    server = conn.read("get_ems_server", {})
    assert server["server"] == "EMS-SERVER-01"
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
