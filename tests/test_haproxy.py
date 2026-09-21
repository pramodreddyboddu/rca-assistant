"""Tests for the real HAProxy connector (connectors/haproxy.py).

No live HAProxy is touched and no real credentials exist anywhere here.
The connector's stats HTTP boundary (``conn._stats``, exposing
``get_text``) is replaced with a ``FakeStatsTransport`` returning canned
stats CSV, and the runtime-API channel is faked with a
``runtime_executor`` callable recording the exact command sent.
Placeholders like "REDACTED-fake" stand in for secrets, and hygiene
tests assert they never leak into repr, exceptions, or logs.

The ``connectors.haproxy`` module is imported lazily inside a
module-scoped fixture (never at module top) and REMOVED from the global
``CONNECTORS`` registry on teardown. Unlike tomcat/kafka, this module is
not imported by ``connectors/__init__``, so the registry must be
restored to its pre-test state to keep
tests/test_connector_contract.py's registry pin intact.
"""

import importlib
import inspect
import json
import urllib.error

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"

_CSV_COLS = [
    "# pxname", "svname", "qcur", "qmax", "scur", "smax", "slim", "stot",
    "status", "weight", "act", "bck", "check_status", "check_code",
    "check_duration",
]


def _row(pxname, svname, scur="", status="", check_status=""):
    vals = {c: "" for c in _CSV_COLS}
    vals["# pxname"] = pxname
    vals["svname"] = svname
    vals["scur"] = str(scur)
    vals["status"] = status
    vals["check_status"] = check_status
    return ",".join(vals[c] for c in _CSV_COLS)


FAKE_STATS_CSV = "\n".join([
    ",".join(_CSV_COLS),
    _row("web_front", "FRONTEND", scur=12, status="OPEN"),
    _row("app_back", "BACKEND", scur=5, status="UP"),
    _row("app_back", "web1", scur=3, status="UP",
         check_status="L7OK/200 in 1ms"),
    _row("app_back", "web2", scur=0, status="MAINT"),
    _row("stats", "FRONTEND", scur=0, status="OPEN"),
    _row("stats", "BACKEND", scur=0, status="UP"),
]) + "\n"


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def haproxy_mod():
    """Import connectors.haproxy lazily; remove its registration afterwards.

    The module self-registers on import; teardown pops it so the
    registry is exactly what connectors/__init__ left behind.
    """
    had = "haproxy" in CONNECTORS
    mod = importlib.import_module("connectors.haproxy")
    yield mod
    if not had:
        CONNECTORS.pop("haproxy", None)


@pytest.fixture(scope="module")
def TC(haproxy_mod):
    return haproxy_mod.HAProxyConnector


class FakeStatsTransport:
    """Canned stats CSV for the HTTP boundary."""

    def __init__(self, csv_text=FAKE_STATS_CSV):
        self.calls: list[tuple[str, str | None]] = []  # (url, user)
        self.csv_text = csv_text
        self.fail_with: Exception | None = None

    def get_text(self, url, auth):
        user = auth[0] if auth else None
        self.calls.append((url, user))
        if self.fail_with is not None:
            raise self.fail_with
        return self.csv_text


class FakeRuntime:
    """Fake runtime-API executor: records commands, returns canned text."""

    def __init__(self):
        self.commands: list[str] = []
        self.response = ""  # success: empty, like the real runtime API
        self.fail_with: Exception | None = None

    def __call__(self, command):
        self.commands.append(command)
        if self.fail_with is not None:
            raise self.fail_with
        return self.response


@pytest.fixture
def conn(TC):
    """A connected connector backed by the fake stats transport + fake runtime."""
    c = TC(host="haproxy01.example", port=8404,
           runtime_executor=FakeRuntime())
    fake_stats = FakeStatsTransport()
    c._stats = fake_stats
    c.connect()
    c._fake_stats = fake_stats  # test introspection only; never real attrs
    c._fake_runtime = c._runtime_executor
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(haproxy_mod):
    assert "haproxy" in CONNECTORS
    spec, factory = CONNECTORS["haproxy"]
    assert spec.name == "haproxy"
    assert factory is haproxy_mod.HAProxyConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_haproxy_stats",
        "set_haproxy_server_state",
    }


def test_constructor_takes_no_raw_secret(TC):
    params = inspect.signature(TC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


def test_default_config_values(TC):
    c = TC(host="h")
    assert c._stats_url == "http://h:8404/stats;csv"
    assert c._socket_path == "/var/run/haproxy/admin.sock"
    assert c._runtime_executor is None
    custom = TC(host="h", port=9999, stats_path="custom;csv",
                socket_path="/tmp/sock")
    assert custom._stats_url == "http://h:9999/custom;csv"
    assert custom._socket_path == "/tmp/sock"


# ------------------------------------------------------------- connect


def test_connect_probes_stats_csv_url(TC):
    c = TC(host="haproxy01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake_stats = FakeStatsTransport()
    c._stats = fake_stats
    c.connect()
    assert fake_stats.calls[0][0] == "http://haproxy01.example:8404/stats;csv"
    assert fake_stats.calls[0][1] == "rca_reader"


def test_connect_uses_env_credentials(TC, monkeypatch):
    monkeypatch.setenv("HAPROXY_STATS_USER", "env_reader")
    monkeypatch.setenv("HAPROXY_STATS_PASSWORD", "env-pass-fake")
    c = TC(host="haproxy01.example")
    fake_stats = FakeStatsTransport()
    c._stats = fake_stats
    c.connect()  # must not raise
    assert fake_stats.calls[0][1] == "env_reader"


def test_connect_anonymous_by_default(TC, monkeypatch):
    monkeypatch.delenv("HAPROXY_STATS_USER", raising=False)
    monkeypatch.delenv("HAPROXY_STATS_PASSWORD", raising=False)
    c = TC(host="haproxy01.example")
    fake_stats = FakeStatsTransport()
    c._stats = fake_stats
    c.connect()  # must not raise: stats endpoints are often unauthenticated
    assert fake_stats.calls[0][1] is None


def test_connect_unreachable_raises_connector_error_not_raw(TC):
    """A dead stats listener must surface ConnectorError, never a raw URLError."""
    c = TC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake_stats = FakeStatsTransport()
    fake_stats.fail_with = urllib.error.URLError("connection refused")
    c._stats = fake_stats
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_non_csv_body(TC):
    """An HTML login page (or any non-CSV) must fail loudly, not parse as stats."""
    c = TC(host="haproxy01.example")
    fake_stats = FakeStatsTransport(csv_text="<html><body>stats</body></html>")
    c._stats = fake_stats
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "CSV" in str(excinfo.value)


def test_connect_rejects_half_configured_auth(TC, monkeypatch):
    monkeypatch.setenv("HAPROXY_STATS_USER", "env_reader")
    monkeypatch.delenv("HAPROXY_STATS_PASSWORD", raising=False)
    c = TC(host="haproxy01.example")
    c._stats = FakeStatsTransport()
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(TC):
    c = TC(host="haproxy01.example", runtime_executor=FakeRuntime())
    c._stats = FakeStatsTransport()
    with pytest.raises(ConnectorError):
        c.get_haproxy_stats()
    with pytest.raises(ConnectorError):
        c.set_haproxy_server_state("app_back", "web1", "drain")


def test_empty_credential_provider_refuses(TC):
    c = TC(host="haproxy01.example",
           credential_provider=lambda: ("", ""))
    c._stats = FakeStatsTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- stats reads


def test_get_haproxy_stats_shape(conn):
    stats = conn.get_haproxy_stats()
    assert stats["host"] == "haproxy01.example"
    assert stats["port"] == 8404
    assert [f["name"] for f in stats["frontends"]] == ["stats", "web_front"]
    web_front = stats["frontends"][1]
    assert web_front == {
        "name": "web_front", "status": "OPEN", "current_sessions": 12}
    assert [b["name"] for b in stats["backends"]] == ["app_back", "stats"]
    app_back = stats["backends"][0]
    assert app_back["status"] == "UP"
    assert app_back["servers"] == [
        {"name": "web1", "status": "UP", "current_sessions": 3,
         "check_status": "L7OK/200 in 1ms"},
        {"name": "web2", "status": "MAINT", "current_sessions": 0,
         "check_status": ""},
    ]
    assert stats["backends"][1]["servers"] == []  # stats backend: no servers
    assert stats["ts"].endswith("Z")
    json.dumps(stats)  # JSON-serializable


def test_get_haproxy_stats_header_only_csv(TC):
    c = TC(host="haproxy01.example", runtime_executor=FakeRuntime())
    c._stats = FakeStatsTransport(csv_text=",".join(_CSV_COLS) + "\n")
    c.connect()
    stats = c.get_haproxy_stats()
    assert stats["frontends"] == []
    assert stats["backends"] == []


def test_get_haproxy_stats_rejects_missing_column(TC):
    cols = [c for c in _CSV_COLS if c != "check_status"]
    rows = [",".join(cols)]
    for line in FAKE_STATS_CSV.splitlines()[1:]:
        vals = line.split(",")
        keep = [v for c, v in zip(_CSV_COLS, vals) if c != "check_status"]
        rows.append(",".join(keep))
    c = TC(host="haproxy01.example", runtime_executor=FakeRuntime())
    c._stats = FakeStatsTransport(csv_text="\n".join(rows) + "\n")
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "check_status" in str(excinfo.value)


def test_get_haproxy_stats_empty_scur_is_none(TC):
    c = TC(host="haproxy01.example", runtime_executor=FakeRuntime())
    csv_text = "\n".join([
        ",".join(_CSV_COLS),
        _row("web_front", "FRONTEND", status="OPEN"),  # scur empty
    ]) + "\n"
    c._stats = FakeStatsTransport(csv_text=csv_text)
    c.connect()
    stats = c.get_haproxy_stats()
    assert stats["frontends"][0]["current_sessions"] is None


def test_get_haproxy_stats_orphan_server_rows(TC):
    """Server rows without a BACKEND aggregate still surface, status unknown."""
    c = TC(host="haproxy01.example", runtime_executor=FakeRuntime())
    csv_text = "\n".join([
        ",".join(_CSV_COLS),
        _row("lonely", "srv1", scur=1, status="UP",
             check_status="L4OK"),
    ]) + "\n"
    c._stats = FakeStatsTransport(csv_text=csv_text)
    c.connect()
    stats = c.get_haproxy_stats()
    assert stats["backends"] == [{
        "name": "lonely", "status": "unknown",
        "servers": [{"name": "srv1", "status": "UP", "current_sessions": 1,
                     "check_status": "L4OK"}],
    }]


# ------------------------------------------------------------- privileged set state


def test_set_haproxy_server_state_happy_path(conn):
    result = conn.set_haproxy_server_state("app_back", "web1", "drain")
    assert result == {
        "backend": "app_back",
        "server": "web1",
        "previous_state": "UP",
        "state": "drain",
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    assert conn._fake_runtime.commands == [
        "set server app_back/web1 state drain"
    ]
    json.dumps(result)


def test_set_haproxy_server_state_rejects_invalid_state(conn):
    with pytest.raises(ConnectorError) as excinfo:
        conn.set_haproxy_server_state("app_back", "web1", "banana")
    assert "banana" in str(excinfo.value)
    assert conn._fake_runtime.commands == []  # never sent


def test_set_haproxy_server_state_unknown_server(conn):
    with pytest.raises(ConnectorError):
        conn.set_haproxy_server_state("app_back", "no-such-server", "drain")
    assert conn._fake_runtime.commands == []  # looked up first, never sent


def test_set_haproxy_server_state_unknown_backend(conn):
    with pytest.raises(ConnectorError):
        conn.set_haproxy_server_state("no-such-backend", "web1", "drain")
    assert conn._fake_runtime.commands == []


def test_set_haproxy_server_state_rejects_whitespace_names(conn):
    with pytest.raises(ConnectorError):
        conn.set_haproxy_server_state("app_back\ninject", "web1", "drain")
    with pytest.raises(ConnectorError):
        conn.set_haproxy_server_state("app_back", "web 1", "drain")
    assert conn._fake_runtime.commands == []


def test_set_haproxy_server_state_refuses_when_socket_unavailable(TC, tmp_path):
    """No runtime_executor + missing socket file: ConnectorError, never raw OSError."""
    missing_sock = str(tmp_path / "no-such-dir" / "admin.sock")
    c = TC(host="haproxy01.example", socket_path=missing_sock)
    c._stats = FakeStatsTransport()
    c.connect()  # stats side is fine; the socket side is not
    with pytest.raises(ConnectorError) as excinfo:
        c.set_haproxy_server_state("app_back", "web1", "maint")
    assert not isinstance(excinfo.value, OSError)
    assert "refused" in str(excinfo.value) or "unavailable" in str(excinfo.value)


def test_set_haproxy_server_state_rejects_runtime_error_text(conn):
    conn._fake_runtime.response = "Can't find server 'web1' in backend"
    with pytest.raises(ConnectorError) as excinfo:
        conn.set_haproxy_server_state("app_back", "web1", "drain")
    assert "rejected" in str(excinfo.value)


def test_set_haproxy_server_state_wraps_executor_failures(conn):
    """A misbehaving executor must not leak raw exceptions."""
    conn._fake_runtime.fail_with = ValueError("boom")
    with pytest.raises(ConnectorError) as excinfo:
        conn.set_haproxy_server_state("app_back", "web1", "drain")
    assert not isinstance(excinfo.value, ValueError)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(TC):
    c = TC(host="haproxy01.example", port=8404,
           socket_path="/var/run/haproxy/admin.sock",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           runtime_executor=FakeRuntime())
    text = repr(c)
    assert FAKE_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "haproxy01.example" in text
    assert "stats;csv" in text


def test_close_is_safe_when_not_connected(TC):
    c = TC(host="haproxy01.example", runtime_executor=FakeRuntime())
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    stats = conn.read("get_haproxy_stats", {})
    assert stats["backends"][0]["name"] == "app_back"
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
    # A privileged tool of another connector can never be reached via read().
    with pytest.raises(ConnectorError):
        conn.read("restart_channel", {})
    # The read tool can never be reached via act().
    with pytest.raises(ConnectorError):
        conn.act("get_haproxy_stats", {})
