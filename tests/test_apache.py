"""Tests for the real Apache HTTPD connector (connectors/apache.py).

No live httpd is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``get_text``) is replaced with a ``FakeTransport`` returning canned
mod_status ``?auto`` text, and the privileged reload boundary
(``conn._reloader``, exposing ``reload()``) is replaced with a
``FakeReloader`` recording calls. Placeholders like "REDACTED-fake"
stand in for secrets, and hygiene tests assert they never leak into
repr, exceptions, or logs.

The ``connectors.apache`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown only when the fixture itself
introduced the registration (``connectors/__init__.py`` now imports
it), so running this file together with
tests/test_connector_contract.py does not change the registry that the
contract tests pin.
"""

import importlib
import json
import os
import urllib.error

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"

STATUS_AUTO = (
    "Total Accesses: 1234\n"
    "Total kBytes: 5678\n"
    "Uptime: 86400\n"
    "ReqPerSec: 0.0142824\n"
    "BytesPerSec: 67.2986\n"
    "BytesPerReq: 4710.61\n"
    "BusyWorkers: 3\n"
    "IdleWorkers: 47\n"
    "ConnsTotal: 0\n"
    'Scoreboard: ___W____________.........................\n'
)

ACCESS_LOG = (
    '10.0.0.1 - frank [21/Sep/2026:15:04:02 -0500] "GET /index.html HTTP/1.1" 200 1234\n'
    '10.0.0.2 - - [21/Sep/2026:15:04:03 -0500] "GET /missing HTTP/1.1" 404 0\n'
)

ERROR_LOG = (
    "[Mon Sep 21 15:04:02.123456 2026] [mpm_event:error] [pid 1234:tid 140] AH00485: scoreboard is full\n"
    "[Mon Sep 21 15:05:10.000000 2026] [core:notice] [pid 1234] AH00094: Command line: '/usr/sbin/httpd'\n"
    "a line with no apache error-log prefix at all\n"
)


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def apache_mod():
    """Import connectors.apache lazily; deregister it afterwards.

    ``connectors/__init__.py`` now imports this module, so the teardown
    only removes the registration when the fixture itself introduced it,
    leaving the package-init registration intact for other test modules.
    """
    had = "apache" in CONNECTORS
    mod = importlib.import_module("connectors.apache")
    yield mod
    if not had:
        CONNECTORS.pop("apache", None)


@pytest.fixture(scope="module")
def TC(apache_mod):
    return apache_mod.ApacheConnector


class FakeTransport:
    """Canned mod_status ?auto responses for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_with: Exception | None = None
        self.status_text = STATUS_AUTO

    def get_text(self, url, auth):
        user = auth[0] if auth else None
        self.calls.append(("get_text", url, user))
        if self.fail_with is not None:
            raise self.fail_with
        if url.endswith("?auto"):
            return self.status_text
        raise AssertionError(f"FakeTransport: unexpected get_text {url}")


class FakeReloader:
    """Fake apachectl boundary: records reload() calls, can refuse."""

    def __init__(self):
        self.calls = 0
        self.refuse_with: Exception | None = None

    def reload(self):
        self.calls += 1
        if self.refuse_with is not None:
            raise self.refuse_with


@pytest.fixture
def conn(TC):
    """A connected connector backed by FakeTransport (no reload override)."""
    c = TC(host="web01.example", port=8080,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


@pytest.fixture
def conn_with_reloader(TC):
    """A connected connector with FakeTransport + FakeReloader."""
    c = TC(host="web01.example", port=8080,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    reloader = FakeReloader()
    c._reloader = reloader
    c.connect()
    c._fake_reloader = reloader  # test introspection only
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(apache_mod):
    assert "apache" in CONNECTORS
    spec, factory = CONNECTORS["apache"]
    assert spec.name == "apache"
    assert factory is apache_mod.ApacheConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_apache_status",
        "read_apache_log",
        "reload_apache",
    }


def test_constructor_takes_no_raw_secret(TC):
    import inspect
    params = inspect.signature(TC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


def test_constructor_defaults(TC):
    c = TC(host="web01.example")
    assert c._port == 80
    assert c._status_url == "http://web01.example:80/server-status?auto"


def test_constructor_custom_status_path(TC):
    c = TC(host="web01.example", port=8443, status_path="/private/status")
    assert c._status_url == "http://web01.example:8443/private/status?auto"


# ------------------------------------------------------------- connect


def test_connect_probes_status_auto(TC):
    c = TC(host="web01.example", port=8080,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0] == (
        "get_text",
        "http://web01.example:8080/server-status?auto",
        "rca_reader",
    )


def test_connect_unreachable_raises_connector_error_not_raw(TC):
    """A dead server must surface ConnectorError, never a raw URLError."""
    c = TC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_non_mod_status_page(TC):
    """A 200 that is not a ?auto page fails closed instead of connecting."""
    c = TC(host="web01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.status_text = "<html><body>It works!</body></html>\n"
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "mod_status" in str(excinfo.value)


def test_connect_sends_no_auth_when_unconfigured(TC):
    c = TC(host="web01.example")  # no provider, no env names
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] is None  # no Authorization header material


def test_connect_uses_env_credentials(TC, monkeypatch):
    monkeypatch.setenv("APACHE_STATUS_USER", "env_reader")
    monkeypatch.setenv("APACHE_STATUS_PASSWORD", "env-pass-fake")
    c = TC(host="web01.example", user_env="APACHE_STATUS_USER",
           password_env="APACHE_STATUS_PASSWORD")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_half_configured_auth_refuses(TC):
    c = TC(host="web01.example", user_env="APACHE_STATUS_USER")
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "BOTH" in str(excinfo.value)


def test_empty_credential_provider_refuses(TC):
    c = TC(host="web01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(TC):
    c = TC(host="web01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_apache_status()
    with pytest.raises(ConnectorError):
        c.read_apache_log()
    with pytest.raises(ConnectorError):
        c.reload_apache()


# ------------------------------------------------------------- status


def test_get_apache_status_shape_and_math(conn):
    status = conn.get_apache_status()
    assert status == {
        "host": "web01.example",
        "port": 8080,
        "total_accesses": 1234,
        "total_kbytes": 5678,
        "uptime_secs": 86400,
        "busy_workers": 3,
        "idle_workers": 47,
        "ts": status["ts"],
    }
    assert status["ts"].endswith("Z")
    json.dumps(status)  # JSON-serializable


def test_status_page_parsing_ignores_extra_lines(TC):
    c = TC(host="web01.example")
    fake = FakeTransport()
    fake.status_text = (
        "  total accesses : 42 \n"   # case + whitespace tolerant
        "TOTAL KBYTES: 100\n"
        "uptime: 7\n"
        "busyworkers: 1\n"
        "IdleWorkers: 9\n"
        "Scoreboard: W\n"
        "a line without a colon\n"
    )
    c._transport = fake
    c.connect()  # parse must succeed for connect to complete
    status = c.get_apache_status()
    assert status["total_accesses"] == 42
    assert status["total_kbytes"] == 100
    assert status["uptime_secs"] == 7
    assert status["busy_workers"] == 1
    assert status["idle_workers"] == 9


def test_status_page_missing_key_fails_closed(TC):
    c = TC(host="web01.example")
    fake = FakeTransport()
    fake.status_text = (
        "Total Accesses: 10\nTotal kBytes: 20\nUptime: 30\n"
        "BusyWorkers: 1\n"  # IdleWorkers missing
    )
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "IdleWorkers" in str(excinfo.value)


def test_status_page_non_integer_value_fails_closed(TC):
    c = TC(host="web01.example")
    fake = FakeTransport()
    fake.status_text = (
        "Total Accesses: lots\nTotal kBytes: 20\nUptime: 30\n"
        "BusyWorkers: 1\nIdleWorkers: 2\n"
    )
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "Total Accesses" in str(excinfo.value)


# ------------------------------------------------------------- log tailing


def _conn_with_log_dir(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "access_log").write_text(ACCESS_LOG)
    (log_dir / "error_log").write_text(ERROR_LOG)
    c = TC(host="web01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    return c, log_dir


def test_read_apache_log_access_lines_are_info(TC, tmp_path):
    c, _ = _conn_with_log_dir(TC, tmp_path)
    entries = c.read_apache_log()
    assert len(entries) == 2
    for entry in entries:
        assert entry["severity"] == "INFO"
        assert entry["ts"] is None
    assert 'GET /index.html' in entries[0]["message"]
    assert 'GET /missing' in entries[1]["message"]
    json.dumps(entries)


def test_read_apache_log_limit(TC, tmp_path):
    c, _ = _conn_with_log_dir(TC, tmp_path)
    entries = c.read_apache_log(limit=1)
    assert len(entries) == 1
    assert 'GET /missing' in entries[0]["message"]  # tail, not head


def test_read_apache_log_error_parses_severity(TC, tmp_path):
    c, _ = _conn_with_log_dir(TC, tmp_path)
    entries = c.read_apache_log(log="error")
    assert len(entries) == 3
    assert entries[0]["severity"] == "ERROR"
    assert entries[0]["ts"] == "Mon Sep 21 15:04:02.123456 2026"
    assert "AH00485" in entries[0]["message"]
    assert entries[1]["severity"] == "NOTICE"
    assert "AH00094" in entries[1]["message"]
    # Unparseable line is kept, not dropped.
    assert entries[2]["severity"] == "INFO"
    assert entries[2]["ts"] is None
    json.dumps(entries)


def test_read_apache_log_picks_newest_file(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    old = log_dir / "access.log.1"
    new = log_dir / "access_log"
    old.write_text("OLD LINE\n")
    new.write_text("NEW LINE\n")
    os.utime(old, (1_700_000_000, 1_700_000_000))
    os.utime(new, (1_800_000_000, 1_800_000_000))
    c = TC(host="web01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    entries = c.read_apache_log(log="access")
    assert [e["message"] for e in entries] == ["NEW LINE"]


def test_read_apache_log_from_env_dir(TC, tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "access_log").write_text(ACCESS_LOG)
    monkeypatch.setenv("APACHE_LOG_DIR", str(log_dir))
    c = TC(host="web01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    entries = c.read_apache_log()
    assert len(entries) == 2


def test_read_apache_log_needs_log_dir(TC, monkeypatch):
    monkeypatch.delenv("APACHE_LOG_DIR", raising=False)
    c = TC(host="web01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_apache_log()


def test_read_apache_log_unknown_log_name(TC, tmp_path):
    c, _ = _conn_with_log_dir(TC, tmp_path)
    with pytest.raises(ConnectorError):
        c.read_apache_log(log="ssl")


def test_read_apache_log_no_matching_files(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()  # no access_log / error_log inside
    c = TC(host="web01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.read_apache_log(log="error")
    assert "error_log" in str(excinfo.value)


# ------------------------------------------------------------- privileged reload


def test_reload_apache_graceful_success(conn_with_reloader):
    result = conn_with_reloader.reload_apache()
    assert result == {"state": "reloaded", "ts": result["ts"]}
    assert result["ts"].endswith("Z")
    assert conn_with_reloader._fake_reloader.calls == 1
    json.dumps(result)


def test_reload_apache_refuses_when_binary_missing(conn_with_reloader):
    conn_with_reloader._fake_reloader.refuse_with = ConnectorError(
        "apache: 'apachectl' not found on PATH; reload refused")
    with pytest.raises(ConnectorError) as excinfo:
        conn_with_reloader.reload_apache()
    assert "apachectl" in str(excinfo.value)


def test_reload_apache_wraps_reloader_failure(conn_with_reloader):
    conn_with_reloader._fake_reloader.refuse_with = ConnectorError(
        "apache: apachectl graceful exited 1: AH00526: Syntax error")
    with pytest.raises(ConnectorError) as excinfo:
        conn_with_reloader.reload_apache()
    assert "exited 1" in str(excinfo.value)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(TC):
    c = TC(host="web01.example", port=8080, log_dir="/var/log/httpd",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           user_env="APACHE_STATUS_USER", password_env="APACHE_STATUS_PASSWORD")
    text = repr(c)
    assert FAKE_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "web01.example" in text
    assert "server-status" in text


def test_close_is_safe_when_not_connected(TC):
    c = TC(host="web01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "access_log").write_text(ACCESS_LOG)
    c = TC(host="web01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    status = c.read("get_apache_status", {})
    assert status["total_accesses"] == 1234
    entries = c.read("read_apache_log", {"log": "access", "limit": 1})
    assert len(entries) == 1
    with pytest.raises(ConnectorError):
        c.read("no_such_tool", {})
    # reload_apache is privileged: read() must never reach it.
    with pytest.raises(ConnectorError):
        c.read("reload_apache", {})
