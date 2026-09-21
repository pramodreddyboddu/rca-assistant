"""Tests for the real Nginx connector (connectors/nginx.py).

No live nginx is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``get_text``) is replaced with a ``FakeTransport`` returning canned
stub_status text. The privileged ``reload_nginx`` exec path is replaced
with a ``reloader`` callable override; the default subprocess path is
exercised only through monkeypatched ``shutil.which`` /
``subprocess.run`` -- no real ``nginx`` binary is ever invoked.
Placeholders like "REDACTED-fake" stand in for secrets, and hygiene
tests assert they never leak into repr, exceptions, or logs.

The ``connectors.nginx`` module is imported lazily inside a
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

STUB_STATUS = (
    "Active connections: 291 \n"
    "server accepts handled requests\n"
    " 1663536 1663536 31070465 \n"
    "Reading: 6 Writing: 179 Waiting: 106 \n"
)

ACCESS_LOG = (
    '10.0.0.1 - - [21/Sep/2026:14:33:20 -0500] "GET /index.html HTTP/1.1" '
    '200 612 "-" "curl/8.0"\n'
    '10.0.0.2 - - [21/Sep/2026:14:33:21 -0500] "POST /api/orders HTTP/1.1" '
    '500 128 "-" "python-requests/2.31"\n'
)

ERROR_LOG = (
    "2026/09/21 14:33:20 [error] 1234#5678: *56 connect() failed "
    "(111: Connection refused) while connecting to upstream, "
    "client: 10.0.0.1, server: example.com\n"
    "2026/09/21 14:34:01 [warn] 1234#5678: *57 an upstream response is "
    "buffered to a temporary file\n"
    "a line that matches neither format\n"
)


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def nginx_mod():
    """Import connectors.nginx lazily; remove its registration afterwards.

    Unlike tomcat (imported by connectors/__init__), nginx is NOT in the
    package init (a parallel worker owns the registry wiring), so the
    teardown pops the registration to leave the registry exactly as it
    was for other test modules.
    """
    had = "nginx" in CONNECTORS
    mod = importlib.import_module("connectors.nginx")
    yield mod
    if not had:
        CONNECTORS.pop("nginx", None)


@pytest.fixture(scope="module")
def NG(nginx_mod):
    return nginx_mod.NginxConnector


class FakeTransport:
    """Canned stub_status text for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_with: Exception | None = None
        self.status_text = STUB_STATUS

    def get_text(self, url, auth):
        user = auth[0] if auth else None
        self.calls.append(("get_text", url, user))
        if self.fail_with is not None:
            raise self.fail_with
        return self.status_text


@pytest.fixture
def conn(NG):
    """A connected connector backed by FakeTransport."""
    c = NG(host="nginx01.example", port=80,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(nginx_mod):
    assert "nginx" in CONNECTORS
    spec, factory = CONNECTORS["nginx"]
    assert spec.name == "nginx"
    assert factory is nginx_mod.NginxConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_nginx_status",
        "read_nginx_log",
        "reload_nginx",
    }


def test_constructor_takes_no_raw_secret(NG):
    import inspect
    params = inspect.signature(NG.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


def test_constructor_defaults(NG):
    c = NG(host="h")
    assert c._port == 80
    assert c._status_path == "nginx_status"
    assert c._status_url == "http://h:80/nginx_status"


# ------------------------------------------------------------- connect


def test_connect_probes_status_page(NG):
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_text"
    assert fake.calls[0][1] == "http://nginx01.example:80/nginx_status"
    assert fake.calls[0][2] == "rca_reader"  # Basic-auth user is sent


def test_connect_is_anonymous_by_default(NG):
    c = NG(host="nginx01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # no provider, no env pair -> anonymous, must not raise
    assert fake.calls[0][2] is None


def test_connect_unresolvable_provider_is_anonymous(NG):
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("", ""))
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # empty provider result -> anonymous, not an error
    assert fake.calls[0][2] is None


def test_connect_uses_env_credentials(NG, monkeypatch):
    monkeypatch.setenv("NGINX_STATUS_USER", "env_reader")
    monkeypatch.setenv("NGINX_STATUS_PASSWORD", "env-pass-fake")
    c = NG(host="nginx01.example",
           user_env="NGINX_STATUS_USER", password_env="NGINX_STATUS_PASSWORD")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(NG):
    """A dead server must surface ConnectorError, never a raw URLError."""
    c = NG(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_non_stub_status_page(NG):
    """A 200 page that is not stub_status fails closed on connect."""
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.status_text = "<html><body>It works!</body></html>"
    c._transport = fake
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(NG):
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_nginx_status()
    with pytest.raises(ConnectorError):
        c.read_nginx_log()
    with pytest.raises(ConnectorError):
        c.reload_nginx()


# ------------------------------------------------------------- status parsing


def test_get_nginx_status_shape(conn):
    status = conn.get_nginx_status()
    assert status == {
        "host": "nginx01.example",
        "port": 80,
        "active_connections": 291,
        "accepts": 1663536,
        "handled": 1663536,
        "requests": 31070465,
        "reading": 6,
        "writing": 179,
        "waiting": 106,
        "ts": status["ts"],
    }
    assert status["ts"].endswith("Z")
    json.dumps(status)  # JSON-serializable


def test_get_nginx_status_missing_active_line(conn):
    conn._fake.status_text = (
        "server accepts handled requests\n"
        " 1 1 1 \n"
        "Reading: 0 Writing: 0 Waiting: 0 \n"
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_nginx_status()
    assert "Active connections" in str(excinfo.value)


def test_get_nginx_status_short_triple(conn):
    conn._fake.status_text = (
        "Active connections: 1 \n"
        "server accepts handled requests\n"
        " 42 43 \n"  # only two counters
        "Reading: 0 Writing: 0 Waiting: 0 \n"
    )
    with pytest.raises(ConnectorError):
        conn.get_nginx_status()


def test_get_nginx_status_missing_rww(conn):
    conn._fake.status_text = (
        "Active connections: 1 \n"
        "server accepts handled requests\n"
        " 42 42 100 \n"
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_nginx_status()
    assert "Reading" in str(excinfo.value)


def test_get_nginx_status_tolerates_case_and_blanks(NG):
    c = NG(host="nginx01.example")
    fake = FakeTransport()
    fake.status_text = (
        "\n"
        "active connections: 7\n"
        "SERVER ACCEPTS HANDLED REQUESTS\n"
        "10 10 25\n"
        "reading: 1 writing: 2 waiting: 3\n"
    )
    c._transport = fake
    c.connect()
    status = c.get_nginx_status()
    assert status["active_connections"] == 7
    assert status["accepts"] == 10
    assert status["requests"] == 25
    assert status["waiting"] == 3


# ------------------------------------------------------------- log tailing


def _conn_with_log_dir(NG, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "access.log").write_text(ACCESS_LOG)
    (log_dir / "error.log").write_text(ERROR_LOG)
    c = NG(host="nginx01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    return c


def test_read_nginx_log_access_kept_as_info(NG, tmp_path):
    c = _conn_with_log_dir(NG, tmp_path)
    entries = c.read_nginx_log(limit=10)
    assert len(entries) == 2
    assert all(e["severity"] == "INFO" for e in entries)
    assert all(e["ts"] is None for e in entries)
    assert "GET /index.html" in entries[0]["message"]
    assert "POST /api/orders" in entries[1]["message"]
    json.dumps(entries)


def test_read_nginx_log_error_parses_severity(NG, tmp_path):
    c = _conn_with_log_dir(NG, tmp_path)
    entries = c.read_nginx_log(log="error")
    assert len(entries) == 3
    assert entries[0]["ts"] == "2026/09/21 14:33:20"
    assert entries[0]["severity"] == "ERROR"
    assert "Connection refused" in entries[0]["message"]
    assert entries[1]["severity"] == "WARN"
    assert entries[2]["severity"] == "INFO"  # unparseable line kept
    assert entries[2]["ts"] is None
    json.dumps(entries)


def test_read_nginx_log_limit(NG, tmp_path):
    c = _conn_with_log_dir(NG, tmp_path)
    entries = c.read_nginx_log(log="error", limit=1)
    assert len(entries) == 1
    assert entries[0]["severity"] == "INFO"  # tail: the last line only
    assert entries[0]["ts"] is None
    entries = c.read_nginx_log(log="error", limit=2)
    assert [e["severity"] for e in entries] == ["WARN", "INFO"]


def test_read_nginx_log_unknown_log_name(NG, tmp_path):
    c = _conn_with_log_dir(NG, tmp_path)
    with pytest.raises(ConnectorError):
        c.read_nginx_log(log="main")


def test_read_nginx_log_needs_log_dir(NG, monkeypatch):
    monkeypatch.delenv("NGINX_LOG_DIR", raising=False)
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_nginx_log()


def test_read_nginx_log_missing_file(NG, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()  # no access.log inside
    c = NG(host="nginx01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_nginx_log()


def test_read_nginx_log_uses_env_log_dir(NG, tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "access.log").write_text(ACCESS_LOG)
    monkeypatch.setenv("NGINX_LOG_DIR", str(log_dir))
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    entries = c.read_nginx_log()
    assert len(entries) == 2


# ------------------------------------------------------------- privileged reload


def _reload_conn(NG, **kwargs):
    calls: list = []

    def fake_reloader():
        calls.append("reload")

    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           reloader=fake_reloader,
           **kwargs)
    c._transport = FakeTransport()
    c.connect()
    c._reload_calls = calls  # test introspection only
    return c


def test_reload_nginx_success(NG):
    c = _reload_conn(NG)
    result = c.reload_nginx()
    assert result == {"state": "reloaded", "ts": result["ts"]}
    assert result["ts"].endswith("Z")
    assert c._reload_calls == ["reload"]
    json.dumps(result)


def test_reload_nginx_reloader_failure_wrapped(NG):
    def bad_reloader():
        raise RuntimeError("boom")

    c = NG(host="nginx01.example", reloader=bad_reloader)
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.reload_nginx()
    assert not isinstance(excinfo.value, RuntimeError)
    assert "boom" in str(excinfo.value)


def test_reload_nginx_refuses_when_binary_missing(NG, monkeypatch):
    """Default exec path: no nginx on PATH -> ConnectorError, no exec."""
    import connectors.nginx as nginx_mod

    monkeypatch.setattr(nginx_mod.shutil, "which", lambda _name: None)
    ran = []

    def spy_run(*a, **k):
        ran.append(a)
        raise AssertionError("subprocess must not run")

    monkeypatch.setattr(nginx_mod.subprocess, "run", spy_run)
    c = NG(host="nginx01.example")  # no reloader override
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.reload_nginx()
    assert "nginx" in str(excinfo.value).lower()
    assert "not found" in str(excinfo.value).lower()
    assert ran == []


def test_reload_nginx_default_path_uses_fixed_argv(NG, monkeypatch):
    """The real exec path: which-resolved binary, fixed argv, no shell."""
    import connectors.nginx as nginx_mod

    seen: dict = {}

    class _Proc:
        returncode = 0
        stderr = ""

    def spy_which(name):
        assert name == "nginx"
        return "/usr/sbin/nginx"

    def spy_run(argv, **kwargs):
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return _Proc()

    monkeypatch.setattr(nginx_mod.shutil, "which", spy_which)
    monkeypatch.setattr(nginx_mod.subprocess, "run", spy_run)
    c = NG(host="nginx01.example", timeout=7)
    c._transport = FakeTransport()
    c.connect()
    result = c.reload_nginx()
    assert result["state"] == "reloaded"
    assert seen["argv"] == ["/usr/sbin/nginx", "-s", "reload"]
    assert "shell" not in seen["kwargs"]  # shell=False is the default
    assert seen["kwargs"]["timeout"] == 7


def test_reload_nginx_nonzero_exit_wrapped(NG, monkeypatch):
    import connectors.nginx as nginx_mod

    class _Proc:
        returncode = 1
        stderr = "nginx: [emerg] bind() failed\n"

    monkeypatch.setattr(nginx_mod.shutil, "which", lambda _n: "/usr/sbin/nginx")
    monkeypatch.setattr(nginx_mod.subprocess, "run", lambda *a, **k: _Proc())
    c = NG(host="nginx01.example")
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.reload_nginx()
    assert "exit 1" in str(excinfo.value)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(NG):
    c = NG(host="nginx01.example", port=80, log_dir="/var/log/nginx",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "nginx01.example" in text


def test_close_is_safe_when_not_connected(NG):
    c = NG(host="nginx01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    status = conn.read("get_nginx_status", {})
    assert status["active_connections"] == 291
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
    with pytest.raises(ConnectorError):
        conn.read("reload_nginx", {})  # privileged: never via read()


def test_act_routing_reaches_privileged(conn):
    conn._reloader = lambda: None  # fake the exec; routing is the point
    result = conn.act("reload_nginx", {})
    assert result["state"] == "reloaded"
    with pytest.raises(ConnectorError):
        conn.act("get_nginx_status", {})  # read-only: never via act()
