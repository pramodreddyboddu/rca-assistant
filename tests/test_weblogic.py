"""Tests for the real Oracle WebLogic connector (connectors/weblogic.py).

No live WebLogic server is touched and no real credentials exist anywhere
here. The connector's internal HTTP boundary (``conn._transport``,
exposing ``get_json`` / ``post_json``) is replaced with a
``FakeTransport`` returning canned Management REST API responses.
Placeholders like "REDACTED-fake" stand in for secrets, and hygiene
tests assert they never leak into repr, exceptions, or logs.

The ``connectors.weblogic`` module is imported lazily inside a
module-scoped fixture (never at module top) and re-registered in the
global ``CONNECTORS`` registry on teardown, so running this file
together with other connector test modules keeps the registry stable.
"""

import importlib
import json
import os
import urllib.error

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

JVM_RUNTIME = {
    "identity": ["JVMRuntime"],
    "type": "JVMRuntime",
    "name": "AdminServer",
    "heapSizeCurrent": 1073741824,
    "heapFreeCurrent": 536870912,
    "heapSizeMax": 2147483648,
}

THREAD_POOL = {
    "identity": ["threadPoolRuntime"],
    "type": "ThreadPoolRuntime",
    "name": "weblogic.kernel.Default",
    "executeThreadTotalCount": 25,
    "executeThreadIdleCount": 18,
    "pendingUserRequestCount": 3,
}

APP_COLLECTION = {
    "items": [
        {"name": "orders"},
        {"name": "reports"},
        {"name": "staleapp"},
    ]
}

APP_DETAIL = {
    "orders": {
        "name": "orders",
        "type": "ApplicationRuntime",
        "healthState": {"state": "OK", "subsystemName": "Application"},
        "componentRuntimes": {
            "items": [
                {"name": "orders-web", "openSessionsCurrentCount": 12},
                {"name": "orders-ejb", "openSessionsCurrentCount": 3},
            ]
        },
    },
    "reports": {
        "name": "reports",
        "type": "ApplicationRuntime",
        "healthState": {"state": "WARN", "subsystemName": "Application"},
        "componentRuntimes": {"items": []},
    },
    "staleapp": {
        "name": "staleapp",
        "type": "ApplicationRuntime",
        "healthState": {"state": "FAILED", "subsystemName": "Application"},
    },
}


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def weblogic_mod():
    """Import connectors.weblogic lazily; restore its registration afterwards.

    The module is self-registering on import; the teardown re-registers
    rather than removing, so later test modules still see a stable
    package-level registration.
    """
    mod = importlib.import_module("connectors.weblogic")
    yield mod
    CONNECTORS["weblogic"] = (mod.WebLogicConnector.SPEC,
                              mod.WebLogicConnector)


@pytest.fixture(scope="module")
def WL(weblogic_mod):
    return weblogic_mod.WebLogicConnector


class FakeTransport:
    """Canned WebLogic Management REST API responses for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_with: Exception | None = None
        self.stop_response = {"links": [
            {"rel": "task", "href": "/management/wls/latest/task/stop/1"}]}
        self.start_response = {"links": [
            {"rel": "task", "href": "/management/wls/latest/task/start/2"}]}

    def _record(self, method, url, auth):
        user = auth[0] if auth else None
        self.calls.append((method, url, user))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the two boundary methods the connector uses -------------------

    def get_json(self, url, auth):
        self._record("get_json", url, auth)
        if url.endswith("serverRuntime/JVMRuntime"):
            return dict(JVM_RUNTIME)
        if url.endswith("serverRuntime/threadPoolRuntime"):
            return dict(THREAD_POOL)
        if url.endswith("serverRuntime/applicationRuntimes"):
            return {"items": [dict(a) for a in APP_COLLECTION["items"]]}
        if "serverRuntime/applicationRuntimes/" in url:
            import urllib.parse
            name = urllib.parse.unquote(url.rsplit("/", 1)[1])
            if name in APP_DETAIL:
                return json.loads(json.dumps(APP_DETAIL[name]))
            return {"message": "no such application"}  # never happens: _app_state guards
        raise AssertionError(f"FakeTransport: unexpected get_json {url}")

    def post_json(self, url, payload, auth):
        self._record("post_json", url, auth)
        if url.endswith("/stop"):
            return dict(self.stop_response)
        if url.endswith("/start"):
            return dict(self.start_response)
        raise AssertionError(f"FakeTransport: unexpected post_json {url}")


@pytest.fixture
def conn(WL):
    """A connected connector backed by FakeTransport."""
    c = WL(host="wls01.example", port=7001,
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(weblogic_mod):
    assert "weblogic" in CONNECTORS
    spec, factory = CONNECTORS["weblogic"]
    assert spec.name == "weblogic"
    assert factory is weblogic_mod.WebLogicConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_weblogic_heap",
        "get_weblogic_threadpool",
        "get_weblogic_apps",
        "read_weblogic_log",
        "restart_weblogic_app",
    }


def test_constructor_takes_no_raw_secret(WL):
    import inspect
    params = inspect.signature(WL.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


# ------------------------------------------------------------- connect


def test_connect_probes_jvm_runtime(WL):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_json"
    assert fake.calls[0][1].endswith("/serverRuntime/JVMRuntime")
    assert fake.calls[0][2] == "wls_reader"  # read user, not admin


def test_connect_uses_env_credentials(WL, monkeypatch):
    monkeypatch.setenv("WEBLOGIC_READ_USER", "env_reader")
    monkeypatch.setenv("WEBLOGIC_READ_PASSWORD", "env-pass-fake")
    c = WL(host="wls01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(WL):
    """A dead server must surface ConnectorError, never a raw URLError."""
    c = WL(host="unreachable.invalid",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_unexpected_payload(WL):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    fake = FakeTransport()

    def no_heap(url, auth):
        return {"identity": ["JVMRuntime"]}  # heapSizeCurrent missing
    fake.get_json = no_heap
    c._transport = fake
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(WL):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_weblogic_heap()
    with pytest.raises(ConnectorError):
        c.get_weblogic_apps()
    with pytest.raises(ConnectorError):
        c.get_weblogic_threadpool()
    with pytest.raises(ConnectorError):
        c.read_weblogic_log()
    with pytest.raises(ConnectorError):
        c.restart_weblogic_app("orders")


def test_empty_credential_provider_refuses(WL):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- heap


def test_get_weblogic_heap_shape_and_math(conn):
    heap = conn.get_weblogic_heap()
    assert heap["host"] == "wls01.example"
    assert heap["port"] == 7001
    # used = heapSizeCurrent - heapFreeCurrent = 1024MiB - 512MiB
    assert heap["heap_used_bytes"] == 536870912
    assert heap["heap_max_bytes"] == 2147483648
    assert heap["heap_used_pct"] == 25.0
    assert heap["heap_init_bytes"] is None  # not exposed by REST
    assert heap["heap_committed_bytes"] is None
    assert heap["ts"].endswith("Z")
    json.dumps(heap)  # JSON-serializable


# ------------------------------------------------------------- thread pool


def test_get_weblogic_threadpool_reports_busy_threads(conn):
    result = conn.get_weblogic_threadpool()
    pools = result["pools"]
    assert len(pools) == 1
    pool = pools[0]
    assert pool["name"] == "weblogic.kernel.Default"
    assert pool["current_threads_busy"] == 7  # 25 - 18
    assert pool["current_thread_count"] == 25
    assert pool["max_threads"] is None  # self-tuning pool: no max exposed
    assert pool["busy_pct"] is None
    assert result["host"] == "wls01.example"
    assert result["ts"].endswith("Z")
    json.dumps(result)


# ------------------------------------------------------------- apps


def test_get_weblogic_apps_states_and_sessions(conn):
    result = conn.get_weblogic_apps()
    apps = {a["name"]: a for a in result["apps"]}
    assert set(apps) == {"orders", "reports", "staleapp"}
    assert apps["orders"]["state"] == "running"      # healthState OK
    assert apps["orders"]["sessions"] == 15          # 12 + 3 across components
    assert apps["reports"]["state"] == "running"     # healthState WARN
    assert apps["reports"]["sessions"] is None      # no component data
    assert apps["staleapp"]["state"] == "failed"     # healthState FAILED
    assert apps["staleapp"]["sessions"] is None
    # sorted by name
    assert [a["name"] for a in result["apps"]] == ["orders", "reports",
                                                   "staleapp"]
    json.dumps(result)


def test_get_weblogic_apps_bad_collection_shape(WL):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    fake = FakeTransport()
    orig = fake.get_json

    def bad_collection(url, auth):
        if url.endswith("serverRuntime/applicationRuntimes"):
            return {"items": "not-a-list"}
        return orig(url, auth)
    fake.get_json = bad_collection
    c._transport = fake
    c.connect()
    with pytest.raises(ConnectorError):
        c.get_weblogic_apps()


# ------------------------------------------------------------- log tailing


SERVER_LOG = """\
####<21-Sep-2026 14:33:20.123> <Info> <Server> <AdminServer> <main> <> <BEA-002606> <Server starting>
####<21-Sep-2026 14:34:01.456> <Error> <EJB> <AdminServer> <pool-1-thread-2> <> <BEA-011002> <EJB bean threw exception>
a continuation line without the marker prefix
"""


def _conn_with_log_dir(WL, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "AdminServer.log").write_text(SERVER_LOG)
    c = WL(host="wls01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    return c


def test_read_weblogic_log_server_parses_severity(WL, tmp_path):
    c = _conn_with_log_dir(WL, tmp_path)
    entries = c.read_weblogic_log(limit=10)
    assert len(entries) == 3
    assert entries[0]["severity"] == "Info"
    assert entries[0]["ts"] == "21-Sep-2026 14:33:20.123"
    assert "Server starting" in entries[0]["message"]
    assert entries[1]["severity"] == "Error"
    assert entries[2]["severity"] == "INFO"  # unparseable line kept
    assert entries[2]["ts"] is None
    assert "continuation line" in entries[2]["message"]
    json.dumps(entries)


def test_read_weblogic_log_limit(WL, tmp_path):
    c = _conn_with_log_dir(WL, tmp_path)
    entries = c.read_weblogic_log(limit=2)
    assert len(entries) == 2
    assert entries[0]["severity"] == "Error"


def test_read_weblogic_log_access_picks_newest(WL, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    old = log_dir / "access.log.20260920"
    new = log_dir / "access.log"
    old.write_text('10.0.0.1 - - [20/Sep/2026:00:00:01 -0500] "GET /old HTTP/1.1" 200 12\n')
    new.write_text('10.0.0.2 - - [21/Sep/2026:00:00:01 -0500] "GET /new HTTP/1.1" 200 34\n')
    os.utime(old, (1_700_000_000, 1_700_000_000))
    os.utime(new, (1_800_000_000, 1_800_000_000))
    c = WL(host="wls01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    entries = c.read_weblogic_log(log="access")
    assert len(entries) == 1
    assert "GET /new" in entries[0]["message"]
    assert entries[0]["ts"] == "21/Sep/2026:00:00:01 -0500"
    assert entries[0]["severity"] == "INFO"


def test_read_weblogic_log_uses_env_log_dir(WL, tmp_path, monkeypatch):
    log_dir = tmp_path / "envlogs"
    log_dir.mkdir()
    (log_dir / "Server01.log").write_text(SERVER_LOG)
    monkeypatch.setenv("WEBLOGIC_LOG_DIR", str(log_dir))
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    entries = c.read_weblogic_log()
    assert len(entries) == 3


def test_read_weblogic_log_needs_log_dir(WL, monkeypatch):
    monkeypatch.delenv("WEBLOGIC_LOG_DIR", raising=False)
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_weblogic_log()


def test_read_weblogic_log_unknown_log_name(WL, tmp_path):
    c = _conn_with_log_dir(WL, tmp_path)
    with pytest.raises(ConnectorError):
        c.read_weblogic_log(log="gc")


def test_read_weblogic_log_missing_file(WL, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()  # no *.log inside
    c = WL(host="wls01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_weblogic_log()


# ------------------------------------------------------------- privileged restart


def _admin_conn(WL, **kwargs):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("wls_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake
    return c


def test_restart_weblogic_app_stop_then_start(WL):
    c = _admin_conn(WL)
    result = c.restart_weblogic_app("orders")
    assert result == {
        "app_name": "orders",
        "previous_state": "running",
        "state": "running",
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    verbs = [call[1] for call in c._fake.calls if call[0] == "post_json"]
    assert verbs == [
        "http://wls01.example:7001/management/wls/latest/"
        "serverRuntime/applicationRuntimes/orders/stop",
        "http://wls01.example:7001/management/wls/latest/"
        "serverRuntime/applicationRuntimes/orders/start",
    ]
    # Privileged calls used the admin identity, not the read user.
    admin_calls = [call for call in c._fake.calls if call[2] == "wls_admin"]
    assert len(admin_calls) == 2
    assert all("wls_reader" != call[2] for call in admin_calls)
    json.dumps(result)


def test_restart_weblogic_app_refuses_without_admin_credential(WL, monkeypatch):
    monkeypatch.delenv("WEBLOGIC_ADMIN_USER", raising=False)
    monkeypatch.delenv("WEBLOGIC_ADMIN_PASSWORD", raising=False)
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.restart_weblogic_app("orders")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_restart_weblogic_app_unknown_app(WL):
    c = _admin_conn(WL)
    with pytest.raises(ConnectorError):
        c.restart_weblogic_app("no-such-app")


def test_restart_weblogic_app_rejects_bad_name(WL):
    c = _admin_conn(WL)
    with pytest.raises(ConnectorError):
        c.restart_weblogic_app("../orders")


def test_restart_weblogic_app_unexpected_response_raises(WL):
    c = _admin_conn(WL)
    c._fake.stop_response = {"status": "done"}  # no task links
    with pytest.raises(ConnectorError):
        c.restart_weblogic_app("orders")


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(WL):
    c = WL(host="wls01.example", port=7001,
           rest_root="management/wls/latest", log_dir="/var/log/wls",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("wls_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "wls01.example" in text


def test_close_is_safe_when_not_connected(WL):
    c = WL(host="wls01.example",
           credential_provider=lambda: ("wls_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    heap = conn.read("get_weblogic_heap", {})
    assert heap["heap_used_bytes"] == 536870912
    apps = conn.read("get_weblogic_apps", {})
    assert len(apps["apps"]) == 3
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
