"""Tests for the real Tomcat connector (connectors/tomcat.py).

No live Tomcat is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``get_json`` / ``post_json`` / ``get_text``) is replaced with a
``FakeTransport`` returning canned Jolokia JSON and Manager text API
responses. Placeholders like "REDACTED-fake" stand in for secrets, and
hygiene tests assert they never leak into repr, exceptions, or logs.

The ``connectors.tomcat`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown, so running this file together
with tests/test_connector_contract.py does not change the registry that
the contract tests pin.
"""

import importlib
import json
import os
import urllib.error

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

MANAGER_LIST = (
    "OK - Listed applications for virtual host [localhost]\n"
    "/:running:0:ROOT\n"
    "/orders:running:12:orders\n"
    "/reports:stopped:0:reports\n"
)


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def tomcat_mod():
    """Import connectors.tomcat lazily; restore its registration afterwards.

    The module is self-registering on import (connectors/__init__ imports
    it too); the teardown re-registers rather than removing, so later test
    modules still see the package-level registration.
    """
    mod = importlib.import_module("connectors.tomcat")
    yield mod
    CONNECTORS["tomcat"] = (mod.TomcatConnector.SPEC, mod.TomcatConnector)


@pytest.fixture(scope="module")
def TC(tomcat_mod):
    return tomcat_mod.TomcatConnector


class FakeTransport:
    """Canned Jolokia JSON + Manager text responses for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_with: Exception | None = None
        self.manager_list = MANAGER_LIST
        self.manager_stop_text = "OK - Stopped application at context path"
        self.manager_start_text = "OK - Started application at context path"

    def _record(self, method, url, auth):
        user = auth[0] if auth else None
        self.calls.append((method, url, user))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the three boundary methods the connector uses -----------------

    def get_json(self, url, auth):
        self._record("get_json", url, auth)
        if url.endswith("/version"):
            return {"value": {"agent": "1.7.2", "info": {"product": "tomcat"}}}
        raise AssertionError(f"FakeTransport: unexpected get_json {url}")

    def post_json(self, url, payload, auth):
        self._record("post_json", url, auth)
        ptype = payload.get("type")
        if ptype == "search":
            return self._search(payload.get("mbean", ""))
        if ptype == "read":
            return {"value": self._read(
                payload.get("mbean", ""), payload.get("attribute"))}
        raise AssertionError(f"FakeTransport: unexpected post {ptype!r}")

    def _search(self, pattern):
        if pattern == "Catalina:type=ThreadPool,*":
            return {"value": [
                'Catalina:type=ThreadPool,name="http-nio-8080"',
                'Catalina:type=ThreadPool,name="ajp-nio-8009"',
            ]}
        if pattern == "Catalina:j2eeType=WebModule,*":
            return {"value": [
                "Catalina:j2eeType=WebModule,name=//localhost/,"
                "J2EEApplication=none,J2EEServer=none",
                "Catalina:j2eeType=WebModule,name=//localhost/orders,"
                "J2EEApplication=none,J2EEServer=none",
                "Catalina:j2eeType=WebModule,name=//localhost/reports,"
                "J2EEApplication=none,J2EEServer=none",
            ]}
        raise AssertionError(f"FakeTransport: unexpected search {pattern!r}")

    def _read(self, mbean, attributes):
        """Canned Jolokia read values; honors an attribute allow-list."""
        if mbean == "java.lang:type=Memory":
            value = {
                "HeapMemoryUsage": {
                    "init": 268435456, "used": 536870912,
                    "committed": 1073741824, "max": 2147483648,
                },
                "NonHeapMemoryUsage": {
                    "init": 2555904, "used": 134217728,
                    "committed": 150994944, "max": -1,
                },
            }
        elif "type=ThreadPool" in mbean:
            if 'name="http-nio-8080"' in mbean:
                value = {
                    "name": "http-nio-8080", "currentThreadsBusy": 7,
                    "currentThreadCount": 10, "maxThreads": 200}
            else:
                value = {
                    "name": "ajp-nio-8009", "currentThreadsBusy": 0,
                    "currentThreadCount": 5, "maxThreads": 200}
        elif "j2eeType=WebModule" in mbean:
            if "//localhost/reports" in mbean:
                value = {"stateName": "STOPPED"}
            else:
                value = {"stateName": "STARTED"}
        elif "type=Manager" in mbean:
            if "context=/orders" in mbean:
                value = {"activeSessions": 12}
            elif "context=,host" in mbean:  # ROOT context
                value = {"activeSessions": 3}
            else:
                value = {}
        else:
            raise AssertionError(f"FakeTransport: unexpected read {mbean!r}")
        if attributes:
            value = {k: v for k, v in value.items() if k in attributes}
        return value

    def get_text(self, url, auth):
        self._record("get_text", url, auth)
        if url.endswith("/list"):
            return self.manager_list
        if "/stop?path=" in url:
            return self.manager_stop_text
        if "/start?path=" in url:
            return self.manager_start_text
        raise AssertionError(f"FakeTransport: unexpected get_text {url}")


@pytest.fixture
def conn(TC):
    """A connected Jolokia-transport connector backed by FakeTransport."""
    c = TC(host="tomcat01.example", port=8080,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(tomcat_mod):
    assert "tomcat" in CONNECTORS
    spec, factory = CONNECTORS["tomcat"]
    assert spec.name == "tomcat"
    assert factory is tomcat_mod.TomcatConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_tomcat_heap",
        "get_tomcat_threadpool",
        "get_tomcat_apps",
        "read_tomcat_log",
        "restart_tomcat_app",
    }


def test_constructor_rejects_unknown_transport(TC):
    with pytest.raises(ConnectorError):
        TC(host="h", transport="rmi")


def test_constructor_takes_no_raw_secret(TC):
    import inspect
    params = inspect.signature(TC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


# ------------------------------------------------------------- connect


def test_connect_probes_jolokia_version(TC):
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_json"
    assert fake.calls[0][1].endswith("/version")
    assert fake.calls[0][2] == "rca_reader"  # read user, not admin


def test_connect_uses_env_credentials(TC, monkeypatch):
    monkeypatch.setenv("TOMCAT_READ_USER", "env_reader")
    monkeypatch.setenv("TOMCAT_READ_PASSWORD", "env-pass-fake")
    c = TC(host="tomcat01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(TC):
    """A dead agent must surface ConnectorError, never a raw URLError."""
    c = TC(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_manager_transport_ok(TC):
    c = TC(host="tomcat01.example", transport="manager",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][1].endswith("/manager/text/list")


def test_connect_manager_transport_rejects_non_ok(TC):
    c = TC(host="tomcat01.example", transport="manager",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.manager_list = "FAIL - Access denied"
    c._transport = fake
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(TC):
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_tomcat_heap()
    with pytest.raises(ConnectorError):
        c.get_tomcat_apps()


def test_empty_credential_provider_refuses(TC):
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- heap


def test_get_tomcat_heap_shape_and_math(conn):
    heap = conn.get_tomcat_heap()
    assert heap["host"] == "tomcat01.example"
    assert heap["port"] == 8080
    assert heap["heap_used_bytes"] == 536870912
    assert heap["heap_max_bytes"] == 2147483648
    assert heap["heap_used_pct"] == 25.0  # 512MiB / 2GiB
    assert heap["heap_committed_bytes"] == 1073741824
    assert heap["non_heap_used_bytes"] == 134217728
    assert heap["non_heap_max_bytes"] == -1  # undefined per JVM spec
    assert heap["ts"].endswith("Z")
    json.dumps(heap)  # JSON-serializable


# ------------------------------------------------------------- thread pools


def test_get_tomcat_threadpool_reports_busy_threads(conn):
    result = conn.get_tomcat_threadpool()
    pools = {p["name"]: p for p in result["pools"]}
    assert set(pools) == {"http-nio-8080", "ajp-nio-8009"}
    http = pools["http-nio-8080"]
    assert http["current_threads_busy"] == 7
    assert http["current_thread_count"] == 10
    assert http["max_threads"] == 200
    assert http["busy_pct"] == 3.5
    assert pools["ajp-nio-8009"]["busy_pct"] == 0.0
    json.dumps(result)


def test_get_tomcat_threadpool_filter_by_name(conn):
    result = conn.get_tomcat_threadpool(pool="http-nio-8080")
    assert [p["name"] for p in result["pools"]] == ["http-nio-8080"]


def test_get_tomcat_threadpool_unknown_pool(conn):
    with pytest.raises(ConnectorError):
        conn.get_tomcat_threadpool(pool="no-such-pool")


def test_threadpool_reads_refused_on_manager_transport(TC):
    c = TC(host="tomcat01.example", transport="manager",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.get_tomcat_threadpool()
    assert "Jolokia" in str(excinfo.value)
    with pytest.raises(ConnectorError):
        c.get_tomcat_heap()


# ------------------------------------------------------------- apps


def test_get_tomcat_apps_via_jolokia(conn):
    result = conn.get_tomcat_apps()
    apps = {a["path"]: a for a in result["apps"]}
    assert set(apps) == {"/", "/orders", "/reports"}
    assert apps["/orders"]["state"] == "running"
    assert apps["/orders"]["sessions"] == 12
    assert apps["/"]["state"] == "running"
    assert apps["/"]["sessions"] == 3
    assert apps["/reports"]["state"] == "stopped"
    assert apps["/reports"]["sessions"] is None  # Manager MBean unreadable
    json.dumps(result)


def test_get_tomcat_apps_via_manager_transport(TC):
    c = TC(host="tomcat01.example", transport="manager",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    result = c.get_tomcat_apps()
    apps = {a["path"]: a for a in result["apps"]}
    assert apps["/orders"] == {
        "path": "/orders", "state": "running", "sessions": 12}
    assert apps["/reports"]["state"] == "stopped"
    assert apps["/"]["state"] == "running"


# ------------------------------------------------------------- log tailing


CATALINA_OUT = """\
21-Sep-2026 14:33:20.123 INFO [main] org.apache.catalina.startup.Catalina.start Server startup in 1234 ms
21-Sep-2026 14:34:01.456 SEVERE [http-nio-8080-exec-3] org.apache.catalina.core.StandardWrapperValve.invoke Servlet.service() threw exception
java.lang.NullPointerException: boom
"""


def _conn_with_log_dir(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "catalina.out").write_text(CATALINA_OUT)
    c = TC(host="tomcat01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    return c


def test_read_tomcat_log_catalina_parses_severity(TC, tmp_path):
    c = _conn_with_log_dir(TC, tmp_path)
    entries = c.read_tomcat_log(limit=10)
    assert len(entries) == 3
    assert entries[0]["severity"] == "INFO"
    assert entries[0]["ts"] == "21-Sep-2026 14:33:20.123"
    assert "Server startup" in entries[0]["message"]
    assert entries[1]["severity"] == "SEVERE"
    assert entries[2]["severity"] == "INFO"  # unparseable line kept
    assert entries[2]["ts"] is None
    assert "NullPointerException" in entries[2]["message"]
    json.dumps(entries)


def test_read_tomcat_log_limit(TC, tmp_path):
    c = _conn_with_log_dir(TC, tmp_path)
    entries = c.read_tomcat_log(limit=2)
    assert len(entries) == 2
    assert entries[0]["severity"] == "SEVERE"


def test_read_tomcat_log_access_picks_newest(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    old = log_dir / "localhost_access_log.2026-09-20.txt"
    new = log_dir / "localhost_access_log.2026-09-21.txt"
    old.write_text('10.0.0.1 - - [20/Sep/2026:00:00:01 -0500] "GET /old HTTP/1.1" 200 12\n')
    new.write_text('10.0.0.2 - - [21/Sep/2026:00:00:01 -0500] "GET /new HTTP/1.1" 200 34\n')
    os.utime(old, (1_700_000_000, 1_700_000_000))
    os.utime(new, (1_800_000_000, 1_800_000_000))
    c = TC(host="tomcat01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    entries = c.read_tomcat_log(log="access")
    assert len(entries) == 1
    assert "GET /new" in entries[0]["message"]
    assert entries[0]["severity"] == "INFO"


def test_read_tomcat_log_needs_log_dir(TC, monkeypatch):
    monkeypatch.delenv("TOMCAT_LOG_DIR", raising=False)
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_tomcat_log()


def test_read_tomcat_log_unknown_log_name(TC, tmp_path):
    c = _conn_with_log_dir(TC, tmp_path)
    with pytest.raises(ConnectorError):
        c.read_tomcat_log(log="gc")


def test_read_tomcat_log_missing_file(TC, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()  # no catalina.out inside
    c = TC(host="tomcat01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_tomcat_log()


# ------------------------------------------------------------- privileged restart


def _admin_conn(TC, **kwargs):
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("tcat_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake
    return c


def test_restart_tomcat_app_stop_then_start(TC):
    c = _admin_conn(TC)
    result = c.restart_tomcat_app("/orders")
    assert result == {
        "app_path": "/orders",
        "previous_state": "running",
        "state": "running",
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    verbs = [call[1] for call in c._fake.calls
             if "/stop?path=" in call[1] or "/start?path=" in call[1]]
    assert verbs == [
        "http://tomcat01.example:8080/manager/text/stop?path=%2Forders",
        "http://tomcat01.example:8080/manager/text/start?path=%2Forders",
    ]
    # Privileged calls used the admin identity, not the read user.
    admin_calls = [call for call in c._fake.calls if call[2] == "tcat_admin"]
    assert len(admin_calls) == 2
    assert all("rca_reader" != call[2] for call in admin_calls)
    json.dumps(result)


def test_restart_tomcat_app_refuses_without_admin_credential(TC, monkeypatch):
    monkeypatch.delenv("TOMCAT_ADMIN_USER", raising=False)
    monkeypatch.delenv("TOMCAT_ADMIN_PASSWORD", raising=False)
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.restart_tomcat_app("/orders")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_restart_tomcat_app_unknown_app(TC):
    c = _admin_conn(TC)
    with pytest.raises(ConnectorError):
        c.restart_tomcat_app("/no-such-app")


def test_restart_tomcat_app_rejects_bad_path(TC):
    c = _admin_conn(TC)
    with pytest.raises(ConnectorError):
        c.restart_tomcat_app("orders")  # not a context path


def test_restart_tomcat_app_manager_failure_raises(TC):
    c = _admin_conn(TC)
    c._fake.manager_stop_text = "FAIL - No context exists for path /orders"
    with pytest.raises(ConnectorError) as excinfo:
        c.restart_tomcat_app("/orders")
    assert "FAIL" in str(excinfo.value)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(TC):
    c = TC(host="tomcat01.example", port=8080, log_dir="/var/log/tomcat",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("tcat_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "tomcat01.example" in text


def test_close_is_safe_when_not_connected(TC):
    c = TC(host="tomcat01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    heap = conn.read("get_tomcat_heap", {})
    assert heap["heap_used_bytes"] == 536870912
    apps = conn.read("get_tomcat_apps", {})
    assert len(apps["apps"]) == 3
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
