"""Tests for the real WebSphere connector (connectors/websphere.py).

No live WebSphere is touched and no real credentials exist anywhere here.
The connector's internal HTTP boundary (``conn._transport``, exposing
``get_json`` / ``post_json`` / ``get_text``) is replaced with a
``FakeTransport`` returning canned Jolokia JSON responses. Placeholders
like "REDACTED-fake" stand in for secrets, and hygiene tests assert they
never leak into repr, exceptions, or logs.

The ``connectors.websphere`` module is imported lazily inside a
module-scoped fixture (never at module top) and re-registered in the
global ``CONNECTORS`` registry on teardown, so running this file together
with the contract tests does not change the registry those tests pin.
"""

import importlib
import json
import os
import urllib.error
import urllib.parse

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"

POOL_WC = ("WebSphere:type=ThreadPoolStats,name=WebContainer,"
           "process=server1,platform=proxy,node=node01")
POOL_ORB = ("WebSphere:type=ThreadPoolStats,name=ORB.thread.pool,"
            "process=server1,platform=proxy,node=node01")
APP_MGR = ("WebSphere:type=ApplicationManager,process=server1,"
           "platform=proxy,node=node01")


def _app(name):
    return (f"WebSphere:j2eeType=J2EEApplication,name={name},"
            f"J2EEServer=server1,node=node01,cell=cell01")


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def websphere_mod():
    """Import connectors.websphere lazily; restore its registration."""
    mod = importlib.import_module("connectors.websphere")
    yield mod
    CONNECTORS["websphere"] = (mod.WebSphereConnector.SPEC,
                               mod.WebSphereConnector)


@pytest.fixture(scope="module")
def WS(websphere_mod):
    return websphere_mod.WebSphereConnector


class FakeTransport:
    """Canned Jolokia JSON responses for the HTTP boundary."""

    def __init__(self):
        self.calls: list = []
        self.fail_with: Exception | None = None
        self.fail_exec_op: str | None = None

    def _record(self, method, url, auth, payload=None):
        user = auth[0] if auth else None
        self.calls.append((method, url, user, payload))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the three boundary methods the connector uses -----------------

    def get_json(self, url, auth):
        self._record("get_json", url, auth)
        if url.endswith("/version"):
            return {"value": {"agent": "1.7.2", "product": "websphere"}}
        if "read/java.lang" in url:
            return {"value": {
                "HeapMemoryUsage": {
                    "init": 268435456, "used": 536870912,
                    "committed": 1073741824, "max": 2147483648,
                },
            }}
        if "/read/" in url:
            mbean = urllib.parse.unquote(url)
            if "ThreadPoolStats" in mbean:
                if "name=WebContainer" in mbean:
                    return {"value": {"poolSize": 10, "activeThreads": 7,
                                      "maximumPoolSize": 50}}
                if "name=ORB.thread.pool" in mbean:
                    return {"value": {"poolSize": 5, "activeThreads": 0,
                                      "maximumPoolSize": 20}}
            if "j2eeType=J2EEApplication" in mbean:
                if url.endswith("/state"):
                    if "name=broken" in mbean:
                        return {"value": "FAILED"}
                    return {"value": "RUNNING"}
                return {"value": {}}
        raise AssertionError(f"FakeTransport: unexpected get_json {url}")

    def post_json(self, url, payload, auth):
        self._record("post_json", url, auth, payload)
        ptype = payload.get("type")
        if ptype == "search":
            pattern = payload.get("mbean", "")
            if pattern == "WebSphere:type=ThreadPoolStats,*":
                return {"value": [POOL_WC, POOL_ORB]}
            if pattern == "WebSphere:j2eeType=J2EEApplication,*":
                return {"value": [_app("orders"), _app("reports"),
                                   _app("broken"), _app("ghost")]}
            if pattern == "WebSphere:type=ApplicationManager,*":
                return {"value": [APP_MGR]}
            raise AssertionError(
                f"FakeTransport: unexpected search {pattern!r}")
        if ptype == "exec":
            op = payload.get("operation", "")
            if op == self.fail_exec_op:
                return {"status": 500, "error": "fake exec failure"}
            args = payload.get("arguments", [])
            if op == "isApplicationStarted":
                name = args[0] if args else ""
                if name == "ghost":
                    return {"status": 404, "error": "no such app"}
                return {"status": 200, "value": name != "reports"}
            if op in ("stopApplication", "startApplication"):
                return {"status": 200, "value": None}
            raise AssertionError(f"FakeTransport: unexpected exec {op!r}")
        raise AssertionError(f"FakeTransport: unexpected post {payload!r}")

    def get_text(self, url, auth):
        self._record("get_text", url, auth)
        raise AssertionError(f"FakeTransport: unexpected get_text {url}")


@pytest.fixture
def conn(WS):
    """A connected WebSphere connector backed by FakeTransport."""
    c = WS(host="was01.example", port=9080,
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(websphere_mod):
    assert "websphere" in CONNECTORS
    spec, factory = CONNECTORS["websphere"]
    assert spec.name == "websphere"
    assert factory is websphere_mod.WebSphereConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_websphere_heap",
        "get_websphere_threadpool",
        "get_websphere_apps",
        "read_websphere_log",
        "restart_websphere_app",
    }


def test_constructor_takes_no_raw_secret(WS):
    import inspect
    params = inspect.signature(WS.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


# ------------------------------------------------------------- connect


def test_connect_probes_jolokia_version(WS):
    c = WS(host="was01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_json"
    assert fake.calls[0][1].endswith("/version")
    assert fake.calls[0][2] == "rca_reader"  # read user, not admin


def test_connect_uses_env_credentials(WS, monkeypatch):
    monkeypatch.setenv("WEBSPHERE_READ_USER", "env_reader")
    monkeypatch.setenv("WEBSPHERE_READ_PASSWORD", "env-pass-fake")
    c = WS(host="was01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_unreachable_raises_connector_error_not_raw(WS):
    """A dead agent must surface ConnectorError, never a raw URLError."""
    c = WS(host="unreachable.invalid",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_reads_require_connect(WS):
    c = WS(host="was01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_websphere_heap()
    with pytest.raises(ConnectorError):
        c.get_websphere_threadpool()
    with pytest.raises(ConnectorError):
        c.get_websphere_apps()
    with pytest.raises(ConnectorError):
        c.read_websphere_log()
    with pytest.raises(ConnectorError):
        c.restart_websphere_app("orders")


def test_empty_credential_provider_refuses(WS):
    c = WS(host="was01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


# ------------------------------------------------------------- heap


def test_get_websphere_heap_shape_and_math(conn):
    heap = conn.get_websphere_heap()
    assert heap["host"] == "was01.example"
    assert heap["port"] == 9080
    assert heap["heap_init_bytes"] == 268435456
    assert heap["heap_used_bytes"] == 536870912
    assert heap["heap_committed_bytes"] == 1073741824
    assert heap["heap_max_bytes"] == 2147483648
    assert heap["heap_used_pct"] == 25.0  # 512MiB / 2GiB
    assert heap["ts"].endswith("Z")
    json.dumps(heap)  # JSON-serializable


# ------------------------------------------------------------- thread pools


def test_get_websphere_threadpool_reports_busy_threads(conn):
    result = conn.get_websphere_threadpool()
    pools = {p["name"]: p for p in result["pools"]}
    assert set(pools) == {"WebContainer", "ORB.thread.pool"}
    wc = pools["WebContainer"]
    assert wc["current_threads_busy"] == 7
    assert wc["current_thread_count"] == 10
    assert wc["max_threads"] == 50
    assert wc["busy_pct"] == 14.0
    assert pools["ORB.thread.pool"]["busy_pct"] == 0.0
    json.dumps(result)


def test_get_websphere_threadpool_filter_by_name(conn):
    result = conn.get_websphere_threadpool(pool="WebContainer")
    assert [p["name"] for p in result["pools"]] == ["WebContainer"]


def test_get_websphere_threadpool_unknown_pool(conn):
    with pytest.raises(ConnectorError):
        conn.get_websphere_threadpool(pool="no-such-pool")


# ------------------------------------------------------------- apps


def test_get_websphere_apps_state_mapping(conn):
    result = conn.get_websphere_apps()
    apps = {a["name"]: a for a in result["apps"]}
    assert set(apps) == {"orders", "reports", "broken", "ghost"}
    assert apps["orders"]["state"] == "running"
    assert apps["reports"]["state"] == "stopped"
    assert apps["broken"]["state"] == "failed"  # JSR-77 state override
    assert apps["ghost"]["state"] == "unknown"  # exec failed, kept
    assert all(a["sessions"] is None for a in apps.values())
    json.dumps(result)


# ------------------------------------------------------------- log tailing


SYSTEMOUT_LOG = """\
[9/21/26 15:30:45:123 CDT] 0000003a SystemOut     O Server server1 open for e-business
[9/21/26 15:31:02:456 CDT] 0000003b SystemErr     E java.lang.NullPointerException: boom
[9/21/26 15:31:03:789 CDT] 0000003c SystemOut     W WSVR0605W: A Thread has been active for too long
not a websphere line at all
"""


def _conn_with_log_dir(WS, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "SystemOut.log").write_text(SYSTEMOUT_LOG)
    (log_dir / "SystemErr.log").write_text(
        "[9/21/26 15:32:00:000 CDT] 0000003d SystemErr     E fatal\n")
    c = WS(host="was01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    return c


def test_read_websphere_log_systemout_parses_severity(WS, tmp_path):
    c = _conn_with_log_dir(WS, tmp_path)
    entries = c.read_websphere_log(limit=10)
    assert len(entries) == 4
    assert entries[0]["severity"] == "INFO"
    assert entries[0]["ts"] == "9/21/26 15:30:45:123 CDT"
    assert "open for e-business" in entries[0]["message"]
    assert entries[1]["severity"] == "ERROR"
    assert entries[2]["severity"] == "WARNING"
    assert entries[3]["severity"] == "INFO"  # unparseable line kept
    assert entries[3]["ts"] is None
    json.dumps(entries)


def test_read_websphere_log_limit(WS, tmp_path):
    c = _conn_with_log_dir(WS, tmp_path)
    entries = c.read_websphere_log(limit=2)
    assert len(entries) == 2
    assert entries[0]["severity"] == "WARNING"


def test_read_websphere_log_systemerr(WS, tmp_path):
    c = _conn_with_log_dir(WS, tmp_path)
    entries = c.read_websphere_log(log="systemerr")
    assert len(entries) == 1
    assert entries[0]["severity"] == "ERROR"
    assert "fatal" in entries[0]["message"]


def test_read_websphere_log_needs_log_dir(WS, monkeypatch):
    monkeypatch.delenv("WEBSPHERE_LOG_DIR", raising=False)
    c = WS(host="was01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_websphere_log()


def test_read_websphere_log_unknown_log_name(WS, tmp_path):
    c = _conn_with_log_dir(WS, tmp_path)
    with pytest.raises(ConnectorError):
        c.read_websphere_log(log="trace")


def test_read_websphere_log_missing_file(WS, tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()  # no SystemOut.log inside
    c = WS(host="was01.example", log_dir=str(log_dir),
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError):
        c.read_websphere_log()


# ------------------------------------------------------------- privileged restart


def _admin_conn(WS, **kwargs):
    c = WS(host="was01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("was_admin",
                                                   FAKE_ADMIN_SECRET),
           **kwargs)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake
    return c


def test_restart_websphere_app_stop_then_start(WS):
    c = _admin_conn(WS)
    result = c.restart_websphere_app("orders")
    assert result == {
        "app_name": "orders",
        "previous_state": "running",
        "state": "running",
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    exec_calls = [call for call in c._fake.calls
                  if call[0] == "post_json" and call[3]
                  and call[3].get("type") == "exec"
                  and call[3].get("operation") in
                  ("stopApplication", "startApplication")]
    assert [call[3]["operation"] for call in exec_calls] == [
        "stopApplication", "startApplication"]
    assert all(call[3]["mbean"] == APP_MGR for call in exec_calls)
    assert all(call[3]["arguments"] == ["orders"] for call in exec_calls)
    # Privileged execs used the admin identity, not the read user.
    assert [call[2] for call in exec_calls] == ["was_admin", "was_admin"]
    json.dumps(result)


def test_restart_websphere_app_refuses_without_admin_credential(
        WS, monkeypatch):
    monkeypatch.delenv("WEBSPHERE_ADMIN_USER", raising=False)
    monkeypatch.delenv("WEBSPHERE_ADMIN_PASSWORD", raising=False)
    c = WS(host="was01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    c.connect()
    with pytest.raises(ConnectorError) as excinfo:
        c.restart_websphere_app("orders")
    assert "privileged" in str(excinfo.value).lower()
    assert FAKE_SECRET not in str(excinfo.value)


def test_restart_websphere_app_unknown_app(WS):
    c = _admin_conn(WS)
    with pytest.raises(ConnectorError):
        c.restart_websphere_app("no-such-app")


def test_restart_websphere_app_rejects_empty_name(WS):
    c = _admin_conn(WS)
    with pytest.raises(ConnectorError):
        c.restart_websphere_app("  ")


def test_restart_websphere_app_exec_failure_raises(WS):
    c = _admin_conn(WS)
    c._fake.fail_exec_op = "stopApplication"
    with pytest.raises(ConnectorError) as excinfo:
        c.restart_websphere_app("orders")
    assert "stopApplication" in str(excinfo.value)


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(WS):
    c = WS(host="was01.example", port=9080, log_dir="/opt/websphere/logs",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET),
           privileged_credential_provider=lambda: ("was_admin",
                                                   FAKE_ADMIN_SECRET))
    text = repr(c)
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "was01.example" in text


def test_close_is_safe_when_not_connected(WS):
    c = WS(host="was01.example",
           credential_provider=lambda: ("rca_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    heap = conn.read("get_websphere_heap", {})
    assert heap["heap_used_bytes"] == 536870912
    apps = conn.read("get_websphere_apps", {})
    assert len(apps["apps"]) == 4
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})
    with pytest.raises(ConnectorError):
        conn.act("get_websphere_heap", {})
