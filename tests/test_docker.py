"""Tests for the real Docker connector (connectors/docker.py).

No live Docker daemon is touched and no real credentials exist anywhere
here. The connector's internal socket boundary (``conn._transport``,
exposing ``get_json`` / ``get_bytes`` / ``post``) is replaced with a
``FakeTransport`` returning canned Engine API JSON and multiplexed log
bytes.

The ``connectors.docker`` module is imported lazily inside a
module-scoped fixture (never at module top) and deregistered from the
global ``CONNECTORS`` registry on teardown, so running this file together
with tests/test_connector_contract.py does not change the registry that
the contract tests pin.
"""

import importlib
import json
import struct

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

WEB_ID = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0"
DB_ID = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

CONTAINERS_JSON = [
    {"Id": WEB_ID, "Names": ["/web-1"], "Image": "nginx:1.27",
     "State": "running", "Status": "Up 3 hours"},
    {"Id": DB_ID, "Names": ["/db-1"], "Image": "postgres:16",
     "State": "exited", "Status": "Exited (0) 2 days ago"},
]

VERSION_JSON = {"Version": "27.3.1", "ApiVersion": "1.47",
                "Os": "linux", "Arch": "amd64"}


def _stats(total, prev_total, system, prev_system, percpu,
           mem_used, mem_limit):
    return {
        "name": "/web-1",
        "cpu_stats": {
            "cpu_usage": {"total_usage": total,
                          "percpu_usage": [0] * percpu},
            "system_cpu_usage": system,
            "online_cpus": percpu,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": prev_total},
            "system_cpu_usage": prev_system,
        },
        "memory_stats": {"usage": mem_used, "limit": mem_limit},
    }


WEB_STATS = _stats(
    total=2_000_000_000, prev_total=1_000_000_000,
    system=20_000_000_000, prev_system=10_000_000_000,
    percpu=4, mem_used=536_870_912, mem_limit=2_147_483_648,
)
# cpu_delta = 1e9, system_delta = 1e10, 4 cpus -> 40.0; mem 512MiB/2GiB -> 25.0

ZERO_DELTA_STATS = _stats(
    total=5_000_000_000, prev_total=5_000_000_000,
    system=30_000_000_000, prev_system=30_000_000_000,
    percpu=2, mem_used=100, mem_limit=0,
)


def _frame(stream_type: int, text: str) -> bytes:
    payload = text.encode("utf-8")
    return (bytes([stream_type, 0, 0, 0])
            + struct.pack(">I", len(payload)) + payload)


MUXED_LOGS = (
    _frame(1, "2026-09-21T15:00:00.123456789Z web listening on :80\n")
    + _frame(2, "2026-09-21T15:00:01Z warn: slow query\n")
    + _frame(1, "plain line without a daemon timestamp\n")
)


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def docker_mod():
    """Import connectors.docker lazily; deregister on teardown.

    Nothing else imports connectors.docker (connectors/__init__ does not),
    so popping the registration restores the registry the contract tests
    pin, whatever order pytest runs the files in.
    """
    had = "docker" in CONNECTORS
    mod = importlib.import_module("connectors.docker")
    yield mod
    if not had:
        CONNECTORS.pop("docker", None)


@pytest.fixture(scope="module")
def DC(docker_mod):
    return docker_mod.DockerConnector


class FakeTransport:
    """Canned Engine API responses for the socket boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.fail_with: Exception | None = None
        self.version = VERSION_JSON
        self.stats_payload: dict = WEB_STATS
        self.logs_payload: bytes = MUXED_LOGS

    def _record(self, method, path):
        self.calls.append((method, path))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the three boundary methods the connector uses -----------------

    def get_json(self, path):
        self._record("GET", path)
        if path == "/version":
            return self.version
        if path.startswith("/containers/json"):
            return CONTAINERS_JSON
        if "/stats" in path:
            return self.stats_payload
        raise AssertionError(f"FakeTransport: unexpected GET {path}")

    def get_bytes(self, path):
        self._record("GET", path)
        if "/logs" in path:
            return self.logs_payload
        raise AssertionError(f"FakeTransport: unexpected GET {path}")

    def post(self, path, body=None):
        self._record("POST", path)
        if "/restart" in path:
            return b""
        raise AssertionError(f"FakeTransport: unexpected POST {path}")


@pytest.fixture
def sock_path(tmp_path):
    """A socket-path file that exists (no daemon behind it needed)."""
    p = tmp_path / "docker.sock"
    p.touch()
    return str(p)


@pytest.fixture
def conn(DC, sock_path):
    """A connected connector backed by FakeTransport."""
    c = DC(socket_path=sock_path)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(docker_mod):
    assert "docker" in CONNECTORS
    spec, factory = CONNECTORS["docker"]
    assert spec.name == "docker"
    assert factory is docker_mod.DockerConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description
    assert spec.credential_refs == ()


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_docker_containers",
        "get_docker_stats",
        "read_docker_logs",
        "restart_docker_container",
    }


def test_constructor_takes_no_raw_secret(DC):
    import inspect
    params = inspect.signature(DC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned
    assert set(params) == {"self", "socket_path", "timeout"}


# ------------------------------------------------------------- connect


def test_connect_probes_version(DC, sock_path):
    c = DC(socket_path=sock_path)
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0] == ("GET", "/version")


def test_connect_missing_socket_raises(DC, tmp_path):
    c = DC(socket_path=str(tmp_path / "no-such.sock"))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "does not exist" in str(excinfo.value)


def test_connect_wraps_raw_transport_failure(DC, sock_path):
    """A dead daemon must surface ConnectorError, never a raw OSError."""
    c = DC(socket_path=sock_path)
    fake = FakeTransport()
    fake.fail_with = ConnectionRefusedError("refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, ConnectionRefusedError)


def test_connect_rejects_bad_version_shape(DC, sock_path):
    c = DC(socket_path=sock_path)
    fake = FakeTransport()
    fake.version = {"not": "a version response"}
    c._transport = fake
    with pytest.raises(ConnectorError):
        c.connect()


def test_real_transport_wraps_missing_socket(docker_mod):
    """The stdlib socket boundary guards itself with no daemon present."""
    t = docker_mod._UnixSocketTransport("/nonexistent/docker.sock",
                                        timeout=1)
    with pytest.raises(ConnectorError):
        t.get_json("/version")


def test_reads_require_connect(DC, sock_path):
    c = DC(socket_path=sock_path)
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_docker_containers()
    with pytest.raises(ConnectorError):
        c.get_docker_stats()
    with pytest.raises(ConnectorError):
        c.read_docker_logs("web-1")
    with pytest.raises(ConnectorError):
        c.restart_docker_container("web-1")


# ------------------------------------------------------------- containers


def test_get_docker_containers_shape(conn):
    result = conn.get_docker_containers()
    assert [c["name"] for c in result["containers"]] == ["db-1", "web-1"]
    web = result["containers"][1]
    assert web == {
        "id": WEB_ID[:12],  # 12-char short id
        "name": "web-1",    # leading "/" stripped
        "image": "nginx:1.27",
        "state": "running",
        "status": "Up 3 hours",
    }
    assert all(len(c["id"]) == 12 for c in result["containers"])
    assert result["ts"].endswith("Z")
    json.dumps(result)  # JSON-serializable


# ------------------------------------------------------------- stats


def test_get_docker_stats_cpu_math(conn):
    result = conn.get_docker_stats()
    stats = {s["name"]: s for s in result["stats"]}
    web = stats["web-1"]
    # cpu_delta 1e9 / system_delta 1e10 * 4 cpus * 100 = 40.0
    assert web["cpu_pct"] == 40.0
    assert web["mem_used_bytes"] == 536_870_912
    assert web["mem_limit_bytes"] == 2_147_483_648
    assert web["mem_pct"] == 25.0  # 512MiB / 2GiB
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_docker_stats_zero_system_delta(conn):
    conn._fake.stats_payload = ZERO_DELTA_STATS
    result = conn.get_docker_stats()
    web = {s["name"]: s for s in result["stats"]}["web-1"]
    assert web["cpu_pct"] == 0.0  # guarded division by zero
    assert web["mem_pct"] is None  # unknown/zero limit


def test_cpu_pct_missing_fields_is_zero(docker_mod):
    assert docker_mod.DockerConnector._cpu_pct({}) == 0.0
    assert docker_mod.DockerConnector._cpu_pct(
        {"cpu_stats": {}, "precpu_stats": {}}) == 0.0


# ------------------------------------------------------------- logs


def test_read_docker_logs_demuxes_stream(conn):
    entries = conn.read_docker_logs("web-1")
    assert len(entries) == 3
    assert entries[0] == {
        "ts": "2026-09-21T15:00:00.123456789Z",
        "message": "web listening on :80",
    }
    assert entries[1]["ts"] == "2026-09-21T15:00:01Z"
    assert entries[1]["message"] == "warn: slow query"
    # line without a daemon timestamp is kept with ts None
    assert entries[2] == {
        "ts": None, "message": "plain line without a daemon timestamp"}
    log_calls = [p for m, p in conn._fake.calls if "/logs" in p]
    assert log_calls and "tail=50" in log_calls[0]
    json.dumps(entries)


def test_read_docker_logs_resolves_name_and_id_prefix(conn):
    by_name = conn.read_docker_logs("web-1")
    by_prefix = conn.read_docker_logs(WEB_ID[:12])
    assert by_name == by_prefix
    assert len(by_name) == 3


def test_read_docker_logs_falls_back_to_raw_text(conn):
    conn._fake.logs_payload = b"not a multiplexed stream\nsecond line\n"
    entries = conn.read_docker_logs("web-1")
    assert entries == [
        {"ts": None, "message": "not a multiplexed stream"},
        {"ts": None, "message": "second line"},
    ]


def test_read_docker_logs_limit(conn):
    entries = conn.read_docker_logs("web-1", limit=2)
    assert len(entries) == 2
    assert entries[0]["message"] == "warn: slow query"


def test_read_docker_logs_unknown_container(conn):
    with pytest.raises(ConnectorError) as excinfo:
        conn.read_docker_logs("no-such-container")
    assert "no-such-container" in str(excinfo.value)


# ------------------------------------------------------------- privileged restart


def test_restart_docker_container(conn):
    result = conn.restart_docker_container("web-1")
    assert result == {
        "container": "web-1",
        "previous_state": "running",
        "state": "running",
        "ts": result["ts"],
    }
    assert result["ts"].endswith("Z")
    posts = [p for m, p in conn._fake.calls if m == "POST"]
    assert posts == [f"/containers/{WEB_ID[:12]}/restart?t=10"]
    json.dumps(result)


def test_restart_reports_previous_state_of_stopped_container(conn):
    result = conn.restart_docker_container("db-1")
    assert result["container"] == "db-1"
    assert result["previous_state"] == "exited"
    assert result["state"] == "running"


def test_restart_unknown_container_raises(conn):
    with pytest.raises(ConnectorError):
        conn.restart_docker_container("ghost")


def test_restart_via_act_routing(conn):
    result = conn.act("restart_docker_container", {"container": "web-1"})
    assert result["state"] == "running"
    assert result["previous_state"] == "running"


def test_restart_refused_when_not_connected(DC, sock_path):
    c = DC(socket_path=sock_path)
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.act("restart_docker_container", {"container": "web-1"})


# ------------------------------------------------------------- hygiene & routing


def test_read_routing_dispatches_by_name(conn):
    result = conn.read("get_docker_containers", {})
    assert len(result["containers"]) == 2
    entries = conn.read("read_docker_logs",
                        {"container": "web-1", "limit": 1})
    assert len(entries) == 1
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})


def test_read_routing_refuses_privileged_tool(conn):
    with pytest.raises(ConnectorError):
        conn.read("restart_docker_container", {"container": "web-1"})


def test_repr_contains_no_secrets(DC):
    # NOTE: not tmp_path-based -- pytest's tmp dir name for this test
    # contains the substring "secret", which would false-positive the
    # assertion below. repr() needs no real socket file.
    c = DC(socket_path="/var/run/docker.sock", timeout=10)
    text = repr(c)
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "/var/run/docker.sock" in text


def test_close_is_safe_when_not_connected(DC, sock_path):
    c = DC(socket_path=sock_path)
    c.close()  # must not raise
    c.close()
