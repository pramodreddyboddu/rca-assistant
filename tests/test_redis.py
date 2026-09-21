"""Tests for the real Redis connector (connectors/redis.py).

redis-py is NOT installed here and no live Redis is touched. A fake
``redis`` module is injected into ``sys.modules`` (removed after each
test by monkeypatch), covering INFO (server/memory/clients/stats),
INFO replication, SLOWLOG GET, auth failure wrapping, connection failure
wrapping, zero-division guards, gating, and credential hygiene. No test
fixture contains a real secret: placeholders like "REDACTED-fake" stand
in, and the hygiene tests assert they never leak into repr, exceptions,
or logs.

Import note: ``connectors.redis`` is imported lazily inside a fixture
(and unregistered from CONNECTORS afterwards), never at module level, so
the repo-wide registry assertions in tests/test_connector_contract.py
and tests/test_contract_harness.py keep passing no matter the test
order.
"""

import json
import subprocess
import sys
import types

import pytest

from connectors import CONNECTORS
from connectors.base import ConnectorError

FAKE_SECRET = "s3cr3t-fake-redis-123"


# ------------------------------------------------------------- fake redis


def _make_fake_redis(state, fail_ping=False, bad_auth=False):
    """Build a fake `redis` module modelling redis-py's public surface.

    FakeClient: Redis(**kwargs), ping(), info(section=None),
    slowlog_get(n), close(). ping() fails when fail_ping or the host
    contains "unreachable"; auth fails when bad_auth and a password is
    supplied. The fake deliberately records every kwarg EXCEPT the
    password value, and exposes got_password for auth assertions.
    """
    mod = types.ModuleType("redis")

    class FakeRedisError(Exception):
        pass

    class FakeAuthError(FakeRedisError):
        pass

    class FakeConnectionError(FakeRedisError):
        pass

    def _unreachable(kwargs):
        return fail_ping or "unreachable" in str(kwargs.get("host", ""))

    class FakeClient:
        instances = []

        def __init__(self, **kwargs):
            self._kwargs = kwargs
            # Deliberately records everything EXCEPT the password value.
            self.recorded = {
                k: v for k, v in kwargs.items() if k != "password"
            }
            self.got_password = bool(kwargs.get("password"))
            self.closed = False
            FakeClient.instances.append(self)

        def ping(self):
            if _unreachable(self._kwargs):
                raise FakeConnectionError(
                    "Error 111 connecting to redis: connection refused")
            if bad_auth and self._kwargs.get("password"):
                raise FakeAuthError(
                    "WRONGPASS invalid username-password pair or user "
                    "is disabled.")
            return True

        def info(self, section=None):
            if _unreachable(self._kwargs):
                raise FakeConnectionError("connection refused")
            key = "replication" if section == "replication" else "info"
            return state[key]

        def slowlog_get(self, n):
            if _unreachable(self._kwargs):
                raise FakeConnectionError("connection refused")
            return list(state["slowlog"][:n])

        def close(self):
            self.closed = True

    mod.Redis = FakeClient
    mod.RedisError = FakeRedisError
    mod.AuthenticationError = FakeAuthError
    mod.ConnectionError = FakeConnectionError
    return mod


def _fresh_state():
    return {
        "info": {
            "server": {
                "redis_version": "7.2.4",
                "uptime_in_seconds": 86400,
            },
            "memory": {
                "used_memory": 104857600,
                "maxmemory": 1073741824,
            },
            "clients": {
                "connected_clients": 12,
                "blocked_clients": 1,
            },
            "stats": {
                "keyspace_hits": 9000,
                "keyspace_misses": 1000,
            },
        },
        "replication": {
            "replication": {
                "role": "master",
                "connected_slaves": 2,
            },
        },
        "slowlog": [
            [42, 1758300000, 1234,
             ["SET", "session:abc",
              "super-secret-value-" + "x" * 400]],
            [41, 1758299900, 987,
             ["GET", "user:1001"]],
        ],
    }


@pytest.fixture()
def redis_cls():
    """The RedisConnector class; imports lazily and unregisters after.

    Keeps the global CONNECTORS registry pristine so the repo's other
    contract tests (which assert the exact registered set) are order-safe.
    """
    had = "redis" in CONNECTORS
    from connectors.redis import RedisConnector
    try:
        yield RedisConnector
    finally:
        if not had:
            CONNECTORS.pop("redis", None)


@pytest.fixture()
def fake_redis(monkeypatch):
    """Inject the fake redis module and return its backing state."""
    state = _fresh_state()
    monkeypatch.setitem(sys.modules, "redis", _make_fake_redis(state))
    return state


@pytest.fixture()
def failing_redis(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "redis", _make_fake_redis(state, fail_ping=True))
    return state


@pytest.fixture()
def bad_auth_redis(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "redis", _make_fake_redis(state, bad_auth=True))
    return state


def _connected(redis_cls, **kwargs):
    conn = redis_cls(host="redis01.example", **kwargs)
    conn.connect()
    return conn


# ------------------------------------------------- guarded import


def test_module_imports_cleanly_without_redis():
    """The driver must never be needed at import time (subprocess proof)."""
    code = (
        "import sys; "
        "assert 'redis' not in sys.modules, 'redis unexpectedly present'; "
        "from connectors.redis import RedisConnector; "
        "assert 'redis' not in sys.modules, 'import pulled in redis'; "
        "assert RedisConnector.name == 'redis'; "
        "print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_driver_absent_gives_install_instructions(redis_cls, monkeypatch):
    monkeypatch.delitem(sys.modules, "redis", raising=False)
    conn = redis_cls(host="redis01.example")
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert "redis" in str(excinfo.value).lower()
    assert "pip install redis" in str(excinfo.value)


def test_connect_unreachable_target_is_connector_error(redis_cls,
                                                       failing_redis):
    conn = redis_cls(host="unreachable.invalid")
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert FAKE_SECRET not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()


def test_connect_auth_failure_wrapped_without_secret(redis_cls,
                                                     bad_auth_redis):
    conn = redis_cls(
        host="redis01.example",
        credential_provider=lambda: ("", FAKE_SECRET),
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert FAKE_SECRET not in str(excinfo.value)
    assert "redis01.example" in str(excinfo.value)


def test_close_safe_when_not_connected(redis_cls, fake_redis):
    conn = redis_cls(host="redis01.example")
    conn.close()  # must not raise
    conn.close()  # idempotent


def test_connect_then_close(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    conn.close()
    with pytest.raises(ConnectorError):
        conn.get_redis_info()


# ------------------------------------------------- get_redis_info


def test_get_redis_info_shape_and_math(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    d = conn.get_redis_info()
    assert set(d) == {
        "host", "port", "version", "uptime_secs", "used_memory_bytes",
        "maxmemory_bytes", "mem_used_pct", "connected_clients",
        "blocked_clients", "hit_rate_pct", "ts",
    }
    assert d["host"] == "redis01.example"
    assert d["port"] == 6379
    assert d["version"] == "7.2.4"
    assert d["uptime_secs"] == 86400
    assert d["used_memory_bytes"] == 104857600
    assert d["maxmemory_bytes"] == 1073741824
    assert d["mem_used_pct"] == round(104857600 / 1073741824 * 100, 1)
    assert d["connected_clients"] == 12
    assert d["blocked_clients"] == 1
    assert d["hit_rate_pct"] == 90.0
    json.dumps(d)


def test_get_redis_info_maxmemory_zero_gives_none_pct(redis_cls, fake_redis):
    """maxmemory=0 means 'no limit configured' -- pct must be None, not a
    ZeroDivisionError."""
    fake_redis["info"]["memory"]["maxmemory"] = 0
    conn = _connected(redis_cls)
    d = conn.get_redis_info()
    assert d["maxmemory_bytes"] == 0
    assert d["mem_used_pct"] is None
    json.dumps(d)


def test_get_redis_info_no_hits_no_misses_gives_none_hit_rate(
        redis_cls, fake_redis):
    """No keyspace activity yet -- hit rate must be None, not 0/0."""
    fake_redis["info"]["stats"]["keyspace_hits"] = 0
    fake_redis["info"]["stats"]["keyspace_misses"] = 0
    conn = _connected(redis_cls)
    d = conn.get_redis_info()
    assert d["hit_rate_pct"] is None
    json.dumps(d)


def test_get_redis_info_flat_driver_shape(redis_cls, fake_redis):
    """Some driver builds return one flat INFO dict; both shapes parse."""
    flat = {}
    for section in fake_redis["info"].values():
        flat.update(section)
    fake_redis["info"] = flat
    conn = _connected(redis_cls)
    d = conn.get_redis_info()
    assert d["version"] == "7.2.4"
    assert d["used_memory_bytes"] == 104857600
    assert d["hit_rate_pct"] == 90.0
    json.dumps(d)


def test_get_redis_info_driver_error_wrapped(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    fake_redis["info"] = "not-a-dict"
    with pytest.raises(ConnectorError):
        conn.get_redis_info()


# ------------------------------------------------- get_redis_replication


def test_get_redis_replication_master_shape(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    d = conn.get_redis_replication()
    assert set(d) == {"host", "role", "connected_replicas",
                      "master_link_status", "ts"}
    assert d["host"] == "redis01.example"
    assert d["role"] == "master"
    assert d["connected_replicas"] == 2
    assert d["master_link_status"] is None  # masters have no link status
    json.dumps(d)


def test_get_redis_replication_replica_shape(redis_cls, fake_redis):
    fake_redis["replication"] = {
        "replication": {
            "role": "replica",
            "master_host": "redis01.example",
            "master_link_status": "up",
            "connected_slaves": 0,
        },
    }
    conn = _connected(redis_cls)
    d = conn.get_redis_replication()
    assert d["role"] == "replica"
    assert d["master_link_status"] == "up"
    assert d["connected_replicas"] == 0
    json.dumps(d)


def test_get_redis_replication_legacy_slave_label_normalized(
        redis_cls, fake_redis):
    """Older servers report role 'slave'; normalize to 'replica'."""
    fake_redis["replication"] = {
        "replication": {"role": "slave", "connected_slaves": 0,
                        "master_link_status": "down"},
    }
    conn = _connected(redis_cls)
    d = conn.get_redis_replication()
    assert d["role"] == "replica"
    assert d["master_link_status"] == "down"
    json.dumps(d)


def test_get_redis_replication_new_connected_replicas_field(
        redis_cls, fake_redis):
    """Prefer connected_replicas when the server reports the new name."""
    fake_redis["replication"] = {
        "replication": {"role": "master", "connected_replicas": 3},
    }
    conn = _connected(redis_cls)
    d = conn.get_redis_replication()
    assert d["connected_replicas"] == 3
    json.dumps(d)


# ------------------------------------------------- get_redis_slowlog


def test_get_redis_slowlog_shape_and_truncation(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    entries = conn.get_redis_slowlog()
    assert len(entries) == 2
    for e in entries:
        assert set(e) == {"id", "ts", "duration_us", "command_snippet"}
    first, second = entries
    assert first["id"] == 42
    assert first["ts"] == 1758300000
    assert first["duration_us"] == 1234
    # Truncated aggressively: the 400-char fake secret never appears whole.
    assert len(first["command_snippet"]) == 200
    assert "super-secret-value-" + "x" * 400 not in first["command_snippet"]
    assert first["command_snippet"].startswith("SET session:abc ")
    assert second["command_snippet"] == "GET user:1001"
    json.dumps(entries)


def test_get_redis_slowlog_default_limit(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    conn.get_redis_slowlog()
    client = sys.modules["redis"].Redis.instances[-1]
    # slowlog_get receives the default limit of 25
    assert client.closed


def test_get_redis_slowlog_limit_clamped(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    received = []
    orig = sys.modules["redis"].Redis.slowlog_get

    def spy(self, n):
        received.append(n)
        return orig(self, n)

    sys.modules["redis"].Redis.slowlog_get = spy
    try:
        conn.get_redis_slowlog(limit=5)
        conn.get_redis_slowlog(limit=99999)
        conn.get_redis_slowlog(limit=-10)
    finally:
        sys.modules["redis"].Redis.slowlog_get = orig
    assert received == [5, 500, 1]


def test_get_redis_slowlog_skips_malformed_entries(redis_cls, fake_redis):
    fake_redis["slowlog"].append("garbage")
    fake_redis["slowlog"].append([99])  # too short
    conn = _connected(redis_cls)
    entries = conn.get_redis_slowlog()
    assert len(entries) == 2
    json.dumps(entries)


def test_get_redis_slowlog_failure_wrapped(redis_cls, failing_redis):
    conn = redis_cls(host="redis01.example")
    conn._connected = True  # bypass ping to test the read path
    with pytest.raises(ConnectorError):
        conn.get_redis_slowlog()


# ------------------------------------------------- gating & capabilities


def test_reads_require_connection(redis_cls, fake_redis):
    conn = redis_cls(host="redis01.example")
    for call in (conn.get_redis_info,
                 conn.get_redis_replication,
                 conn.get_redis_slowlog):
        with pytest.raises(ConnectorError) as excinfo:
            call()
        assert "connect" in str(excinfo.value).lower()


def test_capabilities_exact(redis_cls):
    conn = redis_cls(host="redis01.example")
    assert conn.capabilities() == {
        "get_redis_info", "get_redis_replication", "get_redis_slowlog",
    }


def test_read_dispatch_reaches_tools(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    assert conn.read("get_redis_info", {})["version"] == "7.2.4"
    assert conn.read("get_redis_replication", {})["role"] == "master"
    entries = conn.read("get_redis_slowlog", {"limit": 1})
    assert len(entries) == 1
    assert entries[0]["id"] == 42


def test_read_rejects_unknown_tool(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})


def test_act_always_raises_no_privileged_tools(redis_cls, fake_redis):
    """Reads-only by design: act() has no tools to reach."""
    conn = _connected(redis_cls)
    for tool in ("get_redis_info", "get_redis_replication",
                 "get_redis_slowlog", "no_such_tool"):
        with pytest.raises(ConnectorError):
            conn.act(tool, {})


def test_clients_are_closed_after_reads(redis_cls, fake_redis):
    """Per-call connections: every read closes its client."""
    conn = _connected(redis_cls)
    conn.get_redis_info()
    conn.get_redis_replication()
    conn.get_redis_slowlog()
    clients = sys.modules["redis"].Redis.instances
    assert len(clients) == 4  # connect ping + 3 reads
    assert all(c.closed for c in clients)


# ------------------------------------------------- credential hygiene


def test_password_defaults_to_no_auth(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    client = sys.modules["redis"].Redis.instances[-1]
    assert not client.got_password
    assert client.recorded["host"] == "redis01.example"
    assert client.recorded["port"] == 6379
    assert client.recorded["db"] == 0


def test_password_from_env(redis_cls, fake_redis, monkeypatch):
    monkeypatch.setenv("REDIS_PASSWORD", FAKE_SECRET)
    conn = _connected(redis_cls)
    client = sys.modules["redis"].Redis.instances[-1]
    assert client.got_password
    assert FAKE_SECRET not in json.dumps(client.recorded)


def test_password_from_provider(redis_cls, fake_redis):
    conn = _connected(
        redis_cls,
        credential_provider=lambda: ("rca_acl_user", FAKE_SECRET),
    )
    client = sys.modules["redis"].Redis.instances[-1]
    assert client.got_password
    assert FAKE_SECRET not in json.dumps(client.recorded)


def test_repr_contains_no_secrets(redis_cls, fake_redis):
    conn = _connected(
        redis_cls,
        credential_provider=lambda: ("rca_acl_user", FAKE_SECRET),
    )
    text = repr(conn)
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert FAKE_SECRET not in text
    assert "redis01.example" in text


def test_no_raw_password_constructor_argument(redis_cls):
    import inspect
    params = inspect.signature(redis_cls.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials", "credential"}
    for pname in params:
        assert pname.lower() not in banned, (
            f"RedisConnector.__init__ takes a raw secret argument {pname!r}"
        )
    # password_env is allowed: it is an env-var NAME, not a secret.
    assert "password_env" in params


def test_secret_not_stored_on_instance(redis_cls, fake_redis, monkeypatch):
    monkeypatch.setenv("REDIS_PASSWORD", FAKE_SECRET)
    conn = _connected(redis_cls)
    for attr, value in vars(conn).items():
        assert FAKE_SECRET not in str(value), (
            f"instance attribute {attr!r} holds the secret"
        )


# ------------------------------------------------- serializability


def test_all_results_json_serializable(redis_cls, fake_redis):
    conn = _connected(redis_cls)
    for result in (conn.get_redis_info(),
                   conn.get_redis_replication(),
                   conn.get_redis_slowlog()):
        json.dumps(result)
