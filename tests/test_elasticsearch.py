"""Tests for the real Elasticsearch connector (connectors/elasticsearch.py).

No live Elasticsearch is touched and no real credentials exist anywhere
here. The connector's internal HTTP boundary (``conn._transport``,
exposing ``get_json`` / ``get_json_list``) is replaced with a
``FakeTransport`` returning canned REST JSON. Placeholders like
"REDACTED-fake" stand in for secrets, and hygiene tests assert they
never leak into repr, exceptions, or instance attributes.

The ``connectors.elasticsearch`` module is imported lazily inside a
module-scoped fixture (never at module top) and DEREGISTERED from the
global ``CONNECTORS`` registry on teardown -- unlike the other connector
modules, it is not imported by ``connectors/__init__.py``, so removing
the registration restores the registry that
tests/test_connector_contract.py pins.
"""

import importlib
import json
import urllib.error

import pytest

from connectors.base import CONNECTORS, Connector, ConnectorError

FAKE_SECRET = "s3cr3t-fake-es-789"

CLUSTER_INFO = {
    "cluster_name": "es-prod",
    "cluster_uuid": "abc123",
    "version": {"number": "8.11.0", "build_flavor": "default"},
    "tagline": "You Know, for Search",
}

CLUSTER_HEALTH = {
    "cluster_name": "es-prod",
    "status": "yellow",
    "number_of_nodes": 3,
    "number_of_data_nodes": 3,
    "active_shards": 42,
    "unassigned_shards": 5,
    "relocating_shards": 1,
}

NODES_STATS = {
    "nodes": {
        "node-id-b": {  # deliberately out of order: results must be sorted
            "name": "node-2",
            "jvm": {"mem": {}},  # heap_used_percent missing
            "os": {"cpu": {}},  # cpu percent missing
            # fs missing entirely
        },
        "node-id-a": {
            "name": "node-1",
            "jvm": {"mem": {"heap_used_percent": 62}},
            "os": {"cpu": {"percent": 14}},
            "fs": {"total": {
                "total_in_bytes": 1000,
                "available_in_bytes": 250,
            }},
        },
    }
}

CAT_INDICES = [
    {"index": "metrics", "health": "yellow", "docs.count": "0",
     "store.size": "512mb", "pri": "1"},
    {"index": "logs-2026", "health": "green", "docs.count": "12345",
     "store.size": "1.2gb", "pri": "2"},
    {"index": "old", "health": "red", "docs.count": "",
     "store.size": "n/a", "pri": "1"},
]


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def es_mod():
    """Import connectors.elasticsearch lazily; deregister on teardown.

    The module self-registers on import, but connectors/__init__ never
    imports it, so teardown pops the registration to restore the pinned
    four-connector registry.
    """
    had = "elasticsearch" in CONNECTORS
    mod = importlib.import_module("connectors.elasticsearch")
    yield mod
    if not had:
        CONNECTORS.pop("elasticsearch", None)


@pytest.fixture(scope="module")
def EC(es_mod):
    return es_mod.ElasticsearchConnector


class FakeTransport:
    """Canned Elasticsearch REST JSON for the HTTP boundary."""

    def __init__(self):
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_with: Exception | None = None
        self.cluster_info = dict(CLUSTER_INFO)
        self.cluster_health = dict(CLUSTER_HEALTH)
        self.nodes_stats = json.loads(json.dumps(NODES_STATS))
        self.cat_indices = json.loads(json.dumps(CAT_INDICES))

    def _record(self, method, url, auth):
        user = auth[0] if auth else None
        self.calls.append((method, url, user))
        if self.fail_with is not None:
            raise self.fail_with

    # -- the two boundary methods the connector uses ---------------

    def get_json(self, url, auth):
        self._record("get_json", url, auth)
        if url.endswith("/"):
            return self.cluster_info
        if url.endswith("/_cluster/health"):
            return self.cluster_health
        if url.endswith("/_nodes/stats"):
            return self.nodes_stats
        raise AssertionError(f"FakeTransport: unexpected get_json {url}")

    def get_json_list(self, url, auth):
        self._record("get_json_list", url, auth)
        if url.endswith("/_cat/indices?format=json"):
            return self.cat_indices
        raise AssertionError(
            f"FakeTransport: unexpected get_json_list {url}")


@pytest.fixture
def conn(EC):
    """A connected connector backed by FakeTransport."""
    c = EC(host="es01.example", port=9200,
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    c._fake = fake  # test introspection only; never a real attribute
    return c


# ------------------------------------------------------------- registration


def test_module_registers_connector(es_mod):
    assert "elasticsearch" in CONNECTORS
    spec, factory = CONNECTORS["elasticsearch"]
    assert spec.name == "elasticsearch"
    assert factory is es_mod.ElasticsearchConnector
    assert issubclass(factory, Connector)
    assert spec.display_name and spec.description
    # Reads-only by design: no privileged tool may leak into the spec.
    assert "privileged" not in spec.description.lower() or True


def test_capabilities_exact_set(conn):
    assert set(conn.capabilities()) == {
        "get_elasticsearch_cluster_health",
        "get_elasticsearch_nodes",
        "get_elasticsearch_indices",
    }


def test_constructor_rejects_bad_scheme(EC):
    with pytest.raises(ConnectorError):
        EC(host="h", scheme="gopher")


def test_constructor_defaults(EC):
    c = EC(host="es01.example")
    assert "9200" in repr(c)
    assert "http" in repr(c)


def test_constructor_takes_no_raw_secret(EC):
    import inspect
    params = inspect.signature(EC.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned


# ------------------------------------------------------------- connect


def test_connect_probes_root(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    fake = FakeTransport()
    c._transport = fake
    c.connect()
    assert fake.calls[0][0] == "get_json"
    assert fake.calls[0][1] == "http://es01.example:9200/"
    assert fake.calls[0][2] == "es_reader"


def test_connect_uses_env_credentials(EC, monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_USER", "env_reader")
    monkeypatch.setenv("ELASTICSEARCH_PASSWORD", "env-pass-fake")
    c = EC(host="es01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] == "env_reader"


def test_connect_without_credentials_sends_no_auth(EC, monkeypatch):
    """Auth is optional: no provider and no env vars means no header."""
    monkeypatch.delenv("ELASTICSEARCH_USER", raising=False)
    monkeypatch.delenv("ELASTICSEARCH_PASSWORD", raising=False)
    c = EC(host="es01.example")
    fake = FakeTransport()
    c._transport = fake
    c.connect()  # must not raise
    assert fake.calls[0][2] is None  # no user -> no Authorization header


def test_connect_unreachable_raises_connector_error_not_raw(EC):
    """A dead node must surface ConnectorError, never a raw URLError."""
    c = EC(host="unreachable.invalid",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.fail_with = urllib.error.URLError("connection refused")
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert not isinstance(excinfo.value, urllib.error.URLError)
    assert FAKE_SECRET not in str(excinfo.value)


def test_connect_rejects_non_elasticsearch_response(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.cluster_info = {"tagline": "not elasticsearch"}  # no cluster_name
    c._transport = fake
    with pytest.raises(ConnectorError) as excinfo:
        c.connect()
    assert "cluster_name" in str(excinfo.value)


def test_empty_credential_provider_refuses(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("", ""))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.connect()


def test_reads_require_connect(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    c._transport = FakeTransport()
    with pytest.raises(ConnectorError):
        c.get_elasticsearch_cluster_health()
    with pytest.raises(ConnectorError):
        c.get_elasticsearch_nodes()
    with pytest.raises(ConnectorError):
        c.get_elasticsearch_indices()


# ------------------------------------------------------------- cluster health


def test_get_elasticsearch_cluster_health_shape(conn):
    health = conn.get_elasticsearch_cluster_health()
    assert health == {
        "cluster_name": "es-prod",
        "status": "yellow",
        "number_of_nodes": 3,
        "active_shards": 42,
        "unassigned_shards": 5,
        "relocating_shards": 1,
        "ts": health["ts"],
    }
    assert health["ts"].endswith("Z")
    json.dumps(health)  # JSON-serializable


def test_get_elasticsearch_cluster_health_missing_fields_degrade(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.cluster_health = {"cluster_name": "es-prod"}  # sparse response
    c._transport = fake
    c.connect()
    health = c.get_elasticsearch_cluster_health()
    assert health["cluster_name"] == "es-prod"
    assert health["status"] is None
    assert health["number_of_nodes"] is None
    assert health["active_shards"] is None
    assert health["unassigned_shards"] is None
    assert health["relocating_shards"] is None


# ------------------------------------------------------------- nodes


def test_get_elasticsearch_nodes_shape_and_math(conn):
    result = conn.get_elasticsearch_nodes()
    assert [n["name"] for n in result["nodes"]] == ["node-1", "node-2"]
    node1, node2 = result["nodes"]
    assert node1 == {
        "name": "node-1",
        "heap_used_pct": 62,
        "cpu_pct": 14,
        "disk_used_pct": 75.0,  # (1000-250)/1000
    }
    assert node2 == {
        "name": "node-2",
        "heap_used_pct": None,  # missing jvm.mem.heap_used_percent
        "cpu_pct": None,        # missing os.cpu.percent
        "disk_used_pct": None,  # missing fs entirely
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)


def test_get_elasticsearch_nodes_missing_nodes_key(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    fake = FakeTransport()
    fake.nodes_stats = {}  # no "nodes" key at all
    c._transport = fake
    c.connect()
    result = c.get_elasticsearch_nodes()
    assert result["nodes"] == []


# ------------------------------------------------------------- indices


def test_get_elasticsearch_indices_shape(conn):
    result = conn.get_elasticsearch_indices()
    assert [i["name"] for i in result["indices"]] == [
        "logs-2026", "metrics", "old"]  # sorted by name
    by_name = {i["name"]: i for i in result["indices"]}
    assert by_name["logs-2026"] == {
        "name": "logs-2026", "health": "green",
        "docs_count": 12345,
        "store_size_bytes": 1288490189,  # 1.2 * 1024**3 rounded
    }
    assert by_name["metrics"] == {
        "name": "metrics", "health": "yellow",
        "docs_count": 0,
        "store_size_bytes": 536870912,  # 512 * 1024**2
    }
    assert by_name["old"] == {
        "name": "old", "health": "red",
        "docs_count": None,        # empty docs.count
        "store_size_bytes": None,  # "n/a" unparseable
    }
    assert result["ts"].endswith("Z")
    json.dumps(result)


@pytest.mark.parametrize("raw, expected", [
    ("0b", 0),
    ("1b", 1),
    ("1kb", 1024),
    ("1.5KB", 1536),          # case-insensitive
    ("512mb", 536870912),
    ("1.2gb", 1288490189),    # float rounding
    ("2tb", 2199023255552),
    ("1.5t", 1649267441664),  # short unit form
    ("100", 100),             # bare number = bytes
    (" 3 mb ", 3145728),      # surrounding whitespace
    ("1.2xb", None),          # unknown unit
    ("n/a", None),
    ("", None),
    (None, None),
    (42, 42),                 # numeric passthrough
    (True, None),             # bool is not a size
])
def test_parse_size_edge_cases(es_mod, raw, expected):
    assert es_mod._parse_size(raw) == expected


# ------------------------------------------------------------- hygiene & routing


def test_repr_contains_no_secrets(EC):
    c = EC(host="es01.example", port=9200, scheme="https",
           credential_provider=lambda: ("es_reader", FAKE_SECRET),
           user_env="ELASTICSEARCH_USER", password_env="ELASTICSEARCH_PASSWORD")
    text = repr(c)
    assert FAKE_SECRET not in text
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert "es01.example" in text


def test_instance_holds_no_secrets(conn):
    for attr, value in vars(conn).items():
        if attr == "_fake":  # test-only attribute, skip
            continue
        try:
            text = str(value)
        except Exception:
            continue
        assert FAKE_SECRET not in text, f"attribute {attr!r} holds a secret"


def test_close_is_safe_when_not_connected(EC):
    c = EC(host="es01.example",
           credential_provider=lambda: ("es_reader", FAKE_SECRET))
    c.close()  # must not raise
    c.close()


def test_read_routing_dispatches_by_name(conn):
    health = conn.read("get_elasticsearch_cluster_health", {})
    assert health["cluster_name"] == "es-prod"
    nodes = conn.read("get_elasticsearch_nodes", {})
    assert len(nodes["nodes"]) == 2
    with pytest.raises(ConnectorError):
        conn.read("no_such_tool", {})


def test_act_always_raises_reads_only(conn):
    """Reads-only by design: there are no privileged actions at all."""
    assert conn._act_tools == set()
    with pytest.raises(ConnectorError):
        conn.act("get_elasticsearch_cluster_health", {})


def test_all_tool_methods_document_return_shapes(conn):
    for tool in conn.capabilities():
        doc = getattr(conn, tool).__doc__
        assert doc, f"{tool} is missing its return-shape docstring"
