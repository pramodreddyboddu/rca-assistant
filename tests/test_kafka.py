"""Tests for the real Kafka connector (connectors/kafka.py).

kafka-python is NOT installed here and no live cluster is touched. A fake
``kafka`` module is injected into ``sys.modules`` (and removed after each
test by monkeypatch), covering consumer lag (summed + per-partition),
topic detail, broker config, broker health, the privileged restart hook,
and credential hygiene. No test fixture contains a real secret:
placeholders like "REDACTED-fake" stand in, and the hygiene tests assert
they never leak into repr, exceptions, or logs.
"""

import json
import subprocess
import sys
import types
from collections import namedtuple

import pytest

from connectors.base import ConnectorError
from connectors.kafka import KafkaConnector

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"


# ------------------------------------------------------------- fake kafka


FakeTopicPartition = namedtuple("FakeTopicPartition", ["topic", "partition"])
FakeConfigEntry = namedtuple("FakeConfigEntry", ["name", "value"])


def _make_fake_kafka(state, fail_connect=False, describe_topics=True,
                     legacy_config_shape=False):
    """Build a fake `kafka` module modelling kafka-python's PUBLIC API.

    FakeKafkaConsumer: topics(), partitions_for_topic(), end_offsets(),
    committed(), close(), bootstrap_connected(). FakeKafkaAdminClient:
    describe_configs() returning the kafka-python 3.x resolved nested
    dict (or the legacy {ConfigResource: Future} shape when
    legacy_config_shape=True) and, when describe_topics=True,
    describe_topics() (dict of topic -> future of partition-metadata
    entries).
    """
    mod = types.ModuleType("kafka")
    mod.TopicPartition = FakeTopicPartition
    mod.ConfigEntry = FakeConfigEntry

    class FakeKafkaError(Exception):
        pass

    class FakeFuture:
        def __init__(self, value=None, error=None):
            self._value = value
            self._error = error

        def result(self, timeout=None):
            if self._error is not None:
                raise self._error
            return self._value

    class FakeConfigResource:
        def __init__(self, resource_type, name):
            self.resource_type = resource_type
            self.name = name

        def __eq__(self, other):
            return (
                isinstance(other, FakeConfigResource)
                and (self.resource_type, self.name)
                == (other.resource_type, other.name)
            )

        def __hash__(self):
            return hash((self.resource_type, self.name))

        def __repr__(self):
            return f"ConfigResource({self.resource_type}, {self.name!r})"

    mod.ConfigResource = FakeConfigResource
    mod.ConfigResourceType = types.SimpleNamespace(BROKER=4)
    # kafka-python 3.x layout: `from kafka.admin import ConfigResource`.
    admin_mod = types.ModuleType("kafka.admin")
    admin_mod.ConfigResource = FakeConfigResource
    admin_mod.ConfigResourceType = mod.ConfigResourceType
    mod.admin = admin_mod

    def _unreachable(kwargs):
        return fail_connect or "unreachable" in str(
            kwargs.get("bootstrap_servers", ""))

    class FakeKafkaConsumer:
        instances = []

        def __init__(self, **kwargs):
            self._kwargs = kwargs
            self._group = kwargs.get("group_id")
            # Deliberately records everything EXCEPT the password value.
            self.recorded = {
                k: v for k, v in kwargs.items()
                if k != "sasl_plain_password"
            }
            self.got_password = bool(kwargs.get("sasl_plain_password"))
            self.closed = False
            FakeKafkaConsumer.instances.append(self)

        def topics(self):
            if _unreachable(self._kwargs):
                raise FakeKafkaError("NoBrokersAvailable: connection refused")
            return set(state["topics"])

        def partitions_for_topic(self, topic):
            if _unreachable(self._kwargs):
                raise FakeKafkaError("NoBrokersAvailable: connection refused")
            t = state["topics"].get(topic)
            return set(t["partitions"]) if t else None

        def end_offsets(self, tps):
            out = {}
            for tp in tps:
                t = state["topics"].get(tp.topic, {})
                part = t.get("partitions", {}).get(tp.partition, {})
                out[tp] = part.get("end", 0)
            return out

        def committed(self, tp):
            t = state["topics"].get(tp.topic, {})
            part = t.get("partitions", {}).get(tp.partition, {})
            return part.get("committed", {}).get(self._group)

        def bootstrap_connected(self):
            return not _unreachable(self._kwargs)

        def close(self):
            self.closed = True

    class FakeKafkaAdminClient:
        instances = []

        def __init__(self, **kwargs):
            self._kwargs = kwargs
            self.recorded = {
                k: v for k, v in kwargs.items()
                if k != "sasl_plain_password"
            }
            self.got_password = bool(kwargs.get("sasl_plain_password"))
            self.closed = False
            FakeKafkaAdminClient.instances.append(self)

        def describe_configs(self, resources, config_filter="modified",
                             **kwargs):
            if legacy_config_shape:
                return self.describe_configs_legacy(resources)
            # Models kafka-python 3.x: a resolved nested dict
            # {resource_type: {resource_name: {key: {value, sensitive}}}}.
            out: dict = {}
            for res in resources:
                broker = state["brokers"].get(res.name)
                if (res.resource_type == mod.ConfigResourceType.BROKER
                        and broker is not None):
                    entries = {
                        n: {"value": v, "sensitive": False}
                        for n, v in broker["configs"].items()
                    }
                    out.setdefault("broker", {})[res.name] = entries
                # unknown brokers simply have no entries (3.x shape)
            return out

        def describe_configs_legacy(self, resources):
            # Older driver layout: {ConfigResource: Future}. Kept to prove
            # the connector still handles the legacy shape.
            out = {}
            for res in resources:
                broker = state["brokers"].get(res.name)
                if (res.resource_type == mod.ConfigResourceType.BROKER
                        and broker is not None):
                    entries = [FakeConfigEntry(n, v) for n, v in
                               broker["configs"].items()]
                    out[res] = FakeFuture(value=entries)
                else:
                    out[res] = FakeFuture(
                        error=FakeKafkaError(
                            f"unknown broker {res.name!r}"))
            return out

        def close(self):
            self.closed = True

    if describe_topics:
        def _describe_topics(self, topics, **kwargs):
            out = {}
            for topic in topics:
                t = state["topics"].get(topic)
                if t is None:
                    out[topic] = FakeFuture(
                        error=FakeKafkaError(f"unknown topic {topic!r}"))
                    continue
                entries = [
                    types.SimpleNamespace(
                        partition=p,
                        leader=info["leader"],
                        replicas=list(info["replicas"]),
                        isr=list(info["isr"]),
                    )
                    for p, info in sorted(t["partitions"].items())
                ]
                out[topic] = FakeFuture(value=entries)
            return out

        FakeKafkaAdminClient.describe_topics = _describe_topics

    mod.KafkaError = FakeKafkaError
    mod.KafkaConsumer = FakeKafkaConsumer
    mod.KafkaAdminClient = FakeKafkaAdminClient
    return mod


def _fresh_state():
    return {
        "topics": {
            "orders": {
                "partitions": {
                    0: {"end": 1000, "committed": {"group-a": 990},
                        "leader": 1, "replicas": [1, 2], "isr": [1, 2]},
                    1: {"end": 2000, "committed": {"group-a": 2000},
                        "leader": 2, "replicas": [2, 3], "isr": [2, 3]},
                    2: {"end": 500, "committed": {},
                        "leader": 1, "replicas": [1, 3], "isr": [1]},
                },
            },
        },
        "brokers": {
            "1": {"configs": {
                "log.retention.hours": "168",
                "num.partitions": "3",
                "auto.create.topics.enable": "true",
            }},
            "2": {"configs": {"log.retention.hours": "72"}},
        },
    }


@pytest.fixture()
def fake_kafka(monkeypatch):
    """Inject the fake kafka module and return its backing state."""
    state = _fresh_state()
    monkeypatch.setitem(sys.modules, "kafka", _make_fake_kafka(state))
    return state


@pytest.fixture()
def failing_kafka(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kafka", _make_fake_kafka(state, fail_connect=True))
    return state


@pytest.fixture()
def fake_kafka_no_topic_meta(monkeypatch):
    """Fake driver WITHOUT public describe_topics (degraded metadata)."""
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kafka",
        _make_fake_kafka(state, describe_topics=False))
    return state


def _conn(**kwargs):
    kwargs.setdefault("credential_provider",
                      lambda: ("rca_reader", FAKE_SECRET))
    kwargs.setdefault("privileged_credential_provider",
                      lambda: ("rca_admin", FAKE_ADMIN_SECRET))
    return KafkaConnector(bootstrap_servers="kafka01.example:9092", **kwargs)


# ------------------------------------------------- guarded import


def test_module_imports_cleanly_without_kafka():
    """The driver must never be needed at import time (subprocess proof)."""
    code = (
        "import sys; "
        "assert 'kafka' not in sys.modules, 'kafka unexpectedly present'; "
        "from connectors.kafka import KafkaConnector; "
        "assert 'kafka' not in sys.modules, 'import pulled in kafka'; "
        "print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_driver_absent_gives_install_instructions(monkeypatch):
    monkeypatch.delitem(sys.modules, "kafka", raising=False)
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_kafka_topic_detail("orders")
    assert "kafka-python" in str(excinfo.value)
    assert "pip install kafka-python" in str(excinfo.value)


def test_connect_unreachable_target_is_connector_error(failing_kafka):
    conn = KafkaConnector(
        bootstrap_servers="unreachable.invalid:9092",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert FAKE_SECRET not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()


def test_close_safe_when_not_connected(fake_kafka):
    conn = _conn()
    conn.close()  # must not raise


# ------------------------------------------------- consumer lag


def test_get_kafka_consumer_lag_shape_and_math(fake_kafka):
    conn = _conn()
    d = conn.get_kafka_consumer_lag("orders", "group-a")
    assert set(d) == {"topic", "group", "lag", "ts"}
    # p0: 1000-990=10; p1: 2000-2000=0; p2: 500-0(no commit)=500
    assert d["lag"] == 510
    assert d["topic"] == "orders"
    assert d["group"] == "group-a"
    json.dumps(d)


def test_get_kafka_consumer_lag_unknown_topic(fake_kafka):
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_kafka_consumer_lag("nope", "group-a")
    assert "unknown topic" in str(excinfo.value)


def test_get_kafka_consumer_group_detail_per_partition(fake_kafka):
    conn = _conn()
    d = conn.get_kafka_consumer_group_detail("orders", "group-a")
    assert set(d) == {"topic", "group", "partitions", "total_lag", "ts"}
    assert d["total_lag"] == 510
    parts = {p["partition"]: p for p in d["partitions"]}
    assert sorted(parts) == [0, 1, 2]
    assert parts[0] == {"partition": 0, "end_offset": 1000,
                        "committed": 990, "lag": 10}
    assert parts[1] == {"partition": 1, "end_offset": 2000,
                        "committed": 2000, "lag": 0}
    # never committed -> committed 0, lag is the full end offset
    assert parts[2] == {"partition": 2, "end_offset": 500,
                        "committed": 0, "lag": 500}
    json.dumps(d)


def test_lag_clamped_at_zero(fake_kafka):
    """Committed beyond the end offset (post-retention) never goes negative."""
    state = fake_kafka
    state["topics"]["orders"]["partitions"][0]["committed"]["group-a"] = 99999
    conn = _conn()
    d = conn.get_kafka_consumer_group_detail("orders", "group-a")
    assert d["partitions"][0]["lag"] == 0
    assert d["total_lag"] == 500  # p1 0 + p2 500


# ------------------------------------------------- topic detail


def test_get_kafka_topic_detail_shape(fake_kafka):
    conn = _conn()
    d = conn.get_kafka_topic_detail("orders")
    assert set(d) == {"topic", "partitions", "ts"}
    assert d["topic"] == "orders"
    parts = {p["partition"]: p for p in d["partitions"]}
    assert sorted(parts) == [0, 1, 2]
    assert parts[0] == {"partition": 0, "leader": 1,
                        "replicas": [1, 2], "isr": [1, 2],
                        "end_offset": 1000}
    assert parts[1]["leader"] == 2
    assert parts[1]["end_offset"] == 2000
    assert parts[2] == {"partition": 2, "leader": 1,
                        "replicas": [1, 3], "isr": [1],
                        "end_offset": 500}
    json.dumps(d)


def test_get_kafka_topic_detail_degraded_without_describe_topics(
        fake_kafka_no_topic_meta):
    """No public describe_topics -> leader None, replicas/isr empty, but the
    read still succeeds with end offsets."""
    conn = _conn()
    d = conn.get_kafka_topic_detail("orders")
    parts = {p["partition"]: p for p in d["partitions"]}
    assert parts[0] == {"partition": 0, "leader": None,
                        "replicas": [], "isr": [],
                        "end_offset": 1000}
    json.dumps(d)


def test_get_kafka_topic_detail_unknown_topic(fake_kafka):
    conn = _conn()
    with pytest.raises(ConnectorError):
        conn.get_kafka_topic_detail("nope")


# ------------------------------------------------- broker config & health


def test_get_kafka_broker_config_shape(fake_kafka):
    conn = _conn()
    d = conn.get_kafka_broker_config(1)
    assert set(d) == {"broker_id", "configs", "ts"}
    assert d["broker_id"] == 1
    assert d["configs"] == {
        "log.retention.hours": "168",
        "num.partitions": "3",
        "auto.create.topics.enable": "true",
    }
    json.dumps(d)


def test_get_kafka_broker_config_unknown_broker(fake_kafka):
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_kafka_broker_config(99)
    assert "99" in str(excinfo.value)


def test_get_kafka_broker_config_uses_admin_client(fake_kafka):
    """Broker config goes through KafkaAdminClient, not the consumer."""
    conn = _conn()
    conn.get_kafka_broker_config(2)
    admin = sys.modules["kafka"].KafkaAdminClient.instances[-1]
    assert admin.closed  # released after the call


def test_get_kafka_broker_config_legacy_futures_shape(monkeypatch):
    """Older drivers returning {ConfigResource: Future} still parse."""
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "kafka",
        _make_fake_kafka(state, legacy_config_shape=True))
    conn = _conn()
    d = conn.get_kafka_broker_config(1)
    assert d["broker_id"] == 1
    assert d["configs"]["num.partitions"] == "3"


def test_get_kafka_broker_health_shape(fake_kafka):
    conn = _conn()
    d = conn.get_kafka_broker_health()
    assert d["bootstrap_servers"] == "kafka01.example:9092"
    assert d["reachable"] is True
    assert isinstance(d["latency_ms"], float) and d["latency_ms"] >= 0
    # kafka-python exposes no public broker listing: honest degradation.
    assert d["brokers"] == []
    assert d["controller"] is None
    assert d["degraded"] is True
    assert d["degradation"]
    json.dumps(d)


def test_get_kafka_broker_health_unreachable(failing_kafka):
    conn = _conn()
    with pytest.raises(ConnectorError) as excinfo:
        conn.get_kafka_broker_health()
    assert FAKE_SECRET not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()


# ------------------------------------------------- privileged restart


def test_restart_consumer_with_hook(fake_kafka):
    calls = []

    def hook(topic, group, user, password):
        calls.append((topic, group, user, password))
        return {"restarted": True, "supervisor": "systemd"}

    conn = _conn(restart_hook=hook)
    d = conn.restart_consumer("orders", "group-a")
    assert calls == [("orders", "group-a", "rca_admin", FAKE_ADMIN_SECRET)]
    # privileged identity reaches the hook; the READ credential never does
    assert "rca_reader" not in str(calls)
    assert set(d) == {"topic", "group", "ts", "restarted", "supervisor"}
    assert d["restarted"] is True
    json.dumps(d)


def test_restart_consumer_without_hook_refuses(fake_kafka):
    conn = _conn()  # privileged credential configured, no hook
    with pytest.raises(ConnectorError) as excinfo:
        conn.restart_consumer("orders", "group-a")
    assert "restart_hook" in str(excinfo.value)


def test_restart_consumer_without_privileged_credential_refuses(
        fake_kafka, monkeypatch):
    """The privileged path must not silently reuse the read credential."""
    for var in ("KAFKA_ADMIN_USER", "KAFKA_ADMIN_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    called = []

    def hook(topic, group, user, password):
        called.append((topic, group))
        return {}

    conn = KafkaConnector(
        bootstrap_servers="kafka01.example:9092",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
        restart_hook=hook,
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.restart_consumer("orders", "group-a")
    assert "privileged" in str(excinfo.value).lower()
    assert called == [], "hook must not run without the privileged credential"


def test_restart_consumer_privileged_env_fallback(fake_kafka, monkeypatch):
    monkeypatch.setenv("KAFKA_ADMIN_USER", "env-admin")
    monkeypatch.setenv("KAFKA_ADMIN_PASSWORD", "REDACTED-fake-env-admin")
    seen = {}

    def hook(topic, group, user, password):
        seen["user"] = user
        seen["pw"] = password
        return {}

    conn = KafkaConnector(
        bootstrap_servers="kafka01.example:9092",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
        restart_hook=hook,
    )
    conn.restart_consumer("orders", "group-a")
    assert seen["user"] == "env-admin"
    assert seen["pw"] == "REDACTED-fake-env-admin"


def test_restart_consumer_hook_failure_wrapped(fake_kafka):
    def hook(topic, group, user, password):
        raise RuntimeError("supervisor exploded")

    conn = _conn(restart_hook=hook)
    with pytest.raises(ConnectorError) as excinfo:
        conn.restart_consumer("orders", "group-a")
    assert FAKE_ADMIN_SECRET not in str(excinfo.value)


# ------------------------------------------------- credential hygiene


def test_sasl_only_when_protocol_uses_sasl(fake_kafka):
    conn = _conn(security_protocol="PLAINTEXT")
    conn.get_kafka_consumer_lag("orders", "group-a")
    consumer = sys.modules["kafka"].KafkaConsumer.instances[-1]
    assert not any(k.startswith("sasl_") for k in consumer.recorded)


def test_sasl_kwargs_carry_read_credential(fake_kafka, monkeypatch):
    monkeypatch.setenv("KAFKA_USER", "env-reader")
    monkeypatch.setenv("KAFKA_PASSWORD", "REDACTED-fake-env")
    conn = KafkaConnector(bootstrap_servers="kafka01.example:9092")
    conn.get_kafka_consumer_lag("orders", "group-a")
    consumer = sys.modules["kafka"].KafkaConsumer.instances[-1]
    assert consumer.recorded["sasl_plain_username"] == "env-reader"
    assert consumer.got_password  # passed through, never recorded
    assert "REDACTED-fake-env" not in json.dumps(consumer.recorded)


def test_repr_contains_no_secrets(fake_kafka):
    conn = _conn(restart_hook=lambda t, g, u, p: {})
    text = repr(conn)
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
    assert FAKE_SECRET not in text
    assert FAKE_ADMIN_SECRET not in text
    assert "kafka01.example:9092" in text


def test_no_raw_password_constructor_argument():
    import inspect
    params = inspect.signature(KafkaConnector.__init__).parameters
    banned = {"password", "secret", "passwd", "token", "api_key",
              "credentials"}
    for pname in params:
        assert pname.lower() not in banned, (
            f"KafkaConnector.__init__ takes a raw secret argument {pname!r}"
        )


# ------------------------------------------------- routing & serializability


def test_read_rejects_privileged_action(fake_kafka):
    conn = _conn()
    with pytest.raises(ConnectorError):
        conn.read("restart_consumer", {"topic": "orders", "group": "group-a"})


def test_act_rejects_read_tools(fake_kafka):
    conn = _conn()
    for tool in ("get_kafka_consumer_lag", "get_kafka_consumer_group_detail",
                 "get_kafka_topic_detail", "get_kafka_broker_config",
                 "get_kafka_broker_health"):
        with pytest.raises(ConnectorError):
            conn.act(tool, {})


def test_read_dispatch_reaches_new_tools(fake_kafka):
    conn = _conn()
    assert conn.read("get_kafka_topic_detail",
                     {"topic": "orders"})["topic"] == "orders"
    assert conn.read("get_kafka_broker_config",
                     {"broker_id": 1})["broker_id"] == 1
    assert conn.read("get_kafka_broker_health", {})["reachable"] is True
    assert conn.read("get_kafka_consumer_group_detail",
                     {"topic": "orders",
                      "group": "group-a"})["total_lag"] == 510


def test_all_results_json_serializable(fake_kafka):
    conn = _conn(restart_hook=lambda t, g, u, p: {"ok": True})
    results = [
        conn.get_kafka_consumer_lag("orders", "group-a"),
        conn.get_kafka_consumer_group_detail("orders", "group-a"),
        conn.get_kafka_topic_detail("orders"),
        conn.get_kafka_broker_config(1),
        conn.get_kafka_broker_health(),
        conn.restart_consumer("orders", "group-a"),
    ]
    for r in results:
        json.dumps(r)
