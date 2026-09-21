"""Real Kafka connector: live diagnostics via kafka-python.

Reads (consumer lag, per-partition lag detail, topic detail, broker config,
broker health) go through ``kafka-python`` (imported LAZILY: this module
imports cleanly when kafka-python is absent; only the driver paths raise,
with install instructions). ``restart_consumer`` is deliberately
environment-specific (Kafka has no remote "restart a consumer group" API)
and runs only through a customer-supplied ``restart_hook`` callable;
without a hook it raises ConnectorError. The privileged path resolves a
SEPARATE privileged credential pair and refuses when it is absent rather
than reusing the read credential.

Only PUBLIC kafka-python APIs are used (KafkaConsumer, KafkaAdminClient,
TopicPartition, ConfigResource). kafka-python's public surface exposes no
cluster/broker listing, so ``get_kafka_broker_health`` degrades honestly
to reachable + latency, and leader/replica/ISR fields in
``get_kafka_topic_detail`` fall back to empty when the driver's public
``describe_topics`` is unavailable. No private (``_``-prefixed) driver
internals are touched anywhere.

STATUS: validated against a fake kafka-python driver in our test suite
(see tests/test_kafka.py). Live validation against a real cluster happens
in a customer pilot.

CREDENTIALS
-----------
Same rules as the IBM MQ connector: the constructor never takes a raw
secret. Pass either:

- ``credential_provider``: a zero-arg callable returning ``(user, password)``
  (typically backed by the customer's vault), or
- ``user_env`` / ``password_env``: env-var NAMES (defaults ``KAFKA_USER`` /
  ``KAFKA_PASSWORD``).

SASL is only configured when ``security_protocol`` starts with ``SASL_``.
Secrets resolve at connect/read time, are held only in local variables for
the duration of the call, and never appear in ``repr``, exceptions, or
logs. The privileged ``restart_consumer`` path resolves a SEPARATE
credential pair (``privileged_credential_provider`` or
``KAFKA_ADMIN_USER`` / ``KAFKA_ADMIN_PASSWORD``); when it is not
configured, ``restart_consumer`` refuses rather than silently reusing the
read credential. The resolved privileged identity is passed to the
customer's ``restart_hook`` as ``(topic, group, user, password)`` so the
hook can authenticate to the customer's own consumer supervisor.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)


def _kafka() -> Any:
    """Import kafka lazily; raise ConnectorError with install instructions."""
    try:
        import kafka  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "the Kafka connector needs the 'kafka-python' driver, which is "
            "not installed. Install it with: pip install kafka-python"
        ) from exc
    return kafka


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class KafkaConnector(Connector):
    """Live Kafka diagnostics (+ hook-driven consumer restart). name = "kafka"."""

    name = "kafka"

    SPEC = ConnectorSpec(
        name="kafka",
        display_name="Apache Kafka (kafka-python)",
        description="Live Kafka diagnostics via kafka-python: consumer lag "
        "(summed and per-partition), per-partition topic detail "
        "(leader/replicas/ISR/end offset), broker config, and broker "
        "health. Privileged consumer restarts run only through a "
        "customer-supplied restart hook (no remote restart API exists in "
        "Kafka) under a separate privileged credential.",
        required_config=("bootstrap_servers",),
        optional_config=(
            "security_protocol", "sasl_mechanism", "user_env",
            "password_env", "privileged_user_env",
            "privileged_password_env", "restart_hook",
        ),
        credential_refs=("KAFKA_USER", "KAFKA_PASSWORD"),
        notes="Reads use a read-only Kafka service account. "
        "restart_consumer uses a separate credential (KAFKA_ADMIN_USER / "
        "KAFKA_ADMIN_PASSWORD or a privileged provider) and refuses when it "
        "is absent; it also needs a restart_hook callable wired to the "
        "customer's own consumer supervisor. Only public kafka-python APIs "
        "are used: kafka-python exposes no public cluster/broker listing, "
        "so broker health degrades to reachable + latency and "
        "leader/replica/ISR fields may be empty when the driver's public "
        "describe_topics is unavailable. Fake-transport tested; live "
        "validation against a real cluster happens in a customer pilot.",
    )

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        security_protocol: str = "SASL_SSL",
        sasl_mechanism: str = "PLAIN",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "KAFKA_USER",
        password_env: str = "KAFKA_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "KAFKA_ADMIN_USER",
        privileged_password_env: str = "KAFKA_ADMIN_PASSWORD",
        restart_hook: Callable[[str, str, str, str], dict] | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._restart_hook = restart_hook
        self._consumer: Any = None  # probe consumer, once connected

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_kafka_consumer_lag",
            "get_kafka_consumer_group_detail",
            "get_kafka_topic_detail",
            "get_kafka_broker_config",
            "get_kafka_broker_health",
            "restart_consumer",
        }

    def connect(self) -> None:
        """Build a probe consumer to verify the cluster is reachable."""
        kafka = _kafka()
        consumer = self._build_consumer(kafka, group_id="rca-assistant-probe")
        try:
            # A metadata request proves the bootstrap servers are reachable.
            consumer.topics()
        except Exception as exc:
            _close_quietly(consumer)
            raise ConnectorError(
                f"{self.name}: cannot reach Kafka at "
                f"{self._bootstrap_servers}: {exc}"
            ) from exc
        self._consumer = consumer

    def close(self) -> None:
        consumer, self._consumer = self._consumer, None
        _close_quietly(consumer)

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"KafkaConnector(bootstrap_servers={self._bootstrap_servers!r}, "
            f"security_protocol={self._security_protocol!r}, "
            f"sasl_mechanism={self._sasl_mechanism!r}, "
            f"restart_hook={'set' if self._restart_hook else 'unset'})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            creds = self._credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "Kafka read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="Kafka read user")
        password = resolve_secret(env=self._password_env,
                                  label="Kafka read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by restart_consumer.

        Raises ConnectorError when no privileged credential is configured,
        so the privileged path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            creds = self._priv_credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "Kafka privileged credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="Kafka privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                  label="Kafka privileged password")
        return user, password

    # ------------------------------------------------- driver plumbing

    def _sasl_kwargs(self) -> dict[str, Any]:
        """SASL kwargs only when the protocol actually uses SASL.

        Resolves the READ credential; privileged paths resolve their own
        pair separately and never call this.
        """
        if not self._security_protocol.upper().startswith("SASL_"):
            return {}
        user, password = self._resolve_read_credentials()
        return {
            "security_protocol": self._security_protocol,
            "sasl_mechanism": self._sasl_mechanism,
            "sasl_plain_username": user,
            "sasl_plain_password": password,
        }

    def _common_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._bootstrap_servers,
            "request_timeout_ms": 10_000,
        }
        kwargs.update(self._sasl_kwargs())
        return kwargs

    def _build_consumer(self, kafka: Any, group_id: str) -> Any:
        kwargs = self._common_kwargs()
        kwargs.update({
            "group_id": group_id,
            "enable_auto_commit": False,
        })
        return kafka.KafkaConsumer(**kwargs)

    def _build_admin(self, kafka: Any) -> Any:
        return kafka.KafkaAdminClient(**self._common_kwargs())

    def _topic_partitions(self, kafka: Any, consumer: Any,
                          topic: str) -> list[Any]:
        """TopicPartition list for a topic; ConnectorError if unknown."""
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise ConnectorError(f"{self.name}: unknown topic {topic!r}")
        return [kafka.TopicPartition(topic, p) for p in sorted(partitions)]

    def _wrap(self, op: str, exc: Exception) -> ConnectorError:
        """Translate a driver exception into ConnectorError (no secrets)."""
        return ConnectorError(f"{self.name}: {op} failed: {exc}")

    # ------------------------------------------------- reads

    def get_kafka_consumer_lag(self, topic: str, group: str) -> dict:
        """Sum end-offset minus committed-offset over a topic's partitions."""
        detail = self.get_kafka_consumer_group_detail(topic, group)
        return {
            "topic": topic,
            "group": group,
            "lag": detail["total_lag"],
            "ts": detail["ts"],
        }

    def get_kafka_consumer_group_detail(self, topic: str, group: str) -> dict:
        """Per-partition lag detail for a topic/group, plus the total.

        ``committed`` is None-converted to 0 when the group has never
        committed that partition; lag is clamped at 0 (committed can exceed
        the end offset after retention).
        """
        kafka = _kafka()
        consumer = self._build_consumer(kafka, group_id=group)
        try:
            tps = self._topic_partitions(kafka, consumer, topic)
            end_offsets = consumer.end_offsets(tps)
            partitions = []
            total_lag = 0
            for tp in tps:
                end = end_offsets.get(tp) or 0
                committed = consumer.committed(tp) or 0
                lag = max(0, int(end) - int(committed))
                total_lag += lag
                partitions.append({
                    "partition": int(tp.partition),
                    "end_offset": int(end),
                    "committed": int(committed),
                    "lag": lag,
                })
        except ConnectorError:
            raise
        except Exception as exc:
            raise self._wrap(
                f"get_kafka_consumer_group_detail({topic!r}, {group!r})",
                exc,
            ) from exc
        finally:
            _close_quietly(consumer)
        return {
            "topic": topic,
            "group": group,
            "partitions": partitions,
            "total_lag": total_lag,
            "ts": _now(),
        }

    def get_kafka_topic_detail(self, topic: str) -> dict:
        """Per-partition detail for a topic: leader, replicas, ISR, end offset.

        Leader/replica/ISR come from the driver's public ``describe_topics``
        (best-effort); when the driver does not expose it they are None/[]
        rather than failing the whole read.
        """
        kafka = _kafka()
        consumer = self._build_consumer(kafka, group_id="rca-assistant-probe")
        try:
            tps = self._topic_partitions(kafka, consumer, topic)
            end_offsets = consumer.end_offsets(tps)
        except ConnectorError:
            _close_quietly(consumer)
            raise
        except Exception as exc:
            _close_quietly(consumer)
            raise self._wrap(f"get_kafka_topic_detail({topic!r})",
                             exc) from exc
        admin = self._build_admin(kafka)
        try:
            meta = self._describe_topic_partitions(admin, topic)
        except Exception:
            # Metadata enrichment is best-effort; end offsets already read.
            meta = {}
        finally:
            _close_quietly(admin)
            _close_quietly(consumer)
        partitions = []
        for tp in tps:
            info = meta.get(int(tp.partition), {})
            partitions.append({
                "partition": int(tp.partition),
                "leader": info.get("leader"),
                "replicas": list(info.get("replicas", [])),
                "isr": list(info.get("isr", [])),
                "end_offset": int(end_offsets.get(tp) or 0),
            })
        return {"topic": topic, "partitions": partitions, "ts": _now()}

    def _describe_topic_partitions(self, admin: Any, topic: str) -> dict:
        """Best-effort {partition: {leader, replicas, isr}} via describe_topics.

        Returns {} when the driver does not expose a public describe_topics
        (callers degrade to leader=None / empty replica lists). Parses the
        future results defensively: entries may be attribute objects or
        dicts.
        """
        describe = getattr(admin, "describe_topics", None)
        if not callable(describe):
            return {}
        futures = describe([topic])
        future = futures.get(topic) if isinstance(futures, dict) else None
        if future is None:
            return {}
        result = future.result()
        entries = result if isinstance(result, (list, tuple)) else [result]
        meta: dict[int, dict] = {}
        for entry in entries:
            if isinstance(entry, dict):
                pid = entry.get("partition")
                leader = entry.get("leader")
                replicas = entry.get("replicas", [])
                isr = entry.get("isr", [])
            else:
                pid = getattr(entry, "partition", None)
                leader = getattr(entry, "leader", None)
                replicas = getattr(entry, "replicas", []) or []
                isr = getattr(entry, "isr", []) or []
            if pid is None:
                continue
            meta[int(pid)] = {
                "leader": leader,
                "replicas": list(replicas),
                "isr": list(isr),
            }
        return meta

    def get_kafka_broker_config(self, broker_id: int) -> dict:
        """Broker config via AdminClient.describe_configs (public API).

        Handles both driver layouts: kafka-python 3.x returns a resolved
        nested dict ``{resource_type: {resource_name: {key: {...}}}}``;
        older builds return ``{ConfigResource: Future}``. Config values
        are operational (not secrets), but credentials are still never
        logged. Sensitive entries surface as None, matching the driver's
        own redaction.
        """
        kafka = _kafka()
        resource = self._broker_config_resource(kafka, broker_id)
        admin = self._build_admin(kafka)
        try:
            # config_filter="all": the driver's default ("modified")
            # returns only non-default keys — an RCA read needs the full
            # effective configuration.
            raw = admin.describe_configs([resource], config_filter="all")
            entries = self._config_entries(raw, broker_id)
        except ConnectorError:
            raise
        except Exception as exc:
            raise self._wrap(f"get_kafka_broker_config({broker_id!r})",
                             exc) from exc
        finally:
            _close_quietly(admin)
        configs: dict[str, Any] = {}
        for name, data in entries.items():
            if isinstance(data, dict):
                sensitive = bool(data.get("sensitive", False))
                value = data.get("value")
            else:
                sensitive = bool(getattr(data, "sensitive", False))
                value = getattr(data, "value", None)
            if name is not None:
                configs[str(name)] = None if sensitive else value
        return {
            "broker_id": int(broker_id),
            "configs": configs,
            "ts": _now(),
        }

    def _config_entries(self, raw: Any, broker_id: int) -> dict:
        """Normalize describe_configs() output to {config_name: entry}.

        kafka-python 3.x resolves to a nested dict; older builds return
        one future per resource. Raises ConnectorError when the broker
        has no entries (unknown broker id).
        """
        entries: dict[Any, Any] = {}
        if isinstance(raw, dict):
            # kafka-python 3.x: {resource_type: {resource_name: {key: data}}}
            for _rtype, by_name in raw.items():
                if not isinstance(by_name, dict):
                    continue
                for _rname, keyed in by_name.items():
                    if isinstance(keyed, dict):
                        entries.update(keyed)
            # Legacy: {ConfigResource: Future}
            if not entries:
                for _resource, future in raw.items():
                    result = future.result() \
                        if hasattr(future, "result") else future
                    for entry in result or []:
                        if isinstance(entry, dict):
                            entries[entry.get("name")] = entry
                        else:
                            entries[getattr(entry, "name", None)] = entry
        entries = {k: v for k, v in entries.items() if k is not None}
        if not entries:
            raise ConnectorError(
                f"{self.name}: describe_configs returned no config "
                f"entries for broker {broker_id!r}"
            )
        return entries

    def _broker_config_resource(self, kafka: Any, broker_id: int) -> Any:
        """Build the driver's ConfigResource for a broker id.

        kafka-python 3.x exposes ConfigResource/ConfigResourceType under
        ``kafka.admin`` (``from kafka.admin import ConfigResource``);
        older top-level layouts are probed as fallbacks. Raises
        ConnectorError with upgrade guidance when neither is available.
        """
        admin_pkg = getattr(kafka, "admin", None)
        config_resource = getattr(admin_pkg, "ConfigResource", None)
        resource_type = getattr(admin_pkg, "ConfigResourceType", None)
        if config_resource is None or resource_type is None:
            config_resource = getattr(kafka, "ConfigResource", None)
            resource_type = getattr(kafka, "ConfigResourceType", None)
        if config_resource is None or resource_type is None:
            raise ConnectorError(
                f"{self.name}: this kafka-python build does not expose "
                "ConfigResource; broker config reads need kafka-python "
                ">= 1.4 (pip install --upgrade kafka-python)"
            )
        broker = getattr(resource_type, "BROKER", None)
        if broker is None:
            raise ConnectorError(
                f"{self.name}: this kafka-python build has no "
                "ConfigResourceType.BROKER"
            )
        return config_resource(broker, str(broker_id))

    def get_kafka_broker_health(self) -> dict:
        """Cluster reachability + per-broker health.

        kafka-python's public API exposes no cluster/broker listing, so
        per-broker detail degrades honestly: the read proves reachability
        with a timed metadata request and returns ``degraded: True`` with
        the reason. If a future driver exposes a public cluster/broker
        API, populate ``brokers``/``controller`` and set degraded False.
        """
        kafka = _kafka()
        consumer = self._build_consumer(kafka, group_id="rca-assistant-probe")
        started = time.perf_counter()
        try:
            consumer.topics()
        except Exception as exc:
            _close_quietly(consumer)
            raise ConnectorError(
                f"{self.name}: cannot reach Kafka at "
                f"{self._bootstrap_servers}: {exc}"
            ) from exc
        finally:
            _close_quietly(consumer)
        latency_ms = round((time.perf_counter() - started) * 1000.0, 2)
        return {
            "bootstrap_servers": self._bootstrap_servers,
            "reachable": True,
            "latency_ms": latency_ms,
            "brokers": [],
            "controller": None,
            "degraded": True,
            "degradation": (
                "kafka-python exposes no public cluster/broker listing API; "
                "per-broker detail unavailable without private driver "
                "internals"
            ),
            "ts": _now(),
        }

    # ------------------------------------------------- privileged actions

    def restart_consumer(self, topic: str, group: str) -> dict:
        """PRIVILEGED: restart a consumer group via the customer's hook.

        Kafka has no remote "restart consumer group" API, so this path is
        intentionally environment-specific: it resolves the SEPARATE
        privileged credential pair (refusing when absent -- the read
        credential is never reused here) and delegates to the
        ``restart_hook`` callable as ``hook(topic, group, user, password)``
        so the hook can authenticate to the customer's own consumer
        supervisor (systemd unit bounce, Kubernetes rollout restart, etc.).
        Without a hook it raises ConnectorError rather than pretending to
        restart something.
        """
        user, password = self._resolve_privileged_credentials()
        if self._restart_hook is None:
            raise ConnectorError(
                f"{self.name}: restart_consumer needs a restart_hook "
                "callable wired to the customer's consumer supervisor; "
                "none is configured"
            )
        try:
            detail = self._restart_hook(topic, group, user, password)
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: restart hook failed for "
                f"({topic!r}, {group!r}): {exc}"
            ) from exc
        result = {"topic": topic, "group": group, "ts": _now()}
        if isinstance(detail, dict):
            result.update({k: v for k, v in detail.items()
                           if k not in ("topic", "group", "user", "password")})
        return result


def _close_quietly(client: Any) -> None:
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


register_connector(KafkaConnector.SPEC, KafkaConnector)
