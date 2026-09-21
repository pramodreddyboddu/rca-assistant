"""Real ActiveMQ Artemis connector: broker and queue reads over Jolokia.

TRANSPORT
---------
Transport is the Jolokia JMX-HTTP agent (``http://host:port/jolokia/``),
plain HTTP over stdlib ``urllib`` -- no third-party driver is needed. A
stock Artemis web console serves Jolokia at ``console/jolokia`` instead,
so pass ``jolokia_path="console/jolokia"`` for a default broker.xml /
console setup (needs pilot validation). The guarded boundary is the
internal ``_UrllibTransport`` (``get_json`` / ``post_json`` /
``get_text``): it wraps every urllib/socket failure into
``ConnectorError``, so an unreachable agent never surfaces a raw
exception. Tests replace ``self._transport`` with a fake object exposing
the same three methods (see tests/test_artemis.py).

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_artemis_queues() -> dict: host, port, queues (list of {name,
  message_count, delivering_count, consumer_count}), ts.
- get_artemis_broker() -> dict: host, port, version, started,
  total_message_count, ts.
- purge_artemis_queue(queue) -> PRIVILEGED dict: queue, messages_purged,
  ts. Purges via QueueControl exec ``removeAllMessages`` (returns the
  number of removed messages). Unknown queue names raise ConnectorError;
  multiple queues with the same name are refused (ambiguous).

MBean naming (verified against the Artemis management API Javadoc and the
Red Hat Jolokia invocation reference; live pilot still required):
- QueueControl:
  ``org.apache.activemq.artemis:broker="<broker>",component=addresses,
  address="<a>",subcomponent=queues,routing-type="<rt>",queue="<q>"``.
  Queue listing searches
  ``org.apache.activemq.artemis:broker=*,component=addresses,address=*,
  subcomponent=queues,routing-type=*,queue=*`` and reads attributes
  ``name``, ``messageCount`` (getMessageCount), ``deliveringCount``
  (getDeliveringCount), ``consumerCount`` (getConsumerCount).
- Broker control (ActiveMQServerControl):
  ``org.apache.activemq.artemis:broker=<name>`` with attributes
  ``version`` (getVersion), ``started`` (isStarted), ``totalMessageCount``
  (getTotalMessageCount).

CREDENTIALS
-----------
Same rules as the IBM MQ and Tomcat connectors: the constructor takes a
credential-provider callable or env-var NAMES (``ARTEMIS_READ_USER`` /
``ARTEMIS_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance (HTTP
Basic auth headers are built in a local and discarded), never appear in
``repr``, exceptions, or logs. The privileged ``purge_artemis_queue``
resolves a SEPARATE pair (``ARTEMIS_ADMIN_USER`` /
``ARTEMIS_ADMIN_PASSWORD`` or a privileged provider) at act time and
refuses when it is absent.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_artemis.py): canned Jolokia JSON,
no live Artemis touched. The MBean key layout, attribute names, and the
int return of ``removeAllMessages`` were verified against published
management API docs and a real Jolokia invocation example, but live
validation against a real broker happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

import base64
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)

_QUEUE_SEARCH = (
    "org.apache.activemq.artemis:broker=*,component=addresses,address=*,"
    "subcomponent=queues,routing-type=*,queue=*"
)
_BROKER_SEARCH = "org.apache.activemq.artemis:broker=*"

_QUEUE_ATTRS = "name,messageCount,deliveringCount,consumerCount"
_BROKER_ATTRS = "version,started,totalMessageCount"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _UrllibTransport:
    """Guarded HTTP boundary for the Artemis connector.

    Plain stdlib urllib. Every failure (DNS, refused, timeout, HTTP error,
    bad JSON) is wrapped in ConnectorError here, so connector code above
    this layer never sees a raw urllib/socket exception. Holds no
    credentials: auth is passed per call as a (user, password) tuple and
    sent as a Basic header built in a local.
    """

    def __init__(self, timeout: int = 10) -> None:
        self._timeout = timeout

    @staticmethod
    def _request(url: str,
                 auth: tuple[str, str] | None,
                 data: bytes | None = None) -> urllib.request.Request:
        req = urllib.request.Request(url, data=data)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if auth is not None:
            user, password = auth
            token = base64.b64encode(
                f"{user}:{password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        return req

    def _open(self, op: str, url: str,
              auth: tuple[str, str] | None,
              data: bytes | None = None) -> bytes:
        req = self._request(url, auth, data)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read()
        except Exception as exc:
            # Never include auth material; the URL carries no secrets
            # (credentials travel in the Authorization header).
            raise ConnectorError(
                f"artemis: {op} {url} failed: {exc}"
            ) from exc

    def get_json(self, url: str,
                 auth: tuple[str, str] | None) -> dict:
        raw = self._open("GET", url, auth)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"artemis: GET {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"artemis: GET {url} returned unexpected JSON shape"
            )
        return parsed

    def post_json(self, url: str, payload: dict,
                  auth: tuple[str, str] | None) -> dict:
        raw = self._open(
            "POST", url, auth,
            data=json.dumps(payload).encode("utf-8"),
        )
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"artemis: POST {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"artemis: POST {url} returned unexpected JSON shape"
            )
        return parsed

    def get_text(self, url: str,
                 auth: tuple[str, str] | None) -> str:
        raw = self._open("GET", url, auth)
        return raw.decode("utf-8", errors="replace")


class ArtemisConnector(Connector):
    """Live ActiveMQ Artemis diagnostics via the Jolokia JMX-HTTP agent."""

    name = "artemis"

    SPEC = ConnectorSpec(
        name="artemis",
        display_name="Apache ActiveMQ Artemis (Jolokia JMX-HTTP)",
        description="Broker health and queue depth reads via the Jolokia "
        "JMX-HTTP agent (QueueControl search + read, ActiveMQServerControl "
        "read); approval-gated queue purge via QueueControl "
        "removeAllMessages.",
        required_config=("host",),
        optional_config=(
            "port", "jolokia_path", "broker", "timeout", "user_env",
            "password_env",
        ),
        credential_refs=("ARTEMIS_READ_USER", "ARTEMIS_READ_PASSWORD",
                         "ARTEMIS_ADMIN_USER", "ARTEMIS_ADMIN_PASSWORD"),
        notes="Reads use a read-only Artemis management user. "
        "purge_artemis_queue uses a separate admin pair "
        "(ARTEMIS_ADMIN_USER/ARTEMIS_ADMIN_PASSWORD or a privileged "
        "provider) and refuses when it is absent. Validated against a "
        "fake transport only; live validation happens in a customer "
        "pilot. A stock Artemis console serves Jolokia at "
        "'console/jolokia'; set jolokia_path accordingly.",
    )

    def __init__(
        self,
        host: str,
        port: int = 8161,
        *,
        jolokia_path: str = "jolokia",
        broker: str | None = None,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "ARTEMIS_READ_USER",
        password_env: str = "ARTEMIS_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "ARTEMIS_ADMIN_USER",
        privileged_password_env: str = "ARTEMIS_ADMIN_PASSWORD",
        timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._jolokia_base = (
            f"http://{host}:{int(port)}/{jolokia_path.strip('/')}"
        )
        self._broker = broker
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._transport: Any = _UrllibTransport(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_artemis_queues",
            "get_artemis_broker",
            "purge_artemis_queue",
        }

    def connect(self) -> None:
        """Probe the Jolokia agent with the read credential; fail closed."""
        auth = self._resolve_read_credentials()
        try:
            info = self._transport.get_json(
                f"{self._jolokia_base}/version", auth)
            if not isinstance(info.get("value"), dict):
                raise ConnectorError(
                    "artemis: Jolokia agent at "
                    f"{self._jolokia_base} returned an unexpected "
                    "version response"
                )
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"artemis: connect failed: {exc}") from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"ArtemisConnector(host={self._host!r}, port={self._port!r}, "
            f"jolokia_path={self._jolokia_base!r}, broker={self._broker!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "artemis: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="Artemis read user")
        password = resolve_secret(env=self._password_env,
                                  label="Artemis read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by act() paths.

        Raises ConnectorError when no privileged credential is configured,
        so the purge path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            user, password = self._priv_credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "artemis: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="Artemis privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="Artemis privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _jolokia_search(self, pattern: str,
                        auth: tuple[str, str]) -> list[str]:
        resp = self._transport.post_json(
            self._jolokia_base,
            {"type": "search", "mbean": pattern},
            auth,
        )
        value = resp.get("value")
        if not isinstance(value, list):
            raise ConnectorError(
                f"{self.name}: Jolokia search {pattern!r} returned an "
                "unexpected shape"
            )
        return [str(v) for v in value]

    def _jolokia_read_attrs(self, mbean: str, attrs: str,
                            auth: tuple[str, str]) -> dict:
        quoted = urllib.parse.quote(mbean, safe="")
        resp = self._transport.get_json(
            f"{self._jolokia_base}/read/{quoted}/{attrs}", auth)
        value = resp.get("value")
        if not isinstance(value, dict):
            raise ConnectorError(
                f"{self.name}: Jolokia read of {mbean!r} returned an "
                "unexpected shape"
            )
        return value

    def _jolokia_exec(self, mbean: str, operation: str,
                      auth: tuple[str, str]) -> Any:
        resp = self._transport.post_json(
            self._jolokia_base,
            {"type": "exec", "mbean": mbean, "operation": operation,
             "arguments": []},
            auth,
        )
        if "error" in resp or (
                resp.get("status") is not None and resp.get("status") != 200):
            raise ConnectorError(
                f"{self.name}: Jolokia exec {operation} on {mbean!r} "
                f"failed: {resp.get('error') or resp}"
            )
        return resp.get("value")

    # ------------------------------------------------- broker / queue reads

    def _broker_mbean(self, auth: tuple[str, str]) -> str:
        """Resolve the ActiveMQServerControl ObjectName.

        A pinned ``broker=`` name is used verbatim; otherwise the first
        broker-only MBean from the Jolokia search is taken (deterministic
        by sort order). Address/queue MBeans are filtered out.
        """
        if self._broker is not None:
            return f'org.apache.activemq.artemis:broker="{self._broker}"'
        found = self._jolokia_search(_BROKER_SEARCH, auth)
        server_mbeans = sorted(
            m for m in found
            if m.startswith("org.apache.activemq.artemis:broker=")
            and "," not in m.split(":", 1)[1]
        )
        if not server_mbeans:
            raise ConnectorError(
                f"{self.name}: no ActiveMQServerControl MBean found; is "
                "Artemis running and reachable?"
            )
        return server_mbeans[0]

    def get_artemis_broker(self) -> dict:
        """Broker identity, version, started state, and total message count.

        Returns dict: host, port, version (str), started (bool),
        total_message_count (int), ts.
        """
        self._require_connected()
        auth = self._resolve_read_credentials()
        mbean = self._broker_mbean(auth)
        attrs = self._jolokia_read_attrs(mbean, _BROKER_ATTRS, auth)
        return {
            "host": self._host,
            "port": self._port,
            "version": str(attrs.get("version", "")),
            "started": bool(attrs.get("started", False)),
            "total_message_count": int(attrs.get("totalMessageCount", 0)
                                       or 0),
            "ts": _now(),
        }

    def get_artemis_queues(self) -> dict:
        """Every Artemis queue with message/delivering/consumer counts.

        Returns dict: host, port, queues (list of {name, message_count,
        delivering_count, consumer_count}), ts. ``message_count`` is the
        ready (non-delivering) backlog; ``delivering_count`` is in-flight
        to consumers.
        """
        self._require_connected()
        auth = self._resolve_read_credentials()
        mbeans = self._jolokia_search(_QUEUE_SEARCH, auth)
        queues: list[dict] = []
        for mbean in sorted(mbeans):
            attrs = self._jolokia_read_attrs(mbean, _QUEUE_ATTRS, auth)
            queues.append({
                "name": str(attrs.get("name", "")),
                "message_count": int(attrs.get("messageCount", 0) or 0),
                "delivering_count": int(attrs.get("deliveringCount", 0)
                                       or 0),
                "consumer_count": int(attrs.get("consumerCount", 0) or 0),
            })
        return {
            "host": self._host,
            "port": self._port,
            "queues": queues,
            "ts": _now(),
        }

    # ------------------------------------------------- privileged actions

    def _queue_mbean(self, queue: str,
                     auth: tuple[str, str]) -> str:
        """Find the QueueControl MBean for exactly one queue name."""
        mbeans = self._jolokia_search(_QUEUE_SEARCH, auth)
        matches: list[str] = []
        for mbean in sorted(mbeans):
            attrs = self._jolokia_read_attrs(mbean, _QUEUE_ATTRS, auth)
            if str(attrs.get("name", "")) == queue:
                matches.append(mbean)
        if not matches:
            raise ConnectorError(
                f"{self.name}: unknown queue {queue!r}"
            )
        if len(matches) > 1:
            raise ConnectorError(
                f"{self.name}: {len(matches)} queues named {queue!r} "
                "(e.g. across routing-types); refusing ambiguous purge"
            )
        return matches[0]

    def purge_artemis_queue(self, queue: str) -> dict:
        """PRIVILEGED: purge every message from a queue.

        Runs under the SEPARATE admin credential pair; refuses when it is
        not configured rather than reusing the read account. Uses
        QueueControl ``removeAllMessages``, whose int return is the number
        of removed messages. Unknown queue names raise ConnectorError.

        Returns dict: queue, messages_purged, ts.
        """
        self._require_connected()
        admin_auth = self._resolve_privileged_credentials()
        mbean = self._queue_mbean(queue, admin_auth)
        purged = self._jolokia_exec(mbean, "removeAllMessages", admin_auth)
        return {
            "queue": queue,
            "messages_purged": int(purged or 0),
            "ts": _now(),
        }


register_connector(ArtemisConnector.SPEC, ArtemisConnector)
