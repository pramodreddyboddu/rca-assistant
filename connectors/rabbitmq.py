"""Real RabbitMQ connector: broker diagnostics via the Management HTTP API.

TRANSPORT
---------
The Management HTTP API (``http://host:15672/api/``), plain HTTP over
stdlib ``urllib`` -- no third-party driver is needed. The guarded
boundary is the internal ``_UrllibTransport`` (``get_json`` / ``delete``):
it wraps every urllib/socket failure into ``ConnectorError``, so an
unreachable broker never surfaces a raw exception. Credentials travel in
a Basic Authorization header built in a local and discarded; the URL
carries no secrets. Tests replace ``self._transport`` with a fake object
exposing the same two methods (see tests/test_rabbitmq.py).

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py -- this module must not be edited to match them,
they are the contract):
- get_rabbitmq_queues(vhost="/") -> dict: host, port, vhost, queues (list
  of {name, messages, messages_ready, messages_unacknowledged, consumers,
  state}, sorted by name), ts.
- get_rabbitmq_nodes() -> dict: host, port, nodes (list of {name,
  running, mem_used_bytes, mem_limit_bytes, mem_used_pct, disk_free_bytes,
  fd_used, fd_total}), ts. mem_used_pct is None when no memory limit is
  configured (the API reports mem_limit as false/absent).
- get_rabbitmq_connections() -> dict: host, port, connections (list of
  {name, user, vhost, state, channels}), ts.
- purge_rabbitmq_queue(vhost, queue) -> PRIVILEGED dict: vhost, queue,
  messages_purged, ts. messages_purged is read from the queue detail
  BEFORE the purge. Destructive: discards every ready message in the
  queue; approval-gated upstream.

CREDENTIALS
-----------
Same rules as the other real connectors: the constructor takes a
credential-provider callable or env-var NAMES (``RABBITMQ_READ_USER`` /
``RABBITMQ_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance
(Basic auth headers are built in a local and discarded), never appear in
``repr``, exceptions, or logs. The privileged ``purge_rabbitmq_queue``
resolves a SEPARATE pair (``RABBITMQ_ADMIN_USER`` /
``RABBITMQ_ADMIN_PASSWORD`` or a privileged provider) at act time and
refuses when it is absent, rather than reusing the read account.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_rabbitmq.py): canned Management
API JSON responses, no live RabbitMQ touched. Live validation against a
real broker happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

import base64
import json
import urllib.error
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


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_int(value: Any, default: int = 0) -> int:
    """Best-effort int coercion for Management API number fields.

    The API reports some limits as ``false``/absent (e.g. ``mem_limit``);
    those fall back to ``default`` rather than blowing up.
    """
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    return default


class _UrllibTransport:
    """Guarded HTTP boundary for the RabbitMQ connector.

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
                 method: str = "GET") -> urllib.request.Request:
        req = urllib.request.Request(url, method=method)
        if auth is not None:
            user, password = auth
            token = base64.b64encode(
                f"{user}:{password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        return req

    def _open(self, op: str, url: str,
              auth: tuple[str, str] | None,
              method: str = "GET") -> bytes:
        req = self._request(url, auth, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read()
        except Exception as exc:
            # Never include auth material; the URL carries no secrets
            # (credentials travel in the Authorization header).
            raise ConnectorError(
                f"rabbitmq: {op} {url} failed: {exc}"
            ) from exc

    def get_json(self, url: str,
                 auth: tuple[str, str] | None) -> Any:
        # The Management API returns dicts for /overview and single-object
        # GETs, and lists for /queues, /nodes, /connections.
        raw = self._open("GET", url, auth)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"rabbitmq: GET {url} returned non-JSON data: {exc}"
            ) from exc

    def delete(self, url: str,
               auth: tuple[str, str] | None) -> None:
        # DELETE /api/queues/{vhost}/{queue}/contents answers
        # 204 No Content; there is nothing to parse.
        self._open("DELETE", url, auth, method="DELETE")


class RabbitMQConnector(Connector):
    """Live RabbitMQ diagnostics via the Management HTTP API."""

    name = "rabbitmq"

    SPEC = ConnectorSpec(
        name="rabbitmq",
        display_name="RabbitMQ (Management HTTP API)",
        description="Queue depths and consumer counts, node memory/disk/"
        "file-descriptor health, and connection state via the RabbitMQ "
        "Management HTTP API (port 15672); approval-gated queue purge via "
        "DELETE /api/queues/{vhost}/{queue}/contents.",
        required_config=("host",),
        optional_config=(
            "port", "timeout", "user_env", "password_env",
        ),
        credential_refs=("RABBITMQ_READ_USER", "RABBITMQ_READ_PASSWORD"),
        notes="Reads use a read-only management user. purge_rabbitmq_queue "
        "is destructive (discards every ready message) and uses a separate "
        "admin pair (RABBITMQ_ADMIN_USER/RABBITMQ_ADMIN_PASSWORD or a "
        "privileged provider); it refuses when the admin pair is absent. "
        "Validated against a fake transport only; live validation happens "
        "in a customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = 15672,
        *,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "RABBITMQ_READ_USER",
        password_env: str = "RABBITMQ_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "RABBITMQ_ADMIN_USER",
        privileged_password_env: str = "RABBITMQ_ADMIN_PASSWORD",
        timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._api_base = f"http://{host}:{int(port)}/api"
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
            "get_rabbitmq_queues",
            "get_rabbitmq_nodes",
            "get_rabbitmq_connections",
            "purge_rabbitmq_queue",
        }

    def connect(self) -> None:
        """Probe GET /api/overview with the read credential; fail closed."""
        auth = self._resolve_read_credentials()
        try:
            overview = self._transport.get_json(
                f"{self._api_base}/overview", auth)
            if (not isinstance(overview, dict)
                    or "rabbitmq_version" not in overview):
                raise ConnectorError(
                    f"rabbitmq: GET {self._api_base}/overview returned an "
                    "unexpected response (not a RabbitMQ management API)"
                )
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"rabbitmq: connect failed: {exc}") from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"RabbitMQConnector(host={self._host!r}, port={self._port!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "rabbitmq: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="RabbitMQ read user")
        password = resolve_secret(env=self._password_env,
                                  label="RabbitMQ read password")
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
                    "rabbitmq: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="RabbitMQ privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="RabbitMQ privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    # ------------------------------------------------- reads

    def get_rabbitmq_queues(self, vhost: str = "/") -> dict:
        """Queue depths and consumer counts for one vhost.

        Returns {"host", "port", "vhost", "queues", "ts"} where "queues" is
        a list of {"name", "messages", "messages_ready",
        "messages_unacknowledged", "consumers", "state"} sorted by name.
        """
        self._require_connected()
        url = (f"{self._api_base}/queues/"
               f"{urllib.parse.quote(vhost, safe='')}")
        payload = self._transport.get_json(
            url, self._resolve_read_credentials())
        if not isinstance(payload, list):
            raise ConnectorError(
                f"{self.name}: GET {url} returned an unexpected "
                "queue-list shape"
            )
        queues: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            queues.append({
                "name": str(item.get("name", "")),
                "messages": _as_int(item.get("messages")),
                "messages_ready": _as_int(item.get("messages_ready")),
                "messages_unacknowledged": _as_int(
                    item.get("messages_unacknowledged")),
                "consumers": _as_int(item.get("consumers")),
                "state": str(item.get("state") or "unknown"),
            })
        queues.sort(key=lambda q: q["name"])
        return {
            "host": self._host,
            "port": self._port,
            "vhost": vhost,
            "queues": queues,
            "ts": _now(),
        }

    def get_rabbitmq_nodes(self) -> dict:
        """Node health: memory, disk, and file-descriptor usage.

        Returns {"host", "port", "nodes", "ts"} where "nodes" is a list of
        {"name", "running", "mem_used_bytes", "mem_limit_bytes",
        "mem_used_pct", "disk_free_bytes", "fd_used", "fd_total"}.
        mem_limit_bytes/mem_used_pct are None when no memory limit is
        configured (the API reports mem_limit as false or omits it).
        """
        self._require_connected()
        url = f"{self._api_base}/nodes"
        payload = self._transport.get_json(
            url, self._resolve_read_credentials())
        if not isinstance(payload, list):
            raise ConnectorError(
                f"{self.name}: GET {url} returned an unexpected "
                "node-list shape"
            )
        nodes: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            mem_used = _as_int(item.get("mem_used"))
            raw_limit = item.get("mem_limit")
            mem_limit_bytes = (
                int(raw_limit)
                if isinstance(raw_limit, (int, float))
                and not isinstance(raw_limit, bool) else None
            )
            nodes.append({
                "name": str(item.get("name", "")),
                "running": bool(item.get("running", False)),
                "mem_used_bytes": mem_used,
                "mem_limit_bytes": mem_limit_bytes,
                "mem_used_pct": (
                    round(mem_used / mem_limit_bytes * 100, 1)
                    if mem_limit_bytes and mem_limit_bytes > 0 else None
                ),
                "disk_free_bytes": _as_int(item.get("disk_free")),
                "fd_used": _as_int(item.get("fd_used")),
                "fd_total": _as_int(item.get("fd_total")),
            })
        nodes.sort(key=lambda n: n["name"])
        return {
            "host": self._host,
            "port": self._port,
            "nodes": nodes,
            "ts": _now(),
        }

    def get_rabbitmq_connections(self) -> dict:
        """Open client connections: user, vhost, state, channel count.

        Returns {"host", "port", "connections", "ts"} where "connections"
        is a list of {"name", "user", "vhost", "state", "channels"}.
        """
        self._require_connected()
        url = f"{self._api_base}/connections"
        payload = self._transport.get_json(
            url, self._resolve_read_credentials())
        if not isinstance(payload, list):
            raise ConnectorError(
                f"{self.name}: GET {url} returned an unexpected "
                "connection-list shape"
            )
        connections: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            connections.append({
                "name": str(item.get("name", "")),
                "user": str(item.get("user", "")),
                "vhost": str(item.get("vhost", "")),
                "state": str(item.get("state") or "unknown"),
                "channels": _as_int(item.get("channels")),
            })
        return {
            "host": self._host,
            "port": self._port,
            "connections": connections,
            "ts": _now(),
        }

    # ------------------------------------------------- privileged actions

    def purge_rabbitmq_queue(self, vhost: str, queue: str) -> dict:
        """PRIVILEGED: discard every ready message in one queue.

        DESTRUCTIVE and approval-gated upstream: runs under the SEPARATE
        admin credential pair and refuses when it is absent rather than
        reusing the read account. The pre-purge message count is read from
        GET /api/queues/{vhost}/{queue} first so the caller can see what
        was discarded.

        Returns {"vhost", "queue", "messages_purged", "ts"}.
        """
        self._require_connected()
        if not vhost or not queue:
            raise ConnectorError(
                f"{self.name}: vhost and queue must both be non-empty "
                "strings"
            )
        admin_auth = self._resolve_privileged_credentials()
        queue_url = (f"{self._api_base}/queues/"
                     f"{urllib.parse.quote(vhost, safe='')}/"
                     f"{urllib.parse.quote(queue, safe='')}")
        detail = self._transport.get_json(queue_url, admin_auth)
        if not isinstance(detail, dict):
            raise ConnectorError(
                f"{self.name}: GET {queue_url} returned an unexpected "
                "queue-detail shape"
            )
        messages_purged = _as_int(detail.get("messages"))
        self._transport.delete(f"{queue_url}/contents", admin_auth)
        return {
            "vhost": vhost,
            "queue": queue,
            "messages_purged": messages_purged,
            "ts": _now(),
        }


register_connector(RabbitMQConnector.SPEC, RabbitMQConnector)
