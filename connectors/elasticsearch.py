"""Real Elasticsearch connector: cluster/node/index diagnostics over REST.

TRANSPORT
---------
The Elasticsearch REST API over stdlib ``urllib`` -- no third-party
client is needed. The guarded boundary is the internal
``_UrllibTransport`` (``get_json`` / ``get_json_list``): it wraps every
urllib/socket failure into ``ConnectorError``, so an unreachable node
never surfaces a raw exception. Tests replace ``self._transport`` with a
fake object exposing the same two methods (see
tests/test_elasticsearch.py).

- ``GET {scheme}://{host}:{port}/`` for cluster identity (connect probe).
- ``GET /_cluster/health`` for shard/node health.
- ``GET /_nodes/stats`` for per-node JVM heap, OS CPU, and filesystem
  usage.
- ``GET /_cat/indices?format=json`` for per-index docs and store size.

Authentication is OPTIONAL Basic auth: a credential-provider callable or
env-var names (``ELASTICSEARCH_USER`` / ``ELASTICSEARCH_PASSWORD``).
The ``Authorization`` header is built per request in a local and
discarded -- it is only sent when a credential pair actually resolves
(many clusters, especially dev ones, run with security disabled).

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_elasticsearch_cluster_health() -> dict: cluster_name, status,
  number_of_nodes, active_shards, unassigned_shards, relocating_shards,
  ts. Missing fields degrade to None rather than raising.
- get_elasticsearch_nodes() -> dict: nodes (list of {name,
  heap_used_pct, cpu_pct, disk_used_pct} sorted by name), ts.
  ``heap_used_pct`` comes from jvm.mem.heap_used_percent, ``cpu_pct``
  from os.cpu.percent, ``disk_used_pct`` from
  fs.total.available_in_bytes vs total_in_bytes; each is None when its
  source field is missing.
- get_elasticsearch_indices() -> dict: indices (list of {name, health,
  docs_count, store_size_bytes} sorted by name), ts. ``docs_count`` is
  an int (None when unparseable); ``store_size_bytes`` is parsed from
  the ``store.size`` cat field ("1.2gb", "512mb", ...) via a small
  size parser handling b/kb/mb/gb/tb -- unparseable values become None.

NOTE (2026-09-21): the three tool names above are declared here as the
contract but are not yet registered in ``mcp_server.tools.TOOL_NAMES``
(coordinator wiring, out of this module's scope per the Phase-4
division of labor). The harness ``capabilities`` check passes once the
coordinator adds the matching tool defs.

CREDENTIALS
-----------
Same rules as the other connectors: the constructor takes a
credential-provider callable or env-var NAMES (``ELASTICSEARCH_USER`` /
``ELASTICSEARCH_PASSWORD``), never a raw secret. Secrets resolve per
request and are never stored on the instance (Basic auth headers are
built in a local and discarded), never appear in ``repr``, exceptions,
or logs. Because auth is optional, no credentials at all is a supported
configuration -- the header is simply omitted.

READS ONLY (deliberate)
-----------------------
This connector exposes no privileged tools. Cluster-mutating operations
(shard rerouting, index closes/deletes, snapshot restores) are operator
territory: they change cluster state in ways that need a human with
full cluster context, so they stay out of the RCA tool surface by
design. There is intentionally no ``act()`` surface to refuse.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_elasticsearch.py): canned REST
JSON responses, no live Elasticsearch touched. Live validation against a
real cluster happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

import base64
import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# "1.2gb", "512mb", "1.5kb", "0b", "100" -- unit optional, case-insensitive
_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)\s*$")
_SIZE_MULTIPLIERS = {
    "b": 1,
    "k": 1024, "kb": 1024,
    "m": 1024 ** 2, "mb": 1024 ** 2,
    "g": 1024 ** 3, "gb": 1024 ** 3,
    "t": 1024 ** 4, "tb": 1024 ** 4,
}


def _parse_size(value: Any) -> int | None:
    """Parse an Elasticsearch ``store.size`` string into bytes.

    Handles b/kb/mb/gb/tb (case-insensitive, short forms k/m/g/t too);
    a bare number means bytes. Returns None for None, empty, or
    unparseable values instead of raising.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    match = _SIZE_RE.match(str(value))
    if not match:
        return None
    unit = match.group(2).lower()
    multiplier = _SIZE_MULTIPLIERS.get(unit) if unit else 1
    if multiplier is None:
        return None
    return int(round(float(match.group(1)) * multiplier))


def _as_int(value: Any) -> int | None:
    """Int fields degrade to None when the source value is not an int."""
    if isinstance(value, bool):
        return None
    return int(value) if isinstance(value, int) else None


def _as_number(value: Any) -> float | int | None:
    """Numeric (int/float) fields degrade to None when missing."""
    if isinstance(value, bool):
        return None
    return value if isinstance(value, (int, float)) else None


def _nested(mapping: Any, *keys: str) -> Any:
    """Walk nested dicts; return None when any level is missing/not a dict."""
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


class _UrllibTransport:
    """Guarded HTTP boundary for the Elasticsearch connector.

    Plain stdlib urllib. Every failure (DNS, refused, timeout, HTTP error,
    bad JSON) is wrapped in ConnectorError here, so connector code above
    this layer never sees a raw urllib/socket exception. Holds no
    credentials: auth is passed per call as a (user, password) tuple (or
    None when no credentials resolved) and sent as a Basic header built
    in a local.
    """

    def __init__(self, timeout: int = 10) -> None:
        self._timeout = timeout

    @staticmethod
    def _request(url: str,
                 auth: tuple[str, str] | None) -> urllib.request.Request:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        if auth is not None:
            user, password = auth
            token = base64.b64encode(
                f"{user}:{password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        return req

    def _get(self, url: str,
             auth: tuple[str, str] | None) -> bytes:
        req = self._request(url, auth)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read()
        except Exception as exc:
            # Never include auth material; the URL carries no secrets
            # (credentials travel in the Authorization header).
            raise ConnectorError(
                f"elasticsearch: GET {url} failed: {exc}"
            ) from exc

    def _parse(self, url: str, raw: bytes) -> Any:
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"elasticsearch: GET {url} returned non-JSON data: {exc}"
            ) from exc

    def get_json(self, url: str,
                 auth: tuple[str, str] | None) -> dict:
        parsed = self._parse(url, self._get(url, auth))
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"elasticsearch: GET {url} returned unexpected JSON shape"
            )
        return parsed

    def get_json_list(self, url: str,
                      auth: tuple[str, str] | None) -> list:
        parsed = self._parse(url, self._get(url, auth))
        if not isinstance(parsed, list):
            raise ConnectorError(
                f"elasticsearch: GET {url} returned unexpected JSON shape"
            )
        return parsed


class ElasticsearchConnector(Connector):
    """Live Elasticsearch diagnostics via the REST API (reads only)."""

    name = "elasticsearch"

    SPEC = ConnectorSpec(
        name="elasticsearch",
        display_name="Elasticsearch (REST API, read-only)",
        description="Cluster health, per-node JVM/CPU/disk stats, and "
        "per-index docs/store sizes via the Elasticsearch REST API over "
        "stdlib urllib. Optional Basic auth; no privileged tools -- "
        "cluster-mutating operations (shard rerouting, index "
        "close/delete) are operator territory and stay out by design.",
        required_config=("host",),
        optional_config=(
            "port", "scheme", "timeout", "user_env", "password_env",
        ),
        credential_refs=("ELASTICSEARCH_USER", "ELASTICSEARCH_PASSWORD"),
        notes="Reads only by design: there are no act() tools, so the "
        "privileged path is absent rather than refused. Validated "
        "against a fake transport only; live validation happens in a "
        "customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = 9200,
        *,
        scheme: str = "http",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str | None = None,
        password_env: str | None = None,
        timeout: int = 10,
    ) -> None:
        if scheme not in ("http", "https"):
            raise ConnectorError(
                f"elasticsearch: unknown scheme {scheme!r}; "
                "expected 'http' or 'https'"
            )
        self._host = host
        self._port = int(port)
        self._scheme = scheme
        self._base_url = f"{scheme}://{host}:{int(port)}"
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._transport: Any = _UrllibTransport(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_elasticsearch_cluster_health",
            "get_elasticsearch_nodes",
            "get_elasticsearch_indices",
        }

    def connect(self) -> None:
        """Probe GET / with the (optional) credential; fail closed on error."""
        auth = self._resolve_credentials()
        try:
            info = self._transport.get_json(f"{self._base_url}/", auth)
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(
                f"elasticsearch: connect failed: {exc}") from exc
        if not isinstance(info.get("cluster_name"), str):
            raise ConnectorError(
                "elasticsearch: GET / did not return a cluster_name; "
                "is this an Elasticsearch node?"
            )
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"ElasticsearchConnector(host={self._host!r}, "
            f"port={self._port!r}, scheme={self._scheme!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_credentials(self) -> tuple[str, str] | None:
        """Resolve the OPTIONAL Basic auth pair, or None when unresolved.

        A configured credential provider must return a non-empty
        user/password (empty is a misconfiguration and raises). Env
        vars default to ELASTICSEARCH_USER / ELASTICSEARCH_PASSWORD;
        when either side is missing there is simply no auth header.
        Secrets are never stored on the instance.
        """
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "elasticsearch: credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = os.environ.get(self._user_env or "ELASTICSEARCH_USER")
        password = os.environ.get(
            self._password_env or "ELASTICSEARCH_PASSWORD")
        if user and password:
            return user, password
        return None

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _get_json(self, path: str) -> dict:
        return self._transport.get_json(
            f"{self._base_url}{path}", self._resolve_credentials())

    def _get_json_list(self, path: str) -> list:
        return self._transport.get_json_list(
            f"{self._base_url}{path}", self._resolve_credentials())

    # ------------------------------------------------- reads

    def get_elasticsearch_cluster_health(self) -> dict:
        """Cluster health from GET /_cluster/health.

        Returns {cluster_name, status, number_of_nodes, active_shards,
        unassigned_shards, relocating_shards, ts}. Fields missing from
        the response degrade to None rather than raising.
        """
        self._require_connected()
        health = self._get_json("/_cluster/health")
        return {
            "cluster_name": health.get("cluster_name"),
            "status": health.get("status"),
            "number_of_nodes": _as_int(health.get("number_of_nodes")),
            "active_shards": _as_int(health.get("active_shards")),
            "unassigned_shards": _as_int(health.get("unassigned_shards")),
            "relocating_shards": _as_int(health.get("relocating_shards")),
            "ts": _now(),
        }

    def get_elasticsearch_nodes(self) -> dict:
        """Per-node stats from GET /_nodes/stats.

        Returns {nodes: [{name, heap_used_pct, cpu_pct, disk_used_pct}]
        sorted by name, ts}. heap_used_pct comes from
        jvm.mem.heap_used_percent, cpu_pct from os.cpu.percent, and
        disk_used_pct from fs.total.available_in_bytes vs
        total_in_bytes; each is None when its source field is missing.
        """
        self._require_connected()
        stats = self._get_json("/_nodes/stats")
        nodes_payload = stats.get("nodes")
        if not isinstance(nodes_payload, dict):
            nodes_payload = {}
        nodes: list[dict] = []
        for _node_id, node in nodes_payload.items():
            fs_total = _nested(node, "fs", "total")
            total = (fs_total or {}).get("total_in_bytes")
            available = (fs_total or {}).get("available_in_bytes")
            disk_used_pct: float | None = None
            if (_as_number(total) is not None
                    and _as_number(available) is not None
                    and total > 0):
                disk_used_pct = round(
                    (total - available) / total * 100, 1)
            nodes.append({
                "name": node.get("name") if isinstance(node, dict) else None,
                "heap_used_pct": _as_number(
                    _nested(node, "jvm", "mem", "heap_used_percent")),
                "cpu_pct": _as_number(_nested(node, "os", "cpu", "percent")),
                "disk_used_pct": disk_used_pct,
            })
        nodes.sort(key=lambda n: str(n["name"]))
        return {"nodes": nodes, "ts": _now()}

    def get_elasticsearch_indices(self) -> dict:
        """Per-index docs and store size from GET /_cat/indices?format=json.

        Returns {indices: [{name, health, docs_count, store_size_bytes}]
        sorted by name, ts}. docs_count is an int (None when missing or
        unparseable); store_size_bytes is parsed from the ``store.size``
        cat field ("1.2gb", "512mb", ...) via the b/kb/mb/gb/tb size
        parser -- unparseable values become None.
        """
        self._require_connected()
        rows = self._get_json_list("/_cat/indices?format=json")
        indices: list[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            docs_raw = row.get("docs.count")
            docs_count: int | None = None
            if docs_raw is not None and str(docs_raw).strip() != "":
                try:
                    docs_count = int(str(docs_raw).strip())
                except (ValueError, TypeError):
                    docs_count = None
            indices.append({
                "name": row.get("index"),
                "health": row.get("health"),
                "docs_count": docs_count,
                "store_size_bytes": _parse_size(row.get("store.size")),
            })
        indices.sort(key=lambda i: str(i["name"]))
        return {"indices": indices, "ts": _now()}


register_connector(ElasticsearchConnector.SPEC, ElasticsearchConnector)
