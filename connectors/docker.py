"""Real Docker connector: container state, stats, and logs over the Engine API.

TRANSPORT
---------
Docker Engine API over the Unix socket (``/var/run/docker.sock``), plain
stdlib only: ``socket.AF_UNIX`` plus an ``http.client.HTTPConnection``
subclass (``_UnixSocketHTTPConnection``) that overrides ``connect()``.
The guarded boundary is the internal ``_UnixSocketTransport``
(``get_json`` / ``get_bytes`` / ``post``): it wraps every socket, HTTP,
and JSON failure into ``ConnectorError``, so an unreachable daemon never
surfaces a raw exception. Tests replace ``self._transport`` with a fake
object exposing the same three methods (see tests/test_docker.py).

No third-party driver is used or needed. The socket must be mounted and
visible to the connector process: this connector assumes it runs
co-located with the Docker daemon (same host or a shared socket mount).
Remote daemons over TCP/TLS (``DOCKER_HOST``) are out of scope.

TOOL SURFACE (exact names/args/returns; the coordinator adds these to
mcp_server/tools.py -- this module must not be edited to match them,
they are the contract):
- get_docker_containers() -> dict: containers (list of {id (12-char),
  name, image, state, status}), ts. GET /containers/json.
- get_docker_stats() -> dict: stats (list of {name, cpu_pct,
  mem_used_bytes, mem_limit_bytes, mem_pct}), ts. One-shot stats per
  container; cpu_pct is computed from the precpu/cpu deltas like the
  Docker CLI does ((cpu_delta / system_delta) * CPUs * 100), 0.0 when
  the system delta is zero; mem_pct is None when the memory limit is
  unknown or zero.
- read_docker_logs(container, limit=50) -> list of {ts, message}.
  GET /containers/{id}/logs; the 8-byte multiplexed stream header is
  demuxed, falling back to raw text lines when demux fails. ``container``
  is a container name or id prefix.
- restart_docker_container(container) -> PRIVILEGED dict: container,
  previous_state, state ("running"), ts. POST /containers/{id}/restart.

CREDENTIALS
-----------
There are none: the Unix socket's file permissions ARE the authorization
(typically membership in the ``docker`` group). The constructor takes
only ``socket_path`` and ``timeout`` -- never a secret -- and the spec
declares no credential refs. ``restart_docker_container`` acts with
whatever permissions the socket grants, so it is exposed only through
the privileged ``act()`` path and stays approval-gated upstream.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_docker.py): canned Engine API
JSON and multiplexed log bytes, no live daemon touched. Live validation
against a real Docker daemon happens in a customer pilot.
"""

from __future__ import annotations

import http.client
import json
import os
import re
import socket
from datetime import datetime, timezone
from typing import Any

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
)

# "2026-09-21T15:22:41.123456789Z message" -- Docker daemon log prefix.
_LOG_LINE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s?(.*)$"
)

_MAX_LOG_LINES = 1000


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _DemuxError(ValueError):
    """Raised when the Docker multiplexed log stream does not parse."""


class _UnixSocketHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection that dials a Unix socket instead of TCP.

    The host/port passed to the superclass are dummies used only to build
    the request line; the actual connection goes over AF_UNIX.
    """

    def __init__(self, socket_path: str, timeout: float = 10) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect(self._socket_path)
            self.sock = sock
        except Exception as exc:
            raise ConnectorError(
                f"docker: cannot connect to unix socket "
                f"{self._socket_path!r}: {exc}"
            ) from exc


class _UnixSocketTransport:
    """Guarded Engine API boundary over the Docker Unix socket.

    Stdlib only. Every failure (missing socket, refused connection,
    timeout, HTTP error status, bad JSON) is wrapped in ConnectorError
    here, so connector code above this layer never sees a raw
    socket/http exception. Holds no credentials: the socket's file
    permissions are the authorization.
    """

    def __init__(self, socket_path: str, timeout: float = 10) -> None:
        self._socket_path = socket_path
        self._timeout = timeout

    def _roundtrip(self, method: str, path: str,
                   body: bytes | None = None) -> tuple[int, bytes]:
        conn = _UnixSocketHTTPConnection(
            self._socket_path, timeout=self._timeout)
        try:
            headers: dict[str, str] = {}
            if body is not None:
                headers["Content-Type"] = "application/json"
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            payload = resp.read()
            status = resp.status
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"docker: {method} {path} failed: {exc}"
            ) from exc
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return status, payload

    def _check_status(self, method: str, path: str,
                      status: int, payload: bytes) -> None:
        if status >= 400:
            detail = payload[:200].decode("utf-8", errors="replace")
            raise ConnectorError(
                f"docker: {method} {path} returned HTTP {status}: {detail}"
            )

    def get_json(self, path: str) -> Any:
        status, payload = self._roundtrip("GET", path)
        self._check_status("GET", path, status, payload)
        try:
            return json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"docker: GET {path} returned non-JSON data: {exc}"
            ) from exc

    def get_bytes(self, path: str) -> bytes:
        status, payload = self._roundtrip("GET", path)
        self._check_status("GET", path, status, payload)
        return payload

    def post(self, path: str, body: dict | None = None) -> bytes:
        raw = (json.dumps(body).encode("utf-8")
               if body is not None else None)
        status, payload = self._roundtrip("POST", path, body=raw)
        self._check_status("POST", path, status, payload)
        return payload


def _demux_logs(payload: bytes) -> list[str]:
    """Split a Docker multiplexed log stream into text lines.

    Each frame is an 8-byte header (stream type, 3 zero bytes, big-endian
    uint32 size) followed by the payload. Raises _DemuxError on any
    malformed frame so the caller can fall back to raw text.
    """
    lines: list[str] = []
    pos = 0
    total = len(payload)
    while pos < total:
        if pos + 8 > total:
            raise _DemuxError("truncated frame header")
        header = payload[pos:pos + 8]
        stream_type = header[0]
        if header[1:4] != b"\x00\x00\x00" or stream_type not in (0, 1, 2):
            raise _DemuxError(f"bad frame header: {header!r}")
        size = int.from_bytes(header[4:8], "big")
        pos += 8
        if pos + size > total:
            raise _DemuxError("truncated frame payload")
        chunk = payload[pos:pos + size]
        pos += size
        lines.extend(chunk.decode("utf-8", errors="replace").splitlines())
    return lines


class DockerConnector(Connector):
    """Live Docker diagnostics via the Engine API over the Unix socket."""

    name = "docker"

    SPEC = ConnectorSpec(
        name="docker",
        display_name="Docker Engine (Unix socket API)",
        description="Container listing, one-shot CPU/memory stats, and "
        "container log tail via the Docker Engine API over the Unix "
        "socket (stdlib only, no driver); approval-gated container "
        "restart via the privileged act() path.",
        required_config=(),
        optional_config=("socket_path", "timeout"),
        credential_refs=(),
        notes="No credentials: the socket's file permissions are the "
        "authorization. Requires the daemon socket to be mounted/visible "
        "to the connector process (co-located host). restart acts with "
        "the socket's permissions and is approval-gated upstream. "
        "Validated against a fake transport only; live validation "
        "happens in a customer pilot.",
    )

    def __init__(self, socket_path: str = "/var/run/docker.sock",
                 *, timeout: float = 10) -> None:
        self._socket_path = socket_path
        self._timeout = timeout
        self._transport: Any = _UnixSocketTransport(
            socket_path, timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_docker_containers",
            "get_docker_stats",
            "read_docker_logs",
            "restart_docker_container",
        }

    def connect(self) -> None:
        """Probe the daemon: socket file exists and GET /version parses."""
        if not os.path.exists(self._socket_path):
            raise ConnectorError(
                f"docker: socket {self._socket_path!r} does not exist; "
                "mount the Docker Engine socket into this host to use "
                "the docker connector"
            )
        try:
            info = self._transport.get_json("/version")
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"docker: connect failed: {exc}") from exc
        if not isinstance(info, dict) or "Version" not in info:
            raise ConnectorError(
                "docker: GET /version returned an unexpected response "
                "shape"
            )
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (one connection per request); safe any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only (this connector holds no secrets at all).
        return (
            f"DockerConnector(socket_path={self._socket_path!r}, "
            f"timeout={self._timeout!r})"
        )

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    @staticmethod
    def _normalize_container(entry: dict) -> dict:
        full_id = str(entry.get("Id", ""))
        names = entry.get("Names") or []
        name = str(names[0]).lstrip("/") if names else full_id[:12]
        return {
            "id": full_id[:12],
            "name": name,
            "image": str(entry.get("Image", "")),
            "state": str(entry.get("State", "")),
            "status": str(entry.get("Status", "")),
        }

    def _find_container(self, container: str) -> dict:
        """Resolve a name or id prefix to a normalized container entry.

        Lists with all=true so stopped containers (a valid restart
        target) resolve too. Raises ConnectorError when nothing matches.
        """
        if not container:
            raise ConnectorError(
                f"{self.name}: container reference must not be empty"
            )
        entries = self._transport.get_json("/containers/json?all=true")
        if not isinstance(entries, list):
            raise ConnectorError(
                f"{self.name}: /containers/json returned an unexpected "
                "shape"
            )
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            full_id = str(entry.get("Id", ""))
            norm = self._normalize_container(entry)
            if (container == full_id
                    or (container and full_id.startswith(container))
                    or container == norm["name"]
                    or container == "/" + norm["name"]):
                return norm
        raise ConnectorError(
            f"{self.name}: no container matching {container!r}"
        )

    # ------------------------------------------------- reads

    def get_docker_containers(self) -> dict:
        """List containers: {containers: [{id, name, image, state,
        status}], ts}.

        ``id`` is the 12-char short id; ``name`` has the leading "/"
        stripped. GET /containers/json (running containers).
        """
        self._require_connected()
        entries = self._transport.get_json("/containers/json")
        if not isinstance(entries, list):
            raise ConnectorError(
                f"{self.name}: /containers/json returned an unexpected "
                "shape"
            )
        containers = [self._normalize_container(e) for e in entries
                      if isinstance(e, dict)]
        containers.sort(key=lambda c: c["name"])
        return {"containers": containers, "ts": _now()}

    @staticmethod
    def _cpu_pct(stats: dict) -> float:
        """Docker CLI CPU formula from precpu/cpu deltas.

        cpu_delta / system_delta * CPUs * 100; 0.0 when the system delta
        is zero or the fields are missing (e.g. first sample).
        """
        cpu = stats.get("cpu_stats") or {}
        precpu = stats.get("precpu_stats") or {}
        cur_usage = (cpu.get("cpu_usage") or {}).get("total_usage") or 0
        prev_usage = (precpu.get("cpu_usage") or {}).get("total_usage") or 0
        cur_system = cpu.get("system_cpu_usage") or 0
        prev_system = precpu.get("system_cpu_usage") or 0
        cpu_delta = cur_usage - prev_usage
        system_delta = cur_system - prev_system
        if system_delta <= 0 or cpu_delta <= 0:
            return 0.0
        percpu = (cpu.get("cpu_usage") or {}).get("percpu_usage") or []
        online = cpu.get("online_cpus") or 0
        num_cpus = len(percpu) or online or 1
        return round(cpu_delta / system_delta * num_cpus * 100.0, 1)

    def get_docker_stats(self) -> dict:
        """One-shot CPU/memory stats per container: {stats: [{name,
        cpu_pct, mem_used_bytes, mem_limit_bytes, mem_pct}], ts}.

        ``mem_pct`` is None when the daemon reports no memory limit.
        """
        self._require_connected()
        containers = self.get_docker_containers()["containers"]
        stats: list[dict] = []
        for container in containers:
            payload = self._transport.get_json(
                f"/containers/{container['id']}/stats"
                "?stream=false&one-shot=true"
            )
            if not isinstance(payload, dict):
                raise ConnectorError(
                    f"{self.name}: stats for {container['name']!r} "
                    "returned an unexpected shape"
                )
            mem = payload.get("memory_stats") or {}
            mem_used = int(mem.get("usage") or 0)
            mem_limit = int(mem.get("limit") or 0)
            stats.append({
                "name": container["name"],
                "cpu_pct": self._cpu_pct(payload),
                "mem_used_bytes": mem_used,
                "mem_limit_bytes": mem_limit,
                "mem_pct": (round(mem_used / mem_limit * 100, 1)
                            if mem_limit > 0 else None),
            })
        stats.sort(key=lambda s: s["name"])
        return {"stats": stats, "ts": _now()}

    def read_docker_logs(self, container: str,
                         limit: int = 50) -> list[dict]:
        """Tail a container's logs: list of {ts, message}, newest last.

        Demuxes the Engine API's 8-byte framed stream; falls back to raw
        text lines when demux fails. Unparseable lines are kept with
        ts None rather than dropped.
        """
        self._require_connected()
        target = self._find_container(container)
        tail = max(1, min(int(limit), _MAX_LOG_LINES))
        payload = self._transport.get_bytes(
            f"/containers/{target['id']}/logs"
            f"?stdout=true&stderr=true&timestamps=true&tail={tail}"
        )
        try:
            lines = _demux_logs(payload)
        except _DemuxError:
            lines = payload.decode("utf-8", errors="replace").splitlines()
        lines = lines[-tail:]
        entries: list[dict] = []
        for line in lines:
            m = _LOG_LINE.match(line)
            if m:
                entries.append({"ts": m.group(1), "message": m.group(2)})
            else:
                entries.append({"ts": None, "message": line})
        return entries

    # ------------------------------------------------- privileged actions

    def restart_docker_container(self, container: str) -> dict:
        """PRIVILEGED: restart a container via POST /containers/{id}/restart.

        Resolves the container (name or id prefix) BEFORE acting so the
        returned previous_state is accurate and unknown references fail
        without touching the daemon. Acts with the socket's permissions;
        approval-gated upstream.
        """
        self._require_connected()
        target = self._find_container(container)
        previous_state = target["state"]
        self._transport.post(
            f"/containers/{target['id']}/restart?t=10")
        return {
            "container": target["name"],
            "previous_state": previous_state,
            "state": "running",
            "ts": _now(),
        }


register_connector(DockerConnector.SPEC, DockerConnector)
