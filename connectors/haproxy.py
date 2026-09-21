"""Real HAProxy connector: stats reads over HTTP + server-state acts on the runtime API.

TRANSPORT
---------
Two guarded channels, both stdlib-only:

(a) Stats reads go through the HAProxy stats HTTP CSV endpoint
    (``http://host:8404/stats;csv`` by default, ``stats_path`` configurable)
    via the internal ``_StatsTransport`` (``get_text``). It wraps every
    urllib/socket failure into ``ConnectorError``, so an unreachable
    stats listener never surfaces a raw exception. Tests replace
    ``self._stats`` with a fake object exposing the same method
    (see tests/test_haproxy.py).

(b) The privileged action goes through the HAProxy runtime API on a Unix
    domain socket (``socket_path``, default
    ``/var/run/haproxy/admin.sock``) via the guarded
    ``_unix_socket_sendrecv`` (connect with timeout, send the command,
    read the response until silence -- the runtime API keeps the socket
    open for more commands, so response end is inactivity, not EOF).
    Every socket/OS failure is wrapped in ``ConnectorError``. Tests pass
    ``runtime_executor=`` (a ``(command) -> response-text`` callable) to
    fake this channel entirely.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py -- this module must not be edited to match them,
they are the contract):
- get_haproxy_stats() -> dict: host, port,
  frontends: [{name, status, current_sessions}],
  backends: [{name, status,
              servers: [{name, status, current_sessions, check_status}]}],
  ts.
- set_haproxy_server_state(backend, server, state) -> PRIVILEGED dict:
  backend, server, previous_state, state, ts. ``state`` is one of
  "ready"/"drain"/"maint"; anything else raises ConnectorError.

CREDENTIALS
-----------
HAProxy deployments typically expose the stats endpoint without auth on
a restricted listener, so this connector defaults to NO credentials. An
optional Basic-auth user/password for the stats URL can be supplied via
a ``credential_provider`` callable (or the ``HAPROXY_STATS_USER`` /
``HAPROXY_STATS_PASSWORD`` env vars); it resolves per request via
``resolve_secret`` and is never stored on the instance, never appears in
``repr``, exceptions, or logs. The runtime-socket action needs no
credential at all: it is a LOCAL socket action and runs as the
connector's OS user (HAProxy authorizes the socket peer by file
ownership/group). It is approval-gated upstream and refuses when the
socket is unavailable.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_haproxy.py): canned stats CSV and
a fake runtime executor, no live HAProxy touched. Live validation against
a real HAProxy happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

import base64
import csv
import os
import socket
import urllib.error
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

#: Valid administrative states for ``set_haproxy_server_state``.
VALID_SERVER_STATES = ("ready", "drain", "maint")

#: The runtime API keeps the command socket open, so the response reader
#: stops on this much silence rather than on EOF.
_RUNTIME_IDLE_S = 1.0

#: Response-text markers that mean the runtime API rejected the command.
_RUNTIME_ERROR_MARKERS = (
    "unknown", "invalid", "can't", "cannot", "no such", "permission",
    "denied",
)

_REQUIRED_CSV_COLUMNS = ("pxname", "svname", "status", "scur", "check_status")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _StatsTransport:
    """Guarded HTTP boundary for the stats CSV endpoint.

    Plain stdlib urllib. Every failure (DNS, refused, timeout, HTTP
    error) is wrapped in ConnectorError here, so connector code above
    this layer never sees a raw urllib/socket exception. Holds no
    credentials: auth is passed per call as a (user, password) tuple and
    sent as a Basic header built in a local.
    """

    def __init__(self, timeout: int = 10) -> None:
        self._timeout = timeout

    @staticmethod
    def _request(url: str,
                 auth: tuple[str, str] | None) -> urllib.request.Request:
        req = urllib.request.Request(url)
        if auth is not None:
            user, password = auth
            token = base64.b64encode(
                f"{user}:{password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        return req

    def get_text(self, url: str,
                 auth: tuple[str, str] | None) -> str:
        req = self._request(url, auth)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            # Never include auth material; the URL carries no secrets
            # (credentials travel in the Authorization header).
            raise ConnectorError(
                f"haproxy: GET {url} failed: {exc}"
            ) from exc


def _unix_socket_sendrecv(command: str, socket_path: str,
                          timeout: int) -> str:
    """Guarded Unix-socket boundary for the HAProxy runtime API.

    Connects, sends one command line, and reads the response. The runtime
    API keeps the connection open for further commands, so the read loop
    stops after ``_RUNTIME_IDLE_S`` of silence rather than waiting for
    EOF. Every socket/OS failure becomes ConnectorError; raw exceptions
    never escape this layer.
    """
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    except OSError as exc:
        raise ConnectorError(
            f"haproxy: cannot open runtime socket {socket_path!r}: {exc}"
        ) from exc
    try:
        with sock:
            sock.settimeout(timeout)
            sock.connect(socket_path)
            sock.sendall((command.strip() + "\n").encode("utf-8"))
            chunks: list[bytes] = []
            try:
                chunks.append(sock.recv(65536))
            except socket.timeout as exc:
                raise ConnectorError(
                    f"haproxy: runtime socket {socket_path!r} timed out "
                    "waiting for a response"
                ) from exc
            # Response end is silence, not EOF (the API keeps the socket).
            sock.settimeout(_RUNTIME_IDLE_S)
            while True:
                try:
                    data = sock.recv(65536)
                except socket.timeout:
                    break
                if not data:
                    break
                chunks.append(data)
        return b"".join(chunks).decode("utf-8", errors="replace")
    except (FileNotFoundError, ConnectionRefusedError) as exc:
        raise ConnectorError(
            f"haproxy: runtime socket {socket_path!r} unavailable "
            f"({exc}); privileged action refused"
        ) from exc
    except ConnectorError:
        raise
    except Exception as exc:
        raise ConnectorError(
            f"haproxy: runtime socket {socket_path!r} failed: {exc}"
        ) from exc


class HAProxyConnector(Connector):
    """Live HAProxy stats reads + runtime-API server-state changes."""

    name = "haproxy"

    SPEC = ConnectorSpec(
        name="haproxy",
        display_name="HAProxy (stats CSV + runtime Unix socket)",
        description="Frontend/backend/server health and session counts "
        "from the stats HTTP CSV endpoint; approval-gated server "
        "state changes (ready/drain/maint) via the runtime API Unix "
        "socket.",
        required_config=("host",),
        optional_config=(
            "port", "stats_path", "socket_path", "runtime_executor",
            "timeout", "user_env", "password_env",
        ),
        credential_refs=("HAPROXY_STATS_USER", "HAPROXY_STATS_PASSWORD"),
        notes="Stats reads default to no credentials (Basic auth optional "
        "via credential_provider or HAPROXY_STATS_USER/PASSWORD). The "
        "privileged set_haproxy_server_state runs as the connector's OS "
        "user on the local runtime socket and refuses when the socket is "
        "unavailable. Validated against a fake transport only; live "
        "validation happens in a customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = 8404,
        *,
        stats_path: str = "stats;csv",
        socket_path: str = "/var/run/haproxy/admin.sock",
        runtime_executor: Callable[[str], str] | None = None,
        timeout: int = 10,
        credential_provider: Callable[[], tuple[str, str] | None] | None = None,
        user_env: str = "HAPROXY_STATS_USER",
        password_env: str = "HAPROXY_STATS_PASSWORD",
    ) -> None:
        self._host = host
        self._port = int(port)
        self._stats_url = (
            f"http://{host}:{int(port)}/{stats_path.strip('/')}"
        )
        self._socket_path = socket_path
        self._runtime_executor = runtime_executor
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._timeout = timeout
        self._stats: Any = _StatsTransport(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {"get_haproxy_stats", "set_haproxy_server_state"}

    def connect(self) -> None:
        """Probe the stats CSV endpoint; fail closed on any error."""
        auth = self._resolve_stats_auth()
        try:
            text = self._stats.get_text(self._stats_url, auth)
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"haproxy: connect failed: {exc}") from exc
        self._parse_stats_csv(text)  # validates the CSV shape; discarded
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP + per-call socket); safe any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only: the stats URL carries no credentials
        # (auth travels in the Authorization header, resolved per request).
        return (
            f"HAProxyConnector(host={self._host!r}, port={self._port!r}, "
            f"stats_url={self._stats_url!r}, "
            f"socket_path={self._socket_path!r}, "
            f"runtime_executor="
            f"{'set' if self._runtime_executor is not None else 'unset'})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_stats_auth(self) -> tuple[str, str] | None:
        """Optional Basic-auth pair for the stats URL; None = anonymous.

        Secrets resolve per request and are never stored on the instance.
        """
        if self._credential_provider is not None:
            creds = self._credential_provider()
            if creds is None:
                return None
            user, password = creds[0:2]
            if not user or not password:
                raise ConnectorError(
                    "haproxy: stats credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = os.environ.get(self._user_env)
        password = os.environ.get(self._password_env)
        if user and password:
            return user, password
        if user or password:
            raise ConnectorError(
                "haproxy: only one of "
                f"{self._user_env}/{self._password_env} is set; configure "
                "both or neither"
            )
        return None

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _default_runtime_executor(self, command: str) -> str:
        """Real channel: one command on the runtime Unix socket."""
        return _unix_socket_sendrecv(command, self._socket_path,
                                     self._timeout)

    # ------------------------------------------------- stats parsing

    def _parse_stats_csv(self, text: str) -> dict:
        """Parse the stats CSV into the get_haproxy_stats payload.

        Raises ConnectorError on a non-CSV body or a missing column, so a
        misconfigured endpoint fails loudly instead of returning garbage.
        """
        rows = list(csv.reader(text.splitlines()))
        if not rows or not rows[0]:
            raise ConnectorError(
                "haproxy: stats endpoint returned an empty body; expected "
                "the HAProxy stats CSV"
            )
        header = [cell.strip().lstrip("#").strip() for cell in rows[0]]
        if header[0] != "pxname":
            raise ConnectorError(
                "haproxy: stats endpoint did not return the HAProxy stats "
                "CSV (missing '# pxname' header)"
            )
        missing = [c for c in _REQUIRED_CSV_COLUMNS if c not in header]
        if missing:
            raise ConnectorError(
                "haproxy: stats CSV is missing column(s): "
                + ", ".join(missing)
            )
        idx = {name: header.index(name)
               for name in _REQUIRED_CSV_COLUMNS}
        frontends: list[dict] = []
        backends: dict[str, dict] = {}
        for row in rows[1:]:
            if not row or not any(cell.strip() for cell in row):
                continue
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            pxname = row[idx["pxname"]].strip()
            svname = row[idx["svname"]].strip()
            status = row[idx["status"]].strip()
            scur_raw = row[idx["scur"]].strip()
            try:
                scur = int(scur_raw) if scur_raw else None
            except ValueError:
                scur = None
            if svname == "FRONTEND":
                frontends.append({
                    "name": pxname,
                    "status": status,
                    "current_sessions": scur,
                })
            elif svname == "BACKEND":
                backends.setdefault(pxname, {
                    "name": pxname,
                    "status": status,
                    "servers": [],
                })
            elif pxname:
                entry = backends.setdefault(pxname, {
                    "name": pxname,
                    "status": "unknown",
                    "servers": [],
                })
                entry["servers"].append({
                    "name": svname,
                    "status": status,
                    "current_sessions": scur,
                    "check_status": row[idx["check_status"]].strip(),
                })
        for entry in backends.values():
            entry["servers"].sort(key=lambda s: s["name"])
        frontends.sort(key=lambda f: f["name"])
        ordered = [backends[name] for name in sorted(backends)]
        return {
            "host": self._host,
            "port": self._port,
            "frontends": frontends,
            "backends": ordered,
            "ts": _now(),
        }

    def _server_status(self, backend: str, server: str) -> str:
        """Current status of one server from the stats (ConnectorError if unknown)."""
        stats = self.get_haproxy_stats()
        for be in stats["backends"]:
            if be["name"] == backend:
                for srv in be["servers"]:
                    if srv["name"] == server:
                        return str(srv["status"])
                raise ConnectorError(
                    f"{self.name}: no server {server!r} in backend "
                    f"{backend!r}"
                )
        raise ConnectorError(
            f"{self.name}: no backend {backend!r} in stats"
        )

    # ------------------------------------------------- reads

    def get_haproxy_stats(self) -> dict:
        """Frontend/backend/server health and session counts from stats CSV.

        Returns dict: {host, port,
          frontends: [{name, status, current_sessions}],
          backends: [{name, status,
                      servers: [{name, status, current_sessions,
                                 check_status}]}],
          ts}.
        """
        self._require_connected()
        text = self._stats.get_text(
            self._stats_url, self._resolve_stats_auth())
        return self._parse_stats_csv(text)

    # ------------------------------------------------- privileged actions

    def set_haproxy_server_state(self, backend: str, server: str,
                                 state: str) -> dict:
        """PRIVILEGED: set a server's administrative state via the runtime API.

        Sends ``set server <backend>/<server> state <state>`` on the
        runtime Unix socket. This is a LOCAL socket action: it runs as the
        connector's OS user (HAProxy authorizes the socket peer by file
        ownership/group), needs no credential, and is approval-gated
        upstream. Refuses when the socket is unavailable.

        Returns dict: {backend, server, previous_state, state, ts}.
        """
        self._require_connected()
        if state not in VALID_SERVER_STATES:
            raise ConnectorError(
                f"{self.name}: invalid state {state!r}; expected one of "
                f"{sorted(VALID_SERVER_STATES)}"
            )
        for label, value in (("backend", backend), ("server", server)):
            if not value or not isinstance(value, str):
                raise ConnectorError(
                    f"{self.name}: {label} name must be a non-empty string"
                )
            if any(ch.isspace() for ch in value):
                raise ConnectorError(
                    f"{self.name}: {label} name must not contain "
                    f"whitespace: {value!r}"
                )
        previous_state = self._server_status(backend, server)
        command = f"set server {backend}/{server} state {state}"
        executor = (self._runtime_executor
                    if self._runtime_executor is not None
                    else self._default_runtime_executor)
        try:
            response = executor(command)
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: runtime command failed: {exc}"
            ) from exc
        lowered = response.strip().lower()
        if lowered and any(m in lowered for m in _RUNTIME_ERROR_MARKERS):
            raise ConnectorError(
                f"{self.name}: runtime command {command!r} rejected: "
                f"{response.strip()}"
            )
        return {
            "backend": backend,
            "server": server,
            "previous_state": previous_state,
            "state": state,
            "ts": _now(),
        }


register_connector(HAProxyConnector.SPEC, HAProxyConnector)
