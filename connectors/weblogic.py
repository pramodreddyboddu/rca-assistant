"""Real Oracle WebLogic Server connector: heap/thread-pool/app diagnostics over REST.

TRANSPORT
---------
The Oracle WebLogic Management REST API (``http://host:port/<rest_root>``,
default ``management/wls/latest``), plain HTTP over stdlib ``urllib`` --
no third-party driver is needed. The guarded boundary is the internal
``_UrllibTransport`` (``get_json`` / ``post_json``): it wraps every
urllib/socket failure into ``ConnectorError``, so an unreachable server
never surfaces a raw exception. POST requests carry the ``X-Requested-By``
header (WebLogic's documented CSRF requirement) and credentials travel
only in the Basic Authorization header, never in the URL. Tests replace
``self._transport`` with a fake object exposing the same two methods
(see tests/test_weblogic.py).

Resource paths used (verified against the Oracle REST API reference;
https://docs.oracle.com/en/middleware/standalone/weblogic-server/14.1.1.0/wlrsr/
and the ThreadPoolRuntimeMBean Java API reference):
- ``serverRuntime/JVMRuntime`` -- heapSizeCurrent, heapFreeCurrent,
  heapSizeMax (all read-only, verified field names).
- ``serverRuntime/threadPoolRuntime`` -- executeThreadTotalCount,
  executeThreadIdleCount, pendingUserRequestCount (verified MBean
  attributes).
- ``serverRuntime/applicationRuntimes`` (collection) and per-app
  ``serverRuntime/applicationRuntimes/{name}`` -- healthState,
  componentRuntimes openSessionsCurrentCount (verified in the Oracle
  REST usage guide examples).
- App restart: POST ``serverRuntime/applicationRuntimes/{name}/stop``
  then POST ``.../start`` (action-resource pattern used throughout the
  WLS REST API).

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_weblogic_heap() -> dict: host, port, heap_init_bytes,
  heap_used_bytes, heap_committed_bytes, heap_max_bytes, heap_used_pct
  (of max, None when max <= 0), ts. The REST API exposes only
  current/free/max, so heap_init_bytes and heap_committed_bytes are None.
- get_weblogic_threadpool() -> dict: host, port, pools (list of {name,
  current_threads_busy, current_thread_count, max_threads, busy_pct}),
  ts. busy = executeThreadTotalCount - executeThreadIdleCount. The
  self-tuning pool has no exposed max, so max_threads/busy_pct are None.
- get_weblogic_apps() -> dict: host, port, apps (list of {name, state,
  sessions}), ts. ``state`` is one of running/stopped/failed (mapped from
  the app healthState); ``sessions`` is the summed
  openSessionsCurrentCount across the app's component runtimes, None when
  unavailable.
- read_weblogic_log(log="server", limit=50) -> list of {ts, severity,
  message}. ``log`` is "server" (newest ``*.log`` server log) or
  "access" (newest ``access*.log``).
- restart_weblogic_app(app_name) -> PRIVILEGED dict: app_name,
  previous_state, state ("running"), ts.

CREDENTIALS
-----------
Same rules as the other real connectors: the constructor takes a
credential-provider callable or env-var NAMES
(``WEBLOGIC_READ_USER`` / ``WEBLOGIC_READ_PASSWORD``), never a raw
secret. Secrets resolve per request via ``resolve_secret`` and are never
stored on the instance (HTTP Basic auth headers are built in a local and
discarded), never appear in ``repr``, exceptions, or logs. The privileged
``restart_weblogic_app`` resolves a SEPARATE pair
(``WEBLOGIC_ADMIN_USER`` / ``WEBLOGIC_ADMIN_PASSWORD`` or a privileged
provider) at act time and refuses when it is absent.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_weblogic.py): canned REST JSON
responses, no live WebLogic touched. Live validation against a real
server happens in a customer pilot -- see docs/REAL_CONNECTOR_READINESS.md.

PILOT-VALIDATION NOTES (path/resource uncertainty to confirm on a live box)
--------------------------------------------------------------------------
- The standalone API root is configurable via ``rest_root``. The domain
  API root ``management/weblogic/latest`` exposes the deploymentManager
  (``domainRuntime/deploymentManager/appDeploymentRuntimes/{name}/stop``
  and ``.../start``), which is the documented stop/start path in the
  full domain REST reference; the stop/start action resources under
  ``serverRuntime/applicationRuntimes/{name}`` are the assumed equivalent
  on the standalone root. If the live server rejects those, switch the
  restart POSTs to the deploymentManager paths.
- ``get_weblogic_apps`` state comes from the app healthState mapping;
  a stopped app may simply be absent from the applicationRuntimes
  collection, in which case it never appears in the list at all.
  Confirm the desired stopped-state semantics against a live server.
- ``read_weblogic_log`` tails files on the local disk: it assumes the
  connector runs co-located with the WebLogic server (same host or
  shared log volume); remote log access needs a log shipper, which is
  out of scope for this connector.
"""

from __future__ import annotations

import base64
import glob
import json
import os
import re
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

# "####<21-Sep-2026 14:33:20.123> <Info> <Server> <AdminServer> ... <message>"
_SERVER_LINE = re.compile(r"^####<([^>]+)>\s+<(\w+)>\s*(.*)$")

# '10.0.0.2 - - [21/Sep/2026:14:33:20 -0500] "GET /orders HTTP/1.1" 200 34'
_ACCESS_LINE = re.compile(
    r'^\S+\s+-\s+-\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3})\s+(\S+).*$'
)

_MAX_LOG_LINES = 200

_HEALTH_STATE_MAP = {
    "OK": "running",
    "WARN": "running",
    "OVERLOADED": "running",
    "CRITICAL": "failed",
    "FAILED": "failed",
    "UNKNOWN": "unknown",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _UrllibTransport:
    """Guarded HTTP boundary for the WebLogic connector.

    Plain stdlib urllib. Every failure (DNS, refused, timeout, HTTP error,
    bad JSON) is wrapped in ConnectorError here, so connector code above
    this layer never sees a raw urllib/socket exception. Holds no
    credentials: auth is passed per call as a (user, password) tuple and
    sent as a Basic header built in a local. POST requests carry the
    X-Requested-By header WebLogic requires for mutating operations.
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
            req.add_header("X-Requested-By", "rca-assistant")
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
                f"weblogic: {op} {url} failed: {exc}"
            ) from exc

    def get_json(self, url: str,
                 auth: tuple[str, str] | None) -> dict:
        raw = self._open("GET", url, auth)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"weblogic: GET {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"weblogic: GET {url} returned unexpected JSON shape"
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
                f"weblogic: POST {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"weblogic: POST {url} returned unexpected JSON shape"
            )
        return parsed


class WebLogicConnector(Connector):
    """Live Oracle WebLogic Server diagnostics via its Management REST API."""

    name = "weblogic"

    SPEC = ConnectorSpec(
        name="weblogic",
        display_name="Oracle WebLogic Server (Management REST API)",
        description="JVM heap, default thread-pool, and deployed-application "
        "reads via the WebLogic Management REST API; approval-gated "
        "application restart via lifecycle stop+start; best-effort local "
        "tail of server and access logs.",
        required_config=("host",),
        optional_config=(
            "port", "rest_root", "log_dir", "timeout", "user_env",
            "password_env",
        ),
        credential_refs=("WEBLOGIC_READ_USER", "WEBLOGIC_READ_PASSWORD"),
        notes="Reads use a read-only WebLogic user. restart_weblogic_app "
        "uses a separate admin pair (WEBLOGIC_ADMIN_USER/"
        "WEBLOGIC_ADMIN_PASSWORD or a privileged provider) and refuses "
        "when it is absent. Validated against a fake transport only; "
        "live validation happens in a customer pilot. Log tailing assumes "
        "co-located log files. App stop/start REST paths are pilot-"
        "validation notes until confirmed on a live server.",
    )

    def __init__(
        self,
        host: str,
        port: int = 7001,
        *,
        rest_root: str = "management/wls/latest",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "WEBLOGIC_READ_USER",
        password_env: str = "WEBLOGIC_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "WEBLOGIC_ADMIN_USER",
        privileged_password_env: str = "WEBLOGIC_ADMIN_PASSWORD",
        log_dir: str | None = None,
        timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._rest_root = rest_root.strip("/")
        self._base = f"http://{host}:{int(port)}/{self._rest_root}"
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._log_dir = log_dir
        self._transport: Any = _UrllibTransport(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_weblogic_heap",
            "get_weblogic_threadpool",
            "get_weblogic_apps",
            "read_weblogic_log",
            "restart_weblogic_app",
        }

    def connect(self) -> None:
        """Probe the REST API with the read credential; fail closed on error."""
        auth = self._resolve_read_credentials()
        try:
            info = self._transport.get_json(
                f"{self._base}/serverRuntime/JVMRuntime", auth)
            if not isinstance(info, dict) or "heapSizeCurrent" not in info:
                raise ConnectorError(
                    f"weblogic: REST API at {self._base} did not return a "
                    "JVMRuntime payload"
                )
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"weblogic: connect failed: {exc}") from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"WebLogicConnector(host={self._host!r}, port={self._port!r}, "
            f"rest_root={self._rest_root!r}, log_dir={self._log_dir!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "weblogic: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env,
                              label="WebLogic read user")
        password = resolve_secret(env=self._password_env,
                                  label="WebLogic read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by act() paths.

        Raises ConnectorError when no privileged credential is configured,
        so the restart path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            user, password = self._priv_credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "weblogic: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="WebLogic privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                  label="WebLogic privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _rest_get(self, path: str) -> dict:
        return self._transport.get_json(
            f"{self._base}/{path.lstrip('/')}",
            self._resolve_read_credentials(),
        )

    def _rest_post(self, path: str, payload: dict,
                   auth: tuple[str, str]) -> dict:
        return self._transport.post_json(
            f"{self._base}/{path.lstrip('/')}", payload, auth,
        )

    @staticmethod
    def _state_name(app: dict) -> str:
        health = app.get("healthState") or {}
        state = health.get("state")
        if isinstance(state, str) and state:
            return _HEALTH_STATE_MAP.get(state.upper(), "unknown")
        return "unknown"

    @staticmethod
    def _session_total(app: dict) -> int | None:
        """Sum openSessionsCurrentCount across component runtimes.

        Returns None when the data is unavailable (no component runtimes
        reported with session counts).
        """
        components = app.get("componentRuntimes") or {}
        items = components.get("items") if isinstance(components, dict) else None
        if not isinstance(items, list) or not items:
            return None
        total = 0
        found = False
        for component in items:
            if not isinstance(component, dict):
                continue
            count = component.get("openSessionsCurrentCount")
            if isinstance(count, (int, float)):
                total += int(count)
                found = True
        return total if found else None

    # ------------------------------------------------- reads (REST)

    def get_weblogic_heap(self) -> dict:
        """JVM heap usage from serverRuntime/JVMRuntime.

        Returns {host, port, heap_init_bytes, heap_used_bytes,
        heap_committed_bytes, heap_max_bytes, heap_used_pct, ts}.
        heap_used_bytes = heapSizeCurrent - heapFreeCurrent. The REST API
        exposes no init/committed figures, so those are None.
        """
        self._require_connected()
        info = self._rest_get("serverRuntime/JVMRuntime")
        current = int(info.get("heapSizeCurrent", 0) or 0)
        free = int(info.get("heapFreeCurrent", 0) or 0)
        max_heap = int(info.get("heapSizeMax", -1) or -1)
        used = max(0, current - free)
        return {
            "host": self._host,
            "port": self._port,
            "heap_init_bytes": None,
            "heap_used_bytes": used,
            "heap_committed_bytes": None,
            "heap_max_bytes": max_heap,
            "heap_used_pct": (round(used / max_heap * 100, 1)
                              if max_heap > 0 else None),
            "ts": _now(),
        }

    def get_weblogic_threadpool(self) -> dict:
        """Default execute-thread-pool stats: busy threads vs total.

        Returns {host, port, pools (list of {name,
        current_threads_busy, current_thread_count, max_threads,
        busy_pct}), ts}. busy = executeThreadTotalCount -
        executeThreadIdleCount. WebLogic's self-tuning pool exposes no
        max, so max_threads and busy_pct are None.
        """
        self._require_connected()
        info = self._rest_get("serverRuntime/threadPoolRuntime")
        total = int(info.get("executeThreadTotalCount", 0) or 0)
        idle = int(info.get("executeThreadIdleCount", 0) or 0)
        busy = max(0, total - idle)
        pools = [{
            "name": str(info.get("name") or "weblogic.kernel.Default"),
            "current_threads_busy": busy,
            "current_thread_count": total,
            "max_threads": None,
            "busy_pct": None,
        }]
        return {
            "host": self._host,
            "port": self._port,
            "pools": pools,
            "ts": _now(),
        }

    def get_weblogic_apps(self) -> dict:
        """Deployed applications with state and open sessions.

        Returns {host, port, apps (list of {name, state, sessions}), ts}.
        ``state`` is mapped from the app healthState (running/stopped/
        failed; "unknown" when the server does not report one).
        ``sessions`` is the summed openSessionsCurrentCount across the
        app's component runtimes, None when unavailable.
        """
        self._require_connected()
        collection = self._rest_get("serverRuntime/applicationRuntimes")
        items = collection.get("items") or []
        if not isinstance(items, list):
            raise ConnectorError(
                f"{self.name}: applicationRuntimes returned an unexpected "
                "shape"
            )
        apps: list[dict] = []
        for entry in items:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not isinstance(name, str) or not name:
                continue
            detail = self._rest_get(
                "serverRuntime/applicationRuntimes/"
                + urllib.parse.quote(name, safe="")
            )
            apps.append({
                "name": name,
                "state": self._state_name(detail),
                "sessions": self._session_total(detail),
            })
        apps.sort(key=lambda a: a["name"])
        return {
            "host": self._host,
            "port": self._port,
            "apps": apps,
            "ts": _now(),
        }

    def read_weblogic_log(self, log: str = "server", limit: int = 50) -> list[dict]:
        """Tail the server log or the newest HTTP access log (co-located files).

        Returns a list of {ts, severity, message}. Server-log lines in the
        ``####<ts> <severity> ...`` format are parsed; access-log lines are
        kept whole with severity "INFO". Unparseable lines are kept with
        severity "INFO" and ts None rather than dropped.
        Requires ``log_dir`` (or the WEBLOGIC_LOG_DIR env var).
        """
        self._require_connected()
        log_dir = self._log_dir or os.environ.get("WEBLOGIC_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read_weblogic_log needs log_dir or the "
                "WEBLOGIC_LOG_DIR env var"
            )
        if log == "server":
            candidates = glob.glob(os.path.join(log_dir, "*.log"))
            if not candidates:
                raise ConnectorError(
                    f"{self.name}: no server *.log files found in "
                    f"{log_dir!r}"
                )
            path = max(candidates, key=os.path.getmtime)
        elif log == "access":
            candidates = glob.glob(os.path.join(log_dir, "access*.log"))
            if not candidates:
                raise ConnectorError(
                    f"{self.name}: no access*.log files found in "
                    f"{log_dir!r}"
                )
            path = max(candidates, key=os.path.getmtime)
        else:
            raise ConnectorError(
                f"{self.name}: unknown log {log!r}; expected 'server' or "
                "'access'"
            )
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            raise ConnectorError(
                f"{self.name}: cannot read log {path!r}: {exc}"
            ) from exc
        entries: list[dict] = []
        for line in lines[-max(1, min(int(limit), _MAX_LOG_LINES)):]:
            entries.append(self._parse_log_line(log, line))
        return entries

    @staticmethod
    def _parse_log_line(log: str, line: str) -> dict:
        if log == "server":
            m = _SERVER_LINE.match(line)
            if m:
                return {
                    "ts": m.group(1),
                    "severity": m.group(2),
                    "message": m.group(3).strip(),
                }
        else:
            m = _ACCESS_LINE.match(line)
            if m:
                return {
                    "ts": m.group(1),
                    "severity": "INFO",
                    "message": line,
                }
        return {"ts": None, "severity": "INFO", "message": line}

    # ------------------------------------------------- privileged actions

    def restart_weblogic_app(self, app_name: str) -> dict:
        """PRIVILEGED: restart an application via REST lifecycle stop + start.

        POSTs to serverRuntime/applicationRuntimes/{name}/stop then
        {name}/start. Runs under the SEPARATE admin credential pair;
        refuses when it is not configured rather than reusing the read
        account. Raises ConnectorError for an unknown application name.
        """
        self._require_connected()
        if not app_name or "/" in app_name:
            raise ConnectorError(
                f"{self.name}: app_name must be a plain deployment name, "
                f"got {app_name!r}"
            )
        previous = self._app_state(app_name)
        admin_auth = self._resolve_privileged_credentials()
        quoted = urllib.parse.quote(app_name, safe="")
        for verb in ("stop", "start"):
            resp = self._rest_post(
                f"serverRuntime/applicationRuntimes/{quoted}/{verb}",
                {}, admin_auth)
            # Successful lifecycle POSTs return a task-link payload.
            links = resp.get("links")
            if not isinstance(links, list):
                raise ConnectorError(
                    f"{self.name}: lifecycle {verb} of {app_name!r} "
                    "returned an unexpected response shape"
                )
        return {
            "app_name": app_name,
            "previous_state": previous,
            "state": "running",
            "ts": _now(),
        }

    def _app_state(self, app_name: str) -> str:
        apps = self.get_weblogic_apps()["apps"]
        for app in apps:
            if app["name"] == app_name:
                return str(app["state"])
        raise ConnectorError(
            f"{self.name}: no deployed app named {app_name!r}"
        )


register_connector(WebLogicConnector.SPEC, WebLogicConnector)
