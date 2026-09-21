"""Real Tomcat connector: JVM/thread-pool/app diagnostics over HTTP.

TRANSPORT
---------
Primary transport is the Jolokia JMX-HTTP agent (``http://host:port/jolokia/``),
plain HTTP over stdlib ``urllib`` -- no third-party driver is needed. The
guarded boundary is the internal ``_UrllibTransport`` (``get_json`` /
``post_json`` / ``get_text``): it wraps every urllib/socket failure into
``ConnectorError``, so an unreachable agent never surfaces a raw exception.
Tests replace ``self._transport`` with a fake object exposing the same
three methods (see tests/test_tomcat.py).

An alternate ``transport="manager"`` mode uses the Tomcat Manager text API
(``/manager/text/...``) for app deployment state; heap and thread-pool
reads raise ``ConnectorError`` there because the Manager text API does not
expose them (documented limitation, not a bug). App restart ALWAYS uses the
Manager text API (stop + start), whatever the read transport is, because it
is the supported remote-restart path; that makes the Manager URL a required
piece of config for the privileged path only.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_tomcat_heap() -> dict: host, port, heap_init_bytes, heap_used_bytes,
  heap_committed_bytes, heap_max_bytes, heap_used_pct (of max, None when
  max <= 0), non_heap_init_bytes, non_heap_used_bytes,
  non_heap_committed_bytes, non_heap_max_bytes, ts.
- get_tomcat_threadpool(pool=None) -> dict: host, port, pools (list of
  {name, current_threads_busy, current_thread_count, max_threads,
  busy_pct}), ts. ``pool`` filters to one pool by exact name; unknown pool
  name raises ConnectorError.
- get_tomcat_apps() -> dict: host, port, apps (list of {path, state,
  sessions}), ts. ``state`` is one of running/stopped/failed/...;
  ``sessions`` is None when the Manager MBean is not readable.
- read_tomcat_log(log="catalina", limit=50) -> list of {ts, severity,
  message}. ``log`` is "catalina" (catalina.out) or "access" (newest
  localhost_access_log*.txt).
- restart_tomcat_app(app_path) -> PRIVILEGED dict: app_path,
  previous_state, state ("running"), ts.

CREDENTIALS
-----------
Same rules as the IBM MQ connector: the constructor takes a
credential-provider callable or env-var NAMES (``TOMCAT_READ_USER`` /
``TOMCAT_READ_PASSWORD``), never a raw secret. Secrets resolve per request
via ``resolve_secret`` and are never stored on the instance (HTTP Basic
auth headers are built in a local and discarded), never appear in ``repr``,
exceptions, or logs. The privileged ``restart_tomcat_app`` resolves a
SEPARATE pair (``TOMCAT_ADMIN_USER`` / ``TOMCAT_ADMIN_PASSWORD`` or a
privileged provider) at act time and refuses when it is absent.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_tomcat.py): canned Jolokia JSON and
Manager text responses, no live Tomcat touched. Live validation against a
real Tomcat happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md. ``read_tomcat_log`` tails files on the
local disk: it assumes the connector runs co-located with Tomcat (same host
or shared log volume); remote log access needs a log shipper, which is out
of scope for this connector.
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

_LIFECYCLE_STATES = {
    0: "new",
    1: "initializing",
    2: "initialized",
    3: "starting",
    4: "starting",
    5: "running",
    6: "stopping",
    7: "stopping",
    8: "stopped",
    9: "destroyed",
    10: "failed",
}

# "21-Sep-2026 14:33:20.123 INFO [main] org.apache... Server startup..."
_CATALINA_LINE = re.compile(
    r"^(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+"
    r"(SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST)\b(.*)$"
)

_MAX_LOG_LINES = 200


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _UrllibTransport:
    """Guarded HTTP boundary for the Tomcat connector.

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
                f"tomcat: {op} {url} failed: {exc}"
            ) from exc

    def get_json(self, url: str,
                 auth: tuple[str, str] | None) -> dict:
        raw = self._open("GET", url, auth)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"tomcat: GET {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"tomcat: GET {url} returned unexpected JSON shape"
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
                f"tomcat: POST {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"tomcat: POST {url} returned unexpected JSON shape"
            )
        return parsed

    def get_text(self, url: str,
                 auth: tuple[str, str] | None) -> str:
        raw = self._open("GET", url, auth)
        return raw.decode("utf-8", errors="replace")


class TomcatConnector(Connector):
    """Live Tomcat diagnostics via Jolokia / Manager text API."""

    name = "tomcat"

    SPEC = ConnectorSpec(
        name="tomcat",
        display_name="Apache Tomcat (Jolokia JMX-HTTP / Manager text API)",
        description="JVM heap, Catalina thread-pool, and deployed-webapp "
        "reads via the Jolokia JMX-HTTP agent (default) or the Manager "
        "text API; approval-gated webapp restart via Manager stop+start; "
        "best-effort local tail of catalina.out / access logs.",
        required_config=("host",),
        optional_config=(
            "port", "transport", "jolokia_path", "manager_path", "log_dir",
            "timeout", "user_env", "password_env",
        ),
        credential_refs=("TOMCAT_READ_USER", "TOMCAT_READ_PASSWORD"),
        notes="Reads use a read-only Tomcat user. restart_tomcat_app uses a "
        "separate admin pair (TOMCAT_ADMIN_USER/TOMCAT_ADMIN_PASSWORD or a "
        "privileged provider) and refuses when it is absent. Validated "
        "against a fake transport only; live validation happens in a "
        "customer pilot. Log tailing assumes co-located log files.",
    )

    def __init__(
        self,
        host: str,
        port: int = 8080,
        *,
        transport: str = "jolokia",
        jolokia_path: str = "jolokia",
        manager_path: str = "manager/text",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "TOMCAT_READ_USER",
        password_env: str = "TOMCAT_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "TOMCAT_ADMIN_USER",
        privileged_password_env: str = "TOMCAT_ADMIN_PASSWORD",
        log_dir: str | None = None,
        timeout: int = 10,
    ) -> None:
        if transport not in ("jolokia", "manager"):
            raise ConnectorError(
                f"tomcat: unknown transport {transport!r}; "
                "expected 'jolokia' or 'manager'"
            )
        self._host = host
        self._port = int(port)
        self._transport_name = transport
        self._jolokia_base = (
            f"http://{host}:{int(port)}/{jolokia_path.strip('/')}"
        )
        self._manager_base = (
            f"http://{host}:{int(port)}/{manager_path.strip('/')}"
        )
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
            "get_tomcat_heap",
            "get_tomcat_threadpool",
            "get_tomcat_apps",
            "read_tomcat_log",
            "restart_tomcat_app",
        }

    def connect(self) -> None:
        """Probe the agent with the read credential; fail closed on error."""
        auth = self._resolve_read_credentials()
        try:
            if self._transport_name == "jolokia":
                info = self._transport.get_json(
                    f"{self._jolokia_base}/version", auth)
                if not isinstance(info.get("value"), dict):
                    raise ConnectorError(
                        "tomcat: Jolokia agent at "
                        f"{self._jolokia_base} returned an unexpected "
                        "version response"
                    )
            else:
                text = self._transport.get_text(
                    f"{self._manager_base}/list", auth)
                if not text.startswith("OK"):
                    raise ConnectorError(
                        "tomcat: Manager text API did not return OK: "
                        f"{text.splitlines()[0] if text else '(empty)'}"
                    )
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"tomcat: connect failed: {exc}") from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"TomcatConnector(host={self._host!r}, port={self._port!r}, "
            f"transport={self._transport_name!r}, "
            f"log_dir={self._log_dir!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "tomcat: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="Tomcat read user")
        password = resolve_secret(env=self._password_env,
                                  label="Tomcat read password")
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
                    "tomcat: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="Tomcat privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="Tomcat privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _require_jolokia(self, tool: str) -> None:
        if self._transport_name != "jolokia":
            raise ConnectorError(
                f"{self.name}: {tool} needs the Jolokia JMX-HTTP agent; "
                "this connector is configured with transport='manager', "
                "which exposes app state and restart only"
            )

    def _jolokia_get(self, path: str) -> dict:
        return self._transport.get_json(
            f"{self._jolokia_base}/{path.lstrip('/')}",
            self._resolve_read_credentials(),
        )

    def _jolokia_search(self, pattern: str) -> list[str]:
        resp = self._transport.post_json(
            self._jolokia_base,
            {"type": "search", "mbean": pattern},
            self._resolve_read_credentials(),
        )
        value = resp.get("value")
        if not isinstance(value, list):
            raise ConnectorError(
                f"{self.name}: Jolokia search {pattern!r} returned an "
                "unexpected shape"
            )
        return [str(v) for v in value]

    def _jolokia_read_mbean(self, mbean: str) -> dict:
        quoted = urllib.parse.quote(mbean, safe="")
        resp = self._jolokia_get(f"read/{quoted}")
        value = resp.get("value")
        if not isinstance(value, dict):
            raise ConnectorError(
                f"{self.name}: Jolokia read of {mbean!r} returned an "
                "unexpected shape"
            )
        return value

    @staticmethod
    def _state_name(attrs: dict) -> str:
        if isinstance(attrs.get("stateName"), str) and attrs["stateName"]:
            name = attrs["stateName"].upper()
            return {"STARTED": "running", "STOPPED": "stopped",
                    "FAILED": "failed"}.get(name, name.lower())
        state = attrs.get("state")
        if isinstance(state, int):
            return _LIFECYCLE_STATES.get(state, f"unknown({state})")
        return "unknown"

    @staticmethod
    def _context_path(mbean: str) -> str:
        """Extract the webapp context path from a WebModule ObjectName."""
        for part in mbean.split(","):
            if part.startswith("name="):
                name = part[len("name="):]
                # name looks like //localhost/orders or //localhost/
                idx = name.find("//")
                if idx != -1:
                    rest = name[idx + 2:]
                    slash = rest.find("/")
                    path = rest[slash:] if slash != -1 else "/"
                    return path if path else "/"
        return "/"

    # ------------------------------------------------- reads (Jolokia)

    def get_tomcat_heap(self) -> dict:
        """JVM heap + non-heap usage from java.lang:type=Memory."""
        self._require_connected()
        self._require_jolokia("get_tomcat_heap")
        attrs = self._jolokia_read_mbean("java.lang:type=Memory")
        heap = attrs.get("HeapMemoryUsage") or {}
        non_heap = attrs.get("NonHeapMemoryUsage") or {}
        heap_max = int(heap.get("max", -1) or -1)
        heap_used = int(heap.get("used", 0) or 0)
        used_pct = (round(heap_used / heap_max * 100, 1)
                    if heap_max > 0 else None)
        return {
            "host": self._host,
            "port": self._port,
            "heap_init_bytes": int(heap.get("init", 0) or 0),
            "heap_used_bytes": heap_used,
            "heap_committed_bytes": int(heap.get("committed", 0) or 0),
            "heap_max_bytes": heap_max,
            "heap_used_pct": used_pct,
            "non_heap_init_bytes": int(non_heap.get("init", 0) or 0),
            "non_heap_used_bytes": int(non_heap.get("used", 0) or 0),
            "non_heap_committed_bytes": int(
                non_heap.get("committed", 0) or 0),
            "non_heap_max_bytes": int(non_heap.get("max", -1) or -1),
            "ts": _now(),
        }

    def get_tomcat_threadpool(self, pool: str | None = None) -> dict:
        """Catalina thread-pool stats: busy threads vs max per pool."""
        self._require_connected()
        self._require_jolokia("get_tomcat_threadpool")
        mbeans = self._jolokia_search("Catalina:type=ThreadPool,*")
        pools: list[dict] = []
        for mbean in mbeans:
            attrs = self._jolokia_read_mbean(mbean)
            name = str(attrs.get("name") or mbean)
            busy = int(attrs.get("currentThreadsBusy", 0) or 0)
            count = int(attrs.get("currentThreadCount", 0) or 0)
            max_threads = int(attrs.get("maxThreads", 0) or 0)
            pools.append({
                "name": name,
                "current_threads_busy": busy,
                "current_thread_count": count,
                "max_threads": max_threads,
                "busy_pct": (round(busy / max_threads * 100, 1)
                             if max_threads > 0 else None),
            })
        if pool is not None:
            pools = [p for p in pools if p["name"] == pool]
            if not pools:
                raise ConnectorError(
                    f"{self.name}: unknown thread pool {pool!r}"
                )
        pools.sort(key=lambda p: p["name"])
        return {
            "host": self._host,
            "port": self._port,
            "pools": pools,
            "ts": _now(),
        }

    def get_tomcat_apps(self) -> dict:
        """Deployed webapps with state and active sessions."""
        self._require_connected()
        if self._transport_name == "manager":
            return self._apps_via_manager()
        return self._apps_via_jolokia()

    def _apps_via_jolokia(self) -> dict:
        mbeans = self._jolokia_search("Catalina:j2eeType=WebModule,*")
        apps: list[dict] = []
        for mbean in mbeans:
            attrs = self._jolokia_read_mbean(mbean)
            path = self._context_path(mbean)
            sessions: int | None = None
            manager_mbean = (
                f"Catalina:type=Manager,context={path},host=localhost"
                if path != "/" else
                "Catalina:type=Manager,context=,host=localhost"
            )
            try:
                mgr = self._jolokia_read_mbean(manager_mbean)
                if isinstance(mgr.get("activeSessions"), int):
                    sessions = int(mgr["activeSessions"])
            except ConnectorError:
                sessions = None  # best-effort; app state is the point
            apps.append({
                "path": path,
                "state": self._state_name(attrs),
                "sessions": sessions,
            })
        apps.sort(key=lambda a: a["path"])
        return {
            "host": self._host,
            "port": self._port,
            "apps": apps,
            "ts": _now(),
        }

    def _apps_via_manager(self) -> dict:
        text = self._transport.get_text(
            f"{self._manager_base}/list", self._resolve_read_credentials())
        lines = text.splitlines()
        if not lines or not lines[0].startswith("OK"):
            raise ConnectorError(
                f"{self.name}: Manager list failed: "
                f"{lines[0] if lines else '(empty response)'}"
            )
        apps: list[dict] = []
        for line in lines[1:]:
            # "/orders:running:12:orders"
            parts = line.split(":")
            if len(parts) < 4 or not parts[0].startswith("/"):
                continue
            try:
                sessions = int(parts[2])
            except ValueError:
                sessions = None
            apps.append({
                "path": parts[0],
                "state": parts[1] or "unknown",
                "sessions": sessions,
            })
        apps.sort(key=lambda a: a["path"])
        return {
            "host": self._host,
            "port": self._port,
            "apps": apps,
            "ts": _now(),
        }

    def read_tomcat_log(self, log: str = "catalina", limit: int = 50) -> list[dict]:
        """Tail catalina.out or the newest access log (co-located files).

        Best-effort parse into {ts, severity, message}; unparseable lines
        are kept with severity "INFO" and ts None rather than dropped.
        Requires ``log_dir`` (or the TOMCAT_LOG_DIR env var).
        """
        self._require_connected()
        log_dir = self._log_dir or os.environ.get("TOMCAT_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read_tomcat_log needs log_dir or the "
                "TOMCAT_LOG_DIR env var"
            )
        if log == "catalina":
            path = os.path.join(log_dir, "catalina.out")
        elif log == "access":
            candidates = glob.glob(
                os.path.join(log_dir, "localhost_access_log*.txt"))
            if not candidates:
                raise ConnectorError(
                    f"{self.name}: no localhost_access_log*.txt found in "
                    f"{log_dir!r}"
                )
            path = max(candidates, key=os.path.getmtime)
        else:
            raise ConnectorError(
                f"{self.name}: unknown log {log!r}; expected 'catalina' or "
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
            m = _CATALINA_LINE.match(line)
            if m:
                entries.append({
                    "ts": m.group(1),
                    "severity": m.group(2),
                    "message": m.group(3).strip(),
                })
            else:
                entries.append({
                    "ts": None,
                    "severity": "INFO",
                    "message": line,
                })
        return entries

    # ------------------------------------------------- privileged actions

    def restart_tomcat_app(self, app_path: str) -> dict:
        """PRIVILEGED: restart a webapp via Manager stop + start.

        Runs under the SEPARATE admin credential pair; refuses when it is
        not configured rather than reusing the read account.
        """
        self._require_connected()
        if not app_path.startswith("/"):
            raise ConnectorError(
                f"{self.name}: app_path must be a context path like "
                f"'/orders', got {app_path!r}"
            )
        previous = self._app_state(app_path)
        admin_auth = self._resolve_privileged_credentials()
        quoted = urllib.parse.quote(app_path, safe="")
        for verb in ("stop", "start"):
            text = self._transport.get_text(
                f"{self._manager_base}/{verb}?path={quoted}", admin_auth)
            first = text.splitlines()[0] if text else ""
            if not first.startswith("OK"):
                raise ConnectorError(
                    f"{self.name}: Manager {verb} of {app_path!r} failed: "
                    f"{first or '(empty response)'}"
                )
        return {
            "app_path": app_path,
            "previous_state": previous,
            "state": "running",
            "ts": _now(),
        }

    def _app_state(self, app_path: str) -> str:
        apps = self.get_tomcat_apps()["apps"]
        for app in apps:
            if app["path"] == app_path:
                return str(app["state"])
        raise ConnectorError(
            f"{self.name}: no deployed app at {app_path!r}"
        )


register_connector(TomcatConnector.SPEC, TomcatConnector)
