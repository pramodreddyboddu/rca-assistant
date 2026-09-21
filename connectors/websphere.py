"""Real IBM WebSphere Application Server connector: JVM/app diagnostics over HTTP.

TRANSPORT
---------
Primary (only) transport is the Jolokia JMX-HTTP agent
(``http://host:port/jolokia/``), plain HTTP over stdlib ``urllib`` -- no
third-party driver is needed. The Jolokia javaagent runs on any JVM,
including WebSphere. The guarded boundary is the internal
``_UrllibTransport`` (``get_json`` / ``post_json`` / ``get_text``): it
wraps every urllib/socket failure into ``ConnectorError``, so an
unreachable agent never surfaces a raw exception. Tests replace
``self._transport`` with a fake object exposing the same three methods
(see tests/test_websphere.py).

MBean names (pilot-validation notes -- verify against a real cell in the
customer pilot before trusting diagnostics in production):
- Heap: ``java.lang:type=Memory`` (standard JVM MBean; attribute names
  HeapMemoryUsage/NonHeapMemoryUsage are stable).
- Thread pools: ``WebSphere:type=ThreadPoolStats,*``. The attribute-name
  mapping below (poolSize/activeThreads/maximumPoolSize) follows the PMI
  counter vocabulary; real deployments may expose the MXBean-style
  capitalized forms (PoolSize/ActiveThreads) or PMI counters
  (ActiveCount). The connector tries the primary names first, then the
  documented variants; if a pilot shows neither, the mapping must be
  pinned to what the target actually exposes.
- Apps: ``WebSphere:j2eeType=J2EEApplication,*`` (JSR-77 management
  MBeans; the app name is the ``name=`` key of the ObjectName).
- App state/restart: ``WebSphere:type=ApplicationManager,*`` -- the first
  match is used. State comes from the ``isApplicationStarted(appName)``
  operation; a best-effort read of the app MBean's JSR-77 ``state``
  attribute can override to ``failed`` (uncertain on real cells --
  validate in pilot). Restart is ``stopApplication(appName)`` +
  ``startApplication(appName)`` execs on the same MBean.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_websphere_heap() -> dict: host, port, heap_init_bytes,
  heap_used_bytes, heap_committed_bytes, heap_max_bytes, heap_used_pct
  (of max, None when max <= 0), ts.
- get_websphere_threadpool(pool=None) -> dict: host, port, pools (list of
  {name, current_threads_busy, current_thread_count, max_threads,
  busy_pct}), ts. ``pool`` filters to one pool by exact name; unknown pool
  name raises ConnectorError.
- get_websphere_apps() -> dict: host, port, apps (list of {name, state,
  sessions}), ts. ``state`` is one of running/stopped/failed/unknown;
  ``sessions`` is always None (no session counter is exposed on the
  WebSphere app MBeans this connector reads).
- read_websphere_log(log="systemout", limit=50) -> list of {ts, severity,
  message}. ``log`` is "systemout" (SystemOut.log) or "systemerr"
  (SystemErr.log).
- restart_websphere_app(app_name) -> PRIVILEGED dict: app_name,
  previous_state, state ("running"), ts.

CREDENTIALS
-----------
Same rules as the IBM MQ and Tomcat connectors: the constructor takes a
credential-provider callable or env-var NAMES (``WEBSPHERE_READ_USER`` /
``WEBSPHERE_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance
(HTTP Basic auth headers are built in a local and discarded), never
appear in ``repr``, exceptions, or logs. The privileged
``restart_websphere_app`` resolves a SEPARATE pair
(``WEBSPHERE_ADMIN_USER`` / ``WEBSPHERE_ADMIN_PASSWORD`` or a privileged
provider) at act time and refuses when it is absent.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_websphere.py): canned Jolokia JSON
responses, no live WebSphere touched. Live validation against a real
WebSphere cell happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md. In particular the ThreadPoolStats
attribute mapping, the JSR-77 ``state`` failed-state detection, and the
ApplicationManager exec behavior must be confirmed against the real
MBeans in the pilot. ``read_websphere_log`` tails files on the local
disk: it assumes the connector runs co-located with WebSphere (same host
or shared log volume); remote log access needs a log shipper, which is
out of scope for this connector.
"""

from __future__ import annotations

import base64
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

# "[9/21/26 15:30:45:123 CDT] 0000003a SystemOut     O Server started"
_SYSTEMOUT_LINE = re.compile(
    r"^\[([^\]]+)\]\s+(\S+)\s+(\S+)\s+([A-Z])\s?(.*)$"
)

# WebSphere SystemOut/SystemErr single-letter message-type codes,
# best-effort mapping to readable severities.
_LEVEL_SEVERITY = {
    "O": "INFO",   # SystemOut output
    "I": "INFO",
    "W": "WARNING",
    "E": "ERROR",
    "D": "DEBUG",
    "C": "CONFIG",
    "A": "AUDIT",
}

_MAX_LOG_LINES = 200


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _first_num(attrs: dict, *names: str, default: int = 0) -> int:
    """First present numeric attribute from the candidate name list."""
    for name in names:
        if name in attrs and attrs[name] is not None:
            return _int(attrs[name], default)
    return default


class _UrllibTransport:
    """Guarded HTTP boundary for the WebSphere connector.

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
                f"websphere: {op} {url} failed: {exc}"
            ) from exc

    def get_json(self, url: str,
                 auth: tuple[str, str] | None) -> dict:
        raw = self._open("GET", url, auth)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"websphere: GET {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"websphere: GET {url} returned unexpected JSON shape"
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
                f"websphere: POST {url} returned non-JSON data: {exc}"
            ) from exc
        if not isinstance(parsed, dict):
            raise ConnectorError(
                f"websphere: POST {url} returned unexpected JSON shape"
            )
        return parsed

    def get_text(self, url: str,
                 auth: tuple[str, str] | None) -> str:
        raw = self._open("GET", url, auth)
        return raw.decode("utf-8", errors="replace")


class WebSphereConnector(Connector):
    """Live WebSphere diagnostics via the Jolokia JMX-HTTP agent."""

    name = "websphere"

    SPEC = ConnectorSpec(
        name="websphere",
        display_name="IBM WebSphere Application Server (Jolokia JMX-HTTP)",
        description="JVM heap, thread-pool, and enterprise-application "
        "reads via the Jolokia JMX-HTTP agent; approval-gated application "
        "restart via the ApplicationManager MBean stop+start exec; "
        "best-effort local tail of SystemOut.log / SystemErr.log.",
        required_config=("host",),
        optional_config=(
            "port", "jolokia_path", "log_dir", "timeout", "user_env",
            "password_env",
        ),
        credential_refs=("WEBSPHERE_READ_USER", "WEBSPHERE_READ_PASSWORD"),
        notes="Reads use a read-only WebSphere user. restart_websphere_app "
        "uses a separate admin pair (WEBSPHERE_ADMIN_USER/"
        "WEBSPHERE_ADMIN_PASSWORD or a privileged provider) and refuses "
        "when it is absent. Validated against a fake transport only; live "
        "validation happens in a customer pilot. Log tailing assumes "
        "co-located log files.",
    )

    def __init__(
        self,
        host: str,
        port: int = 9080,
        *,
        jolokia_path: str = "jolokia",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "WEBSPHERE_READ_USER",
        password_env: str = "WEBSPHERE_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "WEBSPHERE_ADMIN_USER",
        privileged_password_env: str = "WEBSPHERE_ADMIN_PASSWORD",
        log_dir: str | None = None,
        timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._jolokia_base = (
            f"http://{host}:{int(port)}/{jolokia_path.strip('/')}"
        )
        self._jolokia_path = jolokia_path
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
            "get_websphere_heap",
            "get_websphere_threadpool",
            "get_websphere_apps",
            "read_websphere_log",
            "restart_websphere_app",
        }

    def connect(self) -> None:
        """Probe the Jolokia agent with the read credential.

        Fails closed: any transport, credential, or shape problem raises
        ConnectorError and the connector stays disconnected.
        """
        auth = self._resolve_read_credentials()
        try:
            info = self._transport.get_json(
                f"{self._jolokia_base}/version", auth)
            if not isinstance(info.get("value"), dict):
                raise ConnectorError(
                    "websphere: Jolokia agent at "
                    f"{self._jolokia_base} returned an unexpected "
                    "version response"
                )
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"websphere: connect failed: {exc}") from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"WebSphereConnector(host={self._host!r}, port={self._port!r}, "
            f"jolokia_path={self._jolokia_path!r}, "
            f"log_dir={self._log_dir!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "websphere: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="WebSphere read user")
        password = resolve_secret(env=self._password_env,
                                  label="WebSphere read password")
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
                    "websphere: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="WebSphere privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="WebSphere privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
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

    def _jolokia_read_attr(self, mbean: str, attr: str) -> Any:
        """Read one MBean attribute; returns the raw Jolokia value."""
        quoted = urllib.parse.quote(mbean, safe="")
        resp = self._jolokia_get(f"read/{quoted}/{attr}")
        return resp.get("value")

    def _jolokia_exec(self, mbean: str, operation: str,
                      args: list, auth: tuple[str, str]) -> Any:
        """Run a JMX operation via Jolokia exec; raise on error status."""
        resp = self._transport.post_json(
            self._jolokia_base,
            {"type": "exec", "mbean": mbean, "operation": operation,
             "arguments": list(args)},
            auth,
        )
        status = resp.get("status", 200)
        if status != 200:
            raise ConnectorError(
                f"{self.name}: Jolokia exec {operation} on {mbean!r} "
                f"failed (status {status}): "
                f"{resp.get('error') or resp.get('stacktrace') or '?'}"
            )
        return resp.get("value")

    @staticmethod
    def _objectname_key(mbean: str, key: str) -> str | None:
        """Extract one key's value from an ObjectName string."""
        for part in mbean.split(","):
            if part.startswith(f"{key}="):
                return part[len(key) + 1:].strip('"')
        return None

    def _application_manager_mbean(self) -> str:
        mbeans = self._jolokia_search("WebSphere:type=ApplicationManager,*")
        if not mbeans:
            raise ConnectorError(
                f"{self.name}: no WebSphere:type=ApplicationManager MBean "
                "found; cannot determine or change app state"
            )
        return mbeans[0]

    # ------------------------------------------------- reads

    def get_websphere_heap(self) -> dict:
        """JVM heap usage from java.lang:type=Memory.

        Returns {host, port, heap_init_bytes, heap_used_bytes,
        heap_committed_bytes, heap_max_bytes, heap_used_pct (of max, None
        when max <= 0), ts}.
        """
        self._require_connected()
        attrs = self._jolokia_read_mbean("java.lang:type=Memory")
        heap = attrs.get("HeapMemoryUsage") or {}
        heap_max = _int(heap.get("max", -1), -1)
        heap_used = _int(heap.get("used", 0))
        used_pct = (round(heap_used / heap_max * 100, 1)
                    if heap_max > 0 else None)
        return {
            "host": self._host,
            "port": self._port,
            "heap_init_bytes": _int(heap.get("init", 0)),
            "heap_used_bytes": heap_used,
            "heap_committed_bytes": _int(heap.get("committed", 0)),
            "heap_max_bytes": heap_max,
            "heap_used_pct": used_pct,
            "ts": _now(),
        }

    def get_websphere_threadpool(self, pool: str | None = None) -> dict:
        """WebSphere thread-pool stats: busy threads vs max per pool.

        Pools come from ``WebSphere:type=ThreadPoolStats,*``; attribute
        mapping is poolSize -> current_thread_count, activeThreads ->
        current_threads_busy, maximumPoolSize (fallback poolSize) ->
        max_threads, with documented capitalized variants as fallbacks
        (pilot-validation note in the module docstring).

        Returns {host, port, pools: [{name, current_threads_busy,
        current_thread_count, max_threads, busy_pct}], ts}. ``pool``
        filters to one pool by exact name; unknown pool name raises
        ConnectorError.
        """
        self._require_connected()
        mbeans = self._jolokia_search("WebSphere:type=ThreadPoolStats,*")
        pools: list[dict] = []
        for mbean in mbeans:
            attrs = self._jolokia_read_mbean(mbean)
            name = (self._objectname_key(mbean, "name")
                    or attrs.get("PoolName") or attrs.get("name")
                    or mbean)
            busy = _first_num(attrs, "activeThreads", "ActiveThreads",
                              "ActiveCount")
            count = _first_num(attrs, "poolSize", "PoolSize")
            max_threads = _first_num(attrs, "maximumPoolSize",
                                     "MaximumPoolSize", "MaxPoolSize",
                                     default=count)
            pools.append({
                "name": str(name),
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

    def get_websphere_apps(self) -> dict:
        """Deployed enterprise applications with state.

        Apps come from ``WebSphere:j2eeType=J2EEApplication,*``; state
        comes from ``isApplicationStarted(appName)`` on the
        ``WebSphere:type=ApplicationManager`` MBean (running / stopped /
        unknown when the exec fails). A best-effort read of the app
        MBean's JSR-77 ``state`` attribute can override to ``failed``;
        attribute-name uncertainty is a pilot-validation note.

        Returns {host, port, apps: [{name, state, sessions}], ts};
        ``sessions`` is always None (no session counter on these MBeans).
        """
        self._require_connected()
        app_mbeans = self._jolokia_search(
            "WebSphere:j2eeType=J2EEApplication,*")
        app_mgr = self._application_manager_mbean()
        read_auth = self._resolve_read_credentials()
        apps: list[dict] = []
        for mbean in app_mbeans:
            name = self._objectname_key(mbean, "name") or mbean
            try:
                started = self._jolokia_exec(
                    app_mgr, "isApplicationStarted", [name], read_auth)
            except ConnectorError:
                state = "unknown"
            else:
                state = "running" if started else "stopped"
            if state in ("running", "stopped"):
                # Best-effort failed-state detection via JSR-77
                # StateManageable: a FAILED state here is real trouble,
                # not merely "stopped".
                try:
                    jsr_state = self._jolokia_read_attr(mbean, "state")
                    if (isinstance(jsr_state, str)
                            and jsr_state.strip().upper() == "FAILED"):
                        state = "failed"
                except ConnectorError:
                    pass
            apps.append({"name": str(name), "state": state, "sessions": None})
        apps.sort(key=lambda a: a["name"])
        return {
            "host": self._host,
            "port": self._port,
            "apps": apps,
            "ts": _now(),
        }

    def read_websphere_log(self, log: str = "systemout",
                           limit: int = 50) -> list[dict]:
        """Tail SystemOut.log or SystemErr.log (co-located files).

        Best-effort parse into {ts, severity, message}: WebSphere
        ``[date] threadId logger level message`` lines are parsed and the
        single-letter level is mapped to a readable severity; unparseable
        lines are kept with severity "INFO" and ts None rather than
        dropped. Requires ``log_dir`` (or the WEBSPHERE_LOG_DIR env var).
        """
        self._require_connected()
        log_dir = self._log_dir or os.environ.get("WEBSPHERE_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read_websphere_log needs log_dir or the "
                "WEBSPHERE_LOG_DIR env var"
            )
        files = {"systemout": "SystemOut.log", "systemerr": "SystemErr.log"}
        if log not in files:
            raise ConnectorError(
                f"{self.name}: unknown log {log!r}; expected 'systemout' or "
                "'systemerr'"
            )
        path = os.path.join(log_dir, files[log])
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            raise ConnectorError(
                f"{self.name}: cannot read log {path!r}: {exc}"
            ) from exc
        entries: list[dict] = []
        for line in lines[-max(1, min(int(limit), _MAX_LOG_LINES)):]:
            m = _SYSTEMOUT_LINE.match(line)
            if m:
                entries.append({
                    "ts": m.group(1),
                    "severity": _LEVEL_SEVERITY.get(m.group(4), "INFO"),
                    "message": m.group(5).strip(),
                })
            else:
                entries.append({
                    "ts": None,
                    "severity": "INFO",
                    "message": line,
                })
        return entries

    # ------------------------------------------------- privileged actions

    def restart_websphere_app(self, app_name: str) -> dict:
        """PRIVILEGED: restart an app via ApplicationManager stop + start.

        Runs under the SEPARATE admin credential pair; refuses when it is
        not configured rather than reusing the read account. Returns
        {app_name, previous_state, state ("running"), ts}.
        """
        self._require_connected()
        if not app_name or not str(app_name).strip():
            raise ConnectorError(
                f"{self.name}: app_name must be a non-empty application "
                f"name, got {app_name!r}"
            )
        app_name = str(app_name).strip()
        previous = self._app_state(app_name)
        admin_auth = self._resolve_privileged_credentials()
        app_mgr = self._application_manager_mbean()
        for op in ("stopApplication", "startApplication"):
            self._jolokia_exec(app_mgr, op, [app_name], admin_auth)
        return {
            "app_name": app_name,
            "previous_state": previous,
            "state": "running",
            "ts": _now(),
        }

    def _app_state(self, app_name: str) -> str:
        apps = self.get_websphere_apps()["apps"]
        for app in apps:
            if app["name"] == app_name:
                return str(app["state"])
        raise ConnectorError(
            f"{self.name}: no deployed app named {app_name!r}"
        )


register_connector(WebSphereConnector.SPEC, WebSphereConnector)
