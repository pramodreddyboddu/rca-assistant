"""Real JBoss EAP / WildFly connector: JVM/thread-pool/deployment diagnostics.

TRANSPORT
---------
The WildFly HTTP management API (``http://host:9990/management`` by
default), plain stdlib ``urllib`` POSTs of JSON operation payloads --
``{"operation": ..., "address": [...], ...}`` -- with HTTP Digest auth
(``urllib.request.HTTPDigestAuthHandler``). No third-party driver is
needed. The guarded boundary is the internal ``_ManagementTransport``
(``post``): it wraps every urllib/socket failure into ``ConnectorError``,
checks the management-API ``outcome`` envelope (``failed`` becomes
``ConnectorError`` with the failure-description), and never sees a
stored credential -- the (user, password) pair is resolved by the
connector per request and passed as a local. Tests replace
``self._transport`` with a fake object exposing the same ``post``
method (see tests/test_jboss.py).

Management operations used (verified against the WildFly admin guide and
the wildscribe model reference; attribute shapes below are the best
documented values -- exact metric availability varies across EAP/WildFly
versions and is a pilot-validation item, see docs/REAL_CONNECTOR_READINESS.md):
- heap:  {"operation":"read-resource","address":[{"core-service":
  "platform-mbean"},{"type":"memory"}]} -> result["heap-memory-usage"]
  with init/used/committed/max.
- thread pools: {"operation":"read-children-resources","address":
  [{"subsystem":"io"}],"child-type":"worker"} to enumerate workers, then
  per worker {"operation":"read-resource","address":[{"subsystem":"io"},
  {"worker": name}],"include-runtime":true} -> runtime metrics
  busy-task-thread-count / core-pool-size / max-pool-size.
- deployments: {"operation":"read-children-resources","address":[],
  "child-type":"deployment"} -> {name: {enabled, status}}.
- logs: {"operation":"read-log-file","name":"server.log","lines":N}
  -> a list of log lines. Falls back to co-located files when the
  operation is unavailable.
- redeploy: {"operation":"redeploy","address":[{"deployment": name}]}.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_jboss_heap() -> dict: host, port, heap_init_bytes, heap_used_bytes,
  heap_committed_bytes, heap_max_bytes, heap_used_pct (of max, None when
  max <= 0), ts.
- get_jboss_threadpool(pool=None) -> dict: host, port, pools (list of
  {name, current_threads_busy, current_thread_count, max_threads,
  busy_pct}), ts. ``pool`` filters to one worker by exact name; unknown
  pool name raises ConnectorError.
- get_jboss_deployments() -> dict: host, port, deployments (list of
  {name, enabled, status}), ts. ``status`` is the server-side status
  string (typically "OK").
- read_jboss_log(log="server", limit=50) -> list of {ts, severity,
  message}. ``log`` is "server" (server.log) or "boot" (boot.log), read
  via the read-log-file operation; falls back to co-located files via
  ``log_dir`` or the JBOSS_LOG_DIR env var.
- restart_jboss_deployment(deployment) -> PRIVILEGED dict: deployment,
  previous_state, state ("running"), ts.

CREDENTIALS
-----------
Same rules as the IBM MQ and Tomcat connectors: the constructor takes a
credential-provider callable or env-var NAMES (``JBOSS_READ_USER`` /
``JBOSS_READ_PASSWORD``), never a raw secret. Secrets resolve per request
via ``resolve_secret`` and are never stored on the instance (the Digest
auth opener is built in a local per request and discarded), never appear
in ``repr``, exceptions, or logs. The privileged
``restart_jboss_deployment`` resolves a SEPARATE admin pair
(``JBOSS_ADMIN_USER`` / ``JBOSS_ADMIN_PASSWORD`` or a privileged
provider) at act time and refuses when it is absent.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_jboss.py): canned management-API
JSON responses, no live server touched. Live validation against a real
JBoss EAP / WildFly happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md. The exact management-API operation
names, address paths, and metric attributes used here were verified
against public documentation but have not been proven against a live
server. ``read_jboss_log`` falls back to tailing files on the local
disk: it assumes the connector runs co-located with the server (same
host or shared log volume); remote log access needs a log shipper,
which is out of scope for this connector.
"""

from __future__ import annotations

import glob
import json
import os
import re
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

# "14:33:20,123 INFO [org.jboss.as] (Controller Boot Thread) WFLYSRV0025: ..."
_WILDFLY_LINE = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}\s+)?(\d{2}:\d{2}:\d{2},\d{3})\s+"
    r"(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\s+\[.*?\]\s+(.*)$"
)
_WILDFLY_LINE_BARE = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}\s+)?(\d{2}:\d{2}:\d{2},\d{3})\s+"
    r"(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\s+(.*)$"
)

_MAX_LOG_LINES = 200

#: Known log names -> read-log-file names and co-located file names.
_LOGS = {
    "server": ("server.log", "server.log"),
    "boot": ("boot.log", "boot.log"),
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _ManagementTransport:
    """Guarded HTTP boundary for the JBoss connector.

    Stdlib urllib POSTs against the WildFly management API with per-call
    HTTP Digest auth. Every failure (DNS, refused, timeout, HTTP error,
    bad JSON, or a management-API ``outcome=failed`` envelope) is wrapped
    in ConnectorError here, so connector code above this layer never sees
    a raw urllib/socket exception and never sees a leaked credential.
    Holds no credentials: auth is passed per call as a (user, password)
    tuple and used only to build a Digest opener in a local.
    """

    def __init__(self, timeout: int = 10) -> None:
        self._timeout = timeout

    def post(self, url: str, payload: dict,
             auth: tuple[str, str] | None) -> Any:
        """POST a management operation; return its ``result`` on success."""
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data)
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        try:
            if auth is not None:
                user, password = auth
                mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
                # None realm: the manager matches the request URL's
                # authority, so the Digest handshake applies per request.
                mgr.add_password(None, url, user, password)
                opener = urllib.request.build_opener(
                    urllib.request.HTTPDigestAuthHandler(mgr))
                with opener.open(req, timeout=self._timeout) as resp:
                    raw = resp.read()
            else:
                with urllib.request.urlopen(req,
                                            timeout=self._timeout) as resp:
                    raw = resp.read()
        except Exception as exc:
            # Never include auth material: credentials travel in the
            # Authorization header, never in the URL.
            raise ConnectorError(
                f"jboss: POST {url} {payload.get('operation')!r} failed: {exc}"
            ) from exc
        try:
            envelope = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ConnectorError(
                f"jboss: management API at {url} returned non-JSON data: "
                f"{exc}"
            ) from exc
        if not isinstance(envelope, dict):
            raise ConnectorError(
                f"jboss: management API at {url} returned an unexpected "
                "response shape"
            )
        if envelope.get("outcome") != "success":
            detail = envelope.get("failure-description", "(no detail)")
            raise ConnectorError(
                f"jboss: {payload.get('operation')!r} failed: {detail}"
            )
        return envelope.get("result")


class JBossConnector(Connector):
    """Live JBoss EAP / WildFly diagnostics via the HTTP management API."""

    name = "jboss"

    SPEC = ConnectorSpec(
        name="jboss",
        display_name="JBoss EAP / WildFly (HTTP management API)",
        description="JVM heap, IO-subsystem worker thread pools, and "
        "deployment reads over the WildFly HTTP management API with "
        "Digest auth; approval-gated deployment redeploy; server.log "
        "tailing via the read-log-file operation with a co-located "
        "file fallback.",
        required_config=("host",),
        optional_config=(
            "port", "management_path", "log_dir", "timeout",
            "user_env", "password_env",
        ),
        credential_refs=("JBOSS_READ_USER", "JBOSS_READ_PASSWORD"),
        notes="Reads use a read-only management user. "
        "restart_jboss_deployment uses a separate admin pair "
        "(JBOSS_ADMIN_USER/JBOSS_ADMIN_PASSWORD or a privileged provider) "
        "and refuses when it is absent. Validated against a fake transport "
        "only; live validation happens in a customer pilot. Log tailing "
        "fallback assumes co-located log files.",
    )

    def __init__(
        self,
        host: str,
        port: int = 9990,
        *,
        management_path: str = "management",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "JBOSS_READ_USER",
        password_env: str = "JBOSS_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "JBOSS_ADMIN_USER",
        privileged_password_env: str = "JBOSS_ADMIN_PASSWORD",
        log_dir: str | None = None,
        timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._mgmt_url = (
            f"http://{host}:{int(port)}/{management_path.strip('/')}"
        )
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._log_dir = log_dir
        self._transport: Any = _ManagementTransport(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_jboss_heap",
            "get_jboss_threadpool",
            "get_jboss_deployments",
            "read_jboss_log",
            "restart_jboss_deployment",
        }

    def connect(self) -> None:
        """Probe the management API with the read credential; fail closed."""
        result = self._post(
            {"operation": "read-attribute", "address": [],
             "name": "server-state"},
            self._resolve_read_credentials(),
        )
        if result not in ("running", "reload-required",
                          "restart-required", "starting", "stopping"):
            # read-attribute succeeded at the envelope level but returned
            # something unexpected; treat as a probing failure.
            raise ConnectorError(
                f"jboss: unexpected server-state probe result: {result!r}"
            )
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"JBossConnector(host={self._host!r}, port={self._port!r}, "
            f"mgmt_url={self._mgmt_url!r}, log_dir={self._log_dir!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "jboss: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="JBoss read user")
        password = resolve_secret(env=self._password_env,
                                  label="JBoss read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by act() paths.

        Raises ConnectorError when no privileged credential is configured,
        so the redeploy path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            user, password = self._priv_credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "jboss: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="JBoss privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="JBoss privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _post(self, payload: dict,
              auth: tuple[str, str] | None = None) -> Any:
        """POST one management operation; returns its ``result``."""
        auth = auth if auth is not None else self._resolve_read_credentials()
        try:
            return self._transport.post(self._mgmt_url, payload, auth)
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"jboss: management call failed: {exc}"
                                 ) from exc

    # ------------------------------------------------- reads

    def get_jboss_heap(self) -> dict:
        """JVM heap usage from core-service=platform-mbean, type=memory.

        Returns {host, port, heap_init_bytes, heap_used_bytes,
        heap_committed_bytes, heap_max_bytes, heap_used_pct, ts}.
        heap_used_pct is None when max is undefined (max <= 0).
        """
        self._require_connected()
        result = self._post({
            "operation": "read-resource",
            "address": [{"core-service": "platform-mbean"},
                        {"type": "memory"}],
        })
        if not isinstance(result, dict):
            raise ConnectorError(
                "jboss: heap read returned an unexpected shape"
            )
        heap = result.get("heap-memory-usage") or {}
        heap_max = int(heap.get("max", -1) or -1)
        heap_used = int(heap.get("used", 0) or 0)
        return {
            "host": self._host,
            "port": self._port,
            "heap_init_bytes": int(heap.get("init", 0) or 0),
            "heap_used_bytes": heap_used,
            "heap_committed_bytes": int(heap.get("committed", 0) or 0),
            "heap_max_bytes": heap_max,
            "heap_used_pct": (round(heap_used / heap_max * 100, 1)
                              if heap_max > 0 else None),
            "ts": _now(),
        }

    def get_jboss_threadpool(self, pool: str | None = None) -> dict:
        """IO-subsystem worker thread-pool stats from include-runtime reads.

        Returns {host, port, pools, ts}; each pool is {name,
        current_threads_busy, current_thread_count, max_threads, busy_pct}.
        ``pool`` filters to one worker by exact name; unknown pool name
        raises ConnectorError.

        Mapping (documented best-effort; metric names come from the
        wildscribe io/worker model reference): current_threads_busy <-
        busy-task-thread-count, current_thread_count <- core-pool-size,
        max_threads <- max-pool-size. Pilot validation must confirm the
        metric names against the live server version.
        """
        self._require_connected()
        workers = self._worker_names()
        pools: list[dict] = []
        for name in workers:
            result = self._post({
                "operation": "read-resource",
                "address": [{"subsystem": "io"}, {"worker": name}],
                "include-runtime": True,
            })
            if not isinstance(result, dict):
                raise ConnectorError(
                    f"jboss: worker {name!r} read returned an unexpected "
                    "shape"
                )
            busy = int(result.get("busy-task-thread-count", 0) or 0)
            count = int(result.get("core-pool-size", 0) or 0)
            max_threads = int(result.get("max-pool-size", 0) or 0)
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

    def _worker_names(self) -> list[str]:
        try:
            result = self._post({
                "operation": "read-children-resources",
                "address": [{"subsystem": "io"}],
                "child-type": "worker",
            })
        except ConnectorError:
            # Best-effort: "default" is the out-of-the-box worker name.
            return ["default"]
        if isinstance(result, dict):
            return sorted(str(k) for k in result)
        return ["default"]

    def get_jboss_deployments(self) -> dict:
        """Deployed apps from read-children-resources, child-type=deployment.

        Returns {host, port, deployments, ts}; each deployment is {name,
        enabled, status} where status is the server-side status string
        (typically "OK").
        """
        self._require_connected()
        result = self._post({
            "operation": "read-children-resources",
            "address": [],
            "child-type": "deployment",
        })
        if not isinstance(result, dict):
            raise ConnectorError(
                "jboss: deployment listing returned an unexpected shape"
            )
        deployments = [
            {
                "name": str(name),
                "enabled": bool(attrs.get("enabled", False)),
                "status": str(attrs.get("status", "unknown")),
            }
            for name, attrs in sorted(result.items())
            if isinstance(attrs, dict)
        ]
        return {
            "host": self._host,
            "port": self._port,
            "deployments": deployments,
            "ts": _now(),
        }

    def read_jboss_log(self, log: str = "server",
                       limit: int = 50) -> list[dict]:
        """Tail server.log (or boot.log) into {ts, severity, message} entries.

        Primary path is the management read-log-file operation; falls
        back to co-located files (``log_dir`` or the JBOSS_LOG_DIR env
        var) when the operation is unavailable. Unparseable lines are kept
        with severity "INFO" and ts None rather than dropped.
        """
        self._require_connected()
        if log not in _LOGS:
            raise ConnectorError(
                f"{self.name}: unknown log {log!r}; expected one of "
                f"{sorted(_LOGS)}"
            )
        op_name, _file_name = _LOGS[log]
        try:
            result = self._post({
                "operation": "read-log-file",
                "name": op_name,
                "lines": max(1, min(int(limit), _MAX_LOG_LINES)),
            })
        except ConnectorError:
            return self._read_log_file_fallback(log, limit)
        if not isinstance(result, list):
            raise ConnectorError(
                "jboss: read-log-file returned an unexpected shape"
            )
        return [self._parse_log_line(str(line)) for line in result]

    def _read_log_file_fallback(self, log: str, limit: int) -> list[dict]:
        log_dir = self._log_dir or os.environ.get("JBOSS_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read-log-file is unavailable and no "
                "co-located log dir is configured (log_dir or the "
                "JBOSS_LOG_DIR env var)"
            )
        _op_name, file_name = _LOGS[log]
        path = os.path.join(log_dir, file_name)
        try:
            with open(path, "r", encoding="utf-8",
                      errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            raise ConnectorError(
                f"{self.name}: cannot read log {path!r}: {exc}"
            ) from exc
        return [self._parse_log_line(line)
                for line in lines[-max(1, min(int(limit), _MAX_LOG_LINES)):]]

    @staticmethod
    def _parse_log_line(line: str) -> dict:
        m = _WILDFLY_LINE.match(line) or _WILDFLY_LINE_BARE.match(line)
        if m:
            return {
                "ts": m.group(1),
                "severity": m.group(2),
                "message": m.group(3).strip(),
            }
        return {"ts": None, "severity": "INFO", "message": line}

    # ------------------------------------------------- privileged actions

    def restart_jboss_deployment(self, deployment: str) -> dict:
        """PRIVILEGED: redeploy an application via the management API.

        Runs under the SEPARATE admin credential pair; refuses when it is
        not configured rather than reusing the read account. Returns
        {deployment, previous_state, state ("running"), ts}.
        """
        self._require_connected()
        if not deployment or "/" in deployment or "\\" in deployment:
            raise ConnectorError(
                f"{self.name}: deployment must be a deployment name like "
                f"'orders.war', got {deployment!r}"
            )
        previous = self._deployment_state(deployment)
        admin_auth = self._resolve_privileged_credentials()
        self._post(
            {"operation": "redeploy",
             "address": [{"deployment": deployment}]},
            admin_auth,
        )
        return {
            "deployment": deployment,
            "previous_state": previous,
            "state": "running",
            "ts": _now(),
        }

    def _deployment_state(self, deployment: str) -> str:
        deployments = self.get_jboss_deployments()["deployments"]
        for dep in deployments:
            if dep["name"] == deployment:
                return str(dep["status"])
        raise ConnectorError(
            f"{self.name}: no deployment named {deployment!r}"
        )


register_connector(JBossConnector.SPEC, JBossConnector)
