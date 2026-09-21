"""Real Apache HTTPD connector: mod_status reads + apachectl graceful reload.

TRANSPORT
---------
Primary transport is the mod_status machine-readable page
(``http://host:port/<status_path>?auto``), plain HTTP over stdlib
``urllib`` -- no third-party driver is needed. The guarded boundary is
the internal ``_UrllibTransport`` (``get_text``): it wraps every
urllib/socket failure into ``ConnectorError``, so an unreachable server
never surfaces a raw exception. Tests replace ``self._transport`` with a
fake object exposing the same method (see tests/test_apache.py).

Log tailing reads files from the local disk (access_log / error_log),
so it assumes the connector runs co-located with httpd (same host or
shared log volume); remote log access needs a log shipper, which is out
of scope for this connector.

The privileged ``reload_apache`` path is a SEPARATE guarded boundary,
``_ApachectlReloader``: it resolves ``apachectl`` via ``shutil.which``
(and refuses when it is missing), then runs the fixed argv
``["apachectl", "graceful"]`` with ``subprocess.run`` -- never
``shell=True``, never a constructed command string -- with a timeout.
Every failure is wrapped in ``ConnectorError``. This is LOCAL EXECUTION:
it runs as the connector's OS user (whatever user runs the gateway),
which must have permission to reload httpd (typically root or a sudoers
entry; sudo itself is deliberately NOT invoked by the connector -- if
your deployment needs privilege escalation, wrap it at the deployment
layer, e.g. setuid apachectl or a sudoers-gated wrapper named
``apachectl`` on PATH). Tests replace ``self._reloader`` with a fake
object exposing ``reload()``.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py -- this module must not be edited to match them,
they are the contract):
- get_apache_status() -> dict: host, port, total_accesses, total_kbytes,
  uptime_secs, busy_workers, idle_workers, ts. Parsed from the
  ``?auto`` page (requires Total Accesses, Total kBytes, Uptime,
  BusyWorkers, IdleWorkers; extra lines like Scoreboard are ignored).
- read_apache_log(log="access", limit=50) -> list of {ts, severity,
  message}. ``log`` is "access" (newest access_log*/access.log* in the
  log dir; lines kept with severity "INFO", ts None) or "error" (newest
  error_log*/error.log*; best-effort ``[ts] [module:severity]`` parse,
  unparseable lines kept with severity "INFO", ts None).
- reload_apache() -> PRIVILEGED dict: state ("reloaded"), ts. Local
  ``apachectl graceful``; refuses when apachectl is not on PATH.

CREDENTIALS
-----------
Same rules as the Tomcat connector, but authentication is OPTIONAL here:
many mod_status pages are localhost-only with no auth at all. The
constructor takes a credential-provider callable or env-var NAMES
(``user_env`` / ``password_env``, defaulting to None), never a raw
secret. When neither a provider nor both env names are configured, no
``Authorization`` header is sent. Secrets resolve per request via
``resolve_secret`` and are never stored on the instance, never appear in
``repr``, exceptions, or logs. The privileged ``reload_apache`` path
uses NO credentials at all -- it is a local ``apachectl graceful``
running as the connector's OS user, approval-gated upstream.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_apache.py): canned ``?auto``
text, temp log files, and a fake reloader callable; no live httpd
touched. Live validation against a real Apache HTTPD happens in a
customer pilot -- see docs/REAL_CONNECTOR_READINESS.md.
``read_apache_log`` tails files on the local disk: it assumes the
connector runs co-located with httpd (same host or shared log volume).
"""

from __future__ import annotations

import base64
import glob
import os
import re
import shutil
import subprocess
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

# [Mon Sep 21 15:04:02.123456 2026] [mpm_event:error] [pid 1234:tid ...] message
_ERROR_LINE = re.compile(r"^\[([^\]]+)\]\s*\[([^\]]+)\](.*)$")

_MAX_LOG_LINES = 200

#: mod_status ?auto keys this connector reads (matched case-insensitively
#: against the stripped key before the colon).
_STATUS_KEYS = (
    "Total Accesses",
    "Total kBytes",
    "Uptime",
    "BusyWorkers",
    "IdleWorkers",
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _UrllibTransport:
    """Guarded HTTP boundary for the Apache connector.

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
                raw = resp.read()
        except Exception as exc:
            # Never include auth material; the URL carries no secrets
            # (credentials travel in the Authorization header).
            raise ConnectorError(
                f"apache: GET {url} failed: {exc}"
            ) from exc
        return raw.decode("utf-8", errors="replace")


class _ApachectlReloader:
    """Guarded local-exec boundary for ``apachectl graceful``.

    Resolves the binary with ``shutil.which`` and refuses when it is
    missing; runs a FIXED argv with ``subprocess.run`` (never
    ``shell=True``) under a timeout; wraps every failure in
    ConnectorError. Runs as the connector's OS user -- no sudo is
    invoked; privilege setup is the deployment's job (see module
    docstring).
    """

    def __init__(self, timeout: int = 10) -> None:
        self._timeout = timeout

    def reload(self) -> None:
        binary = shutil.which("apachectl")
        if binary is None:
            raise ConnectorError(
                "apache: 'apachectl' not found on PATH; reload refused"
            )
        try:
            proc = subprocess.run(
                [binary, "graceful"],
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
        except FileNotFoundError as exc:
            raise ConnectorError(
                "apache: 'apachectl' not found on PATH; reload refused"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise ConnectorError(
                f"apache: apachectl graceful timed out after "
                f"{self._timeout}s"
            ) from exc
        except Exception as exc:
            raise ConnectorError(
                f"apache: apachectl graceful failed: {exc}"
            ) from exc
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            detail = f": {stderr[:200]}" if stderr else ""
            raise ConnectorError(
                f"apache: apachectl graceful exited {proc.returncode}{detail}"
            )


def _parse_status_page(text: str) -> dict[str, int]:
    """Parse a mod_status ``?auto`` page into its integer counters.

    Raises ConnectorError when any required key is missing or not an
    integer, so connect() and get_apache_status() fail closed on a
    non-mod_status page instead of returning partial data.
    """
    want = {k.lower(): k for k in _STATUS_KEYS}
    values: dict[str, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, _, raw = line.partition(":")
        canonical = want.get(key.strip().lower())
        if canonical is None:
            continue
        try:
            values[canonical] = int(raw.strip())
        except ValueError:
            raise ConnectorError(
                f"apache: mod_status page has non-integer value for "
                f"{canonical!r}: {raw.strip()!r}"
            )
    missing = [k for k in _STATUS_KEYS if k not in values]
    if missing:
        raise ConnectorError(
            "apache: response is not a mod_status ?auto page; missing "
            f"key(s): {', '.join(missing)}"
        )
    return values


class ApacheConnector(Connector):
    """Live Apache HTTPD diagnostics via mod_status; apachectl reload."""

    name = "apache"

    SPEC = ConnectorSpec(
        name="apache",
        display_name="Apache HTTPD (mod_status / apachectl)",
        description="Server counters (accesses, kbytes, uptime, workers) "
        "from the mod_status ?auto page over plain HTTP; best-effort local "
        "tail of access and error logs; approval-gated graceful reload "
        "via apachectl.",
        required_config=("host",),
        optional_config=(
            "port", "status_path", "log_dir", "timeout", "user_env",
            "password_env",
        ),
        credential_refs=("APACHE_STATUS_USER", "APACHE_STATUS_PASSWORD"),
        notes="Status auth is optional (no Authorization header when no "
        "credential is configured). reload_apache runs apachectl graceful "
        "LOCALLY as the connector's OS user and refuses when apachectl is "
        "not on PATH. Validated against a fake transport only; live "
        "validation happens in a customer pilot. Log tailing assumes "
        "co-located log files.",
    )

    def __init__(
        self,
        host: str,
        port: int = 80,
        *,
        status_path: str = "server-status",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str | None = None,
        password_env: str | None = None,
        log_dir: str | None = None,
        timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._status_url = (
            f"http://{host}:{int(port)}/{status_path.strip('/')}?auto"
        )
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._log_dir = log_dir
        self._transport: Any = _UrllibTransport(timeout=timeout)
        self._reloader: Any = _ApachectlReloader(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_apache_status",
            "read_apache_log",
            "reload_apache",
        }

    def connect(self) -> None:
        """Probe the mod_status ?auto page; fail closed on any error.

        Auth is sent only when a credential resolves (provider or both
        env vars); otherwise the page is fetched anonymously.
        """
        auth = self._resolve_credentials()
        try:
            text = self._transport.get_text(self._status_url, auth)
        except ConnectorError:
            raise
        except Exception as exc:  # pragma: no cover - transport wraps
            raise ConnectorError(f"apache: connect failed: {exc}") from exc
        # Fail closed when the target answers but is not a mod_status page.
        _parse_status_page(text)
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"ApacheConnector(host={self._host!r}, port={self._port!r}, "
            f"status_url={self._status_url!r}, log_dir={self._log_dir!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_credentials(self) -> tuple[str, str] | None:
        """Resolve the OPTIONAL Basic-auth credential for mod_status.

        Returns None when no credential is configured, in which case the
        page is fetched without an Authorization header. Never stores the
        resolved value on the instance.
        """
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "apache: credential provider returned an empty "
                    "user/password"
                )
            return user, password
        if self._user_env is None and self._password_env is None:
            return None
        if self._user_env is None or self._password_env is None:
            raise ConnectorError(
                "apache: status auth needs BOTH user_env and password_env "
                "(or neither, for anonymous access)"
            )
        user = resolve_secret(env=self._user_env,
                              label="Apache status user")
        password = resolve_secret(env=self._password_env,
                                  label="Apache status password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _fetch_status(self) -> dict[str, int]:
        text = self._transport.get_text(
            self._status_url, self._resolve_credentials())
        return _parse_status_page(text)

    # ------------------------------------------------- reads

    def get_apache_status(self) -> dict:
        """mod_status counters: accesses, kbytes, uptime, busy/idle workers.

        Returns {host, port, total_accesses, total_kbytes, uptime_secs,
        busy_workers, idle_workers, ts}. Raises ConnectorError when the
        ?auto page is missing keys or has non-integer values.
        """
        self._require_connected()
        values = self._fetch_status()
        return {
            "host": self._host,
            "port": self._port,
            "total_accesses": values["Total Accesses"],
            "total_kbytes": values["Total kBytes"],
            "uptime_secs": values["Uptime"],
            "busy_workers": values["BusyWorkers"],
            "idle_workers": values["IdleWorkers"],
            "ts": _now(),
        }

    def read_apache_log(self, log: str = "access",
                        limit: int = 50) -> list[dict]:
        """Tail the newest access or error log (co-located files).

        Best-effort parse into {ts, severity, message}; access-log lines
        are kept with severity "INFO" and ts None, as are error-log lines
        that do not match the ``[ts] [module:severity]`` prefix.
        Requires ``log_dir`` (or the APACHE_LOG_DIR env var).
        """
        self._require_connected()
        log_dir = self._log_dir or os.environ.get("APACHE_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read_apache_log needs log_dir or the "
                "APACHE_LOG_DIR env var"
            )
        if log == "access":
            path = self._newest_log_file(
                log_dir, ("access_log*", "access.log*"))
            parse = self._parse_access_line
        elif log == "error":
            path = self._newest_log_file(
                log_dir, ("error_log*", "error.log*"))
            parse = self._parse_error_line
        else:
            raise ConnectorError(
                f"{self.name}: unknown log {log!r}; expected 'access' or "
                "'error'"
            )
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            raise ConnectorError(
                f"{self.name}: cannot read log {path!r}: {exc}"
            ) from exc
        return [parse(line)
                for line in lines[-max(1, min(int(limit), _MAX_LOG_LINES)):]]

    def _newest_log_file(self, log_dir: str, patterns: tuple[str, ...]) -> str:
        candidates: list[str] = []
        for pattern in patterns:
            candidates.extend(glob.glob(os.path.join(log_dir, pattern)))
        if not candidates:
            raise ConnectorError(
                f"{self.name}: no log files matching "
                f"{' / '.join(patterns)} found in {log_dir!r}"
            )
        return max(candidates, key=os.path.getmtime)

    @staticmethod
    def _parse_access_line(line: str) -> dict:
        return {"ts": None, "severity": "INFO", "message": line}

    @staticmethod
    def _parse_error_line(line: str) -> dict:
        m = _ERROR_LINE.match(line)
        if not m:
            return {"ts": None, "severity": "INFO", "message": line}
        token = m.group(2).strip()
        # Token is usually "module:severity" (e.g. "mpm_event:error").
        severity = token.rsplit(":", 1)[-1].strip().upper() or "INFO"
        return {
            "ts": m.group(1).strip() or None,
            "severity": severity,
            "message": m.group(3).strip(),
        }

    # ------------------------------------------------- privileged actions

    def reload_apache(self) -> dict:
        """PRIVILEGED: graceful reload via ``apachectl graceful``.

        Local execution as the connector's OS user; approval-gated
        upstream. Refuses with ConnectorError when ``apachectl`` is not
        on PATH (or when it exits nonzero / times out).
        """
        self._require_connected()
        self._reloader.reload()  # raises ConnectorError on any failure
        return {
            "state": "reloaded",
            "ts": _now(),
        }


register_connector(ApacheConnector.SPEC, ApacheConnector)
