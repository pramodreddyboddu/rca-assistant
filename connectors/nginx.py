"""Real Nginx connector: stub_status reads, log tailing, privileged reload.

TRANSPORT
---------
Status reads go over plain HTTP (stdlib ``urllib``) to the nginx
``stub_status`` page (``http://host:port/<status_path>``, default
``nginx_status``), which the stub_status module serves as plain text.
The guarded boundary is the internal ``_UrllibTransport`` (``get_text``):
it wraps every urllib/socket failure into ``ConnectorError``, so an
unreachable server never surfaces a raw exception. Tests replace
``self._transport`` with a fake object exposing the same ``get_text``
method (see tests/test_nginx.py).

Log tailing reads co-located files (``access.log`` / ``error.log``) from
the local disk: the connector is assumed to run on (or share a volume
with) the nginx host. Remote log access needs a log shipper, which is
out of scope for this connector.

The privileged ``reload_nginx`` runs ``nginx -s reload`` via
``subprocess`` -- fixed argv, ``shell=False``, the binary resolved with
``shutil.which``, a timeout, and every failure wrapped in
``ConnectorError``. It executes as the connector process's OS user.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_nginx_status() -> dict: host, port, active_connections, accepts,
  handled, requests, reading, writing, waiting, ts. Parsed from the
  stub_status page; raises ConnectorError when the page is unreachable
  or is not a valid stub_status response.
- read_nginx_log(log="access", limit=50) -> list of {ts, severity,
  message}. ``log`` is "access" (access.log) or "error" (error.log);
  co-located files under ``log_dir`` (or the NGINX_LOG_DIR env var).
  error.log lines are parsed into ts/severity/message; access.log lines
  (combined format) are kept with severity "INFO" and ts None when
  unparseable.
- reload_nginx() -> PRIVILEGED dict: state ("reloaded"), ts. Runs
  ``nginx -s reload`` locally as the connector's OS user; refuses when
  the nginx binary is missing. Approval-gated upstream (Gateway); no
  separate credential pair applies.

CREDENTIALS
-----------
No credentials are needed for the stub_status page by default. For
locked-down status pages this connector supports OPTIONAL HTTP Basic
auth: a credential-provider callable (``credential_provider``) or an
env-var pair (``user_env`` / ``password_env``). Auth is attempted only
when credentials resolve; anything unresolvable (no provider, unset or
empty env vars, empty provider result) is treated as ANONYMOUS access --
never an error. Secrets resolve per request via a local and are never
stored on the instance, never appear in ``repr``, exceptions, or logs.
The privileged ``reload_nginx`` uses NO credential pair: it runs as the
connector's OS user and is approval-gated upstream.

STATUS (honest)
---------------
Fake-transport tested only (tests/test_nginx.py): canned stub_status
text, temp log files, and a fake reload executor -- no live nginx
touched. Live validation against a real nginx happens in a customer
pilot -- see docs/REAL_CONNECTOR_READINESS.md. ``read_nginx_log`` tails
files on the local disk and ``reload_nginx`` execs the local nginx
binary: both assume the connector runs co-located with nginx.
"""

from __future__ import annotations

import base64
import os
import re
import shutil
import subprocess
import urllib.error  # noqa: F401 -- re-exported for tests
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
)

# "Active connections: 291"
_ACTIVE_LINE = re.compile(r"Active\s+connections:\s*(\d+)", re.IGNORECASE)
# "server accepts handled requests" (header for the three counters)
_TRIPLE_HEADER = re.compile(
    r"^\s*server\s+accepts\s+handled\s+requests\s*$", re.IGNORECASE)
# "Reading: 6 Writing: 179 Waiting: 106"
_RWW_LINE = re.compile(
    r"Reading:\s*(\d+)\s+Writing:\s*(\d+)\s+Waiting:\s*(\d+)",
    re.IGNORECASE,
)
# nginx error log: "2026/09/21 14:33:20 [error] 1234#5678: *9 connect() failed ..."
_ERROR_LINE = re.compile(
    r"^(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s+\[(\w+)\]\s*(.*)$"
)

_MAX_LOG_LINES = 200


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_stub_status(text: str) -> dict:
    """Parse an nginx stub_status page into its counters.

    Raises ConnectorError when the text is not a valid stub_status
    response (missing any of the four lines / counter groups).
    """
    active = accepts = handled = requests = None
    reading = writing = waiting = None
    expect_triple = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        m = _ACTIVE_LINE.search(line)
        if m:
            active = int(m.group(1))
            continue
        if _TRIPLE_HEADER.match(line):
            expect_triple = True
            continue
        if expect_triple:
            nums = re.findall(r"\d+", line)
            if len(nums) >= 3:
                accepts, handled, requests = (int(n) for n in nums[:3])
            expect_triple = False
            continue
        m = _RWW_LINE.search(line)
        if m:
            reading, writing, waiting = (int(g) for g in m.groups())
    missing = [name for name, value in (
        ("Active connections", active),
        ("accepts", accepts),
        ("handled", handled),
        ("requests", requests),
        ("Reading", reading),
        ("Writing", writing),
        ("Waiting", waiting),
    ) if value is None]
    if missing:
        raise ConnectorError(
            "nginx: stub_status response is missing "
            f"{', '.join(missing)}; not a valid stub_status page"
        )
    return {
        "active_connections": active,
        "accepts": accepts,
        "handled": handled,
        "requests": requests,
        "reading": reading,
        "writing": writing,
        "waiting": waiting,
    }


class _UrllibTransport:
    """Guarded HTTP boundary for the Nginx connector.

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
                f"nginx: GET {url} failed: {exc}"
            ) from exc


class NginxConnector(Connector):
    """Live Nginx diagnostics via the stub_status page + local logs."""

    name = "nginx"

    SPEC = ConnectorSpec(
        name="nginx",
        display_name="Nginx (stub_status HTTP page)",
        description="Connection/counter reads from the nginx stub_status "
        "page (plain HTTP, optional Basic auth for locked-down pages); "
        "best-effort local tail of access.log / error.log; approval-gated "
        "local 'nginx -s reload' (runs as the connector's OS user).",
        required_config=("host",),
        optional_config=(
            "port", "status_path", "log_dir", "timeout",
            "user_env", "password_env",
        ),
        credential_refs=("NGINX_STATUS_USER", "NGINX_STATUS_PASSWORD"),
        notes="No credentials needed by default; Basic auth is optional "
        "and anonymous access is used whenever credentials do not "
        "resolve. reload_nginx uses no credential pair: it execs the "
        "local nginx binary as the connector's OS user and is "
        "approval-gated upstream. Validated against a fake transport "
        "only; live validation happens in a customer pilot. Log tailing "
        "and reload assume co-located nginx (same host or shared volume).",
    )

    def __init__(
        self,
        host: str,
        port: int = 80,
        *,
        status_path: str = "nginx_status",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str | None = None,
        password_env: str | None = None,
        log_dir: str | None = None,
        timeout: int = 10,
        reloader: Callable[[], None] | None = None,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._status_path = status_path.strip("/")
        self._status_url = (
            f"http://{host}:{int(port)}/{self._status_path}"
        )
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._log_dir = log_dir
        self._timeout = timeout
        self._reloader = reloader  # test-only override for the exec path
        self._transport: Any = _UrllibTransport(timeout=timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_nginx_status",
            "read_nginx_log",
            "reload_nginx",
        }

    def connect(self) -> None:
        """Probe the stub_status page; fail closed when unreachable."""
        auth = self._resolve_auth()
        try:
            text = self._transport.get_text(self._status_url, auth)
            # Parse here so a 200 page that is NOT stub_status fails fast.
            _parse_stub_status(text)
        except ConnectorError:
            raise
        except Exception as exc:
            # The real transport wraps everything; this catch-all keeps
            # the guarantee for any transport implementation.
            raise ConnectorError(
                f"nginx: connect to {self._status_url} failed: {exc}"
            ) from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless HTTP); safe to call any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"NginxConnector(host={self._host!r}, port={self._port!r}, "
            f"status_path={self._status_path!r}, "
            f"log_dir={self._log_dir!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_auth(self) -> tuple[str, str] | None:
        """Resolve the OPTIONAL Basic-auth pair for the status page.

        Returns None (anonymous access) whenever credentials do not
        resolve: no provider configured, provider returns an empty
        user/password, or the env pair is unset/empty. Secrets are only
        ever held in this local; never stored on the instance.
        """
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if user and password:
                return user, password
            return None
        if self._user_env and self._password_env:
            user = os.environ.get(self._user_env)
            password = os.environ.get(self._password_env)
            if user and password:
                return user, password
        return None

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    # ------------------------------------------------- reads

    def get_nginx_status(self) -> dict:
        """Fetch and parse the nginx stub_status page.

        Returns {host, port, active_connections, accepts, handled,
        requests, reading, writing, waiting, ts}. Raises ConnectorError
        when the page is unreachable or is not a valid stub_status
        response.
        """
        self._require_connected()
        text = self._transport.get_text(self._status_url,
                                        self._resolve_auth())
        parsed = _parse_stub_status(text)
        return {
            "host": self._host,
            "port": self._port,
            **parsed,
            "ts": _now(),
        }

    def read_nginx_log(self, log: str = "access", limit: int = 50) -> list[dict]:
        """Tail access.log or error.log (co-located files).

        Returns a list of {ts, severity, message}. error.log lines parse
        into ts/severity/message; access.log lines (combined format) do
        not match the error format and are kept with severity "INFO" and
        ts None rather than dropped. Requires ``log_dir`` (or the
        NGINX_LOG_DIR env var).
        """
        self._require_connected()
        log_dir = self._log_dir or os.environ.get("NGINX_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read_nginx_log needs log_dir or the "
                "NGINX_LOG_DIR env var"
            )
        if log == "access":
            filename = "access.log"
        elif log == "error":
            filename = "error.log"
        else:
            raise ConnectorError(
                f"{self.name}: unknown log {log!r}; expected 'access' or "
                "'error'"
            )
        path = os.path.join(log_dir, filename)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            raise ConnectorError(
                f"{self.name}: cannot read log {path!r}: {exc}"
            ) from exc
        entries: list[dict] = []
        for line in lines[-max(1, min(int(limit), _MAX_LOG_LINES)):]:
            m = _ERROR_LINE.match(line)
            if m:
                entries.append({
                    "ts": m.group(1),
                    "severity": m.group(2).upper(),
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

    def reload_nginx(self) -> dict:
        """PRIVILEGED: reload nginx via ``nginx -s reload``.

        Runs locally as the connector process's OS user with a fixed
        argv, no shell, the binary resolved via ``shutil.which``, and a
        timeout; refuses when the nginx binary is missing. Approval-gated
        upstream (Gateway): this connector holds no separate credential
        pair for the action. Returns {state: "reloaded", ts}.
        """
        self._require_connected()
        if self._reloader is not None:
            # Test-only override: skips the subprocess entirely.
            try:
                self._reloader()
            except ConnectorError:
                raise
            except Exception as exc:
                raise ConnectorError(
                    f"{self.name}: reload failed: {exc}"
                ) from exc
            return {"state": "reloaded", "ts": _now()}
        binary = shutil.which("nginx")
        if not binary:
            raise ConnectorError(
                f"{self.name}: cannot reload: the 'nginx' binary was not "
                "found on PATH; reload is only possible on a host where "
                "nginx is installed (co-located with the connector)"
            )
        try:
            proc = subprocess.run(
                [binary, "-s", "reload"],
                capture_output=True,
                text=True,
                timeout=self._timeout,
                check=False,
            )
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: reload failed: {exc}"
            ) from exc
        if proc.returncode != 0:
            raise ConnectorError(
                f"{self.name}: reload failed (exit {proc.returncode}): "
                f"{(proc.stderr or '').strip()}"
            )
        return {"state": "reloaded", "ts": _now()}


register_connector(NginxConnector.SPEC, NginxConnector)
