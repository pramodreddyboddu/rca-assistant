"""Real TIBCO EMS connector: queue depth and server state via the admin CLI.

TRANSPORT
---------
TIBCO EMS ships no Python driver and no HTTP admin API, so the real
client shells out to the vendor ``tibemsadmin`` CLI over stdlib
``subprocess`` -- an honest limitation, guarded hard. The guarded
boundary is the internal ``_real_executor`` (used when the caller does
not inject one): it runs a FIXED argv list only (``shell`` is never
True), with an absolute or ``shutil.which``-resolved binary, a
configurable timeout, and stdout/stderr captured. Every failure --
missing binary, exec-time OSError, timeout, non-zero exit, unparseable
probe -- is wrapped in ``ConnectorError``; stderr text is deliberately
suppressed in errors so CLI error text can never leak credentials.
Tests replace the boundary with a fake ``executor`` callable returning
canned ``show queues`` / ``show server`` output (see
tests/test_tibco_ems.py); no real EMS server is ever touched.

The CLI is invoked as::

    tibemsadmin -server tcp://<host>:<port> -user <u> -password <p> <cmd...>

with ``<cmd...>`` one of ``show server``, ``show queues``, or
``purge queue <name>``. Credentials travel inside argv (the documented
tibemsadmin auth mechanism) and are never logged, echoed into errors,
stored on the instance, or shown in ``repr``.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py -- this module must not be edited to match them,
they are the contract):
- get_ems_queues() -> dict: host, queues (list of {name,
  pending_messages, consumers, state}), ts.
- get_ems_server() -> dict: host, server, state, connections,
  version, ts.
- purge_ems_queue(queue) -> PRIVILEGED dict: queue, messages_purged,
  ts. ``messages_purged`` is None when the CLI output did not report a
  count.

CREDENTIALS
-----------
Same rules as the IBM MQ and Tomcat connectors: the constructor takes a
credential-provider callable or env-var NAMES (``EMS_READ_USER`` /
``EMS_READ_PASSWORD``), never a raw secret. Secrets resolve per request
via ``resolve_secret`` and are never stored on the instance (argv lists
are built in a local and discarded), never appear in ``repr``,
exceptions, or logs. The privileged ``purge_ems_queue`` resolves a
SEPARATE pair (``EMS_ADMIN_USER`` / ``EMS_ADMIN_PASSWORD`` or a
privileged provider) at act time and refuses when it is absent.

STATUS (honest)
---------------
Fake-executor tested only (tests/test_tibco_ems.py): canned CLI output,
no live EMS touched. Live validation against a real EMS server happens
in a customer pilot -- see docs/REAL_CONNECTOR_READINESS.md.
Deployment requirement: the ``tibemsadmin`` CLI must be installed on the
machine running this connector (TIBCO EMS client tools, EMS_HOME/bin),
either on PATH or via ``tibemsadmin_path``. The CLI connects remotely to
``tcp://host:7222``, so co-location with the EMS server is NOT needed --
only network reachability to the EMS listen port.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)

#: Default TIBCO EMS server port (tibemsadmin ``-server`` URL).
DEFAULT_EMS_PORT = 7222

#: Matches "Purged 42 messages from queue 'q'" in purge output.
_PURGE_COUNT = re.compile(r"purged?\s+(\d+)\s+messages?", re.IGNORECASE)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class CliResult:
    """One finished ``tibemsadmin`` invocation: exit code plus I/O text."""

    returncode: int
    stdout: str
    stderr: str


def _real_executor(argv: list[str], timeout: int) -> CliResult:
    """Guarded subprocess boundary for the tibemsadmin CLI.

    Fixed argv list, no shell, captured I/O. Every failure is wrapped in
    ConnectorError. Secrets travel only inside argv: error text never
    echoes argv or stderr, so a credential can never surface here.
    """
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,
        )
    except FileNotFoundError as exc:
        raise ConnectorError(
            "tibco_ems: tibemsadmin binary vanished between resolution "
            "and exec; install the TIBCO EMS client tools (EMS_HOME/bin) "
            "on the machine running this connector"
        ) from exc
    except PermissionError as exc:
        raise ConnectorError(
            "tibco_ems: tibemsadmin binary is not executable; check "
            "permissions on your EMS_HOME/bin installation"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise ConnectorError(
            f"tibco_ems: tibemsadmin timed out after {timeout}s"
        ) from exc
    except OSError as exc:
        raise ConnectorError(
            f"tibco_ems: tibemsadmin could not be started: "
            f"{type(exc).__name__}"
        ) from exc
    return CliResult(
        returncode=proc.returncode,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
    )


def _parse_show_queues(text: str) -> list[dict]:
    """Parse ``tibemsadmin show queues`` table output.

    Column-tolerant: the Pending Msgs / Consumers / State columns are
    located from the header row, so extra or shifted columns do not
    break parsing. Multiword headers ("Queue Name", "Pending Msgs")
    are normalized before tokenizing so header and data columns align
    one-to-one. Separator rows, the prompt echo, footers, and
    unparseable rows are skipped -- this parser never raises.
    """
    queues: list[dict] = []
    header_seen = False
    pending_at = consumers_at = state_at = -1

    def _find(low: list[str], *prefixes: str) -> int:
        for i, token in enumerate(low):
            if any(token.startswith(p) for p in prefixes):
                return i
        return -1

    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or set(line) <= set("-= "):
            continue  # separator row
        if not header_seen:
            normalized = re.sub(r"(?i)\bqueue\s+name\b", "QueueName", line)
            normalized = re.sub(r"(?i)\bpending\s+msgs?\b", "PendingMsgs",
                                normalized)
            low = [t.lower().rstrip(":") for t in normalized.split()]
            if not any(t.startswith("pend") for t in low):
                continue  # preamble: prompt echo, blank banners, etc.
            header_seen = True
            pending_at = _find(low, "pend")
            consumers_at = _find(low, "consumer")
            state_at = _find(low, "state")
            continue
        tokens = line.split()
        first = tokens[0].lower().rstrip(":>")
        if first in {"queue", "total", "totals", "count", "tibemsadmin"}:
            continue  # footer lines like "Queue Count: 3", prompt echoes
        if pending_at >= len(tokens):
            continue
        try:
            pending = int(tokens[pending_at].replace(",", ""))
        except ValueError:
            continue  # footers, prompts, garbage rows: skip, never crash
        consumers: int | None = None
        if 0 <= consumers_at < len(tokens):
            try:
                consumers = int(tokens[consumers_at].replace(",", ""))
            except ValueError:
                consumers = None
        state = (tokens[state_at]
                 if 0 <= state_at < len(tokens) else "unknown")
        queues.append({
            "name": tokens[0],
            "pending_messages": pending,
            "consumers": consumers,
            "state": state or "unknown",
        })
    return queues


def _parse_show_server(text: str) -> dict | None:
    """Parse ``tibemsadmin show server`` ``Key: value`` output.

    Returns the recognized fields, or None when the text does not look
    like a show-server payload at all (used by connect() to reject a
    bad probe).
    """
    info: dict[str, str] = {}
    for raw in (text or "").splitlines():
        if ":" not in raw:
            continue
        key, _, value = raw.partition(":")
        key = key.strip().lower()
        if key in ("server", "state", "connections", "version"):
            info[key] = value.strip()
    if not info.get("server") and not info.get("state"):
        return None
    return info


def _parse_purge_count(text: str) -> int | None:
    """Extract the purged-message count from purge output, if reported."""
    match = _PURGE_COUNT.search(text or "")
    return int(match.group(1)) if match else None


class TibcoEmsConnector(Connector):
    """Live TIBCO EMS diagnostics via the tibemsadmin CLI."""

    name = "tibco_ems"

    SPEC = ConnectorSpec(
        name="tibco_ems",
        display_name="TIBCO EMS (tibemsadmin CLI)",
        description="Queue depth/consumer reads and server state via the "
        "tibemsadmin admin CLI (subprocess, fixed argv, no shell); "
        "approval-gated queue purge under a separate admin credential.",
        required_config=("host",),
        optional_config=(
            "port", "tibemsadmin_path", "timeout", "user_env",
            "password_env",
        ),
        credential_refs=("EMS_READ_USER", "EMS_READ_PASSWORD"),
        notes="Reads use a read-only EMS user. purge_ems_queue uses a "
        "separate admin pair (EMS_ADMIN_USER/EMS_ADMIN_PASSWORD or a "
        "privileged provider) and refuses when it is absent. The "
        "tibemsadmin CLI must be installed where this connector runs (on "
        "PATH or via tibemsadmin_path); it connects remotely to "
        "tcp://host:7222, so no co-location with the EMS server is "
        "needed. Fake-executor tested only; live validation happens in "
        "a customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_EMS_PORT,
        *,
        tibemsadmin_path: str | None = None,
        executor: Callable[[list[str], int], CliResult] | None = None,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "EMS_READ_USER",
        password_env: str = "EMS_READ_PASSWORD",
        privileged_credential_provider: (
            Callable[[], tuple[str, str]] | None
        ) = None,
        privileged_user_env: str = "EMS_ADMIN_USER",
        privileged_password_env: str = "EMS_ADMIN_PASSWORD",
        timeout: int = 30,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._server_url = f"tcp://{host}:{int(port)}"
        self._tibemsadmin_path = tibemsadmin_path
        self._executor: Callable[[list[str], int], CliResult] = (
            executor if executor is not None else _real_executor
        )
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._timeout = int(timeout)
        self._tibemsadmin: str | None = None
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_ems_queues",
            "get_ems_server",
            "purge_ems_queue",
        }

    def connect(self) -> None:
        """Resolve the tibemsadmin binary, then probe with ``show server``.

        Fails closed: a missing binary, missing read credentials, a
        non-zero CLI exit, or an unparseable probe response all raise
        ConnectorError. Secrets never appear in the raised messages.
        """
        binary = self._resolve_binary()
        user, password = self._resolve_read_credentials()
        text = self._run(binary, ["show", "server"], user, password)
        if _parse_show_server(text) is None:
            raise ConnectorError(
                "tibco_ems: 'show server' probe returned unparseable output"
            )
        self._tibemsadmin = binary
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (stateless CLI); safe to call any time."""
        self._connected = False
        self._tibemsadmin = None

    def __repr__(self) -> str:
        # Non-secret fields only: never the users, passwords, or argv.
        return (
            f"TibcoEmsConnector(host={self._host!r}, port={self._port!r}, "
            f"server_url={self._server_url!r}, "
            f"tibemsadmin_path={self._tibemsadmin_path!r}, "
            f"timeout={self._timeout!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "tibco_ems: read credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env,
                              label="EMS read user")
        password = resolve_secret(env=self._password_env,
                                   label="EMS read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by act() paths.

        Raises ConnectorError when no privileged credential is
        configured, so the purge path can never silently reuse the read
        account.
        """
        if self._priv_credential_provider is not None:
            user, password = self._priv_credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "tibco_ems: privileged credential provider returned "
                    "an empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="EMS privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="EMS privileged password")
        return user, password

    # ------------------------------------------------- transport plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _resolve_binary(self) -> str:
        """Absolute, executable tibemsadmin path.

        Raises ConnectorError with install guidance when the binary is
        missing -- the honest deployment requirement is documented in
        the module docstring (STATUS).
        """
        if self._tibemsadmin_path is not None:
            path = os.path.abspath(self._tibemsadmin_path)
            if not (os.path.isfile(path) and os.access(path, os.X_OK)):
                raise ConnectorError(
                    "tibco_ems: tibemsadmin not found or not executable "
                    f"at {path!r}; install the TIBCO EMS client tools "
                    "(EMS_HOME/bin) on the machine running this "
                    "connector, or pass a valid tibemsadmin_path"
                )
            return path
        found = shutil.which("tibemsadmin")
        if not found:
            raise ConnectorError(
                "tibco_ems: 'tibemsadmin' not found on PATH; install the "
                "TIBCO EMS client tools (EMS_HOME/bin) on the machine "
                "running this connector, add it to PATH, or pass "
                "tibemsadmin_path explicitly"
            )
        return found

    def _run(self, binary: str, cmd: list[str],
             user: str, password: str) -> str:
        """Guarded subprocess boundary: fixed argv, no shell, captured I/O.

        The queue name / command tokens travel as single argv elements,
        so no shell injection is possible by construction. Secrets ride
        only inside argv (built in a local, discarded after the call)
        and never appear in raised errors, logs, or repr.
        """
        argv = [binary, "-server", self._server_url,
                "-user", user, "-password", password] + list(cmd)
        try:
            result = self._executor(argv, self._timeout)
        except ConnectorError:
            raise
        except Exception as exc:
            # Deliberately not str(exc): a hostile executor exception
            # could embed argv text (and the credential inside it).
            raise ConnectorError(
                "tibco_ems: tibemsadmin invocation failed: "
                f"{type(exc).__name__}"
            ) from exc
        if result.returncode != 0:
            # stderr is suppressed on purpose: CLI error text must never
            # become a channel for credential leakage.
            raise ConnectorError(
                f"tibco_ems: tibemsadmin '{' '.join(cmd)}' failed with "
                f"exit code {result.returncode} (stderr suppressed so "
                "CLI error text cannot leak credentials)"
            )
        return result.stdout or ""

    # ------------------------------------------------- reads

    def get_ems_queues(self) -> dict:
        """Queue depth snapshot: name, pending messages, consumers, state.

        Returns {host, queues: [{name, pending_messages, consumers,
        state}], ts}. Unparseable ``show queues`` rows are skipped;
        ``consumers`` is None when the column is absent or unparseable.
        """
        self._require_connected()
        user, password = self._resolve_read_credentials()
        text = self._run(self._require_binary(), ["show", "queues"],
                         user, password)
        return {
            "host": self._host,
            "queues": _parse_show_queues(text),
            "ts": _now(),
        }

    def get_ems_server(self) -> dict:
        """EMS server identity and state from ``show server``.

        Returns {host, server, state, connections, version, ts}.
        ``connections`` is None when the server does not report a
        parseable count.
        """
        self._require_connected()
        user, password = self._resolve_read_credentials()
        text = self._run(self._require_binary(), ["show", "server"],
                         user, password)
        info = _parse_show_server(text) or {}
        connections: int | None = None
        raw_connections = info.get("connections")
        if raw_connections is not None:
            try:
                connections = int(raw_connections.replace(",", ""))
            except ValueError:
                connections = None
        return {
            "host": self._host,
            "server": info.get("server"),
            "state": info.get("state"),
            "connections": connections,
            "version": info.get("version"),
            "ts": _now(),
        }

    # ------------------------------------------------- privileged actions

    def purge_ems_queue(self, queue: str) -> dict:
        """PRIVILEGED: purge all pending messages from an EMS queue.

        Runs ``purge queue <name>`` under the SEPARATE admin credential
        pair, resolved at act time; refuses (ConnectorError) when it is
        absent rather than reusing the read account, and refuses when
        the tibemsadmin binary is missing. The queue name travels as a
        single argv element (no shell), so it cannot inject commands.

        Returns {queue, messages_purged, ts}; ``messages_purged`` is
        None when the CLI output did not report a count.
        """
        self._require_connected()
        if not isinstance(queue, str) or not queue.strip():
            raise ConnectorError(
                f"{self.name}: queue must be a non-empty queue name"
            )
        user, password = self._resolve_privileged_credentials()
        binary = self._resolve_binary()
        text = self._run(binary, ["purge", "queue", queue.strip()],
                         user, password)
        return {
            "queue": queue.strip(),
            "messages_purged": _parse_purge_count(text),
            "ts": _now(),
        }

    def _require_binary(self) -> str:
        """Binary resolved at connect(); re-resolved defensively per call."""
        if self._tibemsadmin is not None:
            return self._tibemsadmin
        return self._resolve_binary()


register_connector(TibcoEmsConnector.SPEC, TibcoEmsConnector)
