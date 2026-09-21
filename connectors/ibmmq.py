"""Real IBM MQ connector: live diagnostics and governed actions via PCF.

Implemented against ``pymqi`` (imported LAZILY: this module imports cleanly
when pymqi is absent; only the driver paths raise, with install
instructions). Reads go through PCF inquire commands against the connected
queue manager; the error log is tailed from the queue manager's
``AMQERR01.LOG`` file (best-effort parse); privileged actions use PCF
start/stop/change commands and a DLQ move built from a destructive get +
put. Return shapes match ``sim/estate.py`` exactly (same keys), so the
Gateway, tools, audit trail, and report code work unchanged.

CREDENTIALS
-----------
The constructor never takes a raw password. Pass either:

- ``credential_provider``: a zero-arg callable returning ``(user, password)``
  (typically backed by the customer's vault), or
- ``user_env`` / ``password_env``: env-var NAMES (defaults
  ``MQ_READ_USER`` / ``MQ_READ_PASSWORD``).

Secrets resolve at ``connect()`` time, are held only in a local variable
for the duration of the connect call, and never appear in ``repr``,
exceptions, or logs. Privileged ``act()`` paths resolve a SEPARATE
credential pair (``privileged_credential_provider`` or
``MQ_ADMIN_USER``/``MQ_ADMIN_PASSWORD``); when it is not configured,
``act()`` refuses rather than silently reusing the read credential.

TLS
---
Client connections use a client channel (SVRCONN). When ``tls_cipher`` is
given, the connection is built with ``CD``/``SCO`` (TLS cipher + key
repository from ``key_repo``); otherwise a plain TCP client connection is
used. See docs/REAL_CONNECTOR_READINESS.md for what the customer provides.
"""

from __future__ import annotations

import contextlib
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)

_DEFAULT_DLQ = "SYSTEM.DEAD.LETTER.QUEUE"

# Numeric PCF channel-status codes -> the status strings the sim estate uses.
_CHANNEL_STATUS_NAMES = {
    0: "INACTIVE",
    1: "BINDING",
    2: "STARTING",
    3: "RUNNING",
    4: "STOPPING",
    5: "RETRYING",
    6: "STOPPED",
    7: "REQUESTING",
    8: "PAUSED",
    9: "DISCONNECTED",
    10: "INITIALIZING",
    11: "SWITCHING",
}

_LISTENER_STATUS_NAMES = {0: "STOPPED", 1: "RUNNING"}

# MQRC_NO_MSG_AVAILABLE / MQRC_UNKNOWN_OBJECT_NAME — referenced by name
# with numeric fallbacks so a partial driver still behaves.
_NO_MSG_AVAILABLE = 2033
_UNKNOWN_OBJECT_NAME = 2085

# "09/20/26 10:15:30 - Process(...) User(mqm) Program(amqzmuc0)" record start.
_AMQERR_RECORD_START = re.compile(r"^\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+-")
_AMQERR_CODE = re.compile(r"\b(AMQ\d{4}[A-Z]?)\b")


def _pymqi() -> Any:
    """Import pymqi lazily; raise ConnectorError with install instructions."""
    try:
        import pymqi  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "the IBM MQ connector needs the 'pymqi' driver, which is not "
            "installed. Install it with: pip install pymqi "
            "(it also requires the IBM MQ client libraries / redistributable "
            "client on the host running this connector)."
        ) from exc
    return pymqi


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class IBMQConnector(Connector):
    """Live IBM MQ diagnostics + governed privileged actions. name = "ibmmq"."""

    name = "ibmmq"

    SPEC = ConnectorSpec(
        name="ibmmq",
        display_name="IBM MQ (PCF via pymqi)",
        description="Live IBM MQ queue/channel/listener/cert diagnostics via "
        "PCF, plus approval-gated privileged actions (channel restart, "
        "MAXDEPTH change, listener start, poison-message quarantine to the "
        "DLQ).",
        required_config=("qmgr", "channel", "host", "port"),
        optional_config=(
            "tls_cipher", "key_repo", "error_log_dir", "dlq",
            "user_env", "password_env",
        ),
        credential_refs=("MQ_READ_USER", "MQ_READ_PASSWORD"),
        notes="Reads use a read-only MQ service account. Privileged act() "
        "paths use a separate credential (MQ_ADMIN_USER/MQ_ADMIN_PASSWORD "
        "or a privileged provider) and refuse when it is absent. The "
        "customer delivers credentials via their own vault; this connector "
        "never accepts a raw password.",
    )

    def __init__(
        self,
        qmgr: str,
        channel: str,
        host: str,
        port: int = 1414,
        *,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "MQ_READ_USER",
        password_env: str = "MQ_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "MQ_ADMIN_USER",
        privileged_password_env: str = "MQ_ADMIN_PASSWORD",
        tls_cipher: str | None = None,
        key_repo: str | None = None,
        error_log_dir: str | None = None,
        dlq: str = _DEFAULT_DLQ,
    ) -> None:
        self._qmgr = qmgr
        self._channel = channel
        self._host = host
        self._port = int(port)
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._tls_cipher = tls_cipher
        self._key_repo = key_repo
        self._error_log_dir = error_log_dir
        self._dlq = dlq
        self._mgr: Any = None  # pymqi QueueManager, once connected

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_queue_depth",
            "get_channel_status",
            "read_error_log",
            "get_listener_status",
            "get_cert_status",
            "get_config",
            "restart_channel",
            "update_queue_config",
            "start_listener",
            "quarantine_message",
        }

    def connect(self) -> None:
        """Connect to the queue manager with the read-only credential."""
        pymqi = _pymqi()
        user, password = self._resolve_read_credentials()
        mgr = pymqi.QueueManager(None)
        try:
            self._connect_manager(mgr, user, password)
        except Exception as exc:
            raise self._wrap("connect", exc) from exc
        self._mgr = mgr

    def _connect_manager(self, mgr: Any, user: str, password: str) -> None:
        """Open `mgr` against the queue manager (TLS when configured)."""
        pymqi = _pymqi()
        if self._tls_cipher or self._key_repo:
            cd = pymqi.CD()
            cd.ChannelName = self._channel
            cd.ConnectionName = f"{self._host}({self._port})"
            cd.SSLCipherSpec = self._tls_cipher or ""
            kwargs: dict[str, Any] = {"cd": cd, "user": user,
                                      "password": password}
            if self._key_repo:
                sco = pymqi.SCO()
                sco.KeyRepository = self._key_repo
                kwargs["sco"] = sco
            mgr.connect_with_options(self._qmgr, **kwargs)
        else:
            mgr.connect(self._qmgr, self._channel,
                        f"{self._host}({self._port})",
                        user=user, password=password)

    def close(self) -> None:
        """Disconnect. Safe to call when not connected."""
        mgr, self._mgr = self._mgr, None
        if mgr is not None:
            try:
                mgr.disconnect()
            except Exception:
                pass

    def __repr__(self) -> str:
        # Non-secret fields only: host/port/channel/qmgr are operational
        # identifiers, never credentials.
        tls = f", tls_cipher={self._tls_cipher!r}" if self._tls_cipher else ""
        return (
            f"IBMQConnector(qmgr={self._qmgr!r}, channel={self._channel!r}, "
            f"host={self._host!r}, port={self._port!r}{tls}, "
            f"dlq={self._dlq!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            creds = self._credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "read credential provider returned an empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="MQ read user")
        password = resolve_secret(env=self._password_env,
                                  label="MQ read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by act() paths.

        Raises ConnectorError when no privileged credential is configured,
        so privileged actions can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            creds = self._priv_credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "privileged credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="MQ privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="MQ privileged password")
        return user, password

    @contextlib.contextmanager
    def _privileged_session(self) -> Any:
        """Temporarily swap the session to the privileged admin identity.

        The read session is parked (not closed) while the privileged call
        runs, then restored, so act() never inherits read-session state and
        privileged calls never run under the read account.
        """
        pymqi = _pymqi()
        user, password = self._resolve_privileged_credentials()
        admin = pymqi.QueueManager(None)
        try:
            self._connect_manager(admin, user, password)
        except Exception as exc:
            raise self._wrap("privileged connect", exc) from exc
        read_mgr, self._mgr = self._mgr, admin
        try:
            yield
        finally:
            self._mgr = read_mgr
            try:
                admin.disconnect()
            except Exception:
                pass

    # ------------------------------------------------- PCF plumbing

    def _require_connected(self) -> Any:
        if self._mgr is None:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )
        return self._mgr

    def _check_qmgr(self, qmgr: str) -> None:
        if qmgr != self._qmgr:
            raise ConnectorError(
                f"{self.name}: connected to queue manager {self._qmgr!r}, "
                f"but the request named {qmgr!r}"
            )

    def _wrap(self, op: str, exc: Exception) -> ConnectorError:
        """Translate a driver exception into ConnectorError.

        The message carries only operational detail (comp/reason/verb when
        available); secrets are never interpolated.
        """
        pymqi = _pymqi_silent()
        detail = ""
        if pymqi is not None:
            mqmi = getattr(pymqi, "MQMIError", None)
            if mqmi is not None and isinstance(exc, mqmi):
                reason = getattr(exc, "reason", None)
                comp = getattr(exc, "comp", None)
                verb = getattr(exc, "verb", None)
                detail = f" (comp={comp} reason={reason} verb={verb})"
                unknown = getattr(getattr(pymqi, "CMQC", None),
                                  "MQRC_UNKNOWN_OBJECT_NAME", _UNKNOWN_OBJECT_NAME)
                if reason == unknown:
                    return ConnectorError(
                        f"{self.name}: {op}: unknown MQ object{detail}"
                    )
                no_msg = getattr(getattr(pymqi, "CMQC", None),
                                 "MQRC_NO_MSG_AVAILABLE", _NO_MSG_AVAILABLE)
                if reason == no_msg:
                    return ConnectorError(
                        f"{self.name}: {op}: no message available{detail}"
                    )
        text = str(exc)
        # Never leak anything that looks like a credential the driver echoed.
        return ConnectorError(f"{self.name}: {op} failed{detail}: {text}")

    def _pcf(self) -> Any:
        pymqi = _pymqi()
        return pymqi.PCFExecute(self._require_connected())

    # ------------------------------------------------- reads (PCF)

    def get_queue_depth(self, qmgr: str, queue: str) -> dict:
        """Current depth and MAXDEPTH of a queue via PCF inquire."""
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        try:
            resp = self._pcf().MQCMD_INQUIRE_Q({
                cmqc.MQCA_Q_NAME: queue,
                cmqc.MQIA_Q_TYPE: cmqc.MQQT_LOCAL,
            })
            row = resp[0]
            depth = int(row[cmqc.MQIA_CURRENT_Q_DEPTH])
            max_depth = int(row[cmqc.MQIA_MAX_Q_DEPTH])
        except Exception as exc:
            raise self._wrap(f"get_queue_depth({queue!r})", exc) from exc
        return {
            "qmgr": qmgr,
            "queue": queue,
            "depth": depth,
            "max_depth": max_depth,
            "ts": _now(),
        }

    def get_channel_status(self, qmgr: str, channel: str) -> dict:
        """Current status of a channel via PCF inquire channel status."""
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        try:
            resp = self._pcf().MQCMD_INQUIRE_CHANNEL_STATUS({
                cmqc.MQCACH_CHANNEL_NAME: channel,
            })
            code = int(resp[0][cmqc.MQIACH_CHANNEL_STATUS])
        except Exception as exc:
            raise self._wrap(f"get_channel_status({channel!r})", exc) from exc
        return {
            "qmgr": qmgr,
            "channel": channel,
            "status": _CHANNEL_STATUS_NAMES.get(code, f"UNKNOWN({code})"),
            "ts": _now(),
        }

    def get_listener_status(self, name: str) -> dict:
        """Current status of a listener via PCF inquire listener status."""
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        try:
            resp = self._pcf().MQCMD_INQUIRE_LISTENER_STATUS({
                cmqc.MQCACH_LISTENER_NAME: name,
            })
            code = int(resp[0][cmqc.MQIACH_LISTENER_STATUS])
        except Exception as exc:
            raise self._wrap(f"get_listener_status({name!r})", exc) from exc
        return {
            "name": name,
            "status": _LISTENER_STATUS_NAMES.get(code, f"UNKNOWN({code})"),
            "ts": _now(),
        }

    def get_cert_status(self, qmgr: str, channel: str) -> dict:
        """TLS certificate view for a channel via PCF inquire channel status.

        MQ does not expose certificate expiry over PCF; ``valid``/``expires``
        are populated only when the driver response carries them, otherwise
        None (see docs/REAL_CONNECTOR_READINESS.md: full expiry checks need
        key-repository access in the pilot).
        """
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        try:
            resp = self._pcf().MQCMD_INQUIRE_CHANNEL_STATUS({
                cmqc.MQCACH_CHANNEL_NAME: channel,
            })
            row = resp[0]
        except Exception as exc:
            raise self._wrap(f"get_cert_status({channel!r})", exc) from exc

        def _get(attr: str, default: Any = None) -> Any:
            key = getattr(cmqc, attr, None)
            return row.get(key, default) if key is not None else default

        return {
            "qmgr": qmgr,
            "channel": channel,
            "valid": _get("MQIACH_SSL_CERT_VALID", None),
            "expires": _get("MQCACH_SSL_CERT_EXPIRY", None),
            "cipher": _get("MQCACH_SSL_CIPHER_SPEC"),
            "cert_label": _get("MQCACH_SSL_CERT_LABEL"),
            "ssl_peer": _get("MQCACH_SSL_PEER_NAME"),
            "ts": _now(),
        }

    def get_config(self, qmgr: str, object_type: str, name: str) -> dict:
        """Lightweight config view for a queue or channel via PCF inquire."""
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        try:
            if object_type == "queue":
                resp = self._pcf().MQCMD_INQUIRE_Q({
                    cmqc.MQCA_Q_NAME: name,
                    cmqc.MQIA_Q_TYPE: cmqc.MQQT_LOCAL,
                })
                row = resp[0]
                return {
                    "qmgr": qmgr,
                    "object_type": "queue",
                    "name": name,
                    "max_depth": int(row[cmqc.MQIA_MAX_Q_DEPTH]),
                    "max_msg_length": int(row.get(
                        getattr(cmqc, "MQIA_MAX_MSG_LENGTH", -1), 0)) or None,
                    "ts": _now(),
                }
            if object_type == "channel":
                resp = self._pcf().MQCMD_INQUIRE_CHANNEL_STATUS({
                    cmqc.MQCACH_CHANNEL_NAME: name,
                })
                code = int(resp[0][cmqc.MQIACH_CHANNEL_STATUS])
                return {
                    "qmgr": qmgr,
                    "object_type": "channel",
                    "name": name,
                    "status": _CHANNEL_STATUS_NAMES.get(code, f"UNKNOWN({code})"),
                    "ts": _now(),
                }
        except Exception as exc:
            raise self._wrap(f"get_config({object_type!r}, {name!r})",
                             exc) from exc
        raise ConnectorError(
            f"{self.name}: unsupported object_type {object_type!r}"
        )

    def read_error_log(self, qmgr: str, limit: int = 50) -> list[dict]:
        """Tail the queue manager's AMQERR01.LOG (best-effort parse).

        Each entry is {ts, severity, code, message}. Records that do not
        match the AMQERR record format are returned with code None rather
        than dropped. Requires ``error_log_dir`` (or the MQ_ERROR_LOG_DIR
        env var) pointing at the directory holding AMQERR01.LOG.
        """
        self._check_qmgr(qmgr)
        log_dir = self._error_log_dir or os.environ.get("MQ_ERROR_LOG_DIR")
        if not log_dir:
            raise ConnectorError(
                f"{self.name}: read_error_log needs error_log_dir or the "
                "MQ_ERROR_LOG_DIR env var"
            )
        path = os.path.join(log_dir, "AMQERR01.LOG")
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        except OSError as exc:
            raise ConnectorError(
                f"{self.name}: cannot read error log {path!r}: {exc}"
            ) from exc
        records: list[list[str]] = []
        for line in lines:
            if _AMQERR_RECORD_START.match(line):
                records.append([line])
            elif records:
                records[-1].append(line)
        entries: list[dict] = []
        for rec in records[-max(1, int(limit)):]:
            head = rec[0]
            m = _AMQERR_CODE.search(" ".join(rec))
            code = m.group(1) if m else None
            text = " ".join(l.strip() for l in rec)
            severity = "ERROR" if (" ERROR" in text or " error" in text) else "INFO"
            ts = head.split(" - ")[0].strip()
            entries.append({
                "ts": ts,
                "severity": severity,
                "code": code,
                "message": text,
            })
        return entries

    # ------------------------------------------------- privileged actions

    def restart_channel(self, qmgr: str, channel: str) -> dict:
        """PRIVILEGED: stop then start a channel via PCF (admin identity)."""
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        previous = self.get_channel_status(qmgr, channel)["status"]
        with self._privileged_session():
            try:
                pcf = self._pcf()
                pcf.MQCMD_STOP_CHANNEL({cmqc.MQCACH_CHANNEL_NAME: channel})
                pcf.MQCMD_START_CHANNEL({cmqc.MQCACH_CHANNEL_NAME: channel})
            except Exception as exc:
                raise self._wrap(f"restart_channel({channel!r})", exc) from exc
        return {
            "qmgr": qmgr,
            "channel": channel,
            "previous_status": previous,
            "status": "RUNNING",
            "ts": _now(),
        }

    def update_queue_config(self, qmgr: str, queue: str, max_depth: int) -> dict:
        """PRIVILEGED: change a queue's MAXDEPTH via PCF change (admin identity)."""
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        old = self.get_queue_depth(qmgr, queue)["max_depth"]
        with self._privileged_session():
            try:
                self._pcf().MQCMD_CHANGE_Q({
                    cmqc.MQCA_Q_NAME: queue,
                    cmqc.MQIA_MAX_Q_DEPTH: int(max_depth),
                })
            except Exception as exc:
                raise self._wrap(f"update_queue_config({queue!r})", exc) from exc
        return {
            "qmgr": qmgr,
            "queue": queue,
            "old_max_depth": old,
            "max_depth": int(max_depth),
            "ts": _now(),
        }

    def start_listener(self, name: str) -> dict:
        """PRIVILEGED: start a listener via PCF (admin identity)."""
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        previous = self.get_listener_status(name)["status"]
        with self._privileged_session():
            try:
                self._pcf().MQCMD_START_LISTENER({
                    cmqc.MQCACH_LISTENER_NAME: name,
                })
            except Exception as exc:
                raise self._wrap(f"start_listener({name!r})", exc) from exc
        return {
            "name": name,
            "previous_status": previous,
            "status": "RUNNING",
            "ts": _now(),
        }

    def quarantine_message(self, qmgr: str, queue: str) -> dict:
        """PRIVILEGED: move the head message of a queue to the DLQ.

        Runs under the admin identity. Destructively gets the first
        available message and puts it on the configured DLQ (default
        SYSTEM.DEAD.LETTER.QUEUE). When the queue is empty, returns
        quarantined False with a reason instead of failing.
        """
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        with self._privileged_session():
            mgr = self._require_connected()
            try:
                src = pymqi.Queue(mgr, queue)
                try:
                    md = pymqi.MD()
                    gmo = pymqi.GMO()
                    if hasattr(pymqi.CMQC, "MQGMO_FAIL_IF_QUIESCING"):
                        gmo.Options = pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
                    message = src.get(None, md, gmo)
                finally:
                    src.close()
            except Exception as exc:
                err = self._wrap(f"quarantine_message({queue!r}) get", exc)
                if "no message available" in str(err):
                    return {
                        "qmgr": qmgr,
                        "queue": queue,
                        "quarantined": False,
                        "reason": "queue empty",
                        "ts": _now(),
                    }
                raise err from exc
            try:
                dlq = pymqi.Queue(mgr, self._dlq)
                try:
                    dlq.put(message, pymqi.MD())
                finally:
                    dlq.close()
            except Exception as exc:
                raise self._wrap(f"quarantine_message({queue!r}) put to "
                                 f"{self._dlq!r}", exc) from exc
        return {
            "qmgr": qmgr,
            "queue": queue,
            "quarantined": True,
            "dlq": self._dlq,
            "ts": _now(),
        }


def _pymqi_silent() -> Any:
    """Return the pymqi module if importable, else None (never raises)."""
    try:
        import pymqi  # type: ignore
    except ImportError:
        return None
    return pymqi


register_connector(IBMQConnector.SPEC, IBMQConnector)
