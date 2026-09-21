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
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)
from connectors.recording import Recorder, RecordingError

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

_LISTENER_STATUS_NAMES = {
    # Live-grounded against MQ 10.0.0.5: a running listener reports 2.
    0: "STOPPED",
    2: "RUNNING",
}

# MQRC_NO_MSG_AVAILABLE / MQRC_UNKNOWN_OBJECT_NAME — referenced by name
# with numeric fallbacks so a partial driver still behaves.
_NO_MSG_AVAILABLE = 2033
_UNKNOWN_OBJECT_NAME = 2085  # MQRC_UNKNOWN_OBJECT_NAME
_CHANNEL_NOT_ACTIVE = 4064  # MQRCCF_CHANNEL_NOT_ACTIVE

# "09/20/26 10:15:30 - Process(...) User(mqm) Program(amqzmuc0)" record start.
_AMQERR_RECORD_START = re.compile(r"^\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+-")
_AMQERR_CODE = re.compile(r"\b(AMQ\d{4}[A-Z]?)\b")


# ------------------------------------------------------------- record/replay

_RECORDER = Recorder("ibmmq", default_mode="passthrough")
"""Record/replay for the PCF read paths.

Production default is passthrough: the driver is always hit and behavior
is unchanged. ``RCA_RECORD_MODE=replay`` serves the fixtures recorded
against the live QM1 (``tests/fixtures/recorded/ibmmq/``) with zero driver
installed; ``RCA_RECORD_MODE=live`` re-records them. Only reads go through
the recorder — privileged actions never do.
"""

# IBM-architected PCF attribute ids used by the read-path shaping below.
# Values mirror the live driver's headers (live-grounded against MQ 10.0.0.5;
# same numbers as the fake in tests/test_ibmmq.py); they let replayed
# fixtures be shaped with no driver installed.
_FALLBACK_CMQC = SimpleNamespace(
    MQIA_CURRENT_Q_DEPTH=3,
    MQIA_MAX_Q_DEPTH=15,
    MQIA_MAX_MSG_LENGTH=13,
)
_FALLBACK_CMQCFC = SimpleNamespace(
    MQIACH_CHANNEL_STATUS=1527,
    MQIACH_LISTENER_STATUS=1599,
    MQIACH_SSL_CERT_VALID=1528,
    MQCACH_SSL_CERT_EXPIRY=3564,
    MQCACH_SSL_CIPHER_SPEC=3544,
    MQCACH_SSL_CERT_LABEL=3562,
    MQCACH_SSL_PEER_NAME=3545,
)


def _pcf_attrs() -> tuple[Any, Any]:
    """(CMQC, CMQCFC)-shaped namespaces for read-path shaping.

    The live driver's constants when installed; the IBM-verified fallback
    numbers when replaying fixtures without a driver.
    """
    pymqi = _pymqi_silent()
    if pymqi is not None:
        return pymqi.CMQC, pymqi.CMQCFC
    return _FALLBACK_CMQC, _FALLBACK_CMQCFC


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


def _cmqcfc() -> Any:
    """PCF channel/listener constants (``pymqi.CMQCFC``).

    The real driver keeps ``MQCACH_*``/``MQIACH_*`` constants in CMQCFC,
    not CMQC; using CMQC raises AttributeError against a live driver
    (found by live-grounding against pymqi 1.12).
    """
    return _pymqi().CMQCFC


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
        """Open `mgr` against the queue manager (TLS when configured).

        Uses the pymqi >= 1.12 client API: ``connect_tcp_client`` for plain
        TCP (it fills in ChannelName/ConnectionName on the CD itself) and
        ``connect_with_options`` with CD/SCO for TLS. Older pymqi versions
        exposed ``connect(qmgr, channel, conn_info, ...)``; that signature
        no longer exists, so this method must not use it.
        """
        pymqi = _pymqi()
        cd = pymqi.CD()
        cd.ChannelName = self._channel
        cd.ConnectionName = f"{self._host}({self._port})"
        if self._tls_cipher or self._key_repo:
            cd.SSLCipherSpec = self._tls_cipher or ""
            kwargs: dict[str, Any] = {"cd": cd, "user": user,
                                      "password": password}
            if self._key_repo:
                sco = pymqi.SCO()
                sco.KeyRepository = self._key_repo
                kwargs["sco"] = sco
            mgr.connect_with_options(self._qmgr, **kwargs)
        else:
            mgr.connect_tcp_client(self._qmgr, cd, self._channel,
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
        """Run the privileged call under the separate admin identity.

        The read session is disconnected first and re-established
        afterwards: the driver/client stack allows only one live
        connection per thread, and a second concurrent MQCONNX comes back
        MQRC_ALREADY_CONNECTED (verified live against pymqi 1.12 with the
        MQ 10.0.0.5 client libraries). The read session is always restored,
        even when the privileged connect or the privileged call fails; if
        restoration itself fails the connector is left disconnected rather
        than holding a half-open session.
        """
        pymqi = _pymqi()
        user, password = self._resolve_privileged_credentials()
        read_mgr, self._mgr = self._mgr, None
        if read_mgr is not None:
            with contextlib.suppress(Exception):
                read_mgr.disconnect()
        admin = pymqi.QueueManager(None)
        try:
            self._connect_manager(admin, user, password)
        except Exception as exc:
            self._restore_read_session(read_mgr)
            raise self._wrap("privileged connect", exc) from exc
        self._mgr = admin
        try:
            yield
        finally:
            with contextlib.suppress(Exception):
                admin.disconnect()
            self._restore_read_session(read_mgr)

    def _restore_read_session(self, read_mgr: Any) -> None:
        """Best-effort re-establishment of the read session (never raises)."""
        if read_mgr is None:
            self._mgr = None
            return
        try:
            user, password = self._resolve_read_credentials()
            self._connect_manager(read_mgr, user, password)
        except Exception:
            self._mgr = None
        else:
            self._mgr = read_mgr

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
        """Current depth and MAXDEPTH of a queue via PCF inquire.

        The PCF call runs inside the recorder: live/passthrough hit the
        driver, replay serves the recorded fixture.
        """
        self._check_qmgr(qmgr)

        def _live() -> Any:
            pymqi = _pymqi()
            cmqc = pymqi.CMQC
            try:
                return self._pcf().MQCMD_INQUIRE_Q({
                    cmqc.MQCA_Q_NAME: queue,
                    cmqc.MQIA_Q_TYPE: cmqc.MQQT_LOCAL,
                })
            except Exception as exc:
                raise self._wrap(f"get_queue_depth({queue!r})", exc) from exc

        resp = _RECORDER.call(
            "inquire_q",
            {"op": "MQCMD_INQUIRE_Q", "qmgr": qmgr, "queue": queue},
            _live,
        )
        cmqc, _ = _pcf_attrs()
        row = resp[0]
        return {
            "qmgr": qmgr,
            "queue": queue,
            "depth": int(row[cmqc.MQIA_CURRENT_Q_DEPTH]),
            "max_depth": int(row[cmqc.MQIA_MAX_Q_DEPTH]),
            "ts": _now(),
        }

    def get_channel_status(self, qmgr: str, channel: str) -> dict:
        """Current status of a channel via PCF inquire channel status."""
        self._check_qmgr(qmgr)

        def _live() -> Any:
            cfc = _cmqcfc()
            try:
                return self._pcf().MQCMD_INQUIRE_CHANNEL_STATUS({
                    cfc.MQCACH_CHANNEL_NAME: channel,
                })
            except Exception as exc:
                raise self._wrap(f"get_channel_status({channel!r})",
                                 exc) from exc

        resp = _RECORDER.call(
            "inquire_channel_status",
            {"op": "MQCMD_INQUIRE_CHANNEL_STATUS",
             "qmgr": qmgr, "channel": channel},
            _live,
        )
        _, cfc = _pcf_attrs()
        code = int(resp[0][cfc.MQIACH_CHANNEL_STATUS])
        return {
            "qmgr": qmgr,
            "channel": channel,
            "status": _CHANNEL_STATUS_NAMES.get(code, f"UNKNOWN({code})"),
            "ts": _now(),
        }

    def get_listener_status(self, name: str) -> dict:
        """Current status of a listener via PCF inquire listener status.

        A defined-but-stopped listener has no status instance, so PCF
        answers MQRC_UNKNOWN_OBJECT_NAME; an INQUIRE_LISTENER then tells a
        genuinely unknown listener (ConnectorError) apart from a stopped
        one (reported as STOPPED, matching the sim estate). The
        unknown-object outcome is recorded as ``None`` so the fallback
        path replays faithfully.
        """
        def _live_status() -> Any:
            cfc = _cmqcfc()
            try:
                return self._pcf().MQCMD_INQUIRE_LISTENER_STATUS({
                    cfc.MQCACH_LISTENER_NAME: name,
                })
            except Exception as exc:
                if _is_unknown_object(exc):
                    return None  # no status instance; recordable outcome
                raise self._wrap(f"get_listener_status({name!r})",
                                 exc) from exc

        resp = _RECORDER.call(
            "inquire_listener_status",
            {"op": "MQCMD_INQUIRE_LISTENER_STATUS", "listener": name},
            _live_status,
        )
        if resp is None:
            def _live_define() -> Any:
                cfc = _cmqcfc()
                try:
                    return self._pcf().MQCMD_INQUIRE_LISTENER({
                        cfc.MQCACH_LISTENER_NAME: name,
                    })
                except Exception as exc2:
                    raise self._wrap(f"get_listener_status({name!r})",
                                     exc2) from exc2

            _RECORDER.call(
                "inquire_listener",
                {"op": "MQCMD_INQUIRE_LISTENER", "listener": name},
                _live_define,
            )
            return {"name": name, "status": "STOPPED", "ts": _now()}
        _, cfc = _pcf_attrs()
        code = int(resp[0][cfc.MQIACH_LISTENER_STATUS])
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

        def _live() -> Any:
            cfc = _cmqcfc()
            try:
                return self._pcf().MQCMD_INQUIRE_CHANNEL_STATUS({
                    cfc.MQCACH_CHANNEL_NAME: channel,
                })
            except Exception as exc:
                raise self._wrap(f"get_cert_status({channel!r})", exc) from exc

        resp = _RECORDER.call(
            "inquire_channel_status",
            {"op": "MQCMD_INQUIRE_CHANNEL_STATUS",
             "qmgr": qmgr, "channel": channel},
            _live,
        )
        _, cfc = _pcf_attrs()
        row = resp[0]

        def _get(attr: str, default: Any = None) -> Any:
            key = getattr(cfc, attr, None)
            return row.get(key, default) if key is not None else default

        return {
            "qmgr": qmgr,
            "channel": channel,
            "valid": _get("MQIACH_SSL_CERT_VALID", None),
            "expires": _get("MQCACH_SSL_CERT_EXPIRY", None),
            "cipher": _text(_get("MQCACH_SSL_CIPHER_SPEC")),
            "cert_label": _text(_get("MQCACH_SSL_CERT_LABEL")),
            "ssl_peer": _text(_get("MQCACH_SSL_PEER_NAME")),
            "ts": _now(),
        }

    def get_config(self, qmgr: str, object_type: str, name: str) -> dict:
        """Lightweight config view for a queue or channel via PCF inquire."""
        self._check_qmgr(qmgr)
        cmqc, cfc = _pcf_attrs()
        try:
            if object_type == "queue":
                def _live_q() -> Any:
                    drv = _pymqi()
                    dcc = drv.CMQC
                    return self._pcf().MQCMD_INQUIRE_Q({
                        dcc.MQCA_Q_NAME: name,
                        dcc.MQIA_Q_TYPE: dcc.MQQT_LOCAL,
                    })

                resp = _RECORDER.call(
                    "inquire_q",
                    {"op": "MQCMD_INQUIRE_Q", "qmgr": qmgr, "queue": name},
                    _live_q,
                )
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
                def _live_c() -> Any:
                    dcf = _cmqcfc()
                    return self._pcf().MQCMD_INQUIRE_CHANNEL_STATUS({
                        dcf.MQCACH_CHANNEL_NAME: name,
                    })

                resp = _RECORDER.call(
                    "inquire_channel_status",
                    {"op": "MQCMD_INQUIRE_CHANNEL_STATUS",
                     "qmgr": qmgr, "channel": name},
                    _live_c,
                )
                code = int(resp[0][cfc.MQIACH_CHANNEL_STATUS])
                return {
                    "qmgr": qmgr,
                    "object_type": "channel",
                    "name": name,
                    "status": _CHANNEL_STATUS_NAMES.get(code, f"UNKNOWN({code})"),
                    "ts": _now(),
                }
        except RecordingError:
            raise
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

        def _live() -> list[str]:
            try:
                with open(path, "r", encoding="utf-8",
                          errors="replace") as fh:
                    return fh.read().splitlines()
            except OSError as exc:
                raise ConnectorError(
                    f"{self.name}: cannot read error log {path!r}: {exc}"
                ) from exc

        lines = _RECORDER.call(
            "read_error_log",
            {"op": "read_error_log", "qmgr": qmgr},
            _live,
        )
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

    def restart_channel(
        self,
        qmgr: str,
        channel: str,
        *,
        settle_timeout: float = 10.0,
        settle_interval: float = 0.5,
    ) -> dict:
        """PRIVILEGED: stop then start a channel via PCF (admin identity).

        Stopping an already-inactive channel answers
        MQRCCF_CHANNEL_NOT_ACTIVE; that is tolerated so restart also works
        from a stopped channel (live-grounded against MQ 10.0.0.5).

        The start command only ASKS the command server to start the
        channel; the channel then settles asynchronously through
        BINDING/STARTING/RETRYING before it is actually RUNNING. This
        method therefore polls the real channel status and reports the
        OBSERVED status — never a hardcoded "RUNNING". If the channel has
        not reached RUNNING when the settle timeout expires, the observed
        status (e.g. BINDING) is reported honestly with a note.
        """
        self._check_qmgr(qmgr)
        cfc = _cmqcfc()
        previous = self.get_channel_status(qmgr, channel)["status"]
        with self._privileged_session():
            try:
                pcf = self._pcf()
                try:
                    pcf.MQCMD_STOP_CHANNEL({cfc.MQCACH_CHANNEL_NAME: channel})
                except Exception as stop_exc:
                    if not _reason_is(stop_exc, _CHANNEL_NOT_ACTIVE):
                        raise
                pcf.MQCMD_START_CHANNEL({cfc.MQCACH_CHANNEL_NAME: channel})
            except Exception as exc:
                raise self._wrap(f"restart_channel({channel!r})", exc) from exc
        # Read the real state back: poll until the channel is actually
        # RUNNING or the settle timeout expires.
        observed, polls = self._poll_channel_status(
            qmgr, channel, settle_timeout, settle_interval
        )
        result = {
            "qmgr": qmgr,
            "channel": channel,
            "previous_status": previous,
            "status": observed,
            "polls": polls,
            "ts": _now(),
        }
        if observed != "RUNNING":
            result["note"] = (
                f"channel settled at {observed} after "
                f"{settle_timeout:g}s; not claimed RUNNING"
            )
        return result

    def _poll_channel_status(
        self, qmgr: str, channel: str, timeout: float, interval: float
    ) -> tuple[str, int]:
        """Poll ``get_channel_status`` until RUNNING or ``timeout``.

        Returns (observed_status, polls). The status is read back from the
        queue manager on every poll, so the caller can report the truth.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        polls = 0
        observed = ""
        while True:
            polls += 1
            observed = self.get_channel_status(qmgr, channel)["status"]
            if observed == "RUNNING" or time.monotonic() >= deadline:
                return observed, polls
            time.sleep(max(0.0, interval))

    def update_queue_config(self, qmgr: str, queue: str, max_depth: int) -> dict:
        """PRIVILEGED: change a queue's MAXDEPTH via PCF change (admin identity).

        The command server requires QType alongside QName, so the queue is
        inquired first to learn its type (live-grounded: without QType the
        server answers MQRCCF_CFIN_PARM_ID_ERROR).
        """
        self._check_qmgr(qmgr)
        pymqi = _pymqi()
        cmqc = pymqi.CMQC
        old = self.get_queue_depth(qmgr, queue)["max_depth"]
        with self._privileged_session():
            try:
                pcf = self._pcf()
                current = pcf.MQCMD_INQUIRE_Q({cmqc.MQCA_Q_NAME: queue})
                qtype = int(current[0][cmqc.MQIA_Q_TYPE])
                pcf.MQCMD_CHANGE_Q({
                    cmqc.MQCA_Q_NAME: queue,
                    cmqc.MQIA_Q_TYPE: qtype,
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
        """PRIVILEGED: start a listener via PCF (admin identity).

        The PCF command is MQCMD_START_CHANNEL_LISTENER (live-grounded:
        there is no MQCMD_START_LISTENER in the MQ PCF interface). Starting
        an already-running listener is a no-op success (the live server
        answers MQRCCF_LISTENER_RUNNING, which the sim treats as RUNNING).
        """
        cfc = _cmqcfc()
        previous = self.get_listener_status(name)["status"]
        if previous == "RUNNING":
            return {
                "name": name,
                "previous_status": previous,
                "status": "RUNNING",
                "ts": _now(),
            }
        with self._privileged_session():
            try:
                self._pcf().MQCMD_START_CHANNEL_LISTENER({
                    cfc.MQCACH_LISTENER_NAME: name,
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


def _reason_is(exc: Exception, code: int) -> bool:
    """True when a driver exception carries the given MQ reason code."""
    return getattr(exc, "reason", None) == code


def _is_unknown_object(exc: Exception) -> bool:
    """True when a driver exception is MQRC_UNKNOWN_OBJECT_NAME (2085)."""
    pymqi = _pymqi_silent()
    if pymqi is None:
        return False
    mqmi = getattr(pymqi, "MQMIError", None)
    if mqmi is not None and isinstance(exc, mqmi):
        unknown = getattr(getattr(pymqi, "CMQC", None),
                          "MQRC_UNKNOWN_OBJECT_NAME", _UNKNOWN_OBJECT_NAME)
        return getattr(exc, "reason", None) == unknown
    return False


def _text(value: Any) -> Any:
    """Decode a driver byte-string to stripped text; blank becomes None."""
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", errors="replace").strip()
        return value or None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


register_connector(IBMQConnector.SPEC, IBMQConnector)
