"""Real Oracle Database connector: health, tablespaces, blocking, kill.

TRANSPORT
---------
Real ``oracledb`` driver in THIN mode (pure Python; no Oracle Client
libraries needed), imported LAZILY inside the ``_oracledb()`` helper:
this module imports cleanly when the driver is absent; only the driver
paths raise, with install instructions. Reads go through a single
read-credential connection opened by ``connect()``; every driver failure
is wrapped in ``ConnectorError`` with no secrets in the message. Tests
inject a fake ``oracledb`` module into ``sys.modules`` (see
tests/test_oracle_db.py); no live database is touched.

BIND VARIABLES
--------------
All SELECTs use bind variables only (``:1`` style) -- SQL text is never
built by interpolation. The one exception is ``kill_oracle_session``:
Oracle forbids bind variables in ``ALTER SYSTEM``, so the statement is
assembled from ``sid``/``serial`` only AFTER positive-int validation
(digits only can reach the SQL text). Query text returned from the
database (``sql_snippet``) is truncated to 200 chars.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_oracle_health() -> dict: host, port, service, version (v$version
  banner), open_mode (v$database), sessions_used, sessions_max,
  sessions_pct, ts. sessions_* come from v$resource_limit; sessions_max
  is None (and sessions_pct None) when the limit is UNLIMITED, and
  sessions_pct is None when the max is <= 0.
- get_oracle_tablespaces() -> dict: host, tablespaces (list of {name,
  used_pct, used_mb, max_mb}), ts. Aggregated from dba_data_files /
  dba_free_space; needs DBA view access (see GRANTS below). On ORA-00942
  raises ConnectorError with a clear grants message instead of the raw
  Oracle error.
- get_oracle_blocking() -> dict: host, blockers (list of {sid, serial,
  username, wait_seconds, sql_snippet}), ts. Sessions whose
  blocking_session is not null, joined to v$sql for the current SQL
  text; sql_snippet is whitespace-normalized and truncated to 200 chars
  ("" when the session has no current SQL).
- kill_oracle_session(sid, serial) -> PRIVILEGED dict: sid, serial,
  killed (True), ts. Runs ``ALTER SYSTEM KILL SESSION 'sid,serial'`` on a
  SEPARATE short-lived admin connection; sid/serial are validated as
  positive ints first, and the path refuses when the admin credential is
  not configured rather than reusing the read account.

CREDENTIALS
-----------
Same rules as the IBM MQ / Kafka connectors: the constructor takes a
credential-provider callable or env-var NAMES (``ORACLE_READ_USER`` /
``ORACLE_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance
(the driver holds its own connection state; the connector keeps no
password attributes), never appear in ``repr``, exceptions, or logs.
``kill_oracle_session`` resolves a SEPARATE pair (``ORACLE_ADMIN_USER`` /
``ORACLE_ADMIN_PASSWORD`` or a privileged provider) at act time and
refuses when it is absent.

GRANTS NEEDED
-------------
Read user: SELECT on v$version, v$database, v$resource_limit, v$session,
v$sql (SELECT_CATALOG_ROLE covers these). get_oracle_tablespaces needs
SELECT on the DBA views dba_data_files and dba_free_space -- a plain
read account without those grants gets the ORA-00942 guidance error.
Admin user: ALTER SYSTEM privilege (for KILL SESSION).

STATUS (honest)
---------------
Fake-driver tested only (tests/test_oracle_db.py): canned oracledb
responses, no live Oracle touched. Live validation against a real
database happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Sequence

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)

#: Query text returned from the database is truncated to this many chars.
_SQL_SNIPPET_LEN = 200


def _oracledb() -> Any:
    """Import oracledb lazily; raise ConnectorError with install instructions."""
    try:
        import oracledb  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "oracle_db: install oracledb to use the Oracle connector: "
            "pip install oracledb"
        ) from exc
    return oracledb


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _close_quietly(client: Any) -> None:
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


_HEALTH_VERSION_SQL = "SELECT banner FROM v$version"
_HEALTH_OPEN_MODE_SQL = "SELECT open_mode FROM v$database"
_HEALTH_SESSIONS_SQL = (
    "SELECT current_utilization, limit_value "
    "FROM v$resource_limit "
    "WHERE resource_name = 'sessions'"
)

_TABLESPACES_SQL = (
    "SELECT f.tablespace_name AS name, "
    "ROUND((f.total_bytes - NVL(x.free_bytes, 0)) * 100 "
    "/ NULLIF(f.max_bytes, 0), 1) AS used_pct, "
    "ROUND((f.total_bytes - NVL(x.free_bytes, 0)) / 1048576, 1) AS used_mb, "
    "ROUND(f.max_bytes / 1048576, 1) AS max_mb "
    "FROM (SELECT tablespace_name, SUM(bytes) AS total_bytes, "
    "SUM(CASE WHEN maxbytes = 0 THEN bytes ELSE maxbytes END) AS max_bytes "
    "FROM dba_data_files GROUP BY tablespace_name) f "
    "LEFT JOIN (SELECT tablespace_name, SUM(bytes) AS free_bytes "
    "FROM dba_free_space GROUP BY tablespace_name) x "
    "ON f.tablespace_name = x.tablespace_name "
    "ORDER BY f.tablespace_name"
)

_BLOCKING_SQL = (
    "SELECT s.sid AS sid, "
    "s.serial# AS serial_no, "
    "s.username AS username, "
    "NVL(s.seconds_in_wait, 0) AS wait_seconds, "
    "q.sql_text AS sql_text "
    "FROM v$session s "
    "LEFT JOIN v$sql q "
    "ON q.sql_id = s.sql_id AND q.child_number = s.sql_child_number "
    "WHERE s.blocking_session IS NOT NULL "
    "ORDER BY s.sid"
)


class OracleDbConnector(Connector):
    """Live Oracle Database diagnostics via the oracledb thin driver."""

    name = "oracle_db"

    SPEC = ConnectorSpec(
        name="oracle_db",
        display_name="Oracle Database (oracledb thin driver)",
        description="Live Oracle diagnostics via the oracledb thin driver: "
        "instance health (version, open mode, session utilization), "
        "tablespace usage, sessions blocked on locks, and "
        "approval-gated session kill. All reads use bind variables; "
        "the kill path validates sid/serial as positive ints (Oracle "
        "forbids binds in ALTER SYSTEM).",
        required_config=("host",),
        optional_config=(
            "port", "service", "connect_timeout", "user_env",
            "password_env", "privileged_user_env",
            "privileged_password_env",
        ),
        credential_refs=("ORACLE_READ_USER", "ORACLE_READ_PASSWORD"),
        notes="Reads use a read-only Oracle account (SELECT on the V$ "
        "views; SELECT on dba_data_files/dba_free_space for tablespaces). "
        "kill_oracle_session uses a separate admin account "
        "(ORACLE_ADMIN_USER/ORACLE_ADMIN_PASSWORD or a privileged "
        "provider) with ALTER SYSTEM privilege, on its own short-lived "
        "connection, and refuses when it is absent. Thin mode needs no "
        "Oracle Client libraries. Fake-driver tested; live validation "
        "against a real database happens in a customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = 1521,
        *,
        service: str = "ORCLPDB1",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "ORACLE_READ_USER",
        password_env: str = "ORACLE_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "ORACLE_ADMIN_USER",
        privileged_password_env: str = "ORACLE_ADMIN_PASSWORD",
        connect_timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._service = service
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._connect_timeout = connect_timeout
        self._conn: Any = None  # driver connection, once connected
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_oracle_health",
            "get_oracle_tablespaces",
            "get_oracle_blocking",
            "kill_oracle_session",
        }

    def connect(self) -> None:
        """Open the read-credential session; probe with SELECT 1 FROM dual."""
        oracledb = _oracledb()
        user, password = self._resolve_read_credentials()
        try:
            conn = oracledb.connect(
                user=user,
                password=password,
                host=self._host,
                port=self._port,
                service_name=self._service,
                tcp_connect_timeout=self._connect_timeout,
            )
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM dual")
                cursor.fetchone()
            finally:
                _close_quietly(cursor)
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot connect to "
                f"{self._host}:{self._port}/{self._service}: {exc}"
            ) from exc
        self._conn = conn
        self._connected = True

    def close(self) -> None:
        """Release the session; safe to call when not connected."""
        conn, self._conn = self._conn, None
        self._connected = False
        _close_quietly(conn)

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"OracleDbConnector(host={self._host!r}, port={self._port!r}, "
            f"service={self._service!r}, "
            f"connect_timeout={self._connect_timeout!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "oracle_db: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="Oracle read user")
        password = resolve_secret(env=self._password_env,
                                  label="Oracle read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by kill_oracle_session.

        Raises ConnectorError when no privileged credential is configured,
        so the privileged path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            user, password = self._priv_credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "oracle_db: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="Oracle privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                  label="Oracle privileged password")
        return user, password

    # ------------------------------------------------- driver plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _query(self, op: str, sql: str,
               params: Sequence[Any] | None = None) -> list[dict]:
        """Run a read-only SELECT with bind variables only.

        Returns rows as dicts keyed by lowercased column names. Driver
        failures become ConnectorError (never raw, never with secrets).
        """
        self._require_connected()
        _oracledb()  # install guidance when the driver is absent
        cursor = self._conn.cursor()
        try:
            try:
                cursor.execute(sql, list(params or []))
            except Exception as exc:
                raise ConnectorError(
                    f"{self.name}: {op} failed: {exc}"
                ) from exc
            columns = [d[0].lower() for d in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            _close_quietly(cursor)

    @staticmethod
    def _coerce_positive_int(label: str, value: Any) -> int:
        """Validate sid/serial: positive ints only (bool/str-of-digits ok)."""
        if isinstance(value, bool):
            raise ConnectorError(
                f"oracle_db: {label} must be a positive integer, "
                f"got {value!r}"
            )
        if isinstance(value, int):
            num = value
        elif isinstance(value, str) and value.strip().isdigit():
            num = int(value.strip())
        else:
            raise ConnectorError(
                f"oracle_db: {label} must be a positive integer, "
                f"got {value!r}"
            )
        if num <= 0:
            raise ConnectorError(
                f"oracle_db: {label} must be a positive integer, "
                f"got {value!r}"
            )
        return num

    # ------------------------------------------------- reads

    def get_oracle_health(self) -> dict:
        """Instance health: version banner, open mode, session utilization.

        Returns: {host, port, service, version, open_mode, sessions_used,
        sessions_max, sessions_pct, ts}. sessions_max is None (and
        sessions_pct None) when the sessions limit is UNLIMITED;
        sessions_pct is None when the max is <= 0.
        """
        self._require_connected()
        version_rows = self._query("get_oracle_health", _HEALTH_VERSION_SQL)
        if not version_rows or not version_rows[0].get("banner"):
            raise ConnectorError(
                f"{self.name}: get_oracle_health: v$version returned no rows"
            )
        open_rows = self._query("get_oracle_health", _HEALTH_OPEN_MODE_SQL)
        open_mode = open_rows[0].get("open_mode") if open_rows else None
        sess_rows = self._query("get_oracle_health", _HEALTH_SESSIONS_SQL)
        sessions_used: int | None = None
        sessions_max: int | None = None
        if sess_rows:
            raw_used = sess_rows[0].get("current_utilization")
            try:
                sessions_used = int(raw_used) if raw_used is not None else None
            except (TypeError, ValueError):
                sessions_used = None
            # LIMIT_VALUE is VARCHAR2: "300" or "UNLIMITED".
            raw_max = sess_rows[0].get("limit_value")
            if isinstance(raw_max, str):
                raw_max = raw_max.strip()
                sessions_max = int(raw_max) if raw_max.isdigit() else None
            elif isinstance(raw_max, (int, float)):
                sessions_max = int(raw_max)
        sessions_pct = (
            round(sessions_used / sessions_max * 100, 1)
            if sessions_used is not None and sessions_max
            else None
        )
        return {
            "host": self._host,
            "port": self._port,
            "service": self._service,
            "version": str(version_rows[0]["banner"]),
            "open_mode": open_mode,
            "sessions_used": sessions_used,
            "sessions_max": sessions_max,
            "sessions_pct": sessions_pct,
            "ts": _now(),
        }

    def get_oracle_tablespaces(self) -> dict:
        """Tablespace usage aggregated from dba_data_files/dba_free_space.

        Returns: {host, tablespaces: [{name, used_pct, used_mb, max_mb}],
        ts}. Needs SELECT on the DBA views; on ORA-00942 raises
        ConnectorError with grants guidance instead of the raw error.
        """
        self._require_connected()
        try:
            rows = self._query("get_oracle_tablespaces", _TABLESPACES_SQL)
        except ConnectorError as exc:
            if "ORA-00942" in str(exc):
                raise ConnectorError(
                    f"{self.name}: get_oracle_tablespaces needs SELECT on "
                    "the DBA views dba_data_files and dba_free_space; the "
                    "read user lacks that grant (ORA-00942: table or view "
                    "does not exist). Ask the DBA to grant SELECT on "
                    "dba_data_files and dba_free_space to the read user."
                ) from exc
            raise
        tablespaces = []
        for row in rows:
            used_pct = row.get("used_pct")
            used_mb = row.get("used_mb")
            max_mb = row.get("max_mb")
            tablespaces.append({
                "name": row.get("name"),
                "used_pct": float(used_pct) if used_pct is not None else None,
                "used_mb": float(used_mb) if used_mb is not None else None,
                "max_mb": float(max_mb) if max_mb is not None else None,
            })
        return {"host": self._host, "tablespaces": tablespaces, "ts": _now()}

    def get_oracle_blocking(self) -> dict:
        """Sessions blocked on locks (blocking_session is not null).

        Returns: {host, blockers: [{sid, serial, username, wait_seconds,
        sql_snippet}], ts}. sql_snippet is the session's current SQL text
        from v$sql, whitespace-normalized and truncated to 200 chars
        ("" when the session has no current SQL).
        """
        self._require_connected()
        rows = self._query("get_oracle_blocking", _BLOCKING_SQL)
        blockers = []
        for row in rows:
            sql_text = row.get("sql_text")
            snippet = " ".join(str(sql_text).split()) if sql_text else ""
            blockers.append({
                "sid": int(row["sid"]),
                "serial": int(row["serial_no"]),
                "username": row.get("username"),
                "wait_seconds": int(row.get("wait_seconds") or 0),
                "sql_snippet": snippet[:_SQL_SNIPPET_LEN],
            })
        return {"host": self._host, "blockers": blockers, "ts": _now()}

    # ------------------------------------------------- privileged actions

    def kill_oracle_session(self, sid: Any, serial: Any) -> dict:
        """PRIVILEGED: kill a session via ALTER SYSTEM KILL SESSION.

        Runs on a SEPARATE short-lived admin connection under the
        privileged credential pair; refuses when that pair is not
        configured rather than reusing the read account. sid/serial are
        validated as positive ints first (Oracle forbids bind variables
        in ALTER SYSTEM, so only validated digits reach the SQL text).

        Returns: {sid, serial, killed: True, ts}.
        """
        self._require_connected()
        sid_int = self._coerce_positive_int("sid", sid)
        serial_int = self._coerce_positive_int("serial", serial)
        user, password = self._resolve_privileged_credentials()
        oracledb = _oracledb()
        admin_conn = None
        try:
            admin_conn = oracledb.connect(
                user=user,
                password=password,
                host=self._host,
                port=self._port,
                service_name=self._service,
                tcp_connect_timeout=self._connect_timeout,
            )
            cursor = admin_conn.cursor()
            try:
                cursor.execute(
                    f"ALTER SYSTEM KILL SESSION '{sid_int},{serial_int}'"
                )
            finally:
                _close_quietly(cursor)
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: kill_oracle_session({sid_int}, {serial_int}) "
                f"failed: {exc}"
            ) from exc
        finally:
            _close_quietly(admin_conn)
        return {
            "sid": sid_int,
            "serial": serial_int,
            "killed": True,
            "ts": _now(),
        }


register_connector(OracleDbConnector.SPEC, OracleDbConnector)
