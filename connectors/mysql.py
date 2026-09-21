"""Real MySQL connector: live diagnostics via pymysql.

TRANSPORT
---------
Reads (server health, FULL PROCESSLIST, replica status) go through the
``pymysql`` driver, imported LAZILY inside ``_driver()``: this module
imports cleanly when pymysql is absent, and only the driver paths raise,
with install instructions. Every query is PARAMETERIZED (``KILL QUERY
%s`` with a params tuple); SQL text is never string-formatted with
values. Query snippets in the processlist read are truncated to 200
chars. Driver exceptions are wrapped in ``ConnectorError`` at the
``_query`` boundary so raw driver errors never surface.

``kill_mysql_query`` opens a SHORT-LIVED admin connection under a
SEPARATE privileged credential pair (closed before returning) and
refuses when the privileged pair is not configured, rather than reusing
the read credential. The read connection opened by ``connect()`` is
reused for reads only.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_mysql_health() -> dict: host, port, version, uptime_secs,
  threads_connected, max_connections, connections_pct (None when
  max_connections <= 0), slow_queries, ts.
- get_mysql_processlist() -> dict: host, processes (list of {id, user,
  db, command, time_secs, state, query_snippet} -- query_snippet is the
  first 200 chars of the running statement, None when there is none),
  ts. The connector's own session row is skipped.
- get_mysql_replication() -> dict: host, role ("primary" when no replica
  status row, else "replica"), io_running, sql_running,
  seconds_behind_source (None when unknown), ts. Tries SHOW REPLICA
  STATUS first, falls back to SHOW SLAVE STATUS on older servers, and
  normalizes both column spellings.
- kill_mysql_query(process_id) -> PRIVILEGED dict: process_id, killed
  (True), ts.

CREDENTIALS
-----------
Same rules as the IBM MQ connector: the constructor takes a
credential-provider callable or env-var NAMES (``MYSQL_READ_USER`` /
``MYSQL_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance,
never appear in ``repr``, exceptions, or logs. The privileged
``kill_mysql_query`` resolves a SEPARATE pair (``MYSQL_ADMIN_USER`` /
``MYSQL_ADMIN_PASSWORD`` or a privileged provider) at act time and
refuses when it is absent.

STATUS (honest)
---------------
Fake-driver tested only (tests/test_mysql.py): canned cursor rows, no
live MySQL touched. Live validation against a real MySQL happens in a
customer pilot -- see docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)

#: Processlist query snippets longer than this are truncated.
_QUERY_SNIPPET_MAX = 200


def _driver() -> Any:
    """Import pymysql lazily; raise ConnectorError with install instructions."""
    try:
        import pymysql  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "mysql: the MySQL connector needs the 'pymysql' driver, which "
            "is not installed. Install it with: pip install pymysql"
        ) from exc
    return pymysql


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MySQLConnector(Connector):
    """Live MySQL diagnostics (+ approval-gated KILL QUERY). name = "mysql"."""

    name = "mysql"

    SPEC = ConnectorSpec(
        name="mysql",
        display_name="MySQL (pymysql)",
        description="Live MySQL diagnostics via pymysql: server health "
        "(version, uptime, connection usage, slow queries), FULL "
        "PROCESSLIST with truncated query snippets, and replica status. "
        "Privileged KILL QUERY runs under a separate admin credential. "
        "Parameterized queries only.",
        required_config=("host",),
        optional_config=(
            "port", "database", "connect_timeout", "user_env",
            "password_env", "privileged_user_env",
            "privileged_password_env",
        ),
        credential_refs=("MYSQL_READ_USER", "MYSQL_READ_PASSWORD"),
        notes="Reads use a read-only MySQL account. kill_mysql_query "
        "resolves a separate privileged pair (MYSQL_ADMIN_USER / "
        "MYSQL_ADMIN_PASSWORD or a privileged provider) at act time, "
        "opens a short-lived admin connection, and refuses when it is "
        "absent rather than reusing the read credential. Fake-driver "
        "tested only; live validation happens in a customer pilot. "
        "Query snippets are truncated to 200 chars; the connector's own "
        "processlist row is skipped.",
    )

    def __init__(
        self,
        host: str,
        port: int = 3306,
        *,
        database: str | None = None,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "MYSQL_READ_USER",
        password_env: str = "MYSQL_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "MYSQL_ADMIN_USER",
        privileged_password_env: str = "MYSQL_ADMIN_PASSWORD",
        connect_timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._database = database
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._connect_timeout = connect_timeout
        self._connection: Any = None  # read connection, once connected
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_mysql_health",
            "get_mysql_processlist",
            "get_mysql_replication",
            "kill_mysql_query",
        }

    def connect(self) -> None:
        """Open the read connection and probe it with SELECT VERSION().

        Fail closed: any driver/auth/network failure raises
        ConnectorError (never a raw driver exception, never a secret).
        """
        pymysql = _driver()
        user, password = self._resolve_read_credentials()
        try:
            conn = pymysql.connect(
                host=self._host,
                port=self._port,
                user=user,
                password=password,
                database=self._database,
                connect_timeout=self._connect_timeout,
            )
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot connect to "
                f"{self._host}:{self._port}: {exc}"
            ) from exc
        # Mark connected before the probe so _query can use the session;
        # close() rolls back cleanly if the probe fails.
        self._connection = conn
        self._connected = True
        try:
            self._scalar("SELECT VERSION()", op="connect probe")
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        """Close the read connection. Safe to call when not connected."""
        conn, self._connection = self._connection, None
        self._connected = False
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"MySQLConnector(host={self._host!r}, port={self._port!r}, "
            f"database={self._database!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            user, password = self._credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "mysql: read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="MySQL read user")
        password = resolve_secret(env=self._password_env,
                                  label="MySQL read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by kill_mysql_query.

        Raises ConnectorError when no privileged credential is configured,
        so the privileged path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            user, password = self._priv_credential_provider()[0:2]
            if not user or not password:
                raise ConnectorError(
                    "mysql: privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="MySQL privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                  label="MySQL privileged password")
        return user, password

    # ------------------------------------------------- driver plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _query(self, sql: str, params: tuple = (),
               *, op: str) -> list[dict]:
        """Run a PARAMETERIZED read query on the read connection.

        ``sql`` carries ``%s`` placeholders only -- values travel in
        ``params``, never interpolated into the text. Driver failures are
        wrapped in ConnectorError (no secrets in the message).
        """
        pymysql = _driver()
        try:
            with self._connection.cursor(
                    pymysql.cursors.DictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() or []
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: {op} failed: {exc}"
            ) from exc
        return [dict(row) for row in rows]

    def _scalar(self, sql: str, *, op: str) -> Any:
        """First column of the first row of a single-row query."""
        rows = self._query(sql, op=op)
        if not rows:
            raise ConnectorError(
                f"{self.name}: {op} returned no rows"
            )
        return next(iter(rows[0].values()))

    # ------------------------------------------------- reads

    def get_mysql_health(self) -> dict:
        """Server health: version, uptime, connection usage, slow queries.

        Returns {"host", "port", "version", "uptime_secs",
        "threads_connected", "max_connections", "connections_pct" (None
        when max_connections <= 0), "slow_queries", "ts"}.
        """
        self._require_connected()
        version = self._scalar("SELECT VERSION()", op="get_mysql_health")
        status = {row.get("Variable_name"): row.get("Value")
                  for row in self._query("SHOW GLOBAL STATUS",
                                         op="get_mysql_health")}
        var_rows = self._query(
            "SHOW GLOBAL VARIABLES LIKE 'max_connections'",
            op="get_mysql_health")
        max_connections = (int(var_rows[0].get("Value") or 0)
                           if var_rows else 0)
        uptime_secs = int(status.get("Uptime") or 0)
        threads_connected = int(status.get("Threads_connected") or 0)
        slow_queries = int(status.get("Slow_queries") or 0)
        connections_pct = (round(threads_connected / max_connections * 100, 1)
                           if max_connections > 0 else None)
        return {
            "host": self._host,
            "port": self._port,
            "version": str(version),
            "uptime_secs": uptime_secs,
            "threads_connected": threads_connected,
            "max_connections": max_connections,
            "connections_pct": connections_pct,
            "slow_queries": slow_queries,
            "ts": _now(),
        }

    def get_mysql_processlist(self) -> dict:
        """Live sessions from SHOW FULL PROCESSLIST.

        Returns {"host", "processes": [{id, user, db, command, time_secs,
        state, query_snippet}], "ts"}. The connector's own session row is
        skipped (it would only be noise -- and it must never be a KILL
        target). ``query_snippet`` is the first 200 chars of the running
        statement, or None when the session has none.
        """
        self._require_connected()
        own_id = self._scalar("SELECT CONNECTION_ID()",
                              op="get_mysql_processlist")
        rows = self._query("SHOW FULL PROCESSLIST",
                           op="get_mysql_processlist")
        processes: list[dict] = []
        for row in rows:
            pid = row.get("Id")
            if pid is not None and pid == own_id:
                continue  # skip our own reader session
            info = row.get("Info")
            processes.append({
                "id": int(pid) if pid is not None else None,
                "user": row.get("User"),
                "db": row.get("db"),
                "command": row.get("Command"),
                "time_secs": int(row.get("Time") or 0),
                "state": row.get("State"),
                "query_snippet": (str(info)[:_QUERY_SNIPPET_MAX]
                                  if info is not None else None),
            })
        return {
            "host": self._host,
            "processes": processes,
            "ts": _now(),
        }

    def get_mysql_replication(self) -> dict:
        """Replication role and lag from replica status.

        Returns {"host", "role", "io_running", "sql_running",
        "seconds_behind_source", "ts"}. ``role`` is "primary" when the
        status query returns no rows, otherwise "replica". Tries SHOW
        REPLICA STATUS first and falls back to SHOW SLAVE STATUS on
        older servers; both column spellings are normalized.
        """
        self._require_connected()
        try:
            rows = self._query("SHOW REPLICA STATUS",
                               op="get_mysql_replication")
        except ConnectorError:
            # MySQL < 8.0.23 speaks SLAVE instead of REPLICA.
            rows = self._query("SHOW SLAVE STATUS",
                               op="get_mysql_replication")
        if not rows:
            return {
                "host": self._host,
                "role": "primary",
                "io_running": None,
                "sql_running": None,
                "seconds_behind_source": None,
                "ts": _now(),
            }
        row = rows[0]
        lag = _first_present(row, "Seconds_Behind_Source",
                             "Seconds_Behind_Master")
        return {
            "host": self._host,
            "role": "replica",
            "io_running": _first_present(row, "Replica_IO_Running",
                                         "Slave_IO_Running"),
            "sql_running": _first_present(row, "Replica_SQL_Running",
                                          "Slave_SQL_Running"),
            "seconds_behind_source": (int(lag) if lag is not None else None),
            "ts": _now(),
        }

    # ------------------------------------------------- privileged actions

    @staticmethod
    def _validate_process_id(process_id: Any) -> int:
        """Positive-int validation for KILL targets (bools rejected too)."""
        if (isinstance(process_id, bool)
                or not isinstance(process_id, int)
                or process_id <= 0):
            raise ConnectorError(
                "mysql: process_id must be a positive integer, got "
                f"{process_id!r}"
            )
        return process_id

    def kill_mysql_query(self, process_id: int) -> dict:
        """PRIVILEGED: KILL QUERY <process_id> (statement only, not the
        connection).

        Runs under the SEPARATE privileged credential pair on a
        short-lived admin connection (opened and closed here -- the read
        session is never reused for this); refuses when the privileged
        credential is not configured. The id travels as a query parameter
        (``KILL QUERY %s``), never interpolated into SQL text.

        Returns {"process_id", "killed" (True), "ts"}.
        """
        self._require_connected()
        pid = self._validate_process_id(process_id)
        user, password = self._resolve_privileged_credentials()
        pymysql = _driver()
        try:
            admin = pymysql.connect(
                host=self._host,
                port=self._port,
                user=user,
                password=password,
                database=self._database,
                connect_timeout=self._connect_timeout,
            )
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: privileged connect for kill_mysql_query "
                f"failed: {exc}"
            ) from exc
        try:
            with admin.cursor() as cur:
                cur.execute("KILL QUERY %s", (pid,))
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: kill_mysql_query({pid}) failed: {exc}"
            ) from exc
        finally:
            try:
                admin.close()
            except Exception:
                pass
        return {
            "process_id": pid,
            "killed": True,
            "ts": _now(),
        }


def _first_present(row: dict, *keys: str) -> Any:
    """First non-None value across column-name spellings (replica/slave)."""
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
    return None


register_connector(MySQLConnector.SPEC, MySQLConnector)
