"""Real PostgreSQL connector: live diagnostics via the ``psycopg`` driver.

Reads (server health, lock blocking, replication status) go through
``psycopg`` (psycopg3), imported LAZILY inside the ``_psycopg()`` helper:
this module imports cleanly when the driver is absent; only the driver
paths raise, with install instructions (``pip install "psycopg[binary]"``).
Every read opens its own per-call connection under the READ credential and
closes it when the statement finishes; no connection pool is kept, so
``close()`` has nothing to release and is safe to call at any time.

All reads are SELECT-only. The single privileged path,
``terminate_postgres_backend``, runs ``SELECT pg_terminate_backend(%s)``
and resolves a SEPARATE admin credential pair at act time, refusing when
it is absent rather than reusing the read account. Every statement is a
constant SQL string; the only runtime value (the backend pid) is bound as
a query parameter, never interpolated into the SQL text. Query text handed
back to callers is truncated to 200 characters.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py -- this module must not be edited to match them,
they are the contract):
- get_postgres_health() -> dict: host, port, database, version, up,
  connections_used, connections_max, connections_pct (of max, None when
  max <= 0), ts.
- get_postgres_blocking() -> dict: host, blockers (list of {pid, usename,
  wait_seconds, query_snippet (first 200 chars), locktype}), ts. Empty
  list when nothing is waiting on a lock.
- get_postgres_replication() -> dict: host, role ("primary" or
  "standby"), replay_lag_bytes, replay_lag_seconds, ts. Lags are None on
  a primary; replay lag measures how far behind the standby is applying.
- terminate_postgres_backend(pid) -> PRIVILEGED dict: pid, terminated
  (True), ts.

CREDENTIALS
-----------
Same rules as the IBM MQ connector: the constructor takes a
credential-provider callable or env-var NAMES (``POSTGRES_READ_USER`` /
``POSTGRES_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance
(connections are opened in locals and closed before the method returns),
never appear in ``repr``, exceptions, or logs. The privileged
``terminate_postgres_backend`` resolves a SEPARATE pair
(``POSTGRES_ADMIN_USER`` / ``POSTGRES_ADMIN_PASSWORD`` or a privileged
provider) at act time and refuses when it is absent.

STATUS (honest)
---------------
Fake-driver tested only (tests/test_postgres.py): a fake ``psycopg``
module with canned cursor results, no live PostgreSQL touched. Live
validation against a real PostgreSQL happens in a customer pilot.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)

_QUERY_SNIPPET_LEN = 200


def _psycopg() -> Any:
    """Import psycopg lazily; raise ConnectorError with install instructions."""
    try:
        import psycopg  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "the PostgreSQL connector needs the 'psycopg' (psycopg3) driver, "
            "which is not installed. Install it with: "
            'pip install "psycopg[binary]"'
        ) from exc
    return psycopg


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _snippet(query: Any) -> str:
    """Truncate a query to the 200-char snippet boundary."""
    return str(query or "")[:_QUERY_SNIPPET_LEN]


class PostgresConnector(Connector):
    """Live PostgreSQL diagnostics via psycopg3. name = "postgres"."""

    name = "postgres"

    SPEC = ConnectorSpec(
        name="postgres",
        display_name="PostgreSQL (psycopg3)",
        description="Live PostgreSQL diagnostics via the psycopg driver: "
        "server health (version, connection counts vs max_connections), "
        "lock-blocking backends, and replication role/lag. Approval-gated "
        "pg_terminate_backend runs under a separate privileged credential.",
        required_config=("host",),
        optional_config=(
            "port", "database", "connect_timeout", "user_env",
            "password_env", "privileged_user_env", "privileged_password_env",
        ),
        credential_refs=("POSTGRES_READ_USER", "POSTGRES_READ_PASSWORD"),
        notes="Reads use a read-only PostgreSQL role. "
        "terminate_postgres_backend uses a separate credential "
        "(POSTGRES_ADMIN_USER / POSTGRES_ADMIN_PASSWORD or a privileged "
        "provider) and refuses when it is absent. All statements are "
        "read-only except the privileged terminate; query text is a "
        "constant per statement, values are bound parameters. "
        "Fake-driver tested; live validation against a real PostgreSQL "
        "happens in a customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = 5432,
        *,
        database: str | None = None,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "POSTGRES_READ_USER",
        password_env: str = "POSTGRES_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "POSTGRES_ADMIN_USER",
        privileged_password_env: str = "POSTGRES_ADMIN_PASSWORD",
        connect_timeout: int = 10,
    ) -> None:
        # Param wins, then the POSTGRES_DATABASE env var, then "postgres".
        self._host = host
        self._port = int(port)
        self._database = database or os.environ.get("POSTGRES_DATABASE") \
            or "postgres"
        self._credential_provider = credential_provider
        self._user_env = user_env
        self._password_env = password_env
        self._priv_credential_provider = privileged_credential_provider
        self._priv_user_env = privileged_user_env
        self._priv_password_env = privileged_password_env
        self._connect_timeout = int(connect_timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_postgres_health",
            "get_postgres_blocking",
            "get_postgres_replication",
            "terminate_postgres_backend",
        }

    def connect(self) -> None:
        """Probe the server with the read credential; fail closed on error.

        Opens and immediately closes one connection: a successful probe
        proves reachability, auth, and that the driver works. ALL failures
        (missing driver, bad credentials, unreachable host) surface as
        ConnectorError.
        """
        psycopg = _psycopg()
        user, password = self._resolve_read_credentials()
        try:
            with psycopg.connect(
                host=self._host,
                port=self._port,
                dbname=self._database,
                user=user,
                password=password,
                connect_timeout=self._connect_timeout,
            ):
                pass
        except ConnectorError:
            raise
        except Exception as exc:
            # Secrets never appear in the message; only host/port/database.
            raise ConnectorError(
                f"{self.name}: cannot reach PostgreSQL at "
                f"{self._host}:{self._port}/{self._database}: {exc}"
            ) from exc
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (connections are per-call); safe any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"PostgresConnector(host={self._host!r}, port={self._port!r}, "
            f"database={self._database!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            creds = self._credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "PostgreSQL read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env,
                              label="PostgreSQL read user")
        password = resolve_secret(env=self._password_env,
                                  label="PostgreSQL read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by the act() path.

        Raises ConnectorError when no privileged credential is configured,
        so the terminate path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            creds = self._priv_credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "PostgreSQL privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="PostgreSQL privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                   label="PostgreSQL privileged password")
        return user, password

    # ------------------------------------------------- driver plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _connection(self, user: str, password: str) -> Any:
        """Open one per-call psycopg connection with dict-row cursors.

        Callers own the connection: use ``with self._connection(...) as
        conn`` and it closes at block end. Failures wrap as ConnectorError
        with no secrets in the message.
        """
        psycopg = _psycopg()
        try:
            return psycopg.connect(
                host=self._host,
                port=self._port,
                dbname=self._database,
                user=user,
                password=password,
                connect_timeout=self._connect_timeout,
                row_factory=psycopg.rows.dict_row,
            )
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot reach PostgreSQL at "
                f"{self._host}:{self._port}/{self._database}: {exc}"
            ) from exc

    def _read_conn(self) -> Any:
        """Per-call connection under the READ credential."""
        user, password = self._resolve_read_credentials()
        return self._connection(user, password)

    def _query_one(self, sql: str) -> dict:
        """Run one read-only statement; return its single row as a dict."""
        self._require_connected()
        try:
            with self._read_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: query failed: {exc}"
            ) from exc
        if not isinstance(row, dict):
            raise ConnectorError(
                f"{self.name}: query returned an unexpected row shape"
            )
        return row

    def _query_all(self, sql: str) -> list[dict]:
        """Run one read-only statement; return all rows as dicts."""
        self._require_connected()
        try:
            with self._read_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rows = cur.fetchall()
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: query failed: {exc}"
            ) from exc
        return [r for r in (rows or []) if isinstance(r, dict)]

    # ------------------------------------------------- reads

    def get_postgres_health(self) -> dict:
        """Server health: version, live connection count vs max_connections.

        Returns {host, port, database, version, up, connections_used,
        connections_max, connections_pct, ts}. connections_pct is None
        when max_connections <= 0.
        """
        self._require_connected()
        version_row = self._query_one("SELECT version();")
        used_row = self._query_one(
            "SELECT count(*) AS connections_used FROM pg_stat_activity;")
        max_row = self._query_one(
            "SELECT current_setting('max_connections') AS max_connections;")
        used = int(used_row.get("connections_used") or 0)
        try:
            max_conn = int(str(max_row.get("max_connections") or "0"))
        except ValueError:
            max_conn = 0
        return {
            "host": self._host,
            "port": self._port,
            "database": self._database,
            "version": str(version_row.get("version") or ""),
            "up": True,
            "connections_used": used,
            "connections_max": max_conn,
            "connections_pct": (round(used / max_conn * 100, 1)
                                if max_conn > 0 else None),
            "ts": _now(),
        }

    def get_postgres_blocking(self) -> dict:
        """Backends currently waiting on a lock (blockers to investigate).

        Returns {host, blockers, ts}; each blocker is {pid, usename,
        wait_seconds, query_snippet (first 200 chars), locktype}.
        wait_seconds measures since the state/query started; an empty
        blockers list means no lock waits right now.
        """
        self._require_connected()
        rows = self._query_all(
            "SELECT a.pid AS pid, a.usename AS usename, "
            "EXTRACT(EPOCH FROM now() - "
            "COALESCE(a.state_change, a.query_start)) AS wait_seconds, "
            "a.query AS query, l.locktype AS locktype "
            "FROM pg_locks l "
            "JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE NOT l.granted AND a.wait_event_type = 'Lock';"
        )
        blockers = [
            {
                "pid": int(r.get("pid")),
                "usename": str(r.get("usename") or ""),
                "wait_seconds": float(r.get("wait_seconds") or 0.0),
                "query_snippet": _snippet(r.get("query")),
                "locktype": str(r.get("locktype") or ""),
            }
            for r in rows
        ]
        return {
            "host": self._host,
            "blockers": blockers,
            "ts": _now(),
        }

    def get_postgres_replication(self) -> dict:
        """Replication role and, on a standby, how far replay lags.

        Returns {host, role, replay_lag_bytes, replay_lag_seconds, ts}.
        role is "standby" or "primary"; the lag fields are None on a
        primary (nothing to replay against).
        """
        self._require_connected()
        recovery = self._query_one("SELECT pg_is_in_recovery() AS in_recovery;")
        if not recovery.get("in_recovery"):
            return {
                "host": self._host,
                "role": "primary",
                "replay_lag_bytes": None,
                "replay_lag_seconds": None,
                "ts": _now(),
            }
        lag_bytes_row = self._query_one(
            "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), "
            "pg_last_wal_receive_lsn()) AS lag_bytes;")
        lag_time_row = self._query_one(
            "SELECT EXTRACT(EPOCH FROM now() - "
            "pg_last_xact_replay_timestamp()) AS lag_seconds;")
        lag_bytes = lag_bytes_row.get("lag_bytes")
        lag_seconds = lag_time_row.get("lag_seconds")
        return {
            "host": self._host,
            "role": "standby",
            "replay_lag_bytes": (int(lag_bytes)
                                 if lag_bytes is not None else None),
            "replay_lag_seconds": (float(lag_seconds)
                                   if lag_seconds is not None else None),
            "ts": _now(),
        }

    # ------------------------------------------------- privileged actions

    def terminate_postgres_backend(self, pid: int) -> dict:
        """PRIVILEGED: terminate a backend via pg_terminate_backend(pid).

        Runs under the SEPARATE admin credential pair; refuses when it is
        not configured rather than reusing the read account. The pid is
        bound as a query parameter (never interpolated into SQL) and must
        be a positive int. Returns {pid, terminated: True, ts}.
        """
        self._require_connected()
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            raise ConnectorError(
                f"{self.name}: pid must be a positive int, got {pid!r}"
            )
        user, password = self._resolve_privileged_credentials()
        try:
            with self._connection(user, password) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_terminate_backend(%s);", (pid,))
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: pg_terminate_backend({pid}) failed: {exc}"
            ) from exc
        return {
            "pid": pid,
            "terminated": True,
            "ts": _now(),
        }


register_connector(PostgresConnector.SPEC, PostgresConnector)
