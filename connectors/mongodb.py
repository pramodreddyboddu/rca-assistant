"""Real MongoDB connector: live diagnostics via pymongo.

TRANSPORT
---------
Real ``pymongo`` driver, imported LAZILY inside ``_pymongo()``: this module
imports cleanly when pymongo is absent; only the driver paths raise, with
a ``ConnectorError`` carrying install instructions. All reads use
read-only database commands (``serverStatus``, ``replSetGetStatus``,
``currentOp``); the only mutation is ``killOp``, which is privileged and
gated behind the separate admin credential.

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_mongo_health() -> dict: host, port, version, uptime_secs,
  connections_current, connections_available, ts.
- get_mongo_replset() -> dict: set_name, my_state, members (list of
  {name, state, health, lag_seconds}), ts. ``lag_seconds`` is the member's
  optimeDate lag behind the primary in seconds (0.0 for the primary
  itself), None when the optime is unknown or there is no primary.
  Raises ConnectorError when the node is not running with --replSet.
- get_mongo_current_ops() -> dict: ops (list of {opid, secs_running, op,
  ns, query_snippet}), ts. ``query_snippet`` is the command/query
  rendered as text and truncated to 200 chars.
- kill_mongo_op(opid) -> PRIVILEGED dict: opid, killed (True), ts.
  ``opid`` must be an int or a non-empty string.

CREDENTIALS
-----------
Same rules as the IBM MQ connector: the constructor takes a
credential-provider callable or env-var NAMES (``MONGO_READ_USER`` /
``MONGO_READ_PASSWORD``), never a raw secret. Secrets resolve per
request via ``resolve_secret`` and are never stored on the instance,
never appear in ``repr``, exceptions, or logs. The privileged
``kill_mongo_op`` resolves a SEPARATE admin pair (``MONGO_ADMIN_USER`` /
``MONGO_ADMIN_PASSWORD`` or a privileged provider) at act time and
refuses when it is absent rather than reusing the read account.

STATUS (honest)
---------------
Fake-driver tested only (tests/test_mongodb.py): a fake ``pymongo``
module is injected into sys.modules, no live mongod touched. Live
validation against a real MongoDB deployment happens in a customer
pilot -- see docs/REAL_CONNECTOR_READINESS.md.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
    resolve_secret,
)


def _pymongo() -> Any:
    """Import pymongo lazily; raise ConnectorError with install instructions."""
    try:
        import pymongo  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "mongodb: the MongoDB connector needs the 'pymongo' driver, "
            "which is not installed. Install it with: pip install pymongo"
        ) from exc
    return pymongo


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MongoDbConnector(Connector):
    """Live MongoDB diagnostics (+ approval-gated op kill). name = "mongodb"."""

    name = "mongodb"

    SPEC = ConnectorSpec(
        name="mongodb",
        display_name="MongoDB (pymongo)",
        description="Live MongoDB diagnostics via pymongo: server health "
        "(serverStatus), replica-set status with per-member replication "
        "lag (replSetGetStatus), active operations (currentOp), and "
        "approval-gated operation kill (killOp) under a separate admin "
        "identity.",
        required_config=("host",),
        optional_config=(
            "port", "database", "server_timeout_ms", "user_env",
            "password_env", "privileged_user_env",
            "privileged_password_env",
        ),
        credential_refs=("MONGO_READ_USER", "MONGO_READ_PASSWORD"),
        notes="Reads use a read-only MongoDB user and read-only db "
        "commands only. kill_mongo_op uses a separate admin pair "
        "(MONGO_ADMIN_USER / MONGO_ADMIN_PASSWORD or a privileged "
        "provider) and refuses when it is absent; the read credential is "
        "never reused for the kill. Fake-driver tested only; live "
        "validation against a real deployment happens in a customer pilot.",
    )

    def __init__(
        self,
        host: str,
        port: int = 27017,
        *,
        database: str = "admin",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        user_env: str = "MONGO_READ_USER",
        password_env: str = "MONGO_READ_PASSWORD",
        privileged_credential_provider: Callable[[], tuple[str, str]] | None = None,
        privileged_user_env: str = "MONGO_ADMIN_USER",
        privileged_password_env: str = "MONGO_ADMIN_PASSWORD",
        server_timeout_ms: int = 10000,
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
        self._server_timeout_ms = int(server_timeout_ms)
        self._client: Any = None  # pymongo.MongoClient, once connected
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_mongo_health",
            "get_mongo_replset",
            "get_mongo_current_ops",
            "kill_mongo_op",
        }

    def connect(self) -> None:
        """Build a read-credential client and ping; fail closed on error."""
        pymongo = _pymongo()
        user, password = self._resolve_read_credentials()
        try:
            client = pymongo.MongoClient(
                host=self._host,
                port=self._port,
                username=user,
                password=password,
                authSource=self._database,
                serverSelectionTimeoutMS=self._server_timeout_ms,
            )
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot create a client for "
                f"{self._host}:{self._port}: {exc}"
            ) from exc
        try:
            client[self._database].command("ping")
        except Exception as exc:
            _close_quietly(client)
            raise ConnectorError(
                f"{self.name}: cannot reach MongoDB at "
                f"{self._host}:{self._port}: {exc}"
            ) from exc
        self._client = client
        self._connected = True

    def close(self) -> None:
        """Close the client; safe to call when not connected."""
        client, self._client = self._client, None
        _close_quietly(client)
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only.
        return (
            f"MongoDbConnector(host={self._host!r}, port={self._port!r}, "
            f"database={self._database!r}, "
            f"server_timeout_ms={self._server_timeout_ms!r})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_read_credentials(self) -> tuple[str, str]:
        if self._credential_provider is not None:
            creds = self._credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "MongoDB read credential provider returned an empty "
                    "user/password"
                )
            return user, password
        user = resolve_secret(env=self._user_env, label="MongoDB read user")
        password = resolve_secret(env=self._password_env,
                                  label="MongoDB read password")
        return user, password

    def _resolve_privileged_credentials(self) -> tuple[str, str]:
        """Resolve the SEPARATE admin credential used by kill_mongo_op.

        Raises ConnectorError when no privileged credential is configured,
        so the privileged path can never silently reuse the read account.
        """
        if self._priv_credential_provider is not None:
            creds = self._priv_credential_provider()
            user, password = creds[0], creds[1]
            if not user or not password:
                raise ConnectorError(
                    "MongoDB privileged credential provider returned an "
                    "empty user/password"
                )
            return user, password
        user = resolve_secret(env=self._priv_user_env,
                              label="MongoDB privileged user")
        password = resolve_secret(env=self._priv_password_env,
                                  label="MongoDB privileged password")
        return user, password

    # ------------------------------------------------- driver plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _wrap(self, op: str, exc: Exception) -> ConnectorError:
        """Translate a driver exception into ConnectorError (no secrets)."""
        return ConnectorError(f"{self.name}: {op} failed: {exc}")

    # ------------------------------------------------- reads

    def get_mongo_health(self) -> dict:
        """Server health from the read-only ``serverStatus`` command.

        Returns {"host": str, "port": int, "version": str|None,
        "uptime_secs": int|None, "connections_current": int|None,
        "connections_available": int|None, "ts": str}.
        """
        self._require_connected()
        db = self._client[self._database]
        try:
            status = db.command("serverStatus")
        except Exception as exc:
            raise self._wrap("get_mongo_health", exc) from exc
        conns = status.get("connections") or {}
        return {
            "host": self._host,
            "port": self._port,
            "version": status.get("version"),
            "uptime_secs": status.get("uptime"),
            "connections_current": conns.get("current"),
            "connections_available": conns.get("available"),
            "ts": _now(),
        }

    def get_mongo_replset(self) -> dict:
        """Replica-set status from the read-only ``replSetGetStatus`` command.

        Returns {"set_name": str|None, "my_state": int|None,
        "members": [{"name": str|None, "state": int|None,
        "health": int|None, "lag_seconds": float|None}], "ts": str}.
        ``lag_seconds`` is the member's optimeDate lag behind the primary
        in seconds (0.0 for the primary itself); None when the member's
        optime or the primary's optime is unknown. Raises ConnectorError
        when the node is not running with --replSet.
        """
        self._require_connected()
        pymongo = _pymongo()
        db = self._client[self._database]
        try:
            status = db.command("replSetGetStatus")
        except Exception as exc:
            op_failure = getattr(getattr(pymongo, "errors", None),
                                 "OperationFailure", None)
            if (op_failure is not None
                    and isinstance(exc, op_failure)
                    and getattr(exc, "code", None) == 93):
                raise ConnectorError(
                    f"{self.name}: this mongod is not running with "
                    "--replSet; replica-set status needs a replica set "
                    "deployment"
                ) from exc
            raise self._wrap("get_mongo_replset", exc) from exc
        if not isinstance(status, dict):
            raise ConnectorError(
                f"{self.name}: replSetGetStatus returned an unexpected shape"
            )
        members_in = status.get("members") or []
        primary_optime = None
        for member in members_in:
            if isinstance(member, dict) and member.get("state") == 1:
                primary_optime = member.get("optimeDate")
                break
        members: list[dict] = []
        for member in members_in:
            if not isinstance(member, dict):
                continue
            state = member.get("state")
            optime = member.get("optimeDate")
            lag: float | None = None
            if state == 1:
                lag = 0.0
            elif primary_optime is not None and optime is not None:
                try:
                    lag = round(
                        (primary_optime - optime).total_seconds(), 1)
                except Exception:
                    lag = None  # naive/aware mix or non-datetimes
            members.append({
                "name": member.get("name"),
                "state": state,
                "health": member.get("health"),
                "lag_seconds": lag,
            })
        return {
            "set_name": status.get("set"),
            "my_state": status.get("myState"),
            "members": members,
            "ts": _now(),
        }

    def get_mongo_current_ops(self) -> dict:
        """Active operations from the read-only ``currentOp`` command.

        Returns {"ops": [{"opid": int|str|None, "secs_running": int|None,
        "op": str|None, "ns": str|None, "query_snippet": str}], "ts": str}.
        ``query_snippet`` is the operation's command/query rendered as
        text and truncated to 200 chars.
        """
        self._require_connected()
        db = self._client[self._database]
        try:
            result = db.command("currentOp", {"active": True})
        except Exception as exc:
            raise self._wrap("get_mongo_current_ops", exc) from exc
        ops: list[dict] = []
        for entry in result.get("inprog") or []:
            if not isinstance(entry, dict):
                continue
            ops.append({
                "opid": entry.get("opid"),
                "secs_running": entry.get("secs_running"),
                "op": entry.get("op"),
                "ns": entry.get("ns"),
                "query_snippet": self._query_snippet(entry),
            })
        return {"ops": ops, "ts": _now()}

    @staticmethod
    def _query_snippet(entry: dict) -> str:
        """Render the op's command/query as text, truncated to 200 chars."""
        cmd = entry.get("command")
        if cmd is None:
            cmd = entry.get("query")
        if isinstance(cmd, dict):
            try:
                text = json.dumps(cmd, default=str)
            except Exception:
                text = str(cmd)
        elif cmd is None:
            text = ""
        else:
            text = str(cmd)
        return text[:200]

    # ------------------------------------------------- privileged actions

    def kill_mongo_op(self, opid: int | str) -> dict:
        """PRIVILEGED: kill a running operation via ``killOp``.

        ``opid`` must be an int or a non-empty string (validated before
        anything else). Runs under the SEPARATE admin credential pair on a
        short-lived client; refuses when the privileged credential is not
        configured rather than reusing the read account.

        Returns {"opid": int|str, "killed": True, "ts": str}.
        """
        self._require_connected()
        opid = self._validate_opid(opid)
        pymongo = _pymongo()
        user, password = self._resolve_privileged_credentials()
        try:
            client = pymongo.MongoClient(
                host=self._host,
                port=self._port,
                username=user,
                password=password,
                authSource=self._database,
                serverSelectionTimeoutMS=self._server_timeout_ms,
            )
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot create a privileged client for "
                f"{self._host}:{self._port}: {exc}"
            ) from exc
        try:
            client[self._database].command("killOp", op=opid)
        except Exception as exc:
            raise self._wrap(f"kill_mongo_op({opid!r})", exc) from exc
        finally:
            _close_quietly(client)
        return {"opid": opid, "killed": True, "ts": _now()}

    def _validate_opid(self, opid: Any) -> int | str:
        """Accept an int or non-empty string opid; reject everything else."""
        if isinstance(opid, bool):
            raise ConnectorError(
                f"{self.name}: opid must be an int or a non-empty string, "
                f"got {opid!r}"
            )
        if isinstance(opid, int):
            return opid
        if isinstance(opid, str) and opid.strip():
            return opid
        raise ConnectorError(
            f"{self.name}: opid must be an int or a non-empty string, "
            f"got {opid!r}"
        )


def _close_quietly(client: Any) -> None:
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


register_connector(MongoDbConnector.SPEC, MongoDbConnector)
