"""Real Redis connector: read-only diagnostics via redis-py.

TRANSPORT
---------
The real ``redis`` (redis-py) driver, imported LAZILY inside the
``_redis()`` helper: this module imports cleanly when the driver is
absent; only the driver paths raise, with install instructions. Every
read builds a short-lived client, queries it, and closes it in a
``finally``; ``close()`` therefore only flips the connected flag (per-call
connections, nothing persistent).

TOOL SURFACE (exact names/args/returns for the coordinator, who adds these
to mcp_server/tools.py and sim/estate.py -- this module must not be edited
to match them, they are the contract):
- get_redis_info() -> dict: host, port, version, uptime_secs,
  used_memory_bytes, maxmemory_bytes, mem_used_pct (of maxmemory, None
  when maxmemory=0 i.e. no limit configured), connected_clients,
  blocked_clients, hit_rate_pct (keyspace hits / (hits+misses) * 100, None
  when no hits and no misses), ts. From INFO server/memory/clients/stats.
- get_redis_replication() -> dict: host, role ("master" or "replica";
  the legacy "slave" role label is normalized to "replica"),
  connected_replicas, master_link_status (None on masters), ts. From
  INFO replication.
- get_redis_slowlog(limit=25) -> list of {id, ts, duration_us,
  command_snippet}: SLOWLOG GET entries (limit clamped to [1, 500]);
  command args joined by spaces and truncated to 200 chars -- arg VALUES
  are never reproduced in full, so secrets in SLOWLOG args are truncated
  away.

READS ONLY BY DESIGN
--------------------
This connector exposes NO privileged actions at all: Redis mutations
(FLUSHDB / FLUSHALL, CONFIG SET, DEBUG, key writes/deletes) are never
safe to automate, so there is no act() path to wire. ``act()`` therefore
always raises by construction, and the harness's privileged-refusal check
records an informational skip for this connector -- expected.

CREDENTIALS
-----------
Auth is optional. The constructor takes an env-var NAME (default
``REDIS_PASSWORD``) or a ``credential_provider`` callable returning
``(user, password)`` (user is an ACL name on Redis 6+; older AUTH ignores
it) -- never a raw secret. The password resolves per call into a local
variable, goes straight to the driver, and is never stored on the
instance, never appears in ``repr``, exceptions, or logs. Most
self-hosted Redis runs with no password; the connector then connects
without AUTH. Driver errors reference host:port only, never credentials.

STATUS (honest)
---------------
Fake-driver tested only (tests/test_redis.py): a canned ``redis`` module
injected into sys.modules, no live Redis touched. Live validation against
a real Redis happens in a customer pilot -- see
docs/REAL_CONNECTOR_READINESS.md. Reads-only by design (see above).
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

#: Max length of a slowlog command snippet; arg values are truncated away.
_MAX_SNIPPET_CHARS = 200
#: SLOWLOG GET limit is clamped to this range (negative means "all").
_MAX_SLOWLOG_LIMIT = 500


def _redis() -> Any:
    """Import redis lazily; raise ConnectorError with install instructions."""
    try:
        import redis  # type: ignore
    except ImportError as exc:
        raise ConnectorError(
            "the Redis connector needs the 'redis' driver, which is not "
            "installed. Install it with: pip install redis"
        ) from exc
    return redis


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _close_quietly(client: Any) -> None:
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


def _command_snippet(args: Any) -> str:
    """Join slowlog command args into a truncated snippet.

    Arg VALUES are never reproduced in full: the joined command is cut at
    200 chars, so any secret sitting in an arg value is truncated away.
    """
    if isinstance(args, (bytes, bytearray)):
        args = [args]
    if not isinstance(args, (list, tuple)):
        args = [args]
    parts: list[str] = []
    for arg in args:
        if isinstance(arg, (bytes, bytearray)):
            parts.append(arg.decode("utf-8", errors="replace"))
        else:
            parts.append(str(arg))
    return " ".join(parts)[:_MAX_SNIPPET_CHARS]


class RedisConnector(Connector):
    """Live Redis diagnostics, reads only. name = "redis"."""

    name = "redis"

    SPEC = ConnectorSpec(
        name="redis",
        display_name="Redis (redis-py, read-only)",
        description="Read-only Redis diagnostics via redis-py: INFO "
        "(server, memory, clients, keyspace stats), INFO replication, "
        "and SLOWLOG GET. No privileged actions are exposed: mutating "
        "Redis (FLUSHDB/FLUSHALL, CONFIG SET, DEBUG, key writes/deletes) "
        "is never safe to automate, so act() always raises by "
        "construction.",
        required_config=("host",),
        optional_config=(
            "port", "db", "password_env", "socket_timeout",
        ),
        credential_refs=("REDIS_PASSWORD",),
        notes="Reads use an optional AUTH password (REDIS_PASSWORD env var "
        "or a credential provider); most self-hosted Redis runs without "
        "AUTH. Fake-driver tested only; live validation against a real "
        "Redis happens in a customer pilot. Reads-only by design.",
    )

    def __init__(
        self,
        host: str,
        port: int = 6379,
        *,
        db: int = 0,
        password_env: str = "REDIS_PASSWORD",
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        socket_timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._db = int(db)
        self._password_env = password_env
        self._credential_provider = credential_provider
        self._socket_timeout = int(socket_timeout)
        self._connected = False

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {
            "get_redis_info",
            "get_redis_replication",
            "get_redis_slowlog",
        }

    def connect(self) -> None:
        """Import the driver, then PING (with optional AUTH) to prove reach.

        Driver failures and unreachable targets both surface as
        ConnectorError; nothing raw escapes.
        """
        redis = _redis()
        client = self._client(redis)
        try:
            client.ping()
        except Exception as exc:
            raise ConnectorError(
                f"{self.name}: cannot reach Redis at "
                f"{self._host}:{self._port} (db {self._db}): {exc}"
            ) from exc
        finally:
            _close_quietly(client)
        self._connected = True

    def close(self) -> None:
        """Nothing persistent (per-call connections); safe any time."""
        self._connected = False

    def __repr__(self) -> str:
        # Non-secret fields only (password_env is a reference NAME,
        # shown nowhere near a value).
        return (
            f"RedisConnector(host={self._host!r}, port={self._port!r}, "
            f"db={self._db!r}, socket_timeout={self._socket_timeout!r}, "
            f"credential_provider="
            f"{'set' if self._credential_provider else 'unset'})"
        )

    # ------------------------------------------------- credential handling

    def _resolve_password(self) -> str | None:
        """Optional AUTH password; None means connect without AUTH.

        Resolved per call into a local; never stored on the instance.
        """
        if self._credential_provider is not None:
            creds = self._credential_provider()
            user, password = creds[0], creds[1]
            # Redis AUTH is password-driven (user is an ACL name on
            # Redis 6+); an empty password means no AUTH.
            if not password:
                return None
            return password
        return os.environ.get(self._password_env) or None

    # ------------------------------------------------- driver plumbing

    def _require_connected(self) -> None:
        if not self._connected:
            raise ConnectorError(
                f"{self.name}: not connected; call connect() first"
            )

    def _client(self, redis: Any) -> Any:
        """Build one short-lived client; callers close it in a finally."""
        return redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._resolve_password(),
            socket_timeout=self._socket_timeout,
            decode_responses=True,
            client_name="rca-assistant",
        )

    def _wrap(self, op: str, exc: Exception) -> ConnectorError:
        """Translate a driver exception into ConnectorError (no secrets)."""
        return ConnectorError(f"{self.name}: {op} failed: {exc}")

    @staticmethod
    def _lookup(info: dict, section: str, *keys: str) -> Any:
        """Read a key from a driver INFO result, nested or flat.

        Modern driver output nests sections ({"server": {...}}); older
        builds return one flat dict. Both shapes are accepted; missing
        keys return None rather than raising.
        """
        sec = info.get(section)
        if isinstance(sec, dict):
            for key in keys:
                if key in sec:
                    return sec[key]
        for key in keys:
            if key in info:
                return info[key]
        return None

    # ------------------------------------------------- reads

    def get_redis_info(self) -> dict:
        """Server/memory/client/keyspace stats from the INFO command.

        Returns {host, port, version, uptime_secs, used_memory_bytes,
        maxmemory_bytes, mem_used_pct, connected_clients, blocked_clients,
        hit_rate_pct, ts}. mem_used_pct is None when maxmemory is 0 (no
        limit configured); hit_rate_pct is None when no keyspace hits and
        no misses have been recorded yet.
        """
        self._require_connected()
        redis = _redis()
        client = self._client(redis)
        try:
            info = client.info()
        except Exception as exc:
            raise self._wrap("get_redis_info", exc) from exc
        finally:
            _close_quietly(client)
        if not isinstance(info, dict):
            raise ConnectorError(
                f"{self.name}: INFO returned an unexpected shape"
            )
        try:
            used = int(self._lookup(info, "memory", "used_memory") or 0)
            maxmemory = int(self._lookup(info, "memory", "maxmemory") or 0)
            hits = int(self._lookup(info, "stats", "keyspace_hits") or 0)
            misses = int(
                self._lookup(info, "stats", "keyspace_misses") or 0)
            return {
                "host": self._host,
                "port": self._port,
                "version": str(
                    self._lookup(info, "server", "redis_version") or ""),
                "uptime_secs": int(
                    self._lookup(info, "server", "uptime_in_seconds") or 0),
                "used_memory_bytes": used,
                "maxmemory_bytes": maxmemory,
                "mem_used_pct": (round(used / maxmemory * 100, 1)
                                  if maxmemory > 0 else None),
                "connected_clients": int(
                    self._lookup(info, "clients", "connected_clients")
                    or 0),
                "blocked_clients": int(
                    self._lookup(info, "clients", "blocked_clients") or 0),
                "hit_rate_pct": (round(hits / (hits + misses) * 100, 1)
                                 if hits + misses > 0 else None),
                "ts": _now(),
            }
        except ConnectorError:
            raise
        except Exception as exc:
            raise self._wrap("get_redis_info(parse)", exc) from exc

    def get_redis_replication(self) -> dict:
        """Replication role and replica state from INFO replication.

        Returns {host, role, connected_replicas, master_link_status, ts}.
        role is "master" or "replica" (the legacy "slave" label some
        servers report is normalized to "replica"). master_link_status is
        None on masters; on replicas it is the server's reported link
        state ("up"/"down"). connected_replicas reads connected_slaves on
        servers that still report the old field name.
        """
        self._require_connected()
        redis = _redis()
        client = self._client(redis)
        try:
            info = client.info("replication")
        except Exception as exc:
            raise self._wrap("get_redis_replication", exc) from exc
        finally:
            _close_quietly(client)
        if not isinstance(info, dict):
            raise ConnectorError(
                f"{self.name}: INFO replication returned an unexpected "
                "shape"
            )
        role = str(self._lookup(info, "replication", "role") or "")
        if role == "slave":
            role = "replica"  # legacy label normalization
        try:
            return {
                "host": self._host,
                "role": role,
                "connected_replicas": int(
                    self._lookup(info, "replication", "connected_replicas",
                                 "connected_slaves") or 0),
                "master_link_status": self._lookup(
                    info, "replication", "master_link_status"),
                "ts": _now(),
            }
        except Exception as exc:
            raise self._wrap("get_redis_replication(parse)", exc) from exc

    def get_redis_slowlog(self, limit: int = 25) -> list[dict]:
        """Recent SLOWLOG GET entries, newest first (server order).

        Returns a list of {id, ts, duration_us, command_snippet}. limit is
        clamped to [1, 500] (a negative SLOWLOG limit would mean "all").
        command_snippet is the command with args joined by spaces and
        truncated to 200 chars -- full arg values (which may be secrets)
        are never reproduced.
        """
        self._require_connected()
        n = max(1, min(int(limit), _MAX_SLOWLOG_LIMIT))
        redis = _redis()
        client = self._client(redis)
        try:
            entries = client.slowlog_get(n)
        except Exception as exc:
            raise self._wrap("get_redis_slowlog", exc) from exc
        finally:
            _close_quietly(client)
        out: list[dict] = []
        for entry in entries or []:
            # redis-py yields [id, timestamp, duration_us, [args...]].
            try:
                eid, ts, duration, args = (
                    entry[0], entry[1], entry[2], entry[3])
                out.append({
                    "id": int(eid),
                    "ts": int(ts),
                    "duration_us": int(duration),
                    "command_snippet": _command_snippet(args),
                })
            except (TypeError, IndexError, ValueError):
                continue  # skip malformed entries, don't fail the read
        return out


register_connector(RedisConnector.SPEC, RedisConnector)
