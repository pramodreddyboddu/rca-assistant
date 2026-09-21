"""Real Linux host connector: local /proc + allow-listed log reads.

This is the first *real* connector in the demo: it reads live data from the
machine it runs on, through the exact same MCP tool interface, authz, and
audit path as the simulated estate (Gateway.call with token/scope checks).

SANDBOX CONTRACT (read-only by construction)
--------------------------------------------
This connector is deliberately incapable of mutation:

- It defines NO method that writes, deletes, kills, restarts, executes, or
  otherwise changes anything on the host. Every public method is a read.
- It never shells out (no os.system, subprocess, popen, or equivalent).
- Host metrics come from the /proc pseudo-filesystem, which cannot alter
  system state.
- Log reads are bounded by a constructor-provided allow-list: a path is
  served only if its real path (os.path.realpath, symlinks resolved) is an
  allow-listed file or sits under an allow-listed directory. Anything else
  raises PermissionError. Denied attempts are audited as tool_error by the
  Gateway, which is the sandbox doing its job.

Log content is returned verbatim as inert data. The connector has no code
path that interprets, quotes, or acts on log text, so hostile log lines
(e.g. "ignore previous instructions and ...") can never trigger an action.
"""

from __future__ import annotations

import os
import shutil
import socket
import time
from collections import deque
from datetime import datetime, timezone

from connectors.base import (
    Connector,
    ConnectorError,
    ConnectorSpec,
    register_connector,
)

_CPU_SAMPLE_GAP_S = 0.1
_MAX_LOG_LINES = 200
_MAX_LOG_BYTES = 256 * 1024


class LinuxHostConnector(Connector):
    """Read-only diagnostics for the local Linux host. name = "linux-host"."""

    name = "linux-host"

    SPEC = ConnectorSpec(
        name="linux-host",
        display_name="Linux host (local /proc + allow-listed logs)",
        description="Reads live CPU/memory/disk/load from /proc and tails "
        "allow-listed log files on the machine it runs on. Read-only by "
        "construction: it defines no mutation path at all.",
        required_config=(),
        optional_config=("allowed_log_paths",),
        credential_refs=(),
        notes="No credentials: it can only read its own host, never a "
        "remote one. capabilities() exposes get_host_metrics and tail_log "
        "only; act() always raises because there are no privileged tools.",
    )

    def __init__(self, allowed_log_paths=("/var/log/",)) -> None:
        self._allowed = tuple(
            os.path.realpath(p) for p in allowed_log_paths
        )

    # ------------------------------------------------- Connector contract

    def capabilities(self) -> set[str]:
        return {"get_host_metrics", "tail_log"}

    def connect(self) -> None:
        """No session needed: the target is always this machine."""

    def close(self) -> None:
        """Nothing to release."""

    def __repr__(self) -> str:  # no secrets exist on this connector
        return f"LinuxHostConnector(allowed_log_paths={list(self._allowed)!r})"

    # ------------------------------------------------------------------ host

    def _local_names(self) -> set[str]:
        try:
            hostname = socket.gethostname()
        except OSError as exc:
            raise ConnectorError(f"cannot resolve local hostname: {exc}")
        return {hostname, hostname.split(".")[0], "localhost"}

    def _check_host(self, host: str) -> None:
        if host not in self._local_names():
            raise ConnectorError(
                f"host {host!r} is not this machine "
                f"(known names: {sorted(self._local_names())}); "
                "this connector only reads its own host"
            )

    @staticmethod
    def _read_cpu_times() -> tuple[int, int]:
        """Return (idle_jiffies, total_jiffies) from the aggregate cpu line."""
        try:
            with open("/proc/stat", "r", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("cpu "):
                        parts = [int(v) for v in line.split()[1:]]
                        idle = parts[3] + parts[4]  # idle + iowait
                        return idle, sum(parts)
        except OSError as exc:
            raise ConnectorError(f"cannot read /proc/stat: {exc}")
        raise ConnectorError("no aggregate 'cpu' line found in /proc/stat")

    def _cpu_pct(self) -> float:
        idle1, total1 = self._read_cpu_times()
        time.sleep(_CPU_SAMPLE_GAP_S)
        idle2, total2 = self._read_cpu_times()
        delta_total = total2 - total1
        if delta_total <= 0:
            return 0.0
        pct = 100.0 * (delta_total - (idle2 - idle1)) / delta_total
        return max(0.0, min(100.0, pct))

    @staticmethod
    def _mem_pct() -> float:
        try:
            with open("/proc/meminfo", "r", encoding="utf-8") as fh:
                fields: dict[str, int] = {}
                for line in fh:
                    key, _, rest = line.partition(":")
                    if key in ("MemTotal", "MemAvailable"):
                        fields[key] = int(rest.strip().split()[0])
        except OSError as exc:
            raise ConnectorError(f"cannot read /proc/meminfo: {exc}")
        try:
            total, available = fields["MemTotal"], fields["MemAvailable"]
        except KeyError as exc:
            raise ConnectorError(
                f"/proc/meminfo missing expected field: {exc}"
            )
        if total <= 0:
            raise ConnectorError("/proc/meminfo reports MemTotal <= 0")
        return 100.0 * (total - available) / total

    @staticmethod
    def _disk_pct() -> float:
        try:
            usage = shutil.disk_usage("/")
        except OSError as exc:
            raise ConnectorError(f"cannot stat disk usage of '/': {exc}")
        if usage.total <= 0:
            raise ConnectorError("disk usage reports total <= 0")
        return 100.0 * usage.used / usage.total

    @staticmethod
    def _load1() -> float:
        try:
            with open("/proc/loadavg", "r", encoding="utf-8") as fh:
                return float(fh.read().split()[0])
        except OSError as exc:
            raise ConnectorError(f"cannot read /proc/loadavg: {exc}")
        except (IndexError, ValueError) as exc:
            raise ConnectorError(f"unexpected /proc/loadavg format: {exc}")

    def get_host_metrics(self, host: str = "localhost") -> dict:
        """Real CPU/memory/disk/load for this machine.

        Raises ConnectorError for any host that is not this machine.
        /proc read failures raise ConnectorError with a clear message,
        never a raw traceback.
        """
        self._check_host(host)
        return {
            "host": host,
            "source": "real",
            "cpu_pct": round(self._cpu_pct(), 1),
            "mem_pct": round(self._mem_pct(), 1),
            "disk_pct": round(self._disk_pct(), 1),
            "load1": round(self._load1(), 2),
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # ------------------------------------------------------------------ logs

    def _check_path(self, path: str) -> str:
        """Resolve the path and enforce the allow-list.

        Returns the resolved path. Raises PermissionError for anything not
        explicitly allowed (audited as tool_error by the Gateway).
        """
        real = os.path.realpath(path)
        for allowed in self._allowed:
            if real == allowed or real.startswith(allowed + os.sep):
                return real
        raise PermissionError(
            f"log path {path!r} is not under the allowed log paths "
            f"{list(self._allowed)}"
        )

    def tail_log(self, path: str, limit: int = 50) -> list[dict]:
        """Return the last `limit` lines of an allow-listed log file.

        Each entry is {"line_no": 1-based line number, "text": line}.
        limit is capped at 200 lines and 256KB total; content is returned
        verbatim and is never interpreted.
        """
        limit = max(0, min(int(limit), _MAX_LOG_LINES))
        real = self._check_path(path)
        try:
            fh = open(real, "r", encoding="utf-8", errors="replace")
        except OSError as exc:
            raise ConnectorError(f"cannot read log {real!r}: {exc}")
        try:
            lines: deque[str] = deque(maxlen=limit)
            total_bytes = 0
            total_lines = 0
            for line in fh:
                total_lines += 1
                total_bytes += len(line.encode("utf-8", "replace"))
                if total_bytes > _MAX_LOG_BYTES:
                    break
                if limit:
                    lines.append(line)
        except OSError as exc:
            raise ConnectorError(f"error reading log {real!r}: {exc}")
        finally:
            fh.close()
        first_no = max(1, total_lines - len(lines) + 1)
        return [
            {"line_no": first_no + i, "text": text.rstrip("\n")}
            for i, text in enumerate(lines)
        ]


register_connector(LinuxHostConnector.SPEC, LinuxHostConnector)
