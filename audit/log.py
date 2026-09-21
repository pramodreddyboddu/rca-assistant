"""Append-only, hash-chained audit log.

Every entry is one JSON object per line (JSONL). Entries form a hash chain:
each entry's "hash" covers the entry itself and links to the previous entry
via "prev", so tampering with or deleting an entry breaks verification.

Field order is fixed: seq, ts, event, actor, details, prev, hash.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

_GENESIS_PREV = "GENESIS"


def _utc_now() -> str:
    """Current time as an ISO-8601 UTC string, second precision."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _entry_hash(entry_without_hash: dict) -> str:
    """Hex SHA-256 of the canonical JSON of an entry (no "hash" field)."""
    canonical = json.dumps(entry_without_hash, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class AuditLog:
    """A tamper-evident audit log backed by a JSONL file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            self._write_genesis()

    # -- internal -------------------------------------------------------
    def _write_genesis(self) -> None:
        entry = self._build_entry(seq=0, event="log_opened", actor="system",
                                  details={}, prev=_GENESIS_PREV)
        self._write_line(entry)

    def _build_entry(self, seq: int, event: str, actor: str,
                     details: dict, prev: str) -> dict:
        entry = {
            "seq": seq,
            "ts": _utc_now(),
            "event": event,
            "actor": actor,
            "details": details,
            "prev": prev,
        }
        entry["hash"] = _entry_hash(entry)
        return entry

    def _write_line(self, entry: dict) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _read_lines(self) -> list[str]:
        with open(self.path, "r", encoding="utf-8") as f:
            return f.readlines()

    def _last_entry(self) -> dict | None:
        lines = [ln for ln in self._read_lines() if ln.strip()]
        if not lines:
            return None
        return json.loads(lines[-1])

    # -- public API -----------------------------------------------------
    def append(self, event: str, actor: str, details: dict) -> dict:
        """Append one event; returns the entry. Raises TypeError if details
        is not JSON-serializable (checked before anything is written)."""
        try:
            json.dumps(details)
        except (TypeError, ValueError) as exc:
            raise TypeError(f"details must be JSON-serializable: {exc}") from exc
        prev_entry = self._last_entry()
        prev_hash = prev_entry["hash"] if prev_entry is not None else _GENESIS_PREV
        seq = (prev_entry["seq"] + 1) if prev_entry is not None else 0
        entry = self._build_entry(seq=seq, event=event, actor=actor,
                                  details=details, prev=prev_hash)
        self._write_line(entry)
        return entry

    def entries(self) -> list[dict]:
        """All entries, oldest first. Skips lines that are blank; raises
        ValueError if a non-blank line is not valid JSON."""
        result = []
        for lineno, line in enumerate(self._read_lines(), start=1):
            if not line.strip():
                continue
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {lineno} is not valid JSON: {exc}") from exc
        return result

    def verify(self) -> tuple[bool, str]:
        """Re-read the file and check hashes, prev-links, and seq order.

        Never raises on corrupt content: returns (False, reason) instead.
        """
        raw_lines = self._read_lines()
        if not any(ln.strip() for ln in raw_lines):
            return False, "log is empty: no genesis entry"

        parsed: list[dict] = []
        for lineno, line in enumerate(raw_lines, start=1):
            if not line.strip():
                continue  # tolerate blank lines
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                return False, f"corrupt JSON at line {lineno}"
            if not isinstance(entry, dict):
                return False, f"entry at line {lineno} is not a JSON object"
            parsed.append(entry)

        if not parsed:
            return False, "log is empty: no genesis entry"

        prev_hash = _GENESIS_PREV
        for i, entry in enumerate(parsed):
            expected_seq = i
            if entry.get("seq") != expected_seq:
                return False, (
                    f"seq out of order at line {i + 1}: "
                    f"expected {expected_seq}, found {entry.get('seq')}"
                )
            if entry.get("prev") != prev_hash:
                return False, (
                    f"prev-link mismatch at seq {expected_seq}: "
                    f"expected {prev_hash[:12]}..., found {str(entry.get('prev'))[:12]}..."
                )
            stored_hash = entry.get("hash")
            body = {k: v for k, v in entry.items() if k != "hash"}
            if stored_hash != _entry_hash(body):
                return False, f"hash mismatch at seq {expected_seq}"
            prev_hash = stored_hash

        return True, f"ok: {len(parsed)} entries verified"

    def summary(self) -> dict:
        """Aggregate stats: entry count, first/last timestamps, event counts."""
        counts: dict[str, int] = {}
        first_ts = None
        last_ts = None
        for entry in self.entries():
            first_ts = first_ts or entry.get("ts")
            last_ts = entry.get("ts")
            event = entry.get("event", "?")
            counts[event] = counts.get(event, 0) + 1
        return {
            "entries": sum(counts.values()),
            "first_ts": first_ts,
            "last_ts": last_ts,
            "events": counts,
        }
