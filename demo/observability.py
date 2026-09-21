"""Observability for the RCA Assistant demo (stdlib only).

Prometheus-style metrics, structured JSON logging, /healthz, and run history.

WIRING CONTRACT (for demo/web.py — do NOT edit web.py here):
  Coordinator adds these routes in demo/web.py:

    GET /healthz
        import demo.observability as obs
        status, body = obs.handle_healthz(jev_status)
        # jev_status: dict, e.g. {"enabled": True, "mode": "jev"|"deterministic",
        #                         "key_configured": bool, "note": str}
        # -> send HTTP `status` with JSON body `body`

    GET /metrics
        status, body, content_type = obs.handle_metrics()
        # content_type == "text/plain; version=0.0.4"
        # -> send HTTP `status`, header Content-Type: content_type, body `body`

    After each Jev judgment (hypothesis_rank / evidence_verify / triage / risk):
        obs.note_jev_judgment(kind=<kind>, result=<result>)
        # kind:  "hypothesis_rank" | "evidence_verify" | "triage" | "risk"
        # result:"ok" | "unavailable" | "error"
        # -> increments rca_jev_judgments_total{kind,result}

  Additional optional helpers (same call pattern):
    obs.note_run(scenario, status)          # rca_runs_total{scenario,status}
    obs.note_approval(decision)             # decision: approve|reject|approve_all
    obs.note_privileged_action(tool)        # rca_privileged_actions_total{tool}
    obs.note_audit_entry()                  # rca_audit_entries_total
    obs.set_jev_enabled(True/False)         # rca_jev_enabled gauge 1/0
    obs.run_history = RunHistory()          # .record(summary) / .list(limit=50)
    logger = obs.get_logger("rca.web")      # JSON-lines on stderr, auto-redacted

Metric catalogue (Prometheus text exposition via registry.exposition()):
  rca_runs_total{scenario,status}            counter
  rca_approvals_total{decision}             counter  decision=approve|reject|approve_all
  rca_privileged_actions_total{tool}        counter
  rca_audit_entries_total                   counter
  rca_jev_judgments_total{kind,result}      counter
      kind=hypothesis_rank|evidence_verify|triage|risk
      result=ok|unavailable|error
  rca_jev_enabled                           gauge    1 when Jev reasoning is active
  rca_uptime_seconds                        gauge    process uptime (refreshed on exposition)

Secrets: all log records are passed through redact() before serialization, so
values under keys matching password|secret|token|api_key|authorization
(case-insensitive) become "***REDACTED***". Never log the raw TYPESAFE_API_KEY.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import sys
import threading
import time
import builtins
from datetime import datetime, timezone

VERSION = "0.3.0"

_REDACT_KEYS = re.compile(r"password|secret|token|api_key|authorization", re.IGNORECASE)
_REDACTED = "***REDACTED***"

_START_TIME = time.monotonic()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------- metrics -----

class MetricsRegistry:
    """Counters and gauges with Prometheus text-format exposition."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._meta: dict[str, dict] = {}          # name -> {"type":..., "help":..., "labels":(...)}
        self._counters: dict[str, dict[tuple, float]] = {}
        self._gauges: dict[str, dict[tuple, float]] = {}

    def define(self, name: str, kind: str, help_text: str,
               label_names: tuple[str, ...] = ()) -> None:
        if kind not in ("counter", "gauge"):
            raise ValueError(f"unknown metric kind: {kind!r}")
        with self._lock:
            self._meta[name] = {"type": kind, "help": help_text, "labels": label_names}
            store = self._counters if kind == "counter" else self._gauges
            store.setdefault(name, {})

    def _key(self, name: str, labels: dict | None) -> tuple:
        labels = labels or {}
        expected = self._meta[name]["labels"]
        if expected:
            unknown = builtins.set(labels) - builtins.set(expected)
            if unknown:
                raise ValueError(f"unexpected labels {sorted(unknown)} for metric {name!r}")
            return tuple(labels.get(k, "") for k in expected)
        if labels:
            raise ValueError(f"metric {name!r} takes no labels")
        return ()

    def inc(self, name: str, labels: dict | None = None, amount: float = 1.0) -> None:
        if name not in self._meta or self._meta[name]["type"] != "counter":
            raise KeyError(f"counter {name!r} is not defined")
        key = self._key(name, labels)
        with self._lock:
            series = self._counters[name]
            series[key] = series.get(key, 0.0) + amount

    def set(self, name: str, labels: dict | None = None, value: float = 0.0) -> None:
        if name not in self._meta or self._meta[name]["type"] != "gauge":
            raise KeyError(f"gauge {name!r} is not defined")
        key = self._key(name, labels)
        with self._lock:
            self._gauges[name][key] = float(value)

    @staticmethod
    def _escape_label_value(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    def exposition(self) -> str:
        # Refresh the uptime gauge so /metrics always reports current uptime.
        if "rca_uptime_seconds" in self._meta:
            with self._lock:
                self._gauges.setdefault("rca_uptime_seconds", {})[()] = (
                    time.monotonic() - _START_TIME
                )
        out: list[str] = []
        with self._lock:
            for name, meta in self._meta.items():
                kind, help_text, label_names = meta["type"], meta["help"], meta["labels"]
                out.append(f"# HELP {name} {help_text}")
                out.append(f"# TYPE {name} {kind}")
                store = self._counters if kind == "counter" else self._gauges
                for key, value in sorted(store.get(name, {}).items(),
                                         key=lambda kv: kv[0]):
                    if label_names:
                        pairs = ",".join(
                            f'{n}="{self._escape_label_value(str(v))}"'
                            for n, v in zip(label_names, key)
                        )
                        out.append(f"{name}{{{pairs}}} {value:g}")
                    else:
                        out.append(f"{name} {value:g}")
        return "\n".join(out) + "\n"


registry = MetricsRegistry()
registry.define("rca_runs_total", "counter",
                "Number of RCA runs by scenario and status.",
                ("scenario", "status"))
registry.define("rca_approvals_total", "counter",
                "Number of operator approval decisions.",
                ("decision",))
registry.define("rca_privileged_actions_total", "counter",
                "Number of privileged tool actions executed.",
                ("tool",))
registry.define("rca_audit_entries_total", "counter",
                "Number of audit log entries written.")
registry.define("rca_jev_judgments_total", "counter",
                "Number of Jev (System One) judgments by kind and result.",
                ("kind", "result"))
registry.define("rca_jev_enabled", "gauge",
                "1 when Jev reasoning is the active mode, 0 otherwise.")
registry.define("rca_uptime_seconds", "gauge",
                "Process uptime in seconds.")


def inc(name: str, labels: dict | None = None, amount: float = 1.0) -> None:
    registry.inc(name, labels, amount)


def set(name: str, labels: dict | None = None, value: float = 0.0) -> None:
    registry.set(name, labels, value)


# ------------------------------------------------------- wiring helpers ------

_JEV_KINDS = ("hypothesis_rank", "evidence_verify", "triage", "risk")
_JEV_RESULTS = ("ok", "unavailable", "error")
_APPROVAL_DECISIONS = ("approve", "reject", "approve_all")


def note_jev_judgment(kind: str, result: str) -> None:
    """Increment rca_jev_judgments_total{kind,result} after a Jev judgment."""
    if kind not in _JEV_KINDS:
        raise ValueError(f"kind must be one of {list(_JEV_KINDS)}, got {kind!r}")
    if result not in _JEV_RESULTS:
        raise ValueError(f"result must be one of {list(_JEV_RESULTS)}, got {result!r}")
    registry.inc("rca_jev_judgments_total", {"kind": kind, "result": result})


def note_run(scenario: str, status: str) -> None:
    """Increment rca_runs_total{scenario,status} when an RCA run finishes."""
    registry.inc("rca_runs_total", {"scenario": str(scenario), "status": str(status)})


def note_approval(decision: str) -> None:
    """Increment rca_approvals_total{decision} on an operator approval decision."""
    if decision not in _APPROVAL_DECISIONS:
        raise ValueError(f"decision must be one of {list(_APPROVAL_DECISIONS)}, got {decision!r}")
    registry.inc("rca_approvals_total", {"decision": decision})


def note_privileged_action(tool: str) -> None:
    """Increment rca_privileged_actions_total{tool} when a privileged tool runs."""
    registry.inc("rca_privileged_actions_total", {"tool": str(tool)})


def note_audit_entry() -> None:
    """Increment rca_audit_entries_total when an audit entry is written."""
    registry.inc("rca_audit_entries_total")


def set_jev_enabled(enabled: bool) -> None:
    """Set the rca_jev_enabled gauge (1 when Jev is the active reasoning mode)."""
    registry.set("rca_jev_enabled", value=1.0 if enabled else 0.0)


def uptime_seconds() -> int:
    return int(time.monotonic() - _START_TIME)


# ------------------------------------------------------------------ health -----

def get_healthz(jev_status: dict) -> dict:
    """Build the /healthz payload. jev_status is passed through untouched."""
    return {
        "status": "ok",
        "version": VERSION,
        "uptime_s": uptime_seconds(),
        "jev": dict(jev_status),
    }


def handle_healthz(jev_status: dict) -> tuple[int, dict]:
    """Wiring contract: returns (http_status, json_body) for GET /healthz."""
    return 200, get_healthz(jev_status)


def handle_metrics() -> tuple[int, str, str]:
    """Wiring contract: returns (http_status, body, content_type) for GET /metrics."""
    return 200, registry.exposition(), "text/plain; version=0.0.4"


# ------------------------------------------------------------------ redact -----

def redact(obj):
    """Recursively replace secret-looking values with "***REDACTED***".

    Any dict key matching password|secret|token|api_key|authorization
    (case-insensitive) has its value replaced, however deeply nested,
    including inside lists.
    """
    if isinstance(obj, dict):
        return {
            key: (_REDACTED if _REDACT_KEYS.search(str(key)) else redact(value))
            for key, value in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        redacted = [redact(item) for item in obj]
        return tuple(redacted) if isinstance(obj, tuple) else redacted
    return obj


# ------------------------------------------------------------------ logging ----

_STANDARD_ATTRS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "asctime", "taskName",
})


class _JsonStderrHandler(logging.Handler):
    """Emit one redacted JSON object per record on stderr."""

    def __init__(self, stream=None):
        super().__init__()
        self.stream = stream if stream is not None else sys.stderr

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = {
                "ts": _utc_now_iso(),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            for key, value in record.__dict__.items():
                if key not in _STANDARD_ATTRS and key != "message":
                    payload[key] = value
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            line = json.dumps(redact(payload), default=str)
            self.stream.write(line + "\n")
            self.stream.flush()
        except Exception:  # never let logging break the app
            self.handleError(record)


class JsonLogger(logging.LoggerAdapter):
    """Logger adapter routing extra keyword args into the record as fields.

    Allows: logger.info("run finished", scenario="mq-backlog", steps=7)
    Reserved logging kwargs (exc_info, stack_info, extra, stacklevel) keep
    their stdlib meaning; everything else becomes a record field and is
    auto-redacted by the JSON handler.
    """

    _RESERVED = frozenset({"exc_info", "stack_info", "stacklevel", "extra"})

    def process(self, msg, kwargs):
        extra = dict(kwargs.pop("extra", {}) or {})
        for key in list(kwargs):
            if key not in self._RESERVED:
                extra[key] = kwargs.pop(key)
        if extra:
            kwargs["extra"] = extra
        return msg, kwargs


_loggers: dict[str, JsonLogger] = {}
_logger_lock = threading.Lock()


def get_logger(name: str, stream=None) -> JsonLogger:
    """Return a logger writing one redacted JSON object per line to stderr.

    Extra keyword fields on log calls (logger.info("...", scenario="x")) are
    included and automatically passed through redact().
    """
    with _logger_lock:
        if name in _loggers:
            return _loggers[name]
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        logger.addHandler(_JsonStderrHandler(stream=stream))
        adapter = JsonLogger(logger, {})
        _loggers[name] = adapter
        return adapter


# -------------------------------------------------------------- run history ---

class RunHistory:
    """Append-only JSONL store of run summaries; list() returns newest-first."""

    def __init__(self, path: str | None = None) -> None:
        self.path = path or os.path.join("runs", "history.jsonl")
        self._lock = threading.Lock()

    def record(self, summary: dict) -> None:
        """Append {"ts": ..., **summary} as one JSON line."""
        entry = {"ts": _utc_now_iso(), **dict(summary)}
        with self._lock:
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with io.open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, default=str) + "\n")

    def list(self, limit: int = 50) -> list[dict]:
        """Return up to `limit` summaries, newest first."""
        try:
            with io.open(self.path, "r", encoding="utf-8") as fh:
                lines = [ln.strip() for ln in fh if ln.strip()]
        except FileNotFoundError:
            return []
        entries = []
        for ln in lines[-limit:]:
            try:
                entries.append(json.loads(ln))
            except json.JSONDecodeError:
                continue
        entries.reverse()
        return entries


run_history = RunHistory()
