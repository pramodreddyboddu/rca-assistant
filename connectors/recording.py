"""Record/replay for connector driver responses.

Purpose: ground connectors in REALITY. Run once against a LIVE system with
``RCA_RECORD_MODE=live``; raw driver responses are written as JSON fixtures
under ``tests/fixtures/recorded/<tech>/`` and committed to the repo. CI and
default runs use replay mode (the default): fixtures are served with zero
network, exercising the exact same response-shaping code as the live path.

Modes (``RCA_RECORD_MODE`` env var):
- ``replay`` (default): serve fixtures. A missing fixture raises
  :class:`RecordingError` — fail closed, never silently fake.
- ``live``: call through to the real driver, write the fixture, return the
  live result.
- ``passthrough``: call through without recording (ad-hoc debugging).

When the env var is unset, a Recorder falls back to its ``default_mode``
(``"replay"`` unless the caller says otherwise). Connectors wire their
reads through a module-level recorder with ``default_mode="passthrough"``
so production behavior is unchanged — the driver is always hit — while
tests set ``RCA_RECORD_MODE=replay`` to run the full shaping path against
recorded fixtures with zero network and zero driver installed.

Only READS are recorded. Privileged / mutating actions must never go
through the recorder.

Usage inside a connector (example: IBM MQ PCF inquire)::

    from connectors.recording import get_recorder

    rec = get_recorder("ibmmq")
    resp = rec.call(
        "inquire_q",
        {"op": "MQCMD_INQUIRE_Q", "qmgr": qmgr, "queue": queue},
        lambda: self._pcf().MQCMD_INQUIRE_Q({
            cmqc.MQCA_Q_NAME: queue,
            cmqc.MQIA_Q_TYPE: cmqc.MQQT_LOCAL,
        }),
    )

Recording run (from a shell with the live system reachable)::

    RCA_RECORD_MODE=live python3 /tmp/record_mq.py

Fixtures are JSON with a type-preserving codec: driver responses often use
integer dict keys (e.g. PCF attribute numbers) or carry bytes, which plain
JSON would mangle. The codec round-trips them byte-faithfully while staying
human-reviewable.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

RECORD_ENV_VAR = "RCA_RECORD_MODE"
MODES = ("replay", "live", "passthrough")

_DEFAULT_ROOT = (
    Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "recorded"
)


class RecordingError(Exception):
    """Raised for recording/replay problems (missing fixture, bad mode)."""


def get_mode() -> str:
    """Current record/replay mode from the environment (default: replay)."""
    mode = os.environ.get(RECORD_ENV_VAR, "replay").strip().lower()
    if mode not in MODES:
        raise RecordingError(
            f"invalid {RECORD_ENV_VAR}={mode!r}; expected one of {MODES}"
        )
    return mode


# ------------------------------------------------------------- codec

def _encode(obj: Any) -> Any:
    """Encode a driver response into JSON-safe, type-preserving data."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return {"$bytes": base64.b64encode(obj).decode("ascii")}
    if isinstance(obj, datetime):
        return {"$datetime": obj.isoformat()}
    if isinstance(obj, tuple):
        return {"$tuple": [_encode(v) for v in obj]}
    if isinstance(obj, list):
        return [_encode(v) for v in obj]
    if isinstance(obj, dict):
        if obj and all(isinstance(k, str) for k in obj):
            return {k: _encode(v) for k, v in obj.items()}
        if not obj:
            return {}
        # Non-string keys (e.g. PCF attribute numbers): JSON would coerce
        # them to strings, so keep explicit [key, value] pairs instead.
        return {"$intmap": [[_encode(k), _encode(v)] for k, v in obj.items()]}
    raise RecordingError(
        f"cannot serialize {type(obj).__name__} into a recorded fixture; "
        "extend the codec in connectors/recording.py if this type is a "
        "legitimate driver response"
    )


def _decode(obj: Any) -> Any:
    """Inverse of _encode: restore the original driver response shape."""
    if isinstance(obj, list):
        return [_decode(v) for v in obj]
    if isinstance(obj, dict):
        if set(obj.keys()) == {"$bytes"}:
            return base64.b64decode(obj["$bytes"])
        if set(obj.keys()) == {"$datetime"}:
            return datetime.fromisoformat(obj["$datetime"])
        if set(obj.keys()) == {"$tuple"}:
            return tuple(_decode(v) for v in obj["$tuple"])
        if set(obj.keys()) == {"$intmap"}:
            return {_decode(k): _decode(v) for k, v in obj["$intmap"]}
        return {k: _decode(v) for k, v in obj.items()}
    return obj


def _key_hash(key_args: dict) -> str:
    canonical = json.dumps(key_args, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _sanitize(name: str) -> str:
    return "".join(c if (c.isalnum() or c in ("_", "-")) else "_" for c in name)


# ------------------------------------------------------------- recorder

class Recorder:
    """Wraps driver calls with record/replay behavior for one technology."""

    def __init__(
        self,
        tech: str,
        *,
        mode: str | None = None,
        default_mode: str = "replay",
        fixture_root: str | Path | None = None,
        source: str = "live",
    ) -> None:
        self.tech = _sanitize(tech)
        self._mode = mode
        if default_mode not in MODES:
            raise RecordingError(
                f"invalid recorder default_mode {default_mode!r}; "
                f"expected one of {MODES}"
            )
        self._default_mode = default_mode
        self.fixture_root = Path(fixture_root) if fixture_root else _DEFAULT_ROOT
        self.source = source

    @property
    def mode(self) -> str:
        if self._mode is not None:
            if self._mode not in MODES:
                raise RecordingError(
                    f"invalid recorder mode {self._mode!r}; expected one of {MODES}"
                )
            return self._mode
        if RECORD_ENV_VAR in os.environ:
            return get_mode()  # validates; raises on bogus values
        return self._default_mode

    def fixture_dir(self) -> Path:
        return self.fixture_root / self.tech

    def fixture_path(self, call: str, key_args: dict) -> Path:
        name = f"{_sanitize(call)}_{_key_hash(key_args)}.json"
        return self.fixture_dir() / name

    def call(
        self,
        call: str,
        key_args: dict,
        thunk: Callable[[], Any],
    ) -> Any:
        """Run ``thunk`` (the real driver call) under record/replay.

        ``call`` names the driver operation (e.g. ``"inquire_q"``);
        ``key_args`` is a small JSON-able dict identifying the request
        (operation + object names, never secrets). In replay mode the thunk
        is never executed.
        """
        mode = self.mode
        if mode == "replay":
            return self._replay(call, key_args)
        result = thunk()
        if mode == "live":
            self._write(call, key_args, result)
        return result

    # -- internals

    def _write(self, call: str, key_args: dict, result: Any) -> Path:
        encoded = _encode(result)  # raises on unserializable types: loud
        doc = {
            "meta": {
                "tech": self.tech,
                "call": call,
                "key_args": key_args,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "source": self.source,
                "codec": "connectors.recording v1",
            },
            "response": encoded,
        }
        path = self.fixture_path(call, key_args)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
        return path

    def _replay(self, call: str, key_args: dict) -> Any:
        path = self.fixture_path(call, key_args)
        if not path.is_file():
            raise RecordingError(
                f"no recorded fixture for {self.tech}.{call} "
                f"(args hash {_key_hash(key_args)}); re-record with "
                f"{RECORD_ENV_VAR}=live against a live system, or check "
                f"that the fixture was committed under {path.parent}"
            )
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            return _decode(doc["response"])
        except (json.JSONDecodeError, KeyError) as exc:
            raise RecordingError(
                f"recorded fixture {path} is corrupt: {exc}"
            ) from exc


_recorders: dict[str, Recorder] = {}


def get_recorder(tech: str) -> Recorder:
    """Process-wide Recorder for a technology (mode stays env-dynamic)."""
    if tech not in _recorders:
        _recorders[tech] = Recorder(tech)
    return _recorders[tech]


def fixture_inventory(tech: str,
                      fixture_root: str | Path | None = None) -> list[dict]:
    """Inventory of recorded fixtures for a technology (for docs/tests)."""
    root = Path(fixture_root) if fixture_root else _DEFAULT_ROOT
    out: list[dict] = []
    for path in sorted((root / _sanitize(tech)).glob("*.json")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            meta = doc.get("meta", {})
            out.append({
                "file": path.name,
                "call": meta.get("call"),
                "key_args": meta.get("key_args"),
                "recorded_at": meta.get("recorded_at"),
                "source": meta.get("source"),
            })
        except (json.JSONDecodeError, OSError):
            out.append({"file": path.name, "corrupt": True})
    return out
