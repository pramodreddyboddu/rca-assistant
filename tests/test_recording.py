"""Tests for the record/replay framework (connectors/recording.py).

The framework itself is driver-agnostic: tests use a fake driver thunk so
no live system is needed. Byte-faithfulness and determinism are asserted
directly; live-system recording is exercised by the MQ worker against a
real queue manager and lands as fixtures under tests/fixtures/recorded/.
"""

import json
from datetime import datetime, timezone

import pytest

from connectors.recording import (
    Recorder,
    RecordingError,
    _decode,
    _encode,
    fixture_inventory,
    get_mode,
    get_recorder,
)


def _recorder(tmp_path, **kw):
    kw.setdefault("fixture_root", tmp_path / "recorded")
    return Recorder("fakedb", **kw)


# ------------------------------------------------------------- codec

def test_codec_round_trip_is_byte_faithful():
    original = [
        {2016: "APP.Q1", 3: 42, 15: 5000},          # int-keyed PCF-style row
        {"name": "x", "blob": b"\x00\x01\x02binary"},
        (1, "two", 3.0),                              # tuple preserved
        {"nested": [{"a": 1}], "when": datetime(2026, 9, 21, 12, 0,
                                                tzinfo=timezone.utc)},
        {"plain": "str"},
    ]
    assert _decode(_encode(original)) == original


def test_codec_rejects_unserializable_loudly():
    with pytest.raises(RecordingError):
        _encode(object())


def test_codec_survives_json_serialization():
    # The encoded form must be plain JSON (fixtures are committed as JSON).
    original = [{2016: "APP.Q1", 3: 42}, b"bytes", (1, 2)]
    text = json.dumps(_encode(original))
    assert _decode(json.loads(text)) == original


# ------------------------------------------------------------- modes

def test_live_writes_fixture_and_returns_live_result(tmp_path):
    rec = _recorder(tmp_path, mode="live")
    live = [{"depth": 42}]
    out = rec.call("inquire_q", {"queue": "APP.Q1"}, lambda: live)
    assert out == live
    path = rec.fixture_path("inquire_q", {"queue": "APP.Q1"})
    assert path.is_file()
    doc = json.loads(path.read_text())
    assert doc["meta"]["tech"] == "fakedb"
    assert doc["meta"]["call"] == "inquire_q"
    assert doc["meta"]["key_args"] == {"queue": "APP.Q1"}
    assert "recorded_at" in doc["meta"]


def test_replay_serves_fixture_with_zero_driver_calls(tmp_path):
    rec = _recorder(tmp_path, mode="live")
    live = [{2016: "APP.Q1", 3: 42}]
    rec.call("inquire_q", {"queue": "APP.Q1"}, lambda: live)

    replay = _recorder(tmp_path, mode="replay")
    calls = []
    out = replay.call("inquire_q", {"queue": "APP.Q1"},
                      lambda: calls.append(1) or [{"depth": "WRONG"}])
    assert out == live            # byte-faithful to what was recorded
    assert calls == []            # the driver thunk never ran


def test_replay_is_deterministic(tmp_path):
    rec = _recorder(tmp_path, mode="live")
    rec.call("op", {"a": 1}, lambda: {"v": [1, 2, 3]})
    replay = _recorder(tmp_path, mode="replay")
    first = replay.call("op", {"a": 1}, lambda: None)
    second = replay.call("op", {"a": 1}, lambda: None)
    assert first == second == {"v": [1, 2, 3]}


def test_replay_missing_fixture_fails_closed(tmp_path):
    rec = _recorder(tmp_path, mode="replay")
    with pytest.raises(RecordingError) as ei:
        rec.call("nope", {"x": 1}, lambda: {"v": 1})
    assert "no recorded fixture" in str(ei.value)
    assert "RCA_RECORD_MODE=live" in str(ei.value)


def test_passthrough_calls_through_without_writing(tmp_path):
    rec = _recorder(tmp_path, mode="passthrough")
    out = rec.call("op", {"a": 1}, lambda: {"live": True})
    assert out == {"live": True}
    assert list((tmp_path / "recorded").rglob("*.json")) == []


def test_invalid_mode_raises(tmp_path):
    rec = _recorder(tmp_path, mode="bogus")
    with pytest.raises(RecordingError):
        rec.call("op", {}, lambda: 1)


def test_mode_defaults_to_replay_and_reads_env(monkeypatch):
    monkeypatch.delenv("RCA_RECORD_MODE", raising=False)
    assert get_mode() == "replay"
    monkeypatch.setenv("RCA_RECORD_MODE", "live")
    assert get_mode() == "live"
    monkeypatch.setenv("RCA_RECORD_MODE", "bogus")
    with pytest.raises(RecordingError):
        get_mode()


def test_get_recorder_caches_per_tech_and_mode_stays_dynamic(monkeypatch):
    r1 = get_recorder("fakedb")
    r2 = get_recorder("fakedb")
    assert r1 is r2
    monkeypatch.setenv("RCA_RECORD_MODE", "passthrough")
    assert r1.mode == "passthrough"   # env read per call, not cached


def test_fixture_keying_distinguishes_args(tmp_path):
    rec = _recorder(tmp_path, mode="live")
    rec.call("op", {"queue": "A"}, lambda: {"which": "A"})
    rec.call("op", {"queue": "B"}, lambda: {"which": "B"})
    replay = _recorder(tmp_path, mode="replay")
    assert replay.call("op", {"queue": "A"}, lambda: None) == {"which": "A"}
    assert replay.call("op", {"queue": "B"}, lambda: None) == {"which": "B"}


def test_fixture_inventory_lists_recorded_calls(tmp_path):
    rec = _recorder(tmp_path, mode="live")
    rec.call("inquire_q", {"queue": "APP.Q1"}, lambda: {"d": 1})
    inv = fixture_inventory("fakedb", fixture_root=tmp_path / "recorded")
    assert len(inv) == 1
    assert inv[0]["call"] == "inquire_q"
    assert inv[0]["key_args"] == {"queue": "APP.Q1"}
    assert "corrupt" not in inv[0]


def test_default_mode_applies_when_env_unset(monkeypatch, tmp_path):
    monkeypatch.delenv("RCA_RECORD_MODE", raising=False)
    rec = _recorder(tmp_path, default_mode="passthrough")
    assert rec.mode == "passthrough"
    out = rec.call("op", {"a": 1}, lambda: {"live": True})
    assert out == {"live": True}
    assert list((tmp_path / "recorded").rglob("*.json")) == []


def test_env_overrides_default_mode(monkeypatch, tmp_path):
    monkeypatch.setenv("RCA_RECORD_MODE", "replay")
    rec = _recorder(tmp_path, default_mode="passthrough")
    assert rec.mode == "replay"
    with pytest.raises(RecordingError):
        rec.call("op", {}, lambda: 1)


def test_bad_default_mode_rejected(tmp_path):
    with pytest.raises(RecordingError):
        _recorder(tmp_path, default_mode="bogus")
