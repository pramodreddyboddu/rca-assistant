"""Replay tests: the IBM MQ connector against REAL recorded fixtures.

No pymqi, no network, no queue manager. With ``RCA_RECORD_MODE=replay``
the connector serves the fixtures recorded against the live QM1
(IBM MQ 10.0.0.5, 2026-09-21) under ``tests/fixtures/recorded/ibmmq/``,
exercising the connector's real response-shaping code end to end.

Each test also poisons the ``pymqi`` import to prove the driver is never
touched on the replay path.
"""

import os
import sys

import pytest

from connectors.ibmmq import IBMQConnector
from connectors.recording import RecordingError, fixture_inventory


@pytest.fixture()
def replay(monkeypatch):
    monkeypatch.setenv("RCA_RECORD_MODE", "replay")
    # Prove the replay path never touches the driver: importing pymqi fails.
    monkeypatch.setitem(sys.modules, "pymqi", None)
    # read_error_log's config check runs before the recorder; point it at a
    # bogus dir to prove the file is never opened in replay mode.
    monkeypatch.setenv("MQ_ERROR_LOG_DIR", "/nonexistent-dir-xyz")
    return monkeypatch


@pytest.fixture()
def conn():
    # No connect() call: replay needs no session and no credentials.
    return IBMQConnector(
        qmgr="QM1",
        channel="APP.SVRCONN",
        host="localhost",
        credential_provider=lambda: ("user", "pass"),
    )


def test_fixtures_present():
    inv = fixture_inventory("ibmmq")
    calls = {i["call"] for i in inv}
    assert {"inquire_q", "inquire_channel_status",
            "inquire_listener_status", "read_error_log"} <= calls
    assert not [i for i in inv if i.get("corrupt")]


def test_replay_queue_depths(replay, conn):
    d = conn.get_queue_depth("QM1", "APP.Q1")
    assert d["depth"] == 60
    assert d["max_depth"] == 5000
    assert d["queue"] == "APP.Q1"

    backlog = conn.get_queue_depth("QM1", "BACKLOG.Q")
    assert backlog["depth"] == 200  # incident-shaped backlog, as recorded

    empty = conn.get_queue_depth("QM1", "APP.Q2")
    assert empty["depth"] == 0


def test_replay_channel_status_incident_shape(replay, conn):
    stopped = conn.get_channel_status("QM1", "BATCH.CHL")
    assert stopped["status"] == "STOPPED"  # the seeded incident

    running = conn.get_channel_status("QM1", "APP.SVRCONN")
    assert running["status"] == "RUNNING"


def test_replay_listener_status(replay, conn):
    assert conn.get_listener_status("LISTENER.TCP")["status"] == "RUNNING"


def test_replay_stopped_listener_fallback(replay, conn):
    # Recorded 2085 (no status instance) -> INQUIRE_LISTENER fallback ->
    # STOPPED, all from fixtures.
    assert conn.get_listener_status("STOPPED.LSR")["status"] == "STOPPED"


def test_replay_config_shares_raw_fixtures(replay, conn):
    q = conn.get_config("QM1", "queue", "APP.Q1")
    assert q["max_depth"] == 5000
    assert q["max_msg_length"] > 0

    c = conn.get_config("QM1", "channel", "BATCH.CHL")
    assert c["status"] == "STOPPED"


def test_replay_cert_status_shape_no_tls(replay, conn):
    cert = conn.get_cert_status("QM1", "APP.SVRCONN")
    assert cert["valid"] is None      # plain TCP: no TLS peer, as recorded
    assert cert["expires"] is None
    assert set(cert) >= {"qmgr", "channel", "valid", "expires",
                         "cipher", "cert_label", "ssl_peer", "ts"}


def test_replay_error_log_parse(replay, conn):
    entries = conn.read_error_log("QM1", limit=5)
    assert len(entries) == 5
    for e in entries:
        assert set(e) == {"ts", "severity", "code", "message"}
    assert any((e["code"] or "").startswith("AMQ") for e in entries)


def test_replay_unknown_object_fails_closed(replay, conn):
    # Nothing recorded for this queue: replay must fail loudly, never fake.
    with pytest.raises(RecordingError) as ei:
        conn.get_queue_depth("QM1", "NOPE.Q")
    assert "no recorded fixture" in str(ei.value)


def test_replay_wrong_qmgr_rejected_before_recorder(replay, conn):
    from connectors.base import ConnectorError
    with pytest.raises(ConnectorError):
        conn.get_queue_depth("QM2", "APP.Q1")


def test_default_mode_is_passthrough_not_replay(monkeypatch):
    # Without the env var, the connector always hits the driver (production
    # behavior unchanged); only an explicit RCA_RECORD_MODE opts into replay.
    import connectors.ibmmq as m
    monkeypatch.delenv("RCA_RECORD_MODE", raising=False)
    assert m._RECORDER.mode == "passthrough"
