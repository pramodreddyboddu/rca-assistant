"""Tests for the restart_channel honesty fix (connectors/ibmmq.py).

``restart_channel`` must report the channel status it OBSERVED after the
start command, never a hardcoded "RUNNING". After
``MQCMD_START_CHANNEL`` it polls the real status (settle wait) and reports
what it saw, with a ``note`` when the channel never reached RUNNING.

pymqi is NOT installed and no live MQ system is touched: a fake ``pymqi``
module is injected into ``sys.modules`` (removed after each test by
monkeypatch), following the conventions of tests/test_ibmmq.py. The
fake's start command deliberately lands the channel in BINDING — like a
real command server — and the inquiring read walks a scripted status
sequence, so the settle-poll is genuinely exercised. Short injected
``settle_timeout``/``settle_interval`` values keep the suite fast; no test
sleeps anywhere near the production 10s default. No fixture contains a
real secret: "REDACTED-fake" placeholders stand in.
"""

import json
import sys
import types

import pytest

from connectors.base import ConnectorError
from connectors.ibmmq import IBMQConnector

# Numeric PCF channel-status codes, matching the live values
# (see connectors/ibmmq.py _CHANNEL_STATUS_NAMES).
_STOPPED = 6
_BINDING = 1
_RUNNING = 3
_STARTING = 2
_CHANNEL_NOT_ACTIVE = 4064


# ------------------------------------------------------------- fake pymqi


def _make_fake_pymqi(state):
    mod = types.ModuleType("pymqi")

    class FakeMQMIError(Exception):
        def __init__(self, msg, comp=2, reason=0, verb=""):
            super().__init__(msg)
            self.comp = comp
            self.reason = reason
            self.verb = verb

    CMQC = types.SimpleNamespace(
        MQRC_UNKNOWN_OBJECT_NAME=2085,
        MQRC_NO_MSG_AVAILABLE=2033,
    )

    # Only the PCF channel constants the connector's privileged path and
    # status shaping touch.
    CMQCFC = types.SimpleNamespace(
        MQCACH_CHANNEL_NAME=3501,
        MQIACH_CHANNEL_STATUS=1527,
    )

    class FakeCD:
        pass

    class FakeQueueManager:
        def __init__(self, name):
            self.name = name
            self.connected = False

        def connect_tcp_client(self, name, cd, channel, conn_name,
                               user=None, password=None):
            cd.ChannelName = channel
            cd.ConnectionName = conn_name
            self.connected = True

        def disconnect(self):
            self.connected = False

    class FakePCFExecute:
        def __init__(self, qmgr):
            self.qmgr = qmgr

        def MQCMD_INQUIRE_CHANNEL_STATUS(self, args):
            name = args[CMQCFC.MQCACH_CHANNEL_NAME]
            ch = state["channels"][name]
            seq = ch.get("status_seq")
            if seq:
                # Walk the scripted sequence, holding the last value.
                if len(seq) > 1:
                    ch["status"] = seq.pop(0)
                else:
                    ch["status"] = seq[0]
            return [{CMQCFC.MQCACH_CHANNEL_NAME: name,
                     CMQCFC.MQIACH_CHANNEL_STATUS: ch["status"]}]

        def MQCMD_STOP_CHANNEL(self, args):
            ch = state["channels"][args[CMQCFC.MQCACH_CHANNEL_NAME]]
            if ch["status"] == _STOPPED:
                # Mirrors the live server: stopping an inactive channel
                # answers MQRCCF_CHANNEL_NOT_ACTIVE (4064).
                raise FakeMQMIError("channel not active", comp=2,
                                    reason=_CHANNEL_NOT_ACTIVE,
                                    verb="STOP_CHANNEL")
            ch["status"] = _STOPPED
            return []

        def MQCMD_START_CHANNEL(self, args):
            if state.get("fail_start"):
                raise FakeMQMIError("start rejected", comp=2, reason=3015,
                                    verb="START_CHANNEL")
            ch = state["channels"][args[CMQCFC.MQCACH_CHANNEL_NAME]]
            # Like a real command server, the channel does not become
            # RUNNING synchronously: it lands in BINDING and settles.
            ch["status_seq"] = list(state.get("settle_script",
                                             [_BINDING, _RUNNING]))
            return []

    mod.MQMIError = FakeMQMIError
    mod.CMQC = CMQC
    mod.CMQCFC = CMQCFC
    mod.CD = FakeCD
    mod.QueueManager = FakeQueueManager
    mod.PCFExecute = FakePCFExecute
    return mod


def _fresh_state():
    return {
        "channels": {"PAYMENTS.RCVR": {"status": _STOPPED}},
        "settle_script": [_BINDING, _BINDING, _RUNNING],
    }


@pytest.fixture()
def fake_mq(monkeypatch):
    """Inject the fake pymqi module and return its backing state."""
    state = _fresh_state()
    monkeypatch.setitem(sys.modules, "pymqi", _make_fake_pymqi(state))
    return state


def _conn(**kwargs):
    kwargs.setdefault("credential_provider",
                      lambda: ("rca_reader", "REDACTED-fake"))
    kwargs.setdefault("privileged_credential_provider",
                      lambda: ("rca_admin", "REDACTED-fake-admin"))
    conn = IBMQConnector(qmgr="QM1", channel="SVRCONN.CH",
                         host="mq01.example", **kwargs)
    conn.connect()
    return conn


# ------------------------------------------------- the honesty contract


def test_restart_reports_run_only_when_observed(fake_mq):
    """STOPPED -> BINDING -> BINDING -> RUNNING across polls: the result
    claims RUNNING because the poll SAW it, and reports the poll count."""
    conn = _conn()
    try:
        d = conn.restart_channel(
            "QM1", "PAYMENTS.RCVR",
            settle_timeout=5.0, settle_interval=0.05,
        )
    finally:
        conn.close()
    assert d["previous_status"] == "STOPPED"
    assert d["status"] == "RUNNING"
    assert d["polls"] >= 2
    assert "note" not in d  # nothing to caveat when RUNNING was observed
    json.dumps(d)


def test_restart_reports_binding_honestly_when_stuck(fake_mq):
    """A channel stuck at BINDING past the settle timeout is reported as
    BINDING with a note — never upgraded to a hardcoded RUNNING."""
    fake_mq["settle_script"] = [_BINDING]  # never settles
    conn = _conn()
    try:
        d = conn.restart_channel(
            "QM1", "PAYMENTS.RCVR",
            settle_timeout=0.2, settle_interval=0.02,
        )
    finally:
        conn.close()
    assert d["previous_status"] == "STOPPED"
    assert d["status"] == "BINDING"
    assert d["polls"] >= 1
    assert "note" in d
    assert "BINDING" in d["note"]
    assert "not claimed RUNNING" in d["note"]
    json.dumps(d)


def test_restart_slow_settler_is_not_claimed_early(fake_mq):
    """The poller waits for the truth: with a settle script that reaches
    RUNNING far beyond the timeout, the result reports the transitional
    state, not RUNNING. (RUNNING sits at poll 12; the timeout allows at
    most ~6 polls, so it is unreachable here regardless of machine
    speed — time.sleep never sleeps short.)"""
    fake_mq["settle_script"] = ([_BINDING] + [_STARTING] * 9 + [_RUNNING])
    conn = _conn()
    try:
        d = conn.restart_channel(
            "QM1", "PAYMENTS.RCVR",
            settle_timeout=0.1, settle_interval=0.02,
        )
    finally:
        conn.close()
    assert d["status"] in ("BINDING", "STARTING")
    assert d["status"] != "RUNNING"
    assert "note" in d
    json.dumps(d)


def test_restart_start_failure_raises_connector_error(fake_mq):
    """A failed MQCMD_START_CHANNEL becomes a ConnectorError (via _wrap),
    never a success-shaped dict."""
    fake_mq["fail_start"] = True
    conn = _conn()
    try:
        with pytest.raises(ConnectorError) as excinfo:
            conn.restart_channel("QM1", "PAYMENTS.RCVR")
    finally:
        conn.close()
    assert "restart_channel" in str(excinfo.value)


def test_restart_stop_of_active_channel_still_works(fake_mq):
    """Stopping a RUNNING channel does not raise 4064 and the restart
    proceeds through the settle poll."""
    fake_mq["channels"]["PAYMENTS.RCVR"]["status"] = _RUNNING
    conn = _conn()
    try:
        d = conn.restart_channel(
            "QM1", "PAYMENTS.RCVR",
            settle_timeout=5.0, settle_interval=0.05,
        )
    finally:
        conn.close()
    assert d["previous_status"] == "RUNNING"
    assert d["status"] == "RUNNING"
    json.dumps(d)


def test_restart_refuses_without_privileged_credential(fake_mq, monkeypatch):
    """act() must refuse when no admin credential pair is configured; the
    read session is untouched and no settle poll ever runs."""
    monkeypatch.delenv("MQ_ADMIN_USER", raising=False)
    monkeypatch.delenv("MQ_ADMIN_PASSWORD", raising=False)
    conn = IBMQConnector(
        qmgr="QM1", channel="SVRCONN.CH", host="mq01.example",
        credential_provider=lambda: ("rca_reader", "REDACTED-fake"),
    )
    conn.connect()
    read_mgr = conn._mgr
    try:
        with pytest.raises(ConnectorError) as excinfo:
            conn.restart_channel("QM1", "PAYMENTS.RCVR")
        assert conn._mgr is read_mgr
    finally:
        conn.close()
    assert "privileged" in str(excinfo.value).lower()
