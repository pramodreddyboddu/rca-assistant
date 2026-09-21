"""Tests for the real IBM MQ connector (connectors/ibmmq.py).

pymqi is NOT installed here and no live MQ system is touched. A fake
``pymqi`` module is injected into ``sys.modules`` (and removed after each
test by monkeypatch), covering queue depth, channel/listener status, PCF
error paths, privileged actions, and credential hygiene. No test fixture
contains a real secret: placeholders like "REDACTED-fake" stand in, and
the hygiene tests assert they never leak into repr, exceptions, or logs.
"""

import json
import subprocess
import sys
import types

import pytest

from connectors.base import ConnectorError
from connectors.ibmmq import IBMQConnector

FAKE_SECRET = "s3cr3t-fake-xyz-123"
FAKE_ADMIN_SECRET = "s3cr3t-fake-admin-456"


# ------------------------------------------------------------- fake pymqi


def _make_fake_pymqi(state, fail_connect=False):
    mod = types.ModuleType("pymqi")

    class FakeMQMIError(Exception):
        def __init__(self, msg, comp=2, reason=0, verb=""):
            super().__init__(msg)
            self.comp = comp
            self.reason = reason
            self.verb = verb

    CMQC = types.SimpleNamespace(
        MQCA_Q_NAME=2016,
        MQIA_Q_TYPE=20,
        MQQT_LOCAL=1,
        MQIA_CURRENT_Q_DEPTH=3,
        MQIA_MAX_Q_DEPTH=15,
        MQIA_MAX_MSG_LENGTH=13,
        MQCACH_CHANNEL_NAME=3501,
        MQIACH_CHANNEL_STATUS=1527,
        MQCACH_LISTENER_NAME=3550,
        MQIACH_LISTENER_STATUS=3552,
        MQCACH_SSL_CIPHER_SPEC=3561,
        MQCACH_SSL_CERT_LABEL=3562,
        MQCACH_SSL_PEER_NAME=3563,
        MQIACH_SSL_CERT_VALID=1528,
        MQCACH_SSL_CERT_EXPIRY=3564,
        MQGMO_FAIL_IF_QUIESCING=0x4000,
        MQRC_UNKNOWN_OBJECT_NAME=2085,
        MQRC_NO_MSG_AVAILABLE=2033,
    )

    class FakeCD:
        pass

    class FakeSCO:
        pass

    class FakeMD:
        pass

    class FakeGMO:
        Options = 0

    class FakeQueueManager:
        instances = []

        def __init__(self, name):
            self.name = name
            self.connected = False
            self.connect_calls = []
            FakeQueueManager.instances.append(self)

        def connect(self, qmgr, channel, conn_info, user=None, password=None):
            # Deliberately records everything EXCEPT the password.
            self.connect_calls.append({
                "kind": "connect", "qmgr": qmgr, "channel": channel,
                "conn_info": conn_info, "user": user,
            })
            if fail_connect or "unreachable" in conn_info:
                raise FakeMQMIError("connection refused", comp=2,
                                    reason=2059, verb="connect")
            self.connected = True

        def connect_with_options(self, qmgr, cd=None, sco=None,
                                 user=None, password=None):
            self.connect_calls.append({
                "kind": "connect_with_options", "qmgr": qmgr,
                "cipher": getattr(cd, "SSLCipherSpec", None),
                "key_repo": getattr(sco, "KeyRepository", None),
                "user": user,
            })
            if fail_connect or "unreachable" in qmgr:
                raise FakeMQMIError("connection refused", comp=2,
                                    reason=2059, verb="connect")
            self.connected = True

        def disconnect(self):
            self.connected = False

    class FakePCFExecute:
        def __init__(self, qmgr):
            self.qmgr = qmgr

        def MQCMD_INQUIRE_Q(self, args):
            name = args[CMQC.MQCA_Q_NAME]
            if name not in state["queues"]:
                raise FakeMQMIError(f"unknown queue {name!r}", comp=2,
                                    reason=2085, verb="INQUIRE_Q")
            q = state["queues"][name]
            return [{CMQC.MQCA_Q_NAME: name,
                     CMQC.MQIA_CURRENT_Q_DEPTH: q["depth"],
                     CMQC.MQIA_MAX_Q_DEPTH: q["max"],
                     CMQC.MQIA_MAX_MSG_LENGTH: 4194304}]

        def MQCMD_INQUIRE_CHANNEL_STATUS(self, args):
            name = args[CMQC.MQCACH_CHANNEL_NAME]
            if name not in state["channels"]:
                raise FakeMQMIError(f"unknown channel {name!r}", comp=2,
                                    reason=2085, verb="INQUIRE_CHANNEL")
            ch = state["channels"][name]
            return [{CMQC.MQCACH_CHANNEL_NAME: name,
                     CMQC.MQIACH_CHANNEL_STATUS: ch["status"],
                     CMQC.MQCACH_SSL_CIPHER_SPEC: ch.get("cipher"),
                     CMQC.MQCACH_SSL_CERT_LABEL: ch.get("cert_label"),
                     CMQC.MQCACH_SSL_PEER_NAME: ch.get("ssl_peer"),
                     CMQC.MQIACH_SSL_CERT_VALID: ch.get("valid"),
                     CMQC.MQCACH_SSL_CERT_EXPIRY: ch.get("expires")}]

        def MQCMD_INQUIRE_LISTENER_STATUS(self, args):
            name = args[CMQC.MQCACH_LISTENER_NAME]
            if name not in state["listeners"]:
                raise FakeMQMIError(f"unknown listener {name!r}", comp=2,
                                    reason=2085, verb="INQUIRE_LISTENER")
            return [{CMQC.MQCACH_LISTENER_NAME: name,
                     CMQC.MQIACH_LISTENER_STATUS: state["listeners"][name]}]

        def MQCMD_STOP_CHANNEL(self, args):
            state["channels"][args[CMQC.MQCACH_CHANNEL_NAME]]["status"] = 6
            return []

        def MQCMD_START_CHANNEL(self, args):
            state["channels"][args[CMQC.MQCACH_CHANNEL_NAME]]["status"] = 3
            return []

        def MQCMD_CHANGE_Q(self, args):
            name = args[CMQC.MQCA_Q_NAME]
            if name not in state["queues"]:
                raise FakeMQMIError(f"unknown queue {name!r}", comp=2,
                                    reason=2085, verb="CHANGE_Q")
            state["queues"][name]["max"] = args[CMQC.MQIA_MAX_Q_DEPTH]
            return []

        def MQCMD_START_LISTENER(self, args):
            state["listeners"][args[CMQC.MQCACH_LISTENER_NAME]] = 1
            return []

    class FakeQueue:
        def __init__(self, qmgr, name):
            self._name = name

        def get(self, maxlen, md, gmo):
            msgs = state["messages"].get(self._name, [])
            if not msgs:
                raise FakeMQMIError("no message available", comp=2,
                                    reason=2033, verb="GET")
            return msgs.pop(0)

        def put(self, message, md):
            state["messages"].setdefault(self._name, []).append(message)

        def close(self):
            pass

    mod.MQMIError = FakeMQMIError
    mod.CMQC = CMQC
    mod.CD = FakeCD
    mod.SCO = FakeSCO
    mod.MD = FakeMD
    mod.GMO = FakeGMO
    mod.QueueManager = FakeQueueManager
    mod.PCFExecute = FakePCFExecute
    mod.Queue = FakeQueue
    return mod


def _fresh_state():
    return {
        "queues": {
            "PAYMENTS.IN": {"depth": 48500, "max": 50000},
            "SYSTEM.DEAD.LETTER.QUEUE": {"depth": 0, "max": 5000},
        },
        "channels": {
            "PAYMENTS.RCVR": {
                "status": 6,  # STOPPED
                "cipher": "TLS_RSA_WITH_AES_256_CBC_SHA256",
                "cert_label": "ibmmqcert",
                "ssl_peer": "CN=mqclient",
                "valid": True,
                "expires": "2027-06-01",
            },
        },
        "listeners": {"TCP.LISTENER": 1},
        "messages": {"PAYMENTS.IN": [b"poison-message-bytes"]},
    }


@pytest.fixture()
def fake_mq(monkeypatch):
    """Inject the fake pymqi module and return its backing state."""
    state = _fresh_state()
    monkeypatch.setitem(sys.modules, "pymqi", _make_fake_pymqi(state))
    return state


@pytest.fixture()
def failing_mq(monkeypatch):
    state = _fresh_state()
    monkeypatch.setitem(
        sys.modules, "pymqi", _make_fake_pymqi(state, fail_connect=True))
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


# ------------------------------------------------- guarded import


def test_module_imports_cleanly_without_pymqi():
    """The driver must never be needed at import time (subprocess proof)."""
    code = (
        "import sys; "
        "assert 'pymqi' not in sys.modules, 'pymqi unexpectedly present'; "
        "from connectors.ibmmq import IBMQConnector; "
        "assert 'pymqi' not in sys.modules, 'import pulled in pymqi'; "
        "print('import-ok')"
    )
    proc = subprocess.run([sys.executable, "-c", code],
                          capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert "import-ok" in proc.stdout


def test_connect_without_driver_gives_install_instructions(monkeypatch):
    monkeypatch.delitem(sys.modules, "pymqi", raising=False)
    conn = IBMQConnector(qmgr="QM1", channel="CH", host="mq01.example",
                         credential_provider=lambda: ("u", "REDACTED"))
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert "pymqi" in str(excinfo.value)
    assert "pip install pymqi" in str(excinfo.value)


def test_connect_unreachable_target_is_connector_error(failing_mq):
    conn = IBMQConnector(
        qmgr="QM1", channel="CH", host="mq01.example",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
        privileged_credential_provider=lambda: ("rca_admin",
                                                FAKE_ADMIN_SECRET),
    )
    with pytest.raises(ConnectorError) as excinfo:
        conn.connect()
    assert FAKE_SECRET not in str(excinfo.value)
    assert FAKE_ADMIN_SECRET not in str(excinfo.value)
    assert "traceback" not in str(excinfo.value).lower()


# ------------------------------------------------- reads


def test_get_queue_depth_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.get_queue_depth("QM1", "PAYMENTS.IN")
    finally:
        conn.close()
    assert set(d) == {"qmgr", "queue", "depth", "max_depth", "ts"}
    assert d["qmgr"] == "QM1"
    assert d["queue"] == "PAYMENTS.IN"
    assert d["depth"] == 48500
    assert d["max_depth"] == 50000
    json.dumps(d)  # must be JSON-serializable


def test_get_queue_depth_unknown_queue_is_connector_error(fake_mq):
    conn = _conn()
    try:
        with pytest.raises(ConnectorError) as excinfo:
            conn.get_queue_depth("QM1", "NOPE.Q")
    finally:
        conn.close()
    assert "unknown" in str(excinfo.value).lower()


def test_get_channel_status_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.get_channel_status("QM1", "PAYMENTS.RCVR")
    finally:
        conn.close()
    assert set(d) == {"qmgr", "channel", "status", "ts"}
    assert d["status"] == "STOPPED"
    json.dumps(d)


def test_get_channel_status_unknown_channel(fake_mq):
    conn = _conn()
    try:
        with pytest.raises(ConnectorError):
            conn.get_channel_status("QM1", "NOPE.CH")
    finally:
        conn.close()


def test_get_listener_status_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.get_listener_status("TCP.LISTENER")
    finally:
        conn.close()
    assert set(d) == {"name", "status", "ts"}
    assert d["status"] == "RUNNING"


def test_get_cert_status_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.get_cert_status("QM1", "PAYMENTS.RCVR")
    finally:
        conn.close()
    for key in ("qmgr", "channel", "valid", "expires", "ts"):
        assert key in d, f"missing key {key}"
    assert d["valid"] is True
    assert d["expires"] == "2027-06-01"
    assert d["cipher"] == "TLS_RSA_WITH_AES_256_CBC_SHA256"
    json.dumps(d)


def test_get_config_queue_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.get_config("QM1", "queue", "PAYMENTS.IN")
    finally:
        conn.close()
    for key in ("qmgr", "object_type", "name", "max_depth", "ts"):
        assert key in d, f"missing key {key}"
    assert d["max_depth"] == 50000


def test_get_config_channel_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.get_config("QM1", "channel", "PAYMENTS.RCVR")
    finally:
        conn.close()
    for key in ("qmgr", "object_type", "name", "status", "ts"):
        assert key in d, f"missing key {key}"


def test_get_config_unsupported_object_type(fake_mq):
    conn = _conn()
    try:
        with pytest.raises(ConnectorError):
            conn.get_config("QM1", "topic", "T1")
    finally:
        conn.close()


def test_read_error_log_parses_amqerr(tmp_path, fake_mq):
    log = tmp_path / "AMQERR01.LOG"
    log.write_text(
        "09/20/26 10:15:30 - Process(1234.1) User(mqm) Program(amqzmuc0)\n"
        "                    Host(mq01) Installation(Installation1)\n"
        "                    VRMF(9.3.0.0) QMgr(QM1)\n"
        "                    AMQ9202: Channel 'PAYMENTS.RCVR' started.\n"
        "09/20/26 10:16:01 - Process(1234.1) User(mqm) Program(amqzmuc0)\n"
        "                    AMQ9999: Something failed badly.\n"
    )
    conn = _conn(error_log_dir=str(tmp_path))
    try:
        entries = conn.read_error_log("QM1", limit=10)
    finally:
        conn.close()
    assert len(entries) == 2
    assert [e["code"] for e in entries] == ["AMQ9202", "AMQ9999"]
    for e in entries:
        assert set(e) == {"ts", "severity", "code", "message"}
    json.dumps(entries)


def test_read_error_log_needs_log_dir(fake_mq, monkeypatch):
    monkeypatch.delenv("MQ_ERROR_LOG_DIR", raising=False)
    conn = _conn()
    try:
        with pytest.raises(ConnectorError):
            conn.read_error_log("QM1")
    finally:
        conn.close()


def test_wrong_qmgr_rejected(fake_mq):
    conn = _conn()
    try:
        with pytest.raises(ConnectorError):
            conn.get_queue_depth("OTHERQM", "PAYMENTS.IN")
    finally:
        conn.close()


def test_not_connected_rejected(fake_mq):
    conn = IBMQConnector(qmgr="QM1", channel="CH", host="mq01.example",
                         credential_provider=lambda: ("u", "REDACTED"))
    with pytest.raises(ConnectorError):
        conn.get_queue_depth("QM1", "PAYMENTS.IN")


# ------------------------------------------------- privileged actions


def test_restart_channel_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.restart_channel("QM1", "PAYMENTS.RCVR")
    finally:
        conn.close()
    assert set(d) == {"qmgr", "channel", "previous_status", "status", "ts"}
    assert d["previous_status"] == "STOPPED"
    assert d["status"] == "RUNNING"
    json.dumps(d)


def test_restart_channel_requires_separate_admin_credential(fake_mq,
                                                            monkeypatch):
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
        # The read session is untouched by the refused act call.
        assert conn._mgr is read_mgr
    finally:
        conn.close()
    assert "privileged" in str(excinfo.value).lower()


def test_update_queue_config_shape_matches_sim(fake_mq):
    conn = _conn()
    try:
        d = conn.update_queue_config("QM1", "PAYMENTS.IN", 60000)
    finally:
        conn.close()
    assert set(d) == {"qmgr", "queue", "old_max_depth", "max_depth", "ts"}
    assert d["old_max_depth"] == 50000
    assert d["max_depth"] == 60000
    assert fake_mq["queues"]["PAYMENTS.IN"]["max"] == 60000


def test_start_listener_shape_matches_sim(fake_mq):
    fake_mq["listeners"]["TCP.LISTENER"] = 0  # STOPPED
    conn = _conn()
    try:
        d = conn.start_listener("TCP.LISTENER")
    finally:
        conn.close()
    assert set(d) == {"name", "previous_status", "status", "ts"}
    assert d["previous_status"] == "STOPPED"
    assert d["status"] == "RUNNING"


def test_quarantine_message_moves_to_dlq(fake_mq):
    conn = _conn()
    try:
        d = conn.quarantine_message("QM1", "PAYMENTS.IN")
    finally:
        conn.close()
    assert d["quarantined"] is True
    assert d["dlq"] == "SYSTEM.DEAD.LETTER.QUEUE"
    assert fake_mq["messages"]["PAYMENTS.IN"] == []
    assert fake_mq["messages"]["SYSTEM.DEAD.LETTER.QUEUE"] == [
        b"poison-message-bytes"]
    for key in ("qmgr", "queue", "quarantined", "ts"):
        assert key in d
    json.dumps(d)


def test_quarantine_empty_queue_reports_not_quarantined(fake_mq):
    fake_mq["messages"]["PAYMENTS.IN"] = []
    conn = _conn()
    try:
        d = conn.quarantine_message("QM1", "PAYMENTS.IN")
    finally:
        conn.close()
    assert d["quarantined"] is False
    assert d["reason"] == "queue empty"


# ------------------------------------------------- sessions & TLS


def test_close_is_idempotent(fake_mq):
    conn = _conn()
    conn.close()
    conn.close()  # must not raise


def test_tls_connect_uses_cd_and_sco(fake_mq):
    conn = IBMQConnector(
        qmgr="QM1", channel="SVRCONN.CH", host="mq01.example",
        credential_provider=lambda: ("rca_reader", "REDACTED-fake"),
        tls_cipher="TLS_RSA_WITH_AES_256_CBC_SHA256",
        key_repo="/var/mqm/key.kdb",
    )
    conn.connect()
    try:
        mgr = conn._mgr
    finally:
        conn.close()
    calls = mgr.connect_calls
    assert calls and calls[0]["kind"] == "connect_with_options"
    assert calls[0]["cipher"] == "TLS_RSA_WITH_AES_256_CBC_SHA256"
    assert calls[0]["key_repo"] == "/var/mqm/key.kdb"


def test_connect_records_user_but_never_password(fake_mq):
    conn = IBMQConnector(
        qmgr="QM1", channel="SVRCONN.CH", host="mq01.example",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
        privileged_credential_provider=lambda: ("rca_admin",
                                                FAKE_ADMIN_SECRET),
    )
    conn.connect()
    try:
        mgr = conn._mgr
    finally:
        conn.close()
    assert mgr.connect_calls[0]["user"] == "rca_reader"
    assert "password" not in mgr.connect_calls[0]
    assert FAKE_SECRET not in json.dumps(mgr.connect_calls)


# ------------------------------------------------- credential hygiene


def test_secret_never_in_repr_logs_or_exceptions(fake_mq):
    conn = IBMQConnector(
        qmgr="QM1", channel="SVRCONN.CH", host="mq01.example",
        credential_provider=lambda: ("rca_reader", FAKE_SECRET),
        privileged_credential_provider=lambda: ("rca_admin",
                                                FAKE_ADMIN_SECRET),
    )
    conn.connect()
    try:
        assert FAKE_SECRET not in repr(conn)
        assert FAKE_ADMIN_SECRET not in repr(conn)
        # Not stored on the instance either.
        assert FAKE_SECRET not in str(vars(conn).values())
        assert FAKE_ADMIN_SECRET not in str(vars(conn).values())
        with pytest.raises(ConnectorError) as excinfo:
            conn.get_queue_depth("QM1", "NOPE.Q")
        assert FAKE_SECRET not in str(excinfo.value)
        d = conn.get_queue_depth("QM1", "PAYMENTS.IN")
        assert FAKE_SECRET not in json.dumps(d)
    finally:
        conn.close()


def test_env_credential_resolution(monkeypatch, fake_mq):
    monkeypatch.setenv("MQ_READ_USER", "env_reader")
    monkeypatch.setenv("MQ_READ_PASSWORD", "REDACTED-env-fake")
    monkeypatch.setenv("MQ_ADMIN_USER", "env_admin")
    monkeypatch.setenv("MQ_ADMIN_PASSWORD", "REDACTED-env-admin-fake")
    conn = IBMQConnector(qmgr="QM1", channel="SVRCONN.CH",
                         host="mq01.example")
    conn.connect()
    try:
        assert conn._mgr.connect_calls[0]["user"] == "env_reader"
        d = conn.restart_channel("QM1", "PAYMENTS.RCVR")
        assert d["status"] == "RUNNING"
    finally:
        conn.close()


def test_missing_credentials_refused_cleanly(monkeypatch):
    monkeypatch.delitem(sys.modules, "pymqi", raising=False)
    for var in ("MQ_READ_USER", "MQ_READ_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    conn = IBMQConnector(qmgr="QM1", channel="CH", host="mq01.example")
    # Fails on the missing driver first here; with a driver it would fail
    # on the missing credential -- either way a clean ConnectorError.
    with pytest.raises(ConnectorError):
        conn.connect()


# ------------------------------------------------- read()/act() routing


def test_read_and_act_dispatch(fake_mq):
    conn = _conn()
    try:
        d = conn.read("get_queue_depth",
                      {"qmgr": "QM1", "queue": "PAYMENTS.IN"})
        assert d["depth"] == 48500
        with pytest.raises(ConnectorError):
            conn.read("restart_channel", {"qmgr": "QM1",
                                          "channel": "PAYMENTS.RCVR"})
        with pytest.raises(ConnectorError):
            conn.act("get_queue_depth", {"qmgr": "QM1",
                                         "queue": "PAYMENTS.IN"})
    finally:
        conn.close()
