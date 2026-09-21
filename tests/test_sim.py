"""Tests for the simulated middleware estate and incident injector."""

import pytest

from sim import SCENARIOS, Estate, inject


def test_baseline_estate():
    e = Estate(seed=42)
    assert e.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"] == 1200
    assert e.get_channel_status("QMGR1", "PAYMENTS.RCVR")["status"] == "RUNNING"
    assert set(e.list_queues("QMGR1")) >= {"PAYMENTS.IN", "PAYMENTS.OUT", "ORDERS.IN"}
    assert set(e.list_channels("QMGR1")) >= {"PAYMENTS.RCVR", "PAYMENTS.SDR"}
    m = e.get_host_metrics("app01")
    assert m["disk_pct"] < 90
    assert e.get_kafka_consumer_lag("payments-events", "payments-consumers")["lag"] < 1000


def test_unknown_names_raise():
    e = Estate()
    with pytest.raises(ValueError):
        e.get_queue_depth("QMGR1", "NOPE.Q")
    with pytest.raises(ValueError):
        e.get_channel_status("QMGR1", "NOPE.CH")
    with pytest.raises(ValueError):
        e.get_host_metrics("nope-host")


def test_channel_stopped_scenario():
    e = Estate()
    alert = inject(e, "channel_stopped")
    assert alert["type"] == "queue_backlog"
    assert alert["queue"] == "PAYMENTS.IN"
    assert e.get_channel_status("QMGR1", "PAYMENTS.RCVR")["status"] == "STOPPED"
    assert e.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"] == 48500
    msgs = " ".join(x["message"] for x in e.read_error_log("QMGR1", limit=20))
    assert "stopped" in msgs.lower()


def test_backlog_grows_while_stopped_and_drains_when_running():
    e = Estate()
    inject(e, "channel_stopped")
    d0 = e.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"]
    e.tick(600)
    d1 = e.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"]
    assert d1 >= d0  # still stopped: backlog does not shrink
    e.restart_channel("QMGR1", "PAYMENTS.RCVR")
    assert e.get_channel_status("QMGR1", "PAYMENTS.RCVR")["status"] == "RUNNING"
    for _ in range(40):
        e.tick(60)
    d2 = e.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"]
    assert d2 < d1


def test_disk_full_scenario():
    e = Estate()
    alert = inject(e, "disk_full")
    assert alert["type"] == "tomcat_errors"
    assert e.get_host_metrics("app01")["disk_pct"] >= 90
    msgs = " ".join(x["message"] for x in e.read_app_log("payments-api", limit=20))
    assert "No space left on device" in msgs


def test_kafka_lag_scenario():
    e = Estate()
    alert = inject(e, "kafka_lag")
    assert alert["type"] == "kafka_lag"
    assert e.get_kafka_consumer_lag("payments-events", "payments-consumers")["lag"] > 50000


def test_unknown_scenario_raises():
    with pytest.raises(ValueError) as exc:
        inject(Estate(), "meteor_strike")
    assert "channel_stopped" in str(exc.value)


def test_determinism():
    e1, e2 = Estate(seed=7), Estate(seed=7)
    inject(e1, "channel_stopped")
    inject(e2, "channel_stopped")
    assert (e1.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"]
            == e2.get_queue_depth("QMGR1", "PAYMENTS.IN")["depth"])
    assert e1.read_error_log("QMGR1", limit=5) == e2.read_error_log("QMGR1", limit=5)


def test_poison_log_flag_adds_inert_entry():
    e = Estate()
    inject(e, "channel_stopped", poison_log=True)
    msgs = [x["message"] for x in e.read_error_log("QMGR1", limit=50)]
    assert any("ignore previous instructions" in m for m in msgs)
    # Without the flag, no such entry exists.
    e2 = Estate()
    inject(e2, "channel_stopped")
    msgs2 = [x["message"] for x in e2.read_error_log("QMGR1", limit=50)]
    assert not any("ignore previous instructions" in m for m in msgs2)
