"""Tests for the demo CLI: flag defaults, approval paths, and plan flow."""

import io

from demo.run_incident import build_parser, main


def test_auto_approve_defaults_off():
    parser = build_parser()
    assert parser.get_default("auto_approve") is False
    args = parser.parse_args([])
    assert args.auto_approve is False
    assert args.scenario == "channel_stopped"


def test_demo_reject_path(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
    rc = main(["--scenario", "channel_stopped"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "REJECTED" in out
    assert "No privileged tool was invoked" in out
    assert "The incident remains open" in out
    assert "VALID" in out  # audit chain valid
    logs = list((tmp_path / "runs").rglob("audit.jsonl"))
    assert len(logs) == 1


def test_demo_reject_second_step(tmp_path, monkeypatch, capsys):
    # disk_full plan has 2 steps: approve step 1, reject step 2.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO("y\nn\n"))
    rc = main(["--scenario", "disk_full"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "halted_rejected" in out
    assert "REJECTED" in out
    assert "VALID" in out


def test_demo_auto_approve_path(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    rc = main(["--scenario", "channel_stopped", "--auto-approve"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Recovery confirmed" in out
    assert "VALID" in out
    logs = list((tmp_path / "runs").rglob("audit.jsonl"))
    assert len(logs) == 1


def test_demo_approve_all_via_prompt(tmp_path, monkeypatch, capsys):
    # expired_tls plan has 2 steps; "a" approves all remaining at step 1.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO("a\n"))
    rc = main(["--scenario", "expired_tls"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "completed" in out
    assert "Recovery confirmed" in out
    assert "VALID" in out


def test_demo_all_scenarios_run(tmp_path, monkeypatch, capsys):
    for scenario in ("channel_stopped", "disk_full", "kafka_lag", "tomcat_oom",
                     "expired_tls", "config_drift", "listener_down",
                     "poison_message"):
        monkeypatch.chdir(tmp_path)
        rc = main(["--scenario", scenario, "--auto-approve"])
        assert rc == 0, scenario
        out = capsys.readouterr().out
        assert "VALID" in out, scenario
