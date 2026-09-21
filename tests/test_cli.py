"""Tests for the rca command-line interface (rca_assistant.cli)."""

import pytest

from rca_assistant.cli import main
from rca_assistant import __version__


def _help_exits_zero(argv):
    """argparse --help raises SystemExit(0); assert the code."""
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 0


def test_top_level_help():
    _help_exits_zero(["--help"])


@pytest.mark.parametrize("argv", [
    ["demo", "--help"],
    ["diagnose", "--help"],
    ["connectors", "--help"],
    ["connectors", "list", "--help"],
    ["connectors", "self-test", "--help"],
    ["serve", "--help"],
    ["scenarios", "--help"],
    ["version", "--help"],
])
def test_subcommand_help(argv):
    _help_exits_zero(argv)


def test_no_args_prints_help_and_exits_zero(capsys):
    assert main([]) == 0
    assert "60-second path" in capsys.readouterr().out


def test_version(capsys):
    assert main(["version"]) == 0
    out = capsys.readouterr().out
    assert f"rca-assistant {__version__}" in out
    assert __version__ == "0.7.0"


def test_connectors_list_shows_all_21(capsys):
    from connectors.base import CONNECTORS

    assert len(CONNECTORS) == 21
    assert main(["connectors", "list"]) == 0
    out = capsys.readouterr().out
    for name in CONNECTORS:
        assert name in out, f"connector {name!r} missing from list output"
    assert "21" in out


def test_scenarios_lists_all_45(capsys):
    from sim import SCENARIOS

    assert len(SCENARIOS) == 45
    assert main(["scenarios"]) == 0
    out = capsys.readouterr().out
    for name in SCENARIOS:
        assert name in out, f"scenario {name!r} missing from output"


def test_connectors_self_test_single_connector():
    # One fast connector; the full harness run belongs to CI.
    assert main(["connectors", "self-test",
                 "--connector", "linux-host"]) == 0


def test_connectors_self_test_unknown_connector():
    assert main(["connectors", "self-test",
                 "--connector", "no_such_connector"]) == 2


def test_diagnose_completes_with_top_hypothesis(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["diagnose", "--scenario", "channel_stopped"]) == 0
    out = capsys.readouterr().out
    assert "Top hypothesis" in out
    assert "score" in out
    assert "cited evidence" in out
    assert "No remediation was proposed or executed" in out


def test_diagnose_rejects_unknown_scenario_with_friendly_error(
        tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    from sim import SCENARIOS

    assert main(["diagnose", "--scenario", "bogus_scenario"]) == 2
    err = capsys.readouterr().err
    assert "unknown scenario" in err
    for name in SCENARIOS:
        assert name in err


def test_demo_noninteractive_auto_approves(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["demo", "--scenario", "channel_stopped"]) == 0
    out = capsys.readouterr().out
    assert "AUTO-APPROVED" in out  # the demo notice
    assert "Recovery confirmed" in out
    # run_incident writes its audit log under runs/ in the cwd (tmp_path).
    assert list((tmp_path / "runs").rglob("audit.jsonl"))


def test_demo_interactive_flag_passes_through(monkeypatch, tmp_path, capsys):
    # --interactive must NOT auto-approve: answer "n" to the first prompt.
    import io

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
    assert main(["demo", "--scenario", "channel_stopped",
                 "--interactive"]) == 0
    out = capsys.readouterr().out
    assert "interactive" in out
    assert "AUTO-APPROVED" not in out
    assert "REJECTED" in out
