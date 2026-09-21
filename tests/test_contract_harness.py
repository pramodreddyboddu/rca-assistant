"""Unit tests for the connector self-test harness (connectors/harness.py).

The harness itself is an operational tool (not pytest), so these tests
verify the harness: a deliberately BROKEN fake connector must FAIL the
corresponding checks, the real registered connectors must PASS, the
generic auto-builder must handle a registry entry with no explicit
builder, and the CLI must exit 0/ nonzero correctly.

No network, no real credentials, no drivers required.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from connectors import CONNECTORS, Connector, ConnectorError, ConnectorSpec
from connectors import harness
from connectors.harness import CheckResult, run_checks

REPO = Path(harness.__file__).resolve().parents[1]
PROBE_SECRET = "s3cr3t-value"  # placeholder "secret"; never real


class BrokenConnector(Connector):
    """Deliberately broken on every axis the harness checks.

    - capabilities() lists an unknown tool -> capabilities FAIL
    - read() is overridden to reach act-path code -> read-act-isolation FAIL
    - __init__ takes a raw ``password`` -> ctor-secret-params FAIL
    - repr() and an attribute leak the probe secret -> repr/instance FAILs
    - connect() never touches a driver -> driver-absent-connect FAIL
    - act() never refuses -> privileged-refusal FAIL
    """

    name = "broken"

    SPEC = ConnectorSpec(
        name="broken",
        display_name="Broken (test double)",
        description="Deliberately violates the connector contract.",
        credential_refs=("BROKEN_PASSWORD",),
    )

    def __init__(self, password):
        self._password = password
        self._leak = PROBE_SECRET

    def capabilities(self):
        return {"no_such_tool_xyz", "restart_channel"}

    def connect(self):
        pass

    def close(self):
        pass

    def no_such_tool_xyz(self):
        """Documented unknown tool (still unknown to TOOL_NAMES)."""
        return {}

    def restart_channel(self, qmgr, channel):
        """Privileged tool with no credential refusal."""
        return {"acted": True, "qmgr": qmgr, "channel": channel}

    def read(self, resource, params):
        # Sloppy override: read-path reaches act-path code.
        return self.restart_channel("q", "c")

    def __repr__(self):
        return f"BrokenConnector(leak={self._leak!r})"


def _broken_builder():
    return harness._Builder(
        build=lambda: BrokenConnector(password=PROBE_SECRET),
        sentinels=frozenset({PROBE_SECRET}),
        driver_module="no_such_driver_xyz",
    )


def _results_by_check(results):
    return {r.check: r for r in results}


# ------------------------------------------------- the broken fake fails


def test_broken_connector_fails_expected_checks():
    results = harness.check_connector(
        "broken", BrokenConnector.SPEC, BrokenConnector, _broken_builder())
    by_check = _results_by_check(results)
    assert [r.check for r in results] == list(harness.CHECKS)
    for check in ("capabilities",
                  "read-act-isolation",
                  "ctor-secret-params",
                  "repr-hygiene",
                  "driver-absent-connect",
                  "instance-secret-scan",
                  "privileged-refusal"):
        assert by_check[check].status == "fail", (
            f"expected {check} to FAIL, got {by_check[check]}")
    # Sane parts of the fake still pass / are informational.
    assert by_check["construction"].status == "pass"
    assert by_check["registration"].status == "pass"
    assert by_check["result-shape-boundary"].status == "pass"


def test_broken_connector_failure_details_name_the_problem():
    results = harness.check_connector(
        "broken", BrokenConnector.SPEC, BrokenConnector, _broken_builder())
    by_check = _results_by_check(results)
    assert "no_such_tool_xyz" in by_check["capabilities"].detail
    assert "password" in by_check["ctor-secret-params"].detail
    assert "act-path code" in by_check["read-act-isolation"].detail


# ------------------------------------------------- real connectors pass


def test_registered_connectors_pass_harness():
    # Wave 2: all 21 registered connectors satisfy the contract; the
    # harness must be fully green. Connectors without an explicit probe
    # builder run through the harness's generic auto-builder.
    results = run_checks()
    by_conn: dict[str, list[CheckResult]] = {}
    for r in results:
        by_conn.setdefault(r.connector, []).append(r)
    assert set(by_conn) == {
        "ibmmq", "kafka", "linux-host", "tomcat",
        "websphere", "weblogic", "jboss",
        "rabbitmq", "artemis", "tibco_ems",
        "nginx", "apache", "haproxy",
        "postgres", "mysql", "oracle_db",
        "redis", "elasticsearch", "mongodb",
        "kubernetes", "docker",
    }
    for name, conn_results in by_conn.items():
        fails = [r for r in conn_results if r.status == "fail"]
        assert not fails, (
            f"harness failures for {name}:\n" +
            "\n".join(f"  [{r.check}] {r.detail}" for r in fails))


def test_run_checks_single_connector_and_unknown_name():
    results = run_checks("ibmmq")
    assert {r.connector for r in results} == {"ibmmq"}
    assert all(r.status != "fail" for r in results)
    with pytest.raises(ValueError):
        run_checks("no-such-connector")


# ------------------------------------------------- auto-builder for future connectors


class ToyConnector(Connector):
    """A future-style connector with no explicit harness builder."""

    name = "toy"

    SPEC = ConnectorSpec(
        name="toy",
        display_name="Toy",
        description="Exercises the generic auto-builder.",
        credential_refs=("TOY_USER_ENV",),
    )

    def __init__(self, host, port: int = 9092,
                 credential_provider=None):
        self._host = host
        self._port = port
        self._credential_provider = credential_provider

    def capabilities(self):
        return {"get_host_metrics"}

    def connect(self):
        pass

    def close(self):
        pass

    def get_host_metrics(self, host: str = "localhost"):
        """Toy read tool."""
        return {"host": host}

    def __repr__(self):
        return f"ToyConnector(host={self._host!r})"


def test_auto_builder_handles_unlisted_connector():
    builder = harness._auto_builder(ToyConnector)
    inst = builder.build()
    assert isinstance(inst, Connector)
    assert builder.sentinels == frozenset({harness.PROBE_SECRET})
    results = harness.check_connector(
        "toy", ToyConnector.SPEC, ToyConnector, builder)
    fails = [r for r in results if r.status == "fail"]
    assert not fails, [r for r in fails]


# ------------------------------------------------- CLI smoke tests


def _cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "connectors.harness", *args],
        cwd=REPO, capture_output=True, text=True, timeout=120,
    )


def test_cli_exit_codes():
    ok = _cli("--connector", "linux-host")
    assert ok.returncode == 0, ok.stderr
    assert "[PASS]" in ok.stdout
    assert "summary:" in ok.stdout
    unknown = _cli("--connector", "bogus")
    assert unknown.returncode == 2
    assert "unknown connector" in unknown.stderr


def test_cli_returns_nonzero_when_a_check_fails(monkeypatch, capsys):
    monkeypatch.setattr(
        harness, "run_checks",
        lambda name=None: [CheckResult(connector="ibmmq", check="capabilities",
                                       status="fail", detail="boom")])
    assert harness.main([]) == 1
    out = capsys.readouterr().out
    assert "[FAIL]" in out


def test_cli_json_is_machine_readable():
    proc = _cli("--connector", "linux-host", "--json")
    assert proc.returncode == 0, proc.stderr
    data = json.loads(proc.stdout)
    assert data["summary"]["failed"] == 0
    assert data["summary"]["connectors"] == 1
    checks = {r["check"]: r["status"] for r in data["results"]}
    assert checks["construction"] == "pass"
    assert checks["privileged-refusal"] == "skip"  # no privileged caps
    assert all(set(r) == {"connector", "check", "status", "detail"}
               for r in data["results"])
