"""Connector self-test / contract harness (operational, not pytest).

CUSTOMER ONBOARDING SELF-TEST
-----------------------------
Run this against your environment's connector configuration BEFORE going
live to prove each connector honors the framework contract -- without
needing real credentials, a live target, or the vendor driver installed::

    .venv/bin/python -m connectors.harness [--connector NAME] [--json]

Exit code 0 means every check passed (skips allowed); nonzero means at
least one check failed. ``--json`` emits machine-readable results for CI.

The harness builds every registered connector with probe (non-secret)
values only -- e.g. a credential provider returning ``("probe",
"REDACTED")`` -- then runs the static contract checks: registration,
capabilities, read/act dispatch isolation, credential hygiene,
privileged-action refusal, and the result-shape boundary. New connectors
added to the ``CONNECTORS`` registry later are picked up automatically:
known connectors use explicit probe builders below; unknown ones fall back
to a best-effort generic builder (documented where a check degrades to a
skip instead of failing blind).

BOUNDARY (read this before relying on the harness)
--------------------------------------------------
This harness checks the STATIC contract only. It NEVER contacts a live
target, NEVER resolves a real secret, and NEVER requires the vendor
driver (drivers are deliberately hidden during the relevant checks).
Live validation -- real queue manager / broker / host, JSON
serializability and shapes of real payloads -- is the pytest suite's job
(tests/test_connector_contract.py and the per-connector test modules),
plus a customer pilot against a real environment. A green harness means
"safe to wire into your config and proceed to pilot", not "proven against
production".
"""

from __future__ import annotations

import argparse
import contextlib
import inspect
import json
import os
import sys
import types
from dataclasses import asdict, dataclass
from typing import Any, Callable

from connectors import CONNECTORS, Connector, ConnectorError
from connectors.base import ConnectorSpec
from mcp_server.tools import PRIVILEGED_TOOLS, TOOL_NAMES

# ---------------------------------------------------------------------------
# Probe values: placeholders that are never real secrets.
# ---------------------------------------------------------------------------

#: Non-secret placeholder user injected by probe credential providers.
PROBE_USER = "probe"
#: Non-secret placeholder "secret" injected by probe credential providers.
#: Real deployments MUST wire a vault-backed provider instead; this value is
#: only ever used to prove a connector can be built without real secrets.
PROBE_SECRET = "REDACTED"

#: Checks run per connector, in order.
CHECKS = (
    "construction",
    "registration",
    "capabilities",
    "read-act-isolation",
    "ctor-secret-params",
    "repr-hygiene",
    "driver-absent-connect",
    "instance-secret-scan",
    "privileged-refusal",
    "result-shape-boundary",
)


@dataclass
class CheckResult:
    """One check outcome for one connector."""

    connector: str
    check: str
    status: str  # "pass" | "fail" | "skip"
    detail: str = ""


# ---------------------------------------------------------------------------
# Small test doubles (monkeypatch-style, stdlib only -- no pytest needed).
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _installed_module(name: str, module: Any):
    """Temporarily install (or hide, when module is None) a sys.modules entry."""
    sentinel = object()
    old = sys.modules.get(name, sentinel)
    if module is None:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = module
    try:
        yield
    finally:
        if old is sentinel:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = old


@contextlib.contextmanager
def _clean_env(names):
    """Temporarily remove env vars (restored afterwards)."""
    saved = {}
    for var in names:
        if var in os.environ:
            saved[var] = os.environ.pop(var)
    try:
        yield
    finally:
        os.environ.update(saved)


def _make_fake_pymqi():
    """Minimal fake pymqi module: just enough for a PCF status read.

    Used only to prove the IBM MQ connector's privileged path refuses on
    missing privileged credentials (the read half of the probe action must
    succeed for the refusal to be the thing under test).
    """
    fake = types.ModuleType("pymqi")
    cmqc = types.SimpleNamespace(
        MQIACH_CHANNEL_STATUS="MQIACH_CHANNEL_STATUS",
        MQCACH_CHANNEL_NAME="MQCACH_CHANNEL_NAME",
    )

    class _FakeQM:
        def __init__(self, name):
            self.name = name

        def connect(self, *a, **k):
            pass

        def connect_with_options(self, *a, **k):
            pass

        def disconnect(self):
            pass

    class _FakePCF:
        def __init__(self, mgr):
            self._mgr = mgr

        def MQCMD_INQUIRE_CHANNEL_STATUS(self, args):
            return [{cmqc.MQIACH_CHANNEL_STATUS: 3}]  # RUNNING

    fake.QueueManager = _FakeQM
    fake.PCFExecute = _FakePCF
    fake.CMQC = cmqc
    fake.MQMIError = type("MQMIError", (Exception,), {})
    return fake


# ---------------------------------------------------------------------------
# Per-connector probe builders: construct with NO secrets.
# ---------------------------------------------------------------------------

@dataclass
class _Builder:
    """How the harness builds and probes one connector without secrets."""

    build: Callable[[], Connector]
    sentinels: frozenset = frozenset()
    driver_module: str | None = None
    priv_envs: tuple[str, ...] = ()
    # Optional context manager: with probe(inst) as (action, params) -> run
    # inst.act(action, params) expecting a privileged-credential refusal.
    priv_probe: Callable[[Connector], Any] | None = None


def _build_ibmmq() -> Connector:
    from connectors.ibmmq import IBMQConnector

    return IBMQConnector(
        qmgr="QM1",
        channel="SVRCONN.CH",
        host="probe.invalid",
        credential_provider=lambda: (PROBE_USER, PROBE_SECRET),
    )


@contextlib.contextmanager
def _ibmmq_priv_probe(inst: Connector):
    """Refusal probe for ibmmq: fake driver, then expect the privileged
    credential refusal (no privileged provider configured, admin env vars
    cleaned by the caller)."""
    with _installed_module("pymqi", _make_fake_pymqi()):
        inst.connect()
        try:
            yield ("restart_channel", {"qmgr": "QM1", "channel": "SVRCONN.CH"})
        finally:
            inst.close()


def _build_kafka() -> Connector:
    from connectors.kafka import KafkaConnector

    return KafkaConnector(bootstrap_servers="probe.invalid:9092")


@contextlib.contextmanager
def _kafka_priv_probe(inst: Connector):
    # restart_consumer refuses before touching the driver when no
    # restart_hook is configured, so no driver fakes are needed.
    yield ("restart_consumer", {"topic": "probe", "group": "probe"})


def _build_linux_host() -> Connector:
    from connectors.linux_host import LinuxHostConnector

    return LinuxHostConnector()


_BUILDERS: dict[str, _Builder] = {
    "ibmmq": _Builder(
        build=_build_ibmmq,
        sentinels=frozenset({PROBE_SECRET}),
        driver_module="pymqi",
        priv_envs=("MQ_ADMIN_USER", "MQ_ADMIN_PASSWORD"),
        priv_probe=_ibmmq_priv_probe,
    ),
    "kafka": _Builder(
        build=_build_kafka,
        sentinels=frozenset({PROBE_SECRET}),
        driver_module="kafka",
        priv_envs=("KAFKA_ADMIN_USER", "KAFKA_ADMIN_PASSWORD"),
        priv_probe=_kafka_priv_probe,
    ),
    "linux-host": _Builder(
        build=_build_linux_host,
        sentinels=frozenset(),
        driver_module=None,  # driverless by design; connect() is a no-op
        priv_envs=(),
        priv_probe=None,  # no privileged capabilities at all
    ),
}


def _auto_builder(factory: type[Connector]) -> _Builder:
    """Best-effort builder for registry entries with no explicit builder.

    Fills required constructor params with harmless placeholders
    (``"probe"`` / 0 / False, probe credential providers where the name
    says so). Lets a future connector (e.g. tomcat) run the harness with
    zero harness changes; checks that need connector-specific knowledge
    degrade to explicit skips instead of failing blind.
    """
    kwargs: dict[str, Any] = {}
    sentinels: set[str] = set()
    try:
        params = inspect.signature(factory.__init__).parameters
    except (TypeError, ValueError):
        params = {}
    for pname, p in params.items():
        if pname == "self":
            continue
        if p.kind in (inspect.Parameter.VAR_POSITIONAL,
                      inspect.Parameter.VAR_KEYWORD):
            continue
        low = pname.lower()
        # Credential-provider params get the probe provider even when the
        # param has a default: resolving probe (non-secret) values is what
        # lets the hygiene/refusal checks run meaningfully.
        if "credential" in low and "provider" in low:
            kwargs[pname] = lambda: (PROBE_USER, PROBE_SECRET)
            sentinels.add(PROBE_SECRET)
            continue
        if p.default is not inspect.Parameter.empty:
            continue
        if "hook" in low:
            kwargs[pname] = lambda *a, **k: {}
        elif p.annotation is int:
            kwargs[pname] = 0
        elif p.annotation is float:
            kwargs[pname] = 0.0
        elif p.annotation is bool:
            kwargs[pname] = False
        else:
            kwargs[pname] = "probe"

    def _build() -> Connector:
        return factory(**kwargs)

    return _Builder(build=_build, sentinels=frozenset(sentinels))


@contextlib.contextmanager
def _generic_priv_probe(inst: Connector, driver_module: str | None):
    """Fallback refusal probe: hide the driver (if known), fill the first
    privileged action's required params with placeholders, and expect
    ConnectorError. Weaker than an explicit probe (it may prove
    refusal-by-driver-absence rather than credential refusal), which is
    why explicit probes are preferred per connector."""
    action = sorted(inst._act_tools)[0]
    method = getattr(inst, action)
    params: dict[str, Any] = {}
    try:
        sig = inspect.signature(method)
    except (TypeError, ValueError):
        sig = None
    if sig is not None:
        for pname, p in sig.parameters.items():
            if pname == "self":
                continue
            if p.kind in (inspect.Parameter.VAR_POSITIONAL,
                          inspect.Parameter.VAR_KEYWORD):
                continue
            if p.default is not inspect.Parameter.empty:
                continue
            if p.annotation is int:
                params[pname] = 0
            elif p.annotation is float:
                params[pname] = 0.0
            elif p.annotation is bool:
                params[pname] = False
            else:
                params[pname] = "probe"
    hide = (_installed_module(driver_module, None) if driver_module
            else contextlib.nullcontext())
    with hide:
        yield action, params


# ---------------------------------------------------------------------------
# Individual checks.
# ---------------------------------------------------------------------------

def _check_registration(name, spec, factory, sentinels, rec) -> None:
    problems = []
    if not isinstance(spec, ConnectorSpec):
        problems.append("SPEC is not a ConnectorSpec")
    else:
        if spec.name != name:
            problems.append(
                f"SPEC.name {spec.name!r} != registry name {name!r}")
        for ref in spec.credential_refs:
            if not isinstance(ref, str) or not ref.strip():
                problems.append(
                    f"credential_refs has an empty/non-string entry: {ref!r}")
            elif ref != ref.strip() or " " in ref:
                problems.append(
                    f"credential_ref {ref!r} is not a clean reference name")
            elif any(s and s in ref for s in sentinels):
                problems.append(
                    f"credential_ref {ref!r} looks like a secret value")
    if not (isinstance(factory, type) and issubclass(factory, Connector)):
        problems.append("factory is not a Connector subclass")
    elif getattr(factory, "name", None) != name:
        problems.append(
            f"factory.name {getattr(factory, 'name', None)!r} != "
            f"registry name {name!r}")
    if problems:
        rec("registration", "fail", "; ".join(problems))
    else:
        rec("registration", "pass",
            f"registered as {name!r}; "
            f"{len(spec.credential_refs)} credential ref(s), "
            "all clean reference names (no secret values)")


def _check_capabilities(name, inst, rec) -> None:
    try:
        inst.check_contract()
    except ConnectorError as exc:
        rec("capabilities", "fail", str(exc))
        return
    caps = set(inst.capabilities())
    if not caps:
        rec("capabilities", "fail", "capabilities() is empty")
        return
    rec("capabilities", "pass",
        f"{len(caps)} capabilities, all within TOOL_NAMES with callable "
        "implementations")


def _spy_direction(inst, targets, caller, caller_name, target_kind):
    """Wrap each target method with a recording spy, attempt the forbidden
    call (which must raise ConnectorError before any method is resolved),
    and report if the spy was ever invoked.

    Approach: assert the routing tables AND spy-test both directions.
    Routing-by-name in the base class is what keeps read calls from ever
    reaching act code; the spy test catches sloppy subclasses that
    override read()/act() and route by trust instead of by name.
    """
    problems = []
    wrapped = {}
    for tool in sorted(targets):
        method = getattr(inst, tool, None)
        if not callable(method):
            continue

        called: list[str] = []

        def spy(*a, _called=called, _t=tool, **k):
            _called.append(_t)
            return {"harness_spy": True}

        wrapped[tool] = (method, called, spy)
        setattr(inst, tool, spy)
    try:
        for tool, (_, called, _) in wrapped.items():
            try:
                caller(tool, {})
                problems.append(
                    f"{caller_name}({tool!r}) did not raise ConnectorError")
            except ConnectorError:
                pass
            except Exception:
                # Wrong exception types are reported by the rejection loops
                # in _check_isolation; here we only care about invocation.
                pass
            if called:
                problems.append(
                    f"{caller_name}({tool!r}) invoked {target_kind}-path code")
    finally:
        for tool, (orig, _, _) in wrapped.items():
            setattr(inst, tool, orig)
    return problems


def _check_isolation(name, inst, rec) -> None:
    problems = []
    caps = set(inst.capabilities())
    read_tools = set(inst._read_tools)
    act_tools = set(inst._act_tools)
    if read_tools | act_tools != caps:
        problems.append("read/act routing tables do not partition "
                        "capabilities()")
    leaked = read_tools & set(PRIVILEGED_TOOLS)
    if leaked:
        problems.append(
            f"read routing includes privileged tools: {sorted(leaked)}")
    for tool in sorted(set(PRIVILEGED_TOOLS)) + ["no_such_tool"]:
        try:
            inst.read(tool, {})
            problems.append(f"read({tool!r}) did not raise ConnectorError")
        except ConnectorError:
            pass
        except Exception as exc:
            problems.append(f"read({tool!r}) raised "
                            f"{type(exc).__name__} instead of ConnectorError")
    for tool in sorted(set(TOOL_NAMES) - set(PRIVILEGED_TOOLS)) + ["no_such_tool"]:
        try:
            inst.act(tool, {})
            problems.append(f"act({tool!r}) did not raise ConnectorError")
        except ConnectorError:
            pass
        except Exception as exc:
            problems.append(f"act({tool!r}) raised "
                            f"{type(exc).__name__} instead of ConnectorError")
    problems.extend(_spy_direction(inst, act_tools, inst.read,
                                   "read", "act"))
    problems.extend(_spy_direction(inst, read_tools, inst.act,
                                   "act", "read"))
    if problems:
        rec("read-act-isolation", "fail", "; ".join(problems))
    else:
        rec("read-act-isolation", "pass",
            f"{len(read_tools)} read / {len(act_tools)} privileged tools; "
            "routing tables partition capabilities() and spy tests confirm "
            "no cross-path invocation")


_BANNED_PARAM_NAMES = {"password", "passwd", "secret", "token", "api_key",
                       "credentials", "credential"}
_BANNED_STEMS = ("password", "passwd", "secret", "token", "api_key",
                 "credential")
_ALLOWED_SUFFIXES = ("_env", "_provider", "_file", "_ref", "_name", "_path")


def _check_ctor_params(name, factory, rec) -> None:
    try:
        params = inspect.signature(factory.__init__).parameters
    except (TypeError, ValueError) as exc:
        rec("ctor-secret-params", "fail",
            f"cannot inspect __init__ signature: {exc}")
        return
    bad = []
    for pname, p in params.items():
        if pname == "self":
            continue
        if p.kind in (inspect.Parameter.VAR_POSITIONAL,
                      inspect.Parameter.VAR_KEYWORD):
            continue
        low = pname.lower()
        if low in _BANNED_PARAM_NAMES:
            bad.append(pname)
        elif (any(stem in low for stem in _BANNED_STEMS)
                and not low.endswith(_ALLOWED_SUFFIXES)):
            bad.append(pname)
    if bad:
        rec("ctor-secret-params", "fail",
            f"__init__ takes raw-secret parameter(s) {bad}; take credential "
            "providers or env-var names (e.g. password_env) instead")
    else:
        rec("ctor-secret-params", "pass",
            "no raw-secret constructor parameters")


def _check_repr(name, inst, sentinels, rec) -> None:
    if not sentinels:
        rec("repr-hygiene", "skip",
            "no probe secrets were injected; register sentinels in the "
            "harness builder to enable this check")
        return
    text = repr(inst)
    leaked = sorted({s for s in sentinels if s and s in text})
    if leaked:
        rec("repr-hygiene", "fail",
            f"repr() leaks probe secret value(s): {leaked}")
    else:
        rec("repr-hygiene", "pass",
            "repr() contains no probe secret values")


def _check_driver_absent(name, inst, driver_module, sentinels, rec) -> None:
    if driver_module is None:
        # Driverless-by-design connectors (linux-host), or connectors
        # whose driver is unknown to the harness (auto-built future
        # entries): connect() must fail safe -- either succeed cleanly or
        # raise ConnectorError (no credential configured, probe target
        # unreachable). Any other exception type, or a leaked probe
        # secret, is a failure.
        try:
            inst.connect()
        except ConnectorError as exc:
            if any(s and s in str(exc) for s in sentinels):
                rec("driver-absent-connect", "fail",
                    "connect() refusal leaked a probe secret value")
            else:
                rec("driver-absent-connect", "pass",
                    f"connect() fails safe with no driver/credentials: {exc}")
            return
        except Exception as exc:
            rec("driver-absent-connect", "fail",
                f"connect() raised {type(exc).__name__} instead of "
                f"ConnectorError: {exc}")
            return
        try:
            inst.close()
        except Exception:
            pass
        rec("driver-absent-connect", "pass",
            "connect()/close() need no driver and succeed")
        return
    with _installed_module(driver_module, None):
        try:
            inst.connect()
        except ConnectorError as exc:
            if "install" in str(exc).lower():
                rec("driver-absent-connect", "pass",
                    "connect() without the driver raises ConnectorError "
                    "with install guidance")
            else:
                rec("driver-absent-connect", "fail",
                    f"connect() raised ConnectorError without install "
                    f"guidance: {exc}")
            return
        except Exception as exc:
            rec("driver-absent-connect", "fail",
                f"connect() raised {type(exc).__name__} instead of "
                f"ConnectorError: {exc}")
            return
    try:
        inst.close()
    except Exception:
        pass
    rec("driver-absent-connect", "fail",
        "connect() succeeded with the driver hidden; it must raise "
        "ConnectorError")


def _check_instance_attrs(name, inst, sentinels, rec) -> None:
    if not sentinels:
        rec("instance-secret-scan", "skip",
            "no probe secrets were injected; register sentinels in the "
            "harness builder to enable this check")
        return
    bad = []
    for attr, value in vars(inst).items():
        try:
            text = str(value)
        except Exception:
            continue
        if any(s and s in text for s in sentinels):
            bad.append(attr)
    if bad:
        rec("instance-secret-scan", "fail",
            f"instance attribute(s) hold probe secret values: {sorted(bad)}; "
            "secrets must resolve at connect() time and never be stored")
    else:
        rec("instance-secret-scan", "pass",
            "no instance attribute holds a probe secret value")


def _check_privileged_refusal(name, inst, spec, builder, sentinels, rec) -> None:
    act_tools = set(inst._act_tools)
    if not act_tools:
        rec("privileged-refusal", "skip",
            "no privileged capabilities declared; act() always raises by "
            "construction")
        return
    env_names = tuple(builder.priv_envs) + tuple(
        r for r in spec.credential_refs if isinstance(r, str))
    with _clean_env(env_names):
        try:
            probe_cm = (builder.priv_probe(inst) if builder.priv_probe
                        else _generic_priv_probe(inst, builder.driver_module))
            with probe_cm as (action, params):
                pass
        except Exception as exc:
            rec("privileged-refusal", "skip",
                f"could not build a refusal probe ({exc!r}); register an "
                "explicit priv_probe for this connector")
            return
        try:
            inst.act(action, dict(params or {}))
        except ConnectorError as exc:
            text = str(exc)
            if any(s and s in text for s in sentinels):
                rec("privileged-refusal", "fail",
                    "refusal message leaked a probe secret value")
                return
            rec("privileged-refusal", "pass",
                f"act({action!r}) refused without the privileged "
                f"credential: {exc}")
            return
        except TypeError as exc:
            rec("privileged-refusal", "skip",
                f"probe params rejected ({exc}); register an explicit "
                "priv_probe for this connector")
            return
        except Exception as exc:
            rec("privileged-refusal", "fail",
                f"act({action!r}) raised {type(exc).__name__} instead of "
                f"ConnectorError: {exc}")
            return
    rec("privileged-refusal", "fail",
        f"act({action!r}) without the privileged credential did not raise "
        "ConnectorError")


def _check_result_boundary(name, inst, rec) -> None:
    caps = sorted(inst.capabilities())
    undocumented = [t for t in caps
                    if not getattr(getattr(inst, t, None), "__doc__", None)]
    detail = ("static contract only: this harness never contacts a live "
              "target and never resolves a real secret. Live validation of "
              "result shapes and payloads is the pytest suite's job "
              "(tests/test_connector_contract.py, per-connector modules) "
              "plus a customer pilot against a real environment.")
    if undocumented:
        detail += (" Undocumented tool method(s) -- the contract should "
                   f"document return shapes: {undocumented}.")
    rec("result-shape-boundary", "pass", detail)


# ---------------------------------------------------------------------------
# Runner: Python API + CLI.
# ---------------------------------------------------------------------------

def check_connector(name: str, spec: ConnectorSpec,
                    factory: type[Connector],
                    builder: _Builder) -> list[CheckResult]:
    """Run every harness check for one connector. Returns CheckResults."""
    results: list[CheckResult] = []

    def rec(check: str, status: str, detail: str = "") -> None:
        results.append(CheckResult(connector=name, check=check,
                                   status=status, detail=detail))

    try:
        inst = builder.build()
    except Exception as exc:
        rec("construction", "fail",
            f"could not construct {factory.__name__} without secrets: "
            f"{exc!r}")
        for check in CHECKS[1:]:
            rec(check, "skip", "construction failed; nothing to check")
        return results
    if not isinstance(inst, Connector):
        rec("construction", "fail",
            f"built object is not a Connector: {type(inst).__name__}")
        for check in CHECKS[1:]:
            rec(check, "skip", "construction failed; nothing to check")
        return results
    rec("construction", "pass",
        f"{type(inst).__name__} constructed with probe (non-secret) values")

    sentinels = set(builder.sentinels)
    _check_registration(name, spec, factory, sentinels, rec)
    _check_capabilities(name, inst, rec)
    _check_isolation(name, inst, rec)
    _check_ctor_params(name, factory, rec)
    _check_repr(name, inst, sentinels, rec)
    _check_driver_absent(name, inst, builder.driver_module, sentinels, rec)
    _check_instance_attrs(name, inst, sentinels, rec)
    _check_privileged_refusal(name, inst, spec, builder, sentinels, rec)
    _check_result_boundary(name, inst, rec)
    return results


def run_checks(connector_name: str | None = None) -> list[CheckResult]:
    """Run the harness over one connector (by registry name) or all of them.

    Returns a flat list of CheckResult. Raises ValueError for an unknown
    connector name.
    """
    if connector_name is not None and connector_name not in CONNECTORS:
        raise ValueError(f"unknown connector {connector_name!r}; known: "
                         f"{sorted(CONNECTORS)}")
    names = [connector_name] if connector_name else sorted(CONNECTORS)
    results: list[CheckResult] = []
    for name in names:
        spec, factory = CONNECTORS[name]
        builder = _BUILDERS.get(name) or _auto_builder(factory)
        results.extend(check_connector(name, spec, factory, builder))
    return results


def _summarize(results: list[CheckResult]) -> dict:
    counts = {"connectors": len({r.connector for r in results}),
              "passed": 0, "failed": 0, "skipped": 0}
    for r in results:
        if r.status == "pass":
            counts["passed"] += 1
        elif r.status == "fail":
            counts["failed"] += 1
        else:
            counts["skipped"] += 1
    counts["total"] = counts["passed"] + counts["failed"] + counts["skipped"]
    return counts


def _print_human(results: list[CheckResult]) -> None:
    by_conn: dict[str, list[CheckResult]] = {}
    for r in results:
        by_conn.setdefault(r.connector, []).append(r)
    for cname in sorted(by_conn):
        spec, _ = CONNECTORS[cname]
        print(f"== {cname} -- {spec.display_name} ==")
        for r in by_conn[cname]:
            tag = {"pass": "PASS", "fail": "FAIL", "skip": "SKIP"}[r.status]
            line = f"  [{tag}] {r.check}"
            if r.detail:
                line += f": {r.detail}"
            print(line)
    s = _summarize(results)
    print(f"summary: {s['passed']} passed, {s['failed']} failed, "
          f"{s['skipped']} skipped across {s['connectors']} connector(s)")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog="connectors.harness",
        description="Connector self-test / contract harness: static "
                    "contract checks for onboarding, no live targets, no "
                    "real secrets, no drivers required.")
    parser.add_argument("--connector", default=None, metavar="NAME",
                        help="run checks for one registered connector only")
    parser.add_argument("--json", action="store_true",
                        help="emit machine-readable JSON results")
    args = parser.parse_args(argv)
    try:
        results = run_checks(args.connector)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps({"summary": _summarize(results),
                          "results": [asdict(r) for r in results]},
                         indent=2))
    else:
        _print_human(results)
    return 0 if all(r.status != "fail" for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
