# ADR 0010: Connector self-test / contract harness as the customer onboarding gate

- **Status:** accepted
- **Date:** 2026-09-21

## Context

Phase 4 shipped three real connector clients (`ibmmq`, `kafka`, `tomcat`)
that are **fake-transport tested only** — validated against fakes in the
pytest suite, never against live middleware (see
`docs/REAL_CONNECTOR_READINESS.md`). The first thing a customer must do
before any pilot is prove our connector configuration honors the
framework contract *in their environment* — with their Python version,
their dependency layout, their env-var wiring. The pytest suite already
holds the full contract tests, but pytest is a developer tool: a customer
onboarding engineer cannot reasonably be asked to install it, and the
suite runs against our fakes, not their config.

Without a runnable gate, the failure mode is silent contract drift: a
customer wires a connector with a typo'd env-var name, a missing driver,
or a capability string the gateway does not know, and the failure appears
mid-pilot as a confusing runtime error rather than at onboarding as a
clear "check 7 failed" line.

## Decision

Ship `connectors/harness.py`: an operational self-test with no pytest
dependency, run as:

```
.venv/bin/python -m connectors.harness [--connector NAME] [--json]
```

Exit 0 means every check passed (informational skips allowed); 1 means at
least one check failed; 2 means a connector's status was unknown.
`--json` emits machine-readable results for CI.

The harness builds every connector in the `CONNECTORS` registry with
**probe (non-secret) values only** — e.g. a credential provider returning
`("probe", "REDACTED")` — then runs ten static contract checks:

1. **construction** — the connector builds from probe values.
2. **registration** — registered under its name; credential refs are clean
   reference names, not secret values.
3. **capabilities** — every capability is within `mcp_server.tools.TOOL_NAMES`
   and has a callable implementation.
4. **read-act-isolation** — read routing tables admit no privileged tool;
   spy tests confirm no cross-path invocation.
5. **ctor-secret-params** — no constructor parameter takes a raw secret.
6. **repr-hygiene** — `repr()` contains no probe secret values.
7. **driver-absent-connect** — `connect()` fails safe with the vendor
   driver hidden (deliberately, via a sys.modules stub).
8. **instance-secret-scan** — no instance attribute holds a probe secret.
9. **privileged-refusal** — privileged `act()` refuses without the separate
   privileged credential rather than reusing the read identity.
10. **result-shape-boundary** — informational: the harness's contract is
    static; real payload shapes are a pilot concern (see Non-goals).

New connectors added to `CONNECTORS` later are picked up automatically;
unknown ones fall back to a best-effort generic probe builder, and checks
that cannot run degrade to documented skips rather than failing blind.
Current result: 37 passed, 0 failed, 3 informational skips across the four
registered connectors.

## Non-goals (explicit)

- **Not live validation.** The harness never contacts a live target, never
  resolves a real secret, and never requires the vendor driver. Real
  payload JSON serializability and result shapes are the pytest suite's job
  (`tests/test_connector_contract.py`, per-connector modules), and real
  environment behavior is the customer pilot's job
  (`docs/REAL_CONNECTOR_READINESS.md`).
- **Not a connectivity probe.** Anything that "can we reach your broker"
  needs your credentials and your network; that is step 3 of the pilot
  runbook, deliberately not a self-test.
- **Not a replacement for the pytest suite.** The harness re-checks the
  contract against *customer configuration*; the suite proves the
  connectors against *fakes*. Both are needed; neither implies the other.

A green harness therefore means exactly one thing: "safe to wire into
your config and proceed to pilot" — never "proven against production".

## Alternatives considered

- **pytest-only contract tests.** Kept, and remain the deeper check, but
  rejected as the *onboarding* gate: customers do not (and should not)
  install the dev test toolchain to validate their config.
- **A README checklist ("make sure the driver is installed").** Rejected:
  checklists drift from code. The harness asserts the same contract the
  gateway enforces (`TOOL_NAMES`, `PRIVILEGED_TOOLS`) in the same process,
  so a drifted connector fails the harness the day it ships.
- **An automated live-connectivity pre-check.** Rejected for this
  artifact: it would need customer credentials and network access before
  trust is established, which violates the vault rule in
  `docs/REAL_CONNECTOR_READINESS.md` (we never take raw credentials).
  The pilot runbook covers connectivity with the customer present.
