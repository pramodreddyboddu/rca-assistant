# ADR 0006: Multi-step remediation plans with per-step approval and re-verification

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The original demo modeled remediation as a single privileged action behind
one approval prompt (e.g. "restart this channel, approve?"). That shape
matched the first three scenarios but broke on the scenario breadth
workstream: `expired_tls` needs renew-certificate *then* restart-channel,
and `disk_full` needs archive-logs *then* restart-app. A second privileged
action either happened silently after the first approval (unsafe) or the
human had to run the whole demo flow again (unusable). The design had to
grow from "one action, one approval" to an ordered sequence of privileged
steps without weakening the safety story.

## Decision

1. **Plans instead of single actions.** A `RemediationPlan` (`agent/plans.py`)
   is an ordered tuple of `PlanStep` records, proposed per hypothesis by
   `RCAEngine.propose_plan()` and built by pure-data builders in
   `agent/remediation.py`. Plans are data exactly like the legacy remediation
   dict: the engine and builders name privileged tools but can never invoke
   them. `execute_approved_step()` and `run_plan()` hold the single shared
   copy of plan-execution logic so the CLI and any future runner cannot
   diverge.

2. **Per-step approval with an explicit approve-all.** The gate creates one
   linked approval request per step (`request_plan`), and the runner asks a
   human for each step in order. Bulk approval is a *separate, deliberate
   choice* (`approve_all`), not a default: it is offered as its own option
   ("[a] approve all remaining") and audited as the distinct event
   `approval_decided_all`, so an auditor can always distinguish bulk approval
   from step-by-step approval. A `reject` halts the plan immediately with the
   incident left open.

3. **Verify specs are data.** Each step carries an optional `VerifySpec`
   (read-only tool + args + expectations, with equality and `$lt/$lte/$gt/$
   gte/$eq` operators). After every executed step, `check_verify()` re-reads
   state through read-only tools and matches expectations. It returns
   `(ok, detail)` and never raises, because verification is evidence
   gathering: a mismatch must halt the plan loudly (`halted_verify_failed`)
   with the incident left in its current state, never crash the runner.

## Consequences

- **Safer than one-shot approval:** the human sees the whole sequence up
  front and still decides each step; nothing privileged runs on assumptions
  from an earlier step's approval.
- **Reproducible recovery proofs:** the plan's own verify specs are re-run
  after the estate ticks forward, so "recovery confirmed" means the same
  checks passed twice, not just that a tool returned success.
- **Explicit halt states** (`completed` / `halted_rejected` /
  `halted_verify_failed` / `halted_error`) make it obvious what happened and
  what is left open, and every halt is audited with its reason and step.
- **Downsides:** multi-step plans ask the human more questions; approve-all
  mitigates this but concentrates trust in one click, which is why it is
  separately audited and never the default. Plans are static: a step cannot
  be re-planned mid-run from its own verification result; the runner halts
  and leaves that judgment to a human.
