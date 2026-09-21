# ADR 0002: Deterministic rules engine instead of an LLM

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The RCA engine turns tool outputs into a ranked diagnosis. An LLM could do
this flexibly, but diagnosis feeds a human approval gate and a tamper-evident
audit log, so the engine's behavior must be predictable and reviewable.

## Decision

Use a deterministic rules engine (`agent/rca.py`): fixed evidence plans per
alert type, checks that substring-match or numerically compare tool output,
hypothesis scores as the fraction of matched checks (0.0 - 1.0), and cited
evidence as short excerpts of actual output. No model, no prompts, no
generated text in the decision path. An LLM hook is left as a stub: a future
caller could consult a model for *suggestions*, but the engine's scored,
cited output remains the auditable decision.

## Consequences

- **Auditability:** every hypothesis can be traced to the checks it matched
  and the exact tool output cited; two runs with the same inputs give the
  same diagnosis.
- **Testability:** hypotheses are plain data with scores, so behavior can be
  asserted exactly (e.g. the adversarial log entry must not change the
  diagnosis).
- **No prompt-injection via the model:** there is no instruction-following
  component at all; `rca.py` contains no `eval`/`exec`/`compile` and treats
  all tool output as data (see `docs/SECURITY.md`).
- **Downsides:** the engine only knows the alerts and checks coded into it
  (`queue_backlog`, `tomcat_errors`, `kafka_lag`); novel incidents get no
  diagnosis beyond what the rules cover. Extending coverage means writing
  more checks, not more prompts. An LLM would generalize better to unseen
  failure modes at the cost of the guarantees above.
