# ADR 0009: Jev reasoning layer on by default, deterministic engine as fallback

- **Status:** accepted
- **Date:** 2026-09-21
- **Supersedes:** ADR 0002 (for the reasoning layer only — its deterministic
  guarantees are retained as the base and the fallback)

## Context

ADR 0002 chose a deterministic rules engine over an LLM in the decision
path, with a stub LLM hook for future suggestions. The user has since
directed that the product be **Jev-based**: TypeSafe System One should be
the default reasoning experience, with its confidence and classification
visible to users as first-class UI and report elements — not a hidden
opt-in. The deterministic engine remains essential (it runs without a key,
which matters for customer-hosted deploys).

## Decision

Add a Jev reasoning layer (`agent/jev.py`) with this contract:

1. **Jev is on by default.** When `TYPESAFE_API_KEY` is configured (and
   `RCA_JEV_ENABLED` is not `"0"`), every diagnosis carries a Jev advisory:
   typed hypothesis ranking with calibrated probabilities, per-claim
   evidence-support verification, incident triage priority (P1–P4), and
   remediation risk class (low/medium/high). These render prominently in
   the web UI and the markdown incident report. `/api/jev/status` reports
   the mode; the UI banner shows "Jev confidence scoring active" vs
   "Deterministic mode". Remediation risk is the exception to "per
   diagnosis": it is scored at plan time (`advise_remediation_risk`), never
   at diagnose time, because there are no steps to judge before the plan
   exists. The diagnose-time advisory therefore carries no risk verdict;
   the plan-time call asks one narrow score question with the proposed
   steps in the Jev state and is audited as a separate
   `jev_advisory` event (note `remediation-risk-only`).
2. **Deterministic fallback, clearly labeled.** With no key (or Jev
   disabled), the deterministic engine stands alone and the UI/report say
   "deterministic mode — set TYPESAFE_API_KEY for Jev confidence scoring".
3. **Jev advises only.** It never overrides human approval, default-deny,
   the audit trail, or verification after mutation. Jev output can never
   produce or execute a privileged call. When Jev disagrees with the
   deterministic top hypothesis, the UI shows the disagreement and the
   deterministic result still decides.
4. **One batched request per diagnosis, one targeted question per plan.**
   Independent narrow typed questions at diagnose time (choice for
   ranking/triage, noul per top-hypothesis claim), per the TypeSafe skill
   playbook; a single score question for remediation risk at plan time, so
   Jev judges the actual proposed steps.
5. **Failures degrade, never crash.** Timeouts and service errors yield an
   advisory with `available: false`; the deterministic diagnosis stands.
6. **Key hygiene.** The key comes from the environment only
   (`TYPESAFE_API_KEY`), never source code or committed files. It is never
   logged, audited, or returned. The audit log carries question hashes and
   bounded scalars, not request bodies.
7. **Tests never hit the live API.** `StubJevClient` for unit tests;
   `DemoJevClient` produces clearly-labeled illustrative values for the
   hosted demo artifact, which has no key.

The old unwired `LLMHook` stub was removed from `agent/rca.py`.

## Consequences

- **Buyer-visible confidence:** a buyer trying the demo *feels* the
  confidence — every diagnosis shows calibrated hypothesis probabilities,
  evidence-support verdicts, triage class, and remediation risk.
- **Deterministic guarantees preserved:** same-input/same-output
  diagnosis, exact test assertions, and no instruction-following component
  in the decision path all remain (ADR 0002's core, minus "no wired model").
- **New failure mode:** Jev outages. Mitigated by (5) and by the fact that
  approvals and audit never depended on Jev.
- **Cost/latency:** one API call per diagnosis (~20s timeout, ~1–2s
  typical). The UI diagnoses synchronously; a production deployment may
  want to make this asynchronous — noted as future work, not done here.
- **Data sent to TypeSafe:** alert fields, hypothesis titles, and short
  tool-output excerpts (≤160 chars). No credentials, no customer PII in
  the reference demo; customer deployments must review this against their
  data policy (noted in `docs/SECURITY.md`).
