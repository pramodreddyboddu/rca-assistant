# Jev product-readiness evaluation — 2026-09-21

## What ran

- Tool: `~/workspace/skills/typesafe/bin/jev_ask.py` → `https://api.typesafe.ai/v1/systemone`
- Model: `jev-1.13.0`
- When: 2026-09-21 ~00:05 CDT (second run; first run ~23:55 CDT before the
  Jev-based direction was added to the evaluated state)
- 7 typed questions in one batched call: 3 × noul, 3 × choice, 1 × score.
- Raw response saved at `/tmp/jev_phase0_raw.json` (build machine only).

The evaluated state described the product as it stands (8 scenarios,
deterministic evidence-cited engine, governed approvals, hash-chained audit,
125/125 tests, simulated estate + one real Linux connector, local git only,
no deployment packaging, no discovery interviews yet) **plus the approved
Jev-based direction**: TypeSafe System One as the core reasoning layer
(typed hypothesis ranking, per-claim evidence verification, triage scoring,
remediation risk scoring) over the deterministic engine, which stays as the
base/fallback so the app runs without a Jev key. Jev advises; hard
governance (approvals, audit) is never overridden.

## Verdict (second run)

| # | Question | Result |
|---|----------|--------|
| 1 | Ready for real discovery calls today? (noul) | **0.13 — no.** |
| 2 | Does the demo prove the product wedge? (noul) | **0.31 — not yet.** |
| 3 | Does the Jev reasoning layer materially strengthen the wedge story? (noul) | **0.70 — yes.** |
| 4 | Largest credibility gap a skeptical buyer would poke at (choice) | **simulated_evidence: 0.96** (confidence 0.94). Runners-up: no_deployment_story 0.04; no_ai_reasoning 0.00; prototype_signals 0.00. |
| 5 | Best next build gate (choice) | **deploy_packaging 0.51 vs connector_framework 0.48** — a tie (confidence 0.26, genuine uncertainty). observability 0.01. |
| 6 | First priority among the four investments (choice) | **customer_discovery 0.67**, real_mq_connector 0.32, deploy_packaging 0.01, observability 0.00 (confidence 0.56). |
| 7 | Buyer-readiness level (score 0–3) | **0.97 → "friendly-only"** (p=0.86): demoable to friendly contacts with heavy caveats, not to real prospects. buyer-ready p=0.00. |

## Conclusion

- **Not ready for real discovery calls** (0.13). Readiness is "friendly-only".
- The **Jev-based reasoning layer strengthens the wedge story** (0.70) and
  collapses the old "no AI reasoning" gap to 0.00 — the remaining dominant
  gap is **simulated evidence** (0.96): no real IBM MQ / Kafka connector.
- **Next build gate is a tie**: deployment packaging (0.51) vs connector
  framework (0.48). Doing both is justified; sequencing below puts the
  user-directed Jev layer first, then the connector work that attacks the
  0.96 gap, then packaging.
- **Customer discovery ranks first** (0.67) but is blocked: outreach needs
  the user/Owner (names, approved drafts). It is handed back as the top
  user-gated item, not built by the loop.
- Among buildable investments the order is: **Jev reasoning layer** (user
  direction) → **connector framework + real IBM MQ connector** (0.32,
  attacks the 0.96 gap) → **deployment packaging** (tied gate) →
  **observability** (needed for deploy-ready even at 0.00–0.01 priority).

## First run (pre-Jev-direction) deltas

- next_build_gate was connector_framework 0.63 (vs deploy_packaging 0.36);
  adding the Jev direction moved it to a 0.51/0.48 tie.
- priority_ranking was customer_discovery 0.75 / real_mq_connector 0.25;
  now 0.67 / 0.32. Direction of travel is stable.
- credibility_gap was simulated_evidence 0.96 in both runs; no_ai_reasoning
  fell from 0.01 to 0.00 once the Jev direction was in the state.

## Bar for the final re-run

"Ready to show to a buyer" = discovery_ready ≥ 0.6 AND readiness_level ≥ 2
("credible") with the same questionnaire. The loop does not stop until the
re-run clears the bar or the only remaining blockers are user-gated
(discovery intros, GitHub push approval).
