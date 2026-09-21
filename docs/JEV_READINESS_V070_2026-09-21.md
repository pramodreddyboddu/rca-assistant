# Jev Readiness — v0.7.0 (product polish release)

- Date: 2026-09-21 (Central)
- Model: `jev-1.13.0` (same model as the Phase-4, v0.6.0, and v0.3.0 runs)
- Product state evaluated: RCA Assistant v0.7.0 — 21 technologies /
  21 connectors, 90 evidence tools (24 privileged), 45 incident scenarios,
  1282/1282 tests passing, connector harness 201 passed / 0 failed / 9 skipped
  across 21 connectors. NEW in v0.7.0: real installable packaging
  (`rca-assistant` 0.7.0 wheel + sdist, `rca` / `rca-assistant` console entry
  points, fresh-venv installs verified from both the repo and the wheel),
  company-grade README, docs/GETTING_STARTED.md, GitHub Actions CI running
  the full test suite, issue/PR templates, SECURITY.md, public GitHub release
  v0.7.0 with the wheel attached.
- Honest limitations stated to the model: ALL 21 connectors
  fake-transport/fake-driver tested only — no live validation against any
  real IBM MQ, Kafka, Tomcat, WebSphere, WebLogic, JBoss, RabbitMQ, Artemis,
  TIBCO EMS, Nginx, Apache, HAProxy, PostgreSQL, MySQL, Oracle, Redis,
  Elasticsearch, MongoDB, Kubernetes, or Docker system; Docker image build
  and Compose startup unverified; every demo data point simulated; zero
  customer discovery conversations, zero live pilot, zero production
  deployment, zero customer data. v0.7.0 changed NOTHING about product
  evidence or validation — only packaging, docs, and CI.

This file is additive. The v0.6.0 record
(`JEV_READINESS_V060_2026-09-21.md`) is preserved unchanged.

Invocation note: Q1–Q3 ran via `~/workspace/skills/jev/bin/jev`
(`--type noul`); Q4–Q7 ran as one batched `jev_ask.py` call over the same
state, because the `bin/jev` `--criteria` wrapper expects a single shape
while the API wants choice criteria as a name→description map and score
criteria as a bare level list. Same credential, same model (`jev-1.13.0`),
same honest-limits framing as the v0.6.0 run.

## Results (same 7-question instrument)

| # | Question | v0.7.0 | v0.6.0 | Delta |
|---|----------|--------|--------|-------|
| 1 | Ready for real discovery calls? | 0.09 | 0.09 | 0.00 |
| 2 | Does the demo prove the wedge? | 0.11 | 0.21 | -0.10 |
| 3 | Does Jev materially strengthen the wedge? | 0.23 | 0.21 | +0.02 |
| 4 | Largest buyer credibility gap | no live connector validation, 0.99 (conf 0.99) | no live connector validation, 0.99 (conf 0.98) | unchanged |
| 5 | Best next build gate | live validation pilot, 0.94 (conf 0.93) | live validation pilot, 0.88 (conf 0.84) | same gate, slightly more certain |
| 6 | First strategic priority | customer discovery, 0.80 (conf 0.76) | customer discovery, 0.93 (conf 0.86) | same priority, slightly weaker |
| 7 | Buyer-readiness score (0–3) | 0.97 (conf 0.93; level 1 at 0.94) | 1.0 (conf 0.98) | flat |

## Reading

The polish release moved the scores nowhere material, and that is the
honest finding: v0.7.0 added real packaging, a company-grade README,
a getting-started guide, CI, and a public GitHub release — but zero live
evidence. Jev rates discovery-readiness flat at 0.09, demo-proves-wedge
down slightly (0.21 → 0.11, within noise of the same framing), and
buyer-readiness at 0.97, still squarely level 1: "early working demo —
everything runs end to end, but all evidence is simulated or unvalidated
against real systems" (level 1 probability 0.94).

The largest credibility gap is unchanged and unanimous: "no live
connector validation" takes 0.99 of the gap probability for the third
consecutive run (v0.4.0: 0.80, v0.6.0: 0.99, v0.7.0: 0.99). Packaging did
not dilute it, because the stated limitations did not move.

The best next build gate is also unchanged: a live validation pilot —
the harness green against one real middleware system the founder
controls — at 0.94 (conf 0.93), slightly more certain than v0.6.0's 0.88.
First strategic priority stays customer discovery at 0.80 (conf 0.76),
with the live pilot as the only meaningful runner-up (0.20).

## What would move the score

1. One live pilot environment (any real middleware/DB/messaging system the
   founder controls) with the harness run green against it — the 0.94
   next-gate pick.
2. Three to four discovery conversations with middleware/SRE buyers.
3. A verified Docker image build and Compose startup.

All three remain user/Owner-gated. Confirmed by this run: no packaging or
docs tranche moves the score — only live evidence and buyer contact do.
