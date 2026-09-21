# Jev Readiness — v0.7.0 + dev-machine live validation (post-fix)

- Date: 2026-09-21 (Central)
- Model: `jev-1.13.0` (same model as the Phase-4, v0.6.0, and v0.7.0 runs)
- Product state evaluated: RCA Assistant v0.7.0 — 21 technologies /
  21 connectors, 90 evidence tools (24 privileged), 45 incident scenarios,
  1283/1283 tests passing in a clean venv, connector harness 201 passed /
  0 failed / 9 skipped across 21 connectors. Public GitHub release v0.7.0,
  76-second silent demo video.
- NEW since the v0.7.0 run: first-ever live connector validation on a dev
  machine — 6 connectors run against real local services (Redis 7.0.15,
  PostgreSQL 16.15, nginx 1.24.0, Kafka 3.9.1 KRaft, Tomcat 10.1.46, local
  Linux host): 26/29 checks passed. The 3 failures were genuine connector
  bugs (Kafka `get_kafka_broker_config` vs kafka-python 3.x API; Tomcat
  `get_tomcat_threadpool` bulk-read fragility; Tomcat `get_tomcat_apps`
  over-URL-quoting); all three were fixed the same day and re-validated
  live (314 Kafka broker configs with correct values; real Tomcat
  threadpool numbers busy=1/count=10/max=200; all 6 Tomcat apps `running`
  with real session counts).
- Honest limitations stated to the model: no connector validated in a
  customer environment; 15 of 21 connectors never run against any live
  system; zero discovery conversations; Docker image build and Compose
  startup unverified; every demo data point simulated; zero production
  deployment; zero customer data.

This file is additive. The v0.7.0 record
(`JEV_READINESS_V070_2026-09-21.md`) is preserved unchanged.

Invocation note: all 7 questions ran in one batched `jev_ask.py` call
(same credential, same model, same honest-limits framing as prior runs).

## Results (same 7-question instrument)

| # | Question | Post-fix live-val | v0.7.0 | Delta |
|---|----------|-------------------|--------|-------|
| 1 | Ready for real discovery calls? | 0.22 | 0.09 | +0.13 |
| 2 | Does the demo prove the wedge? | 0.50 | 0.11 | +0.39 (but 0.50 = genuine uncertainty, not confidence) |
| 3 | Does Jev materially strengthen the wedge? | 0.27 | 0.23 | +0.04 |
| 4 | Largest buyer credibility gap | no validation in a customer environment, 0.92 (conf 0.88) | no live connector validation, 0.99 (conf 0.99) | gap reframed and sharpened: dev-machine validation moved the gap outward, to the customer environment |
| 5 | Best next build gate | live-test the remaining 15 connectors, 0.48 (conf 0.36); verify Docker 0.32 runner-up, customer-env pilot 0.17 | live validation pilot, 0.94 (conf 0.93) | gate shifted and confidence dropped — the model is split three ways now |
| 6 | First strategic priority | customer discovery, 0.81 | customer discovery, 0.80 | unchanged |
| 7 | Buyer-readiness score (0–3) | 1.27 (conf 0.71; level 1 at 0.71, level 2 at 0.28) | 0.97 (conf 0.93; level 1 at 0.94) | +0.30; level 2 ("validated in real environments") now holds 0.28 probability |

## Reading

The dev-machine live validation moved the scores for the first time in
four runs — modestly, and in the right direction. Discovery-readiness rose
0.09 → 0.22: still far below any "ready" bar, but no longer single digits.
Buyer-readiness rose 0.97 → 1.27, with level 2 now holding 0.28 of the
probability mass: the product is starting to look like something validated
in real environments, not just a working demo.

The largest gap sharpened correctly rather than shrinking: it is no longer
"no live validation at all" but specifically "no validation in a customer
environment" at 0.92 (conf 0.88). The dev-machine run closed the inner gap
and exposed the outer one — exactly what a pilot is supposed to do.

Q2 at 0.50 is genuine model uncertainty, not a verdict: the demo video plus
live validation genuinely leave the "does the demo prove the wedge"
question unresolved. Q5's low confidence (0.36) is the most actionable
signal in this run: the model splits the next gate three ways —
live-test remaining connectors (0.48), verify Docker (0.32), customer-env
pilot (0.17). All three are cheap, founder-controlled, and none needs the
Owner. Q6 is unchanged and unanimous: customer discovery first (0.81).

## What would move the score

1. A customer-environment pilot — the 0.92 gap pick. Needs user/Owner:
   a real middleware system and 3–4 buyer contacts.
2. Live-test the remaining 15 connectors against local OSS services —
   the 0.48 next-gate pick. Founder-controlled; repeats today's playbook.
3. Verify the Docker image build and Compose startup — the 0.32
   runner-up. Founder-controlled if a Docker host is available.

Standing: no further fake-tested connector or packaging tranche moves the
score — only live evidence (dev-machine, then customer) and buyer contact do.
