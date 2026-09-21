# ADR 0001: Simulated estate instead of real MQ/Kafka connections

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The RCA assistant needs diagnostic data (queue depths, channel status, error
logs, consumer lag, host metrics) and one privileged action (channel restart)
to demonstrate the full incident -> diagnosis -> approval -> fix -> verify
loop. The real targets are IBM MQ, Kafka, Tomcat, and Linux hosts.

## Decision

Ship a deterministic, in-memory simulated estate (`sim/estate.py`) with a
seedable clock, and define the integration point for real systems as a
`Connector` protocol in `connectors/base.py`. The MCP tool handlers receive
the estate at call time from the Gateway, so swapping the sim for real
connectors does not change the tools, the agent, or the audit trail.

## Consequences

- **Determinism:** same seed + same scenario = same state, so runs are
  reproducible and comparable. Fault injection (`sim/incidents.py`) gives
  three realistic incident scenarios without any test infrastructure.
- **Zero infrastructure:** the demo runs with `pip install -e .` and no
  brokers, JVMs, or credentials.
- **Safe adversarial testing:** the `poison_log` option injects hostile log
  text into the sim, letting us test the engine's data-not-instructions
  posture with no risk to real systems.
- **Downsides:** the demo never talks to a real middleware system, so it
  cannot validate driver code, credential plumbing, network behavior, or
  timing. Anyone adapting this must write and test the connectors
  themselves; the `Connector` protocol documents the minimum surface and
  credential rules, but the hard operational work (PCF, JMX, SSH) is theirs.
