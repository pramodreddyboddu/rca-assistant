# ADR 0005: In-process client through the same Gateway as the MCP transport

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The demo needs to drive tool calls through the authorization and audit
policy, and external MCP clients need a real transport (stdio) to reach the
same tools. Two entry points to the same policy must not drift.

## Decision

Make `Gateway` (`mcp_server/gateway.py`) the single policy enforcement
point. The demo uses `InProcessClient` (`mcp_server/client.py`), a thin
wrapper that passes a fixed token to `gateway.call()`. The stdio MCP server
(`mcp_server/server.py`, server name `rca-diagnostics`) registers the same
7 tools; each reads the caller token from the `RCA_TOKEN` env var at call
time and passes it to the same `gateway.call()`. No tool handler is
reachable except through the Gateway.

## Consequences

- **Identical authz/audit path:** token verification, scope check, and audit
  logging (`tool_call`, `scope_denied`, `auth_denied`, `tool_error`) run in
  one place, so the demo and the stdio transport cannot diverge.
- **Speed and determinism:** the demo avoids subprocesses and serialization
  round-trips, which keeps seeded runs fast and reproducible.
- **Stdio available:** external MCP clients can still use `server.py` over
  stdio against the identical policy point (token via `RCA_TOKEN`,
  audit path via `RCA_AUDIT_PATH`).
- **Downsides:** the demo path does not exercise real MCP transport
  behavior (framing, serialization, per-request auth context), so transport
  bugs in `server.py` would not show up in demo runs. The `RCA_TOKEN` env-var
  convention in `server.py` is a demo simplification, not a production auth
  pattern.
