"""In-process client for the RCA assistant MCP server.

The in-process client exercises the IDENTICAL authz + audit code path as
the MCP transport: it calls Gateway.call() directly with an explicit
token, and the Gateway performs the same token verification, scope
check, audit logging, and dispatch that a tool call arriving over stdio
goes through in server.py.

The stdio transport (server.py) is available for external MCP clients.
The demo uses the in-process client for speed and determinism: no
subprocess, no serialization round-trips, no flaky I/O.
"""

from __future__ import annotations

from typing import Any


class InProcessClient:
    """Thin client over a Gateway, holding a fixed demo token."""

    def __init__(self, gateway: Any, token: str) -> None:
        self.gateway = gateway
        self.token = token

    def call_tool(self, name: str, args: dict) -> Any:
        """Call a tool through the full Gateway authz + audit path."""
        return self.gateway.call(name, args, self.token)
