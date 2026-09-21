"""RCA assistant MCP server package.

Reference demo wiring the simulated middleware estate behind an MCP
server with token-based scope authorization and an audit trail.

Public API:
    Gateway          - authz + audit + dispatch for tool calls (gateway.py)
    InProcessClient  - exercises the identical Gateway code path in-process (client.py)
    verify_token     - token -> scope frozenset, or None (auth.py)
    TOOL_NAMES       - names of the 18 registered tools (tools.py)
"""

from mcp_server.auth import verify_token
from mcp_server.gateway import Gateway
from mcp_server.client import InProcessClient
from mcp_server.tools import TOOL_NAMES

__all__ = ["Gateway", "InProcessClient", "verify_token", "TOOL_NAMES"]
