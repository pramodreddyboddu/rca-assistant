"""Authorization + audit + dispatch for tool calls.

Gateway is the single policy enforcement point: the in-process client,
the stdio MCP server, and any future transport all funnel through it,
so authz and auditing cannot drift between paths. The raw token is never
written to the audit log or anywhere else; only token_id() identifiers.
"""

from __future__ import annotations

from typing import Any

from mcp_server.auth import token_id, verify_token
from mcp_server.tools import build_tool_defs


class Gateway:
    """Authenticate, authorize, audit, then dispatch a tool call."""

    def __init__(self, estate: Any, audit: Any,
                 include: set[str] | None = None) -> None:
        self._estate = estate
        self._audit = audit
        self._tools = {d.name: d for d in build_tool_defs(estate, include=include)}

    @property
    def tool_names(self) -> list[str]:
        return list(self._tools)

    def call(self, name: str, args: dict, token: str) -> Any:
        """Run tool `name` with `args` after token/scope checks.

        Audits every outcome: auth_denied, scope_denied, tool_error, and
        tool_call (logged before the result is returned). Re-raises the
        original exception after auditing tool_error.
        """
        scopes = verify_token(token)
        if scopes is None:
            self._audit.append(
                "auth_denied", actor="unknown", details={"tool": name}
            )
            raise PermissionError("invalid token")

        actor = token_id(token)
        tool = self._tools.get(name)
        if tool is None:
            self._audit.append(
                "tool_error", actor=actor,
                details={"tool": name, "error": "unknown tool"},
            )
            raise KeyError(f"unknown tool: {name}")

        if tool.scope not in scopes:
            self._audit.append(
                "scope_denied",
                actor=actor,
                details={"tool": name, "required_scope": tool.scope},
            )
            raise PermissionError(
                f"token lacks required scope '{tool.scope}' for tool '{name}'"
            )

        try:
            result = tool.handler(self._estate, dict(args))
        except Exception as exc:
            self._audit.append(
                "tool_error", actor=actor,
                details={"tool": name, "error": str(exc)},
            )
            raise

        self._audit.append(
            "tool_call", actor=actor,
            details={"tool": name, "args": dict(args)},
        )
        return result
