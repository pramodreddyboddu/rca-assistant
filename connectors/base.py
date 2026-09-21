"""Connector framework: contract, credential rules, and registry.

A Connector adapts a real system (IBM MQ, Kafka, Linux hosts) to the
diagnostic tool surface the MCP server exposes. The reference demo uses
the simulated estate (sim/estate.py) instead; a production deployment
swaps the sim for Connector implementations here without changing the
tools, the agent, or the audit trail.

CONNECTOR CONTRACT
------------------
Every connector subclasses :class:`Connector` and must:

- declare ``name`` (registry key) and ``SPEC`` (a :class:`ConnectorSpec`),
- implement ``capabilities()`` returning the subset of tool names from
  ``mcp_server.tools.TOOL_NAMES`` it truly implements (one public method
  per tool name, e.g. ``get_queue_depth``),
- implement ``connect()`` / ``close()`` for session lifecycle,
- expose reads through ``read(resource, params)`` and privileged
  mutations through ``act(action, params)``. The base class routes these
  by tool name against the FIXED privileged set in
  ``mcp_server.tools.PRIVILEGED_TOOLS``: a read call can never reach an
  act-path method, even if a subclass is sloppy, because routing is by
  name, not by trust.

CREDENTIAL RULES (hard)
-----------------------
- Constructors take a credential-provider callable or env-var names.
  They NEVER take a raw secret value as an argument.
- Secrets are resolved at ``connect()`` time via :func:`resolve_secret`
  and are never stored on the instance, never appear in ``repr``,
  ``str(exc)``, logs, or audit entries.
- Read operations use a read-only service account. Privileged ``act()``
  operations use a SEPARATE, tightly-scoped credential (a second
  provider/env pair); if the privileged pair is not configured, ``act()``
  refuses rather than reusing read credentials silently.
- No ambient authority: every call carries the caller's scopes (enforced
  by the Gateway, not the connector).

Drivers (pymqi, kafka-python, ...) are imported LAZILY inside the methods
that need them. Connector modules must import cleanly when the driver is
absent; the driver-missing error (with install instructions) is raised
only when a driver path is actually used.
"""

from __future__ import annotations

import abc
import os
from dataclasses import dataclass
from typing import Any, Callable

from mcp_server.tools import PRIVILEGED_TOOLS, TOOL_NAMES


class ConnectorError(Exception):
    """Raised when a connector cannot reach, read, or act on its target."""


@dataclass(frozen=True)
class ConnectorSpec:
    """Static description of a connector for onboarding and the registry.

    ``credential_refs`` names the credential REFERENCES (env-var names or
    vault reference names, e.g. ``"MQ_READ_PASSWORD"``). It must NEVER
    contain a raw secret value.
    """

    name: str
    display_name: str
    description: str
    required_config: tuple[str, ...] = ()
    optional_config: tuple[str, ...] = ()
    credential_refs: tuple[str, ...] = ()
    notes: str = ""


# Registry: connector name -> (spec, factory class). Populated by each
# connector module calling register_connector() at import time.
CONNECTORS: dict[str, tuple[ConnectorSpec, type["Connector"]]] = {}


def register_connector(spec: ConnectorSpec, factory: type["Connector"]) -> None:
    """Register a connector implementation under ``spec.name``."""
    if spec.name in CONNECTORS:
        raise ValueError(f"connector {spec.name!r} already registered")
    CONNECTORS[spec.name] = (spec, factory)


def resolve_secret(*, provider: Callable[[], str] | None = None,
                   env: str | None = None,
                   label: str = "credential") -> str:
    """Resolve a secret from a provider callable or an env var.

    Raises ConnectorError when nothing is configured or the value is
    empty. Callers must not log or re-raise the returned value.
    """
    value: str | None = None
    if provider is not None:
        value = provider()
    elif env:
        value = os.environ.get(env)
    else:
        raise ConnectorError(
            f"{label}: no credential provider or env var configured"
        )
    if not value:
        raise ConnectorError(f"{label}: credential unavailable (empty or unset)")
    return value


class Connector(abc.ABC):
    """Abstract base class every real connector implements.

    Subclasses implement one public method per tool name in
    ``capabilities()`` (matching the sim estate's method names and return
    shapes so the Gateway, tools, and audit path work unchanged), plus
    ``connect()``/``close()``. The ``read()``/``act()`` dispatch below is
    concrete: it routes strictly by the fixed privileged tool set, which
    is what keeps read paths from ever reaching act code.
    """

    name: str = "connector"
    SPEC: ConnectorSpec

    # -- contract ------------------------------------------------------

    @classmethod
    def spec(cls) -> ConnectorSpec:
        """Return this connector's static spec (raises if not defined)."""
        spec = getattr(cls, "SPEC", None)
        if not isinstance(spec, ConnectorSpec):
            raise ConnectorError(
                f"{cls.__name__}: SPEC is not a ConnectorSpec"
            )
        return spec

    @abc.abstractmethod
    def capabilities(self) -> set[str]:
        """Tool names (subset of TOOL_NAMES) this connector implements."""
        ...

    @abc.abstractmethod
    def connect(self) -> None:
        """Establish a session to the target system. May raise ConnectorError."""
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """Release the session. Must be safe to call when not connected."""
        ...

    # -- routing: read() / act() are name-dispatched against the fixed
    # -- privileged set, so read calls can never reach act-path methods. --

    @property
    def _read_tools(self) -> set[str]:
        return set(self.capabilities()) - PRIVILEGED_TOOLS

    @property
    def _act_tools(self) -> set[str]:
        return set(self.capabilities()) & PRIVILEGED_TOOLS

    def read(self, resource: str, params: dict) -> Any:
        """Run a read-only tool by name. Never mutates the target system.

        Raises ConnectorError for privileged or unknown names (before any
        method is resolved), so read-path calls cannot reach act code.
        """
        if resource not in self._read_tools:
            raise ConnectorError(
                f"{self.name}: {resource!r} is not a read resource of "
                "this connector"
            )
        method = getattr(self, resource, None)
        if not callable(method):
            raise ConnectorError(
                f"{self.name}: read resource {resource!r} has no implementation"
            )
        return method(**dict(params or {}))

    def act(self, action: str, params: dict) -> Any:
        """Run a privileged tool by name. Only reachable via the approval gate.

        Raises ConnectorError for read-only or unknown names.
        """
        if action not in self._act_tools:
            raise ConnectorError(
                f"{self.name}: {action!r} is not a privileged action of "
                "this connector"
            )
        method = getattr(self, action, None)
        if not callable(method):
            raise ConnectorError(
                f"{self.name}: privileged action {action!r} has no implementation"
            )
        return method(**dict(params or {}))

    def check_contract(self) -> None:
        """Validate this connector against the framework contract.

        Raises ConnectorError on any violation: unknown tool names in
        capabilities(), or a capability with no callable implementation.
        """
        try:
            spec = self.spec()
        except ConnectorError as exc:
            raise ConnectorError(f"{self.name}: {exc}") from exc
        if spec.name != self.name:
            raise ConnectorError(
                f"{self.name}: SPEC.name {spec.name!r} does not match "
                f"connector name {self.name!r}"
            )
        caps = set(self.capabilities())
        unknown = caps - set(TOOL_NAMES)
        if unknown:
            raise ConnectorError(
                f"{self.name}: capabilities lists unknown tool(s): "
                f"{sorted(unknown)}"
            )
        missing = [t for t in caps if not callable(getattr(self, t, None))]
        if missing:
            raise ConnectorError(
                f"{self.name}: capabilities list tool(s) with no "
                f"implementation: {sorted(missing)}"
            )
        for ref in spec.credential_refs:
            if not ref or not isinstance(ref, str):
                raise ConnectorError(
                    f"{self.name}: credential_refs must be non-empty strings"
                )
