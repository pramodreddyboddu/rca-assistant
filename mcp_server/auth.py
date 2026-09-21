"""Demo token auth for the RCA assistant MCP server.

!! DEMO ONLY !!

The tokens in TOKENS are hard-coded for the reference demo so the
simulation runs with zero setup. DO NOT DO THIS IN PRODUCTION.
Production must use a real identity provider (OAuth/OIDC) or a secret
manager: issue short-lived scoped tokens, store hashes not plaintext,
rotate regularly, and never commit credentials to source control.
"""

from __future__ import annotations

SCOPES_READ = frozenset({"diagnostics:read"})
SCOPES_ADMIN = frozenset({"diagnostics:read", "admin:write"})

# !! DEMO ONLY hard-coded tokens. Production: real IdP / secret manager.
TOKENS: dict[str, frozenset[str]] = {
    "demo-read-token-0001": SCOPES_READ,
    "demo-admin-token-0002": SCOPES_ADMIN,
}


def verify_token(token: str) -> frozenset[str] | None:
    """Return the scope set for a known token, else None.

    Comparison is a plain dict lookup; this is a demo, not a security
    primitive.
    """
    if not token:
        return None
    return TOKENS.get(token)


def token_id(token: str) -> str:
    """Return a non-sensitive identifier for a token, e.g. "tok:...0001".

    NEVER returns the raw token. Used for audit log actors so the audit
    trail cannot leak credentials. Unknown/empty tokens map to
    "tok:unknown".
    """
    if not token or token not in TOKENS:
        return "tok:unknown"
    return f"tok:...{token[-4:]}"
