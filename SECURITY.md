# Security Policy

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository:

**Security → Report a vulnerability** on
[pramodreddyboddu/rca-assistant](https://github.com/pramodreddyboddu/rca-assistant)

This opens a private advisory that only maintainers can see. Do not file a
public issue for a suspected security problem.

We aim to acknowledge a report within 5 business days. If a report is
confirmed, we will coordinate a fix and a disclosure timeline with the
reporter before any public mention.

Please do not report vulnerabilities by email: there is no security contact
email, and emails may contain personal identifiers that we want to keep out
of this process.

## Scope

In scope:

- **Connectors** — credential handling, transport security, privilege
  boundaries of the 90+ MCP tools (especially the 24 privileged ones)
- **MCP server / gateway** — authentication, authorization, audit logging
- **Agent engine** — reasoning loop, remediation approval flow, audit trail
- **Demo** — anything in the demo that could be mistaken for production
  behavior

Out of scope:

- The demo bearer tokens in `mcp_server/auth.py` are intentionally fake,
  demo-only values. They are not a vulnerability; do not report them.
- Issues in third-party dependencies with no reachable exploit path in this
  project (please report those upstream).

## Supported versions

| Version | Supported          |
| ------- | ------------------ |
| 0.7.x   | :white_check_mark: |
| < 0.7   | Best effort only   |

Only the latest 0.7.x release receives security fixes. Earlier versions are
supported on a best-effort basis; upgrading is the recommended path.
