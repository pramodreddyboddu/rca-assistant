# ADR 0004: MIT license

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The reference demo is meant to be copied, adapted, and built upon by anyone
integrating secure, governed MCP connectors for middleware operations. The
license choice sets how freely downstream users can do that.

## Decision

Release under the MIT license (`LICENSE`, also declared in
`pyproject.toml`). MIT is the most permissive common choice: it allows
private modification, redistribution, and commercial use with only an
attribution and license-notice requirement.

## Consequences

- **Maximum reuse:** teams can vendor the code, adapt the Gateway pattern,
  and build commercial products on it without license friction, which is the
  point of a reference demo.
- **Downsides:** permissive licensing offers no protection against
  competitors shipping the same pattern, and provides no patent grant or
  copyleft. **This is changeable before any commercial use:** if the code is
  later productized, the license can be revisited (e.g. a commercial license
  or a source-available license) as long as no third-party contributions have
  been accepted under MIT.
