# ADR 0003: Hash-chained JSONL audit log

- **Status:** accepted
- **Date:** 2026-09-20

## Context

Every security-relevant event (tool calls, auth/scope denials, approval
requests and decisions, privileged executions and failures) must be recorded
in a form that detects later tampering, without adding a database dependency
to the demo.

## Decision

Use an append-only JSONL file (`audit/log.py`, `AuditLog`): one JSON object
per line with fixed fields `seq, ts, event, actor, details, prev, hash`. Each
entry's SHA-256 hash covers its content plus the previous entry's hash,
forming a chain from a genesis entry (`log_opened`, `prev: GENESIS`).
`verify()` re-reads the file and checks hashes, prev-links, and seq order,
returning `(True, msg)` or `(False, reason)` without ever raising on corrupt
content.

## Consequences

- **Tamper evidence:** modifying or deleting any entry breaks the chain and
  is detected by `verify()`; raw tokens are never logged (only
  `token_id()` identifiers like `tok:...0001`).
- **Human-readable, stdlib-only:** the log is plain JSONL with no database
  server; `audit/` depends on the standard library only.
- **Downsides / limits:** the chain detects tampering but does not prevent
  deletion of the whole file, so backups (or append-only remote storage) are
  required. There is no log rotation, no concurrent-writer protection, and
  timestamps come from the system clock, which is not an authoritative time
  source. Details must be JSON-serializable or `append()` raises `TypeError`
  before writing.
