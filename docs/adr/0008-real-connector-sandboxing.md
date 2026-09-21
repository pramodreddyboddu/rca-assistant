# ADR 0008: Real-connector sandboxing (allow-list, read-only by construction, /proc)

- **Status:** accepted
- **Date:** 2026-09-20

## Context

The demo's `connectors/` seam was only a protocol stub. The first real
connector — `LinuxHostConnector` (`connectors/linux_host.py`) — reads the
machine's own CPU, memory, disk, load, and log files through the same
`Gateway` authz/audit path as the simulated estate. A real connector that
can touch production-adjacent data needs its safety argument to live in the
design, not in good intentions: logs can contain hostile text (the demo
already proved log injection is a thing in `poison_message`), log paths
come from callers, and a "read-only" label is worthless if the code can
secretly mutate.

## Decision

Three layered choices, in order of importance:

1. **Read-only by construction, not by convention.** The connector simply
   has no mutating code path: no method writes, deletes, kills, restarts,
   or executes anything, and the class never shells out (no `os.system`,
   `subprocess`, popen). A test asserts no method name suggests mutation.
   Hostile log lines are returned verbatim as inert data — there is no code
   that interprets log text, so a "ignore previous instructions and ..."
   line can never trigger an action. Code review burden shifts from "did
   they use the safe API correctly?" to "does any mutation exist?" — a
   much smaller search space.

2. **Allow-list over block-list for log reads.** Log paths arrive from the
   caller, and the filesystem is full of things we must never expose
   (`/etc/shadow`, SSH keys, credentials). A block-list tries to enumerate
   every dangerous path — an open-ended, losing game. An allow-list
   enumerates the small set of intended logs (`/var/log/` by default) and
   denies everything else. Enforcement is `os.path.realpath(path)` must
   equal an allow-listed file or sit under an allow-listed directory, which
   closes both symlink escapes and `..` traversal in one check. Denials
   raise `PermissionError`, which the gateway audits as `tool_error` —
   the sandbox doing its job, on the record.

3. **`/proc` instead of shelling out.** Host metrics come from the
   `/proc` pseudo-filesystem: parsed directly in Python, no subprocess, no
   command-line arguments to inject, no dependency on `ps`/`df`/`top`
   being installed or behaving identically. `/proc` reads cannot alter
   system state, and the format is stable enough to parse defensively
   (missing files become `ConnectorError` with a clear message, never a
   raw traceback to the caller).

## Consequences

- **Subset-tool gateways:** a connector implements only part of the tool
  surface (the Linux connector has no MQ/Kafka methods). `build_tool_defs`
  and `Gateway` take an `include` set so the gateway registers exactly the
  tools the estate supports; every registered handler is then safe to
  dispatch. The sim gateway "simply never includes" `tail_log`.
- **Downsides:** the allow-list is constructor config, so a careless
  deployment could allow-list too much; policy review at wiring time is
  still required. `/proc` parsing is Linux-only, which is honest for a
  *Linux* host connector but limits portability. The connector reads only
  its own host (a non-local host raises `ConnectorError`) — remote hosts
  need a proper read-only transport (e.g. SSH with a read-only account),
  which is out of scope for this seam-prover.
