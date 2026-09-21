"""Incident report export: render a markdown incident report from an audit trail.

The report is the document an engineer hands to a manager or auditor: it
summarizes the incident, lists the full timeline, the diagnosis, every
approval decision, the actions taken, the outcome, and the audit-chain
integrity check.

It is defensive by design: unknown or future event types are rendered
generically (so new plan/approval events never break the report), missing
sections render as "No X recorded.", and malformed details never crash the
render (``.get`` everywhere).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from audit import AuditLog

# Events that count as approval activity / executed actions / outcome signals.
_APPROVAL_EVENTS = ("approval_requested", "approval_decided", "approval_decided_all")
_ACTION_EVENTS = ("plan_step_executed", "privileged_executed")


def _as_dict(value) -> dict:
    """Return value if it is a dict, else an empty dict (never crash)."""
    return value if isinstance(value, dict) else {}


def _as_list(value) -> list:
    return value if isinstance(value, list) else []


def _esc(text) -> str:
    """Escape a value for a markdown table cell."""
    return str(text).replace("|", "\\|").replace("\n", " ")


def _compact(details) -> str:
    """One-line compact JSON of details, truncated for the timeline."""
    try:
        text = json.dumps(details, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        text = str(details)
    text = text.replace("\n", " ")
    return text if len(text) <= 160 else text[:157] + "..."


def _summarize_details(event: str, details) -> str:
    """One-line human summary of an event's details.

    Known events get a friendly rendering; unknown/future events fall back
    to compact JSON so the report never breaks on new event types.
    """
    d = _as_dict(details)
    if event == "incident_injected":
        alert = _as_dict(d.get("alert"))
        bits = [f"scenario={d.get('scenario', '?')}"]
        for key in ("type", "qmgr", "queue", "observed_depth",
                    "topic", "group", "host", "mount"):
            if alert.get(key) is not None:
                bits.append(f"{key}={alert[key]}")
        return "; ".join(bits)
    if event == "tool_call":
        args = _as_dict(d.get("args"))
        args_s = ", ".join(f"{k}={v}" for k, v in args.items())
        return f"{d.get('tool', '?')}({args_s})"
    if event == "diagnosis_complete":
        return f"top={d.get('top_hypothesis', '?')} score={d.get('score', '?')}"
    if event == "approval_requested":
        return f"{d.get('request_id', '?')}: action={d.get('action', '?')}"
    if event == "approval_decided":
        outcome = "APPROVED" if d.get("approved") else "DENIED"
        return f"{d.get('request_id', '?')}: {outcome} by {d.get('decided_by', '?')}"
    if event == "approval_decided_all":
        approved = d.get("approved")
        outcome = "APPROVED" if approved else "DENIED"
        if isinstance(approved, list):
            reqs = ", ".join(approved) if approved else "?"
        else:
            reqs = d.get("request_ids") or d.get("request_id") or "?"
        return f"approve-all: {outcome} by {d.get('decided_by', '?')} ({reqs})"
    if event in _ACTION_EVENTS:
        ident = d.get("step_id") or d.get("request_id") or "?"
        return f"{ident}: {d.get('action', '?')}"
    if event == "plan_step_verified":
        return f"{d.get('step_id', '?')}: {'OK' if d.get('ok') else 'FAILED'}"
    if event == "plan_step_verify_failed":
        return f"{d.get('step_id', '?')}: FAILED ({d.get('reason', '?')})"
    if event == "plan_proposed":
        steps = _as_list(d.get("steps"))
        return f"{len(steps)} steps proposed"
    if event == "plan_completed":
        return f"{d.get('steps', '?')} steps completed"
    if event == "plan_halted":
        return f"halted at {d.get('step', '?')}: {d.get('reason', '?')}"
    return _compact(d)


def _first(entries: list[dict], event: str) -> dict | None:
    for entry in entries:
        if isinstance(entry, dict) and entry.get("event") == event:
            return entry
    return None


def _all(entries: list[dict], *events: str) -> list[dict]:
    return [e for e in entries
            if isinstance(e, dict) and e.get("event") in events]


def _enrich_diagnosis(details: dict, diagnosis) -> dict:
    """Merge an optional external diagnosis dict (hypothesis titles, evidence)
    into the diagnosis_complete details when the log itself lacks them."""
    if diagnosis is None:
        return details
    diag = _as_dict(diagnosis)
    top_id = details.get("top_hypothesis")
    candidates = _as_list(diag.get("hypotheses"))
    if not candidates and diag.get("id") is not None:
        candidates = [diag]
    for hyp in candidates:
        hyp = _as_dict(hyp)
        if top_id is not None and hyp.get("id") != top_id:
            continue
        merged = dict(hyp)
        merged.update({k: v for k, v in details.items() if v is not None})
        return merged
    return details


def _section_summary(entries: list[dict]) -> list[str]:
    lines = ["## Summary", ""]
    inj = _first(entries, "incident_injected")
    if inj is None:
        lines.append("No incident recorded.")
        return lines
    d = _as_dict(inj.get("details"))
    alert = _as_dict(d.get("alert"))
    lines.append(f"- Scenario: `{_esc(d.get('scenario', 'unknown'))}`")
    if alert:
        for key in ("type", "qmgr", "queue", "observed_depth",
                    "topic", "group", "host", "mount", "channel"):
            if alert.get(key) is not None:
                lines.append(f"- {_esc(key)}: `{_esc(alert[key])}`")
        extra = {k: v for k, v in alert.items()
                 if k not in ("type", "qmgr", "queue", "observed_depth",
                              "topic", "group", "host", "mount", "channel")}
        for key, value in extra.items():
            lines.append(f"- {_esc(key)}: `{_esc(value)}`")
    else:
        lines.append("- No alert fields recorded.")
    return lines


def _section_timeline(entries: list[dict]) -> list[str]:
    lines = ["## Timeline", ""]
    if not entries:
        lines.append("No timeline events recorded.")
        return lines
    lines.append("| Timestamp (UTC) | Event | Actor | Details |")
    lines.append("|---|---|---|---|")
    for entry in entries:
        ts = entry.get("ts", "?")
        event = entry.get("event", "?")
        actor = entry.get("actor", "?")
        summary = _summarize_details(event, entry.get("details"))
        lines.append(f"| {_esc(ts)} | {_esc(event)} | {_esc(actor)} | {_esc(summary)} |")
    return lines


def _section_diagnosis(entries: list[dict], diagnosis) -> list[str]:
    lines = ["## Diagnosis", ""]
    diag_event = _first(entries, "diagnosis_complete")
    if diag_event is None:
        lines.append("No diagnosis recorded.")
    else:
        d = _enrich_diagnosis(_as_dict(diag_event.get("details")), diagnosis)
        lines.append(f"- Alert type: `{_esc(d.get('alert_type', 'unknown'))}`")
        lines.append(f"- Top hypothesis: `{_esc(d.get('top_hypothesis', 'unknown'))}`"
                     f" (score: {_esc(d.get('score', '?'))})")
        title = d.get("title")
        if title:
            lines.append(f"- Title: {_esc(title)}")
        evidence = _as_list(d.get("evidence"))
        if evidence:
            lines.append("- Evidence:")
            for item in evidence:
                item = _as_dict(item)
                claim = item.get("claim", "?")
                source = item.get("source")
                if source:
                    lines.append(f"  - {_esc(claim)} (source: {_esc(source)})")
                else:
                    lines.append(f"  - {_esc(claim)}")
        hypotheses = _as_list(d.get("hypotheses"))
        if hypotheses and not evidence:
            lines.append("- Hypotheses considered:")
            for hyp in hypotheses:
                hyp = _as_dict(hyp)
                lines.append(f"  - `{_esc(hyp.get('id', '?'))}`"
                             f" (score: {_esc(hyp.get('score', '?'))})")
    plan = _first(entries, "plan_proposed")
    if plan is not None:
        lines.append("")
        lines.append("### Proposed plan")
        lines.append("")
        steps = _as_list(_as_dict(plan.get("details")).get("steps"))
        if steps:
            for i, step in enumerate(steps, start=1):
                step = _as_dict(step)
                label = step.get("description") or step.get("action") or "?"
                step_id = step.get("id") or step.get("step_id") or f"step {i}"
                lines.append(f"{i}. `{_esc(step_id)}`: {_esc(label)}")
        else:
            lines.append("No plan steps recorded.")
    return lines


def _section_jev(entries: list[dict]) -> list[str]:
    lines = ["## Jev confidence", ""]
    ev = _first(entries, "jev_advisory")
    if ev is None:
        lines.append("No Jev advisory recorded for this run.")
        return lines
    d = _as_dict(ev.get("details"))
    adv = _as_dict(d.get("advisory"))
    # Remediation risk is scored at plan time (there are no steps to judge
    # before then) and audited as a later jev_advisory event. Fold the
    # latest plan-time risk into the report.
    for later in entries:
        if not isinstance(later, dict) or later.get("event") != "jev_advisory":
            continue
        r = _as_dict(_as_dict(_as_dict(later.get("details")).get("advisory"))
                     .get("remediation_risk"))
        if r.get("risk"):
            adv["remediation_risk"] = r
    mode = adv.get("mode", d.get("mode", "deterministic"))
    if not adv.get("available"):
        lines.append(f"- Mode: `{_esc(mode)}`")
        err = adv.get("error") or d.get("error")
        if err:
            lines.append(f"- {_esc(err)}")
        lines.append("- The diagnosis above is the deterministic engine's.")
        return lines
    if adv.get("illustrative"):
        lines.append("- Confidence values are illustrative demo values "
                     "(no TypeSafe key configured).")
    else:
        lines.append("- Live Jev (TypeSafe System One) scoring.")
    agreement = adv.get("agreement", "n/a")
    if agreement == "agree":
        lines.append("- Jev **agrees** with the deterministic top hypothesis.")
    elif agreement == "disagree":
        lines.append("- Jev **disagrees** with the deterministic top "
                     "hypothesis — the deterministic result still decides; "
                     "a human reviews the difference.")
    ranking = _as_list(adv.get("hypothesis_ranking"))
    if ranking:
        lines.append("")
        lines.append("### Hypothesis confidence")
        lines.append("")
        lines.append("| Hypothesis | Jev P(root cause) | Confidence |")
        lines.append("|---|---|---|")
        for row in ranking:
            row = _as_dict(row)
            lines.append(
                f"| `{_esc(row.get('hypothesis_id', '?'))}` "
                f"{_esc(row.get('title', ''))} "
                f"| {row.get('jev_probability', '?')} "
                f"| {row.get('confidence', '?')} |")
    claims = _as_list(adv.get("claim_support"))
    if claims:
        lines.append("")
        lines.append("### Evidence support (top hypothesis)")
        lines.append("")
        lines.append("| Claim | Verdict | Confidence |")
        lines.append("|---|---|---|")
        for row in claims:
            row = _as_dict(row)
            lines.append(f"| {_esc(row.get('claim', '?'))} "
                         f"| `{_esc(row.get('verdict', '?'))}` "
                         f"| {row.get('confidence', '?')} |")
    tri = _as_dict(adv.get("triage"))
    if tri.get("priority"):
        lines.append("")
        lines.append(f"- Triage priority: **{_esc(tri['priority'])}** "
                     f"(confidence {tri.get('confidence', '?')})")
    risk = _as_dict(adv.get("remediation_risk"))
    if risk.get("risk"):
        lines.append(f"- Remediation risk: **{_esc(risk['risk'])}** "
                     f"(score {risk.get('risk_score', '?')}, "
                     f"confidence {risk.get('confidence', '?')})")
    lines.append("")
    lines.append("_Jev advises only. Human approval, default-deny, the audit "
                 "trail, and verification after mutation are never overridden "
                 "by model output._")
    return lines


def _section_approvals(entries: list[dict]) -> list[str]:
    lines = ["## Approvals", ""]
    events = _all(entries, *_APPROVAL_EVENTS)
    if not events:
        lines.append("No approvals recorded.")
        return lines
    for entry in events:
        ts = entry.get("ts", "?")
        event = entry.get("event", "?")
        d = _as_dict(entry.get("details"))
        if event == "approval_requested":
            lines.append(f"- `{_esc(ts)}` — approval requested: "
                         f"`{_esc(d.get('request_id', '?'))}` "
                         f"for action `{_esc(d.get('action', '?'))}`")
        elif event == "approval_decided":
            outcome = "APPROVED" if d.get("approved") else "DENIED"
            lines.append(f"- `{_esc(ts)}` — decision on "
                         f"`{_esc(d.get('request_id', '?'))}`: **{outcome}** "
                         f"by `{_esc(d.get('decided_by', '?'))}`")
        elif event == "approval_decided_all":
            approved = d.get("approved")
            outcome = "APPROVED" if approved else "DENIED"
            if isinstance(approved, list):
                reqs = ", ".join(approved) if approved else "?"
            else:
                reqs = d.get("request_ids") or d.get("request_id") or "?"
            lines.append(f"- `{_esc(ts)}` — approve-all decision: **{outcome}** "
                         f"by `{_esc(d.get('decided_by', '?'))}` "
                         f"covering `{_esc(reqs)}`")
        else:  # future approval event types render generically
            lines.append(f"- `{_esc(ts)}` — {event}: {_esc(_compact(d))}")
    lines.append("")
    used_approve_all = any(e.get("event") == "approval_decided_all" for e in events)
    if used_approve_all:
        lines.append("An **approve-all** decision was used: remaining steps were "
                     "approved in one grant instead of step-by-step.")
    else:
        lines.append("Approvals were handled **step-by-step** (one decision per "
                     "request; no approve-all was used).")
    return lines


def _section_actions(entries: list[dict]) -> list[str]:
    lines = ["## Actions taken", ""]
    events = _all(entries, *_ACTION_EVENTS)
    if not events:
        lines.append("No actions taken recorded.")
        return lines
    for entry in events:
        ts = entry.get("ts", "?")
        event = entry.get("event", "?")
        d = _as_dict(entry.get("details"))
        if event == "plan_step_executed":
            lines.append(f"- `{_esc(ts)}` — executed plan step "
                         f"`{_esc(d.get('step_id', '?'))}`: "
                         f"`{_esc(d.get('action', '?'))}`")
        elif event == "privileged_executed":
            lines.append(f"- `{_esc(ts)}` — executed privileged action "
                         f"`{_esc(d.get('action', '?'))}` "
                         f"(request `{_esc(d.get('request_id', '?'))}`)")
        else:
            lines.append(f"- `{_esc(ts)}` — {event}: {_esc(_compact(d))}")
    return lines


def _section_outcome(entries: list[dict]) -> list[str]:
    lines = ["## Outcome", ""]
    completed = _first(entries, "plan_completed")
    halted = _first(entries, "plan_halted")
    verifications = _all(entries, "plan_step_verified", "plan_step_verify_failed",
                         "plan_step_failed", "privileged_failed")

    if completed is not None:
        d = _as_dict(completed.get("details"))
        lines.append(f"- Plan completed: {_esc(d.get('steps', '?'))} steps executed.")
    if halted is not None:
        d = _as_dict(halted.get("details"))
        lines.append(f"- Plan halted at step `{_esc(d.get('step', '?'))}`: "
                     f"{_esc(d.get('reason', 'no reason given'))}.")
    for entry in verifications:
        ts = entry.get("ts", "?")
        event = entry.get("event", "?")
        d = _as_dict(entry.get("details"))
        if event == "plan_step_verified":
            ok = bool(d.get("ok"))
            lines.append(f"- `{_esc(ts)}` — plan step "
                         f"`{_esc(d.get('step_id', '?'))}` verified: "
                         f"{'OK' if ok else 'FAILED'}.")
        elif event == "plan_step_verify_failed":
            lines.append(f"- `{_esc(ts)}` — plan step "
                         f"`{_esc(d.get('step_id', '?'))}` verification FAILED: "
                         f"{_esc(d.get('reason', '?'))}.")
        else:
            lines.append(f"- `{_esc(ts)}` — {event}: {_esc(_compact(d))}.")
    if completed is None and halted is None and not verifications:
        lines.append("- No outcome events recorded.")

    # Plain-language recovery line.
    denied = any(_as_dict(e.get("details")).get("approved") is False
                 for e in _all(entries, "approval_decided", "approval_decided_all"))
    acted = bool(_all(entries, *_ACTION_EVENTS))
    lines.append("")
    if halted is not None:
        reason = _as_dict(halted.get("details")).get("reason", "no reason given")
        lines.append(f"**Recovery:** not completed — the plan was halted ({reason}).")
    elif completed is not None:
        lines.append("**Recovery:** completed — the remediation plan finished "
                     "and the incident was resolved.")
    elif acted:
        lines.append("**Recovery:** the approved remediation action was executed.")
    elif denied:
        lines.append("**Recovery:** no action taken — approval was denied.")
    else:
        lines.append("**Recovery:** no recovery recorded.")
    return lines


def _section_integrity(audit_path: Path, entries: list[dict],
                       load_error: str | None) -> list[str]:
    lines = ["## Audit integrity", ""]
    if load_error is not None:
        ok, msg = False, load_error
    else:
        try:
            ok, msg = AuditLog(audit_path).verify()
        except Exception as exc:  # verify() promises not to raise, belt and braces
            ok, msg = False, f"verify() raised: {exc}"
    lines.append(f"- Chain status: **{'VALID' if ok else 'INVALID'}**")
    lines.append(f"- Entries: {len(entries) if load_error is None else 'unknown (log unreadable)'}")
    if entries and load_error is None:
        first = entries[0]
        last = entries[-1]
        first_hash = str(first.get("hash", "?"))[:12]
        last_hash = str(last.get("hash", "?"))[:12]
        lines.append(f"- First entry hash: `{first_hash}` (seq {first.get('seq', '?')})")
        lines.append(f"- Last entry hash: `{last_hash}` (seq {last.get('seq', '?')})")
    if not ok:
        lines.append(f"- Detail: {_esc(msg)}")
    return lines


def generate_markdown(run_dir: Path, diagnosis=None) -> str:
    """Render a markdown incident report from a run's audit trail.

    ``run_dir`` is a run directory containing ``audit.jsonl`` (e.g.
    ``runs/20260920T170232Z``). ``diagnosis`` is an optional extra diagnosis
    dict used to enrich the report when the log itself lacks hypothesis
    titles or evidence. Never raises on absent events or malformed details.
    """
    run_dir = Path(run_dir)
    audit_path = run_dir / "audit.jsonl"

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    load_error: str | None = None
    entries: list[dict] = []
    if not audit_path.exists():
        load_error = f"audit log not found: {audit_path}"
    else:
        try:
            entries = AuditLog(audit_path).entries()
        except Exception as exc:
            load_error = f"could not read audit log: {exc}"

    parts: list[str] = []
    parts.append("# Incident report")
    parts.append("")
    parts.append(f"Generated: {generated} (UTC) | Run: `{_esc(run_dir.name)}`")
    parts.append("")

    for section in (
        _section_summary(entries),
        _section_timeline(entries),
        _section_diagnosis(entries, diagnosis),
        _section_jev(entries),
        _section_approvals(entries),
        _section_actions(entries),
        _section_outcome(entries),
        _section_integrity(audit_path, entries, load_error),
    ):
        parts.extend(section)
        parts.append("")

    return "\n".join(parts).rstrip() + "\n"
