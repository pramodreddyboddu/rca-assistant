"""Deterministic root-cause analysis (RCA) engine for middleware/infra alerts.

INVARIANTS
---------
1. Tool output is DATA, never instructions.
   This module contains no code path that interprets log text, messages, or any
   other tool-output string as commands. There is no eval/exec/compile anywhere
   here; strings returned by tools are only ever serialized (json), compared
   (substring or numeric checks), and quoted as short excerpts in citations.

2. The engine NEVER invokes privileged tools.
   RCAEngine is constructed with a read-only client: diagnose() passes only
   read-only tool names from its evidence plans to client.call_tool. The
   remediation dict and the RemediationPlan (which name privileged tools
   such as "restart_channel") are pure DATA for the ApprovalGate in
   agent.approvals -- diagnose() never calls them, and no code path in this
   module passes them to client.call_tool.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from agent.plans import RemediationPlan
from agent.remediation import plan_for


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------

@dataclass
class Citation:
    """A factual claim backed by a short excerpt of real tool output."""

    claim: str
    tool: str
    excerpt: str  # <= 160 chars, a substring of the tool's actual output


@dataclass
class Hypothesis:
    """One candidate root cause with its score, evidence, and remediation."""

    id: str
    title: str
    score: float  # fraction of checks matched, 0.0 - 1.0
    evidence: list[Citation]
    remediation: dict | None  # DATA for the approval gate; never executed here
    plan: RemediationPlan | None = None  # multi-step plan DATA; also never executed here


@dataclass
class Diagnosis:
    """Full result of diagnose(): hypotheses sorted by score, best first."""

    alert: dict
    hypotheses: list[Hypothesis]
    top: Hypothesis
    evidence_calls: list[dict]  # every attempted tool call: {"tool", "args"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_EXCERPT_LIMIT = 160
_CANDIDATE_CHANNELS = ["PAYMENTS.RCVR", "PAYMENTS.SDR", "ORDERS.RCVR"]
_KAFKA_TOPIC = "payments-events"
_KAFKA_GROUP = "payments-consumers"
_DEEP_BACKLOG = 10_000
_LAG_STALL = 50_000

_CheckFn = Callable[[dict, dict[str, Any]], tuple[bool, "Citation | None"]]
_RemediationFn = Callable[[dict, dict[str, Any]], "dict | None"]


def _short(text: str, limit: int = _EXCERPT_LIMIT) -> str:
    """Clip to a short excerpt; the result stays a substring of the input."""
    return text[:limit]


def _ev_key(name: str, args: dict) -> str:
    return name + "|" + json.dumps(args, sort_keys=True)


def _num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


# ---------------------------------------------------------------------------
# Evidence checks: (alert, evidence) -> (matched, citation)
# ---------------------------------------------------------------------------

def _channel_statuses(alert: dict, ev: dict[str, Any]) -> list[tuple[str, dict]]:
    """(channel, output) for every candidate channel that answered."""
    out = []
    for ch in _CANDIDATE_CHANNELS:
        resp = ev.get(_ev_key("get_channel_status",
                             {"qmgr": alert["qmgr"], "channel": ch}))
        if isinstance(resp, dict):
            out.append((ch, resp))
    return out


def _check_any_channel_stopped(alert: dict, ev: dict[str, Any]):
    for ch, resp in _channel_statuses(alert, ev):
        if resp.get("status") == "STOPPED":
            return True, Citation(
                claim=f"Channel {ch} is STOPPED",
                tool="get_channel_status",
                excerpt=_short(json.dumps(resp, sort_keys=True)),
            )
    return False, None


def _check_all_channels_running(alert: dict, ev: dict[str, Any]):
    statuses = _channel_statuses(alert, ev)
    if statuses and all(resp.get("status") == "RUNNING" for _, resp in statuses):
        ch, resp = statuses[0]
        return True, Citation(
            claim=f"All {len(statuses)} queried channels are RUNNING (showing {ch})",
            tool="get_channel_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_error_log_mentions_stopped(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("read_error_log", {"qmgr": alert["qmgr"], "limit": 30}))
    text = json.dumps(resp, sort_keys=True)
    idx = text.lower().find("stopped")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Error log mentions a channel being stopped",
        tool="read_error_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _queue_depth(alert: dict, ev: dict[str, Any], queue: str | None = None):
    q = queue or alert["queue"]
    resp = ev.get(_ev_key("get_queue_depth", {"qmgr": alert["qmgr"], "queue": q}))
    if isinstance(resp, dict) and _num(resp.get("depth")):
        return resp["depth"], resp
    return None, resp


def _app_log_text(ev: dict[str, Any], app: str, limit: int = 30) -> str:
    resp = ev.get(_ev_key("read_app_log", {"app": app, "limit": limit}))
    return json.dumps(resp, sort_keys=True) if isinstance(resp, list) else ""


def _check_app_log_mentions_oom(alert: dict, ev: dict[str, Any]):
    text = _app_log_text(ev, alert["app"])
    idx = text.find("OutOfMemoryError")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="App log mentions OutOfMemoryError",
        tool="read_app_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_mem_over_90(alert: dict, ev: dict[str, Any]):
    m = _host_metrics(alert, ev)
    mem = m.get("mem_pct")
    if _num(mem) and mem > 90:
        return True, Citation(
            claim=f"Host {alert['host']} memory usage is {mem}% (over 90%)",
            tool="get_host_metrics",
            excerpt=_short(json.dumps(m, sort_keys=True)),
        )
    return False, None


def _check_disk_under_90(alert: dict, ev: dict[str, Any]):
    m = _host_metrics(alert, ev)
    disk = m.get("disk_pct")
    if _num(disk) and disk < 90:
        return True, Citation(
            claim=f"Host {alert['host']} disk usage is {disk}% (under 90%), "
            "so this is not a disk-full incident",
            tool="get_host_metrics",
            excerpt=_short(json.dumps(m, sort_keys=True)),
        )
    return False, None


def _check_cert_invalid(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_cert_status",
                          {"qmgr": alert["qmgr"], "channel": alert["channel"]}))
    if isinstance(resp, dict) and resp.get("valid") is False:
        return True, Citation(
            claim=(f"TLS certificate for channel {alert['channel']} is "
                   f"INVALID (expires {resp.get('expires')})"),
            tool="get_cert_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_error_log_mentions_cert(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("read_error_log", {"qmgr": alert["qmgr"], "limit": 30}))
    text = json.dumps(resp, sort_keys=True).lower()
    idx = text.find("certificate")
    if idx < 0:
        idx = text.find("expired")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Error log mentions an expired/certificate problem",
        tool="read_error_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _alert_channel_status(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_channel_status",
                          {"qmgr": alert["qmgr"], "channel": alert["channel"]}))
    if isinstance(resp, dict):
        return resp.get("status"), resp
    return None, resp


def _check_alert_channel_not_running(alert: dict, ev: dict[str, Any]):
    status, resp = _alert_channel_status(alert, ev)
    if status is not None and status != "RUNNING":
        return True, Citation(
            claim=f"Channel {alert['channel']} status is {status}, not RUNNING",
            tool="get_channel_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_alert_channel_stopped(alert: dict, ev: dict[str, Any]):
    status, resp = _alert_channel_status(alert, ev)
    if status == "STOPPED":
        return True, Citation(
            claim=f"Channel {alert['channel']} is STOPPED",
            tool="get_channel_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_listener_stopped(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_listener_status", {"name": alert["name"]}))
    if isinstance(resp, dict) and resp.get("status") == "STOPPED":
        return True, Citation(
            claim=f"Listener {alert['name']} is STOPPED",
            tool="get_listener_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _error_log_text(alert: dict, ev: dict[str, Any], qmgr: str | None = None,
                    limit: int = 30) -> str:
    resp = ev.get(_ev_key("read_error_log",
                          {"qmgr": qmgr or alert.get("qmgr", "QMGR1"),
                           "limit": limit}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_error_log_mentions_listener(alert: dict, ev: dict[str, Any]):
    text = _error_log_text(alert, ev)
    idx = text.find("listener")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Error log mentions a listener problem",
        tool="read_error_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_error_log_mentions_address_in_use(alert: dict, ev: dict[str, Any]):
    text = _error_log_text(alert, ev)
    idx = text.find("address in use")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Error log mentions 'address in use', suggesting a port conflict",
        tool="read_error_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_depth_at_maxdepth(alert: dict, ev: dict[str, Any]):
    depth, resp = _queue_depth(alert, ev)
    maxd = resp.get("max_depth") if isinstance(resp, dict) else None
    if _num(depth) and _num(maxd) and depth >= maxd:
        return True, Citation(
            claim=(f"Queue {alert['queue']} depth {int(depth)} has reached "
                   f"its MAXDEPTH {int(maxd)}"),
            tool="get_queue_depth",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _queue_config(alert: dict, ev: dict[str, Any], queue: str | None = None):
    resp = ev.get(_ev_key("get_config",
                          {"qmgr": alert["qmgr"], "object_type": "queue",
                           "name": queue or alert["queue"]}))
    return resp if isinstance(resp, dict) else {}


def _check_maxdepth_below_baseline(alert: dict, ev: dict[str, Any]):
    cfg = _queue_config(alert, ev)
    maxd, base = cfg.get("max_depth"), cfg.get("baseline_max_depth")
    if _num(maxd) and _num(base) and maxd < base:
        return True, Citation(
            claim=(f"Queue {alert['queue']} MAXDEPTH is {int(maxd)}, below its "
                   f"baseline {int(base)}: the config has drifted"),
            tool="get_config",
            excerpt=_short(json.dumps(cfg, sort_keys=True)),
        )
    return False, None


def _check_app_log_mentions_backout(alert: dict, ev: dict[str, Any]):
    text = _app_log_text(ev, "payments-api")
    idx = text.find("backed out")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="App log shows messages being backed out with application errors",
        tool="read_app_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_repeated_backouts(alert: dict, ev: dict[str, Any]):
    text = _app_log_text(ev, "payments-api")
    n = text.count("backed out")
    if n >= 3:
        return True, Citation(
            claim=f"App log shows {n} backout mentions (3+), the signature of "
            "a poison message looping",
            tool="read_app_log",
            excerpt=_short(text[:_EXCERPT_LIMIT]),
        )
    return False, None


def _check_no_repeated_backouts(alert: dict, ev: dict[str, Any]):
    text = _app_log_text(ev, "payments-api")
    if text.count("backed out") < 3:
        return True, Citation(
            claim="App log shows no repeated backouts, ruling out a poison "
            "message",
            tool="read_app_log",
            excerpt=_short(text[:_EXCERPT_LIMIT]),
        )
    return False, None


def _check_dlq_above_baseline(alert: dict, ev: dict[str, Any]):
    depth_resp = ev.get(_ev_key("get_queue_depth",
                                {"qmgr": alert["qmgr"], "queue": "SYSTEM.DLQ"}))
    cfg = _queue_config(alert, ev, "SYSTEM.DLQ")
    depth = depth_resp.get("depth") if isinstance(depth_resp, dict) else None
    baseline = cfg.get("baseline_depth")
    if _num(depth) and _num(baseline) and depth > baseline:
        return True, Citation(
            claim=(f"SYSTEM.DLQ depth is {int(depth)}, above its baseline of "
                   f"{int(baseline)}: dead letters are accumulating"),
            tool="get_queue_depth",
            excerpt=_short(json.dumps(depth_resp, sort_keys=True)),
        )
    return False, None


def _check_deep_backlog(alert: dict, ev: dict[str, Any]):
    depth, resp = _queue_depth(alert, ev)
    if depth is not None and depth > _DEEP_BACKLOG:
        return True, Citation(
            claim=f"Queue {alert['queue']} depth is {int(depth)} (over {_DEEP_BACKLOG})",
            tool="get_queue_depth",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _lag_high(topic: str, group: str, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_kafka_consumer_lag",
                          {"topic": topic, "group": group}))
    lag = resp.get("lag") if isinstance(resp, dict) else None
    if _num(lag) and lag > _LAG_STALL:
        return True, Citation(
            claim=(f"Kafka consumer lag for {topic}/{group} is {int(lag)} "
                   f"(over {_LAG_STALL})"),
            tool="get_kafka_consumer_lag",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_downstream_lag_high(alert: dict, ev: dict[str, Any]):
    return _lag_high(_KAFKA_TOPIC, _KAFKA_GROUP, ev)


def _check_topic_lag_high(alert: dict, ev: dict[str, Any]):
    return _lag_high(alert["topic"], alert["group"], ev)


def _broker_configs(alert: dict, ev: dict[str, Any]) -> dict[int, dict]:
    """broker_id -> configs dict for every broker that answered."""
    out = {}
    for bid in (1, 2, 3):
        resp = ev.get(_ev_key("get_kafka_broker_config", {"broker_id": bid}))
        if isinstance(resp, dict):
            out[bid] = resp.get("configs", {})
    return out


def _check_broker_config_drift(alert: dict, ev: dict[str, Any]):
    configs = _broker_configs(alert, ev)
    if len(configs) < 2:
        return False, None
    retention = {bid: cfg.get("log.retention.hours") for bid, cfg in configs.items()}
    if len(set(retention.values())) > 1:
        odd = sorted(bid for bid, val in retention.items()
                     if val != retention[1])
        majority = retention[1]
        claim = (f"Broker {odd[0]} has log.retention.hours={retention[odd[0]]} "
                 f"while broker 1 has {majority}: broker config has drifted")
        excerpt = _short(json.dumps(configs[odd[0]], sort_keys=True))
        return True, Citation(claim=claim, tool="get_kafka_broker_config",
                              excerpt=excerpt)
    return False, None


def _check_broker_configs_agree(alert: dict, ev: dict[str, Any]):
    ok, _ = _check_broker_config_drift(alert, ev)
    if ok:
        return False, None
    resp = ev.get(_ev_key("get_kafka_broker_config", {"broker_id": 1}))
    if isinstance(resp, dict):
        return True, Citation(
            claim="Broker configs are consistent across brokers 1-3",
            tool="get_kafka_broker_config",
            excerpt=_short(json.dumps(resp.get("configs", {}),
                                      sort_keys=True)),
        )
    return False, None


def _check_isr_shrunk(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_kafka_topic_detail",
                          {"topic": alert["topic"]}))
    if not isinstance(resp, dict):
        return False, None
    for p in resp.get("partitions", []):
        replicas = p.get("replicas", [])
        isr = p.get("isr", [])
        if len(isr) < len(replicas):
            return True, Citation(
                claim=(f"Partition {p.get('partition')} of {alert['topic']} "
                       f"is under-replicated: ISR {isr} is smaller than "
                       f"replicas {replicas}"),
                tool="get_kafka_topic_detail",
                excerpt=_short(json.dumps(p, sort_keys=True)),
            )
    return False, None


def _check_cluster_reachable(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_kafka_broker_health", {}))
    if isinstance(resp, dict) and resp.get("reachable") is True \
            and not resp.get("degraded"):
        return True, Citation(
            claim="Kafka cluster is reachable and no broker reports degraded",
            tool="get_kafka_broker_health",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_cluster_unreachable(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_kafka_broker_health", {}))
    if isinstance(resp, dict) and (resp.get("reachable") is not True
                                   or resp.get("degraded")):
        return True, Citation(
            claim="Kafka cluster is unreachable or degraded",
            tool="get_kafka_broker_health",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_threadpool_saturated(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_tomcat_threadpool", {"pool": alert["pool"]}))
    if not isinstance(resp, dict):
        return False, None
    pools = resp.get("pools", [])
    if pools:
        p = pools[0]
        busy, mx = p.get("current_threads_busy"), p.get("max_threads")
        if _num(busy) and _num(mx) and busy >= mx:
            return True, Citation(
                claim=(f"Thread pool {alert['pool']} is saturated: "
                       f"{int(busy)}/{int(mx)} threads busy"),
                tool="get_tomcat_threadpool",
                excerpt=_short(json.dumps(p, sort_keys=True)),
            )
    return False, None


def _check_threadpool_not_saturated(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_tomcat_threadpool", {"pool": alert["pool"]}))
    if not isinstance(resp, dict):
        return False, None
    pools = resp.get("pools", [])
    if pools:
        p = pools[0]
        busy, mx = p.get("current_threads_busy"), p.get("max_threads")
        if _num(busy) and _num(mx) and busy < mx:
            return True, Citation(
                claim=(f"Thread pool {alert['pool']} has headroom: "
                       f"{int(busy)}/{int(mx)} threads busy"),
                tool="get_tomcat_threadpool",
                excerpt=_short(json.dumps(p, sort_keys=True)),
            )
    return False, None


def _check_tomcat_log_threads_busy(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("read_tomcat_log", {"log": "catalina", "limit": 30}))
    text = json.dumps(resp, sort_keys=True) if resp is not None else ""
    idx = text.find("currently busy")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="catalina log reports that all threads are currently busy",
        tool="read_tomcat_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_tomcat_heap_healthy(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_tomcat_heap", {}))
    pct = resp.get("heap_used_pct") if isinstance(resp, dict) else None
    if _num(pct) and pct < 85:
        return True, Citation(
            claim=f"Tomcat heap usage is {pct:.1f}% (under 85%), "
            "so this is not a heap-exhaustion incident",
            tool="get_tomcat_heap",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_tomcat_heap_exhausted(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_tomcat_heap", {}))
    pct = resp.get("heap_used_pct") if isinstance(resp, dict) else None
    if _num(pct) and pct >= 90:
        return True, Citation(
            claim=f"Tomcat heap usage is {pct:.1f}% (90% or more)",
            tool="get_tomcat_heap",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_orders_app_running(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_tomcat_apps", {}))
    if not isinstance(resp, dict):
        return False, None
    for app in resp.get("apps", []):
        if app.get("path") == alert["app"] and app.get("state") == "running":
            return True, Citation(
                claim=(f"App {alert['app']} is still running (degraded, "
                       "not crashed): a restart can recover it"),
                tool="get_tomcat_apps",
                excerpt=_short(json.dumps(app, sort_keys=True)),
            )
    return False, None


def _mq_side(alert: dict, ev: dict[str, Any]):
    """(depth, depth_resp, channel_status, channel_resp) for QMGR1."""
    depth_resp = ev.get(_ev_key("get_queue_depth",
                                {"qmgr": "QMGR1", "queue": "PAYMENTS.IN"}))
    ch_resp = ev.get(_ev_key("get_channel_status",
                             {"qmgr": "QMGR1", "channel": "PAYMENTS.RCVR"}))
    depth = depth_resp.get("depth") if isinstance(depth_resp, dict) else None
    status = ch_resp.get("status") if isinstance(ch_resp, dict) else None
    return depth, depth_resp, status, ch_resp


def _check_mq_side_healthy(alert: dict, ev: dict[str, Any]):
    depth, depth_resp, status, _ = _mq_side(alert, ev)
    if _num(depth) and depth < _DEEP_BACKLOG and status == "RUNNING":
        return True, Citation(
            claim=(f"MQ queue depth is {int(depth)} (under {_DEEP_BACKLOG}) and "
                   "PAYMENTS.RCVR is RUNNING, isolating the fault to Kafka"),
            tool="get_queue_depth",
            excerpt=_short(json.dumps(depth_resp, sort_keys=True)),
        )
    return False, None


def _check_mq_side_degraded(alert: dict, ev: dict[str, Any]):
    depth, depth_resp, status, ch_resp = _mq_side(alert, ev)
    depth_high = _num(depth) and depth >= _DEEP_BACKLOG
    ch_bad = status is not None and status != "RUNNING"
    if depth_high or ch_bad:
        if depth_high:
            claim = (f"MQ queue PAYMENTS.IN depth is {int(depth)} "
                     f"(at/over {_DEEP_BACKLOG})")
            tool, resp = "get_queue_depth", depth_resp
        else:
            claim = f"Channel PAYMENTS.RCVR status is {status}, not RUNNING"
            tool, resp = "get_channel_status", ch_resp
        return True, Citation(claim=claim, tool=tool,
                              excerpt=_short(json.dumps(resp, sort_keys=True)))
    return False, None


def _host_metrics(alert: dict, ev: dict[str, Any]) -> dict:
    resp = ev.get(_ev_key("get_host_metrics", {"host": alert["host"]}))
    return resp if isinstance(resp, dict) else {}


def _check_disk_pressure(alert: dict, ev: dict[str, Any]):
    m = _host_metrics(alert, ev)
    disk = m.get("disk_pct")
    if _num(disk) and disk > 90:
        return True, Citation(
            claim=f"Host {alert['host']} disk usage is {disk}% (over 90%)",
            tool="get_host_metrics",
            excerpt=_short(json.dumps(m, sort_keys=True)),
        )
    return False, None


def _check_compute_healthy(alert: dict, ev: dict[str, Any]):
    m = _host_metrics(alert, ev)
    cpu, mem = m.get("cpu_pct"), m.get("mem_pct")
    if _num(cpu) and _num(mem) and cpu < 80 and mem < 85:
        return True, Citation(
            claim=(f"Host {alert['host']} CPU ({cpu}%) and memory ({mem}%) are "
                   "within normal bounds, pointing at disk rather than compute"),
            tool="get_host_metrics",
            excerpt=_short(json.dumps(m, sort_keys=True)),
        )
    return False, None


def _check_host_all_healthy(alert: dict, ev: dict[str, Any]):
    m = _host_metrics(alert, ev)
    cpu, mem, disk = m.get("cpu_pct"), m.get("mem_pct"), m.get("disk_pct")
    if (_num(cpu) and _num(mem) and _num(disk)
            and cpu < 80 and mem < 85 and disk < 90):
        return True, Citation(
            claim=(f"Host {alert['host']} metrics are all healthy, pointing to an "
                   "application-level fault outside the current toolset's view"),
            tool="get_host_metrics",
            excerpt=_short(json.dumps(m, sort_keys=True)),
        )
    return False, None


def _remediation_restart_stopped(alert: dict, ev: dict[str, Any]) -> dict | None:
    """Build (not execute) the restart remediation for the stopped channel."""
    for ch, resp in _channel_statuses(alert, ev):
        if resp.get("status") == "STOPPED":
            return {
                "tool": "restart_channel",
                "args": {"qmgr": alert["qmgr"], "channel": ch},
                "requires_scope": "admin:write",
                "rationale": (
                    f"Channel {ch} on {alert['qmgr']} is STOPPED while queue "
                    f"{alert['queue']} has a deep backlog; restarting the "
                    "receiver channel should resume message flow. "
                    "Requires human approval."
                ),
            }
    return None


# ---------------------------------------------------------------------------
# Wave 3 evidence checks: (alert, evidence) -> (matched, citation)
# ---------------------------------------------------------------------------

def _websphere_pool(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_websphere_threadpool", {"pool": alert["pool"]}))
    if isinstance(resp, dict):
        pools = resp.get("pools", [])
        if pools:
            return pools[0]
    return None


def _check_websphere_pool_saturated(alert: dict, ev: dict[str, Any]):
    p = _websphere_pool(alert, ev)
    pct = p.get("busy_pct") if p else None
    if _num(pct) and pct > 90:
        return True, Citation(
            claim=(f"WebSphere thread pool {alert['pool']} is {pct:.1f}% busy "
                   "(over 90%)"),
            tool="get_websphere_threadpool",
            excerpt=_short(json.dumps(p, sort_keys=True)),
        )
    return False, None


def _websphere_log_text(ev: dict[str, Any], limit: int = 30) -> str:
    resp = ev.get(_ev_key("read_websphere_log",
                          {"log": "systemout", "limit": limit}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_websphere_log_mentions_thread(alert: dict, ev: dict[str, Any]):
    text = _websphere_log_text(ev)
    idx = text.find("thread")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="WebSphere system log mentions thread-pool pressure",
        tool="read_websphere_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _websphere_app(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_websphere_apps", {}))
    if isinstance(resp, dict):
        for app in resp.get("apps", []):
            if app.get("name") == alert["app"]:
                return app
    return None


def _check_websphere_app_stopped(alert: dict, ev: dict[str, Any]):
    app = _websphere_app(alert, ev)
    if app is not None and app.get("state") == "stopped":
        return True, Citation(
            claim=f"WebSphere app {alert['app']} is stopped",
            tool="get_websphere_apps",
            excerpt=_short(json.dumps(app, sort_keys=True)),
        )
    return False, None


def _check_websphere_app_missing(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_websphere_apps", {}))
    apps = resp.get("apps", []) if isinstance(resp, dict) else []
    if apps and all(a.get("name") != alert["app"] for a in apps):
        return True, Citation(
            claim=(f"App {alert['app']} is absent from the deployed apps "
                   "list, suggesting a failed deployment"),
            tool="get_websphere_apps",
            excerpt=_short(json.dumps(apps, sort_keys=True)),
        )
    return False, None


def _weblogic_heap_pct(ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_weblogic_heap", {}))
    pct = resp.get("heap_used_pct") if isinstance(resp, dict) else None
    return (pct, resp) if _num(pct) else (None, resp)


def _check_weblogic_heap_over_90(alert: dict, ev: dict[str, Any]):
    pct, resp = _weblogic_heap_pct(ev)
    if pct is not None and pct > 90:
        return True, Citation(
            claim=f"WebLogic heap usage is {pct:.1f}% (over 90%)",
            tool="get_weblogic_heap",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_weblogic_heap_moderate(alert: dict, ev: dict[str, Any]):
    pct, resp = _weblogic_heap_pct(ev)
    if pct is not None and 85 <= pct < 90:
        return True, Citation(
            claim=f"WebLogic heap usage is {pct:.1f}% (elevated but under 90%)",
            tool="get_weblogic_heap",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _weblogic_log_text(ev: dict[str, Any]) -> str:
    resp = ev.get(_ev_key("read_weblogic_log",
                          {"log": "server", "limit": 30}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_weblogic_log_mentions_pressure(alert: dict, ev: dict[str, Any]):
    text = _weblogic_log_text(ev)
    idx = text.find("utilization")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="WebLogic server log shows a resource-utilization warning",
        tool="read_weblogic_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _weblogic_pools(ev: dict[str, Any]) -> list[dict]:
    resp = ev.get(_ev_key("get_weblogic_threadpool", {}))
    if isinstance(resp, dict):
        return resp.get("pools", [])
    return []


def _check_weblogic_pool_fully_busy(alert: dict, ev: dict[str, Any]):
    for p in _weblogic_pools(ev):
        busy, count = p.get("current_threads_busy"), p.get("current_thread_count")
        if _num(busy) and _num(count) and count > 0 and busy >= count:
            return True, Citation(
                claim=(f"WebLogic thread pool {p.get('name')} is fully busy "
                       f"({int(busy)}/{int(count)} threads)"),
                tool="get_weblogic_threadpool",
                excerpt=_short(json.dumps(p, sort_keys=True)),
            )
    return False, None


def _check_weblogic_pool_has_headroom(alert: dict, ev: dict[str, Any]):
    for p in _weblogic_pools(ev):
        busy, count = p.get("current_threads_busy"), p.get("current_thread_count")
        if _num(busy) and _num(count) and count > 0 and busy < count:
            return True, Citation(
                claim=(f"WebLogic thread pool {p.get('name')} has headroom "
                       f"({int(busy)}/{int(count)} threads busy)"),
                tool="get_weblogic_threadpool",
                excerpt=_short(json.dumps(p, sort_keys=True)),
            )
    return False, None


def _jboss_deployment(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_jboss_deployments", {}))
    if isinstance(resp, dict):
        for dep in resp.get("deployments", []):
            if dep.get("name") == alert["deployment"]:
                return dep
    return None


def _check_jboss_deployment_failed(alert: dict, ev: dict[str, Any]):
    dep = _jboss_deployment(alert, ev)
    if dep is not None and (not dep.get("enabled")
                            or dep.get("status") != "OK"):
        return True, Citation(
            claim=(f"JBoss deployment {alert['deployment']} is unhealthy "
                   f"(enabled={dep.get('enabled')}, status={dep.get('status')})"),
            tool="get_jboss_deployments",
            excerpt=_short(json.dumps(dep, sort_keys=True)),
        )
    return False, None


def _jboss_log_text(ev: dict[str, Any]) -> str:
    resp = ev.get(_ev_key("read_jboss_log", {"log": "server", "limit": 30}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_jboss_log_mentions_failed(alert: dict, ev: dict[str, Any]):
    text = _jboss_log_text(ev)
    idx = text.find("failed")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="JBoss server log records a failed invocation",
        tool="read_jboss_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_jboss_log_mentions_dependency(alert: dict, ev: dict[str, Any]):
    text = _jboss_log_text(ev)
    idx = text.find("dependency")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="JBoss server log mentions a missing dependency",
        tool="read_jboss_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_jboss_deployments_healthy(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_jboss_deployments", {}))
    deps = resp.get("deployments", []) if isinstance(resp, dict) else []
    if deps and all(d.get("enabled") and d.get("status") == "OK" for d in deps):
        return True, Citation(
            claim=f"All {len(deps)} JBoss deployments are enabled and OK",
            tool="get_jboss_deployments",
            excerpt=_short(json.dumps(deps, sort_keys=True)),
        )
    return False, None


def _jboss_heap_pct(ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_jboss_heap", {}))
    pct = resp.get("heap_used_pct") if isinstance(resp, dict) else None
    return (pct, resp) if _num(pct) else (None, resp)


def _check_jboss_heap_over_90(alert: dict, ev: dict[str, Any]):
    pct, resp = _jboss_heap_pct(ev)
    if pct is not None and pct > 90:
        return True, Citation(
            claim=f"JBoss heap usage is {pct:.1f}% (over 90%)",
            tool="get_jboss_heap",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_jboss_heap_moderate(alert: dict, ev: dict[str, Any]):
    pct, resp = _jboss_heap_pct(ev)
    if pct is not None and 85 <= pct < 90:
        return True, Citation(
            claim=f"JBoss heap usage is {pct:.1f}% (elevated but under 90%)",
            tool="get_jboss_heap",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _rmq_queue(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_rabbitmq_queues", {"vhost": alert["vhost"]}))
    if isinstance(resp, dict):
        for q in resp.get("queues", []):
            if q.get("name") == alert["queue"]:
                return q
    return None


def _check_rmq_queue_deep(alert: dict, ev: dict[str, Any]):
    q = _rmq_queue(alert, ev)
    n = q.get("messages") if q else None
    if _num(n) and n > 50_000:
        return True, Citation(
            claim=(f"RabbitMQ queue {alert['queue']} has {int(n)} messages "
                   "(over 50,000)"),
            tool="get_rabbitmq_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_rmq_queue_no_consumers(alert: dict, ev: dict[str, Any]):
    q = _rmq_queue(alert, ev)
    if q is not None and q.get("consumers") == 0:
        return True, Citation(
            claim=f"RabbitMQ queue {alert['queue']} has zero consumers",
            tool="get_rabbitmq_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_rmq_queue_has_consumers(alert: dict, ev: dict[str, Any]):
    q = _rmq_queue(alert, ev)
    n = q.get("consumers") if q else None
    if _num(n) and n > 0:
        return True, Citation(
            claim=(f"RabbitMQ queue {alert['queue']} has {int(n)} consumers "
                   "draining it"),
            tool="get_rabbitmq_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _rmq_nodes(ev: dict[str, Any]) -> list[dict]:
    resp = ev.get(_ev_key("get_rabbitmq_nodes", {}))
    if isinstance(resp, dict):
        return resp.get("nodes", [])
    return []


def _check_rmq_disk_free_low(alert: dict, ev: dict[str, Any]):
    for n in _rmq_nodes(ev):
        free = n.get("disk_free_bytes")
        if _num(free) and free < 1073741824:
            return True, Citation(
                claim=(f"RabbitMQ node {n.get('name')} has only "
                       f"{int(free)} bytes disk free (under 1 GiB): "
                       "disk alarm"),
                tool="get_rabbitmq_nodes",
                excerpt=_short(json.dumps(n, sort_keys=True)),
            )
    return False, None


def _check_rmq_disk_free_healthy(alert: dict, ev: dict[str, Any]):
    nodes = _rmq_nodes(ev)
    if nodes and all(_num(n.get("disk_free_bytes"))
                     and n["disk_free_bytes"] >= 1073741824 for n in nodes):
        return True, Citation(
            claim="All RabbitMQ nodes have healthy disk space (1 GiB+ free)",
            tool="get_rabbitmq_nodes",
            excerpt=_short(json.dumps(nodes[0], sort_keys=True)),
        )
    return False, None


def _check_rmq_mem_high(alert: dict, ev: dict[str, Any]):
    for n in _rmq_nodes(ev):
        pct = n.get("mem_used_pct")
        if _num(pct) and pct > 90:
            return True, Citation(
                claim=(f"RabbitMQ node {n.get('name')} memory is {pct:.1f}% "
                       "(over 90%)"),
                tool="get_rabbitmq_nodes",
                excerpt=_short(json.dumps(n, sort_keys=True)),
            )
    return False, None


def _artemis_queue(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_artemis_queues", {}))
    if isinstance(resp, dict):
        for q in resp.get("queues", []):
            if q.get("name") == alert["queue"]:
                return q
    return None


def _check_artemis_queue_deep(alert: dict, ev: dict[str, Any]):
    q = _artemis_queue(alert, ev)
    n = q.get("message_count") if q else None
    if _num(n) and n > 10_000:
        return True, Citation(
            claim=(f"Artemis queue {alert['queue']} has {int(n)} messages "
                   "(over 10,000)"),
            tool="get_artemis_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_artemis_queue_no_consumers(alert: dict, ev: dict[str, Any]):
    q = _artemis_queue(alert, ev)
    if q is not None and q.get("consumer_count") == 0:
        return True, Citation(
            claim=f"Artemis queue {alert['queue']} has zero consumers",
            tool="get_artemis_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_artemis_queue_has_consumers(alert: dict, ev: dict[str, Any]):
    q = _artemis_queue(alert, ev)
    n = q.get("consumer_count") if q else None
    if _num(n) and n > 0:
        return True, Citation(
            claim=(f"Artemis queue {alert['queue']} has {int(n)} consumers "
                   "draining it"),
            tool="get_artemis_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_artemis_broker_down(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_artemis_broker", {}))
    if isinstance(resp, dict) and resp.get("started") is False:
        return True, Citation(
            claim="Artemis broker reports started=false: the broker is down",
            tool="get_artemis_broker",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_artemis_broker_started(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_artemis_broker", {}))
    if isinstance(resp, dict) and resp.get("started") is True:
        return True, Citation(
            claim="Artemis broker reports started=true",
            tool="get_artemis_broker",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _ems_queue(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_ems_queues", {}))
    if isinstance(resp, dict):
        for q in resp.get("queues", []):
            if q.get("name") == alert["queue"]:
                return q
    return None


def _check_ems_queue_deep(alert: dict, ev: dict[str, Any]):
    q = _ems_queue(alert, ev)
    n = q.get("pending_messages") if q else None
    if _num(n) and n > 10_000:
        return True, Citation(
            claim=(f"EMS queue {alert['queue']} has {int(n)} pending messages "
                   "(over 10,000)"),
            tool="get_ems_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_ems_queue_no_consumers(alert: dict, ev: dict[str, Any]):
    q = _ems_queue(alert, ev)
    if q is not None and q.get("consumers") == 0:
        return True, Citation(
            claim=f"EMS queue {alert['queue']} has zero consumers",
            tool="get_ems_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _check_ems_queue_has_consumers(alert: dict, ev: dict[str, Any]):
    q = _ems_queue(alert, ev)
    n = q.get("consumers") if q else None
    if _num(n) and n > 0:
        return True, Citation(
            claim=(f"EMS queue {alert['queue']} has {int(n)} consumers "
                   "draining it"),
            tool="get_ems_queues",
            excerpt=_short(json.dumps(q, sort_keys=True)),
        )
    return False, None


def _ems_connections(ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_ems_server", {}))
    conns = resp.get("connections") if isinstance(resp, dict) else None
    return conns, resp


def _check_ems_connection_storm(alert: dict, ev: dict[str, Any]):
    conns, resp = _ems_connections(ev)
    if _num(conns) and conns > 5000:
        return True, Citation(
            claim=(f"EMS server has {int(conns)} connections (over 5,000): "
                   "a connection storm"),
            tool="get_ems_server",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_ems_connections_normal(alert: dict, ev: dict[str, Any]):
    conns, resp = _ems_connections(ev)
    if _num(conns) and conns < 500:
        return True, Citation(
            claim=f"EMS server connections are normal ({int(conns)})",
            tool="get_ems_server",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _nginx_error_text(ev: dict[str, Any]) -> str:
    resp = ev.get(_ev_key("read_nginx_log", {"log": "error", "limit": 30}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_nginx_log_mentions_upstream(alert: dict, ev: dict[str, Any]):
    text = _nginx_error_text(ev)
    idx = text.find("upstream")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="nginx error log records upstream connection failures",
        tool="read_nginx_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_nginx_log_mentions_timeout(alert: dict, ev: dict[str, Any]):
    text = _nginx_error_text(ev)
    idx = text.find("timeout")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="nginx error log mentions upstream timeouts",
        tool="read_nginx_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_nginx_log_mentions_failed(alert: dict, ev: dict[str, Any]):
    text = _nginx_error_text(ev)
    idx = text.find("failed")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="nginx error log records failed connections",
        tool="read_nginx_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_nginx_serving(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_nginx_status", {}))
    active = resp.get("active_connections") if isinstance(resp, dict) else None
    if _num(active) and active > 0:
        return True, Citation(
            claim=f"nginx is still serving ({int(active)} active connections)",
            tool="get_nginx_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_nginx_waiting_high(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_nginx_status", {}))
    waiting = resp.get("waiting") if isinstance(resp, dict) else None
    if _num(waiting) and waiting > 1000:
        return True, Citation(
            claim=(f"nginx has {int(waiting)} connections waiting "
                   "(over 1,000): an overload signature"),
            tool="get_nginx_status",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _apache_status(alert: dict, ev: dict[str, Any]) -> dict:
    resp = ev.get(_ev_key("get_apache_status", {}))
    return resp if isinstance(resp, dict) else {}


def _check_apache_workers_saturated(alert: dict, ev: dict[str, Any]):
    s = _apache_status(alert, ev)
    busy, idle = s.get("busy_workers"), s.get("idle_workers")
    if _num(busy) and _num(idle) and busy >= 100 and idle == 0:
        return True, Citation(
            claim=(f"Apache workers are saturated: {int(busy)} busy, "
                   f"{int(idle)} idle"),
            tool="get_apache_status",
            excerpt=_short(json.dumps(s, sort_keys=True)),
        )
    return False, None


def _check_apache_workers_available(alert: dict, ev: dict[str, Any]):
    s = _apache_status(alert, ev)
    idle = s.get("idle_workers")
    if _num(idle) and idle > 0:
        return True, Citation(
            claim=f"Apache has {int(idle)} idle workers available",
            tool="get_apache_status",
            excerpt=_short(json.dumps(s, sort_keys=True)),
        )
    return False, None


def _apache_error_text(ev: dict[str, Any]) -> str:
    resp = ev.get(_ev_key("read_apache_log", {"log": "error", "limit": 30}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_apache_log_mentions_worker(alert: dict, ev: dict[str, Any]):
    text = _apache_error_text(ev)
    idx = text.find("worker")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Apache error log mentions a failed backend worker",
        tool="read_apache_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_apache_log_mentions_failed(alert: dict, ev: dict[str, Any]):
    text = _apache_error_text(ev)
    idx = text.find("failed")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Apache error log records failed backend requests",
        tool="read_apache_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_apache_log_mentions_config(alert: dict, ev: dict[str, Any]):
    text = _apache_error_text(ev)
    idx = text.find("config")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Apache error log mentions a configuration problem",
        tool="read_apache_log",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _haproxy_server(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_haproxy_stats", {}))
    if isinstance(resp, dict):
        for b in resp.get("backends", []):
            if b.get("name") == alert["backend"]:
                for s in b.get("servers", []):
                    if s.get("name") == alert["server"]:
                        return s
    return None


def _check_haproxy_server_check_failing(alert: dict, ev: dict[str, Any]):
    s = _haproxy_server(alert, ev)
    if s is not None and s.get("check_status") != "L7OK":
        return True, Citation(
            claim=(f"HAProxy server {alert['server']} in backend "
                   f"{alert['backend']} is failing its health check "
                   f"({s.get('check_status')})"),
            tool="get_haproxy_stats",
            excerpt=_short(json.dumps(s, sort_keys=True)),
        )
    return False, None


def _check_haproxy_server_up(alert: dict, ev: dict[str, Any]):
    s = _haproxy_server(alert, ev)
    if (s is not None and s.get("status") == "UP"
            and s.get("check_status") == "L7OK"):
        return True, Citation(
            claim=(f"HAProxy server {alert['server']} is UP with a passing "
                   "health check"),
            tool="get_haproxy_stats",
            excerpt=_short(json.dumps(s, sort_keys=True)),
        )
    return False, None


def _haproxy_frontend(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_haproxy_stats", {}))
    if isinstance(resp, dict):
        for f in resp.get("frontends", []):
            if f.get("name") == alert["frontend"]:
                return f
    return None


def _check_haproxy_frontend_saturated(alert: dict, ev: dict[str, Any]):
    f = _haproxy_frontend(alert, ev)
    n = f.get("current_sessions") if f else None
    if _num(n) and n >= 1000:
        return True, Citation(
            claim=(f"HAProxy frontend {alert['frontend']} has {int(n)} "
                   "current sessions (1,000+): the session table is full"),
            tool="get_haproxy_stats",
            excerpt=_short(json.dumps(f, sort_keys=True)),
        )
    return False, None


def _check_haproxy_frontend_normal(alert: dict, ev: dict[str, Any]):
    f = _haproxy_frontend(alert, ev)
    n = f.get("current_sessions") if f else None
    if _num(n) and n < 1000:
        return True, Citation(
            claim=(f"HAProxy frontend {alert['frontend']} session count is "
                   f"normal ({int(n)})"),
            tool="get_haproxy_stats",
            excerpt=_short(json.dumps(f, sort_keys=True)),
        )
    return False, None


def _pg_blocker(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_postgres_blocking", {}))
    if isinstance(resp, dict):
        for b in resp.get("blockers", []):
            if b.get("pid") == alert["pid"]:
                return b
    return None


def _check_pg_blocker_present(alert: dict, ev: dict[str, Any]):
    b = _pg_blocker(alert, ev)
    if b is not None:
        return True, Citation(
            claim=f"Postgres backend pid {alert['pid']} is blocking sessions",
            tool="get_postgres_blocking",
            excerpt=_short(json.dumps(b, sort_keys=True)),
        )
    return False, None


def _check_pg_blocker_long_running(alert: dict, ev: dict[str, Any]):
    b = _pg_blocker(alert, ev)
    wait = b.get("wait_seconds") if b else None
    if _num(wait) and wait > 600:
        return True, Citation(
            claim=(f"Postgres blocker pid {alert['pid']} has been waiting "
                   f"{int(wait)}s (over 600s)"),
            tool="get_postgres_blocking",
            excerpt=_short(json.dumps(b, sort_keys=True)),
        )
    return False, None


def _check_pg_multiple_blockers(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_postgres_blocking", {}))
    blockers = resp.get("blockers", []) if isinstance(resp, dict) else []
    if len(blockers) > 1:
        return True, Citation(
            claim=(f"Postgres has {len(blockers)} blocking sessions: "
                   "a lock storm"),
            tool="get_postgres_blocking",
            excerpt=_short(json.dumps(blockers, sort_keys=True)),
        )
    return False, None


def _pg_replication_lag(ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_postgres_replication", {}))
    lag = resp.get("replay_lag_seconds") if isinstance(resp, dict) else None
    return (lag, resp) if _num(lag) else (None, resp)


def _check_pg_replication_lag_high(alert: dict, ev: dict[str, Any]):
    lag, resp = _pg_replication_lag(ev)
    if lag is not None and lag > 300:
        return True, Citation(
            claim=f"Postgres standby replay lag is {int(lag)}s (over 300s)",
            tool="get_postgres_replication",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_pg_replication_current(alert: dict, ev: dict[str, Any]):
    lag, resp = _pg_replication_lag(ev)
    if lag is not None and lag == 0:
        return True, Citation(
            claim="Postgres standby is fully caught up (lag 0s)",
            tool="get_postgres_replication",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _mysql_process(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_mysql_processlist", {}))
    if isinstance(resp, dict):
        for p in resp.get("processes", []):
            if p.get("id") == alert["process_id"]:
                return p
    return None


def _check_mysql_process_present(alert: dict, ev: dict[str, Any]):
    p = _mysql_process(alert, ev)
    if p is not None:
        return True, Citation(
            claim=f"MySQL process {alert['process_id']} is present",
            tool="get_mysql_processlist",
            excerpt=_short(json.dumps(p, sort_keys=True)),
        )
    return False, None


def _check_mysql_process_long(alert: dict, ev: dict[str, Any]):
    p = _mysql_process(alert, ev)
    t = p.get("time_secs") if p else None
    if _num(t) and t > 1800:
        return True, Citation(
            claim=(f"MySQL process {alert['process_id']} has been running "
                   f"{int(t)}s (over 1,800s): a runaway query"),
            tool="get_mysql_processlist",
            excerpt=_short(json.dumps(p, sort_keys=True)),
        )
    return False, None


def _check_mysql_process_lock_wait(alert: dict, ev: dict[str, Any]):
    p = _mysql_process(alert, ev)
    state = (p.get("state") or "") if p else ""
    if "lock" in state.lower():
        return True, Citation(
            claim=(f"MySQL process {alert['process_id']} is waiting on a lock"),
            tool="get_mysql_processlist",
            excerpt=_short(json.dumps(p, sort_keys=True)),
        )
    return False, None


def _mysql_replication(ev: dict[str, Any]) -> dict:
    resp = ev.get(_ev_key("get_mysql_replication", {}))
    return resp if isinstance(resp, dict) else {}


def _check_mysql_replication_lag_high(alert: dict, ev: dict[str, Any]):
    r = _mysql_replication(ev)
    lag = r.get("seconds_behind_source")
    if _num(lag) and lag > 300:
        return True, Citation(
            claim=(f"MySQL replica is {int(lag)}s behind the source "
                   "(over 300s)"),
            tool="get_mysql_replication",
            excerpt=_short(json.dumps(r, sort_keys=True)),
        )
    return False, None


def _check_mysql_replication_stopped(alert: dict, ev: dict[str, Any]):
    r = _mysql_replication(ev)
    if r and (r.get("io_running") is not True
              or r.get("sql_running") is not True):
        return True, Citation(
            claim="MySQL replication threads are not both running",
            tool="get_mysql_replication",
            excerpt=_short(json.dumps(r, sort_keys=True)),
        )
    return False, None


def _oracle_tablespace(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_oracle_tablespaces", {}))
    if isinstance(resp, dict):
        for t in resp.get("tablespaces", []):
            if t.get("name") == alert["tablespace"]:
                return t
    return None


def _check_oracle_tablespace_full(alert: dict, ev: dict[str, Any]):
    t = _oracle_tablespace(alert, ev)
    pct = t.get("used_pct") if t else None
    if _num(pct) and pct > 95:
        return True, Citation(
            claim=(f"Oracle tablespace {alert['tablespace']} is {pct:.1f}% "
                   "full (over 95%)"),
            tool="get_oracle_tablespaces",
            excerpt=_short(json.dumps(t, sort_keys=True)),
        )
    return False, None


def _check_oracle_tablespace_healthy(alert: dict, ev: dict[str, Any]):
    t = _oracle_tablespace(alert, ev)
    pct = t.get("used_pct") if t else None
    if _num(pct) and pct < 80:
        return True, Citation(
            claim=(f"Oracle tablespace {alert['tablespace']} usage is "
                   f"{pct:.1f}% (under 80%)"),
            tool="get_oracle_tablespaces",
            excerpt=_short(json.dumps(t, sort_keys=True)),
        )
    return False, None


def _oracle_blocker(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_oracle_blocking", {}))
    if isinstance(resp, dict):
        for b in resp.get("blockers", []):
            if b.get("sid") == alert["sid"] and b.get("serial") == alert["serial"]:
                return b
    return None


def _check_oracle_blocker_present(alert: dict, ev: dict[str, Any]):
    b = _oracle_blocker(alert, ev)
    if b is not None:
        return True, Citation(
            claim=(f"Oracle session sid={alert['sid']},serial={alert['serial']} "
                   "is blocking"),
            tool="get_oracle_blocking",
            excerpt=_short(json.dumps(b, sort_keys=True)),
        )
    return False, None


def _check_oracle_many_blockers(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_oracle_blocking", {}))
    blockers = resp.get("blockers", []) if isinstance(resp, dict) else []
    if len(blockers) > 5:
        return True, Citation(
            claim=(f"Oracle has {len(blockers)} blocking sessions: "
                   "an enqueue storm"),
            tool="get_oracle_blocking",
            excerpt=_short(json.dumps(blockers, sort_keys=True)),
        )
    return False, None


def _redis_mem_pct(ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_redis_info", {}))
    pct = resp.get("mem_used_pct") if isinstance(resp, dict) else None
    return (pct, resp) if _num(pct) else (None, resp)


def _check_redis_mem_high(alert: dict, ev: dict[str, Any]):
    pct, resp = _redis_mem_pct(ev)
    if pct is not None and pct > 90:
        return True, Citation(
            claim=f"Redis memory usage is {pct:.1f}% of maxmemory (over 90%)",
            tool="get_redis_info",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_redis_mem_normal(alert: dict, ev: dict[str, Any]):
    pct, resp = _redis_mem_pct(ev)
    if pct is not None and pct < 50:
        return True, Citation(
            claim=f"Redis memory usage is {pct:.1f}% of maxmemory (under 50%)",
            tool="get_redis_info",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_redis_master_link_down(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_redis_replication", {}))
    if isinstance(resp, dict) and resp.get("master_link_status") == "down":
        return True, Citation(
            claim="Redis master link status is down: the replica is "
                  "disconnected",
            tool="get_redis_replication",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_redis_not_master(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_redis_replication", {}))
    if isinstance(resp, dict) and resp.get("role") != "master":
        return True, Citation(
            claim=(f"Redis role is {resp.get('role')}, not master: a failover "
                   "may be in progress"),
            tool="get_redis_replication",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_es_red(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_elasticsearch_cluster_health", {}))
    if isinstance(resp, dict) and resp.get("status") == "red":
        return True, Citation(
            claim="Elasticsearch cluster health is red",
            tool="get_elasticsearch_cluster_health",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_es_unassigned_shards(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_elasticsearch_cluster_health", {}))
    n = resp.get("unassigned_shards") if isinstance(resp, dict) else None
    if _num(n) and n > 0:
        return True, Citation(
            claim=f"Elasticsearch has {int(n)} unassigned shards",
            tool="get_elasticsearch_cluster_health",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _check_es_node_lost(alert: dict, ev: dict[str, Any]):
    resp = ev.get(_ev_key("get_elasticsearch_cluster_health", {}))
    n = resp.get("number_of_nodes") if isinstance(resp, dict) else None
    if _num(n) and n < 3:
        return True, Citation(
            claim=(f"Elasticsearch cluster has {int(n)} nodes (expected 3): "
                   "a node was lost"),
            tool="get_elasticsearch_cluster_health",
            excerpt=_short(json.dumps(resp, sort_keys=True)),
        )
    return False, None


def _es_nodes(ev: dict[str, Any]) -> list[dict]:
    resp = ev.get(_ev_key("get_elasticsearch_nodes", {}))
    if isinstance(resp, dict):
        return resp.get("nodes", [])
    return []


def _check_es_node_heap_high(alert: dict, ev: dict[str, Any]):
    for n in _es_nodes(ev):
        pct = n.get("heap_used_pct")
        if _num(pct) and pct > 90:
            return True, Citation(
                claim=(f"Elasticsearch node {n.get('name')} heap is "
                       f"{pct:.1f}% (over 90%)"),
                tool="get_elasticsearch_nodes",
                excerpt=_short(json.dumps(n, sort_keys=True)),
            )
    return False, None


def _check_es_nodes_heap_healthy(alert: dict, ev: dict[str, Any]):
    nodes = _es_nodes(ev)
    if nodes and all(_num(n.get("heap_used_pct"))
                     and n["heap_used_pct"] < 70 for n in nodes):
        return True, Citation(
            claim="All Elasticsearch nodes are under 70% heap",
            tool="get_elasticsearch_nodes",
            excerpt=_short(json.dumps(nodes[0], sort_keys=True)),
        )
    return False, None


def _mongo_op(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_mongo_current_ops", {}))
    if isinstance(resp, dict):
        for o in resp.get("ops", []):
            if o.get("opid") == alert["opid"]:
                return o
    return None


def _check_mongo_op_present(alert: dict, ev: dict[str, Any]):
    o = _mongo_op(alert, ev)
    if o is not None:
        return True, Citation(
            claim=f"MongoDB op {alert['opid']} is present",
            tool="get_mongo_current_ops",
            excerpt=_short(json.dumps(o, sort_keys=True)),
        )
    return False, None


def _check_mongo_op_long(alert: dict, ev: dict[str, Any]):
    o = _mongo_op(alert, ev)
    t = o.get("secs_running") if o else None
    if _num(t) and t > 600:
        return True, Citation(
            claim=(f"MongoDB op {alert['opid']} has been running {int(t)}s "
                   "(over 600s): a runaway operation"),
            tool="get_mongo_current_ops",
            excerpt=_short(json.dumps(o, sort_keys=True)),
        )
    return False, None


def _check_mongo_op_lock(alert: dict, ev: dict[str, Any]):
    o = _mongo_op(alert, ev)
    if o is not None and o.get("op") == "lock":
        return True, Citation(
            claim=f"MongoDB op {alert['opid']} is a lock operation",
            tool="get_mongo_current_ops",
            excerpt=_short(json.dumps(o, sort_keys=True)),
        )
    return False, None


def _mongo_members(ev: dict[str, Any]) -> list[dict]:
    resp = ev.get(_ev_key("get_mongo_replset", {}))
    if isinstance(resp, dict):
        return resp.get("members", [])
    return []


def _check_mongo_member_lagging(alert: dict, ev: dict[str, Any]):
    for m in _mongo_members(ev):
        lag = m.get("lag_seconds")
        if _num(lag) and lag > 300:
            return True, Citation(
                claim=(f"MongoDB member {m.get('name')} lags {int(lag)}s "
                       "(over 300s)"),
                tool="get_mongo_replset",
                excerpt=_short(json.dumps(m, sort_keys=True)),
            )
    return False, None


def _check_mongo_member_down(alert: dict, ev: dict[str, Any]):
    for m in _mongo_members(ev):
        if m.get("health") == 0:
            return True, Citation(
                claim=f"MongoDB member {m.get('name')} is down (health 0)",
                tool="get_mongo_replset",
                excerpt=_short(json.dumps(m, sort_keys=True)),
            )
    return False, None


def _k8s_pod_for_deployment(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_k8s_pod_status",
                          {"namespace": alert["namespace"]}))
    if isinstance(resp, dict):
        for p in resp.get("pods", []):
            if p.get("name", "").startswith(alert["deployment"] + "-"):
                return p
    return None


def _check_k8s_pod_crashloop(alert: dict, ev: dict[str, Any]):
    p = _k8s_pod_for_deployment(alert, ev)
    if p is not None and p.get("phase") == "CrashLoopBackOff":
        return True, Citation(
            claim=(f"Pod {p.get('name')} is in CrashLoopBackOff "
                   f"({p.get('restarts')} restarts)"),
            tool="get_k8s_pod_status",
            excerpt=_short(json.dumps(p, sort_keys=True)),
        )
    return False, None


def _k8s_events_text(alert: dict, ev: dict[str, Any]) -> str:
    resp = ev.get(_ev_key("get_k8s_events",
                          {"namespace": alert["namespace"], "limit": 25}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_k8s_events_oomkilled(alert: dict, ev: dict[str, Any]):
    text = _k8s_events_text(alert, ev)
    idx = text.find("oomkilled")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Kubernetes events show the container was OOMKilled",
        tool="get_k8s_events",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_k8s_events_imagepull(alert: dict, ev: dict[str, Any]):
    text = _k8s_events_text(alert, ev)
    idx = text.find("imagepull")
    if idx < 0:
        return False, None
    return True, Citation(
        claim="Kubernetes events show an image-pull failure",
        tool="get_k8s_events",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _k8s_deployments(alert: dict, ev: dict[str, Any]) -> list[dict]:
    resp = ev.get(_ev_key("get_k8s_deployments",
                          {"namespace": alert["namespace"]}))
    if isinstance(resp, dict):
        return resp.get("deployments", [])
    return []


def _check_k8s_deployment_unavailable(alert: dict, ev: dict[str, Any]):
    for d in _k8s_deployments(alert, ev):
        n = d.get("replicas_unavailable")
        if d.get("name") == alert["deployment"] and _num(n) and n > 0:
            return True, Citation(
                claim=(f"Kubernetes deployment {alert['deployment']} has "
                       f"{int(n)} unavailable replicas: the rollout is stuck"),
                tool="get_k8s_deployments",
                excerpt=_short(json.dumps(d, sort_keys=True)),
            )
    return False, None


def _check_k8s_deployments_healthy(alert: dict, ev: dict[str, Any]):
    deps = _k8s_deployments(alert, ev)
    if deps and all(d.get("replicas_unavailable", 0) == 0 for d in deps):
        return True, Citation(
            claim="All Kubernetes deployments have zero unavailable replicas",
            tool="get_k8s_deployments",
            excerpt=_short(json.dumps(deps[0], sort_keys=True)),
        )
    return False, None


def _docker_container(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_docker_containers", {}))
    if isinstance(resp, dict):
        for c in resp.get("containers", []):
            if c.get("name") == alert["container"]:
                return c
    return None


def _check_docker_container_exited(alert: dict, ev: dict[str, Any]):
    c = _docker_container(alert, ev)
    if c is not None and c.get("state") == "exited":
        return True, Citation(
            claim=(f"Docker container {alert['container']} has exited "
                   f"({c.get('status')})"),
            tool="get_docker_containers",
            excerpt=_short(json.dumps(c, sort_keys=True)),
        )
    return False, None


def _docker_log_text(alert: dict, ev: dict[str, Any]) -> str:
    resp = ev.get(_ev_key("read_docker_logs",
                          {"container": alert["container"], "limit": 30}))
    return json.dumps(resp, sort_keys=True).lower() if resp is not None else ""


def _check_docker_log_mentions_error(alert: dict, ev: dict[str, Any]):
    text = _docker_log_text(alert, ev)
    idx = text.find("error")
    if idx < 0:
        return False, None
    return True, Citation(
        claim=f"Container {alert['container']} logs contain an error",
        tool="read_docker_logs",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _check_docker_log_mentions_oomkilled(alert: dict, ev: dict[str, Any]):
    text = _docker_log_text(alert, ev)
    idx = text.find("oomkilled")
    if idx < 0:
        return False, None
    return True, Citation(
        claim=f"Container {alert['container']} logs mention OOMKilled",
        tool="read_docker_logs",
        excerpt=text[max(0, idx - 40):idx + 120][:_EXCERPT_LIMIT],
    )


def _docker_stat(alert: dict, ev: dict[str, Any]) -> dict | None:
    resp = ev.get(_ev_key("get_docker_stats", {}))
    if isinstance(resp, dict):
        for s in resp.get("stats", []):
            if s.get("name") == alert["container"]:
                return s
    return None


def _check_docker_mem_high(alert: dict, ev: dict[str, Any]):
    s = _docker_stat(alert, ev)
    pct = s.get("mem_pct") if s else None
    if _num(pct) and pct > 90:
        return True, Citation(
            claim=(f"Docker container {alert['container']} memory is "
                   f"{pct:.1f}% of its limit (over 90%)"),
            tool="get_docker_stats",
            excerpt=_short(json.dumps(s, sort_keys=True)),
        )
    return False, None


def _check_docker_mem_normal(alert: dict, ev: dict[str, Any]):
    s = _docker_stat(alert, ev)
    pct = s.get("mem_pct") if s else None
    if _num(pct) and pct < 50:
        return True, Citation(
            claim=(f"Docker container {alert['container']} memory is "
                   f"{pct:.1f}% of its limit (under 50%)"),
            tool="get_docker_stats",
            excerpt=_short(json.dumps(s, sort_keys=True)),
        )
    return False, None


# ---------------------------------------------------------------------------
# Evidence plans and hypothesis specs, per alert type
# ---------------------------------------------------------------------------

@dataclass
class _HypothesisSpec:
    id: str
    title: str
    checks: list[_CheckFn]
    remediation_fn: _RemediationFn | None = None
    plan_fn: Callable[[dict, dict[str, Any]], RemediationPlan | None] | None = None


def _plan_for(hypothesis_id: str):
    """Build a plan_fn that delegates to agent.remediation.plan_for."""
    def _fn(alert: dict, evidence: dict[str, Any]) -> RemediationPlan | None:
        return plan_for(hypothesis_id, alert, evidence)
    return _fn


def _plan_queue_backlog(alert: dict) -> list[tuple[str, dict]]:
    qmgr = alert["qmgr"]
    plan = [("get_queue_depth", {"qmgr": qmgr, "queue": alert["queue"]})]
    plan += [("get_channel_status", {"qmgr": qmgr, "channel": ch})
             for ch in _CANDIDATE_CHANNELS]
    plan.append(("read_error_log", {"qmgr": qmgr, "limit": 30}))
    plan.append(("get_kafka_consumer_lag",
                 {"topic": _KAFKA_TOPIC, "group": _KAFKA_GROUP}))
    # Extra views needed by the config-drift (h4) and poison-message (h5)
    # checks: the app log, the DLQ depth, and the queue configs.
    plan.append(("read_app_log", {"app": "payments-api", "limit": 30}))
    plan.append(("get_queue_depth", {"qmgr": qmgr, "queue": "SYSTEM.DLQ"}))
    plan.append(("get_config", {"qmgr": qmgr, "object_type": "queue",
                                "name": alert["queue"]}))
    plan.append(("get_config", {"qmgr": qmgr, "object_type": "queue",
                                "name": "SYSTEM.DLQ"}))
    return plan


def _plan_tomcat_errors(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_host_metrics", {"host": alert["host"]}),
        ("read_error_log", {"qmgr": "QMGR1", "limit": 20}),
        ("get_config", {"qmgr": "QMGR1", "object_type": "channel",
                        "name": "PAYMENTS.RCVR"}),
    ]


def _plan_kafka_lag(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_kafka_consumer_lag", {"topic": alert["topic"],
                                    "group": alert["group"]}),
        ("get_queue_depth", {"qmgr": "QMGR1", "queue": "PAYMENTS.IN"}),
        ("get_channel_status", {"qmgr": "QMGR1", "channel": "PAYMENTS.RCVR"}),
    ]


def _plan_app_oom(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_host_metrics", {"host": alert["host"]}),
        ("read_app_log", {"app": alert["app"], "limit": 30}),
    ]


def _plan_channel_tls_error(alert: dict) -> list[tuple[str, dict]]:
    qmgr = alert["qmgr"]
    return [
        ("get_cert_status", {"qmgr": qmgr, "channel": alert["channel"]}),
        ("get_channel_status", {"qmgr": qmgr, "channel": alert["channel"]}),
        ("read_error_log", {"qmgr": qmgr, "limit": 30}),
    ]


def _plan_listener_down(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_listener_status", {"name": alert["name"]}),
        ("read_error_log", {"qmgr": "QMGR1", "limit": 30}),
    ]


def _plan_consumer_lag(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_kafka_consumer_lag", {"topic": alert["topic"],
                                    "group": alert["group"]}),
        ("get_kafka_consumer_group_detail", {"topic": alert["topic"],
                                             "group": alert["group"]}),
        ("get_kafka_broker_config", {"broker_id": 1}),
        ("get_kafka_broker_config", {"broker_id": 2}),
        ("get_kafka_broker_config", {"broker_id": 3}),
    ]


def _plan_threadpool_saturated(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_tomcat_threadpool", {"pool": alert["pool"]}),
        ("read_tomcat_log", {"log": "catalina", "limit": 30}),
        ("get_tomcat_apps", {}),
        ("get_tomcat_heap", {}),
    ]


def _plan_kafka_under_replicated(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_kafka_topic_detail", {"topic": alert["topic"]}),
        ("get_kafka_broker_health", {}),
    ]


# Wave 3 evidence plans (2 per new connector, matching the alert fields).
def _plan_websphere_thread_saturated(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_websphere_threadpool", {"pool": alert["pool"]}),
        ("read_websphere_log", {"log": "systemout", "limit": 30}),
        ("get_websphere_apps", {}),
    ]


def _plan_websphere_app_stopped(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_websphere_apps", {}),
        ("read_websphere_log", {"log": "systemout", "limit": 30}),
    ]


def _plan_weblogic_heap_high(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_weblogic_heap", {}),
        ("read_weblogic_log", {"log": "server", "limit": 30}),
    ]


def _plan_weblogic_stuck_threads(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_weblogic_threadpool", {}),
        ("read_weblogic_log", {"log": "server", "limit": 30}),
    ]


def _plan_jboss_deployment_failed(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_jboss_deployments", {}),
        ("read_jboss_log", {"log": "server", "limit": 30}),
    ]


def _plan_jboss_heap_high(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_jboss_heap", {}),
        ("get_jboss_deployments", {}),
        ("read_jboss_log", {"log": "server", "limit": 30}),
    ]


def _plan_rabbitmq_queue_backlog(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_rabbitmq_queues", {"vhost": alert["vhost"]}),
        ("get_rabbitmq_connections", {}),
    ]


def _plan_rabbitmq_node_resource_alarm(alert: dict) -> list[tuple[str, dict]]:
    return [("get_rabbitmq_nodes", {})]


def _plan_artemis_queue_backlog(alert: dict) -> list[tuple[str, dict]]:
    return [("get_artemis_queues", {})]


def _plan_artemis_broker_down(alert: dict) -> list[tuple[str, dict]]:
    return [("get_artemis_broker", {})]


def _plan_ems_queue_backlog(alert: dict) -> list[tuple[str, dict]]:
    return [("get_ems_queues", {})]


def _plan_ems_connection_storm(alert: dict) -> list[tuple[str, dict]]:
    return [("get_ems_server", {})]


def _plan_nginx_upstream_errors(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("read_nginx_log", {"log": "error", "limit": 30}),
        ("get_nginx_status", {}),
    ]


def _plan_nginx_worker_crash(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("read_nginx_log", {"log": "error", "limit": 30}),
        ("get_nginx_status", {}),
    ]


def _plan_apache_workers_saturated(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_apache_status", {}),
        ("read_apache_log", {"log": "error", "limit": 30}),
    ]


def _plan_apache_5xx_spike(alert: dict) -> list[tuple[str, dict]]:
    return [("read_apache_log", {"log": "error", "limit": 30})]


def _plan_haproxy_backend_down(alert: dict) -> list[tuple[str, dict]]:
    return [("get_haproxy_stats", {})]


def _plan_haproxy_session_saturation(alert: dict) -> list[tuple[str, dict]]:
    return [("get_haproxy_stats", {})]


def _plan_postgres_blocking(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_postgres_blocking", {}),
        ("get_postgres_health", {}),
    ]


def _plan_postgres_replication_lag(alert: dict) -> list[tuple[str, dict]]:
    return [("get_postgres_replication", {})]


def _plan_mysql_runaway_query(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_mysql_processlist", {}),
        ("get_mysql_health", {}),
    ]


def _plan_mysql_replication_lag(alert: dict) -> list[tuple[str, dict]]:
    return [("get_mysql_replication", {})]


def _plan_oracle_tablespace_full(alert: dict) -> list[tuple[str, dict]]:
    return [("get_oracle_tablespaces", {})]


def _plan_oracle_blocking(alert: dict) -> list[tuple[str, dict]]:
    return [("get_oracle_blocking", {})]


def _plan_redis_memory_high(alert: dict) -> list[tuple[str, dict]]:
    return [("get_redis_info", {})]


def _plan_redis_replication_down(alert: dict) -> list[tuple[str, dict]]:
    return [("get_redis_replication", {})]


def _plan_elasticsearch_cluster_red(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_elasticsearch_cluster_health", {}),
        ("get_elasticsearch_indices", {}),
    ]


def _plan_elasticsearch_heap_pressure(alert: dict) -> list[tuple[str, dict]]:
    return [("get_elasticsearch_nodes", {})]


def _plan_mongo_long_running_op(alert: dict) -> list[tuple[str, dict]]:
    return [("get_mongo_current_ops", {})]


def _plan_mongo_replset_lag(alert: dict) -> list[tuple[str, dict]]:
    return [("get_mongo_replset", {})]


def _plan_k8s_pod_crashloop(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_k8s_pod_status", {"namespace": alert["namespace"]}),
        ("get_k8s_events", {"namespace": alert["namespace"], "limit": 25}),
        ("get_k8s_deployments", {"namespace": alert["namespace"]}),
    ]


def _plan_k8s_deployment_stalled(alert: dict) -> list[tuple[str, dict]]:
    return [("get_k8s_deployments", {"namespace": alert["namespace"]})]


def _plan_docker_container_exited(alert: dict) -> list[tuple[str, dict]]:
    return [
        ("get_docker_containers", {}),
        ("read_docker_logs", {"container": alert["container"], "limit": 30}),
    ]


def _plan_docker_memory_high(alert: dict) -> list[tuple[str, dict]]:
    return [("get_docker_stats", {})]


_ALERT_HANDLERS: dict[str, tuple] = {
    "queue_backlog": (
        _plan_queue_backlog,
        [
            _HypothesisSpec(
                "h1", "Receiver channel stopped",
                [_check_any_channel_stopped,
                 _check_error_log_mentions_stopped,
                 _check_deep_backlog],
                _remediation_restart_stopped,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Producer surge",
                [_check_all_channels_running, _check_deep_backlog,
                 _check_no_repeated_backouts]),
            _HypothesisSpec(
                "h3", "Downstream consumer stalled",
                [_check_downstream_lag_high]),
            _HypothesisSpec(
                "h4", "Config drift: MAXDEPTH lowered",
                [_check_depth_at_maxdepth, _check_maxdepth_below_baseline],
                None,
                _plan_for("h4")),
            _HypothesisSpec(
                "h5", "Poison message blocking queue",
                [_check_app_log_mentions_backout, _check_repeated_backouts,
                 _check_dlq_above_baseline, _check_all_channels_running],
                None,
                _plan_for("h5")),
        ],
        ("get_channel_status",),  # swallow ValueError: unknown channel
    ),
    "tomcat_errors": (
        _plan_tomcat_errors,
        [
            _HypothesisSpec(
                "h1", "Disk pressure on app host",
                [_check_disk_pressure, _check_compute_healthy],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Application-level fault (not visible via current toolset)",
                [_check_host_all_healthy]),
        ],
        (),
    ),
    "kafka_lag": (
        _plan_kafka_lag,
        [
            _HypothesisSpec(
                "h1", "Kafka consumer group stalled",
                [_check_topic_lag_high, _check_mq_side_healthy],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Upstream MQ backlog spilling over",
                [_check_topic_lag_high, _check_mq_side_degraded]),
        ],
        (),
    ),
    "app_oom": (
        _plan_app_oom,
        [
            _HypothesisSpec(
                "h1", "JVM heap exhaustion",
                [_check_app_log_mentions_oom, _check_mem_over_90,
                 _check_disk_under_90],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Memory pressure without OOM signature",
                [_check_mem_over_90]),
        ],
        (),
    ),
    "channel_tls_error": (
        _plan_channel_tls_error,
        [
            _HypothesisSpec(
                "h1", "Expired TLS certificate",
                [_check_cert_invalid, _check_error_log_mentions_cert,
                 _check_alert_channel_not_running],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Channel stopped (non-TLS cause)",
                [_check_alert_channel_stopped]),
        ],
        (),
    ),
    "listener_down": (
        _plan_listener_down,
        [
            _HypothesisSpec(
                "h1", "Listener stopped",
                [_check_listener_stopped, _check_error_log_mentions_listener],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Port conflict",
                [_check_listener_stopped,
                 _check_error_log_mentions_address_in_use]),
        ],
        (),
    ),
    "consumer_lag": (
        _plan_consumer_lag,
        [
            _HypothesisSpec(
                "h1", "Broker config drift slowing consumption",
                [_check_topic_lag_high, _check_broker_config_drift],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Consumer stalled with no config drift",
                [_check_topic_lag_high, _check_broker_configs_agree]),
        ],
        (),
    ),
    "threadpool_saturated": (
        _plan_threadpool_saturated,
        [
            _HypothesisSpec(
                "h1", "Thread pool exhausted by stuck requests",
                [_check_threadpool_saturated, _check_tomcat_log_threads_busy,
                 _check_tomcat_heap_healthy, _check_orders_app_running],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "JVM heap exhaustion",
                [_check_tomcat_heap_exhausted, _check_threadpool_not_saturated]),
        ],
        (),
    ),
    "kafka_under_replicated": (
        _plan_kafka_under_replicated,
        [
            _HypothesisSpec(
                "h1", "Partition under-replicated (ISR smaller than replicas)",
                [_check_isr_shrunk, _check_cluster_reachable]),
            _HypothesisSpec(
                "h2", "Cluster unreachable or degraded",
                [_check_cluster_unreachable]),
        ],
        (),
    ),
    # Wave 3 alert handlers (2 per new connector).
    "websphere_thread_saturated": (
        _plan_websphere_thread_saturated,
        [
            _HypothesisSpec(
                "h1", "WebContainer thread pool exhausted",
                [_check_websphere_pool_saturated,
                 _check_websphere_log_mentions_thread],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Application-level fault",
                [_check_websphere_app_stopped]),
        ],
        (),
    ),
    "websphere_app_stopped": (
        _plan_websphere_app_stopped,
        [
            _HypothesisSpec(
                "h1", "Application stopped",
                [_check_websphere_app_stopped],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Failed deployment",
                [_check_websphere_app_missing]),
        ],
        (),
    ),
    "weblogic_heap_high": (
        _plan_weblogic_heap_high,
        [
            _HypothesisSpec(
                "h1", "JVM heap exhaustion",
                [_check_weblogic_heap_over_90,
                 _check_weblogic_log_mentions_pressure],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Memory pressure without OOM signature",
                [_check_weblogic_heap_moderate]),
        ],
        (),
    ),
    "weblogic_stuck_threads": (
        _plan_weblogic_stuck_threads,
        [
            _HypothesisSpec(
                "h1", "Stuck threads in application code",
                [_check_weblogic_pool_fully_busy,
                 _check_weblogic_log_mentions_pressure]),
            _HypothesisSpec(
                "h2", "Transient spike",
                [_check_weblogic_pool_has_headroom]),
        ],
        (),
    ),
    "jboss_deployment_failed": (
        _plan_jboss_deployment_failed,
        [
            _HypothesisSpec(
                "h1", "Failed deployment needs redeploy",
                [_check_jboss_deployment_failed,
                 _check_jboss_log_mentions_failed],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Missing dependency",
                [_check_jboss_log_mentions_dependency]),
        ],
        (),
    ),
    "jboss_heap_high": (
        _plan_jboss_heap_high,
        [
            _HypothesisSpec(
                "h1", "JVM heap exhaustion",
                [_check_jboss_heap_over_90,
                 _check_jboss_deployments_healthy],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Memory pressure without OOM signature",
                [_check_jboss_heap_moderate]),
        ],
        (),
    ),
    "rabbitmq_queue_backlog": (
        _plan_rabbitmq_queue_backlog,
        [
            _HypothesisSpec(
                "h1", "Consumers gone",
                [_check_rmq_queue_deep, _check_rmq_queue_no_consumers]),
            _HypothesisSpec(
                "h2", "Producer surge",
                [_check_rmq_queue_deep, _check_rmq_queue_has_consumers]),
        ],
        (),
    ),
    "rabbitmq_node_resource_alarm": (
        _plan_rabbitmq_node_resource_alarm,
        [
            _HypothesisSpec(
                "h1", "Disk alarm blocking publishers",
                [_check_rmq_disk_free_low]),
            _HypothesisSpec(
                "h2", "Memory alarm",
                [_check_rmq_mem_high, _check_rmq_disk_free_healthy]),
        ],
        (),
    ),
    "artemis_queue_backlog": (
        _plan_artemis_queue_backlog,
        [
            _HypothesisSpec(
                "h1", "No consumers draining queue",
                [_check_artemis_queue_deep,
                 _check_artemis_queue_no_consumers]),
            _HypothesisSpec(
                "h2", "Producer surge",
                [_check_artemis_queue_deep,
                 _check_artemis_queue_has_consumers]),
        ],
        (),
    ),
    "artemis_broker_down": (
        _plan_artemis_broker_down,
        [
            _HypothesisSpec(
                "h1", "Broker down",
                [_check_artemis_broker_down]),
            _HypothesisSpec(
                "h2", "Broker degraded",
                [_check_artemis_broker_started]),
        ],
        (),
    ),
    "ems_queue_backlog": (
        _plan_ems_queue_backlog,
        [
            _HypothesisSpec(
                "h1", "Consumers stalled",
                [_check_ems_queue_deep, _check_ems_queue_no_consumers]),
            _HypothesisSpec(
                "h2", "Producer surge",
                [_check_ems_queue_deep, _check_ems_queue_has_consumers]),
        ],
        (),
    ),
    "ems_connection_storm": (
        _plan_ems_connection_storm,
        [
            _HypothesisSpec(
                "h1", "Connection leak in client app",
                [_check_ems_connection_storm]),
            _HypothesisSpec(
                "h2", "Legitimate traffic spike",
                [_check_ems_connections_normal]),
        ],
        (),
    ),
    "nginx_upstream_errors": (
        _plan_nginx_upstream_errors,
        [
            _HypothesisSpec(
                "h1", "Upstream application failing",
                [_check_nginx_log_mentions_upstream, _check_nginx_serving]),
            _HypothesisSpec(
                "h2", "Upstream timeouts",
                [_check_nginx_log_mentions_timeout]),
        ],
        (),
    ),
    "nginx_worker_crash": (
        _plan_nginx_worker_crash,
        [
            _HypothesisSpec(
                "h1", "Worker crash",
                [_check_nginx_log_mentions_failed, _check_nginx_serving],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Overload",
                [_check_nginx_waiting_high]),
        ],
        (),
    ),
    "apache_workers_saturated": (
        _plan_apache_workers_saturated,
        [
            _HypothesisSpec(
                "h1", "Workers stuck on slow requests",
                [_check_apache_workers_saturated,
                 _check_apache_log_mentions_worker],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Legitimate traffic spike",
                [_check_apache_workers_saturated,
                 _check_apache_workers_available]),
        ],
        (),
    ),
    "apache_5xx_spike": (
        _plan_apache_5xx_spike,
        [
            _HypothesisSpec(
                "h1", "Backend application fault",
                [_check_apache_log_mentions_failed]),
            _HypothesisSpec(
                "h2", "Config error",
                [_check_apache_log_mentions_config]),
        ],
        (),
    ),
    "haproxy_backend_down": (
        _plan_haproxy_backend_down,
        [
            _HypothesisSpec(
                "h1", "Backend failed health check",
                [_check_haproxy_server_check_failing],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Flapping health check",
                [_check_haproxy_server_up]),
        ],
        (),
    ),
    "haproxy_session_saturation": (
        _plan_haproxy_session_saturation,
        [
            _HypothesisSpec(
                "h1", "Frontend session table full",
                [_check_haproxy_frontend_saturated]),
            _HypothesisSpec(
                "h2", "Traffic spike",
                [_check_haproxy_frontend_normal]),
        ],
        (),
    ),
    "postgres_blocking": (
        _plan_postgres_blocking,
        [
            _HypothesisSpec(
                "h1", "Long-running blocker",
                [_check_pg_blocker_present, _check_pg_blocker_long_running],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Lock storm",
                [_check_pg_multiple_blockers]),
        ],
        (),
    ),
    "postgres_replication_lag": (
        _plan_postgres_replication_lag,
        [
            _HypothesisSpec(
                "h1", "Standby lagging",
                [_check_pg_replication_lag_high]),
            _HypothesisSpec(
                "h2", "WAL receiver stalled",
                [_check_pg_replication_current]),
        ],
        (),
    ),
    "mysql_runaway_query": (
        _plan_mysql_runaway_query,
        [
            _HypothesisSpec(
                "h1", "Runaway query",
                [_check_mysql_process_present, _check_mysql_process_long],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Lock wait",
                [_check_mysql_process_lock_wait]),
        ],
        (),
    ),
    "mysql_replication_lag": (
        _plan_mysql_replication_lag,
        [
            _HypothesisSpec(
                "h1", "Replica lagging",
                [_check_mysql_replication_lag_high]),
            _HypothesisSpec(
                "h2", "Replication stopped",
                [_check_mysql_replication_stopped]),
        ],
        (),
    ),
    "oracle_tablespace_full": (
        _plan_oracle_tablespace_full,
        [
            _HypothesisSpec(
                "h1", "Tablespace full",
                [_check_oracle_tablespace_full]),
            _HypothesisSpec(
                "h2", "Runaway segment growth",
                [_check_oracle_tablespace_healthy]),
        ],
        (),
    ),
    "oracle_blocking": (
        _plan_oracle_blocking,
        [
            _HypothesisSpec(
                "h1", "Blocking session",
                [_check_oracle_blocker_present],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Enqueue storm",
                [_check_oracle_many_blockers]),
        ],
        (),
    ),
    "redis_memory_high": (
        _plan_redis_memory_high,
        [
            _HypothesisSpec(
                "h1", "Memory pressure near maxmemory",
                [_check_redis_mem_high]),
            _HypothesisSpec(
                "h2", "Big-key growth",
                [_check_redis_mem_normal]),
        ],
        (),
    ),
    "redis_replication_down": (
        _plan_redis_replication_down,
        [
            _HypothesisSpec(
                "h1", "Replica disconnected",
                [_check_redis_master_link_down]),
            _HypothesisSpec(
                "h2", "Failover in progress",
                [_check_redis_not_master]),
        ],
        (),
    ),
    "elasticsearch_cluster_red": (
        _plan_elasticsearch_cluster_red,
        [
            _HypothesisSpec(
                "h1", "Unassigned shards",
                [_check_es_red, _check_es_unassigned_shards]),
            _HypothesisSpec(
                "h2", "Node lost",
                [_check_es_node_lost]),
        ],
        (),
    ),
    "elasticsearch_heap_pressure": (
        _plan_elasticsearch_heap_pressure,
        [
            _HypothesisSpec(
                "h1", "Node heap pressure",
                [_check_es_node_heap_high]),
            _HypothesisSpec(
                "h2", "Heavy aggregations",
                [_check_es_nodes_heap_healthy]),
        ],
        (),
    ),
    "mongo_long_running_op": (
        _plan_mongo_long_running_op,
        [
            _HypothesisSpec(
                "h1", "Runaway operation",
                [_check_mongo_op_present, _check_mongo_op_long],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Lock contention",
                [_check_mongo_op_lock]),
        ],
        (),
    ),
    "mongo_replset_lag": (
        _plan_mongo_replset_lag,
        [
            _HypothesisSpec(
                "h1", "Secondary lagging",
                [_check_mongo_member_lagging]),
            _HypothesisSpec(
                "h2", "Secondary down",
                [_check_mongo_member_down]),
        ],
        (),
    ),
    "k8s_pod_crashloop": (
        _plan_k8s_pod_crashloop,
        [
            _HypothesisSpec(
                "h1", "CrashLoopBackOff after OOMKill",
                [_check_k8s_pod_crashloop, _check_k8s_events_oomkilled],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "Bad image",
                [_check_k8s_events_imagepull]),
        ],
        (),
    ),
    "k8s_deployment_stalled": (
        _plan_k8s_deployment_stalled,
        [
            _HypothesisSpec(
                "h1", "Rollout stuck",
                [_check_k8s_deployment_unavailable]),
            _HypothesisSpec(
                "h2", "Node pressure",
                [_check_k8s_deployments_healthy]),
        ],
        (),
    ),
    "docker_container_exited": (
        _plan_docker_container_exited,
        [
            _HypothesisSpec(
                "h1", "Container crashed",
                [_check_docker_container_exited,
                 _check_docker_log_mentions_error],
                None,
                _plan_for("h1")),
            _HypothesisSpec(
                "h2", "OOMKilled",
                [_check_docker_log_mentions_oomkilled]),
        ],
        (),
    ),
    "docker_memory_high": (
        _plan_docker_memory_high,
        [
            _HypothesisSpec(
                "h1", "Memory leak in container",
                [_check_docker_mem_high]),
            _HypothesisSpec(
                "h2", "Undersized limit",
                [_check_docker_mem_normal]),
        ],
        (),
    ),
}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class RCAEngine:
    """Deterministic RCA engine over a read-only tool client.

    Construct with a read-only client (call_tool) and an audit log
    (append). See this module's docstring for the two hard invariants.
    """

    def __init__(self, client, audit):
        self._client = client
        self._audit = audit
        self._evidence: dict[str, Any] = {}
        self._evidence_calls: list[dict] = []
        self._last_alert: dict | None = None

    def diagnose(self, alert: dict) -> Diagnosis:
        """Run the evidence plan, score hypotheses, audit, return Diagnosis."""
        self._evidence = {}
        self._evidence_calls = []
        self._last_alert = alert
        alert_type = alert.get("type")
        handler = _ALERT_HANDLERS.get(alert_type)
        if handler is None:
            raise ValueError(f"unknown alert type: {alert_type!r}")
        build_plan, specs, swallow = handler

        self._run_plan(build_plan(alert), alert_type, swallow)

        hypotheses = [self._score(spec, alert) for spec in specs]
        hypotheses.sort(key=lambda h: h.score, reverse=True)
        top = hypotheses[0]

        self._audit.append("diagnosis_complete", "rca-engine",
                           {"alert_type": alert_type,
                            "top_hypothesis": top.id,
                            "score": top.score,
                            # Additive: makes the audit self-contained for the
                            # incident-report generator.
                            "top_title": top.title,
                            "evidence": [{"claim": c.claim, "tool": c.tool,
                                          "excerpt": c.excerpt}
                                         for c in top.evidence]})
        return Diagnosis(alert=alert, hypotheses=hypotheses, top=top,
                         evidence_calls=self._evidence_calls)

    def propose_plan(self, hypothesis: Hypothesis) -> RemediationPlan | None:
        """Return the remediation plan for a hypothesis, or None.

        Prefers hypothesis.plan (attached by the scorer during diagnose());
        otherwise rebuilds one via agent.remediation.plan_for with the alert
        and evidence from the last diagnose() call. Total: returns None for
        unknown hypothesis ids instead of raising.
        """
        if hypothesis.plan is not None:
            return hypothesis.plan
        if self._last_alert is None:
            return None
        return plan_for(hypothesis.id, self._last_alert, self._evidence)

    def _run_plan(self, plan: list[tuple[str, dict]], alert_type: str,
                  swallow_value_error: tuple[str, ...]) -> None:
        """Execute each plan step via the read-only client (degraded mode)."""
        for name, args in plan:
            self._evidence_calls.append({"tool": name, "args": args})
            try:
                output = self._client.call_tool(name, args)
            except PermissionError:
                self._audit.append("evidence_denied", "rca-engine",
                                   {"alert_type": alert_type, "tool": name})
                continue
            except ValueError:
                if name in swallow_value_error:
                    continue  # e.g. unknown channel in candidate list
                raise
            self._evidence[_ev_key(name, args)] = output

    def _score(self, spec: _HypothesisSpec, alert: dict) -> Hypothesis:
        matched = 0
        evidence: list[Citation] = []
        for check in spec.checks:
            ok, citation = check(alert, self._evidence)
            if ok:
                matched += 1
                if citation is not None:
                    evidence.append(citation)
        score = matched / len(spec.checks) if spec.checks else 0.0
        remediation = (spec.remediation_fn(alert, self._evidence)
                       if spec.remediation_fn else None)
        plan = (spec.plan_fn(alert, self._evidence)
                if spec.plan_fn else None)
        return Hypothesis(id=spec.id, title=spec.title, score=score,
                          evidence=evidence, remediation=remediation,
                          plan=plan)


# ---------------------------------------------------------------------------
# Reasoning layer: see agent/jev.py
# ---------------------------------------------------------------------------
# The old unwired LLMHook stub lived here. It was replaced by the Jev
# reasoning layer (TypeSafe System One, agent/jev.py), which is ON BY
# DEFAULT when TYPESAFE_API_KEY is configured: typed hypothesis ranking,
# per-claim evidence verification, triage priority, and remediation risk
# scoring, rendered as first-class UI and report elements. The deterministic
# engine stays the decider and the fallback; Jev advises only and can never
# produce or execute privileged calls.
