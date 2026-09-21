"""Jev reasoning layer (TypeSafe System One) — the default reasoning experience.

Product contract (user direction, 2026-09-21):

* Jev is ON BY DEFAULT when a key is configured. Users SEE its output:
  hypothesis confidence, per-claim evidence support, triage priority, and
  remediation risk are first-class elements of the UI and the incident
  report — never buried in a log.
* The deterministic engine is the FALLBACK ONLY when no key is configured,
  clearly labeled "deterministic mode" in the UI.
* Jev ADVISES only. It never overrides human approval, default-deny, the
  audit trail, or verification after mutation. Jev output can never produce
  or execute a privileged call.
* The key comes from the environment (``TYPESAFE_API_KEY``) or an
  equivalent runtime config — never source code, never a committed file.
  It is never logged, never audited, never returned in any response.
* Unit tests use :class:`StubJevClient`; :class:`DemoJevClient` produces
  clearly-labeled illustrative values for the hosted demo artifact, which
  has no key.

One batched request carries all independent judgments for a diagnosis
(narrow typed question per judgment, per the TypeSafe skill playbook). A
second batched request re-asks ONLY the final root-cause verdict with
2-3 paraphrasings and treats disagreement as uncertainty (GAP 2
self-consistency); every other Jev question stays on the fast
single-shot path.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Protocol

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

JEV_API_KEY_ENV = "TYPESAFE_API_KEY"
JEV_ENABLED_ENV = "RCA_JEV_ENABLED"      # "0" disables Jev; default on
JEV_DEMO_ENV = "RCA_JEV_DEMO"            # "1" -> labeled illustrative values
JEV_API_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-latest"
JEV_TIMEOUT_S = 20

_MAX_HYPOTHESES = 5
_MAX_CLAIMS = 4          # per-claim verification, top hypothesis only
_MAX_EXCERPT = 160

_DETERMINISTIC_MESSAGE = (
    "deterministic mode — set TYPESAFE_API_KEY for Jev confidence scoring")


class JevError(Exception):
    """Raised when the Jev service cannot be reached or answers badly."""


# ---------------------------------------------------------------------------
# Verdict types (what the UI and reports render)
# ---------------------------------------------------------------------------

@dataclass
class HypothesisConfidence:
    """Jev's calibrated take on one hypothesis."""

    hypothesis_id: str
    title: str
    jev_probability: float        # P(this hypothesis is the root cause)
    confidence: float              # distribution concentration, 0..1
    agrees_with_deterministic: bool
    self_consistency: str = "n/a"  # "consistent" | "uncertain" | "n/a"
    # "uncertain" means the GAP-2 3x paraphrased re-ask of this being the
    # root cause genuinely disagreed with (or could not confirm) the
    # single-shot verdict; confidence was then capped at the 0.5
    # genuine-uncertainty boundary. "n/a" means the check did not run or
    # the client never answered it — absence of a second opinion is not
    # a disagreement.


@dataclass
class ClaimSupport:
    """Jev's per-claim evidence verification."""

    claim: str
    verdict: str                   # "supported" | "partially_supported" | "unsupported"
    confidence: float              # distance from 0.5 uncertainty, 0..1
    probability: float             # P(claim is supported by the excerpt)


@dataclass
class TriageVerdict:
    """Jev's incident triage."""

    priority: str                  # "P1" | "P2" | "P3" | "P4"
    urgency_probability: float
    confidence: float


@dataclass
class RemediationRiskVerdict:
    """Jev's risk class for the proposed remediation plan."""

    risk: str                      # "low" | "medium" | "high"
    risk_score: float              # 0..2 along the rubric
    confidence: float


@dataclass
class JevAdvisory:
    """Everything Jev said about one diagnosis. Advisory only."""

    available: bool
    mode: str                      # "jev" | "deterministic"
    illustrative: bool = False     # True only for DemoJevClient values
    hypothesis_ranking: list[HypothesisConfidence] = field(default_factory=list)
    claim_support: list[ClaimSupport] = field(default_factory=list)
    triage: TriageVerdict | None = None
    remediation_risk: RemediationRiskVerdict | None = None
    agreement: str = "n/a"         # "agree" | "disagree" | "n/a"
    latency_ms: int = 0
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "available": self.available,
            "mode": self.mode,
            "illustrative": self.illustrative,
            "agreement": self.agreement,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "hypothesis_ranking": [
                {"hypothesis_id": r.hypothesis_id, "title": r.title,
                 "jev_probability": round(r.jev_probability, 3),
                 "confidence": round(r.confidence, 3),
                 "agrees_with_deterministic": r.agrees_with_deterministic,
                 "self_consistency": r.self_consistency}
                for r in self.hypothesis_ranking
            ],
            "claim_support": [
                {"claim": c.claim, "verdict": c.verdict,
                 "confidence": round(c.confidence, 3),
                 "probability": round(c.probability, 3)}
                for c in self.claim_support
            ],
            "triage": (None if self.triage is None else {
                "priority": self.triage.priority,
                "urgency_probability": round(self.triage.urgency_probability, 3),
                "confidence": round(self.triage.confidence, 3)}),
            "remediation_risk": (None if self.remediation_risk is None else {
                "risk": self.remediation_risk.risk,
                "risk_score": round(self.remediation_risk.risk_score, 3),
                "confidence": round(self.remediation_risk.confidence, 3)}),
        }


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

class JevClient(Protocol):
    """Answers a batch of typed questions about a state."""

    def ask(self, state: dict, questions: dict) -> dict:
        """Return the raw `answers` map from the System One endpoint."""
        ...  # pragma: no cover


class HttpJevClient:
    """Live TypeSafe System One client. The key is held in memory only."""

    def __init__(self, api_key: str,
                 api_url: str = JEV_API_URL,
                 timeout_s: int = JEV_TIMEOUT_S) -> None:
        if not api_key:
            raise JevError("empty TypeSafe API key")
        self._api_key = api_key
        self._api_url = api_url
        self._timeout_s = timeout_s

    def ask(self, state: dict, questions: dict) -> dict:
        body = json.dumps(
            {"state": state, "model": JEV_MODEL,
             "questions": questions}).encode("utf-8")
        req = urllib.request.Request(
            self._api_url, data=body,
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + self._api_key},
            method="POST")
        try:
            with urllib.request.urlopen(req,
                                        timeout=self._timeout_s) as resp:
                payload = json.loads(
                    resp.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            # Read a bounded error body; never include the key.
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise JevError(f"jev http {exc.code}: {detail}") from exc
        except TimeoutError as exc:
            raise JevError("jev request timed out") from exc
        except OSError as exc:
            raise JevError(f"jev request failed: {exc}") from exc
        answers = payload.get("answers")
        if not isinstance(answers, dict):
            raise JevError("jev response had no answers map")
        return answers


class StubJevClient:
    """Scripted answers for unit tests. Never touches the network."""

    def __init__(self, answers: dict) -> None:
        self._answers = answers
        self.seen: list[tuple[dict, dict]] = []

    def ask(self, state: dict, questions: dict) -> dict:
        self.seen.append((state, questions))
        out: dict = {}
        for qid in questions:
            if qid in self._answers:
                out[qid] = self._answers[qid]
        return out


class DemoJevClient:
    """Deterministic, clearly-labeled illustrative values.

    Used ONLY for the hosted demo artifact, which has no API key.
    Every verdict is marked illustrative so it can never be mistaken
    for live Jev output.
    """

    def ask(self, state: dict, questions: dict) -> dict:
        hyps = state.get("hypotheses", [])
        top_id = hyps[0]["id"] if hyps else "h1"
        others = [h["id"] for h in hyps[1:]]
        probs = {top_id: 0.72}
        rest = 0.28 / max(len(others), 1)
        for oid in others:
            probs[oid] = round(rest, 3)
        answers: dict = {}
        if "rank_hypotheses" in questions:
            answers["rank_hypotheses"] = {
                "type": "choice", "choice": top_id,
                "probabilities": probs, "confidence": 0.72}
        for qid in questions:
            if qid.startswith("claim_supported_"):
                answers[qid] = {"type": "noul", "noul": 0.86}
        if "triage_priority" in questions:
            answers["triage_priority"] = {
                "type": "choice", "choice": "P2",
                "probabilities": {"P1": 0.08, "P2": 0.72,
                                  "P3": 0.15, "P4": 0.05},
                "confidence": 0.72}
        if "remediation_risk" in questions:
            answers["remediation_risk"] = {
                "type": "score", "score": 0.8, "confidence": 0.7,
                "legend": {"0": "Low", "1": "Medium", "2": "High"},
                "probabilities": {"0": 0.2, "1": 0.8, "2": 0.0}}
        return answers


# ---------------------------------------------------------------------------
# Reasoner
# ---------------------------------------------------------------------------

_TRIAGE_CRITERIA = {
    "P1": "Production traffic stopped or data loss in progress; page now",
    "P2": "Degraded service with a clear workaround; fix within hours",
    "P3": "Localized issue, no customer impact; fix within days",
    "P4": "Cosmetic or informational; backlog",
}

_RISK_CRITERIA = [
    "Low risk: read-only verification or fully reversible change with a "
    "verified rollback, executed with per-step approval",
    "Medium risk: state-changing remediation where rollback is manual or "
    "partially verified; still gated by human approval",
    "High risk: destructive or irreversible action, or remediation whose "
    "verification cannot confirm the outcome",
]


class JevReasoner:
    """Builds the Jev advisory for a diagnosis. Advisory only — the
    deterministic engine stays the decider and the fallback."""

    def __init__(self, client: JevClient | None,
                 audit=None,
                 enabled: bool = True,
                 illustrative: bool = False) -> None:
        self._client = client
        self._audit = audit
        self._enabled = enabled and client is not None
        self._illustrative = illustrative

    # -- configuration -------------------------------------------------

    @classmethod
    def from_env(cls, audit=None) -> "JevReasoner":
        """Build from the environment.

        ``TYPESAFE_API_KEY`` set (and ``RCA_JEV_ENABLED`` not "0") ->
        live Jev client. ``RCA_JEV_DEMO=1`` -> illustrative demo client.
        Otherwise -> no client; deterministic fallback.
        """
        illustrative = os.environ.get(JEV_DEMO_ENV, "") == "1"
        enabled = os.environ.get(JEV_ENABLED_ENV, "1") != "0"
        key = os.environ.get(JEV_API_KEY_ENV, "")
        if illustrative:
            return cls(DemoJevClient(), audit=audit,
                       enabled=True, illustrative=True)
        if enabled and key:
            return cls(HttpJevClient(key), audit=audit, enabled=True)
        return cls(None, audit=audit, enabled=False)

    def status(self) -> dict:
        """Jev status for the UI banner and /api/jev/status."""
        if self._illustrative:
            return {"mode": "jev", "enabled": True, "configured": False,
                    "illustrative": True,
                    "message": ("demo confidence values — connect a "
                                "TypeSafe key for live Jev scoring")}
        if self._enabled:
            return {"mode": "jev", "enabled": True, "configured": True,
                    "illustrative": False,
                    "message": "Jev confidence scoring active"}
        return {"mode": "deterministic", "enabled": False,
                "configured": False, "illustrative": False,
                "message": _DETERMINISTIC_MESSAGE}

    @property
    def enabled(self) -> bool:
        return self._enabled

    # -- advisory ------------------------------------------------------

    def advise(self, diagnosis, plan=None) -> JevAdvisory:
        """Return the Jev advisory for a diagnosis.

        Never raises for service problems: on any failure the advisory
        carries available=False and the caller keeps the deterministic
        diagnosis. Jev output never triggers privileged calls.
        """
        mode = "jev" if self._enabled else "deterministic"
        if not self._enabled or self._client is None:
            return JevAdvisory(
                available=False, mode="deterministic",
                illustrative=self._illustrative,
                error="jev unavailable: " + _DETERMINISTIC_MESSAGE)
        state = _build_state(diagnosis, plan)
        questions = _build_questions(diagnosis, include_risk=(plan is not None))
        started = time.monotonic()
        sc_result = None
        try:
            answers = self._client.ask(state, questions)
            advisory = _interpret(diagnosis, answers,
                                  illustrative=self._illustrative)
            advisory.latency_ms = int((time.monotonic() - started) * 1000)
            advisory.mode = mode
            # GAP 2: the FINAL root-cause verdict — the top hypothesis
            # probability the UI/report renders — goes through the 3x
            # paraphrased self-consistency check. Genuine disagreement
            # surfaces as uncertainty instead of a false-confident
            # single-shot number. Never raises; the single-shot verdict
            # stands when the client never answers the check.
            sc_result = self._check_verdict_consistency(state, advisory)
        except Exception as exc:  # service faults degrade, never crash
            advisory = JevAdvisory(
                available=False, mode=mode,
                illustrative=self._illustrative,
                error=f"jev unavailable ({type(exc).__name__}): {exc}")
        self._record_audit(diagnosis, state, questions, advisory,
                           sc_result=sc_result)
        return advisory

    def advise_remediation_risk(self, diagnosis, plan):
        """Score the risk of executing the PROPOSED plan.

        Called once a plan exists — risk scored at diagnose time would be a
        guess, because there are no steps to judge yet. Returns a
        RemediationRiskVerdict, or None when Jev is unavailable. Never
        raises for service problems; advisory only, never an approval.
        """
        if not self._enabled or self._client is None:
            return None
        state = _build_state(diagnosis, plan)
        questions = {"remediation_risk": _remediation_risk_question()}
        started = time.monotonic()
        try:
            answers = self._client.ask(state, questions)
        except Exception:
            return None
        verdict = _interpret_risk(answers)
        self._record_audit(
            diagnosis, state, questions,
            JevAdvisory(available=verdict is not None, mode="jev",
                        illustrative=self._illustrative,
                        remediation_risk=verdict,
                        latency_ms=int((time.monotonic() - started) * 1000)),
            note="remediation-risk-only")
        return verdict

    def _check_verdict_consistency(self, state: dict,
                                   advisory: JevAdvisory) -> dict | None:
        """3x paraphrased self-consistency check on the final verdict.

        Never raises. If the paraphrased re-asks genuinely disagree with
        (or cannot confirm) the single-shot root-cause verdict, the top
        hypothesis's confidence is capped at the genuine-uncertainty
        boundary (0.5) and marked "uncertain" so the UI/report never
        present a false-confident probability. When the client never
        answers the check (stub/demo clients, partial failure), the
        single-shot verdict stands, marked "n/a" — absence of a second
        opinion is not a disagreement. Returns the raw check result
        (for the audit trail), or None when there is no verdict to check.
        """
        if not advisory.hypothesis_ranking:
            return None
        top = advisory.hypothesis_ranking[0]
        claim = (f"Hypothesis '{top.title}' ({top.hypothesis_id}) is the "
                 f"most likely root cause of this incident "
                 f"(Jev probability {top.jev_probability:.2f}).")
        try:
            result = self_consistent_noul(self._client, state, claim)
        except Exception:
            # Belt and braces: self_consistent_noul is non-raising by
            # contract, so this is unreachable in practice.
            return None
        if result["verdict"] == "yes":
            top.self_consistency = "consistent"
        elif "error" in result:
            top.self_consistency = "n/a"
        else:
            # "no" or "uncertain": the re-asks disagree or cannot confirm.
            top.self_consistency = "uncertain"
            top.confidence = min(top.confidence, 0.5)
        return result

    # -- audit ----------------------------------------------------------

    def _record_audit(self, diagnosis, state: dict,
                      questions: dict, advisory: JevAdvisory,
                      note: str | None = None,
                      sc_result: dict | None = None) -> None:
        if self._audit is None:
            return
        digest = hashlib.sha256(
            json.dumps(questions, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        details = {
            "mode": advisory.mode,
            "illustrative": advisory.illustrative,
            "available": advisory.available,
            "questions_sha": digest,
            "question_kinds": sorted({q["type"] for q in questions.values()}),
            "latency_ms": advisory.latency_ms,
            "agreement": advisory.agreement,
        }
        if sc_result is not None:
            # Bounded scalars only: verdict, spread, and the three
            # already-rounded probabilities.
            details["self_consistency"] = {
                "verdict": sc_result.get("verdict"),
                "spread": sc_result.get("spread"),
                "scores": sc_result.get("scores", [])[:3],
            }
        if note:
            details["note"] = note
        if advisory.error:
            details["error"] = advisory.error[:200]
        if advisory.available:
            details["triage"] = (advisory.triage.priority
                                 if advisory.triage else None)
            details["risk"] = (advisory.remediation_risk.risk
                               if advisory.remediation_risk else None)
            details["top_confidence"] = (
                round(advisory.hypothesis_ranking[0].confidence, 3)
                if advisory.hypothesis_ranking else None)
            # Full advisory: bounded scalars and short claim strings only —
            # safe for the audit trail and lets the incident report render
            # exactly what Jev said. Never the API key.
            details["advisory"] = advisory.to_dict()
        # Never the API key, never raw request bodies — hashes and bounded
        # scalars only.
        self._audit.append("jev_advisory", "jev-reasoner", details)


# ---------------------------------------------------------------------------
# Question building (one batched call, independent narrow questions)
# ---------------------------------------------------------------------------

def _build_state(diagnosis, plan=None) -> dict:
    hyps = []
    for h in diagnosis.hypotheses[:_MAX_HYPOTHESES]:
        hyps.append({
            "id": h.id,
            "title": h.title,
            "deterministic_score": round(h.score, 3),
            "evidence": [
                {"claim": c.claim[:200], "tool": c.tool,
                 "excerpt": c.excerpt[:_MAX_EXCERPT]}
                for c in h.evidence[:_MAX_CLAIMS]
            ],
        })
    alert = diagnosis.alert if isinstance(diagnosis.alert, dict) else {}
    state: dict[str, Any] = {
        "alert": {k: alert.get(k) for k in
                  ("title", "severity", "scenario", "type", "qmgr",
                   "queue", "topic", "host") if alert.get(k) is not None},
        "hypotheses": hyps,
    }
    if plan is not None:
        steps = getattr(plan, "steps", []) or []
        state["plan"] = {
            "title": getattr(plan, "title", ""),
            "steps": [{"id": s.id, "action": s.action,
                       "verify": getattr(s, "verify", "")}
                      for s in steps],
        }
    return state


def _build_questions(diagnosis, include_risk: bool = False) -> dict:
    """Build the typed Jev questions for a diagnosis.

    The remediation-risk question is only included when a real plan exists
    (``include_risk=True``): scoring risk without the proposed steps would
    be a guess, not a judgment.
    """
    hyps = diagnosis.hypotheses[:_MAX_HYPOTHESES]
    criteria = {}
    for h in hyps:
        claims = "; ".join(c.claim for c in h.evidence[:_MAX_CLAIMS]) or "no evidence"
        criteria[h.id] = (
            f"{h.title}. Deterministic match score {round(h.score, 2)}. "
            f"Evidence: {claims}")
    questions: dict[str, dict] = {
        "rank_hypotheses": {
            "type": "choice",
            "instructions": ("Given the alert and the cited tool evidence, "
                             "which hypothesis is the most likely root cause "
                             "of this incident?"),
            "criteria": criteria,
        },
        "triage_priority": {
            "type": "choice",
            "instructions": ("What priority class should this incident be "
                             "triaged at, based on its described impact?"),
            "criteria": _TRIAGE_CRITERIA,
        },
    }
    if include_risk:
        questions["remediation_risk"] = _remediation_risk_question()
    # Per-claim evidence verification for the top hypothesis only.
    top = diagnosis.top
    if top is not None:
        for i, c in enumerate(top.evidence[:_MAX_CLAIMS]):
            questions[f"claim_supported_{i}"] = {
                "type": "noul",
                "instructions": {
                    "question": ("Is the claim directly supported by the "
                                 "cited tool output excerpt?"),
                    "claim": c.claim[:200],
                    "tool": c.tool,
                    "excerpt": c.excerpt[:_MAX_EXCERPT],
                },
                "criteria": {
                    "true": "the excerpt directly states or shows the claim",
                    "false": ("the excerpt does not state the claim, or "
                              "contradicts it"),
                },
            }
    return questions


# ---------------------------------------------------------------------------
# Interpretation
# ---------------------------------------------------------------------------

def _remediation_risk_question() -> dict:
    return {
        "type": "score",
        "instructions": ("Rate the risk of executing the proposed "
                         "remediation plan as described."),
        "criteria": _RISK_CRITERIA,
    }


def _interpret_risk(answers: dict) -> RemediationRiskVerdict | None:
    risk = answers.get("remediation_risk", {})
    if "score" not in risk:
        return None
    score = max(0.0, min(2.0, float(risk["score"])))
    label = "low" if score < 0.67 else "medium" if score < 1.34 else "high"
    return RemediationRiskVerdict(
        risk=label, risk_score=round(score, 3),
        confidence=_as_float(risk.get("confidence", 0.0)))

def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, out))


def _claim_verdict(probability: float) -> tuple[str, float]:
    # ~0.5 means genuinely uncertain, not medium confidence (skill playbook).
    if probability >= 0.70:
        return "supported", probability
    if probability >= 0.35:
        return "partially_supported", abs(probability - 0.5) * 2
    return "unsupported", 1.0 - probability


def _interpret(diagnosis, answers: dict,
               illustrative: bool = False) -> JevAdvisory:
    advisory = JevAdvisory(available=True, mode="jev",
                           illustrative=illustrative)

    rank = answers.get("rank_hypotheses", {})
    probs = rank.get("probabilities", {}) or {}
    choice_conf = _as_float(rank.get("confidence", 0.0))
    top_id = diagnosis.top.id if diagnosis.top is not None else None
    ranking = []
    for h in diagnosis.hypotheses[:_MAX_HYPOTHESES]:
        p = _as_float(probs.get(h.id, 0.0))
        ranking.append(HypothesisConfidence(
            hypothesis_id=h.id, title=h.title,
            jev_probability=p, confidence=choice_conf,
            agrees_with_deterministic=(h.id == top_id)))
    # Any option the model invented that is not ours is ignored.
    ranking.sort(key=lambda r: r.jev_probability, reverse=True)
    advisory.hypothesis_ranking = ranking
    if rank.get("choice") and top_id:
        advisory.agreement = ("agree" if rank["choice"] == top_id
                              else "disagree")

    if diagnosis.top is not None:
        for i, c in enumerate(diagnosis.top.evidence[:_MAX_CLAIMS]):
            ans = answers.get(f"claim_supported_{i}", {})
            p = _as_float(ans.get("noul", 0.5))
            verdict, conf = _claim_verdict(p)
            advisory.claim_support.append(ClaimSupport(
                claim=c.claim, verdict=verdict,
                confidence=round(conf, 3), probability=p))

    tri = answers.get("triage_priority", {})
    if tri.get("choice") in _TRIAGE_CRITERIA:
        advisory.triage = TriageVerdict(
            priority=tri["choice"],
            urgency_probability=_as_float(
                (tri.get("probabilities") or {}).get(tri["choice"], 0.0)),
            confidence=_as_float(tri.get("confidence", 0.0)))

    risk = _interpret_risk(answers)
    if risk is not None:
        advisory.remediation_risk = risk

    return advisory


# ---------------------------------------------------------------------------
# Self-consistency on the final root-cause verdict (GAP 2)
# ---------------------------------------------------------------------------
#
# TypeSafe's docs recommend asking high-stakes questions with 2-3
# paraphrasings in one batched request and treating disagreement as
# uncertainty. Only the FINAL root-cause verdict gets this 3x treatment:
# every other Jev question stays on the fast single-shot path.

def self_consistent_noul(client: JevClient, state: dict,
                         instructions: str,
                         paraphrases: list[str] | None = None,
                         question_key: str = "sc") -> dict:
    """Ask one high-stakes claim three ways in ONE batched request.

    Builds three Noul questions (keys f"{question_key}_p1..p3") with
    paraphrased instructions — by default [instructions, "Judge whether
    the following claim is true: {instructions}", "Is the following
    statement accurate? {instructions}"] — and sends them in a single
    client.ask(state, questions) call.

    Returns {"verdict": "yes" | "no" | "uncertain", "scores": [...],
    "spread": float}. The verdict is "yes" iff all three probabilities
    are above 0.5 with spread < 0.25, "no" iff all three are below 0.5
    with spread < 0.25, and "uncertain" otherwise.

    On client error or missing answers the verdict is "uncertain" with
    empty scores, spread 1.0 and an "error" key — this helper never
    raises into the diagnosis flow.
    """
    if paraphrases is None:
        paraphrases = [
            instructions,
            f"Judge whether the following claim is true: {instructions}",
            f"Is the following statement accurate? {instructions}",
        ]
    questions = {
        f"{question_key}_p{i}": {
            "type": "noul",
            "instructions": para,
            "criteria": {
                "true": "the claim is accurate given the incident state",
                "false": "the claim is inaccurate or unsupported",
            },
        }
        for i, para in enumerate(paraphrases, 1)
    }
    try:
        answers = client.ask(state, questions)
    except Exception as exc:  # never raise into the diagnosis flow
        return {"verdict": "uncertain", "scores": [], "spread": 1.0,
                "error": f"{type(exc).__name__}: {exc}"}
    if not isinstance(answers, dict):
        return {"verdict": "uncertain", "scores": [], "spread": 1.0,
                "error": "jev answers map was not a dict"}
    scores = []
    for i in range(1, len(paraphrases) + 1):
        qid = f"{question_key}_p{i}"
        ans = answers.get(qid)
        # Tolerate the same answer shapes the per-claim verifier
        # handles: a Noul answer carrying a numeric "noul" probability.
        if not isinstance(ans, dict) or ans.get("noul") is None:
            return {"verdict": "uncertain", "scores": [], "spread": 1.0,
                    "error": f"missing noul answer for {qid}"}
        scores.append(_as_float(ans["noul"]))
    spread = max(scores) - min(scores)
    if all(p > 0.5 for p in scores) and spread < 0.25:
        verdict = "yes"
    elif all(p < 0.5 for p in scores) and spread < 0.25:
        verdict = "no"
    else:
        verdict = "uncertain"
    return {"verdict": verdict,
            "scores": [round(p, 3) for p in scores],
            "spread": round(spread, 3)}
