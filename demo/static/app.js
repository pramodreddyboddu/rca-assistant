/* RCA Assistant web demo UI. Vanilla JS, fetch only, no frameworks. */

"use strict";

const state = {
  runId: null,
  scenario: null,
  alert: null,
  diagnosis: null,
  plan: null,          // plan steps from /api/runs/{id}/plan
  stepStatus: {},      // step_id -> chip state
  pendingRequest: null,
  runStatus: null,
};

const TIMELINE = [
  "Incident injected",
  "Evidence gathered",
  "Diagnosis",
  "Plan proposed",
  "Approvals",
  "Outcome",
];
let timelineDone = new Set();   // indexes finished
let timelineActive = -1;        // index currently in progress

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  let data = null;
  try { data = await res.json(); } catch (_) { /* non-JSON: leave null */ }
  return { status: res.status, data };
}

function setEnabled(id, on) { document.getElementById(id).disabled = !on; }

/* ------------------------------------------------------------------ */
/* Timeline                                                            */
/* ------------------------------------------------------------------ */

function renderTimeline() {
  const ol = document.getElementById("timeline");
  ol.innerHTML = TIMELINE.map((label, i) => {
    let cls = "";
    if (timelineDone.has(i)) cls = "done";
    else if (i === timelineActive) cls = "active";
    return `<li class="${cls}">${esc(label)}</li>`;
  }).join("");
}

/* ------------------------------------------------------------------ */
/* Scenarios + run info                                                */
/* ------------------------------------------------------------------ */

async function loadScenarios() {
  const { status, data } = await api("GET", "/api/scenarios");
  const box = document.getElementById("scenarios");
  if (status !== 200 || !Array.isArray(data)) {
    box.innerHTML = `<p class="muted">Could not load scenarios.</p>`;
    return;
  }
  box.innerHTML = data.map((s) => `
    <div class="scenario">
      <h3>${esc(s.title)}</h3>
      <p>${esc(s.description)}</p>
      <button data-scenario="${esc(s.id)}">Inject</button>
    </div>`).join("");
  box.querySelectorAll("button[data-scenario]").forEach((btn) => {
    btn.addEventListener("click", () => injectScenario(btn.dataset.scenario));
  });
}

function renderRunInfo() {
  const box = document.getElementById("run-info");
  if (!state.runId) {
    box.innerHTML = `<p class="muted">No run yet. Pick a scenario above.</p>`;
    return;
  }
  const alertBits = Object.entries(state.alert || {})
    .filter(([k]) => k !== "type")
    .map(([k, v]) => `<div class="kv"><strong>${esc(k)}:</strong> <code>${esc(v)}</code></div>`)
    .join("");
  box.innerHTML = `
    <div class="kv"><strong>run:</strong> <code>${esc(state.runId)}</code></div>
    <div class="kv"><strong>scenario:</strong> <code>${esc(state.scenario)}</code></div>
    <div class="kv"><strong>alert:</strong> <code>${esc(state.alert ? state.alert.type : "?")}</code></div>
    ${alertBits}
    <div class="kv"><strong>status:</strong> <code>${esc(state.runStatus || "?")}</code></div>`;
}

/* ------------------------------------------------------------------ */
/* Inject -> diagnose -> plan                                          */
/* ------------------------------------------------------------------ */

async function injectScenario(scenarioId) {
  const { status, data } = await api("POST", "/api/runs", { scenario: scenarioId });
  if (status !== 200) {
    alert("Inject failed: " + (data && data.error ? data.error : status));
    return;
  }
  state.runId = data.run_id;
  state.scenario = data.scenario;
  state.alert = data.alert;
  state.diagnosis = null;
  state.plan = null;
  state.stepStatus = {};
  state.pendingRequest = null;
  state.runStatus = "injected";
  timelineDone = new Set([0]);
  timelineActive = 1;
  renderTimeline();
  renderRunInfo();
  document.getElementById("evidence").innerHTML = `<p class="muted">No diagnosis yet.</p>`;
  document.getElementById("evidence-count").textContent = "";
  document.getElementById("plan").innerHTML = `<p class="muted">No plan proposed yet.</p>`;
  document.getElementById("approval").innerHTML = `<p class="muted">No pending approval.</p>`;
  setEnabled("btn-diagnose", true);
  setEnabled("btn-plan", false);
  setEnabled("btn-audit", true);
  setEnabled("btn-report", false);
  refreshAudit();
}

async function diagnose() {
  const { status, data } = await api("POST", `/api/runs/${state.runId}/diagnose`);
  if (status !== 200) {
    alert("Diagnose failed: " + (data && data.error ? data.error : status));
    return;
  }
  state.diagnosis = data;
  state.runStatus = "diagnosed";
  timelineDone = new Set([0, 1, 2]);
  timelineActive = 3;
  renderTimeline();
  renderRunInfo();

  const top = data.hypotheses.find((h) => h.id === data.top_id) || data.hypotheses[0];
  document.getElementById("evidence-count").textContent =
    `— ${data.evidence_calls} read-only tool calls`;

  /* Jev confidence is a first-class part of the diagnosis, not an add-on. */
  const jev = data.jev || null;
  const rankById = {};
  ((jev && jev.hypothesis_ranking) || []).forEach((r) => { rankById[r.hypothesis_id] = r; });
  const supportByClaim = {};
  ((jev && jev.claim_support) || []).forEach((c) => { supportByClaim[c.claim] = c; });

  let jevHead = "";
  if (jev && jev.available) {
    const agr = jev.agreement === "agree"
      ? "Jev <strong>agrees</strong> with the deterministic top hypothesis."
      : jev.agreement === "disagree"
        ? "Jev <strong>disagrees</strong> with the deterministic top — the deterministic result still decides; a human reviews the difference."
        : "";
    const tri = jev.triage
      ? `Triage <strong>${esc(jev.triage.priority)}</strong> (confidence ${Number(jev.triage.confidence).toFixed(2)})` : "";
    const risk = jev.remediation_risk
      ? `Remediation risk <strong>${esc(jev.remediation_risk.risk)}</strong> (score ${Number(jev.remediation_risk.risk_score).toFixed(2)}, confidence ${Number(jev.remediation_risk.confidence).toFixed(2)})`
      : `Remediation risk <span class="muted">pending — scored once a plan is proposed</span>`;
    const ill = jev.illustrative
      ? `<p class="muted small">Illustrative demo confidence values — connect a TypeSafe key for live Jev scoring.</p>` : "";
    jevHead = `<div class="jev-panel"><div class="jev-title">Jev confidence</div>`
      + (agr ? `<p>${agr}</p>` : "")
      + ((tri || risk) ? `<div class="kv">${tri}${tri && risk ? " · " : ""}${risk}</div>` : "")
      + ill + `</div>`;
  } else {
    jevHead = `<div class="jev-panel jev-off"><div class="jev-title">Deterministic mode</div>`
      + `<p class="muted small">Jev confidence unavailable — set <code>TYPESAFE_API_KEY</code> for calibrated confidence scoring.</p></div>`;
  }

  const ranked = data.hypotheses.map((h) => {
    const r = rankById[h.id];
    const jevBit = r
      ? `<span class="jev-conf"><span class="bar"><span style="width:${Math.round(r.jev_probability * 100)}%"></span></span>`
        + ` Jev P(root cause) <strong>${Number(r.jev_probability).toFixed(2)}</strong>`
        + ` · confidence ${Number(r.confidence).toFixed(2)}</span>`
      : "";
    return `<div class="kv"><code>${esc(h.id)}</code> ${esc(h.title)}
     — score <strong>${Number(h.score).toFixed(2)}</strong>${h.id === data.top_id ? " ← top" : ""}${jevBit}</div>`;
  }).join("");
  const cards = (top && top.evidence.length ? top.evidence : []).map((e) => {
    const s = supportByClaim[e.claim];
    const sBit = s
      ? `<p class="jev-verdict verdict-${esc(s.verdict)}">Jev: ${esc(s.verdict.replace(/_/g, " "))} · confidence ${Number(s.confidence).toFixed(2)}</p>`
      : "";
    return `
    <div class="evidence-card">
      <p class="claim">${esc(e.claim)}</p>
      <p class="source">via <code>${esc(e.tool)}</code></p>
      <p class="excerpt">${esc(e.excerpt)}</p>
      ${sBit}
    </div>`;
  }).join("");
  document.getElementById("evidence").innerHTML =
    jevHead + ranked + `<h3 style="font-size:0.9rem;margin:0.75rem 0 0.5rem;">Cited evidence for “${esc(top ? top.title : "?")}”</h3>` +
    (cards || `<p class="muted">No cited evidence.</p>`);
  setEnabled("btn-plan", true);
  refreshAudit();
}

async function proposePlan() {
  const { status, data } = await api("POST", `/api/runs/${state.runId}/plan`);
  if (status !== 200) {
    alert("Propose plan failed: " + (data && data.error ? data.error : status));
    return;
  }
  state.runStatus = "plan_proposed";
  timelineDone.add(3);
  timelineActive = 4;
  renderTimeline();
  renderRunInfo();
  if (data.plan === null || !data.steps) {
    document.getElementById("plan").innerHTML =
      `<p class="muted">${esc(data.note || "No plan proposed.")}</p>`;
    document.getElementById("approval").innerHTML = `<p class="muted">Nothing to approve.</p>`;
    timelineDone.add(4); timelineDone.add(5); timelineActive = -1;
    renderTimeline();
    refreshAudit();
    return;
  }
  state.plan = data.steps;
  state.planRisk = data.remediation_risk || null;
  state.stepStatus = {};
  data.steps.forEach((s) => { state.stepStatus[s.id] = "pending"; });
  renderPlan();
  state.pendingRequest = data.pending_request;
  renderApproval();
  refreshAudit();
}

/* ------------------------------------------------------------------ */
/* Plan steps + approval                                               */
/* ------------------------------------------------------------------ */

function chipFor(stepId) {
  const s = state.stepStatus[stepId] || "pending";
  const labels = {
    pending: "pending", approved: "approved", executed: "executed",
    verified: "verified", failed: "verify failed",
    rejected: "rejected", halted: "halted",
  };
  return `<span class="chip ${s}">${esc(labels[s] || s)}</span>`;
}

function renderPlan() {
  const box = document.getElementById("plan");
  if (!state.plan) {
    box.innerHTML = `<p class="muted">No plan proposed yet.</p>`;
    return;
  }
  /* Jev scored the remediation risk against this exact plan — advisory
     only; the human still approves or rejects each step. */
  const r = state.planRisk;
  const riskHead = r
    ? `<div class="jev-panel"><div class="jev-title">Jev remediation risk</div>`
      + `<p>Executing this plan: <strong>${esc(r.risk)}</strong> risk `
      + `(score ${Number(r.risk_score).toFixed(2)}, confidence ${Number(r.confidence).toFixed(2)}). `
      + `This advises; it does not approve anything.</p></div>`
    : "";
  box.innerHTML = riskHead + state.plan.map((s) => `
    <div class="step" id="step-${esc(s.id)}">
      <div class="step-head">
        <strong><code>${esc(s.id)}</code> · ${esc(s.action)}</strong>
        ${chipFor(s.id)}
      </div>
      <dl>
        <dt>args</dt><dd class="mono">${esc(JSON.stringify(s.args))}</dd>
        <dt>scope</dt><dd class="mono">${esc(s.required_scope)}</dd>
      </dl>
      <div class="kv">${esc(s.rationale)}</div>
      ${s.verify ? `<div class="kv small">verify: <code>${esc(s.verify.tool)}</code>
        <code>${esc(JSON.stringify(s.verify.args))}</code>
        expect <code>${esc(JSON.stringify(s.verify.expect))}</code></div>` : ""}
    </div>`).join("");
}

function stepById(stepId) {
  return (state.plan || []).find((s) => s.id === stepId);
}

function renderApproval() {
  const box = document.getElementById("approval");
  const pending = state.pendingRequest;
  if (!pending) {
    box.innerHTML = `<p class="muted">No pending approval.</p>`;
    return;
  }
  const step = stepById(pending.step_id);
  if (!step) {
    box.innerHTML = `<p class="muted">Pending request has no matching plan step.</p>`;
    return;
  }
  box.innerHTML = `
    <div class="pending-step">
      <strong>Pending approval: <code>${esc(step.action)}</code>
      <code>${esc(step.id)}</code></strong>
      <dl>
        <dt>Action</dt><dd class="mono">${esc(step.action)}</dd>
        <dt>Args</dt><dd class="mono">${esc(JSON.stringify(step.args))}</dd>
        <dt>Rationale</dt><dd>${esc(step.rationale)}</dd>
        <dt>Required scope</dt><dd class="mono">${esc(step.required_scope)}</dd>
        ${step.verify ? `<dt>Verify after</dt>
          <dd class="mono">${esc(step.verify.tool)} ${esc(JSON.stringify(step.verify.args))}
          → ${esc(JSON.stringify(step.verify.expect))}</dd>` : ""}
      </dl>
      <div class="btn-row">
        <button class="primary" id="btn-approve">Approve</button>
        <button class="danger" id="btn-reject">Reject</button>
        <button id="btn-approve-all">Approve all remaining</button>
      </div>
    </div>`;
  document.getElementById("btn-approve").addEventListener("click", () => decide("approve"));
  document.getElementById("btn-reject").addEventListener("click", () => decide("reject"));
  document.getElementById("btn-approve-all").addEventListener("click", () => decide("approve_all"));
}

function markStep(outcome, finalState) {
  if (outcome && outcome.step_id) state.stepStatus[outcome.step_id] = finalState;
  renderPlan();
}

async function decide(decision) {
  const pending = state.pendingRequest;
  if (!pending) return;
  const { status, data } = await api("POST", `/api/runs/${state.runId}/decide`,
    { request_id: pending.request_id, decision });
  if (status !== 200) {
    alert("Decision failed: " + (data && data.error ? data.error : status));
    return;
  }
  const outcomeBox = document.getElementById("approval");
  refreshAudit();

  if (data.status === "step_completed") {
    markStep(data.outcome, data.outcome.verify_ok === false ? "failed" :
      (data.outcome.verify_ok ? "verified" : "executed"));
    state.pendingRequest = data.next_pending_request;
    state.runStatus = "awaiting_approval";
    renderApproval();
    renderRunInfo();
    return;
  }

  if (data.status === "completed") {
    const outcomes = data.outcomes || (data.outcome ? [data.outcome] : []);
    outcomes.forEach((o) => markStep(o, o.verify_ok === false ? "failed" :
      (o.verify_ok ? "verified" : "executed")));
    state.pendingRequest = null;
    state.runStatus = "completed";
    timelineDone.add(4); timelineDone.add(5); timelineActive = -1;
    renderTimeline();
    renderRunInfo();
    const steps = outcomes.length;
    outcomeBox.innerHTML = `<div class="outcome completed">
      <strong>Plan completed.</strong> ${steps} step${steps === 1 ? "" : "s"}
      approved, executed, and verified.</div>`;
    setEnabled("btn-report", true);
    return;
  }

  // halted_rejected | halted_error | halted_verify_failed
  if (data.outcome) {
    markStep(data.outcome,
      data.status === "halted_rejected" ? "rejected" : "failed");
  }
  state.pendingRequest = null;
  state.runStatus = data.status;
  timelineDone.add(5); timelineActive = -1;
  renderTimeline();
  renderRunInfo();
  const reasons = {
    halted_rejected: `Rejected at step <code>${esc(data.halted_at || "?")}</code>. No further privileged tools were invoked.`,
    halted_error: `Step <code>${esc(data.halted_at || "?")}</code> failed to execute. The incident is left in its current state.`,
    halted_verify_failed: `Verification failed at step <code>${esc(data.halted_at || "?")}</code>. The incident is left in its current state.`,
  };
  outcomeBox.innerHTML = `<div class="outcome halted">
    <strong>Plan halted (${esc(data.status)}).</strong>
    ${reasons[data.status] || ""}</div>`;
  setEnabled("btn-report", true);
}

/* ------------------------------------------------------------------ */
/* Audit trail, report, host compare                                   */
/* ------------------------------------------------------------------ */

async function refreshAudit() {
  if (!state.runId) return;
  const { status, data } = await api("GET", `/api/runs/${state.runId}/audit`);
  const body = document.getElementById("audit-body");
  const label = document.getElementById("audit-status");
  if (status !== 200 || !data) {
    body.innerHTML = `<tr><td colspan="5" class="muted">Could not load audit trail.</td></tr>`;
    return;
  }
  body.innerHTML = data.entries.map((e) => `
    <tr>
      <td>${esc(e.seq)}</td>
      <td>${esc(e.ts)}</td>
      <td>${esc(e.event)}</td>
      <td>${esc(e.actor)}</td>
      <td class="details">${esc(JSON.stringify(e.details))}</td>
    </tr>`).join("");
  label.textContent = `${data.entries.length} entries · chain ${data.valid ? "VALID" : "INVALID"}`;
}

async function downloadReport() {
  if (!state.runId) return;
  const { status, data } = await api("GET", `/api/runs/${state.runId}/report`);
  if (status !== 200 || !data || !data.markdown) {
    alert("Report failed: " + (data && data.error ? data.error : status));
    return;
  }
  const blob = new Blob([data.markdown], { type: "text/markdown" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `incident-report-${state.runId}.md`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 5000);
}

async function compareHosts() {
  const box = document.getElementById("host-compare");
  box.innerHTML = `<p class="muted">Loading…</p>`;
  const { status, data } = await api("GET", "/api/host/compare");
  if (status !== 200 || !data || !data.sim || !data.real) {
    box.innerHTML = `<p class="muted">Compare failed: ${esc(data && data.error ? data.error : status)}</p>`;
    return;
  }
  const rows = [
    ["host", data.sim.host, data.real.host],
    ["source", data.sim.source || "sim", data.real.source || "?"],
    ["cpu_pct", data.sim.cpu_pct, data.real.cpu_pct],
    ["mem_pct", data.sim.mem_pct, data.real.mem_pct],
    ["disk_pct", data.sim.disk_pct, data.real.disk_pct],
    ["load1", data.sim.load1, data.real.load1],
  ];
  box.innerHTML = `<table>
    <thead><tr><th>metric</th><th>sim</th><th>real</th></tr></thead>
    <tbody>${rows.map((r) => `<tr><td>${esc(r[0])}</td><td>${esc(r[1])}</td><td>${esc(r[2])}</td></tr>`).join("")}</tbody>
  </table>`;
}

/* ------------------------------------------------------------------ */
/* Jev status banner + confidence                                      */
/* ------------------------------------------------------------------ */

async function loadJevStatus() {
  const box = document.getElementById("jev-banner");
  const { status, data } = await api("GET", "/api/jev/status");
  if (status !== 200 || !data) {
    box.innerHTML = `<p class="muted small">Jev status unavailable.</p>`;
    return;
  }
  if (data.mode === "jev") {
    box.className = "jev-banner on";
    box.innerHTML = `<strong>Jev confidence scoring active.</strong>`
      + ` Every diagnosis carries calibrated confidence, evidence-support`
      + ` checks, triage priority, and remediation risk.`
      + (data.illustrative ? ` <em>(illustrative demo values)</em>` : "");
  } else {
    box.className = "jev-banner off";
    box.innerHTML = `<strong>Deterministic mode.</strong> Set`
      + ` <code>TYPESAFE_API_KEY</code> to enable Jev confidence scoring.`;
  }
}

/* ------------------------------------------------------------------ */
/* Boot                                                               */
/* ------------------------------------------------------------------ */

document.getElementById("btn-diagnose").addEventListener("click", diagnose);
document.getElementById("btn-plan").addEventListener("click", proposePlan);
document.getElementById("btn-audit").addEventListener("click", refreshAudit);
document.getElementById("btn-report").addEventListener("click", downloadReport);
document.getElementById("btn-host").addEventListener("click", compareHosts);
renderTimeline();
loadScenarios();
loadJevStatus();
