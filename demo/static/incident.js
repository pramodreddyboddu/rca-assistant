/* incident.html — incident-room behavior (vanilla JS, no build step, fully offline).
   Flow: POST /api/incident/run → render → POST /api/incident/decide →
         POST /api/incident/verify → refresh audit. */
"use strict";

(function () {
  // ---------- small helpers ----------
  var $ = function (id) { return document.getElementById(id); };

  // Escape untrusted/API text before injecting into HTML.
  var esc = function (s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  };

  // Central time, per standing convention. "never raise the behind-the-scenes
  // UTC explanation" — so we just show local times, no labels.
  var fmtTime = function (ts) {
    try {
      var d = new Date(ts);
      if (isNaN(d.getTime())) return ts;
      return d.toLocaleString("en-US", {
        timeZone: "America/Chicago",
        month: "short", day: "numeric",
        hour: "numeric", minute: "2-digit"
      });
    } catch (e) { return ts; }
  };
  var nowIso = function () { return new Date().toISOString(); };

  // Minimal POST/GET helpers against the frozen incident API contract.
  var api = function (method, path, body) {
    return fetch(path, {
      method: method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body)
    }).then(function (res) {
      return res.json().then(function (data) {
        if (!res.ok) {
          var msg = (data && data.error) || ("HTTP " + res.status);
          throw new Error(msg);
        }
        return data;
      });
    });
  };
  var get = function (path) { return api("GET", path); };
  var post = function (path, body) { return api("POST", path, body); };

  // ---------- timeline ----------
  // Stage ids in display order. "approved" is inserted only after approval.
  var STAGE_DEFS = [
    { id: "alert",      label: "Alert received" },
    { id: "evidence",   label: "Evidence gathered" },
    { id: "diagnosis",  label: "Diagnosis complete" },
    { id: "jev",        label: "Jev assessment" },
    { id: "plan",       label: "Fix proposed" },
    { id: "awaiting",   label: "Awaiting approval" },
    { id: "approved",   label: "Approved", afterApprove: true },
    { id: "verify",     label: "Verification", afterApprove: true },
    { id: "resolved",   label: "Resolved", afterApprove: true }
  ];

  var stageStates = {}; // id -> {state: "todo"|"current"|"done", time: iso|null, note: string|null}
  STAGE_DEFS.forEach(function (s) {
    stageStates[s.id] = { state: "todo", time: null, note: null };
  });

  var renderTimeline = function () {
    var ol = $("timeline");
    ol.innerHTML = "";
    STAGE_DEFS.forEach(function (def) {
      var st = stageStates[def.id];
      if (def.afterApprove && st.state === "todo" && !state.approvedFlowShown) {
        return; // hide post-approval stages until approval begins
      }
      var li = document.createElement("li");
      li.className = st.state === "done" ? "done" : (st.state === "current" ? "current" : "");
      var html = esc(def.label);
      if (st.note) html += '<span class="t-note">' + esc(st.note) + "</span>";
      if (st.time) html += '<span class="t-time">' + esc(fmtTime(st.time)) + "</span>";
      li.innerHTML = html;
      ol.appendChild(li);
    });
  };

  var setStage = function (id, stageState, time, note) {
    var st = stageStates[id];
    if (!st) return;
    st.state = stageState;
    if (time !== undefined) st.time = time;
    if (note !== undefined) st.note = note;
    renderTimeline();
  };
  var done = function (id, time, note) { setStage(id, "done", time, note); };
  var current = function (id, time, note) { setStage(id, "current", time, note); };

  // ---------- shared state ----------
  var state = {
    runData: null,          // full /run payload
    approvedFlowShown: false,
    decided: false
  };

  // ---------- fatal error (honest, never silent) ----------
  var showFatal = function (err) {
    var box = $("fatal-error");
    box.innerHTML = "<strong>Could not start the incident run.</strong>" +
      '<span class="mono">' + esc(err && err.message ? err.message : String(err)) + "</span>" +
      "<br><span>The demo server returned an error — no evidence was replayed. " +
      "Try “Run again” or check the server.</span>";
    box.hidden = false;
    $("btn-run-again").disabled = false;
  };
  var hideFatal = function () { $("fatal-error").hidden = true; };

  // ---------- render: header + summary ----------
  var renderRun = function (data) {
    var inc = data.incident || {};
    state.runData = data;

    $("incident-short-id").textContent =
      inc.id ? String(inc.id).slice(0, 8) : "unknown";

    var title = $("incident-title");
    title.textContent = inc.title || "Untitled incident";
    title.hidden = false;
    $("summary-skeleton").hidden = true;

    var meta = $("incident-meta");
    var src = (inc.evidence_source && inc.evidence_source.source) || "recorded evidence";
    meta.innerHTML = "Mode: <code>" + esc(inc.mode || "replay-demo") + "</code>" +
      " · Source: <code>" + esc(src) + "</code>";
    meta.hidden = false;

    // timeline: alert + evidence light up from the run payload
    var startedAt = inc.started_at || nowIso();
    done("alert", startedAt);
    var calls = data.evidence_calls || [];
    done("evidence", startedAt,
      calls.length ? calls.length + " tool calls" : "no tool calls recorded");
    current("diagnosis");

    renderDiagnosis(data);
    renderEvidence(data);
    renderFix(data);

    done("diagnosis", startedAt);
    done("jev", startedAt);
    done("plan", startedAt);
    current("awaiting");

    $("btn-run-again").disabled = false;
    refreshAudit();
  };

  // ---------- render: diagnosis card ----------
  var renderDiagnosis = function (data) {
    var body = $("diagnosis-body");
    var diag = data.diagnosis || {};
    var top = diag.top || {};
    var hyps = diag.hypotheses || [];
    var jev = data.jev || null; // may be null

    // Confidence: jev.top_probability/confidence; deterministic fallback otherwise.
    var conf = null, confSource = null;
    if (jev && jev.available) {
      conf = (jev.top_probability != null) ? jev.top_probability
           : (jev.confidence != null ? jev.confidence : null);
      confSource = (jev.mode === "live") ? "Jev · live assessment" : "Jev · deterministic mode";
    }

    // Self-consistency badge — never hidden; it is the brand.
    var badgeHtml;
    if (jev && jev.available && jev.self_consistency === "consistent" && !jev.uncertain) {
      badgeHtml = '<span class="consistency-badge ok">✓ Self-consistent</span>';
    } else if (jev && jev.available && (jev.uncertain || jev.self_consistency === "uncertain")) {
      badgeHtml = '<span class="consistency-badge warn">⚠ Uncertain — capped at 0.50</span>';
    } else if (jev && jev.available) {
      badgeHtml = '<span class="consistency-badge na">Self-consistency: ' +
        esc(jev.self_consistency || "n/a") + "</span>";
    } else {
      badgeHtml = '<span class="consistency-badge na">Deterministic mode — no Jev scoring</span>';
    }

    var confHtml;
    if (conf != null && isFinite(conf)) {
      var pct = Math.round(Number(conf) * 100);
      confHtml =
        '<div class="conf-row">' +
          '<div class="conf-bar" role="img" aria-label="Confidence ' + pct + ' percent">' +
            '<span style="width:' + Math.max(0, Math.min(100, pct)) + '%"></span>' +
          "</div>" +
          '<span class="conf-num">' + pct + "%</span>" +
        "</div>" +
        '<p class="mode-line">' + esc(confSource || "") + "</p>";
    } else {
      confHtml = '<p class="mode-line"><span class="mono">deterministic mode</span> — ' +
        "no live Jev probability for this run.</p>";
    }

    // Other hypotheses, dimmed, with scores.
    var others = hyps.filter(function (h) { return h.id !== top.id; });
    var othersHtml = "";
    if (others.length) {
      othersHtml = '<ul class="hypotheses">' + others.map(function (h) {
        return "<li><span>" + esc(h.title || h.id) + "</span>" +
          '<span class="h-score">' + esc(formatScore(h.score)) + "</span></li>";
      }).join("") + "</ul>";
    }

    body.innerHTML =
      '<p class="root-cause-label">Root cause</p>' +
      '<p class="root-cause-title">' + esc(top.title || "Unknown") + "</p>" +
      confHtml +
      badgeHtml +
      othersHtml;
  };

  var formatScore = function (s) {
    if (s == null || !isFinite(s)) return "—";
    var n = Number(s);
    return n <= 1 ? Math.round(n * 100) + "%" : String(n);
  };

  // ---------- render: evidence ----------
  var renderEvidence = function (data) {
    var body = $("evidence-body");
    var diag = data.diagnosis || {};
    var top = diag.top || {};
    var hyps = diag.hypotheses || [];
    var topHyp = null;
    for (var i = 0; i < hyps.length; i++) {
      if (hyps[i].id === top.id) { topHyp = hyps[i]; break; }
    }
    var items = (topHyp && topHyp.evidence) || [];

    var count = $("evidence-count");
    count.textContent = items.length + (items.length === 1 ? " item" : " items");
    count.hidden = false;

    if (!items.length) {
      body.innerHTML = '<p class="meta">No evidence items recorded for the top hypothesis.</p>';
      return;
    }
    // Excerpts are real recorded values — render verbatim.
    body.innerHTML = items.map(function (ev) {
      return '<div class="evidence-item">' +
        '<p class="ev-claim">' + esc(ev.claim) + "</p>" +
        '<span class="tool-chip">' + esc(ev.tool) + "</span>" +
        "<blockquote class=\"ev-excerpt\">" + esc(ev.excerpt) + "</blockquote>" +
        "</div>";
    }).join("");
  };

  // ---------- render: proposed fix ----------
  var renderFix = function (data) {
    var body = $("fix-body");
    var steps = (data.plan && data.plan.steps) || [];
    var step = steps[0];

    if (!step) {
      body.innerHTML = '<p class="meta">No remediation step was proposed for this run.</p>';
      return;
    }

    var argsStr = "";
    try { argsStr = JSON.stringify(step.args || {}); }
    catch (e) { argsStr = String(step.args); }
    var verify = step.verify || {};
    var verifyStr = verify.tool
      ? "After the fix, " + verify.tool + "(" + argsSummary(verify.args) + ") " +
        "must report " + JSON.stringify(verify.expect) + "."
      : "";

    body.innerHTML =
      '<div class="fix-action"><span class="fn">' + esc(step.action) + "</span>" +
        '<span dir="ltr">' + esc(argsStr) + "</span></div>" +
      (step.rationale ? '<p class="fix-rationale">' + esc(step.rationale) + "</p>" : "") +
      (verifyStr ? '<p class="fix-verify">Verify: <span class="mono">' + esc(verifyStr) + "</span></p>" : "") +
      '<div class="decision-row" id="decision-row">' +
        '<button id="btn-approve" class="btn btn-approve">Approve fix</button>' +
        '<button id="btn-reject" class="btn btn-reject">Reject</button>' +
      "</div>" +
      '<div id="decision-outcome"></div>';

    $("btn-approve").addEventListener("click", onApprove);
    $("btn-reject").addEventListener("click", onReject);
  };

  var argsSummary = function (args) {
    if (!args) return "";
    try {
      return Object.keys(args).map(function (k) {
        return k + "=" + args[k];
      }).join(", ");
    } catch (e) { return String(args); }
  };

  // ---------- decide: approve ----------
  var onApprove = function () {
    if (state.decided) return;
    state.decided = true;
    var btn = $("btn-approve");
    btn.disabled = true;
    btn.textContent = "Approving…";
    $("btn-reject").disabled = true;

    post("/api/incident/decide", { decision: "approve" })
      .then(function (resp) {
        state.approvedFlowShown = true;
        var out = $("decision-outcome");
        var audit = resp.audit || {};
        out.innerHTML =
          '<div class="decision-note approved">Approved — recorded in audit trail' +
          (audit.ts ? " · " + esc(fmtTime(audit.ts)) : "") + ".</div>" +
          executionPanelHtml(resp.execution);
        done("awaiting", nowIso());
        done("approved", nowIso());
        current("verify");
        refreshAudit();
        runVerification();
      })
      .catch(function (err) {
        state.decided = false;
        btn.disabled = false;
        btn.textContent = "Approve fix";
        $("btn-reject").disabled = false;
        $("decision-outcome").innerHTML =
          '<div class="decision-note rejected">Approval failed: ' + esc(err.message) +
          " — no action was taken.</div>";
      });
  };

  // Never imply the fix really ran: rehearsal honesty from execution.note.
  var executionPanelHtml = function (exec) {
    exec = exec || {};
    var would = exec.would_execute || {};
    var what = would.action
      ? would.action + "(" + argsSummary(would.args) + ")"
      : "the proposed action";
    var note = exec.note ||
      "Rehearsal mode — no live queue manager. In production this would execute " +
      what + " via the admin credential.";
    return '<div class="execution"><strong>Execution · rehearsal mode</strong>' +
      '<span class="mono">' + esc(note) + "</span></div>";
  };

  // ---------- decide: reject ----------
  var onReject = function () {
    if (state.decided) return;
    state.decided = true;
    $("btn-approve").disabled = true;
    var btn = $("btn-reject");
    btn.disabled = true;
    btn.textContent = "Rejecting…";

    post("/api/incident/decide", { decision: "reject" })
      .then(function (resp) {
        var out = $("decision-outcome");
        out.innerHTML = '<div class="decision-note rejected">' +
          "Fix rejected — no action taken.</div>";
        // Timeline: awaiting approval is done (decided), nothing after.
        done("awaiting", nowIso());
        refreshAudit();
      })
      .catch(function (err) {
        state.decided = false;
        $("btn-approve").disabled = false;
        btn.disabled = false;
        btn.textContent = "Reject";
        $("decision-outcome").innerHTML =
          '<div class="decision-note rejected">Rejection failed: ' + esc(err.message) +
          " — the fix was NOT rejected. Try again.</div>";
      });
  };

  // ---------- verification ----------
  var runVerification = function () {
    var card = $("verify-card");
    card.hidden = false;
    $("verify-body").innerHTML = '<div class="skeleton skeleton-block" aria-hidden="true"></div>';
    card.scrollIntoView({ behavior: "smooth", block: "nearest" });

    post("/api/incident/verify")
      .then(function (resp) {
        renderVerification(resp);
        done("verify", nowIso());
        done("resolved", nowIso(), resp.recovered ? "Incident recovered" : "Checks failed — not recovered");
        refreshAudit();
      })
      .catch(function (err) {
        $("verify-body").innerHTML =
          '<div class="decision-note rejected">Verification failed: ' + esc(err.message) + "</div>";
      });
  };

  var renderVerification = function (resp) {
    var body = $("verify-body");
    var checks = resp.checks || [];
    var ok = !!resp.recovered;

    var banner = '<div class="recovered-banner ' + (ok ? "yes" : "no") + '">' +
      (ok ? "✓ Incident recovered" : "✗ Not recovered — checks failed") + "</div>";

    var rows = checks.map(function (c) {
      var pass = !!c.pass;
      return '<div class="check-row">' +
        '<span class="check-mark ' + (pass ? "pass" : "fail") + '">' +
          (pass ? "✓" : "✗") + "</span>" +
        '<div class="check-detail">' +
          '<div class="check-tool">' + esc(c.tool) +
            (c.args ? "(" + esc(argsSummary(c.args)) + ")" : "") + "</div>" +
          '<div class="check-vals">observed <span class="mono">' + esc(JSON.stringify(c.observed)) +
            "</span> · expected <span class=\"mono\">" + esc(JSON.stringify(c.expected)) + "</span></div>" +
        "</div></div>";
    }).join("");

    body.innerHTML = banner + rows +
      (resp.note ? '<p class="verify-note">' + esc(resp.note) + "</p>" : "");
  };

  // ---------- audit trail ----------
  var refreshAudit = function () {
    get("/api/incident/audit")
      .then(function (resp) {
        var entries = resp.entries || [];
        var log = $("audit-log");
        if (!entries.length) {
          log.textContent = "No audit entries yet.";
          return;
        }
        log.textContent = entries.map(function (e) {
          var line = "[" + fmtTime(e.ts) + "] " + (e.actor || "?") + " · " + (e.action || "?");
          if (e.details) {
            var det = e.details;
            if (typeof det === "object") { try { det = JSON.stringify(det); } catch (x) {} }
            line += " — " + det;
          }
          return line;
        }).join("\n");
      })
      .catch(function (err) {
        $("audit-log").textContent = "Could not load audit trail: " + err.message;
      });
  };

  $("audit-toggle").addEventListener("click", function () {
    var body = $("audit-body");
    var open = body.hidden;
    body.hidden = !open;
    this.setAttribute("aria-expanded", String(open));
    if (open) refreshAudit();
  });

  // ---------- run again ----------
  var resetRoom = function () {
    state.runData = null;
    state.approvedFlowShown = false;
    state.decided = false;
    STAGE_DEFS.forEach(function (s) {
      stageStates[s.id] = { state: "todo", time: null, note: null };
    });
    $("incident-short-id").textContent = "…";
    $("incident-title").hidden = true;
    $("summary-skeleton").hidden = false;
    $("incident-meta").hidden = true;
    $("diagnosis-body").innerHTML =
      '<div class="skeleton skeleton-block" aria-hidden="true"></div>' +
      '<div class="skeleton skeleton-block short" aria-hidden="true"></div>';
    $("evidence-count").hidden = true;
    $("evidence-body").innerHTML =
      '<div class="skeleton skeleton-block" aria-hidden="true"></div>' +
      '<div class="skeleton skeleton-block" aria-hidden="true"></div>';
    $("fix-body").innerHTML = '<div class="skeleton skeleton-block" aria-hidden="true"></div>';
    $("verify-card").hidden = true;
    $("verify-body").innerHTML = "";
    $("audit-log").textContent = "Loading…";
    hideFatal();
    renderTimeline();
    startRun();
  };
  $("btn-run-again").addEventListener("click", resetRoom);

  // ---------- boot ----------
  var startRun = function () {
    $("btn-run-again").disabled = true;
    current("alert");
    post("/api/incident/run")
      .then(renderRun)
      .catch(showFatal);
  };

  renderTimeline();
  startRun();
})();
