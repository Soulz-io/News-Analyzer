/**
 * News Analyzer Dashboard — Main SPA
 *
 * Polls /api/dashboard/overview every 30 seconds and renders
 * run-up cards, decision tree detail views, prediction scoreboard,
 * and article feed. Follows the same iframe/polling patterns as
 * the openclaw-subagents dashboard.
 */

const API_BASE = "/plugins/openclaw-news-analyzer/api";
const POLL_INTERVAL = 30000;

/* ── State ───────────────────────────────────────────────────── */

let state = {
  runups: [],
  activeTree: null,     // { id, root, consequences_yes, consequences_no, history }
  overview: {},          // { stats: { predictions, correct, incorrect, accuracy } }
  status: {},            // { engine, feeds, articles }
  loading: true,
  error: null,
};

let activeTreeId = null; // which run-up is expanded
let pollTimer = null;
let treePollTimer = null;

/* ── Region color mapping ────────────────────────────────────── */

const REGION_CLASSES = {
  "middle-east": "region-badge--middle-east",
  "europe": "region-badge--europe",
  "asia": "region-badge--asia",
  "americas": "region-badge--americas",
  "africa": "region-badge--africa",
  "global": "region-badge--global",
};

function regionClass(region) {
  if (!region) return "region-badge--global";
  const key = region.toLowerCase().replace(/[\s_]+/g, "-");
  return REGION_CLASSES[key] || "region-badge--global";
}

/* ── Helpers ─────────────────────────────────────────────────── */

function esc(str) {
  const d = document.createElement("div");
  d.textContent = str || "";
  return d.innerHTML;
}

function pct(val) {
  if (val == null) return "0";
  return Math.round(val);
}

function gaugeColorClass(score) {
  if (score >= 75) return "score-gauge__fill--critical";
  if (score >= 50) return "score-gauge__fill--high";
  if (score >= 25) return "score-gauge__fill--medium";
  return "score-gauge__fill--low";
}

function trendArrow(trend) {
  if (!trend) return { symbol: "&#8594;", cls: "trend-arrow--stable" };
  const t = trend.toLowerCase();
  if (t === "rising" || t === "up") return { symbol: "&#8593;", cls: "trend-arrow--rising" };
  if (t === "falling" || t === "down") return { symbol: "&#8595;", cls: "trend-arrow--falling" };
  return { symbol: "&#8594;", cls: "trend-arrow--stable" };
}

function probBarFill(val) {
  // Generate filled/empty block characters for text representation
  const filled = Math.round((val || 0) / 10);
  const empty = 10 - filled;
  return "\u2588".repeat(filled) + "\u2591".repeat(empty);
}

function formatDate(dateStr) {
  if (!dateStr) return "";
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  } catch {
    return dateStr;
  }
}

function formatTime(dateStr) {
  if (!dateStr) return "";
  try {
    const d = new Date(dateStr);
    return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
  } catch {
    return dateStr;
  }
}

function ago(ms) {
  if (!ms) return "";
  const sec = Math.floor((Date.now() - ms) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

/* ── API Fetch ───────────────────────────────────────────────── */

async function fetchOverview() {
  try {
    const [overviewRes, statusRes] = await Promise.all([
      fetch(`${API_BASE}/dashboard/overview`),
      fetch(`${API_BASE}/status`),
    ]);

    if (overviewRes.ok) {
      const data = await overviewRes.json();
      state.runups = data.runups || [];
      state.overview = data.stats || data.overview || {};
    }

    if (statusRes.ok) {
      state.status = await statusRes.json();
    }

    state.loading = false;
    state.error = null;
  } catch (err) {
    console.warn("[news-analyzer] fetch overview error:", err);
    state.loading = false;
    state.error = String(err.message || err);
  }
  render();
}

async function fetchTree(runUpId) {
  try {
    const res = await fetch(`${API_BASE}/dashboard/tree/${encodeURIComponent(runUpId)}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.activeTree = await res.json();
  } catch (err) {
    console.warn("[news-analyzer] fetch tree error:", err);
    state.activeTree = null;
  }
  render();
}

/* ── Render: Main ────────────────────────────────────────────── */

function render() {
  const app = document.getElementById("app");
  if (!app) return;

  // If a tree is active, show tree detail view
  if (activeTreeId) {
    app.innerHTML = renderTreeView();
    bindTreeEvents();
    return;
  }

  // Overview
  let html = "";

  // Header
  html += renderHeader();

  // Loading state
  if (state.loading) {
    html += `<div class="loading">
      <div class="loading__spinner"></div>
      <div class="loading__text">Loading dashboard...</div>
    </div>`;
    app.innerHTML = html;
    return;
  }

  // Summary cards
  html += renderSummaryCards();

  // Active run-ups
  html += renderRunUpSection();

  // Prediction scoreboard
  html += renderScoreboard();

  app.innerHTML = html;
  bindOverviewEvents();
}

/* ── Render: Header ──────────────────────────────────────────── */

function renderHeader() {
  const s = state.status;
  const engineRunning = s.engine === "running";
  const engineCls = engineRunning ? "engine-badge--running" : "engine-badge--stopped";
  const engineLabel = engineRunning ? "Running" : "Stopped";

  const stats = state.overview;
  const activeCount = state.runups.filter(r => r.active !== false).length;

  return `<div class="header-row">
    <div class="header-left">
      <h1>News Analyzer</h1>
      <span class="engine-badge ${engineCls}">
        <span class="engine-badge__dot"></span>
        ${esc(engineLabel)}
      </span>
    </div>
    <div class="header-stats">
      <span><span class="stat-value">${activeCount}</span> active run-ups</span>
      <span><span class="stat-value">${pct(stats.predictions || 0)}</span> predictions</span>
      <span><span class="stat-value">${pct(stats.accuracy || 0)}%</span> accuracy</span>
      <button class="refresh-btn" data-refresh>&#8635; Refresh</button>
    </div>
  </div>`;
}

/* ── Render: Summary Cards ───────────────────────────────────── */

function renderSummaryCards() {
  const stats = state.overview;
  const activeCount = state.runups.filter(r => r.active !== false).length;
  const s = state.status;

  return `<div class="summary">
    <div class="card card--runups">
      <div class="card__value">${activeCount}</div>
      <div class="card__label">Active Run-ups</div>
    </div>
    <div class="card card--predictions">
      <div class="card__value">${pct(stats.predictions || 0)}</div>
      <div class="card__label">Predictions</div>
    </div>
    <div class="card card--accuracy">
      <div class="card__value">${pct(stats.accuracy || 0)}%</div>
      <div class="card__label">Accuracy</div>
    </div>
    <div class="card card--feeds">
      <div class="card__value">${pct(s.feeds || 0)}</div>
      <div class="card__label">Feeds</div>
    </div>
  </div>`;
}

/* ── Render: Run-Up Cards ────────────────────────────────────── */

function renderRunUpSection() {
  if (state.runups.length === 0) {
    return `<div class="section-title">Active Run-ups</div>
    <div class="empty">
      <div class="empty__icon">&#x1F4F0;</div>
      <div class="empty__text">No active run-ups</div>
      <div class="empty__hint">Run-ups will appear here when the news analyzer detects developing narratives.</div>
    </div>`;
  }

  let html = `<div class="section-title">Active Run-ups</div><div class="runup-grid">`;

  for (const r of state.runups) {
    const arrow = trendArrow(r.trend);
    const score = r.score || r.runup_score || 0;
    const gaugeCls = gaugeColorClass(score);
    const isActive = r.active !== false;
    const prob = r.root_probability || r.probability || 0;
    const probChange = r.probability_change || 0;
    const probChangeCls = probChange > 0 ? "prob-change--up" : (probChange < 0 ? "prob-change--down" : "prob-change--stable");
    const probChangeSign = probChange > 0 ? "+" : "";
    const probChangeArrow = probChange > 0 ? "&#8593;" : (probChange < 0 ? "&#8595;" : "&#8594;");

    html += `<div class="runup-card ${isActive ? "runup-card--active" : ""}" data-runup-id="${esc(r.id || r.runup_id || "")}">
      <div class="runup-card__header">
        <div class="runup-card__name">${esc(r.narrative || r.name || "Unnamed")}</div>
        <div class="runup-card__meta">
          <span class="region-badge ${regionClass(r.region)}">${esc(r.region || "Global")}</span>
          <span class="trend-arrow ${arrow.cls}" title="${esc(r.trend || "stable")}">${arrow.symbol}</span>
        </div>
      </div>

      <div class="score-gauge">
        <div class="score-gauge__header">
          <span class="score-gauge__label">Run-up Score</span>
          <span class="score-gauge__value">${pct(score)}</span>
        </div>
        <div class="score-gauge__bar">
          <div class="score-gauge__fill ${gaugeCls}" style="width:${pct(score)}%"></div>
        </div>
      </div>

      <div class="runup-card__details">
        <span>${r.days_active || 0} days active</span>
        <span>${r.article_count || 0} articles</span>
      </div>

      <div class="runup-card__decision">
        <div class="decision-question">${esc(r.root_question || r.decision || "")}</div>
        <div class="prob-bar">
          <div class="prob-bar__track">
            <div class="prob-bar__fill" style="width:${pct(prob)}%"></div>
          </div>
          <div class="prob-bar__label">${pct(prob)}%</div>
        </div>
        ${probChange !== 0 ? `<div class="prob-change ${probChangeCls}">${probChangeArrow} ${probChangeSign}${pct(probChange)}% deze week</div>` : ""}
      </div>
    </div>`;
  }

  html += `</div>`;
  return html;
}

/* ── Render: Decision Tree Detail ────────────────────────────── */

function renderTreeView() {
  const tree = state.activeTree;
  const runup = state.runups.find(r => (r.id || r.runup_id) === activeTreeId);

  let html = `<div class="tree-view">`;

  // Back button
  html += `<button class="back-btn" data-back>&#8592; Back to Overview</button>`;

  if (!tree) {
    html += `<div class="loading">
      <div class="loading__spinner"></div>
      <div class="loading__text">Loading decision tree...</div>
    </div></div>`;
    return html;
  }

  // Title
  const root = tree.root || {};
  html += `<div class="tree-title">${esc(runup ? (runup.narrative || runup.name) : (root.narrative || "Decision Tree"))}</div>`;
  if (runup) {
    html += `<div class="tree-subtitle">
      <span class="region-badge ${regionClass(runup.region)}">${esc(runup.region || "Global")}</span>
      &nbsp; ${runup.days_active || 0} days active &middot; ${runup.article_count || 0} articles tracked
    </div>`;
  }

  // Root node
  const rootProb = root.probability || 0;
  html += `<div class="tree-root">
    <div class="tree-root__question">${esc(root.question || root.decision || "")}</div>
    <div class="prob-bar">
      <div class="prob-bar__track">
        <div class="prob-bar__fill" style="width:${pct(rootProb)}%"></div>
      </div>
      <div class="prob-bar__label">${pct(rootProb)}%</div>
    </div>
  </div>`;

  // Connector
  html += `<div class="tree-connector"><div class="tree-connector__line"></div></div>`;

  // Branches
  const yesConsequences = tree.consequences_yes || tree.consequences?.yes || [];
  const noConsequences = tree.consequences_no || tree.consequences?.no || [];

  html += `<div class="tree-branches">`;

  // YES branch
  html += `<div class="tree-branch">
    <div class="tree-branch__header tree-branch__header--yes">&#10003; Yes</div>
    <div class="tree-branch__body">`;
  if (yesConsequences.length === 0) {
    html += `<div class="text-dim" style="padding:12px;font-size:0.82rem;">No consequences mapped yet.</div>`;
  }
  for (let i = 0; i < yesConsequences.length; i++) {
    html += renderConsequenceCard(yesConsequences[i], i + 1);
  }
  html += `</div></div>`;

  // NO branch
  html += `<div class="tree-branch">
    <div class="tree-branch__header tree-branch__header--no">&#10007; No</div>
    <div class="tree-branch__body">`;
  if (noConsequences.length === 0) {
    html += `<div class="text-dim" style="padding:12px;font-size:0.82rem;">No consequences mapped yet.</div>`;
  }
  for (let i = 0; i < noConsequences.length; i++) {
    html += renderConsequenceCard(noConsequences[i], i + 1);
  }
  html += `</div></div>`;

  html += `</div>`; // close tree-branches

  // Probability history
  const history = tree.history || tree.probability_history || [];
  if (history.length > 0) {
    html += renderProbHistory(history);
  }

  html += `</div>`; // close tree-view
  return html;
}

/* ── Render: Consequence Card ────────────────────────────────── */

function renderConsequenceCard(c, num) {
  const prob = c.probability || 0;
  const isNearCertain = prob >= 90;
  const impacts = c.impacts || c.impact_tags || [];
  const keywords = c.keywords || [];
  const statusVal = c.status || "";
  const followup = c.followup || c.follow_up || null;

  let html = `<div class="consequence-card">
    <div class="consequence-card__header">
      <span class="consequence-card__number">${num}</span>
      <span class="consequence-card__desc">${esc(c.description || c.text || "")}</span>
    </div>

    <div class="consequence-card__prob">
      <div class="prob-bar">
        <div class="prob-bar__track">
          <div class="prob-bar__fill" style="width:${pct(prob)}%"></div>
        </div>
        <div class="prob-bar__label">${pct(prob)}%</div>
      </div>
    </div>`;

  // Near-certainty badge
  if (isNearCertain) {
    html += `<span class="near-certainty">Near-certainty</span> `;
  }

  // Status badge
  if (statusVal) {
    const statusCls = statusVal === "confirmed" ? "prediction-status--confirmed"
      : statusVal === "denied" ? "prediction-status--denied"
      : "prediction-status--predicted";
    html += `<span class="prediction-status ${statusCls}">${esc(statusVal)}</span> `;
  }

  // Impact badges
  if (impacts.length > 0) {
    html += `<div class="impact-badges">`;
    for (const imp of impacts) {
      const impKey = (typeof imp === "string" ? imp : imp.type || "").toLowerCase();
      const impLabel = typeof imp === "string" ? imp : (imp.label || imp.type || "");
      const impDetail = typeof imp === "object" && imp.detail ? `: ${imp.detail}` : "";
      const impCls = `impact-badge--${impKey}`;
      html += `<span class="impact-badge ${impCls}">${esc(impLabel)}${esc(impDetail)}</span>`;
    }
    html += `</div>`;
  }

  // Keywords
  if (keywords.length > 0) {
    html += `<div class="keyword-tags">`;
    for (const kw of keywords) {
      html += `<span class="keyword-tag">${esc(kw)}</span>`;
    }
    html += `</div>`;
  }

  // Follow-up question
  if (followup) {
    const fProb = followup.probability || 0;
    html += `<div class="followup">
      <div class="followup__question">&#8594; ${esc(followup.question || "")}</div>
      <div class="followup__prob">P(ja) = ${pct(fProb)}%</div>
    </div>`;
  }

  html += `</div>`;
  return html;
}

/* ── Render: Probability History ─────────────────────────────── */

function renderProbHistory(history) {
  let html = `<div class="prob-history">
    <div class="prob-history__title">Probability History</div>
    <div class="prob-history__list">`;

  for (const entry of history) {
    const from = entry.from || entry.previous || 0;
    const to = entry.to || entry.current || 0;
    const diff = to - from;
    const sign = diff > 0 ? "+" : "";
    const changeCls = diff > 0 ? "prob-history__change--up" : (diff < 0 ? "prob-history__change--down" : "");
    const date = formatDate(entry.date || entry.timestamp);
    const reason = entry.reason || entry.trigger || "";
    const articles = entry.articles || entry.article_count || 0;

    html += `<div class="prob-history__entry">
      <span class="prob-history__date">${esc(date)}</span>
      <span class="prob-history__change ${changeCls}">${pct(from)}% &#8594; ${pct(to)}% (${sign}${pct(diff)}%)</span>
      <span class="prob-history__reason">${articles > 0 ? `${articles} articles` : ""} ${esc(reason)}</span>
    </div>`;
  }

  html += `</div></div>`;
  return html;
}

/* ── Render: Scoreboard ──────────────────────────────────────── */

function renderScoreboard() {
  const stats = state.overview;
  const total = stats.predictions || 0;
  const correct = stats.correct || 0;
  const incorrect = stats.incorrect || 0;
  const accuracy = stats.accuracy || 0;
  const outcomes = stats.recent_outcomes || [];

  let html = `<div class="scoreboard">
    <div class="section-title">Prediction Scoreboard</div>
    <div class="scoreboard-grid">
      <div class="scoreboard-card">
        <div class="scoreboard-card__value">${pct(total)}</div>
        <div class="scoreboard-card__label">Total</div>
      </div>
      <div class="scoreboard-card scoreboard-card--correct">
        <div class="scoreboard-card__value">${pct(correct)}</div>
        <div class="scoreboard-card__label">Correct</div>
      </div>
      <div class="scoreboard-card scoreboard-card--incorrect">
        <div class="scoreboard-card__value">${pct(incorrect)}</div>
        <div class="scoreboard-card__label">Incorrect</div>
      </div>
      <div class="scoreboard-card scoreboard-card--accuracy">
        <div class="scoreboard-card__value">${pct(accuracy)}%</div>
        <div class="scoreboard-card__label">Accuracy</div>
      </div>
    </div>`;

  // Recent outcomes table
  if (outcomes.length > 0) {
    html += `<table class="outcomes-table">
      <thead><tr>
        <th>Prediction</th>
        <th>Probability</th>
        <th>Outcome</th>
        <th>Date</th>
      </tr></thead><tbody>`;

    for (const o of outcomes) {
      const statusCls = o.outcome === "correct" ? "prediction-status--confirmed"
        : o.outcome === "incorrect" ? "prediction-status--denied"
        : "prediction-status--predicted";
      const outcomeLabel = o.outcome === "correct" ? "Confirmed"
        : o.outcome === "incorrect" ? "Denied"
        : "Pending";

      html += `<tr>
        <td>${esc(o.description || o.prediction || "")}</td>
        <td class="font-tabular">${pct(o.probability || 0)}%</td>
        <td><span class="prediction-status ${statusCls}">${esc(outcomeLabel)}</span></td>
        <td class="text-dim">${esc(formatDate(o.date || o.resolved_at || ""))}</td>
      </tr>`;
    }

    html += `</tbody></table>`;
  }

  html += `</div>`;
  return html;
}

/* ── Event Bindings ──────────────────────────────────────────── */

function bindOverviewEvents() {
  // Refresh button
  const refreshBtn = document.querySelector("[data-refresh]");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => {
      state.loading = true;
      render();
      fetchOverview();
    });
  }

  // Run-up card clicks
  document.querySelectorAll("[data-runup-id]").forEach((card) => {
    card.addEventListener("click", () => {
      const id = card.getAttribute("data-runup-id");
      if (id) {
        activeTreeId = id;
        state.activeTree = null;
        render();
        fetchTree(id);
        startTreePolling(id);
      }
    });
  });
}

function bindTreeEvents() {
  // Back button
  const backBtn = document.querySelector("[data-back]");
  if (backBtn) {
    backBtn.addEventListener("click", () => {
      activeTreeId = null;
      state.activeTree = null;
      stopTreePolling();
      render();
    });
  }
}

/* ── Polling ─────────────────────────────────────────────────── */

function startPolling() {
  if (pollTimer) return;
  pollTimer = setInterval(fetchOverview, POLL_INTERVAL);
}

function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

function startTreePolling(id) {
  stopTreePolling();
  treePollTimer = setInterval(() => fetchTree(id), POLL_INTERVAL);
}

function stopTreePolling() {
  if (treePollTimer) { clearInterval(treePollTimer); treePollTimer = null; }
}

// Pause/resume on visibility change
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    stopPolling();
    stopTreePolling();
  } else {
    fetchOverview();
    startPolling();
    if (activeTreeId) {
      fetchTree(activeTreeId);
      startTreePolling(activeTreeId);
    }
  }
});

/* ── Theme sync from parent iframe ───────────────────────────── */

function syncTheme() {
  try {
    const parent = window.parent;
    if (parent === window) return;
    const app = parent.document.querySelector("openclaw-app");
    if (!app) return;
    const root = app.shadowRoot || app;
    const style = window.parent.getComputedStyle(root);
    const vars = ["--bg", "--bg-card", "--text", "--accent", "--border"];
    for (const v of vars) {
      const val = style.getPropertyValue(v);
      if (val) document.documentElement.style.setProperty(v, val);
    }
  } catch {
    /* cross-origin or unavailable */
  }
}

/* ── Init ────────────────────────────────────────────────────── */

syncTheme();
fetchOverview();
startPolling();
