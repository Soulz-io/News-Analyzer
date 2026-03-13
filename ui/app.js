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
  feeds: [],             // All RSS feeds (default + user)
  budget: null,          // { daily_budget_eur, spent_today_eur, remaining_eur, percentage_used }
  apiKeyStatus: null,    // { has_key, masked }
  polymarket: [],        // Polymarket matches for current tree
  loading: true,
  error: null,
};

let activeTreeId = null; // which run-up is expanded
let pollTimer = null;
let treePollTimer = null;
let feedsExpanded = false;
let feedFormVisible = false;

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
      fetch(`${API_BASE}?_api=overview`),
      fetch(`${API_BASE}?_api=status`),
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

async function fetchFeeds() {
  try {
    const res = await fetch(`${API_BASE}?_api=feeds`);
    if (res.ok) {
      state.feeds = await res.json();
    }
  } catch (err) {
    console.warn("[news-analyzer] fetch feeds error:", err);
  }
}

async function addFeed(name, url, region) {
  try {
    const res = await fetch(`${API_BASE}?_api=feeds`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, url, region }),
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      throw new Error(data.detail || `HTTP ${res.status}`);
    }
    await fetchFeeds();
    render();
  } catch (err) {
    alert("Failed to add feed: " + err.message);
  }
}

async function deleteFeed(feedId) {
  try {
    const res = await fetch(`${API_BASE}?_api=feeds&id=${encodeURIComponent(feedId)}`, { method: "DELETE" });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      throw new Error(data.detail || `HTTP ${res.status}`);
    }
    await fetchFeeds();
    render();
  } catch (err) {
    alert("Failed to delete feed: " + err.message);
  }
}

async function fetchBudget() {
  try {
    const res = await fetch(`${API_BASE}?_api=budget`);
    if (res.ok) {
      state.budget = await res.json();
    }
  } catch (err) {
    console.warn("[news-analyzer] fetch budget error:", err);
  }
}

async function updateBudget(amount) {
  try {
    const res = await fetch(`${API_BASE}?_api=budget`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ daily_budget_eur: parseFloat(amount) }),
    });
    if (res.ok) {
      state.budget = await res.json();
      await fetchBudget();
      render();
    }
  } catch (err) {
    console.warn("[news-analyzer] update budget error:", err);
  }
}

async function fetchTree(runUpId) {
  try {
    const res = await fetch(`${API_BASE}?_api=tree&id=${encodeURIComponent(runUpId)}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    // Transform API format { run_up, tree: [nodes], polymarket: [...] } into UI format
    const nodes = data.tree || [];
    const rootNode = nodes.find(n => n.branch === "root" || n.depth === 0) || nodes[0];

    // Polymarket data comes inline from the tree endpoint
    state.polymarket = data.polymarket || [];

    if (!rootNode) {
      state.activeTree = null;
    } else {
      const yesCons = (rootNode.consequences || []).filter(c => c.branch === "yes").map(mapConsequence);
      const noCons = (rootNode.consequences || []).filter(c => c.branch === "no").map(mapConsequence);

      state.activeTree = {
        root: {
          question: rootNode.question || "",
          probability: (rootNode.yes_probability || 0) * 100,
          timeline: rootNode.timeline_estimate || "",
          narrative: rootNode.question || "",
        },
        consequences_yes: yesCons,
        consequences_no: noCons,
        history: [],
      };
    }
  } catch (err) {
    console.warn("[news-analyzer] fetch tree error:", err);
    state.activeTree = null;
    state.polymarket = [];
  }
  render();
}

function mapConsequence(c) {
  const impacts = [];
  if (c.impact_economic) impacts.push({ type: "Economic", detail: c.impact_economic });
  if (c.impact_geopolitical) impacts.push({ type: "Geopolitical", detail: c.impact_geopolitical });
  if (c.impact_social) impacts.push({ type: "Social", detail: c.impact_social });
  return {
    description: c.description || "",
    probability: (c.probability || 0) * 100,
    branch_probability: (c.branch_probability || 0) * 100,
    effective_probability: (c.effective_probability || 0) * 100,
    impacts: impacts,
    keywords: c.keywords || [],
    status: c.status || "predicted",
    stock_impacts: (c.stock_impacts || []).map(si => ({
      ticker: si.ticker || "",
      name: si.name || "",
      asset_type: si.asset_type || "stock",
      direction: si.direction || "bullish",
      magnitude: si.magnitude || "moderate",
      reasoning: si.reasoning || "",
      available_on_bunq: si.available_on_bunq || false,
    })),
  };
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

  // Token budget section
  html += renderBudgetSection();

  // RSS Feeds section
  html += renderFeedsSection();

  // Active run-ups
  html += renderRunUpSection();

  // Prediction scoreboard
  html += renderScoreboard();

  app.innerHTML = html;
  bindOverviewEvents();
  bindFeedEvents();
  bindBudgetEvents();
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
    <div class="card card--feeds" style="cursor:pointer" data-scroll-feeds>
      <div class="card__value" style="display:flex;align-items:center;gap:8px;">
        <span style="width:24px;height:24px;display:inline-block;color:var(--orange)">${RSS_ICON}</span>
        ${pct(s.feeds || state.feeds.length || 0)}
      </div>
      <div class="card__label">RSS Feeds</div>
    </div>
    <div class="card card--budget" style="cursor:pointer" data-scroll-budget>
      <div class="card__value" style="display:flex;align-items:center;gap:6px;">
        <span style="font-size:20px">💰</span>
        €${state.budget ? state.budget.spent_today_eur.toFixed(2) : '0.00'}
      </div>
      <div class="card__label">/ €${state.budget ? state.budget.daily_budget_eur.toFixed(2) : '1.00'} budget</div>
    </div>
  </div>`;
}

/* ── Render: Budget Section ──────────────────────────────────── */

function renderBudgetSection() {
  const b = state.budget;
  if (!b) return '';

  const pctUsed = Math.min(b.percentage_used, 100);
  const barColor = pctUsed > 80 ? 'var(--red, #f44)' : pctUsed > 50 ? 'var(--orange, #f90)' : 'var(--green, #4f4)';
  const hasKey = state.apiKeyStatus && state.apiKeyStatus.has_key;
  const keyMasked = state.apiKeyStatus ? state.apiKeyStatus.masked : '';

  return `<div class="budget-section" id="budget-section">
    <div class="budget-header">
      <div class="budget-title">
        <span style="font-size:18px">💰</span> Token Budget & API
      </div>
      <div class="budget-controls">
        <label class="budget-label">Daily limit: €</label>
        <input type="number" class="budget-input" id="budget-input"
          value="${b.daily_budget_eur.toFixed(2)}" min="0" max="100" step="0.10" />
        <button class="budget-save-btn" id="budget-save">Save</button>
      </div>
    </div>
    <div class="budget-bar-container">
      <div class="budget-bar" style="width:${pctUsed}%;background:${barColor}"></div>
    </div>
    <div class="budget-stats">
      <span>Spent today: <strong>€${b.spent_today_eur.toFixed(4)}</strong></span>
      <span>Remaining: <strong>€${b.remaining_eur.toFixed(4)}</strong></span>
      <span>Used: <strong>${pctUsed.toFixed(1)}%</strong></span>
    </div>
    <div class="api-key-row">
      <div class="api-key-status ${hasKey ? 'api-key--set' : 'api-key--missing'}">
        ${hasKey
          ? `<span class="api-key-dot api-key-dot--ok"></span> API Key: ${esc(keyMasked)}`
          : `<span class="api-key-dot api-key-dot--missing"></span> No API Key set`}
      </div>
      <div class="api-key-form">
        <input type="password" class="api-key-input" id="api-key-input"
          placeholder="sk-ant-..." value="" />
        <button class="budget-save-btn" id="api-key-save">
          ${hasKey ? 'Update' : 'Set Key'}
        </button>
      </div>
    </div>
  </div>`;
}

function bindBudgetEvents() {
  const saveBtn = document.getElementById('budget-save');
  if (saveBtn) {
    saveBtn.addEventListener('click', () => {
      const input = document.getElementById('budget-input');
      if (input) {
        const val = parseFloat(input.value);
        if (!isNaN(val) && val >= 0) {
          updateBudget(val);
        }
      }
    });
  }
  // API key save
  const keySaveBtn = document.getElementById('api-key-save');
  if (keySaveBtn) {
    keySaveBtn.addEventListener('click', async () => {
      const input = document.getElementById('api-key-input');
      if (input && input.value.trim()) {
        try {
          const res = await fetch(`${API_BASE}?_api=apikey`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ api_key: input.value.trim() }),
          });
          if (res.ok) {
            input.value = '';
            await fetchApiKeyStatus();
            render();
          } else {
            alert('Failed to save API key');
          }
        } catch (err) {
          alert('Error: ' + err.message);
        }
      }
    });
  }
  // Scroll-to from summary card
  const scrollBtn = document.querySelector('[data-scroll-budget]');
  if (scrollBtn) {
    scrollBtn.addEventListener('click', () => {
      const section = document.getElementById('budget-section');
      if (section) section.scrollIntoView({ behavior: 'smooth' });
    });
  }
}

async function fetchApiKeyStatus() {
  try {
    const res = await fetch(`${API_BASE}?_api=apikey`);
    if (res.ok) {
      state.apiKeyStatus = await res.json();
    }
  } catch (err) {
    console.warn("[news-analyzer] fetch api-key status error:", err);
  }
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

  // Polymarket calibration panel
  const polyMatches = state.polymarket || [];
  if (polyMatches.length > 0) {
    html += `<div class="polymarket-panel">
      <div class="polymarket-panel__header">
        <span class="polymarket-logo">PM</span>
        Polymarket Calibration
      </div>`;

    for (const pm of polyMatches) {
      const pmProb = Math.round((pm.outcome_yes_price || 0) * 100);
      const ourProb = pct(rootProb);
      const calProb = pm.calibrated_probability
        ? Math.round(pm.calibrated_probability * 100)
        : null;
      const diff = pmProb - ourProb;
      const diffCls = diff > 5 ? "poly-diff--higher"
        : diff < -5 ? "poly-diff--lower"
        : "poly-diff--aligned";
      const volume = pm.volume ? `$${(pm.volume / 1000).toFixed(0)}K` : "";

      html += `<div class="polymarket-match">
        <div class="polymarket-match__question">${esc(pm.polymarket_question)}</div>
        <div class="polymarket-match__probs">
          <div class="prob-compare">
            <span class="prob-compare__label">Our estimate:</span>
            <span class="prob-compare__value">${ourProb}%</span>
          </div>
          <div class="prob-compare">
            <span class="prob-compare__label">Polymarket:</span>
            <span class="prob-compare__value">${pmProb}%</span>
          </div>
          ${calProb !== null ? `
          <div class="prob-compare prob-compare--calibrated">
            <span class="prob-compare__label">Calibrated:</span>
            <span class="prob-compare__value">${calProb}%</span>
          </div>` : ""}
        </div>
        <div class="polymarket-match__meta">
          <span class="poly-diff ${diffCls}">
            ${diff > 0 ? "+" : ""}${diff}pp difference
          </span>
          ${volume ? `<span class="poly-volume">Vol: ${volume}</span>` : ""}
          ${pm.polymarket_url ? `<a href="${esc(pm.polymarket_url)}" target="_blank" rel="noopener" class="poly-link">View on Polymarket &#8599;</a>` : ""}
        </div>
      </div>`;
    }

    html += `</div>`;
  }

  // Connector
  html += `<div class="tree-connector"><div class="tree-connector__line"></div></div>`;

  // Branches
  const yesConsequences = tree.consequences_yes || tree.consequences?.yes || [];
  const noConsequences = tree.consequences_no || tree.consequences?.no || [];

  const yesProb = pct(rootProb);
  const noProb = pct(100 - rootProb);

  html += `<div class="tree-branches">`;

  // YES branch
  html += `<div class="tree-branch">
    <div class="tree-branch__header tree-branch__header--yes">
      &#10003; Yes <span class="tree-branch__prob">${yesProb}%</span>
    </div>
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
    <div class="tree-branch__header tree-branch__header--no">
      &#10007; No <span class="tree-branch__prob">${noProb}%</span>
    </div>
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
  const condProb = c.probability || 0;
  const effProb = c.effective_probability || 0;
  const isNearCertain = effProb >= 70;
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
          <div class="prob-bar__fill" style="width:${pct(effProb)}%"></div>
        </div>
        <div class="prob-bar__label">${pct(effProb)}%</div>
      </div>
      <div class="prob-bar__detail">Effective probability &middot; <span class="text-dim">if this path: ${pct(condProb)}%</span></div>
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

  // Stock impacts
  const stocks = c.stock_impacts || [];
  if (stocks.length > 0) {
    html += `<div class="stock-impacts">`;
    for (const si of stocks) {
      const dirCls = si.direction === "bullish" ? "stock-badge--bullish" : "stock-badge--bearish";
      const arrow = si.direction === "bullish" ? "&#9650;" : "&#9660;";
      const magCls = si.magnitude === "high" ? "stock-mag--high" : (si.magnitude === "extreme" ? "stock-mag--extreme" : "");
      const bunqBadge = si.available_on_bunq
        ? `<span class="stock-badge__bunq" title="Beschikbaar op bunq Stocks">bunq</span>`
        : `<span class="stock-badge__no-bunq" title="Niet beschikbaar op bunq">&#8709;</span>`;
      html += `<div class="stock-badge ${dirCls}" title="${esc(si.reasoning)}">
        ${bunqBadge}
        <span class="stock-badge__arrow">${arrow}</span>
        <span class="stock-badge__ticker">${esc(si.ticker)}</span>
        <span class="stock-badge__name">${esc(si.name)}</span>
        <span class="stock-badge__mag ${magCls}">${esc(si.magnitude)}</span>
      </div>`;
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

/* ── RSS Icon SVG ────────────────────────────────────────────── */

const RSS_ICON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 11a9 9 0 0 1 9 9"/><path d="M4 4a16 16 0 0 1 16 16"/><circle cx="5" cy="19" r="1"/></svg>`;

const RSS_ICON_SM = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M4 11a9 9 0 0 1 9 9"/><path d="M4 4a16 16 0 0 1 16 16"/><circle cx="5" cy="19" r="1"/></svg>`;

/* ── Render: RSS Feeds Section ───────────────────────────────── */

function renderFeedsSection() {
  const feeds = state.feeds || [];
  const defaultFeeds = feeds.filter(f => f.source === "default");
  const userFeeds = feeds.filter(f => f.source === "user");
  const totalCount = feeds.length;

  const regions = [
    "global", "middle-east", "east-asia", "south-asia", "southeast-asia",
    "russia-cis", "europe", "north-america", "latam", "africa"
  ];

  let html = `<div class="feeds-section">
    <div class="feeds-header">
      <div class="feeds-header__left">
        <span class="feeds-header__icon">${RSS_ICON}</span>
        <span class="feeds-header__title">RSS Feeds</span>
        <span class="feeds-header__count">${totalCount} feeds</span>
      </div>
      <div style="display:flex;gap:8px;align-items:center;">
        <button class="feeds-add-btn" data-feed-add-toggle>+ Add Feed</button>
        <button class="feeds-toggle-btn" data-feeds-toggle>
          ${feedsExpanded ? "Hide" : "Show"} feeds
        </button>
      </div>
    </div>`;

  // Add feed form
  html += `<div class="feed-add-form ${feedFormVisible ? "feed-add-form--visible" : ""}" id="feed-add-form">
    <input type="text" name="name" placeholder="Feed name" />
    <input type="url" name="url" placeholder="https://example.com/rss.xml" />
    <select name="region">
      ${regions.map(r => `<option value="${esc(r)}">${esc(r)}</option>`).join("")}
    </select>
    <div class="feed-add-form__actions">
      <button class="feed-add-form__submit" data-feed-submit>Add</button>
      <button class="feed-add-form__cancel" data-feed-cancel>Cancel</button>
    </div>
  </div>`;

  // Feed list (collapsible)
  html += `<div class="feeds-grid ${feedsExpanded ? "feeds-grid--expanded" : ""}">`;

  // User feeds first (editable)
  for (const f of userFeeds) {
    html += `<div class="feed-item">
      <span class="feed-item__icon">${RSS_ICON_SM}</span>
      <div class="feed-item__info">
        <div class="feed-item__name">${esc(f.name)}</div>
        <div class="feed-item__url">${esc(f.url)}</div>
      </div>
      <div class="feed-item__badges">
        <span class="region-badge ${regionClass(f.region)}">${esc(f.region)}</span>
        <span class="feed-item__source-badge feed-item__source-badge--user">user</span>
        <button class="feed-item__delete" data-feed-delete="${f.id}" title="Remove feed">&times;</button>
      </div>
    </div>`;
  }

  // Default feeds
  for (const f of defaultFeeds) {
    html += `<div class="feed-item">
      <span class="feed-item__icon">${RSS_ICON_SM}</span>
      <div class="feed-item__info">
        <div class="feed-item__name">${esc(f.name)}</div>
        <div class="feed-item__url">${esc(f.url)}</div>
      </div>
      <div class="feed-item__badges">
        <span class="region-badge ${regionClass(f.region)}">${esc(f.region)}</span>
        <span class="feed-item__source-badge feed-item__source-badge--default">default</span>
      </div>
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
      fetchBudget();
    });
  }

  // Run-up card clicks
  document.querySelectorAll("[data-runup-id]").forEach((card) => {
    card.addEventListener("click", () => {
      const id = parseInt(card.getAttribute("data-runup-id"), 10);
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

function bindFeedEvents() {
  // Toggle feed list
  const toggleBtn = document.querySelector("[data-feeds-toggle]");
  if (toggleBtn) {
    toggleBtn.addEventListener("click", () => {
      feedsExpanded = !feedsExpanded;
      render();
    });
  }

  // Scroll feeds card click
  const scrollBtn = document.querySelector("[data-scroll-feeds]");
  if (scrollBtn) {
    scrollBtn.addEventListener("click", () => {
      feedsExpanded = true;
      render();
      setTimeout(() => {
        const section = document.querySelector(".feeds-section");
        if (section) section.scrollIntoView({ behavior: "smooth", block: "start" });
      }, 50);
    });
  }

  // Toggle add form
  const addToggle = document.querySelector("[data-feed-add-toggle]");
  if (addToggle) {
    addToggle.addEventListener("click", () => {
      feedFormVisible = !feedFormVisible;
      feedsExpanded = true;
      render();
    });
  }

  // Submit new feed
  const submitBtn = document.querySelector("[data-feed-submit]");
  if (submitBtn) {
    submitBtn.addEventListener("click", () => {
      const form = document.getElementById("feed-add-form");
      if (!form) return;
      const name = form.querySelector('input[name="name"]').value.trim();
      const url = form.querySelector('input[name="url"]').value.trim();
      const region = form.querySelector('select[name="region"]').value;
      if (!name || !url) { alert("Name and URL are required."); return; }
      feedFormVisible = false;
      addFeed(name, url, region);
    });
  }

  // Cancel add form
  const cancelBtn = document.querySelector("[data-feed-cancel]");
  if (cancelBtn) {
    cancelBtn.addEventListener("click", () => {
      feedFormVisible = false;
      render();
    });
  }

  // Delete feed buttons
  document.querySelectorAll("[data-feed-delete]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const feedId = btn.getAttribute("data-feed-delete");
      if (feedId && confirm("Remove this feed?")) {
        deleteFeed(feedId);
      }
    });
  });
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
    fetchBudget();
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
fetchFeeds();
fetchBudget();
fetchApiKeyStatus();
startPolling();
