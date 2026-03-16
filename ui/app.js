/**
 * OpenClaw News Analyzer — Clean Modern Dashboard
 *
 * Tab-based SPA: Signals | Trees | Intel | Settings
 * Progressive disclosure: cards → modals for detail.
 */

const API_BASE = "/plugins/openclaw-news-analyzer/api";
const POLL_INTERVAL = 30000;

/* ── State ───────────────────────────────────────────────────── */

let state = {
  activeTab: "signals",
  runups: [],
  activeTree: null,
  overview: {},
  status: {},
  feeds: [],
  budget: null,
  apiKeyStatus: null,
  polymarket: [],
  analysis: null,
  signals: [],
  indicators: null,
  swarmStatus: null,
  opportunities: [],
  focus: null,
  advisory: null,
  advisoryHistory: null,
  portfolioAlignment: null,
  usageData: null,
  loading: true,
  error: null,
};

let activeTreeId = null;
let pollTimer = null;
let treePollTimer = null;
let _priceModalTicker = null;

/* ── Helpers ─────────────────────────────────────────────────── */

function esc(str) {
  const d = document.createElement("div");
  d.textContent = str || "";
  return d.innerHTML;
}

function pct(val) { return val == null ? 0 : Math.round(val); }

function truncateText(text, max) {
  if (!text) return "";
  return text.length > max ? text.slice(0, max - 1) + "\u2026" : text;
}

function ago(ms) {
  if (!ms) return "";
  const sec = Math.floor((Date.now() - ms) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function formatNum(n, dec = 0) {
  if (n == null) return "-";
  return Number(n).toLocaleString("en-US", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function verdictClass(v) {
  if (!v) return "";
  return "verdict-badge--" + v.toLowerCase().replace(/_/g, "-");
}

function verdictLabel(v) { return (v || "HOLD").replace(/_/g, " "); }

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
  return REGION_CLASSES[region.toLowerCase().replace(/[\s_]+/g, "-")] || "region-badge--global";
}

/* ── API Layer ───────────────────────────────────────────────── */

async function fetchOverview() {
  try {
    const [oRes, sRes] = await Promise.all([
      fetch(`${API_BASE}?_api=overview`),
      fetch(`${API_BASE}?_api=status`),
    ]);
    if (oRes.ok) {
      const d = await oRes.json();
      state.runups = d.runups || [];
      state.overview = d.stats || d.overview || {};
      state.overview.resolved_predictions = d.resolved_predictions || [];
      state.autoScorer = d.auto_scorer || null;
    }
    if (sRes.ok) state.status = await sRes.json();
    state.loading = false;
    state.error = null;
  } catch (err) {
    state.loading = false;
    state.error = String(err.message || err);
  }
  // NOTE: callers decide whether to render (boot, visibility handler).
  // Polling does NOT re-render to avoid scroll-position glitches.
}

async function fetchSignals() {
  try {
    const r = await fetch(`${API_BASE}?_api=signals`);
    if (r.ok) state.signals = await r.json();
  } catch {}
}

async function fetchIndicators() {
  try {
    const r = await fetch(`${API_BASE}?_api=indicators`);
    if (r.ok) state.indicators = await r.json();
  } catch {}
}

async function fetchAnalysis() {
  try {
    const r = await fetch(`${API_BASE}?_api=analysis`);
    if (r.ok) state.analysis = await r.json();
  } catch {}
}

async function fetchAdvisory() {
  try {
    const r = await fetch(`${API_BASE}?_api=advisory`);
    if (r.ok) state.advisory = await r.json();
  } catch {}
}

async function fetchAdvisoryHistory() {
  try {
    const r = await fetch(`${API_BASE}?_api=advisory-history&limit=14`);
    if (r.ok) state.advisoryHistory = await r.json();
  } catch {}
}

async function fetchPortfolioAlignment() {
  try {
    const r = await fetch(`${API_BASE}?_api=portfolio-alignment`);
    if (r.ok) state.portfolioAlignment = await r.json();
  } catch {}
}

async function fetchUsage(days = 7) {
  try {
    const r = await fetch(`${API_BASE}?_api=usage-breakdown&days=${days}`);
    if (r.ok) state.usageData = await r.json();
  } catch {}
}

async function fetchFeeds() {
  try {
    const r = await fetch(`${API_BASE}?_api=feeds`);
    if (r.ok) state.feeds = await r.json();
  } catch {}
}

async function fetchBudget() {
  try {
    const r = await fetch(`${API_BASE}?_api=budget`);
    if (r.ok) state.budget = await r.json();
  } catch {}
}

async function fetchApiKeyStatus() {
  try {
    const r = await fetch(`${API_BASE}?_api=apikey`);
    if (r.ok) state.apiKeyStatus = await r.json();
  } catch {}
}

async function fetchSwarmStatus() {
  try {
    const r = await fetch(`${API_BASE}?_api=swarm-status`);
    if (r.ok) state.swarmStatus = await r.json();
  } catch {}
}

async function fetchOpportunities() {
  try {
    const r = await fetch(`${API_BASE}?_api=opportunities&minEdge=3`);
    if (r.ok) state.opportunities = await r.json();
  } catch {}
}

async function fetchFocus() {
  try {
    const r = await fetch(`${API_BASE}?_api=focus`);
    if (r.ok) state.focus = await r.json();
  } catch {}
}

async function setFocus(runupIds) {
  // Optimistic update for instant visual feedback
  if (!state.focus) state.focus = { focused_runup_ids: [], focused_runups: [], polymarket_links: {} };
  state.focus.focused_runup_ids = runupIds;
  render();

  try {
    const r = await fetch(`${API_BASE}?_api=focus`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ runup_ids: runupIds }),
    });
    if (r.ok) {
      const data = await r.json();
      state.focus.focused_runup_ids = data.focused_runup_ids;
    }
    await fetchFocus();
    await fetchOverview();
    render();
  } catch (err) {
    console.warn("[focus] setFocus failed:", err);
    // Re-fetch to get actual state
    await fetchFocus();
    render();
  }
}

async function addPolymarketLink(runUpId, url) {
  try {
    await fetch(`${API_BASE}?_api=focus-polymarket-link`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_up_id: runUpId, polymarket_url: url }),
    });
    await fetchFocus();
    render();
  } catch {}
}

async function regenerateFocusTree(runUpId) {
  try {
    await fetch(`${API_BASE}?_api=focus-regenerate-tree&id=${runUpId}`, {
      method: "POST",
    });
    // Reload tree data
    if (activeTreeId === runUpId) {
      await fetchTree(runUpId);
    }
    await fetchOverview();
    render();
  } catch {}
}

async function fetchTree(runUpId) {
  try {
    const r = await fetch(`${API_BASE}?_api=tree&id=${encodeURIComponent(runUpId)}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const nodes = data.tree || [];
    state.polymarket = data.polymarket || [];
    const rootNode = nodes.find(n => n.branch === "root" || n.depth === 0) || nodes[0];
    if (!rootNode) {
      state.activeTree = null;
    } else {
      state.activeTree = {
        root: {
          question: rootNode.question || "",
          probability: (rootNode.yes_probability || 0) * 100,
          timeline: rootNode.timeline_estimate || "",
        },
        tree: nodes,
      };
    }
  } catch {
    state.activeTree = null;
    state.polymarket = [];
  }
  render();
}

async function fetchSwarmVerdict(nodeId) {
  try {
    const r = await fetch(`${API_BASE}?_api=swarm-verdict&nodeId=${nodeId}`);
    if (r.ok) return await r.json();
  } catch {}
  return null;
}

/* ── Render: Main Router ─────────────────────────────────────── */

function render() {
  const app = document.getElementById("app");
  if (!app) return;
  const scrollY = window.scrollY;

  let html = renderNavbar();

  if (state.loading) {
    html += `<div class="main"><div class="loading">
      <div class="loading__spinner"></div>
      <div class="loading__text">Loading dashboard...</div>
    </div></div>`;
    app.innerHTML = html;
    return;
  }

  // If viewing a tree detail (card layout)
  if (activeTreeId && state.activeTab === "trees") {
    html += `<div class="main">${renderTreeView()}</div>`;
    app.innerHTML = html;
    bindNavEvents();
    bindTreeViewEvents();
    bindTabEvents();
    return;
  }

  // Tab content
  html += `<div class="main">`;
  switch (state.activeTab) {
    case "signals": html += renderSignalsTab(); break;
    case "portfolio": html += renderPortfolioTab(); break;
    case "trees":   html += renderTreesTab(); break;
    case "intel":   html += renderIntelTab(); break;
    case "usage":   html += renderUsageTab(); break;
    case "settings": html += renderSettingsTab(); break;
  }
  html += `</div>`;

  app.innerHTML = html;
  bindNavEvents();
  bindTabEvents();
  requestAnimationFrame(() => window.scrollTo(0, scrollY));
}

/* ── Render: Navbar ──────────────────────────────────────────── */

function renderNavbar() {
  const running = state.status.engine === "running";
  const tabs = [
    { id: "signals", label: "Signals" },
    { id: "portfolio", label: "Portfolio" },
    { id: "trees", label: "Trees" },
    { id: "intel", label: "Intel" },
    { id: "usage", label: "Usage" },
    { id: "settings", label: "Settings" },
  ];

  let indHtml = "";
  const ind = state.indicators || {};
  for (const [key, label] of [["bitcoin", "BTC"], ["gold", "Gold"], ["oil", "Oil"], ["vix", "VIX"]]) {
    const d = ind[key];
    if (!d || d.price == null) continue;
    const chg = d.change_pct || d.change_24h_pct || 0;
    const chgCls = chg >= 0 ? "nav-ind__chg--up" : "nav-ind__chg--down";
    const prefix = chg >= 0 ? "+" : "";
    const ticker = key === "bitcoin" ? "BTC-USD" : key === "gold" ? "GC=F" : key === "oil" ? "CL=F" : "^VIX";
    indHtml += `<span class="nav-ind" data-chart-ticker="${ticker}">
      <span class="nav-ind__label">${label}</span>
      <span class="nav-ind__price">${formatNum(d.price, key === "vix" ? 1 : 0)}</span>
      <span class="${chgCls}">${prefix}${chg.toFixed(1)}%</span>
    </span>`;
  }

  return `<nav class="navbar"><div class="navbar__inner">
    <div class="navbar__brand">
      <span class="navbar__status navbar__status--${running ? "ok" : "err"}"></span>
      <span class="navbar__title">OpenClaw</span>
    </div>
    <div class="navbar__tabs">
      ${tabs.map(t => `<button class="nav-tab${state.activeTab === t.id ? " nav-tab--active" : ""}" data-tab="${t.id}">${t.label}</button>`).join("")}
    </div>
    <div class="navbar__indicators">${indHtml}</div>
  </div></nav>`;
}

function bindNavEvents() {
  document.querySelectorAll("[data-tab]").forEach(btn => {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.tab;
      if (tab === state.activeTab) return;
      state.activeTab = tab;
      activeTreeId = null;
      location.hash = tab;
      render();
      // Lazy-load tab data
      if (tab === "portfolio" && !state.advisory) {
        Promise.all([fetchAdvisory(), fetchAdvisoryHistory(), fetchPortfolioAlignment()]).then(render);
      }
      if (tab === "intel" && !state.analysis) fetchAnalysis().then(render);
      if (tab === "usage" && !state.usageData) fetchUsage().then(render);
      if (tab === "settings") {
        Promise.all([fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus(), fetchFocus()]).then(render);
      }
    });
  });
  // Indicator clicks → price chart
  document.querySelectorAll(".nav-ind[data-chart-ticker]").forEach(el => {
    el.addEventListener("click", () => openPriceChart(el.dataset.chartTicker));
  });
}

/* ══════════════════════════════════════════════════════════════
   SIGNALS TAB
   ══════════════════════════════════════════════════════════════ */

function renderSignalsTab() {
  let html = "";

  // Stock signals first (most relevant for the user)
  html += renderStockSignals();

  // Opportunities Board (our estimate vs Polymarket)
  html += renderOpportunitiesBoard();

  // Resolved predictions track record
  html += renderResolvedOutcomes();

  return html;
}

function renderStockSignals() {
  // Use trading signals as primary source
  const sigs = state.signals || [];
  const sorted = [...sigs].sort((a, b) => {
    const order = { STRONG_BUY: 0, BUY: 1, ALERT: 2, WATCH: 3 };
    return (order[a.signal_level] || 9) - (order[b.signal_level] || 9);
  });

  if (!sorted.length) {
    return `<div class="signals-section">
      <div class="section-label">Stock Signals</div>
      <div class="empty-state">
        <div class="empty-state__icon">&#x1F4CA;</div>
        <div>No active trading signals. The swarm consensus engine evaluates decision nodes every hour.</div>
      </div>
    </div>`;
  }

  let cards = "";
  for (const s of sorted.slice(0, 12)) {
    const lvl = s.signal_level || "WATCH";
    const badgeCls = lvl === "STRONG_BUY" ? "verdict-badge--strong-buy"
      : lvl === "BUY" ? "verdict-badge--buy"
      : lvl === "ALERT" ? "verdict-badge--hold"
      : "verdict-badge--hold";
    const dirCls = s.direction === "bullish" ? "signal-card__ticker--long" : "signal-card__ticker--short";
    const arrow = s.direction === "bullish" ? "&#9650;" : s.direction === "bearish" ? "&#9660;" : "";

    const meta = [];
    if (s.confidence) meta.push(`<span class="signal-card__conf">${Math.round(s.confidence * 100)}%</span>`);
    if (s.news_count > 0) meta.push(`<span>${s.news_count} sources</span>`);
    if (s.polymarket_prob != null) meta.push(`<span>PM ${Math.round(s.polymarket_prob * 100)}%</span>`);

    // Swarm verdict badge (x_signal in components repurposed to store swarm score)
    let swarmBadge = "";
    const swarmScore = (s.components && s.components.x_signal) || 0;
    if (swarmScore > 0) {
      // Try to find the matching swarm verdict from the overview data
      const matchRu = (state.runups || []).find(r => r.narrative === s.narrative_name);
      if (matchRu && matchRu.swarm_verdict) {
        const vCls = verdictClass(matchRu.swarm_verdict);
        swarmBadge = `<span class="verdict-badge verdict-badge--sm ${vCls}" title="Swarm consensus">${verdictLabel(matchRu.swarm_verdict)}</span>`;
      } else {
        // Fallback: show score
        swarmBadge = `<span class="badge badge--swarm" title="Swarm signal strength">&gt; ${Math.round(swarmScore * 100)}%</span>`;
      }
    }

    cards += `<div class="signal-card" data-signal-narrative="${esc(s.narrative_name || "")}" data-signal-ticker="${esc(s.ticker || "")}">
      <div class="signal-card__top">
        <span class="verdict-badge ${badgeCls}">${lvl.replace(/_/g, " ")}</span>
        ${swarmBadge}
        ${s.ticker ? `<span class="signal-card__ticker ${dirCls}" data-chart-ticker="${esc(s.ticker)}">${arrow} ${esc(s.ticker)}</span>` : ""}
      </div>
      <div class="signal-card__narrative">${esc(s.narrative_name || "Unknown narrative")}</div>
      <div class="signal-card__meta">${meta.join(" &middot; ")}</div>
      ${s.reasoning ? `<div class="signal-card__reason">${esc(s.reasoning)}</div>` : ""}
    </div>`;
  }

  return `<div class="signals-section">
    <div class="signals-header">
      <span class="section-label" style="margin:0">Stock Signals</span>
      <button class="btn btn--sm" data-refresh-signals>Refresh</button>
    </div>
    <div class="signal-grid">${cards}</div>
  </div>`;
}

function renderResolvedOutcomes() {
  const resolved = (state.overview && state.overview.resolved_predictions) || [];
  if (!resolved.length) return "";

  let rows = "";
  for (const r of resolved) {
    const icon = r.correct ? "&#x2713;" : "&#x2717;";
    const cls = r.correct ? "badge--resolved" : "badge--incorrect";
    const outcomeCls = r.outcome === "YES" ? "badge--resolved" : "badge--incorrect";
    rows += `<div class="resolved-row">
      <span class="resolved-row__icon ${cls}">${icon}</span>
      <div class="resolved-row__body">
        <div class="resolved-row__question">${esc(truncateText(r.question, 120))}</div>
        <div class="resolved-row__meta">
          <span>Predicted: ${r.predicted_probability}%</span>
          <span class="badge ${outcomeCls}">Outcome: ${r.outcome}</span>
          ${r.days_active ? `<span>${r.days_active}d active</span>` : ""}
          ${r.article_count ? `<span>${r.article_count} articles</span>` : ""}
          <span class="resolved-row__narrative">${esc(r.narrative_name)}</span>
        </div>
      </div>
    </div>`;
  }

  const correctCount = resolved.filter(r => r.correct).length;
  const accuracy = resolved.length > 0 ? Math.round(correctCount / resolved.length * 100) : 0;

  return `<div class="signals-section">
    <div class="signals-header">
      <span class="section-label" style="margin:0">Resolved Predictions</span>
      <span class="badge badge--neutral">${correctCount}/${resolved.length} correct (${accuracy}%)</span>
    </div>
    <div class="resolved-list">${rows}</div>
  </div>`;
}

function renderOpportunitiesBoard() {
  const opps = state.opportunities || [];
  if (!opps.length) {
    return `<div class="signals-section">
      <div class="section-label">Opportunities Board</div>
      <div class="empty-state">
        <div class="empty-state__icon">&#x1F4A1;</div>
        <div>No opportunities detected yet. Opportunities appear when our probability estimate diverges from Polymarket by 3%+.</div>
      </div>
    </div>`;
  }

  let cards = "";
  for (const opp of opps.slice(0, 12)) {
    const isLong = opp.edge_direction === "long";
    const edgeAbs = Math.abs(opp.edge);
    const intensity = Math.min(edgeAbs / 30, 1).toFixed(2);
    const dirCls = isLong ? "opp-card--long" : "opp-card--short";
    const actionLabel = isLong ? "BUY OPPORTUNITY" : "SHORT OPPORTUNITY";
    const actionCls = isLong ? "opp-card__action--long" : "opp-card__action--short";
    const edgeCls = opp.edge >= 0 ? "opp-card__edge--positive" : "opp-card__edge--negative";
    const edgeSign = opp.edge >= 0 ? "+" : "";

    let metaParts = [];
    if (opp.article_count) metaParts.push(`<span>${opp.article_count} articles</span>`);
    if (opp.days_active) metaParts.push(`<span>${opp.days_active}d active</span>`);
    if (opp.volume) metaParts.push(`<span>$${(opp.volume / 1000).toFixed(0)}K vol</span>`);
    if (opp.polymarket_url) metaParts.push(`<a href="${esc(opp.polymarket_url)}" target="_blank" rel="noopener" style="color:var(--purple)">Polymarket &rarr;</a>`);

    let swarmHtml = "";
    if (opp.swarm_verdict && opp.swarm_confidence > 0) {
      const vCls = verdictClass(opp.swarm_verdict);
      swarmHtml = `<div class="opp-card__swarm">
        <span class="verdict-badge ${vCls}">${verdictLabel(opp.swarm_verdict)}</span>
        <span style="font-family:var(--mono);font-size:0.72rem;color:var(--text-dim)"> ${opp.swarm_confidence}%</span>
        ${opp.swarm_ticker ? `<span class="ticker-pill ${opp.swarm_ticker_direction === "long" ? "ticker-pill--long" : "ticker-pill--short"}" data-chart-ticker="${esc(opp.swarm_ticker)}">${opp.swarm_ticker_direction === "long" ? "&#9650;" : "&#9660;"} ${esc(opp.swarm_ticker)}</span>` : ""}
      </div>`;
    }

    cards += `<div class="opp-card ${dirCls}" style="--edge-intensity:${intensity}" data-opp-runup="${opp.run_up_id}">
      <div class="opp-card__action ${actionCls}">${actionLabel}</div>
      <div class="opp-card__question">${esc(truncateText(opp.question, 100))}</div>
      <div class="opp-card__comparison">
        <span class="opp-card__prob opp-card__prob--ours">Our: ${opp.our_probability}%</span>
        <span class="opp-card__vs">vs</span>
        <span class="opp-card__prob opp-card__prob--market">Market: ${opp.market_probability}%</span>
        <span class="opp-card__edge ${edgeCls}">${edgeSign}${opp.edge}%</span>
      </div>
      ${swarmHtml}
      <div class="opp-card__meta">${metaParts.join(" &middot; ")}</div>
    </div>`;
  }

  return `<div class="signals-section">
    <div class="signals-header">
      <span class="section-label" style="margin:0">Opportunities Board</span>
      <span class="text-dim" style="font-size:0.72rem">${opps.length} opportunities</span>
    </div>
    <div class="opp-grid">${cards}</div>
  </div>`;
}

/* ══════════════════════════════════════════════════════════════
   TREES TAB
   ══════════════════════════════════════════════════════════════ */

function renderTreesTab() {
  const runups = state.runups || [];
  if (!runups.length) {
    return `<div class="empty-state">
      <div class="empty-state__icon">&#x1F333;</div>
      <div>No active decision trees. Add RSS feeds and run analysis to generate trees.</div>
    </div>`;
  }

  let cards = "";
  for (const ru of runups) {
    const question = ru.root_question || (ru.narrative_name || "").replace(/-/g, " ");
    const prob = Math.round(ru.root_probability || 0);
    const score = (ru.score || ru.current_score || 0).toFixed(0);
    const tierCls = prob >= 80 ? "tree-card--prob-high" : prob >= 50 ? "tree-card--prob-mid" : "tree-card--prob-low";
    const isFocused = ru.is_focused || false;
    const focusCls = isFocused ? "tree-card--focused" : "";

    cards += `<div class="tree-card ${tierCls} ${focusCls}" data-tree-id="${ru.id}">
      <div class="tree-card__status">
        <span class="tree-card__live"><span class="tree-card__live-dot"></span> ${ru.status === "active" ? "Live" : "Expired"}</span>
        ${isFocused ? `<span class="focus-badge">\u26A1 FOCUS</span>` : ""}
        <span class="tree-card__nodes">${ru.node_count || 1} nodes</span>
      </div>
      <div class="tree-card__prob-display">
        <span class="tree-card__prob-num">${prob}%</span>
        <span class="tree-card__prob-label">probability</span>
      </div>
      <div class="tree-card__question">${esc(question)}</div>
      <div class="tree-card__bottom">
        <div class="tree-card__stats">
          <span>${ru.article_count || 0} articles</span>
          <span>Score ${score}</span>
          ${ru.days_active ? `<span>${ru.days_active}d</span>` : ""}
        </div>
      </div>
    </div>`;
  }

  return `<span class="section-label">Decision Trees (${runups.length})</span>
    <div class="tree-grid">${cards}</div>`;
}

/* ── Tree Fullscreen View ─────────────────────────────────────── */

function renderTreeView() {
  const ru = state.runups.find(r => r.id === activeTreeId);
  const title = ru ? (ru.root_question || ru.narrative_name || "").slice(0, 80) : "Decision Tree";
  const tree = state.activeTree;
  const nodes = tree ? (tree.tree || []) : [];
  const polymatches = state.polymarket || [];

  const isFocused = ru && ru.is_focused;

  let html = `<div class="tree-view">
    <div class="tree-view__header">
      <div style="display:flex;align-items:center;gap:12px">
        <button class="btn btn--sm" data-tree-back>&larr; Back</button>
        <span class="tree-view__title">${esc(title)}</span>
        ${isFocused ? `<span class="focus-badge">\u26A1 FOCUS</span>` : ""}
      </div>
      ${isFocused ? `<div class="focus-actions">
        <button class="btn btn--sm" data-regen-tree="${activeTreeId}">Regenerate Tree</button>
      </div>` : ""}
    </div>`;

  // Polymarket comparison bar
  if (polymatches.length) {
    html += `<div class="poly-comparison-bar">`;
    for (const pm of polymatches.slice(0, 3)) {
      const pmProb = Math.round((pm.outcome_yes_price || 0) * 100);
      const vol = pm.volume ? `$${(pm.volume / 1000).toFixed(0)}K` : "";
      html += `<div class="poly-inline-card">
        <div class="poly-inline-card__question">${esc(truncateText(pm.polymarket_question || "", 60))}</div>
        <span class="poly-inline-card__price">${pmProb}%</span>
        ${vol ? `<span class="poly-inline-card__vol">${vol}</span>` : ""}
        ${pm.polymarket_url ? `<a class="poly-inline-card__link" href="${esc(pm.polymarket_url)}" target="_blank" rel="noopener">View &rarr;</a>` : ""}
      </div>`;
    }
    html += `</div>`;
  }

  // Node cards
  if (!nodes.length) {
    html += `<div class="empty-state"><div class="empty-state__icon">&#x1F333;</div><div>No nodes in this tree yet.</div></div>`;
  } else {
    // Sort: root first, then by depth, then by probability desc
    const sorted = [...nodes].sort((a, b) => {
      if ((a.depth || 0) !== (b.depth || 0)) return (a.depth || 0) - (b.depth || 0);
      return ((b.yes_probability || 0) - (a.yes_probability || 0));
    });

    html += `<div class="node-cards">`;
    for (const node of sorted) {
      html += renderNodeCard(node, polymatches);
    }
    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

function renderNodeCard(node, polymatches) {
  const yesProb = Math.round((node.yes_probability || 0) * 100);
  const isRoot = (node.depth || 0) === 0;
  const status = node.status || "open";

  // Probability tier
  let tierCls = "node-card--low";
  if (yesProb >= 80) tierCls = "node-card--high";
  else if (yesProb >= 50) tierCls = "node-card--mid";

  // Status class
  let statusCls = "";
  if (status === "confirmed_yes") statusCls = "node-card--confirmed-yes";
  if (status === "confirmed_no") statusCls = "node-card--confirmed-no";

  const rootCls = isRoot ? "node-card--root" : "";

  // Status badge
  const statusLabel = status === "confirmed_yes" ? "Confirmed YES" : status === "confirmed_no" ? "Confirmed NO" : "Open";
  const statusBadgeCls = status === "confirmed_yes" ? "node-card__status-badge--confirmed-yes"
    : status === "confirmed_no" ? "node-card__status-badge--confirmed-no"
    : "node-card__status-badge--open";

  let html = `<div class="node-card ${tierCls} ${rootCls} ${statusCls}" data-node-id="${node.id}" data-node-detail>`;

  // Header: depth badge + timeline + status
  html += `<div class="node-card__header">
    <span class="node-card__depth">${isRoot ? "R" : "D" + (node.depth || 0)}</span>
    ${node.timeline_estimate ? `<span class="node-card__timeline">${esc(node.timeline_estimate)}</span>` : ""}
    <span class="node-card__status-badge ${statusBadgeCls}">${statusLabel}</span>
  </div>`;

  // Body: question + probability
  html += `<div class="node-card__body">
    <div class="node-card__question">${esc(node.question || "")}</div>
    <div class="node-card__prob">
      <span class="node-card__prob-num">${yesProb}%</span>
      <div class="node-card__prob-bar"><div class="node-card__prob-fill" style="width:${yesProb}%"></div></div>
    </div>
  </div>`;

  // Swarm verdict (skip failed evaluations)
  const sv = node.swarm_verdict;
  if (sv && sv.confidence > 0 && !(sv.entry_reasoning || "").includes("Synthesis failed")) {
    const vCls = verdictClass(sv.verdict);
    const dir = sv.ticker_direction === "long" ? "\u25B2" : "\u25BC";
    const tCls = sv.ticker_direction === "long" ? "ticker-pill--long" : "ticker-pill--short";
    html += `<div class="node-card__swarm">
      <span class="verdict-badge ${vCls}">${verdictLabel(sv.verdict)}</span>
      <span style="font-family:var(--mono);font-size:0.75rem;color:var(--text-dim)">${sv.confidence}%</span>
      ${sv.primary_ticker ? `<span class="node-card__swarm-ticker"><span class="ticker-pill ${tCls}" data-chart-ticker="${esc(sv.primary_ticker)}">${dir} ${esc(sv.primary_ticker)}</span></span>` : ""}
      ${sv.consensus_strength ? `<span style="font-size:0.7rem;color:var(--text-muted);margin-left:auto">consensus ${sv.consensus_strength}%</span>` : ""}
    </div>`;
  }

  // Polymarket inline comparison (find best match for this node)
  const nodepm = polymatches.find(pm => pm.decision_node_id === node.id);
  if (nodepm && nodepm.outcome_yes_price != null) {
    const marketProb = Math.round(nodepm.outcome_yes_price * 100);
    const edge = yesProb - marketProb;
    const edgeSign = edge >= 0 ? "+" : "";
    const edgeCls = edge >= 0 ? "opp-card__edge--positive" : "opp-card__edge--negative";
    html += `<div class="node-card__polymarket">
      <span class="node-card__pm-val node-card__pm-our">Our: ${yesProb}%</span>
      <span style="color:var(--text-muted)">vs</span>
      <span class="node-card__pm-val node-card__pm-market">Market: ${marketProb}%</span>
      <span class="opp-card__edge ${edgeCls}">${edgeSign}${edge}%</span>
      ${nodepm.polymarket_url ? `<a href="${esc(nodepm.polymarket_url)}" target="_blank" rel="noopener" style="color:var(--purple);font-size:0.72rem;margin-left:auto" onclick="event.stopPropagation()">PM &rarr;</a>` : ""}
    </div>`;
  } else if (isRoot && polymatches.length > 0) {
    // Show first polymarket match on root node
    const pm = polymatches[0];
    const marketProb = Math.round((pm.outcome_yes_price || 0) * 100);
    const edge = yesProb - marketProb;
    const edgeSign = edge >= 0 ? "+" : "";
    const edgeCls = edge >= 0 ? "opp-card__edge--positive" : "opp-card__edge--negative";
    html += `<div class="node-card__polymarket">
      <span class="node-card__pm-val node-card__pm-our">Our: ${yesProb}%</span>
      <span style="color:var(--text-muted)">vs</span>
      <span class="node-card__pm-val node-card__pm-market">Market: ${marketProb}%</span>
      <span class="opp-card__edge ${edgeCls}">${edgeSign}${edge}%</span>
      ${pm.polymarket_url ? `<a href="${esc(pm.polymarket_url)}" target="_blank" rel="noopener" style="color:var(--purple);font-size:0.72rem;margin-left:auto" onclick="event.stopPropagation()">PM &rarr;</a>` : ""}
    </div>`;
  }

  // Collapsible consequences
  const consequences = node.consequences || [];
  const yesCons = consequences.filter(c => c.branch === "yes");
  const noCons = consequences.filter(c => c.branch === "no");

  if (yesCons.length || noCons.length) {
    html += `<div class="node-card__consequences">`;
    for (const [label, cons, branchCls] of [["IF YES", yesCons, "node-card__branch-yes"], ["IF NO", noCons, "node-card__branch-no"]]) {
      if (!cons.length) continue;
      html += `<details class="node-card__branch ${branchCls}">
        <summary>${label} (${cons.length})</summary>`;
      for (const c of cons) {
        const itemCls = c.branch === "yes" ? "node-card__cons-item--yes" : "node-card__cons-item--no";
        html += `<div class="node-card__cons-item ${itemCls}">
          <div>${esc(c.description || "")}</div>`;
        if (c.proximity_display) {
          html += `<div style="font-size:0.72rem;color:var(--orange);margin-top:2px">${esc(c.proximity_display)}</div>`;
        }
        const stocks = c.stock_impacts || [];
        if (stocks.length) {
          html += `<div class="node-card__tickers">`;
          for (const si of stocks) {
            const arrow = si.direction === "bullish" ? "\u25B2" : "\u25BC";
            const cls = si.direction === "bullish" ? "ticker-pill--long" : "ticker-pill--short";
            const dots = si.magnitude === "strong" ? "\u25CF\u25CF\u25CF" : si.magnitude === "moderate" ? "\u25CF\u25CF" : "\u25CF";
            html += `<span class="ticker-pill ${cls}" data-chart-ticker="${esc(si.ticker)}" title="${esc(si.reasoning || "")}">${arrow} ${esc(si.ticker)} <small style="opacity:0.6">${dots}</small></span>`;
          }
          html += `</div>`;
        }
        html += `</div>`;
      }
      html += `</details>`;
    }
    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

function bindTreeViewEvents() {
  document.querySelector("[data-tree-back]")?.addEventListener("click", () => {
    activeTreeId = null;
    stopTreePolling();
    render();
  });
  // Node card click → show detail modal
  document.querySelectorAll("[data-node-detail]").forEach(card => {
    card.addEventListener("click", (e) => {
      // Don't open modal if clicking a link or ticker pill
      if (e.target.closest("a") || e.target.closest("[data-chart-ticker]")) return;
      const nodeId = parseInt(card.dataset.nodeId);
      const tree = state.activeTree;
      if (!tree) return;
      const node = (tree.tree || []).find(n => n.id === nodeId);
      if (!node) return;
      const yesProb = Math.round((node.yes_probability || 0) * 100);
      showNodeDetailModal({
        fullData: node,
        fullQuestion: node.question || "",
        yes_prob: yesProb,
        timeline: node.timeline_estimate || "",
        status: node.status || "open",
      });
    });
  });
  // Focus: Regenerate Tree button
  document.querySelectorAll("[data-regen-tree]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const ruId = parseInt(btn.getAttribute("data-regen-tree"));
      btn.disabled = true;
      btn.textContent = "Regenerating...";
      await regenerateFocusTree(ruId);
    });
  });
}

/* ── Node Detail Modal (replaces side panel) ──────────────────── */

function showNodeDetailModal(nodeData) {
  const fd = nodeData.fullData || {};
  const question = nodeData.fullQuestion || fd.question || "";
  const yesProb = nodeData.yes_prob || 0;
  const timeline = nodeData.timeline || fd.timeline_estimate || "";
  const status = nodeData.status || "open";
  const evidence = fd.evidence || fd.confirmation_evidence || "";

  const statusLabel = status === "confirmed_yes" ? "Confirmed YES" : status === "confirmed_no" ? "Confirmed NO" : "Open";

  let body = `<div class="modal-section">
    <div style="font-size:1.05rem;font-weight:600;line-height:1.4;margin-bottom:12px">${esc(question)}</div>
    <div style="display:flex;gap:12px;font-size:0.82rem;color:var(--text-dim)">
      <span>Likelihood: <strong style="color:var(--text)">${yesProb}%</strong></span>
      ${timeline ? `<span>${esc(timeline)}</span>` : ""}
      <span>${statusLabel}</span>
    </div>
  </div>`;

  if (evidence) {
    body += `<div class="modal-section"><div class="modal-section__label">Evidence</div><div class="modal-section__text">${esc(evidence)}</div></div>`;
  }

  // Consequences
  const consequences = fd.consequences || [];
  const yesCons = consequences.filter(c => c.branch === "yes");
  const noCons = consequences.filter(c => c.branch === "no");

  for (const [label, cons, color] of [["If YES", yesCons, "var(--green)"], ["If NO", noCons, "var(--red)"]]) {
    if (!cons.length) continue;
    body += `<div class="modal-section"><div class="modal-section__label" style="color:${color}">${label}</div>`;
    for (const c of cons) {
      body += `<div style="margin-bottom:8px"><div class="modal-section__text">${esc(c.description || "")}</div>`;
      if (c.proximity_display) {
        body += `<div style="font-size:0.78rem;color:var(--orange);margin-top:2px">${esc(c.proximity_display)}</div>`;
      }
      const stocks = c.stock_impacts || [];
      if (stocks.length) {
        body += `<div style="margin-top:4px">`;
        for (const si of stocks) {
          const arrow = si.direction === "bullish" ? "\u25B2" : "\u25BC";
          const cls = si.direction === "bullish" ? "ticker-pill--long" : "ticker-pill--short";
          body += `<span class="ticker-pill ${cls}" data-chart-ticker="${esc(si.ticker)}" title="${esc(si.reasoning || "")}">${arrow} ${esc(si.ticker)}</span>`;
        }
        body += `</div>`;
      }
      body += `</div>`;
    }
    body += `</div>`;
  }

  // Polymarket
  for (const pm of (state.polymarket || [])) {
    const pmProb = Math.round((pm.outcome_yes_price || 0) * 100);
    const vol = pm.volume ? ` &middot; $${(pm.volume / 1000).toFixed(0)}K vol` : "";
    body += `<div class="modal-section"><div class="modal-section__label" style="color:var(--purple)">Polymarket</div>
      <div class="modal-section__text">${pmProb}%${vol}${pm.polymarket_url ? ` <a href="${esc(pm.polymarket_url)}" target="_blank" rel="noopener" style="color:var(--accent)">View &rarr;</a>` : ""}</div>
    </div>`;
  }

  // Swarm Verdict (skip failed evaluations)
  const sw = fd.swarm_verdict;
  if (sw && sw.confidence > 0 && !(sw.entry_reasoning || "").includes("Synthesis failed")) {
    body += `<div class="modal-section"><div class="modal-section__label" style="color:var(--green)">Swarm Consensus</div>`;
    body += `<div class="ndp-swarm">`;
    body += `<div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
      <span class="verdict-badge ${verdictClass(sw.verdict)}">${verdictLabel(sw.verdict)}</span>
      <span style="font-family:var(--mono);font-size:0.82rem">Conf: ${sw.confidence}%</span>
      <span style="font-size:0.78rem;color:var(--text-dim)">Consensus: ${sw.consensus_strength}%</span>
    </div>`;
    if (sw.primary_ticker) {
      const dir = sw.ticker_direction === "long" ? "\u25B2 LONG" : "\u25BC SHORT";
      const cls = sw.ticker_direction === "long" ? "ticker-pill--long" : "ticker-pill--short";
      body += `<div class="ndp-swarm__ticker">${dir} <span class="ticker-pill ${cls}" data-chart-ticker="${esc(sw.primary_ticker)}">${esc(sw.primary_ticker)}</span></div>`;
    }
    if (sw.entry_reasoning) body += `<div class="ndp-swarm__reasoning"><strong>Entry:</strong> ${esc(sw.entry_reasoning)}</div>`;
    if (sw.exit_trigger) body += `<div class="ndp-swarm__reasoning"><strong>Exit:</strong> ${esc(sw.exit_trigger)}</div>`;
    if (sw.risk_note) body += `<div class="ndp-swarm__risk">\u26A0 ${esc(sw.risk_note)}</div>`;
    if (sw.dissent_note) body += `<div class="ndp-swarm__dissent">\uD83D\uDD04 ${esc(sw.dissent_note)}</div>`;
    const tickers = sw.all_ticker_signals || [];
    if (tickers.length) {
      body += `<div class="ndp-swarm__tickers">`;
      for (const t of tickers) {
        const a = t.direction === "long" ? "\u25B2" : "\u25BC";
        const c = t.direction === "long" ? "ticker-pill--long" : "ticker-pill--short";
        body += `<span class="ticker-pill ${c}" data-chart-ticker="${esc(t.ticker)}">${a} ${esc(t.ticker)} (${t.votes || 0})</span>`;
      }
      body += `</div>`;
    }
    if (sw.created_at) body += `<div class="ndp-swarm__time">${new Date(sw.created_at).toLocaleString()}</div>`;
    body += `</div></div>`;
  }

  openModal("Decision Node", body);
}

function showConsequenceDetailModal(nodeData) {
  const fd = nodeData.fullData || {};
  const branch = nodeData.branch || "yes";

  let body = `<div class="modal-section">
    <div class="modal-section__label" style="color:${branch === "yes" ? "var(--green)" : "var(--red)"}">${branch.toUpperCase()} outcome</div>
    <div style="font-size:0.95rem;font-weight:500;margin-bottom:8px">${esc(fd.description || "")}</div>`;

  if (fd.proximity_display) {
    body += `<div style="font-size:0.82rem;color:var(--orange)">${esc(fd.proximity_display)}</div>`;
  }

  const stocks = fd.stock_impacts || [];
  if (stocks.length) {
    body += `<div style="margin-top:10px">`;
    for (const si of stocks) {
      const arrow = si.direction === "bullish" ? "\u25B2" : "\u25BC";
      const cls = si.direction === "bullish" ? "ticker-pill--long" : "ticker-pill--short";
      body += `<span class="ticker-pill ${cls}" data-chart-ticker="${esc(si.ticker)}" title="${esc(si.reasoning || "")}">${arrow} ${esc(si.ticker)}</span>`;
    }
    body += `</div>`;
  }

  body += `</div>`;
  openModal("Consequence Detail", body);
}

/* ── Also keep old panel functions for backward compat ─────────── */

function showNodeDetailPanel(nd) { showNodeDetailModal(nd); }
function showConsequenceDetailPanel(nd) { showConsequenceDetailModal(nd); }
function closeNodeDetailPanel() { closeModal(); }

/* ══════════════════════════════════════════════════════════════
   INTEL TAB
   ══════════════════════════════════════════════════════════════ */

function renderIntelTab() {
  let html = `<div class="intel-grid">`;

  // Strategic outlook
  html += renderIntelOutlook();

  // Trending keywords
  html += renderIntelTrending();

  // Regional threats
  html += renderIntelRegions();

  // Prediction accuracy
  html += renderIntelPredictions();

  // Latest briefs
  html += renderIntelBriefs();

  html += `</div>`;
  return html;
}

function renderIntelOutlook() {
  const a = state.analysis;
  if (!a || !a.data) {
    return `<div class="intel-card intel-card--full">
      <div class="intel-card__title">Strategic Outlook</div>
      <div class="intel-card__body text-dim">
        No analysis report yet.
        <button class="btn btn--sm" style="margin-left:12px" data-run-analysis>Run Analysis</button>
      </div>
    </div>`;
  }

  const d = a.data;
  let outlook = "";
  if (d.strategic_outlook) {
    outlook = typeof d.strategic_outlook === "string" ? d.strategic_outlook : (d.strategic_outlook.summary || "");
  } else if (d.overview) {
    outlook = d.overview;
  }

  const ts = a.created_at ? ago(new Date(a.created_at).getTime()) : "";

  return `<div class="intel-card intel-card--full">
    <div class="intel-card__title">Strategic Outlook <span class="text-muted" style="font-weight:400;font-size:0.7rem;margin-left:auto">${ts}</span></div>
    <div class="intel-card__body">${esc(truncateText(outlook, 400))}</div>
  </div>`;
}

function renderIntelTrending() {
  const a = state.analysis;
  if (!a || !a.data || !a.data.vocabulary) return "";
  const trending = (a.data.vocabulary.trending_keywords || []).slice(0, 10);
  if (!trending.length) return "";

  let pills = "";
  for (const item of trending) {
    const kw = item.keyword || item[0] || "";
    const ratio = Math.round((item.ratio || item[1] || 0) * 100);
    const cls = ratio > 300 ? "kw-pill--hot" : ratio > 200 ? "kw-pill--warm" : "";
    pills += `<span class="kw-pill ${cls}">${esc(kw)} <small>+${ratio}%</small></span>`;
  }

  return `<div class="intel-card">
    <div class="intel-card__title">Trending Keywords</div>
    <div class="intel-card__body">${pills}</div>
  </div>`;
}

function renderIntelRegions() {
  const a = state.analysis;
  if (!a || !a.data || !a.data.regions) return "";
  const regions = (a.data.regions.regions || []).slice(0, 6);
  if (!regions.length) return "";

  let badges = "";
  for (const r of regions) {
    const tl = r.threat_level || 0;
    const dot = tl >= 0.7 ? "threat-dot--critical" : tl >= 0.4 ? "threat-dot--high" : "threat-dot--low";
    badges += `<span class="threat-badge"><span class="threat-dot ${dot}"></span>${esc(r.region)} <small style="margin-left:4px;opacity:0.6">${(tl * 100).toFixed(0)}%</small></span>`;
  }

  return `<div class="intel-card">
    <div class="intel-card__title">Regional Threats</div>
    <div class="intel-card__body">${badges}</div>
  </div>`;
}

function renderIntelPredictions() {
  const s = state.overview || {};
  const total = s.predictions || 0;
  const correct = s.correct || 0;
  const accuracy = s.accuracy || 0;

  return `<div class="intel-card">
    <div class="intel-card__title">Prediction Accuracy</div>
    <div class="intel-card__body">
      <div class="pred-stats">
        <div><div class="pred-stat__val">${pct(accuracy)}%</div><div class="pred-stat__label">Accuracy</div></div>
        <div><div class="pred-stat__val">${total}</div><div class="pred-stat__label">Total</div></div>
        <div><div class="pred-stat__val text-green">${correct}</div><div class="pred-stat__label">Correct</div></div>
      </div>
    </div>
  </div>`;
}

function renderIntelBriefs() {
  // Show run-ups as news briefs (we don't have direct brief endpoint in overview)
  const runups = state.runups || [];
  if (!runups.length) return "";

  let rows = "";
  for (const ru of runups.slice(0, 8)) {
    const question = ru.root_question || ru.narrative_name || "";
    const prob = Math.round(ru.root_probability || 0);
    const intCls = prob >= 70 ? "brief-row__intensity--critical" : prob >= 50 ? "brief-row__intensity--high-threat" : prob >= 30 ? "brief-row__intensity--moderate" : "brief-row__intensity--low";

    rows += `<div class="brief-row">
      <span class="brief-row__intensity ${intCls}"></span>
      <span class="brief-row__title">${esc(truncateText(question, 80))}</span>
      <span class="brief-row__source">${prob}%</span>
    </div>`;
  }

  return `<div class="intel-card intel-card--full">
    <div class="intel-card__title">Active Narratives</div>
    <div class="intel-card__body">${rows}</div>
  </div>`;
}

/* ══════════════════════════════════════════════════════════════
   USAGE TAB
   ══════════════════════════════════════════════════════════════ */

let _usageLoaded = false;
let _usageDays = 7;

function renderUsageTab() {
  if (!_usageLoaded) {
    _usageLoaded = true;
    fetchUsage(_usageDays).then(render);
  }

  const u = state.usageData;
  if (!u) {
    return `<div class="usage-grid"><div class="empty-state"><div class="empty-state__icon">&#x23F3;</div><div>Laden...</div></div></div>`;
  }

  let html = `<div class="usage-grid">`;

  // Budget summary card (full width)
  html += renderUsageBudgetCard(u);

  // Platform breakdown card
  html += renderUsagePlatformCard(u);

  // Purpose breakdown card
  html += renderUsagePurposeCard(u);

  // Model details card (full width)
  html += renderUsageModelCard(u);

  // Daily history chart (full width)
  html += renderUsageDailyCard(u);

  html += `</div>`;
  return html;
}

function renderUsageBudgetCard(u) {
  const b = u.budget || {};
  const t = u.totals || {};
  const pctUsed = b.pct_used_today || 0;
  const barColor = pctUsed > 80 ? "var(--red)" : pctUsed > 50 ? "var(--yellow)" : "var(--green)";
  const totalTokens = (t.input_tokens || 0) + (t.output_tokens || 0);

  return `<div class="usage-card usage-card--full">
    <div class="usage-card__header">
      <span class="usage-card__icon">&#x1F4B0;</span>
      <span>Budget Overzicht</span>
      <div class="usage-period-selector" style="margin-left:auto">
        <button class="btn btn--xs ${_usageDays === 1 ? "btn--primary" : ""}" data-usage-days="1">1d</button>
        <button class="btn btn--xs ${_usageDays === 7 ? "btn--primary" : ""}" data-usage-days="7">7d</button>
        <button class="btn btn--xs ${_usageDays === 30 ? "btn--primary" : ""}" data-usage-days="30">30d</button>
        <button class="btn btn--xs ${_usageDays === 90 ? "btn--primary" : ""}" data-usage-days="90">90d</button>
      </div>
    </div>
    <div class="usage-card__body">
      <div class="usage-stats-row">
        <div class="usage-stat">
          <div class="usage-stat__label">Dagbudget</div>
          <div class="usage-stat__val">&euro;${b.daily_budget_eur?.toFixed(2) || "1.00"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Vandaag besteed</div>
          <div class="usage-stat__val" style="color:${barColor}">&euro;${b.spent_today_eur?.toFixed(4) || "0.0000"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Resterend</div>
          <div class="usage-stat__val">&euro;${b.remaining_today_eur?.toFixed(4) || "0.0000"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Periode totaal (${u.period_days}d)</div>
          <div class="usage-stat__val">&euro;${t.cost_eur?.toFixed(4) || "0.0000"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">API calls (${u.period_days}d)</div>
          <div class="usage-stat__val">${t.calls || 0}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Tokens (${u.period_days}d)</div>
          <div class="usage-stat__val">${formatTokenCount(totalTokens)}</div>
        </div>
      </div>
      <div class="usage-budget-bar">
        <div class="usage-budget-bar__fill" style="width:${Math.min(pctUsed, 100)}%;background:${barColor}"></div>
        <span class="usage-budget-bar__label">${pctUsed.toFixed(1)}% van dagbudget</span>
      </div>
    </div>
  </div>`;
}

function renderUsagePlatformCard(u) {
  const platforms = u.by_platform || {};
  const platformColors = { Claude: "var(--accent)", Groq: "var(--green)", OpenRouter: "var(--purple)" };
  const totalCost = Object.values(platforms).reduce((s, p) => s + (p.cost_eur || 0), 0) || 1;

  let rows = "";
  for (const [name, data] of Object.entries(platforms).sort((a, b) => b[1].cost_eur - a[1].cost_eur)) {
    const color = platformColors[name] || "var(--text-dim)";
    const pct = ((data.cost_eur / totalCost) * 100).toFixed(1);
    const tokens = (data.input_tokens || 0) + (data.output_tokens || 0);
    const isFree = data.cost_eur === 0;
    rows += `<div class="usage-platform-row">
      <div class="usage-platform-row__name">
        <span class="usage-platform-dot" style="background:${color}"></span>
        ${esc(name)}
      </div>
      <div class="usage-platform-row__stats">
        <span class="usage-platform-row__cost">${isFree ? '<span style="color:var(--green)">GRATIS</span>' : '&euro;' + data.cost_eur.toFixed(4)}</span>
        <span class="usage-platform-row__calls">${data.calls} calls</span>
        <span class="usage-platform-row__tokens">${formatTokenCount(tokens)}</span>
      </div>
      <div class="usage-platform-row__bar">
        <div class="usage-platform-row__bar-fill" style="width:${pct}%;background:${color}"></div>
      </div>
    </div>`;
  }

  return `<div class="usage-card">
    <div class="usage-card__header">
      <span class="usage-card__icon">&#x1F30D;</span>
      <span>Per Platform</span>
    </div>
    <div class="usage-card__body">${rows || '<div class="text-dim">Geen data</div>'}</div>
  </div>`;
}

function renderUsagePurposeCard(u) {
  const purposes = u.by_purpose || {};
  const purposeLabels = {
    tree_generation: "Decision Trees",
    child_tree_generation: "Child Trees",
    daily_advisory: "Daily Advisory",
    strategic_outlook: "Deep Analysis",
    swarm_round1: "Swarm Round 1",
    swarm_round2: "Swarm Round 2",
    swarm_synthesis: "Swarm Synthesis",
    unknown: "Overig",
  };
  const purposeColors = {
    tree_generation: "var(--accent)",
    child_tree_generation: "var(--blue)",
    daily_advisory: "var(--yellow)",
    strategic_outlook: "var(--orange)",
    swarm_round1: "var(--green)",
    swarm_round2: "var(--cyan)",
    swarm_synthesis: "var(--purple)",
    unknown: "var(--text-dim)",
  };

  const totalCalls = Object.values(purposes).reduce((s, p) => s + (p.calls || 0), 0) || 1;

  let rows = "";
  for (const [name, data] of Object.entries(purposes).sort((a, b) => b[1].calls - a[1].calls)) {
    const label = purposeLabels[name] || name;
    const color = purposeColors[name] || "var(--text-dim)";
    const pct = ((data.calls / totalCalls) * 100).toFixed(1);
    rows += `<div class="usage-platform-row">
      <div class="usage-platform-row__name">
        <span class="usage-platform-dot" style="background:${color}"></span>
        ${esc(label)}
      </div>
      <div class="usage-platform-row__stats">
        <span class="usage-platform-row__cost">&euro;${data.cost_eur.toFixed(4)}</span>
        <span class="usage-platform-row__calls">${data.calls} calls</span>
      </div>
      <div class="usage-platform-row__bar">
        <div class="usage-platform-row__bar-fill" style="width:${pct}%;background:${color}"></div>
      </div>
    </div>`;
  }

  return `<div class="usage-card">
    <div class="usage-card__header">
      <span class="usage-card__icon">&#x2699;</span>
      <span>Per Component</span>
    </div>
    <div class="usage-card__body">${rows || '<div class="text-dim">Geen data</div>'}</div>
  </div>`;
}

function renderUsageModelCard(u) {
  const models = u.by_model || {};

  let rows = "";
  for (const [name, data] of Object.entries(models)) {
    const provBadge = data.provider === "Claude"
      ? '<span class="badge badge--blue">Claude</span>'
      : data.provider === "Groq"
      ? '<span class="badge badge--green">Groq</span>'
      : '<span class="badge badge--purple">OpenRouter</span>';
    const shortName = name.replace("OpenRouter/", "").replace("Groq/", "");
    const isFree = data.cost_eur === 0;
    rows += `<tr>
      <td>${provBadge}</td>
      <td class="usage-model-name">${esc(shortName)}</td>
      <td style="text-align:right">${data.calls}</td>
      <td style="text-align:right">${formatTokenCount(data.input_tokens || 0)}</td>
      <td style="text-align:right">${formatTokenCount(data.output_tokens || 0)}</td>
      <td style="text-align:right;font-family:var(--mono)">${isFree ? '<span style="color:var(--green)">gratis</span>' : '&euro;' + data.cost_eur.toFixed(4)}</td>
    </tr>`;
  }

  return `<div class="usage-card usage-card--full">
    <div class="usage-card__header">
      <span class="usage-card__icon">&#x1F916;</span>
      <span>Per Model</span>
    </div>
    <div class="usage-card__body" style="overflow-x:auto">
      <table class="usage-model-table">
        <thead><tr>
          <th>Platform</th><th>Model</th><th style="text-align:right">Calls</th>
          <th style="text-align:right">Input</th><th style="text-align:right">Output</th>
          <th style="text-align:right">Kosten</th>
        </tr></thead>
        <tbody>${rows || '<tr><td colspan="6" class="text-dim">Geen data</td></tr>'}</tbody>
      </table>
    </div>
  </div>`;
}

function renderUsageDailyCard(u) {
  const history = u.daily_history || [];
  if (!history.length) {
    return `<div class="usage-card usage-card--full">
      <div class="usage-card__header"><span class="usage-card__icon">&#x1F4C8;</span><span>Dagelijks Verbruik</span></div>
      <div class="usage-card__body"><div class="text-dim">Geen historie beschikbaar</div></div>
    </div>`;
  }

  // Find max for scaling bars
  const maxCost = Math.max(...history.map(d => d.cost_eur), 0.001);

  let bars = "";
  for (const day of [...history].reverse()) {
    const pct = (day.cost_eur / maxCost * 100).toFixed(1);
    const bp = day.by_platform || {};
    const claudePct = ((bp.Claude || 0) / (day.cost_eur || 1) * 100).toFixed(0);
    const groqPct = ((bp.Groq || 0) / (day.cost_eur || 1) * 100).toFixed(0);
    const orPct = ((bp.OpenRouter || 0) / (day.cost_eur || 1) * 100).toFixed(0);
    const dateLabel = day.date.slice(5); // MM-DD

    bars += `<div class="usage-daily-bar" title="${day.date}: &euro;${day.cost_eur.toFixed(4)} | ${day.calls} calls">
      <div class="usage-daily-bar__stack" style="height:${pct}%">
        ${bp.Claude ? `<div class="usage-daily-bar__seg" style="flex:${claudePct};background:var(--accent)" title="Claude: &euro;${(bp.Claude || 0).toFixed(4)}"></div>` : ""}
        ${bp.Groq ? `<div class="usage-daily-bar__seg" style="flex:${groqPct};background:var(--green)" title="Groq: &euro;${(bp.Groq || 0).toFixed(4)}"></div>` : ""}
        ${bp.OpenRouter ? `<div class="usage-daily-bar__seg" style="flex:${orPct};background:var(--purple)" title="OpenRouter: &euro;${(bp.OpenRouter || 0).toFixed(4)}"></div>` : ""}
      </div>
      <div class="usage-daily-bar__label">${dateLabel}</div>
      <div class="usage-daily-bar__cost">&euro;${day.cost_eur.toFixed(3)}</div>
    </div>`;
  }

  return `<div class="usage-card usage-card--full">
    <div class="usage-card__header">
      <span class="usage-card__icon">&#x1F4C8;</span>
      <span>Dagelijks Verbruik</span>
      <div class="usage-legend" style="margin-left:auto;display:flex;gap:12px;font-size:0.72rem">
        <span><span class="usage-platform-dot" style="background:var(--accent)"></span> Claude</span>
        <span><span class="usage-platform-dot" style="background:var(--green)"></span> Groq</span>
        <span><span class="usage-platform-dot" style="background:var(--purple)"></span> OpenRouter</span>
      </div>
    </div>
    <div class="usage-card__body">
      <div class="usage-daily-chart">${bars}</div>
    </div>
  </div>`;
}

function formatTokenCount(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + "M";
  if (n >= 1000) return (n / 1000).toFixed(1) + "K";
  return String(n);
}

function bindUsageTabEvents() {
  document.querySelectorAll("[data-usage-days]").forEach(btn => {
    btn.addEventListener("click", () => {
      _usageDays = parseInt(btn.dataset.usageDays, 10);
      state.usageData = null;
      fetchUsage(_usageDays).then(render);
    });
  });
}

/* ══════════════════════════════════════════════════════════════
   SETTINGS TAB
   ══════════════════════════════════════════════════════════════ */

let _settingsLoaded = false;
function renderSettingsTab() {
  // Auto-fetch settings data if not yet loaded
  if (!_settingsLoaded) {
    _settingsLoaded = true;
    Promise.all([fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus(), fetchTelegramStatus()]).then(render);
  }

  let html = `<div class="settings-grid">`;

  // Focus Mode (full-width, first)
  html += renderSettingsFocus();

  // API Keys
  html += renderSettingsKeys();

  // Budget
  html += renderSettingsBudget();

  // Swarm
  html += renderSettingsSwarm();

  // Telegram
  html += renderSettingsTelegram();

  // Feeds
  html += renderSettingsFeeds();

  html += `</div>`;
  return html;
}

function renderSettingsTelegram() {
  const tg = state.telegramStatus || {};
  const configured = tg.configured;
  const statusIcon = configured ? "🟢" : "⚪";
  const statusText = configured ? "Actief" : "Niet geconfigureerd";

  return `<div class="card">
    <div class="card__header">
      <span class="card__icon">📱</span>
      <span>Telegram Notificaties</span>
      <span class="badge ${configured ? "badge--green" : "badge--dim"}" style="margin-left:auto">${statusIcon} ${statusText}</span>
    </div>
    <div class="card__body">
      <p style="font-size:0.78rem;color:var(--text-dim);margin-bottom:12px">
        Ontvang de dagelijkse advisory om 07:30 UTC direct in Telegram.
        Maak een bot via <a href="https://t.me/BotFather" target="_blank" style="color:var(--accent)">@BotFather</a>.
      </p>
      <div style="display:flex;flex-direction:column;gap:8px">
        <input id="tg-token" type="password" class="input" placeholder="Bot Token (van @BotFather)" value="${tg.token_set ? '••••••••••' : ''}" />
        <input id="tg-chatid" type="text" class="input" placeholder="Chat ID" value="${tg.chat_id_set ? '••••••' : ''}" />
        <div style="display:flex;gap:8px">
          <button class="btn btn--primary btn--sm" data-save-telegram>Opslaan</button>
          <button class="btn btn--sm" data-test-telegram ${!configured ? 'disabled' : ''}>Test</button>
          <button class="btn btn--sm" data-send-telegram-advisory ${!configured ? 'disabled' : ''}>Stuur Advisory</button>
        </div>
      </div>
    </div>
  </div>`;
}

async function fetchTelegramStatus() {
  try {
    const res = await fetch(API_BASE + "?_api=telegram-status");
    if (res.ok) state.telegramStatus = await res.json();
  } catch { /* ignore */ }
}

function renderSettingsFocus() {
  const focus = state.focus || { focused_runup_ids: [] };
  const focusedIds = new Set(focus.focused_runup_ids || []);
  const allRunups = state.runups || [];

  // Show ALL non-merged active run-ups as candidates (from overview data)
  // The overview only returns run-ups with trees, so we also show focused_runups from focus endpoint
  const focusedRunups = (focus.focused_runups || []);
  const overviewIds = new Set(allRunups.map(r => r.id));

  // Merge: overview run-ups + any focused run-ups not in overview
  let candidates = [...allRunups];
  for (const fr of focusedRunups) {
    if (!overviewIds.has(fr.id)) {
      candidates.push({
        id: fr.id,
        narrative_name: fr.narrative_name,
        current_score: fr.score,
        article_count: fr.article_count,
        status: fr.status,
        is_focused: true,
      });
    }
  }
  candidates.sort((a, b) => {
    // Focused first, then by score
    const af = focusedIds.has(a.id) ? 0 : 1;
    const bf = focusedIds.has(b.id) ? 0 : 1;
    if (af !== bf) return af - bf;
    return (b.current_score || b.score || 0) - (a.current_score || a.score || 0);
  });

  let html = `<div class="settings-card settings-card--full">
    <div class="settings-card__title">
      <span>Focus Mode</span>
      ${focusedIds.size > 0 ? `<button class="btn btn--sm" data-clear-focus>Clear All</button>` : ""}
    </div>
    <div style="font-size:0.82rem;color:var(--text-dim);margin-bottom:12px">
      Select 1\u20133 narratives to concentrate analysis. Focused narratives get cross-region merging, priority tree generation, 2\u00D7 swarm evaluation, and hourly price tracking.
    </div>`;

  if (candidates.length === 0) {
    html += `<div style="font-size:0.82rem;color:var(--text-muted);padding:12px 0">No run-ups available. The engine is still building narratives.</div>`;
  } else {
    html += `<div class="focus-list">`;
    for (const ru of candidates) {
      const isFocused = focusedIds.has(ru.id);
      const score = ru.current_score || ru.score || 0;
      const name = ru.narrative_name || ru.name || "";
      const articles = ru.article_count || ru.article_count_total || 0;
      html += `<div class="focus-row">
        <span class="focus-row__name" title="${esc(name)}">${esc(name)}</span>
        <span class="focus-row__meta">${Math.round(score)} pts \u00B7 ${articles} art.</span>
        <button class="btn btn--sm focus-row__btn ${isFocused ? "btn--accent" : ""}" data-toggle-focus="${ru.id}">
          ${isFocused ? "\u26A1 Focused" : "Focus"}
        </button>
      </div>`;
    }
    html += `</div>`;
  }

  // Polymarket linking for focused run-ups
  if (focusedIds.size > 0) {
    html += `<div class="focus-pm-form">
      <div class="focus-pm-form__label">Link Polymarket</div>
      <div class="focus-pm-form__row">
        <select id="focus-pm-runup">
          ${Array.from(focusedIds).map(id => {
            const ru = candidates.find(r => r.id === id);
            const name = ru ? (ru.narrative_name || ru.name || "") : "Run-up #" + id;
            return `<option value="${id}">${esc(name)}</option>`;
          }).join("")}
        </select>
        <input id="focus-pm-url" placeholder="https://polymarket.com/event/..." />
        <button class="btn btn--sm" data-add-pm-link>Link</button>
      </div>
    </div>`;
  }

  html += `</div>`;
  return html;
}

function renderSettingsKeys() {
  const ak = state.apiKeyStatus || {};
  const hasKey = ak.has_key ? "Configured" : "Not set";
  const keyClass = ak.has_key ? "settings-row__val--ok" : "settings-row__val--warn";

  return `<div class="settings-card">
    <div class="settings-card__title">API Keys</div>
    <div class="settings-row">
      <span class="settings-row__label">Anthropic (Claude)</span>
      <span class="settings-row__val ${keyClass}">${hasKey}</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Groq</span>
      <span class="settings-row__val ${state.swarmStatus?.groq_configured ? "settings-row__val--ok" : "settings-row__val--warn"}">${state.swarmStatus?.groq_configured ? "Configured" : "Not set"}</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">OpenRouter</span>
      <span class="settings-row__val ${state.swarmStatus?.openrouter_configured ? "settings-row__val--ok" : "settings-row__val--warn"}">${state.swarmStatus?.openrouter_configured ? "Configured" : "Not set"}</span>
    </div>
  </div>`;
}

function renderSettingsBudget() {
  const b = state.budget || {};
  const daily = b.daily_budget_eur || 0.33;
  const spent = b.spent_today_eur || 0;
  const pctUsed = daily > 0 ? Math.min(100, (spent / daily) * 100) : 0;
  const fillCls = pctUsed > 80 ? "budget-bar__fill--danger" : pctUsed > 50 ? "budget-bar__fill--warn" : "";

  return `<div class="settings-card">
    <div class="settings-card__title">Token Budget</div>
    <div class="settings-row">
      <span class="settings-row__label">Daily limit</span>
      <span class="settings-row__val">\u20AC${daily.toFixed(2)}</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Spent today</span>
      <span class="settings-row__val">\u20AC${spent.toFixed(3)} (${pctUsed.toFixed(0)}%)</span>
    </div>
    <div class="budget-bar"><div class="budget-bar__fill ${fillCls}" style="width:${pctUsed}%"></div></div>
    <div class="settings-row">
      <span class="settings-row__label">Monthly est.</span>
      <span class="settings-row__val">\u20AC${(spent * 30).toFixed(2)}</span>
    </div>
  </div>`;
}

function renderSettingsSwarm() {
  const sw = state.swarmStatus || {};
  return `<div class="settings-card">
    <div class="settings-card__title">Swarm Consensus
      <button class="btn btn--sm" data-run-swarm>Run Cycle</button>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Status</span>
      <span class="settings-row__val ${sw.enabled ? "settings-row__val--ok" : "settings-row__val--warn"}">${sw.enabled ? "Active" : "Disabled"}</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Active verdicts</span>
      <span class="settings-row__val">${sw.active_verdicts || 0}</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Interval</span>
      <span class="settings-row__val">${sw.interval_minutes || 60} min</span>
    </div>
    ${sw.verdicts_by_type ? `<div style="display:flex;gap:8px;margin-top:8px">
      ${Object.entries(sw.verdicts_by_type).map(([k, v]) => `<span class="verdict-badge ${verdictClass(k)}">${k}: ${v}</span>`).join("")}
    </div>` : ""}
  </div>`;
}

function renderSettingsFeeds() {
  const feeds = state.feeds || [];
  const active = feeds.filter(f => f.enabled !== false).length;

  return `<div class="settings-card settings-card--full">
    <div class="settings-card__title">RSS Feeds (${active} active)
      <button class="btn btn--sm" data-show-feeds>Manage</button>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Total feeds</span>
      <span class="settings-row__val">${feeds.length}</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Articles fetched</span>
      <span class="settings-row__val">${state.status.total_articles || 0}</span>
    </div>
  </div>`;
}

/* ══════════════════════════════════════════════════════════════
   MODAL SYSTEM
   ══════════════════════════════════════════════════════════════ */

function openModal(title, bodyHtml, wide) {
  closeModal();
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.id = "modal-overlay";
  overlay.innerHTML = `<div class="modal${wide ? " modal--wide" : ""}">
    <div class="modal__header">
      <span class="modal__title">${esc(title)}</span>
      <button class="modal__close" data-modal-close>&times;</button>
    </div>
    <div class="modal__body">${bodyHtml}</div>
  </div>`;
  document.body.appendChild(overlay);

  overlay.querySelector("[data-modal-close]").addEventListener("click", closeModal);
  overlay.addEventListener("click", e => { if (e.target === overlay) closeModal(); });

  // Bind ticker clicks inside modal
  overlay.querySelectorAll("[data-chart-ticker]").forEach(el => {
    el.addEventListener("click", () => {
      const t = el.getAttribute("data-chart-ticker");
      if (t) openPriceChart(t);
    });
  });

  // Escape key
  const handler = e => { if (e.key === "Escape") { closeModal(); document.removeEventListener("keydown", handler); }};
  document.addEventListener("keydown", handler);
}

function closeModal() {
  const el = document.getElementById("modal-overlay");
  if (el) el.remove();
}

/* ══════════════════════════════════════════════════════════════
   PRICE CHART MODAL
   ══════════════════════════════════════════════════════════════ */

function openPriceChart(ticker) {
  if (!ticker) return;
  _priceModalTicker = ticker;
  _renderPriceModal(ticker, "3mo");
}

function closePriceChart() {
  _priceModalTicker = null;
  const modal = document.getElementById("price-modal");
  if (modal) modal.remove();
}

async function _renderPriceModal(ticker, period) {
  let modal = document.getElementById("price-modal");
  if (modal) modal.remove();

  modal = document.createElement("div");
  modal.id = "price-modal";
  modal.className = "price-modal";
  modal.innerHTML = `<div class="price-modal__box">
    <div class="price-modal__header">
      <span class="price-modal__title">${esc(ticker)}</span>
      <button class="price-modal__close" id="pm-close">&times;</button>
    </div>
    <div class="price-modal__body">
      <div class="price-modal__quote" id="pm-quote">Loading...</div>
      <div class="price-modal__periods">
        ${["1mo","3mo","6mo","1y"].map(p =>
          `<button class="period-btn${p === period ? " period-btn--active" : ""}" data-pm-period="${p}">${p.toUpperCase()}</button>`
        ).join("")}
      </div>
      <div class="price-modal__chart" id="pm-chart"></div>
    </div>
  </div>`;
  document.body.appendChild(modal);

  document.getElementById("pm-close").addEventListener("click", closePriceChart);
  modal.addEventListener("click", e => { if (e.target === modal) closePriceChart(); });
  const escH = e => { if (e.key === "Escape") { closePriceChart(); document.removeEventListener("keydown", escH); }};
  document.addEventListener("keydown", escH);

  modal.querySelectorAll("[data-pm-period]").forEach(btn => {
    btn.addEventListener("click", () => _renderPriceModal(ticker, btn.dataset.pmPeriod));
  });

  try {
    const [quoteRes, chartRes] = await Promise.all([
      fetch(`${API_BASE}?_api=price&ticker=${encodeURIComponent(ticker)}`),
      fetch(`${API_BASE}?_api=price-chart&ticker=${encodeURIComponent(ticker)}&period=${period}`),
    ]);
    const quote = quoteRes.ok ? await quoteRes.json() : {};
    const candles = chartRes.ok ? await chartRes.json() : [];

    const quoteEl = document.getElementById("pm-quote");
    if (quoteEl && quote.price != null) {
      const chg = quote.change_pct || 0;
      const cls = chg >= 0 ? "price-modal__change--up" : "price-modal__change--down";
      quoteEl.innerHTML = `<span class="price-modal__price">$${quote.price.toLocaleString("en-US", {minimumFractionDigits: 2})}</span>
        <span class="price-modal__change ${cls}">${chg >= 0 ? "+" : ""}${chg.toFixed(2)}%</span>
        <span class="text-dim" style="font-size:0.82rem">${esc(quote.name || "")}</span>`;
    }

    const chartContainer = document.getElementById("pm-chart");
    if (chartContainer && candles.length > 0 && typeof LightweightCharts !== "undefined") {
      chartContainer.innerHTML = "";
      const chart = LightweightCharts.createChart(chartContainer, {
        width: chartContainer.clientWidth, height: 300,
        layout: { background: { color: "#181b24" }, textColor: "#e4e4ea" },
        grid: { vertLines: { color: "#262a36" }, horzLines: { color: "#262a36" } },
        crosshair: { mode: 0 },
        timeScale: { borderColor: "#262a36", timeVisible: false },
        rightPriceScale: { borderColor: "#262a36" },
      });
      const cs = chart.addCandlestickSeries({
        upColor: "#34d399", downColor: "#f87171",
        borderUpColor: "#34d399", borderDownColor: "#f87171",
        wickUpColor: "#34d399", wickDownColor: "#f87171",
      });
      cs.setData(candles);
      if (candles[0]?.volume != null) {
        const vs = chart.addHistogramSeries({ priceFormat: { type: "volume" }, priceScaleId: "vol" });
        chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.85, bottom: 0 }});
        vs.setData(candles.map(c => ({ time: c.time, value: c.volume, color: c.close >= c.open ? "rgba(52,211,153,0.3)" : "rgba(248,113,113,0.3)" })));
      }
      chart.timeScale().fitContent();
      new ResizeObserver(() => chart.applyOptions({ width: chartContainer.clientWidth })).observe(chartContainer);
    } else if (chartContainer) {
      chartContainer.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-dim)">No chart data</div>`;
    }
  } catch {
    const c = document.getElementById("pm-chart");
    if (c) c.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-dim)">Failed to load chart</div>`;
  }
}

/* ══════════════════════════════════════════════════════════════
   PORTFOLIO TAB
   ══════════════════════════════════════════════════════════════ */

function renderPortfolioTab() {
  const adv = state.advisory;
  if (!adv || !adv.advisory) {
    return `<div class="advisory-empty">
      <div class="advisory-empty__icon">📊</div>
      <div class="advisory-empty__text">No advisory generated yet.<br>Generate your first daily advisory to see BUY/SELL recommendations.</div>
      <button class="btn btn--primary" data-generate-advisory>Generate Advisory</button>
    </div>`;
  }

  const data = adv.advisory;
  const buys = data.buy_recommendations || [];
  const sells = data.sell_recommendations || [];
  const stance = data.market_stance || "neutral";
  const ctx = data.market_context || {};
  const genAt = data.generated_at ? new Date(data.generated_at).toLocaleString() : "—";

  let html = "";

  // Header
  html += `<div class="advisory-header">
    <div>
      <div class="advisory-header__title">
        📊 Daily Advisory — ${data.generated_at ? data.generated_at.slice(0, 10) : "Today"}
        <span class="stance-badge stance-badge--${stance}">${_stanceLabel(stance)}</span>
      </div>
      <div class="advisory-header__meta">Generated ${genAt}</div>
    </div>
    <button class="btn btn--sm" data-generate-advisory>Regenerate</button>
  </div>`;

  // Market context bar
  const fg = ctx.fear_greed || {};
  const vix = ctx.vix || {};
  const oil = ctx.oil || {};
  const gold = ctx.gold || {};
  html += `<div class="market-context-bar">
    ${fg.score != null ? `<div class="market-context-bar__item"><span class="market-context-bar__label">Fear & Greed:</span><span class="market-context-bar__value">${fg.score} (${fg.label || "—"})</span></div>` : ""}
    ${vix.price != null ? `<div class="market-context-bar__item"><span class="market-context-bar__label">VIX:</span><span class="market-context-bar__value">${formatNum(vix.price, 1)}</span></div>` : ""}
    ${oil.price != null ? `<div class="market-context-bar__item"><span class="market-context-bar__label">Oil:</span><span class="market-context-bar__value">$${formatNum(oil.price, 1)} ${_chgBadge(oil.change_pct)}</span></div>` : ""}
    ${gold.price != null ? `<div class="market-context-bar__item"><span class="market-context-bar__label">Gold:</span><span class="market-context-bar__value">$${formatNum(gold.price, 0)} ${_chgBadge(gold.change_pct)}</span></div>` : ""}
  </div>`;

  // Narrative summary
  if (data.narrative_summary) {
    html += `<div class="narrative-summary">${_esc(data.narrative_summary)}</div>`;
  }

  // Risk banner
  if (data.risk_warning) {
    html += `<div class="risk-banner">
      <span class="risk-banner__icon">⚠️</span>
      <span>${_esc(data.risk_warning)}</span>
    </div>`;
  }

  // ── MY PORTFOLIO (alignment with advisory) ──────────────────
  html += renderMyPortfolio();

  // BUY recommendations
  if (buys.length > 0) {
    html += `<div class="advisory-section-label">🟢 BUY Recommendations (${buys.length})</div>`;
    html += `<div class="advisory-grid">`;
    for (const rec of buys) {
      html += renderAdvisoryCard(rec, "buy");
    }
    html += `</div>`;
  }

  // SELL recommendations
  if (sells.length > 0) {
    html += `<div class="advisory-section-label">🔴 SELL Signals (${sells.length})</div>`;
    html += `<div class="advisory-grid">`;
    for (const rec of sells) {
      html += renderAdvisoryCard(rec, "sell");
    }
    html += `</div>`;
  }

  // Hold watchlist
  const watch = data.hold_watchlist || [];
  if (watch.length > 0) {
    html += `<div class="advisory-section-label">👀 Watchlist</div>`;
    html += `<div class="advisory-grid">`;
    for (const w of watch) {
      html += `<div class="advisory-card" style="border-left:3px solid var(--yellow)">
        <div class="advisory-card__top">
          <span class="advisory-card__ticker" style="color:var(--yellow)" data-chart-ticker="${_esc(w.ticker)}">${_esc(w.ticker)}</span>
        </div>
        <div class="advisory-card__name">${_esc(w.name)}</div>
        <div class="advisory-card__reasoning">${_esc(w.note)}</div>
      </div>`;
    }
    html += `</div>`;
  }

  // Sectors outlook
  const sectors = data.sectors_outlook || [];
  if (sectors.length > 0) {
    html += `<div class="advisory-section-label">🏭 Sectors Outlook</div>`;
    html += `<div class="sectors-grid">`;
    for (const s of sectors) {
      const dir = s.direction || "neutral";
      html += `<div class="sector-chip">
        <span class="sector-chip__dir sector-chip__dir--${dir}">${dir === "bullish" ? "▲" : dir === "bearish" ? "▼" : "●"}</span>
        <span>${_esc(s.sector)}</span>
      </div>`;
    }
    html += `</div>`;
  }

  // Track record
  html += renderTrackRecord();

  // Learning stats
  html += renderLearningStats();

  return html;
}

function renderMyPortfolio() {
  const align = state.portfolioAlignment;
  // Also check for holdings in advisory response
  const holdings = (state.advisory || {}).portfolio_holdings || [];

  if ((!align || !align.alignment || align.alignment.length === 0) && holdings.length === 0) {
    return `<div class="portfolio-holdings portfolio-holdings--empty">
      <div class="advisory-section-label">💼 Mijn Portfolio</div>
      <div class="portfolio-holdings__empty-msg">
        Geen portfolio geconfigureerd. Voeg je Bunq posities toe om gepersonaliseerd advies te krijgen.
      </div>
      <button class="btn btn--sm" data-edit-portfolio>Portfolio instellen</button>
    </div>`;
  }

  const items = (align && align.alignment) ? align.alignment : [];
  const missed = (align && align.missed_opportunities) ? align.missed_opportunities : [];
  const totalValue = items.reduce((sum, h) => sum + (h.value_eur || 0), 0);
  const totalPnl = items.reduce((sum, h) => sum + (h.pnl_eur || 0), 0);

  let html = `<div class="portfolio-holdings">
    <div class="advisory-section-label">
      💼 Mijn Portfolio
      <span class="portfolio-total">${totalValue > 0 ? `€${formatNum(totalValue, 2)}` : ""}</span>
      <button class="btn btn--xs" data-edit-portfolio style="margin-left:auto">Bewerken</button>
    </div>`;

  // P&L Summary Card
  if (items.some(h => h.pnl_eur != null && h.pnl_eur !== 0)) {
    const pnlCls = totalPnl >= 0 ? "pnl-card--profit" : "pnl-card--loss";
    const pnlSign = totalPnl >= 0 ? "+" : "";
    html += `<div class="pnl-card ${pnlCls}">
      <div class="pnl-card__header">
        <span class="pnl-card__icon">${totalPnl >= 0 ? "📈" : "📉"}</span>
        <span class="pnl-card__label">Totaal P&L</span>
        <span class="pnl-card__total">${pnlSign}€${formatNum(totalPnl, 2)}</span>
      </div>
      <div class="pnl-card__breakdown">`;
    for (const h of items) {
      if (h.pnl_eur == null || h.pnl_eur === 0) continue;
      const itemCls = h.pnl_eur >= 0 ? "pnl-item--profit" : "pnl-item--loss";
      const itemSign = h.pnl_eur >= 0 ? "+" : "";
      html += `<div class="pnl-item ${itemCls}">
        <span class="pnl-item__ticker" data-chart-ticker="${_esc(h.ticker)}">${_esc(h.ticker)}</span>
        <span class="pnl-item__value">${itemSign}€${formatNum(h.pnl_eur, 2)}</span>
      </div>`;
    }
    html += `</div></div>`;
  }

  // Alignment cards
  if (items.length > 0) {
    html += `<div class="portfolio-grid">`;
    for (const h of items) {
      const signal = h.signal || "NEUTRAL";
      const signalCls = {
        HOLD_ADD: "hold-add", REDUCE: "reduce", HOLD: "hold",
        WATCH: "watch", NEUTRAL: "neutral",
      }[signal] || "neutral";

      const hPnl = h.pnl_eur || 0;
      const hPnlCls = hPnl >= 0 ? "portfolio-card__pnl--profit" : "portfolio-card__pnl--loss";
      const hPnlSign = hPnl >= 0 ? "+" : "";

      html += `<div class="portfolio-card portfolio-card--${signalCls}">
        <div class="portfolio-card__top">
          <span class="portfolio-card__ticker" data-chart-ticker="${_esc(h.ticker)}">${_esc(h.ticker)}</span>
          <span class="portfolio-card__value">€${formatNum(h.value_eur || 0, 2)}</span>
        </div>
        ${hPnl !== 0 ? `<div class="portfolio-card__pnl ${hPnlCls}">${hPnlSign}€${formatNum(hPnl, 2)}</div>` : ""}
        <div class="portfolio-card__name">${_esc(h.name)}</div>
        <div class="portfolio-card__signal portfolio-card__signal--${signalCls}">
          ${_esc(h.label)}
        </div>
        ${h.reasoning ? `<div class="portfolio-card__reasoning">${_esc(h.reasoning)}</div>` : ""}
        ${h.composite_score != null ? `<div class="portfolio-card__score">Score: ${Number(h.composite_score).toFixed(2)}</div>` : ""}
      </div>`;
    }
    html += `</div>`;
  }

  // Missed opportunities
  if (missed.length > 0) {
    html += `<div class="portfolio-missed">
      <div class="portfolio-missed__title">💡 Gemiste kansen — niet in portfolio</div>
      <div class="portfolio-missed__list">`;
    for (const m of missed) {
      html += `<span class="portfolio-missed__item" data-chart-ticker="${_esc(m.ticker)}">
        <strong>${_esc(m.ticker)}</strong>
        ${m.current_price != null ? `$${formatNum(m.current_price, 2)}` : ""}
        <span class="portfolio-missed__score">Score ${Number(m.composite_score || 0).toFixed(2)}</span>
      </span>`;
    }
    html += `</div></div>`;
  }

  html += `</div>`;
  return html;
}

function renderAdvisoryCard(rec, type) {
  const ticker = rec.ticker || "?";
  const name = rec.name || "";
  const price = rec.current_price;
  const score = rec.composite_score || 0;
  const components = rec.components || {};
  const reasoning = rec.reasoning || "";
  const hasReasoning = reasoning && (typeof reasoning === "object" ? reasoning.thesis : reasoning);
  const risk = rec.risk_levels || {};
  const sizing = rec.position_sizing || {};

  // Store reasoning + risk data for popup
  const popupData = { reasoning, risk_levels: risk, position_sizing: sizing };
  const dataAttr = hasReasoning || risk.stop_loss
    ? `data-reasoning='${_esc(JSON.stringify(popupData)).replace(/'/g, "&#39;")}'`
    : "";

  let html = `<div class="advisory-card advisory-card--${type} ${(hasReasoning || risk.stop_loss) ? "advisory-card--clickable" : ""}" ${dataAttr} data-ticker="${_esc(ticker)}" data-name="${_esc(name)}" data-type="${type}">`;
  html += `<div class="advisory-card__top">
    <span class="advisory-card__ticker advisory-card__ticker--${type}">${_esc(ticker)}</span>
    ${price != null ? `<span class="advisory-card__price">$${formatNum(price, 2)}</span>` : ""}
  </div>`;
  html += `<div class="advisory-card__name">${_esc(name)}</div>`;
  html += `<div class="advisory-card__score advisory-card__score--${type}">
    Score: ${score.toFixed(2)}
    ${rec.signal_level ? `<span style="font-size:0.68rem;color:var(--text-dim);margin-left:6px">${_esc(rec.signal_level)}</span>` : ""}
    ${rec.swarm_verdict ? `<span class="badge--swarm" style="margin-left:4px">${_esc(rec.swarm_verdict)}</span>` : ""}
  </div>`;

  // Risk levels mini-bar (SL / Entry / TP)
  if (risk.stop_loss && risk.take_profit && price) {
    const isBuy = type === "buy";
    const slLabel = isBuy ? "SL" : "SL";
    html += `<div class="risk-levels-mini">
      <span class="risk-mini risk-mini--sl" title="Stop-Loss">${slLabel} $${formatNum(risk.stop_loss, 2)}</span>
      <span class="risk-mini risk-mini--entry">▸ $${formatNum(price, 2)}</span>
      <span class="risk-mini risk-mini--tp" title="Take-Profit">TP $${formatNum(risk.take_profit, 2)}</span>
      <span class="risk-mini risk-mini--rr" title="Reward:Risk">${risk.reward_risk || "—"}R</span>
    </div>`;
  }

  // Position sizing badge
  if (sizing.position_pct) {
    html += `<div class="sizing-badge">
      <span class="sizing-badge__pct">${sizing.position_pct}%</span>
      <span class="sizing-badge__label">portfolio</span>
      ${sizing.shares ? `<span class="sizing-badge__shares">${sizing.shares} shares</span>` : ""}
      ${sizing.eur_amount ? `<span class="sizing-badge__eur">€${formatNum(sizing.eur_amount, 0)}</span>` : ""}
    </div>`;
  }

  // Component bars
  const compMap = [
    ["geo", "geopolitical", "--geo"],
    ["conf", "confidence", "--conf"],
    ["swarm", "swarm", "--swarm"],
    ["mom", "momentum", "--mom"],
    ["ins", "insider", "--ins"],
  ];
  html += `<div class="component-bars">`;
  for (const [label, key, cls] of compMap) {
    const val = components[key] || 0;
    const pct = Math.min(100, Math.round(val * 100));
    html += `<div class="component-bar">
      <span class="component-bar__label">${label}</span>
      <div class="component-bar__track"><div class="component-bar__fill component-bar__fill${cls}" style="width:${pct}%"></div></div>
      <span class="component-bar__val">${pct}%</span>
    </div>`;
  }
  html += `</div>`;

  // Short thesis teaser (1 line) + tap hint
  if (hasReasoning) {
    const teaser = typeof reasoning === "object"
      ? (reasoning.thesis || "").slice(0, 80)
      : String(reasoning).slice(0, 80);
    html += `<div class="advisory-card__teaser">${_esc(teaser)}${teaser.length >= 80 ? "…" : ""} <span class="advisory-card__tap-hint">tap voor detail</span></div>`;
  }

  html += `</div>`;
  return html;
}

function _showAdvisoryDetail(card) {
  let popupData;
  try {
    popupData = JSON.parse(card.dataset.reasoning);
  } catch { return; }

  // Support both old format (reasoning only) and new format (with risk_levels/position_sizing)
  const reasoning = popupData.reasoning || popupData;
  const risk = popupData.risk_levels || {};
  const sizing = popupData.position_sizing || {};

  const ticker = card.dataset.ticker || "?";
  const name = card.dataset.name || "";
  const type = card.dataset.type || "buy";
  const isBuy = type === "buy";
  const headerColor = isBuy ? "var(--bull)" : "var(--bear)";
  const actionLabel = isBuy ? "BUY" : "SELL";

  let body = "";

  // Risk levels section (top of modal — most actionable)
  if (risk.stop_loss) {
    body += `<div class="advisory-detail__section advisory-detail__section--levels">
      <div class="advisory-detail__label">📐 Risico Niveaus</div>
      <div class="risk-levels-detail">
        <div class="risk-level risk-level--sl">
          <span class="risk-level__label">Stop-Loss</span>
          <span class="risk-level__price">$${formatNum(risk.stop_loss, 2)}</span>
          <span class="risk-level__pct risk-level__pct--neg">-${risk.risk_pct || "?"}%</span>
        </div>
        ${risk.trailing_stop ? `<div class="risk-level risk-level--ts">
          <span class="risk-level__label">Trailing Stop</span>
          <span class="risk-level__price">$${formatNum(risk.trailing_stop, 2)}</span>
        </div>` : ""}
        <div class="risk-level risk-level--tp">
          <span class="risk-level__label">Take-Profit</span>
          <span class="risk-level__price">$${formatNum(risk.take_profit, 2)}</span>
          <span class="risk-level__pct risk-level__pct--pos">+${risk.reward_pct || "?"}%</span>
        </div>
        <div class="risk-level risk-level--rr">
          <span class="risk-level__label">R:R Ratio</span>
          <span class="risk-level__value">${risk.reward_risk || "—"}:1</span>
        </div>
        ${risk.atr ? `<div class="risk-level risk-level--atr">
          <span class="risk-level__label">ATR (14d)</span>
          <span class="risk-level__value">$${formatNum(risk.atr, 2)} (${risk.atr_pct}%)</span>
        </div>` : ""}
      </div>
    </div>`;
  }

  // Position sizing section
  if (sizing.position_pct) {
    body += `<div class="advisory-detail__section advisory-detail__section--sizing">
      <div class="advisory-detail__label">💰 Positie Grootte (Half-Kelly)</div>
      <div class="sizing-detail">
        <div class="sizing-detail__row">
          <span>Allocatie:</span>
          <strong>${sizing.position_pct}% van portfolio</strong>
        </div>
        <div class="sizing-detail__row">
          <span>Bedrag:</span>
          <strong>€${formatNum(sizing.eur_amount, 2)}</strong>
          ${sizing.shares ? `<span class="sizing-detail__shares">(${sizing.shares} shares)</span>` : ""}
        </div>
        <div class="sizing-detail__row sizing-detail__row--dim">
          <span>Win kans:</span>
          <span>${Math.round((sizing.win_prob || 0) * 100)}%</span>
        </div>
        <div class="sizing-detail__row sizing-detail__row--dim">
          <span>R:R ratio:</span>
          <span>${sizing.reward_risk_ratio || "—"}:1</span>
        </div>
        <div class="sizing-detail__row sizing-detail__row--dim">
          <span>Kelly raw / half:</span>
          <span>${((sizing.kelly_raw || 0) * 100).toFixed(1)}% / ${((sizing.kelly_half || 0) * 100).toFixed(1)}%</span>
        </div>
      </div>
    </div>`;
  }

  // Reasoning sections
  if (typeof reasoning === "object") {
    if (reasoning.thesis) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">📋 Thesis</div>
        <div class="advisory-detail__text">${_esc(reasoning.thesis)}</div>
      </div>`;
    }
    if (reasoning.catalyst) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">⚡ Catalyst — waarom NU?</div>
        <div class="advisory-detail__text">${_esc(reasoning.catalyst)}</div>
      </div>`;
    }
    if (reasoning.timing) {
      body += `<div class="advisory-detail__section advisory-detail__section--timing">
        <div class="advisory-detail__label">🕐 Timing (CET)</div>
        <div class="advisory-detail__text">${_esc(reasoning.timing)}</div>
      </div>`;
    }
    if (reasoning.risk) {
      body += `<div class="advisory-detail__section advisory-detail__section--risk">
        <div class="advisory-detail__label">⚠️ Risico</div>
        <div class="advisory-detail__text">${_esc(reasoning.risk)}</div>
      </div>`;
    }
    if (reasoning.target) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">🎯 Koersdoel</div>
        <div class="advisory-detail__text">${_esc(reasoning.target)}</div>
      </div>`;
    }
  } else if (reasoning) {
    body = `<div class="advisory-detail__text">${_esc(reasoning)}</div>` + body;
  }

  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.innerHTML = `
    <div class="modal advisory-detail-modal">
      <div class="modal__header" style="border-left: 4px solid ${headerColor}">
        <span class="advisory-detail__badge advisory-detail__badge--${type}">${actionLabel}</span>
        <span class="advisory-detail__ticker">${_esc(ticker)}</span>
        <span class="advisory-detail__name">${_esc(name)}</span>
        <button class="modal__close">&times;</button>
      </div>
      <div class="modal__body">${body}</div>
    </div>`;
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.querySelector(".modal__close").onclick = close;
  overlay.addEventListener("click", (e) => { if (e.target === overlay) close(); });
}

function renderTrackRecord() {
  const hist = state.advisoryHistory;
  if (!hist || !hist.history || hist.history.length === 0) {
    return "";
  }

  const items = hist.history;
  // Aggregate stats from items that have evaluations
  let totalEvals = 0, totalCorrect = 0;
  const allReturns = [];
  for (const h of items) {
    totalEvals += h.outcomes_evaluated || 0;
    totalCorrect += h.outcomes_correct || 0;
    if (h.avg_return_pct != null) allReturns.push(h.avg_return_pct);
  }
  const accuracy = totalEvals > 0 ? ((totalCorrect / totalEvals) * 100).toFixed(1) : "—";
  const avgReturn = allReturns.length > 0 ? (allReturns.reduce((a, b) => a + b, 0) / allReturns.length).toFixed(2) : "—";
  const accCls = totalEvals > 0 ? (totalCorrect / totalEvals >= 0.5 ? "--positive" : "--negative") : "--neutral";
  const retCls = avgReturn !== "—" ? (parseFloat(avgReturn) >= 0 ? "--positive" : "--negative") : "--neutral";

  let html = `<div class="track-record">
    <div class="track-record__title">
      <span>📈 Track Record (last ${items.length} advisories)</span>
    </div>
    <div class="track-record__stats">
      <div class="track-record__stat">Accuracy: <span class="track-record__stat-value track-record__stat-value${accCls}">${accuracy}%</span></div>
      <div class="track-record__stat">Avg Return: <span class="track-record__stat-value track-record__stat-value${retCls}">${avgReturn !== "—" ? avgReturn + "%" : "—"}</span></div>
      <div class="track-record__stat">Evaluations: <span class="track-record__stat-value">${totalEvals}</span></div>
    </div>`;

  // Table of recent advisories
  html += `<table class="track-record__table">
    <thead><tr>
      <th>Date</th><th>Stance</th><th>BUY</th><th>SELL</th><th>Evals</th><th>Accuracy</th><th>Avg Return</th>
    </tr></thead><tbody>`;

  for (const h of items.slice(0, 10)) {
    const date = h.date || "—";
    const stanceLabel = _stanceLabel(h.market_stance || "neutral");
    const buyTicks = (h.buy_tickers || []).join(", ") || "—";
    const sellTicks = (h.sell_tickers || []).join(", ") || "—";
    const evals = h.outcomes_evaluated || 0;
    const acc = h.accuracy != null ? `${(h.accuracy * 100).toFixed(0)}%` : "—";
    const ret = h.avg_return_pct != null ? `${h.avg_return_pct > 0 ? "+" : ""}${h.avg_return_pct.toFixed(2)}%` : "—";
    const retClass = h.avg_return_pct != null ? (h.avg_return_pct >= 0 ? "return--positive" : "return--negative") : "";

    html += `<tr>
      <td>${date}</td>
      <td><span class="stance-badge stance-badge--${h.market_stance || "neutral"}" style="font-size:0.68rem;padding:2px 8px">${stanceLabel}</span></td>
      <td class="ticker-cell" style="color:var(--green)">${buyTicks}</td>
      <td class="ticker-cell" style="color:var(--red)">${sellTicks}</td>
      <td>${evals}</td>
      <td>${acc}</td>
      <td class="${retClass}">${ret}</td>
    </tr>`;
  }

  html += `</tbody></table></div>`;
  return html;
}

function renderLearningStats() {
  const hist = state.advisoryHistory;
  if (!hist || !hist.learning_stats) return "";

  const stats = hist.learning_stats;
  const weights = stats.advisory_weights;
  const brier = stats.advisory_brier_scores;
  const emas = stats.advisory_component_emas;

  if (!weights && !brier) return "";

  let html = `<div class="learning-stats">
    <div class="learning-stats__title">🧠 Self-Learning System</div>
    <div class="learning-stats__grid">`;

  // Current weights
  if (weights && typeof weights === "object") {
    for (const [comp, w] of Object.entries(weights)) {
      html += `<div class="learning-stat">
        <div class="learning-stat__label">${comp} weight</div>
        <div class="learning-stat__value">${(w * 100).toFixed(1)}%</div>
      </div>`;
    }
  }

  // Brier scores
  if (brier && typeof brier === "object") {
    for (const [key, val] of Object.entries(brier)) {
      if (key.startsWith("T+")) {
        html += `<div class="learning-stat">
          <div class="learning-stat__label">Brier ${key}</div>
          <div class="learning-stat__value" style="color:${val < 0.25 ? "var(--green)" : val < 0.4 ? "var(--yellow)" : "var(--red)"}">${val.toFixed(3)}</div>
        </div>`;
      }
    }
  }

  html += `</div></div>`;
  return html;
}

function _stanceLabel(stance) {
  const map = {
    strong_bullish: "🟢 Strong Bullish",
    cautious_bullish: "🟢 Cautious Bullish",
    neutral: "🟡 Neutral",
    cautious_bearish: "🔴 Cautious Bearish",
    strong_bearish: "🔴 Strong Bearish",
  };
  return map[stance] || stance;
}

function _chgBadge(pct) {
  if (pct == null) return "";
  const cls = pct >= 0 ? "nav-ind__chg--up" : "nav-ind__chg--down";
  return `<span class="${cls}" style="font-size:0.72rem">${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%</span>`;
}

function _esc(str) {
  if (!str) return "";
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function bindPortfolioTabEvents() {
  // Generate advisory button
  document.querySelector("[data-generate-advisory]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-generate-advisory]");
    if (btn) { btn.disabled = true; btn.textContent = "Generating..."; }
    try {
      await fetch(`${API_BASE}?_api=advisory-generate`, { method: "POST" });
      await new Promise(r => setTimeout(r, 3000));
      await Promise.all([fetchAdvisory(), fetchAdvisoryHistory(), fetchPortfolioAlignment()]);
      render();
    } catch {}
    if (btn) { btn.disabled = false; btn.textContent = "Regenerate"; }
  });

  // Edit portfolio button
  document.querySelector("[data-edit-portfolio]")?.addEventListener("click", () => {
    _showPortfolioEditor();
  });

  // Advisory card click → detail popup
  document.querySelectorAll(".advisory-card--clickable").forEach(card => {
    card.addEventListener("click", (e) => {
      // Don't open popup if clicking a ticker link for chart
      if (e.target.closest("[data-chart-ticker]")) return;
      _showAdvisoryDetail(card);
    });
  });
}

function _showPortfolioEditor() {
  // Get current holdings from state
  const holdings = (state.advisory || {}).portfolio_holdings || [];
  const align = state.portfolioAlignment;
  const currentItems = (align && align.alignment) ? align.alignment : holdings.map(h => ({
    ticker: h.ticker, name: h.name, shares: h.shares || 0, avg_buy_price_eur: h.avg_buy_price_eur || 0,
  }));

  let rows = "";
  for (const h of currentItems) {
    rows += `<tr>
      <td><input type="text" class="portfolio-edit__ticker" value="${_esc(h.ticker)}" placeholder="XOM" style="width:70px"></td>
      <td><input type="text" class="portfolio-edit__name" value="${_esc(h.name)}" placeholder="Exxon Mobil" style="width:150px"></td>
      <td><input type="number" class="portfolio-edit__shares" value="${h.shares || ''}" step="0.01" min="0" style="width:70px"></td>
      <td><input type="number" class="portfolio-edit__avgbuy" value="${h.avg_buy_price_eur || ''}" step="0.01" min="0" style="width:80px"></td>
      <td><button class="btn btn--xs btn--danger portfolio-edit__remove">✕</button></td>
    </tr>`;
  }
  // Add empty row
  rows += `<tr>
    <td><input type="text" class="portfolio-edit__ticker" value="" placeholder="XOM" style="width:70px"></td>
    <td><input type="text" class="portfolio-edit__name" value="" placeholder="Naam" style="width:150px"></td>
    <td><input type="number" class="portfolio-edit__shares" value="" step="0.01" min="0" style="width:70px"></td>
    <td><input type="number" class="portfolio-edit__avgbuy" value="" step="0.01" min="0" style="width:80px"></td>
    <td><button class="btn btn--xs btn--danger portfolio-edit__remove">✕</button></td>
  </tr>`;

  const modal = document.createElement("div");
  modal.className = "modal-overlay";
  modal.innerHTML = `<div class="modal portfolio-editor-modal">
    <div class="modal__header">
      <h3>💼 Portfolio bewerken</h3>
      <button class="modal__close" data-close-modal>✕</button>
    </div>
    <div class="modal__body">
      <p style="color:var(--text-dim);font-size:0.8rem;margin-bottom:12px">
        Voer je Bunq Stocks posities in (ticker + aantal shares + gemiddelde koopprijs in EUR).
      </p>
      <table class="portfolio-edit__table">
        <thead><tr><th>Ticker</th><th>Naam</th><th>Shares</th><th>Koopprijs (€)</th><th></th></tr></thead>
        <tbody id="portfolio-edit-rows">${rows}</tbody>
      </table>
      <button class="btn btn--xs" id="portfolio-add-row" style="margin-top:8px">+ Positie toevoegen</button>
    </div>
    <div class="modal__footer">
      <button class="btn btn--sm" data-close-modal>Annuleren</button>
      <button class="btn btn--sm btn--primary" id="portfolio-save">Opslaan</button>
    </div>
  </div>`;

  document.body.appendChild(modal);

  // Close handler
  modal.querySelectorAll("[data-close-modal]").forEach(el => {
    el.addEventListener("click", () => modal.remove());
  });
  modal.addEventListener("click", (e) => {
    if (e.target === modal) modal.remove();
  });

  // Add row handler
  document.getElementById("portfolio-add-row")?.addEventListener("click", () => {
    const tbody = document.getElementById("portfolio-edit-rows");
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="text" class="portfolio-edit__ticker" value="" placeholder="XOM" style="width:70px"></td>
      <td><input type="text" class="portfolio-edit__name" value="" placeholder="Naam" style="width:150px"></td>
      <td><input type="number" class="portfolio-edit__shares" value="" step="0.01" min="0" style="width:70px"></td>
      <td><input type="number" class="portfolio-edit__avgbuy" value="" step="0.01" min="0" style="width:80px"></td>
      <td><button class="btn btn--xs btn--danger portfolio-edit__remove">✕</button></td>
    `;
    tbody.appendChild(tr);
    _bindRemoveButtons();
  });

  _bindRemoveButtons();

  // Save handler
  document.getElementById("portfolio-save")?.addEventListener("click", async () => {
    const rowEls = document.querySelectorAll("#portfolio-edit-rows tr");
    const holdings = [];
    for (const row of rowEls) {
      const ticker = (row.querySelector(".portfolio-edit__ticker")?.value || "").trim().toUpperCase();
      const name = (row.querySelector(".portfolio-edit__name")?.value || "").trim();
      const shares = parseFloat(row.querySelector(".portfolio-edit__shares")?.value || "0");
      const avgBuy = parseFloat(row.querySelector(".portfolio-edit__avgbuy")?.value || "0");
      if (ticker && shares > 0) {
        holdings.push({ ticker, name: name || ticker, shares, avg_buy_price_eur: avgBuy });
      }
    }

    try {
      const r = await fetch(`${API_BASE}?_api=portfolio-holdings`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ holdings }),
      });
      if (r.ok) {
        modal.remove();
        await Promise.all([fetchAdvisory(), fetchPortfolioAlignment()]);
        render();
      }
    } catch (err) {
      console.error("Failed to save portfolio:", err);
    }
  });
}

function _bindRemoveButtons() {
  document.querySelectorAll(".portfolio-edit__remove").forEach(btn => {
    btn.onclick = () => btn.closest("tr")?.remove();
  });
}

/* ══════════════════════════════════════════════════════════════
   EVENT BINDING
   ══════════════════════════════════════════════════════════════ */

function bindTabEvents() {
  switch (state.activeTab) {
    case "signals": bindSignalTabEvents(); break;
    case "portfolio": bindPortfolioTabEvents(); break;
    case "trees": bindTreesTabEvents(); break;
    case "intel": bindIntelTabEvents(); break;
    case "usage": bindUsageTabEvents(); break;
    case "settings": bindSettingsTabEvents(); break;
  }
  // Global ticker clicks
  document.querySelectorAll("[data-chart-ticker]").forEach(el => {
    if (el._chartBound) return;
    el._chartBound = true;
    el.style.cursor = "pointer";
    el.addEventListener("click", e => {
      e.stopPropagation();
      if (el.dataset.chartTicker) openPriceChart(el.dataset.chartTicker);
    });
  });
}

function bindSignalTabEvents() {
  document.querySelector("[data-refresh-signals]")?.addEventListener("click", async (btn) => {
    const el = document.querySelector("[data-refresh-signals]");
    if (el) { el.disabled = true; el.textContent = "..."; }
    try {
      await fetch(`${API_BASE}?_api=signals-refresh`, { method: "POST" });
      await new Promise(r => setTimeout(r, 1500));
      await fetchSignals();
      render();
    } catch {}
    if (el) { el.disabled = false; el.textContent = "Refresh"; }
  });

  // Signal card click → load tree and show swarm detail
  document.querySelectorAll(".signal-card").forEach(card => {
    card.addEventListener("click", async () => {
      const narrative = card.dataset.signalNarrative;
      const ticker = card.dataset.signalTicker;
      // Find matching run-up
      const ru = state.runups.find(r => r.narrative_name === narrative);
      if (ru) {
        // Switch to trees and open it
        state.activeTab = "trees";
        activeTreeId = ru.id;
        await fetchTree(ru.id);
      }
    });
  });

  // Opportunity card click → load tree
  document.querySelectorAll(".opp-card").forEach(card => {
    card.addEventListener("click", async () => {
      const ruId = parseInt(card.dataset.oppRunup);
      if (ruId) {
        state.activeTab = "trees";
        activeTreeId = ruId;
        await fetchTree(ruId);
        startTreePolling(ruId);
      }
    });
  });
}

function bindTreesTabEvents() {
  document.querySelectorAll("[data-tree-id]").forEach(card => {
    card.addEventListener("click", async () => {
      const id = parseInt(card.dataset.treeId);
      activeTreeId = id;
      await fetchTree(id);
      startTreePolling(id);
    });
  });
}

function bindIntelTabEvents() {
  document.querySelector("[data-run-analysis]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-run-analysis]");
    if (btn) { btn.disabled = true; btn.textContent = "Running..."; }
    try {
      await fetch(`${API_BASE}?_api=analysis-run`, { method: "POST" });
      await new Promise(r => setTimeout(r, 3000));
      await fetchAnalysis();
      render();
    } catch {}
  });
}

function bindSettingsTabEvents() {
  document.querySelector("[data-run-swarm]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-run-swarm]");
    if (btn) { btn.disabled = true; btn.textContent = "Running..."; }
    try {
      await fetch(`${API_BASE}?_api=swarm-cycle`, { method: "POST" });
      await new Promise(r => setTimeout(r, 2000));
      await fetchSwarmStatus();
      render();
    } catch {}
  });

  document.querySelector("[data-show-feeds]")?.addEventListener("click", () => {
    showFeedsModal();
  });

  // Focus Mode: toggle focus on/off
  document.querySelectorAll("[data-toggle-focus]").forEach(el => {
    el.addEventListener("click", () => {
      const id = parseInt(el.getAttribute("data-toggle-focus"));
      const current = new Set((state.focus?.focused_runup_ids) || []);
      if (current.has(id)) {
        current.delete(id);
      } else if (current.size < 3) {
        current.add(id);
      } else {
        return; // max 3
      }
      setFocus([...current]);
    });
  });

  // Focus Mode: clear all
  document.querySelector("[data-clear-focus]")?.addEventListener("click", () => {
    setFocus([]);
  });

  // Focus Mode: add Polymarket link
  document.querySelector("[data-add-pm-link]")?.addEventListener("click", () => {
    const runUpId = parseInt(document.getElementById("focus-pm-runup")?.value);
    const url = (document.getElementById("focus-pm-url")?.value || "").trim();
    if (runUpId && url && url.startsWith("http")) {
      addPolymarketLink(runUpId, url);
    }
  });

  // Telegram: save config
  document.querySelector("[data-save-telegram]")?.addEventListener("click", async () => {
    const token = document.getElementById("tg-token")?.value?.trim();
    const chatId = document.getElementById("tg-chatid")?.value?.trim();
    if (!token || !chatId || token.startsWith("••")) {
      alert("Vul bot token en chat ID in.");
      return;
    }
    try {
      const res = await fetch(`${API_BASE}?_api=telegram-configure`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ bot_token: token, chat_id: chatId }),
      });
      const data = await res.json();
      if (data.success) {
        await fetchTelegramStatus();
        render();
        alert("Telegram geconfigureerd!");
      } else {
        alert(data.message || "Fout bij opslaan.");
      }
    } catch (e) { alert("Fout: " + e.message); }
  });

  // Telegram: test
  document.querySelector("[data-test-telegram]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-test-telegram]");
    if (btn) { btn.disabled = true; btn.textContent = "Sending..."; }
    try {
      const res = await fetch(`${API_BASE}?_api=telegram-test`, { method: "POST" });
      const data = await res.json();
      alert(data.message || (data.success ? "OK" : "Mislukt"));
    } catch (e) { alert("Fout: " + e.message); }
    if (btn) { btn.disabled = false; btn.textContent = "Test"; }
  });

  // Telegram: send advisory
  document.querySelector("[data-send-telegram-advisory]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-send-telegram-advisory]");
    if (btn) { btn.disabled = true; btn.textContent = "Sending..."; }
    try {
      const res = await fetch(`${API_BASE}?_api=telegram-send-advisory`, { method: "POST" });
      const data = await res.json();
      alert(data.message || (data.success ? "Verstuurd!" : "Mislukt"));
    } catch (e) { alert("Fout: " + e.message); }
    if (btn) { btn.disabled = false; btn.textContent = "Stuur Advisory"; }
  });
}

/* ── Feeds Modal ──────────────────────────────────────────────── */

function showFeedsModal() {
  const feeds = state.feeds || [];
  let body = `<div style="max-height:400px;overflow-y:auto">`;
  for (const f of feeds) {
    const status = f.enabled !== false ? "text-green" : "text-dim";
    body += `<div class="feed-row">
      <span class="feed-row__name ${status}">${esc(f.name || f.url || "Unknown")}</span>
      <span class="feed-row__region">${esc(f.region || "global")}</span>
      ${f.is_default ? "" : `<button class="feed-row__del" data-del-feed="${f.id}">&times;</button>`}
    </div>`;
  }
  body += `</div>
  <div style="margin-top:16px;border-top:1px solid var(--border);padding-top:16px">
    <div class="section-label">Add Feed</div>
    <div class="form-row">
      <input class="form-input" id="feed-name" placeholder="Feed name" style="flex:1">
      <input class="form-input" id="feed-region" placeholder="Region" style="width:100px">
    </div>
    <div class="form-row">
      <input class="form-input" id="feed-url" placeholder="RSS URL" style="flex:1">
      <button class="btn btn--primary" id="feed-add-btn">Add</button>
    </div>
  </div>`;

  openModal(`RSS Feeds (${feeds.length})`, body, true);

  // Bind delete
  document.querySelectorAll("[data-del-feed]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.delFeed;
      try {
        await fetch(`${API_BASE}?_api=feeds&id=${encodeURIComponent(id)}`, { method: "DELETE" });
        await fetchFeeds();
        showFeedsModal(); // Re-render modal
      } catch {}
    });
  });

  // Bind add
  document.getElementById("feed-add-btn")?.addEventListener("click", async () => {
    const name = document.getElementById("feed-name")?.value;
    const url = document.getElementById("feed-url")?.value;
    const region = document.getElementById("feed-region")?.value || "global";
    if (!url) return;
    try {
      await fetch(`${API_BASE}?_api=feeds`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: name || url, url, region }),
      });
      await fetchFeeds();
      showFeedsModal();
    } catch (err) {
      alert("Failed: " + err.message);
    }
  });
}

/* ══════════════════════════════════════════════════════════════
   POLLING & INIT
   ══════════════════════════════════════════════════════════════ */

function startPolling() { pollTimer = setInterval(() => { fetchOverview(); fetchOpportunities(); }, POLL_INTERVAL); }
function stopPolling() { clearInterval(pollTimer); }
function startTreePolling(id) { treePollTimer = setInterval(() => fetchTree(id), POLL_INTERVAL); }
function stopTreePolling() { clearInterval(treePollTimer); }

function syncTheme() {
  try {
    const parent = window.parent;
    if (parent === window) return;
    const app = parent.document.querySelector("openclaw-app");
    if (!app) return;
    const root = app.shadowRoot || app;
    const style = window.parent.getComputedStyle(root);
    for (const v of ["--bg", "--bg-card", "--text", "--accent", "--border"]) {
      const val = style.getPropertyValue(v);
      if (val) document.documentElement.style.setProperty(v, val);
    }
  } catch {}
}

// Visibility change handler — fetch fresh data + render once on tab-restore
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    stopPolling();
    stopTreePolling();
  } else {
    Promise.all([fetchOverview(), fetchOpportunities(), fetchFocus()]).then(() => render());
    startPolling();
    if (activeTreeId) startTreePolling(activeTreeId);
  }
});

// Hash routing
function initRoute() {
  const hash = location.hash.replace("#", "");
  if (["signals", "portfolio", "trees", "intel", "usage", "settings"].includes(hash)) {
    state.activeTab = hash;
  }
}

// Boot
syncTheme();
initRoute();
const bootFetches = [
  fetchOverview(),
  fetchSignals(),
  fetchIndicators(),
  fetchOpportunities(),
  fetchFocus(),
];
// If landing on portfolio tab, also fetch advisory data immediately
if (state.activeTab === "portfolio") {
  bootFetches.push(fetchAdvisory(), fetchAdvisoryHistory(), fetchPortfolioAlignment());
}
// If landing on usage tab, fetch usage data immediately
if (state.activeTab === "usage") {
  bootFetches.push(fetchUsage(_usageDays));
}
// If landing on settings tab, also fetch settings data immediately
if (state.activeTab === "settings") {
  bootFetches.push(fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus());
}
Promise.all(bootFetches).then(() => render());

// Refresh indicators every 5 min
setInterval(fetchIndicators, 300000);
startPolling();
