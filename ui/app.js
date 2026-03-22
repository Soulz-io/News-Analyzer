/**
 * OpenClaw News Analyzer — Clean Modern Dashboard
 *
 * Tab-based SPA: Signals | Trees | Intel | Settings
 * Progressive disclosure: cards → modals for detail.
 */

const API_BASE = "/plugins/openclaw-news-analyzer/api";
const POLL_INTERVAL = 30000;

/* ── Auth ────────────────────────────────────────────────────── */

let _bearerToken = localStorage.getItem("oc_bearer_token") || "";

function authHeaders(extra = {}) {
  const h = { ...extra };
  if (_bearerToken) h["Authorization"] = `Bearer ${_bearerToken}`;
  return h;
}

function authFetch(url, opts = {}) {
  opts.headers = authHeaders(opts.headers || {});
  return fetch(url, opts);
}

/* ── State ───────────────────────────────────────────────────── */

let state = {
  activeTab: "portfolio",
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
  flashAlerts: [],
  portfolioAlignment: null,
  usageData: null,
  mlData: null,
  currentUser: null,
  feedData: null,
  feedOffset: 0,
  loading: true,
  error: null,
};

let activeTreeId = null;
let pollTimer = null;
let treePollTimer = null;
let _priceModalTicker = null;
let _priceChartInstance = null;
let _priceChartObserver = null;

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
  "east-asia": "region-badge--asia",
  "south-asia": "region-badge--asia",
  "southeast-asia": "region-badge--asia",
  "asia": "region-badge--asia",
  "americas": "region-badge--americas",
  "africa": "region-badge--africa",
  "global": "region-badge--global",
};

/* ── Toast Notifications ──────────────────────────────────────── */

function showToast(message, type = "info", duration = 4000) {
  let container = document.getElementById("toast-container");
  if (!container) {
    container = document.createElement("div");
    container.id = "toast-container";
    container.setAttribute("aria-live", "polite");
    document.body.appendChild(container);
  }
  const toast = document.createElement("div");
  toast.className = `toast toast--${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add("toast--visible"));
  setTimeout(() => {
    toast.classList.remove("toast--visible");
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

function showError(msg) { showToast(msg, "error", 5000); }
function showSuccess(msg) { showToast(msg, "success", 3000); }
function showWarning(msg) { showToast(msg, "warning", 4000); }

function regionClass(region) {
  if (!region) return "region-badge--global";
  return REGION_CLASSES[region.toLowerCase().replace(/[\s_]+/g, "-")] || "region-badge--global";
}

/* ── API Layer ───────────────────────────────────────────────── */

async function fetchOverview() {
  try {
    const [oRes, sRes] = await Promise.all([
      authFetch(`${API_BASE}?_api=overview`),
      authFetch(`${API_BASE}?_api=status`),
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
    const r = await authFetch(`${API_BASE}?_api=signals`);
    if (r.ok) state.signals = await r.json();
  } catch (e) { console.warn("[API] fetchSignals failed:", e.message); showError("Failed to load signals. Retrying..."); }
}

async function fetchIndicators() {
  try {
    const r = await authFetch(`${API_BASE}?_api=indicators`);
    if (r.ok) state.indicators = await r.json();
  } catch (e) { console.warn("[API] fetchIndicators failed:", e.message); showError("Failed to load indicators. Retrying..."); }
}

async function fetchAnalysis() {
  try {
    const r = await authFetch(`${API_BASE}?_api=analysis`);
    if (r.ok) state.analysis = await r.json();
  } catch (e) { console.warn("[API] fetchAnalysis failed:", e.message); }
}

async function fetchAdvisory() {
  try {
    const r = await authFetch(`${API_BASE}?_api=advisory`);
    if (r.ok) state.advisory = await r.json();
  } catch (e) { console.warn("[API] fetchAdvisory failed:", e.message); showError("Failed to load advisory. Retrying..."); }
}

async function fetchAdvisoryHistory() {
  try {
    const r = await authFetch(`${API_BASE}?_api=advisory-history&limit=14`);
    if (r.ok) state.advisoryHistory = await r.json();
  } catch (e) { console.warn("[API] fetchAdvisoryHistory failed:", e.message); showError("Failed to load advisory history. Retrying..."); }
}

async function fetchPortfolioAlignment() {
  try {
    const r = await authFetch(`${API_BASE}?_api=portfolio-alignment`);
    if (r.ok) state.portfolioAlignment = await r.json();
  } catch (e) { console.warn("[API] fetchPortfolioAlignment failed:", e.message); showError("Failed to load portfolio alignment. Retrying..."); }
}

async function fetchUsage(days = 7) {
  try {
    const r = await authFetch(`${API_BASE}?_api=usage-breakdown&days=${days}`);
    if (r.ok) state.usageData = await r.json();
  } catch (e) { console.warn("[API] fetchUsage failed:", e.message); }
}

async function fetchFeeds() {
  try {
    const r = await authFetch(`${API_BASE}?_api=feeds`);
    if (r.ok) state.feeds = await r.json();
  } catch (e) { console.warn("[API] fetchFeeds failed:", e.message); }
}

async function fetchBudget() {
  try {
    const r = await authFetch(`${API_BASE}?_api=budget`);
    if (r.ok) state.budget = await r.json();
  } catch (e) { console.warn("[API] fetchBudget failed:", e.message); }
}

async function fetchApiKeyStatus() {
  try {
    const r = await authFetch(`${API_BASE}?_api=apikey`);
    if (r.ok) state.apiKeyStatus = await r.json();
  } catch (e) { console.warn("[API] fetchApiKeyStatus failed:", e.message); }
}

async function fetchSwarmStatus() {
  try {
    const r = await authFetch(`${API_BASE}?_api=swarm-status`);
    if (r.ok) state.swarmStatus = await r.json();
  } catch (e) { console.warn("[API] fetchSwarmStatus failed:", e.message); }
}

async function fetchPortfolioSize() {
  try {
    const r = await authFetch(`${API_BASE}?_api=portfolio-size`);
    if (r.ok) {
      const data = await r.json();
      state.portfolioSize = data.portfolio_size_eur || 5000;
    }
  } catch (e) { console.warn("[API] fetchPortfolioSize failed:", e.message); }
}

async function fetchFeed(params = {}) {
  const qs = new URLSearchParams({ limit: "50", ...params }).toString();
  try {
    const r = await authFetch(`${API_BASE}?_api=briefs&${qs}`);
    if (r.ok) state.feedData = await r.json();
  } catch (e) { console.warn("[API] fetchFeed failed:", e.message); }
}

async function fetchOpportunities() {
  try {
    const r = await authFetch(`${API_BASE}?_api=opportunities&minEdge=3`);
    if (r.ok) state.opportunities = await r.json();
  } catch (e) { console.warn("[API] fetchOpportunities failed:", e.message); showError("Failed to load opportunities. Retrying..."); }
}

async function fetchFocus() {
  try {
    const r = await authFetch(`${API_BASE}?_api=focus`);
    if (r.ok) state.focus = await r.json();
  } catch (e) { console.warn("[API] fetchFocus failed:", e.message); showError("Failed to load focus. Retrying..."); }
}

async function setFocus(runupIds) {
  // Optimistic update for instant visual feedback
  if (!state.focus) state.focus = { focused_runup_ids: [], focused_runups: [], polymarket_links: {} };
  state.focus.focused_runup_ids = runupIds;
  render();

  try {
    const r = await authFetch(`${API_BASE}?_api=focus`, {
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
    await authFetch(`${API_BASE}?_api=focus-polymarket-link`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_up_id: runUpId, polymarket_url: url }),
    });
    await fetchFocus();
    render();
  } catch (e) { showError("Failed to add Polymarket link: " + e.message); }
}

async function regenerateFocusTree(runUpId) {
  try {
    await authFetch(`${API_BASE}?_api=focus-regenerate-tree&id=${runUpId}`, {
      method: "POST",
    });
    // Reload tree data
    if (activeTreeId === runUpId) {
      await fetchTree(runUpId);
    }
    await fetchOverview();
    render();
  } catch (e) { showError("Failed to regenerate tree: " + e.message); }
}

async function fetchTree(runUpId) {
  try {
    const r = await authFetch(`${API_BASE}?_api=tree&id=${encodeURIComponent(runUpId)}`);
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
  } catch (err) {
    console.warn("[API] fetchTree failed:", err);
    state.activeTree = null;
    state.polymarket = [];
  }
  render();
}

async function fetchSwarmVerdict(nodeId) {
  try {
    const r = await authFetch(`${API_BASE}?_api=swarm-verdict&nodeId=${nodeId}`);
    if (r.ok) return await r.json();
  } catch (e) { console.warn("[API] fetchSwarmVerdict failed:", e.message); }
  return null;
}

/* ── Render: Main Router ─────────────────────────────────────── */

/* ── MiFID II Disclaimer ─────────────────────────────────────── */

function hasMifidConsent() {
  return localStorage.getItem("oc_mifid_consent") === "true";
}

function renderMifidDisclaimer() {
  return `<div class="mifid-overlay" id="mifid-overlay">
    <div class="mifid-modal">
      <h2 class="mifid-modal__title">Important Regulatory Disclaimer</h2>
      <div class="mifid-modal__body">
        <p><strong>MiFID II / Investment Research Disclaimer</strong></p>
        <p>This tool provides automated financial analysis and market intelligence
           for <strong>informational purposes only</strong>. It does <strong>not</strong>
           constitute investment advice, a personal recommendation, or an offer or
           solicitation to buy or sell any financial instrument.</p>
        <p>All signals, decision trees, advisory outputs, and portfolio analytics
           are generated by automated algorithms and AI models. They may contain
           errors, become outdated, or be based on incomplete data. Past performance
           is not indicative of future results.</p>
        <p>You should not rely on any information provided by this tool as the sole
           basis for making investment decisions. Always seek independent professional
           financial advice before acting on any information presented here.</p>
        <p>By clicking "I Understand &amp; Accept" you acknowledge that:</p>
        <ul>
          <li>You have read and understood this disclaimer.</li>
          <li>You accept that this tool is not a regulated investment service.</li>
          <li>You bear sole responsibility for any investment decisions.</li>
          <li>The operators of this tool accept no liability for financial losses.</li>
        </ul>
      </div>
      <div class="mifid-modal__actions">
        <button class="btn btn--accent" id="mifid-accept">I Understand &amp; Accept</button>
      </div>
    </div>
  </div>`;
}

function bindMifidEvents() {
  const btn = document.getElementById("mifid-accept");
  if (btn) {
    btn.addEventListener("click", () => {
      localStorage.setItem("oc_mifid_consent", "true");
      const overlay = document.getElementById("mifid-overlay");
      if (overlay) overlay.remove();
    });
  }
}

/* ── Render ──────────────────────────────────────────────────── */

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
    if (!hasMifidConsent()) html += renderMifidDisclaimer();
    app.innerHTML = html;
    bindNavEvents();
    bindTreeViewEvents();
    bindTabEvents();
    bindMifidEvents();
    return;
  }

  // Tab content
  html += `<div class="main">`;
  html += renderBreakingBanner();
  switch (state.activeTab) {
    case "signals": html += renderSignalsTab(); break;
    case "portfolio": html += renderPortfolioTab(); break;
    case "feed": html += renderFeedTab(); break;
    case "trees":   html += renderTreesTab(); break;
    case "intel":   html += renderIntelTab(); break;
    case "usage":   html += renderUsageTab(); break;
    case "ml":      html += renderMLTab(); break;
    case "users":   html += renderUsersTab(); break;
    case "settings": html += renderSettingsTab(); break;
    case "swarm": html += renderSwarmTab(); break;
  }
  html += `</div>`;

  if (!hasMifidConsent()) html += renderMifidDisclaimer();
  app.innerHTML = html;
  bindNavEvents();
  bindTabEvents();
  bindMifidEvents();
  requestAnimationFrame(() => window.scrollTo(0, scrollY));
}

/* ── Render: Navbar ──────────────────────────────────────────── */

function renderNavbar() {
  const running = state.status.engine === "running";
  const isAdmin = state.currentUser && state.currentUser.is_admin;
  const allTabs = [
    { id: "portfolio", label: "Portfolio", adminOnly: false },
    { id: "swarm", label: "Swarm", adminOnly: false },
    { id: "ml", label: "ML", adminOnly: true },
    { id: "users", label: "Users", adminOnly: true },
  ];
  const tabs = allTabs.filter(t => !t.adminOnly || isAdmin);

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

  if (_embedMode) {
    return `<nav class="navbar"><div class="navbar__inner">
      <div class="navbar__brand">
        <span class="navbar__status navbar__status--${running ? "ok" : "err"}"></span>
        <span class="navbar__title">\u{1F4CA} Portfolio</span>
      </div>
      <div class="navbar__indicators">${indHtml}</div>
      <button class="help-btn" data-show-help title="Help & Guide">?</button>
    </div></nav>`;
  }

  const userName = state.currentUser ? state.currentUser.display_name || state.currentUser.username : "";

  return `<nav class="navbar"><div class="navbar__inner">
    <div class="navbar__brand">
      <span class="navbar__status navbar__status--${running ? "ok" : "err"}"></span>
      <span class="navbar__title">W25</span>
      <span class="navbar__activity">${(state.status.total_articles || 0).toLocaleString()} articles</span>
    </div>
    <div class="navbar__tabs">
      ${tabs.map(t => `<button class="nav-tab${state.activeTab === t.id ? " nav-tab--active" : ""}" data-tab="${t.id}">${t.label}</button>`).join("")}
    </div>
    <div class="navbar__indicators">${indHtml}</div>
    <div class="navbar__user">
      <button class="user-menu-btn" data-toggle-user-menu>${_esc(userName)} ▾</button>
      <div class="user-menu" id="user-menu">
        ${isAdmin ? `<div class="user-menu__item" data-tab="usage">Usage</div>` : ""}
        ${isAdmin ? `<div class="user-menu__item" data-tab="settings">Settings</div>` : ""}
        <div class="user-menu__divider"></div>
        <div class="user-menu__item user-menu__item--danger" data-logout>Logout</div>
      </div>
    </div>
  </div></nav>`;
}

function bindNavEvents() {
  document.querySelectorAll("[data-tab]").forEach(btn => {
    btn.addEventListener("click", () => {
      if (_embedMode) return;
      // Close user menu if open
      document.getElementById("user-menu")?.classList.remove("open");
      const tab = btn.dataset.tab;
      if (tab === state.activeTab) return;
      state.activeTab = tab;
      activeTreeId = null;
      location.hash = tab;
      render();
      // Lazy-load tab data
      if (tab === "portfolio" && !state.advisory) {
        Promise.all([fetchAdvisory(), fetchAdvisoryHistory(), fetchPortfolioAlignment(), fetchSwarmFeed()]).then(render);
      }
      if (tab === "usage" && !state.usageData) fetchUsage().then(render);
      if (tab === "ml" && !state.mlData) fetchMLData().then(render);
      if (tab === "users" && !state.usersData) fetchUsers().then(render);
      if (tab === "swarm" && (!state.swarmActivity || !state._swarmFetchedAt || Date.now() - state._swarmFetchedAt > 300000)) {
        fetchSwarmActivity().then(render);
      }
      if (tab === "settings") {
        Promise.all([fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus(), fetchFocus()]).then(render);
      }
    });
  });
  // Indicator clicks → price chart
  document.querySelectorAll(".nav-ind[data-chart-ticker]").forEach(el => {
    el.addEventListener("click", () => openPriceChart(el.dataset.chartTicker));
  });
  // User menu toggle
  document.querySelector("[data-toggle-user-menu]")?.addEventListener("click", (e) => {
    e.stopPropagation();
    document.getElementById("user-menu")?.classList.toggle("open");
  });
  // Close user menu on outside click
  document.addEventListener("click", () => {
    document.getElementById("user-menu")?.classList.remove("open");
  });
  // User menu item clicks (Usage/Settings via dropdown)
  document.querySelectorAll(".user-menu__item[data-tab]").forEach(item => {
    item.addEventListener("click", () => {
      const tab = item.dataset.tab;
      state.activeTab = tab;
      location.hash = tab;
      document.getElementById("user-menu")?.classList.remove("open");
      render();
      if (tab === "usage" && !state.usageData) fetchUsage().then(render);
      if (tab === "settings") {
        Promise.all([fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus(), fetchFocus()]).then(render);
      }
    });
  });
  // Logout
  document.querySelector("[data-logout]")?.addEventListener("click", async () => {
    await fetch("/auth/logout", { method: "POST" });
    window.location.href = "/login";
  });
}

function _showHelpModal() {
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.innerHTML = `
    <div class="modal help-modal">
      <div class="modal__header">
        <h3>OpenClaw Guide</h3>
        <button class="modal__close">&times;</button>
      </div>
      <div class="modal__body help-content">
        <div class="help-section">
          <h4>What is OpenClaw?</h4>
          <p>OpenClaw is an automated geopolitical investment analysis tool that monitors global news, detects market-moving events, and generates actionable trading signals for European retail investors using <strong>bunq Stocks</strong> (Tradegate/Xetra).</p>
        </div>
        <div class="help-section">
          <h4>Tabs Explained</h4>
          <div class="help-item"><strong>Signals</strong> — Live trading signals with confidence scores from 6 independent components. Higher scores = stronger conviction.</div>
          <div class="help-item"><strong>Portfolio</strong> — Daily investment signals (bullish/bearish) with position sizing (Half-Kelly), risk levels (stop-loss, take-profit), and track record. Set up your holdings for personalised analysis.</div>
          <div class="help-item"><strong>Trees</strong> — Interactive decision trees showing geopolitical scenarios, probabilities, and stock impacts. Click nodes to explore branches.</div>
          <div class="help-item"><strong>Intel</strong> — Deep strategic analysis reports covering macro trends and sector outlook.</div>
          <div class="help-item"><strong>Usage</strong> — API cost breakdown by platform (Claude, Groq, OpenRouter) and purpose.</div>
          <div class="help-item"><strong>Settings</strong> — Configure budget, portfolio size, and focus mode.</div>
        </div>
        <div class="help-section">
          <h4>Signal Components</h4>
          <div class="help-item"><strong>geo</strong> — Geopolitical run-up score from news analysis</div>
          <div class="help-item"><strong>conf</strong> — Composite confidence from 6 sources (swarm, Polymarket, news acceleration, source convergence, ML)</div>
          <div class="help-item"><strong>swarm</strong> — 9-expert AI panel debating in 2 rounds</div>
          <div class="help-item"><strong>mom</strong> — Price momentum (14-day trend)</div>
          <div class="help-item"><strong>ins</strong> — Insider/OSINT intelligence</div>
        </div>
        <div class="help-section">
          <h4>Position Sizing</h4>
          <p>Uses Half-Kelly criterion: conservative sizing that accounts for win probability and reward-to-risk ratio. Maximum 20% per position, minimum 3%. Configure your portfolio size in Settings.</p>
        </div>
        <div class="help-section">
          <h4>Important Disclaimer</h4>
          <p>This tool provides informational analysis only and does NOT constitute investment advice under MiFID II. Always consult a licensed financial advisor. Past performance is not indicative of future results.</p>
        </div>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.querySelector(".modal__close").onclick = () => overlay.remove();
  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
}

/* ══════════════════════════════════════════════════════════════
   SIGNALS TAB
   ══════════════════════════════════════════════════════════════ */

function renderSignalsTab() {
  let html = "";

  // Stock signals first (most relevant for the user)
  html += renderStockSignals();

  // Flash alerts (breaking news)
  html += renderFlashAlertCards();

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
    return `<div class="usage-grid"><div class="empty-state"><div class="empty-state__icon">&#x23F3;</div><div>Loading...</div></div></div>`;
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
  const tier = b.tier || "premium";
  const ceiling = b.hard_ceiling_eur || ((b.daily_budget_eur || 2) + 1.0);
  const barColor = pctUsed > 100 ? "var(--red)" : pctUsed > 85 ? "var(--red)" : pctUsed > 60 ? "var(--yellow)" : "var(--green)";
  const totalTokens = (t.input_tokens || 0) + (t.output_tokens || 0);
  const tierColors = { premium: "var(--green)", standard: "var(--accent)", economy: "var(--yellow, #eab308)", emergency: "var(--orange, #f97316)", blocked: "var(--red)" };
  const tierColor = tierColors[tier] || "var(--text-dim)";

  return `<div class="usage-card usage-card--full">
    <div class="usage-card__header">
      <span class="usage-card__icon">&#x1F4B0;</span>
      <span>Budget Overview</span>
      <span class="budget-tier-badge" style="color:${tierColor};border-color:${tierColor};margin-left:8px">${tier.toUpperCase()}</span>
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
          <div class="usage-stat__label">Daily Budget</div>
          <div class="usage-stat__val">&euro;${b.daily_budget_eur?.toFixed(2) || "2.00"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Spent Today</div>
          <div class="usage-stat__val" style="color:${barColor}">&euro;${b.spent_today_eur?.toFixed(4) || "0.0000"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Remaining</div>
          <div class="usage-stat__val">&euro;${b.remaining_today_eur?.toFixed(4) || "0.0000"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Hard ceiling</div>
          <div class="usage-stat__val">&euro;${ceiling.toFixed(2)}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">Period Total (${u.period_days}d)</div>
          <div class="usage-stat__val">&euro;${t.cost_eur?.toFixed(4) || "0.0000"}</div>
        </div>
        <div class="usage-stat">
          <div class="usage-stat__label">API calls (${u.period_days}d)</div>
          <div class="usage-stat__val">${t.calls || 0}</div>
        </div>
      </div>
      <div class="usage-budget-bar">
        <div class="usage-budget-bar__fill" style="width:${Math.min(pctUsed, 100)}%;background:${barColor}"></div>
        <span class="usage-budget-bar__label">${pctUsed.toFixed(1)}% of daily budget</span>
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
        <span class="usage-platform-row__cost">${isFree ? '<span style="color:var(--green)">FREE</span>' : '&euro;' + data.cost_eur.toFixed(4)}</span>
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
    <div class="usage-card__body">${rows || '<div class="text-dim">No data</div>'}</div>
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
    <div class="usage-card__body">${rows || '<div class="text-dim">No data</div>'}</div>
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
      <td style="text-align:right;font-family:var(--mono)">${isFree ? '<span style="color:var(--green)">free</span>' : '&euro;' + data.cost_eur.toFixed(4)}</td>
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
          <th style="text-align:right">Cost</th>
        </tr></thead>
        <tbody>${rows || '<tr><td colspan="6" class="text-dim">No data</td></tr>'}</tbody>
      </table>
    </div>
  </div>`;
}

function renderUsageDailyCard(u) {
  const history = u.daily_history || [];
  if (!history.length) {
    return `<div class="usage-card usage-card--full">
      <div class="usage-card__header"><span class="usage-card__icon">&#x1F4C8;</span><span>Daily Usage</span></div>
      <div class="usage-card__body"><div class="text-dim">No history available</div></div>
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
      <span>Daily Usage</span>
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
    Promise.all([fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus()]).then(render);
  }

  let html = `<div class="settings-grid">`;

  // Focus Mode / Storylines (full-width, first)
  html += renderSettingsFocus();

  // API Keys
  html += renderSettingsKeys();

  // Budget
  html += renderSettingsBudget();

  // Swarm
  html += renderSettingsSwarm();

  // Feeds
  html += renderSettingsFeeds();

  html += `</div>`;
  return html;
}

function renderSettingsFocus() {
  const focus = state.focus || { focused_runup_ids: [] };
  const focusedIds = new Set(focus.focused_runup_ids || []);
  const allRunups = state.runups || [];
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
  candidates.sort((a, b) => (b.current_score || b.score || 0) - (a.current_score || a.score || 0));

  let html = `<div class="settings-card settings-card--full">
    <div class="settings-card__title">
      <span>Storylines</span>
    </div>
    <div class="storylines-auto-note">
      Storylines are automatically detected and tracked. The system focuses on the most significant narratives.
      Cross-region merging, priority tree generation, swarm evaluation, and price tracking are applied automatically based on narrative score.
    </div>`;

  if (candidates.length === 0) {
    html += `<div style="font-size:0.82rem;color:var(--text-muted);padding:12px 0">No storylines detected yet. The engine is still building narratives.</div>`;
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
        ${isFocused ? `<span class="storyline-tracked-badge">Tracked</span>` : ""}
      </div>`;
    }
    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

function renderSettingsKeys() {
  const ak = state.apiKeyStatus || {};
  const sw = state.swarmStatus || {};
  const keys = [
    { name: "Anthropic (Claude)", id: "anthropic", configured: ak.has_key, endpoint: "settings/api-key", field: "api_key" },
    { name: "Groq", id: "groq", configured: sw.groq_configured, endpoint: "swarm/config", field: "groq_api_key" },
    { name: "OpenRouter", id: "openrouter", configured: sw.openrouter_configured, endpoint: "swarm/config", field: "openrouter_api_key" },
  ];

  let html = `<div class="settings-card">
    <div class="settings-card__title">API Keys</div>`;

  for (const k of keys) {
    const statusClass = k.configured ? "settings-row__val--ok" : "settings-row__val--warn";
    const statusText = k.configured ? "Configured" : "Not set";
    html += `<div class="api-key-block">
      <div class="settings-row">
        <span class="settings-row__label">${k.name}</span>
        <span class="settings-row__val ${statusClass}" id="apikey-status-${k.id}">${statusText}</span>
      </div>
      <div class="api-key-input-row">
        <input type="password" class="form-input api-key-input" id="apikey-input-${k.id}"
               placeholder="Paste new key..." autocomplete="off">
        <button class="btn btn--sm btn--primary" data-save-apikey="${k.id}"
                data-endpoint="${k.endpoint}" data-field="${k.field}">Save</button>
      </div>
      <div class="api-key-msg" id="apikey-msg-${k.id}"></div>
    </div>`;
  }

  html += `</div>`;
  return html;
}

function renderSettingsBudget() {
  const b = state.budget || {};
  const daily = b.daily_budget_eur || 2.00;
  const spent = b.spent_today_eur || 0;
  const ceiling = b.hard_ceiling_eur || (daily + 1.0);
  const tier = b.tier || "premium";
  const spillover = b.spillover_eur || 1.0;
  const pctUsed = daily > 0 ? Math.min(100, (spent / daily) * 100) : 0;
  const pctCeiling = ceiling > 0 ? Math.min(100, (spent / ceiling) * 100) : 0;

  const tierColors = {
    premium: "var(--green)", standard: "var(--accent)",
    economy: "var(--yellow, #eab308)", emergency: "var(--orange, #f97316)",
    blocked: "var(--red)",
  };
  const tierColor = tierColors[tier] || "var(--text-dim)";
  const tierDescs = {
    premium: "Best models (Sonnet + paid swarm)",
    standard: "Haiku for trees, Sonnet for advisory",
    economy: "Haiku everywhere, free swarm fallbacks",
    emergency: "Essential calls only, all free models",
    blocked: "All paid API calls blocked",
  };
  const fillCls = pctUsed > 100 ? "budget-bar__fill--danger"
    : pctUsed > 85 ? "budget-bar__fill--danger"
    : pctUsed > 60 ? "budget-bar__fill--warn" : "";

  return `<div class="settings-card">
    <div class="settings-card__title">Token Budget</div>
    <div class="settings-row">
      <span class="settings-row__label">Daily limit</span>
      <div class="budget-input-group">
        <span class="budget-currency">\u20AC</span>
        <input type="number" class="budget-input" id="budget-input"
               value="${daily.toFixed(2)}" min="0.50" max="100" step="0.50"
               data-original="${daily.toFixed(2)}">
        <button class="btn btn--sm btn--primary" id="budget-save-btn"
                style="display:none">Save</button>
      </div>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Spent today</span>
      <span class="settings-row__val">\u20AC${spent.toFixed(3)} (${pctUsed.toFixed(0)}%)</span>
    </div>
    <div class="budget-bar"><div class="budget-bar__fill ${fillCls}" style="width:${Math.min(pctUsed, 100)}%"></div></div>
    <div class="settings-row">
      <span class="settings-row__label">Hard ceiling</span>
      <span class="settings-row__val">\u20AC${ceiling.toFixed(2)} (budget + \u20AC${spillover.toFixed(2)} spillover)</span>
    </div>
    <div class="settings-row">
      <span class="settings-row__label">Current tier</span>
      <span class="budget-tier-badge" style="color:${tierColor};border-color:${tierColor}">${tier.toUpperCase()}</span>
    </div>
    <div class="budget-tier-desc">${tierDescs[tier] || ""}</div>
    <div class="settings-row">
      <span class="settings-row__label">Monthly est.</span>
      <span class="settings-row__val">\u20AC${(spent * 30).toFixed(2)}</span>
    </div>
  </div>`;
}

function renderSettingsPortfolioSize() {
  const size = state.portfolioSize || 5000;
  return `<div class="settings-card">
    <div class="settings-card__title">Portfolio Size</div>
    <div class="settings-row">
      <span class="settings-row__label">Assumed portfolio value</span>
      <div class="budget-input-group">
        <span class="budget-currency">\u20AC</span>
        <input type="number" class="budget-input" id="portfolio-size-input"
               value="${size}" min="100" max="10000000" step="500"
               data-original="${size}">
        <button class="btn btn--sm btn--primary" id="portfolio-size-save-btn"
                style="display:none">Save</button>
      </div>
    </div>
    <div class="settings-row">
      <span class="settings-row__label" style="color:var(--text-dim);font-size:0.75rem">
        Used for Half-Kelly position sizing calculations. Override this if your actual portfolio differs from holdings-based valuation.
      </span>
    </div>
  </div>`;
}

function renderSettingsSwarm() {
  const sw = state.swarmStatus || {};
  const interval = sw.interval_minutes || 60;
  const costDay = ((24 * 60 / interval) * 0.02).toFixed(2);
  const costMonth = ((24 * 60 / interval) * 0.02 * 30).toFixed(0);

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
      <div class="budget-input-group">
        <input type="number" class="budget-input" id="swarm-interval-input"
               value="${interval}" min="10" max="120" step="5"
               data-original="${interval}">
        <span style="color:var(--text-dim);font-size:0.78rem">min</span>
        <button class="btn btn--sm btn--primary" id="swarm-interval-save-btn"
                style="display:none">Save</button>
      </div>
    </div>
    <div class="swarm-cost-estimate" id="swarm-cost-estimate">
      Estimated cost: ~\u20AC${costDay}/day (~\u20AC${costMonth}/month)
    </div>
    ${sw.verdicts_by_type ? `<div style="display:flex;gap:8px;margin-top:8px">
      ${Object.entries(sw.verdicts_by_type).map(([k, v]) => `<span class="verdict-badge ${verdictClass(k)}">${k}: ${v}</span>`).join("")}
    </div>` : ""}
  </div>`;
}

function updateSwarmCostEstimate() {
  const input = document.getElementById("swarm-interval-input");
  const el = document.getElementById("swarm-cost-estimate");
  if (!input || !el) return;
  const interval = Math.max(10, Math.min(120, parseInt(input.value) || 60));
  const costDay = ((24 * 60 / interval) * 0.02).toFixed(2);
  const costMonth = ((24 * 60 / interval) * 0.02 * 30).toFixed(0);
  el.textContent = `Estimated cost: ~\u20AC${costDay}/day (~\u20AC${costMonth}/month)`;
}

function _feedRegion(f) {
  if (f.region) return f.region;
  const name = (f.name || f.url || "").toLowerCase();
  const regionMap = [
    [/middle.east|israel|iran|syria|gaza|lebanon|iraq|saudi|gulf|yemen|jordan|egypt/i, "Middle East"],
    [/europe|eu\b|france|germany|uk\b|britain|spain|italy|poland|ukraine|nato/i, "Europe"],
    [/asia|china|japan|india|korea|taiwan|asean|pacific/i, "Asia-Pacific"],
    [/africa|nigeria|kenya|south.africa|ethiopia/i, "Africa"],
    [/latin|brazil|mexico|argentina|colombia|americas/i, "Americas"],
    [/us\b|usa|america|washington|congress|pentagon|fed\b|wall.street/i, "United States"],
    [/crypto|bitcoin|ethereum|defi|web3/i, "Crypto"],
    [/tech|ai\b|semiconductor|silicon/i, "Technology"],
  ];
  for (const [re, region] of regionMap) {
    if (re.test(name)) return region;
  }
  return "General";
}

function renderSettingsFeeds() {
  const feeds = state.feeds || [];
  const active = feeds.filter(f => f.enabled !== false).length;

  // Group feeds by region
  const regionGroups = {};
  for (const f of feeds) {
    const region = _feedRegion(f);
    if (!regionGroups[region]) regionGroups[region] = [];
    regionGroups[region].push(f);
  }
  const sortedRegions = Object.keys(regionGroups).sort();

  let html = `<div class="settings-card settings-card--full">
    <div class="settings-card__title">RSS Feeds (${active} active / ${feeds.length} total)
      <button class="btn btn--sm" data-add-feed-show>Add Feed</button>
    </div>

    <div class="feed-add-form" id="feed-add-form" style="display:none">
      <div class="form-row" style="margin-bottom:8px">
        <input class="form-input" id="feed-inline-url" placeholder="RSS URL" style="flex:2">
        <select class="form-input" id="feed-inline-region" style="flex:1">
          <option value="General">General</option>
          <option value="Middle East">Middle East</option>
          <option value="Europe">Europe</option>
          <option value="Asia-Pacific">Asia-Pacific</option>
          <option value="United States">United States</option>
          <option value="Americas">Americas</option>
          <option value="Africa">Africa</option>
          <option value="Crypto">Crypto</option>
          <option value="Technology">Technology</option>
        </select>
        <button class="btn btn--primary btn--sm" data-add-feed-submit>Add</button>
      </div>
    </div>

    <div class="settings-row">
      <span class="settings-row__label">Articles fetched</span>
      <span class="settings-row__val">${state.status?.total_articles || 0}</span>
    </div>`;

  for (const region of sortedRegions) {
    const regionFeeds = regionGroups[region];
    const regionActive = regionFeeds.filter(f => f.enabled !== false).length;
    const regionId = region.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
    html += `<div class="feed-region-group">
      <div class="feed-region-header" data-toggle-region="${regionId}">
        <span class="feed-region-arrow" id="feed-arrow-${regionId}">\u25B6</span>
        <span class="feed-region-name">${esc(region)}</span>
        <span class="feed-region-count">${regionActive}/${regionFeeds.length} feeds</span>
      </div>
      <div class="feed-region-body" id="feed-region-${regionId}" style="display:none">`;
    for (const f of regionFeeds) {
      const enabled = f.enabled !== false;
      const articles = f.article_count || 0;
      html += `<div class="feed-row-inline">
          <span class="feed-row-inline__name ${enabled ? "" : "text-dim"}">${esc(f.name || f.url || "Unknown")}</span>
          <span class="feed-row-inline__articles">${articles} art.</span>
          <label class="feed-toggle">
            <input type="checkbox" ${enabled ? "checked" : ""} data-feed-toggle="${f.id}">
            <span class="feed-toggle__slider"></span>
          </label>
        </div>`;
    }
    html += `</div></div>`;
  }

  html += `</div>`;
  return html;
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
  if (_priceChartObserver) { _priceChartObserver.disconnect(); _priceChartObserver = null; }
  if (_priceChartInstance) { try { _priceChartInstance.remove(); } catch {} _priceChartInstance = null; }
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
      authFetch(`${API_BASE}?_api=price&ticker=${encodeURIComponent(ticker)}`),
      authFetch(`${API_BASE}?_api=price-chart&ticker=${encodeURIComponent(ticker)}&period=${period}`),
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
      _priceChartInstance = chart;
      _priceChartObserver = new ResizeObserver(() => chart.applyOptions({ width: chartContainer.clientWidth }));
      _priceChartObserver.observe(chartContainer);
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
      <div class="advisory-empty__text">No advisory generated yet.<br>The system will automatically generate your first daily advisory shortly.</div>
    </div>`;
  }

  const data = adv.advisory;
  const buys = data.buy_recommendations || [];
  const sells = data.sell_recommendations || [];
  const stance = data.market_stance || "neutral";
  const ctx = data.market_context || {};
  const genAt = data.generated_at ? new Date(data.generated_at).toLocaleString() : "—";

  let html = "";

  // Recent Activity Ticker
  html += renderActivityTicker();

  // Header with countdown to next analysis
  const _nextAdvisoryUTC = (() => {
    const now = new Date();
    const utcH = now.getUTCHours();
    const utcM = now.getUTCMinutes();
    // Advisory schedule: 06:30, 10:00, 14:30, 18:00 UTC
    const schedule = [[6,30],[10,0],[14,30],[18,0]];
    for (const [h,m] of schedule) {
      if (utcH < h || (utcH === h && utcM < m)) {
        const next = new Date(now);
        next.setUTCHours(h, m, 0, 0);
        return next;
      }
    }
    // All passed today → first one tomorrow
    const next = new Date(now);
    next.setUTCDate(next.getUTCDate() + 1);
    next.setUTCHours(schedule[0][0], schedule[0][1], 0, 0);
    return next;
  })();
  const _minsUntil = Math.max(0, Math.round((_nextAdvisoryUTC - Date.now()) / 60000));
  const _countdownText = _minsUntil >= 60
    ? `${Math.floor(_minsUntil/60)}h ${_minsUntil%60}m`
    : `${_minsUntil}m`;

  // Swarm countdown (runs every 60min)
  const swarmStatus = (state.swarmActivity && state.swarmActivity.status) || {};
  let swarmMeta = "";
  if (swarmStatus.next_run) {
    const swarmDiff = new Date(swarmStatus.next_run) - Date.now();
    if (swarmDiff > 0) {
      const swmMins = Math.floor(swarmDiff / 60000);
      swarmMeta = `Swarm active · Next verdict in <strong>${swmMins}m</strong>`;
    } else {
      swarmMeta = `Swarm active · <strong>In progress...</strong>`;
    }
  } else {
    swarmMeta = `Next advisory in <strong>${_countdownText}</strong>`;
  }

  html += `<div class="advisory-header">
    <div>
      <div class="advisory-header__title">
        📊 Daily Advisory — ${data.generated_at ? data.generated_at.slice(0, 10) : "Today"}
        <span class="stance-badge stance-badge--${esc(stance.replace(/[^a-z_]/gi, ""))}">${esc(_stanceLabel(stance))}</span>
      </div>
      <div class="advisory-header__meta">
        ${swarmMeta}
        <span style="opacity:0.4;margin-left:8px">· Updated ${genAt}</span>
      </div>
    </div>
    <div style="display:flex;gap:8px;align-items:center">
      <button class="btn btn--sm btn--ghost" data-open-feed style="margin-top:0">📰 Feed</button>
    </div>
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

  // ── INVESTMENT SWARM V3 ─────────────────────────────────────
  const _swarmV3 = adv.swarm_v3 || null;
  const _swarmIsLatest = !!adv.swarm_is_latest;
  const narrativeText = data.narrative_summary || data.delta_narrative || "";

  if (_swarmV3 && _swarmIsLatest) {
    // Swarm V3 is primary: show swarm summary as main narrative, old advisory collapsed
    html += renderSwarmV3(_swarmV3);
    if (narrativeText) {
      html += `<details class="prev-advisory-collapse" style="
        margin-bottom:12px;
        background:rgba(255,255,255,0.02);
        border:1px solid rgba(255,255,255,0.05);
        border-radius:8px;
        padding:0;
      ">
        <summary style="
          cursor:pointer;
          padding:10px 14px;
          font-size:0.78rem;
          color:var(--text-dim);
          user-select:none;
        ">Previous advisory</summary>
        <div style="padding:4px 14px 12px;font-size:0.82rem;line-height:1.5;color:var(--text-muted)">${_esc(narrativeText)}</div>
      </details>`;
    }
  } else if (_swarmV3) {
    // Swarm V3 exists but is not latest: show old narrative first, then swarm V3 below
    if (narrativeText) {
      html += `<div class="narrative-summary">${_esc(narrativeText)}</div>`;
    }
    html += renderSwarmV3(_swarmV3);
  } else {
    // No swarm V3: original behavior
    if (narrativeText) {
      html += `<div class="narrative-summary">${_esc(narrativeText)}</div>`;
    }
  }

  // Risk banner
  if (data.risk_warning) {
    html += `<div class="risk-banner">
      <span class="risk-banner__icon">⚠️</span>
      <span>${_esc(data.risk_warning)}</span>
    </div>`;
  }

  // MiFID II Disclaimer (dismissible — remembers via localStorage)
  if (!localStorage.getItem("oc_disclaimer_dismissed")) {
    html += `<div class="mifid-disclaimer" id="mifid-disclaimer">
      <div class="mifid-disclaimer__header">
        <span class="mifid-disclaimer__icon">&#x2696;</span>
        <span class="mifid-disclaimer__title">Regulatory Disclaimer (MiFID II)</span>
        <button class="mifid-disclaimer__close" data-dismiss-disclaimer title="Dismiss">&times;</button>
      </div>
      <div class="mifid-disclaimer__body">
        ${_esc(data.disclaimer || "This tool provides informational analysis only and does NOT constitute investment advice under MiFID II (Directive 2014/65/EU). OpenClaw is not a licensed investment firm. All recommendations are generated by automated algorithms and should not be relied upon as a sole basis for investment decisions. Past performance is not indicative of future results. You may lose some or all of your invested capital. Always consult a licensed financial advisor before making investment decisions.")}
      </div>
    </div>`;
  }

  // ── SWARM INTELLIGENCE FEED ─────────────────────────────────
  html += renderSwarmFeed();

  // ── MY PORTFOLIO (alignment with advisory) ──────────────────
  html += renderMyPortfolio();

  // Recommendations — merged buy + sell (sell filtered to portfolio holdings only)
  const alignItems = (state.portfolioAlignment && state.portfolioAlignment.alignment) || [];
  const holdingsList = (state.advisory || {}).portfolio_holdings || [];
  const portfolioTickers = new Set([
    ...alignItems.map(h => (h.ticker || "").toUpperCase()),
    ...holdingsList.map(h => (h.ticker || "").toUpperCase())
  ]);
  const filteredSells = sells.filter(rec => portfolioTickers.has((rec.ticker || "").toUpperCase()));
  const totalRecs = buys.length + filteredSells.length;

  html += `<div class="advisory-section-label">📋 Recommendations (${totalRecs})</div>`;
  if (totalRecs > 0) {
    html += `<div class="advisory-grid">`;
    for (const rec of buys) {
      html += renderAdvisoryCard(rec, "buy");
    }
    for (const rec of filteredSells) {
      html += renderAdvisoryCard(rec, "sell");
    }
    html += `</div>`;
  } else {
    html += `<div class="advisory-empty-section">
      <div class="advisory-empty-section__title">No recommendations right now</div>
      <div class="advisory-empty-section__reason">
        The expert panel found no assets meeting our risk-adjusted criteria in the current market environment.
        With a ${stance} stance, the swarm recommends caution until clearer signals emerge.
      </div>
    </div>`;
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

  // ── STORYLINES (decision trees from run-ups) ────────────────
  html += renderStorylines();

  return html;
}

function renderStorylines() {
  const runups = (state.runups || []).filter(r => r.status === "active" || r.is_focused);
  if (runups.length === 0) return "";

  let html = `<div class="storylines-section">
    <div class="section-title">📖 Storylines <span class="badge">${runups.length}</span></div>
    <div class="storylines-grid">`;

  for (const r of runups.slice(0, 8)) {
    const prob = r.root_probability != null ? Math.round(r.root_probability) : "?";
    const articles = r.article_count || r.article_count_total || 0;
    const swarm = r.swarm_verdict || "";
    const swarmColor = swarm === "BUY" ? "var(--green)" : swarm === "SELL" ? "var(--red)" : "var(--yellow)";
    const name = (r.narrative_name || r.name || "").replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());

    html += `<div class="storyline-card" data-storyline-id="${r.id}" style="cursor:pointer">
      <div class="storyline-card__name">${_esc(name)}</div>
      <div class="storyline-card__meta">
        <span class="storyline-card__prob">${prob}%</span>
        <span class="storyline-card__articles">${articles} articles</span>
        ${swarm ? `<span class="storyline-card__swarm" style="color:${swarmColor}">${swarm}</span>` : ""}
      </div>
      <div class="storyline-card__question">${_esc((r.root_question || "").slice(0, 120))}</div>
    </div>`;
  }

  html += `</div></div>`;
  return html;
}

async function _showStorylineDetail(runupId) {
  document.getElementById("storyline-panel")?.remove();
  document.querySelector(".feed-backdrop")?.remove();

  // Create backdrop
  const backdrop = document.createElement("div");
  backdrop.className = "feed-backdrop";
  document.body.appendChild(backdrop);
  requestAnimationFrame(() => backdrop.classList.add("open"));

  // Create panel
  const panel = document.createElement("div");
  panel.id = "storyline-panel";
  panel.className = "feed-panel";
  panel.innerHTML = `
    <div class="feed-panel__header">
      <h3>\u{1F4D6} Storyline</h3>
      <button class="feed-panel__close" data-close-storyline>&times;</button>
    </div>
    <div class="feed-panel__articles" id="storyline-content">
      <div class="feed-panel__loading">Loading storyline...</div>
    </div>
  `;
  document.body.appendChild(panel);
  requestAnimationFrame(() => panel.classList.add("open"));

  // Close handlers
  const closePanel = () => {
    panel.classList.remove("open");
    backdrop.classList.remove("open");
    setTimeout(() => { panel.remove(); backdrop.remove(); }, 300);
  };
  panel.querySelector("[data-close-storyline]").addEventListener("click", closePanel);
  backdrop.addEventListener("click", closePanel);
  document.addEventListener("keydown", function escHandler(e) {
    if (e.key === "Escape") { closePanel(); document.removeEventListener("keydown", escHandler); }
  });

  // Fetch tree data
  try {
    const r = await authFetch(`${API_BASE}?_api=tree&id=${runupId}`);
    if (!r.ok) throw new Error("Failed to load");
    const tree = await r.json();
    const contentEl = document.getElementById("storyline-content");
    if (!contentEl) return;

    const name = (tree.narrative_name || tree.name || "").replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    let body = "";

    // Narrative title
    if (name) {
      body += `<div style="font-size:18px;font-weight:700;margin-bottom:16px;color:var(--text)">${_esc(name)}</div>`;
    }

    // Root question with probability bars
    if (tree.root_question) {
      const yesProb = tree.root_probability != null ? Math.round(tree.root_probability) : null;
      const noProb = yesProb != null ? (100 - yesProb) : null;
      body += `<div style="padding:12px;margin-bottom:12px;background:var(--bg-card);border:1px solid var(--border);border-radius:8px">
        <div style="font-weight:600;margin-bottom:8px">${_esc(tree.root_question)}</div>`;
      if (yesProb != null) {
        body += `<div style="display:flex;gap:4px;height:22px;border-radius:6px;overflow:hidden;font-size:12px;font-weight:700;margin-bottom:4px">
          <div style="flex:${yesProb};background:var(--green);color:#fff;display:flex;align-items:center;justify-content:center;min-width:32px">YES ${yesProb}%</div>
          <div style="flex:${noProb};background:var(--red);color:#fff;display:flex;align-items:center;justify-content:center;min-width:32px">NO ${noProb}%</div>
        </div>`;
      } else {
        body += `<div style="display:flex;gap:12px;font-weight:700">
          <span style="color:var(--green)">YES ?%</span>
          <span style="color:var(--red)">NO ?%</span>
        </div>`;
      }
      body += `</div>`;
    }

    // Decision nodes
    const nodes = tree.nodes || tree.decision_nodes || [];
    if (nodes.length > 0) {
      body += `<div style="font-size:13px;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;margin:16px 0 8px">Decision Nodes</div>`;
      for (const node of nodes) {
        const keywords = (node.keywords || []).map(k => `<span class="badge" style="margin:2px">${_esc(k)}</span>`).join(" ");
        body += `<div style="padding:10px;margin:8px 0;background:var(--bg-card);border:1px solid var(--border);border-radius:8px">
          <div style="font-weight:600;margin-bottom:4px">${_esc(node.question || node.title || "")}</div>
          <div style="display:flex;gap:10px;font-size:12px;margin-bottom:4px">
            <span style="color:var(--green)">YES ${node.yes_probability != null ? Math.round(node.yes_probability * 100) + "%" : "?"}</span>
            <span style="color:var(--red)">NO ${node.no_probability != null ? Math.round(node.no_probability * 100) + "%" : "?"}</span>
            ${node.status ? `<span style="color:var(--text-dim)">Status: ${_esc(node.status)}</span>` : ""}
          </div>
          ${keywords ? `<div style="margin-top:4px">${keywords}</div>` : ""}`;

        // Consequences
        const consequences = node.consequences || [];
        if (consequences.length > 0) {
          body += `<div style="margin-top:6px;font-size:12px;color:var(--text-dim)">`;
          for (const c of consequences) {
            body += `<div style="padding:4px 0;border-top:1px solid var(--border)">
              <span>${_esc(c.description || "")}</span>
              ${c.probability != null ? `<span style="margin-left:8px;font-weight:600">${Math.round(c.probability * 100)}%</span>` : ""}`;
            const impacts = c.stock_impacts || c.impacts || [];
            if (impacts.length > 0) {
              body += `<div style="margin-top:2px">`;
              for (const imp of impacts) {
                const impColor = (imp.direction === "up" || imp.impact > 0) ? "var(--green)" : "var(--red)";
                body += `<span style="color:${impColor};margin-right:6px">${_esc(imp.ticker || imp.stock || "")} ${_esc(imp.direction || "")}</span>`;
              }
              body += `</div>`;
            }
            body += `</div>`;
          }
          body += `</div>`;
        }
        body += `</div>`;
      }
    }

    // Swarm verdict
    const verdict = tree.swarm_verdict || tree.verdict;
    if (verdict) {
      body += `<div style="font-size:13px;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;margin:16px 0 8px">Swarm Verdict</div>`;
      body += `<div style="padding:12px;background:var(--bg-card);border:1px solid var(--border);border-radius:8px;white-space:pre-wrap">${_esc(typeof verdict === "object" ? JSON.stringify(verdict, null, 2) : verdict)}</div>`;
    }

    contentEl.innerHTML = body;
  } catch (e) {
    const contentEl = document.getElementById("storyline-content");
    if (contentEl) contentEl.innerHTML = '<div class="feed-panel__empty">Failed to load storyline data</div>';
  }
}

function renderMyPortfolio() {
  const align = state.portfolioAlignment;
  // Also check for holdings in advisory response
  const holdings = (state.advisory || {}).portfolio_holdings || [];

  if ((!align || !align.alignment || align.alignment.length === 0) && holdings.length === 0) {
    return `<div class="portfolio-holdings portfolio-holdings--empty">
      <div class="advisory-section-label">💼 My Portfolio</div>
      <div class="portfolio-holdings__empty-msg">
        No portfolio configured. Add your Bunq positions to receive personalised advisory recommendations.
      </div>
      <button class="btn btn--sm" data-edit-portfolio>Set Up Portfolio</button>
    </div>`;
  }

  const items = (align && align.alignment) ? align.alignment : [];
  const missed = (align && align.missed_opportunities) ? align.missed_opportunities : [];
  const totalValue = items.reduce((sum, h) => sum + (h.value_eur || 0), 0);
  const totalPnl = items.reduce((sum, h) => sum + (h.pnl_eur || 0), 0);

  // Price freshness indicator — prices are fetched live on each page load
  const freshnessLabel = "🟢 Live";

  // Portfolio staleness check (warn if holdings not updated in >7 days)
  const portfolioUpdatedAt = (state.advisory || {}).portfolio_updated_at;
  let stalenessWarning = "";
  if (portfolioUpdatedAt) {
    const updTs = new Date(portfolioUpdatedAt);
    const ageDays = !isNaN(updTs.getTime()) ? Math.floor((Date.now() - updTs.getTime()) / 86400000) : -1;
    if (ageDays >= 7) {
      stalenessWarning = `<div class="portfolio-stale-banner">
        ⚠️ Portfolio not updated in ${ageDays} days. Sizing recommendations may be inaccurate.
        <button class="btn btn--xs" data-edit-portfolio style="margin-left:8px">Update Now</button>
      </div>`;
    }
  }

  let html = `<div class="portfolio-holdings">
    <div class="advisory-section-label">
      💼 My Portfolio
      <span class="portfolio-total">${totalValue > 0 ? `€${formatNum(totalValue, 2)}` : ""}</span>
      <span class="portfolio-price-time">Prices as of ${new Date().toLocaleTimeString("nl-NL", { hour: "2-digit", minute: "2-digit" })}</span>
      ${freshnessLabel ? `<span class="portfolio-freshness">${freshnessLabel}</span>` : ""}
      <button class="btn btn--xs" data-edit-portfolio style="margin-left:auto">Edit</button>
    </div>`;

  // Staleness warning banner
  html += stalenessWarning;

  // P&L Summary Card
  if (items.some(h => h.pnl_eur != null && h.pnl_eur !== 0)) {
    const pnlCls = totalPnl >= 0 ? "pnl-card--profit" : "pnl-card--loss";
    const pnlSign = totalPnl >= 0 ? "+" : "";
    html += `<div class="pnl-card ${pnlCls}">
      <div class="pnl-card__header">
        <span class="pnl-card__icon">${totalPnl >= 0 ? "📈" : "📉"}</span>
        <span class="pnl-card__label">Total P&L</span>
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

      const portfolioDetailData = {
        ticker: h.ticker, name: h.name, value_eur: h.value_eur || 0,
        pnl_eur: hPnl, label: h.label, signal: signal,
        reasoning: h.reasoning || "", composite_score: h.composite_score,
        source: h.source || "", sector: h.sector || "",
        current_price: h.current_price, avg_buy_price_eur: h.avg_buy_price_eur,
        mid_term_outlook: h.mid_term_outlook || "", sector_context: h.sector_context || "",
      };
      html += `<div class="portfolio-card portfolio-card--${signalCls}" data-portfolio-detail='${_esc(JSON.stringify(portfolioDetailData)).replace(/'/g, "&#39;")}' style="cursor:pointer">
        <div class="portfolio-card__top">
          <span class="portfolio-card__ticker" data-chart-ticker="${_esc(h.ticker)}">${_esc(h.ticker)}</span>
          <span class="portfolio-card__value">€${formatNum(h.value_eur || 0, 2)}</span>
        </div>
        ${hPnl !== 0 ? `<div class="portfolio-card__pnl ${hPnlCls}">${hPnlSign}€${formatNum(hPnl, 2)}</div>` : ""}
        <div class="portfolio-card__name">${_esc(h.name)}</div>
        <div class="portfolio-card__signal portfolio-card__signal--${signalCls}">
          ${_esc(h.label)}
        </div>
        ${h.reasoning ? `<div class="portfolio-card__reasoning">${_esc(typeof h.reasoning === "object" ? h.reasoning.thesis || "" : h.reasoning)}</div>` : ""}
        ${h.composite_score != null ? `<div class="portfolio-card__score">Score: ${Number(h.composite_score).toFixed(2)}</div>` : ""}
        ${h.source && h.source !== "none" ? `<div class="portfolio-card__source">${{"advisory":"📋 Advisory","live_signal":"⚡ Live signaal","swarm":"🐝 Swarm consensus","advisory_sector":"📋 Sector outlook","sector_detect":"📊 Sector"}[h.source] || h.source}</div>` : ""}
      </div>`;
    }
    html += `</div>`;
  }

  // Missed opportunities
  if (missed.length > 0) {
    html += `<div class="portfolio-missed">
      <div class="portfolio-missed__title">💡 Missed Opportunities — not in portfolio</div>
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
  const rawReasoning = rec.reasoning || "";
  const reasoning = typeof rawReasoning === "object" ? rawReasoning : { thesis: rawReasoning };
  const hasReasoning = reasoning.thesis || "";
  const risk = rec.risk_levels || {};
  const sizing = rec.position_sizing || {};

  // Store reasoning + risk data for popup
  const popupData = { reasoning, risk_levels: risk, position_sizing: sizing };
  const dataAttr = `data-reasoning='${_esc(JSON.stringify(popupData)).replace(/'/g, "&#39;")}'`;

  let html = `<div class="advisory-card advisory-card--${type} advisory-card--clickable" ${dataAttr} data-ticker="${_esc(ticker)}" data-name="${_esc(name)}" data-type="${type}">`;
  html += `<div class="advisory-card__top">
    <span class="advisory-card__ticker advisory-card__ticker--${type}">${_esc(ticker)}</span>
    ${price != null ? `<span class="advisory-card__price">$${formatNum(price, 2)}</span>` : ""}
  </div>`;
  html += `<div class="advisory-card__name">${_esc(name)}</div>`;
  html += `<div class="advisory-card__score advisory-card__score--${type}">
    Score: ${(typeof score === "number" ? score : Number(score) || 0).toFixed(2)}
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

  // Position sizing badge (with confidence interval)
  if (sizing.position_pct) {
    const hasCI = sizing.ci_lo_shares != null && sizing.ci_hi_shares != null && sizing.ci_lo_shares !== sizing.ci_hi_shares;
    const sharesLabel = hasCI
      ? `${sizing.ci_lo_shares}–${sizing.ci_hi_shares} shares`
      : (sizing.shares ? `${sizing.shares} shares` : "");
    const eurLabel = hasCI && sizing.ci_lo_eur != null && sizing.ci_hi_eur != null
      ? `€${formatNum(sizing.ci_lo_eur, 0)}–${formatNum(sizing.ci_hi_eur, 0)}`
      : (sizing.eur_amount ? `€${formatNum(sizing.eur_amount, 0)}` : "");
    const pctLabel = hasCI && sizing.ci_lo_pct != null && sizing.ci_hi_pct != null
      ? `${sizing.ci_lo_pct}–${sizing.ci_hi_pct}%`
      : `${sizing.position_pct}%`;
    html += `<div class="sizing-badge">
      <span class="sizing-badge__pct">${pctLabel}</span>
      <span class="sizing-badge__label">portfolio</span>
      ${sharesLabel ? `<span class="sizing-badge__shares">${sharesLabel}</span>` : ""}
      ${eurLabel ? `<span class="sizing-badge__eur">${eurLabel}</span>` : ""}
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
    html += `<div class="advisory-card__teaser">${_esc(teaser)}${teaser.length >= 80 ? "…" : ""} <span class="advisory-card__tap-hint">tap for detail</span></div>`;
  }

  // Feedback buttons
  html += `<div class="advisory-card__feedback" data-feedback-ticker="${_esc(ticker)}" data-feedback-action="${type === "buy" ? "BUY" : "SELL"}">
    <button class="feedback-btn feedback-btn--agree" data-rating="agree" title="Agree">&#x1F44D;</button>
    <button class="feedback-btn feedback-btn--partial" data-rating="partially_agree" title="Partially agree">&#x1F914;</button>
    <button class="feedback-btn feedback-btn--disagree" data-rating="disagree" title="Disagree">&#x1F44E;</button>
  </div>`;

  html += `</div>`;
  return html;
}

function _showPortfolioDetail(card) {
  let data;
  try {
    data = JSON.parse(card.dataset.portfolioDetail);
  } catch { return; }

  const ticker = data.ticker || "?";
  const name = data.name || "";
  const pnl = data.pnl_eur || 0;
  const pnlCls = pnl >= 0 ? "portfolio-card__pnl--profit" : "portfolio-card__pnl--loss";
  const pnlSign = pnl >= 0 ? "+" : "";
  const label = data.label || "—";
  const signal = data.signal || "NEUTRAL";
  const signalCls = {
    HOLD_ADD: "hold-add", REDUCE: "reduce", HOLD: "hold",
    WATCH: "watch", NEUTRAL: "neutral",
  }[signal] || "neutral";

  let body = "";

  // Price & P&L summary
  body += `<div class="advisory-detail__section">
    <div class="advisory-detail__label">Price & P&L</div>
    <div class="advisory-detail__text">
      ${data.current_price != null ? `Current price: $${formatNum(data.current_price, 2)}<br>` : ""}
      ${data.avg_buy_price_eur != null ? `Avg buy: €${formatNum(data.avg_buy_price_eur, 2)}<br>` : ""}
      Value: €${formatNum(data.value_eur || 0, 2)}<br>
      P&L: <span class="${pnlCls}">${pnlSign}€${formatNum(pnl, 2)}</span>
    </div>
  </div>`;

  // Signal
  body += `<div class="advisory-detail__section">
    <div class="advisory-detail__label">Signal</div>
    <div class="portfolio-card__signal portfolio-card__signal--${signalCls}" style="display:inline-block;margin-top:4px">${_esc(label)}</div>
    ${data.composite_score != null ? `<div style="margin-top:4px;opacity:0.7">Composite score: ${Number(data.composite_score).toFixed(2)}</div>` : ""}
  </div>`;

  // Sector context
  if (data.sector_context) {
    body += `<div class="advisory-detail__section">
      <div class="advisory-detail__label">Sector Context</div>
      <div class="advisory-detail__text">${_esc(data.sector_context)}</div>
    </div>`;
  } else if (data.sector) {
    body += `<div class="advisory-detail__section">
      <div class="advisory-detail__label">Sector</div>
      <div class="advisory-detail__text">${_esc(data.sector)}</div>
    </div>`;
  }

  // Mid-term outlook
  if (data.mid_term_outlook) {
    body += `<div class="advisory-detail__section">
      <div class="advisory-detail__label">Mid-term Outlook (Swarm Consensus)</div>
      <div class="advisory-detail__text">${_esc(data.mid_term_outlook)}</div>
    </div>`;
  }

  // Reasoning
  const reasoning = data.reasoning;
  if (reasoning && typeof reasoning === "object") {
    if (reasoning.thesis) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">Thesis</div>
        <div class="advisory-detail__text">${_esc(reasoning.thesis)}</div>
      </div>`;
    }
    if (reasoning.catalyst) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">Catalyst</div>
        <div class="advisory-detail__text">${_esc(reasoning.catalyst)}</div>
      </div>`;
    }
    if (reasoning.timing) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">Timing</div>
        <div class="advisory-detail__text">${_esc(reasoning.timing)}</div>
      </div>`;
    }
    if (reasoning.risk) {
      body += `<div class="advisory-detail__section advisory-detail__section--risk">
        <div class="advisory-detail__label">Risk</div>
        <div class="advisory-detail__text">${_esc(reasoning.risk)}</div>
      </div>`;
    }
  } else if (reasoning) {
    body += `<div class="advisory-detail__section">
      <div class="advisory-detail__label">Reasoning</div>
      <div class="advisory-detail__text">${_esc(reasoning)}</div>
    </div>`;
  }

  // View Chart button
  body += `<div class="advisory-detail__section" style="text-align:center;padding-top:8px">
    <button class="btn btn--secondary portfolio-detail__chart-btn" data-chart-ticker="${_esc(ticker)}">View Chart</button>
  </div>`;

  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.innerHTML = `
    <div class="modal advisory-detail-modal">
      <div class="modal__header" style="border-left: 4px solid var(--accent)">
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

  // Wire up the View Chart button inside the modal
  const chartBtn = overlay.querySelector(".portfolio-detail__chart-btn");
  if (chartBtn) {
    chartBtn.addEventListener("click", () => {
      close();
      // Trigger the existing chart-ticker click flow
      const fakeEl = document.querySelector(`[data-chart-ticker="${_esc(ticker)}"]`);
      if (fakeEl) fakeEl.click();
    });
  }
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
  const headerColor = isBuy ? "var(--green)" : "var(--red)";
  const actionLabel = isBuy ? "BUY" : "SELL";

  let body = "";

  // Risk levels section (top of modal — most actionable)
  if (risk.stop_loss) {
    body += `<div class="advisory-detail__section advisory-detail__section--levels">
      <div class="advisory-detail__label">📐 Risk Levels</div>
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
      <div class="advisory-detail__label">💰 Position Size (Half-Kelly)</div>
      <div class="sizing-detail">
        <div class="sizing-detail__row">
          <span>Allocation:</span>
          <strong>${sizing.position_pct}% van portfolio</strong>
        </div>
        <div class="sizing-detail__row">
          <span>Amount:</span>
          <strong>€${formatNum(sizing.eur_amount, 2)}</strong>
          ${sizing.shares ? `<span class="sizing-detail__shares">(${sizing.shares} shares)</span>` : ""}
        </div>
        <div class="sizing-detail__row sizing-detail__row--dim">
          <span>Win Probability:</span>
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
        <div class="advisory-detail__label">⚡ Catalyst — why NOW?</div>
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
        <div class="advisory-detail__label">⚠️ Risk</div>
        <div class="advisory-detail__text">${_esc(reasoning.risk)}</div>
      </div>`;
    }
    if (reasoning.target) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">🎯 Price Target</div>
        <div class="advisory-detail__text">${_esc(reasoning.target)}</div>
      </div>`;
    }
    // Why attractive right now
    if (reasoning.why_now || reasoning.catalyst) {
      const whyNow = reasoning.why_now || reasoning.catalyst;
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">🔥 Why This Stock Right Now</div>
        <div class="advisory-detail__text">${_esc(whyNow)}</div>
      </div>`;
    }
    // Swarm entry reasoning
    if (reasoning.swarm_entry || reasoning.swarm_reasoning) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">🐝 Swarm Expects</div>
        <div class="advisory-detail__text">${_esc(reasoning.swarm_entry || reasoning.swarm_reasoning)}</div>
      </div>`;
    }
    // Exit trigger / stop loss narrative
    if (reasoning.exit_trigger || risk.exit_trigger) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">🚪 Exit Trigger / Stop Loss</div>
        <div class="advisory-detail__text">${_esc(reasoning.exit_trigger || risk.exit_trigger)}</div>
      </div>`;
    }
    // World context from the advisory narrative
    if (reasoning.world_context || reasoning.narrative) {
      body += `<div class="advisory-detail__section">
        <div class="advisory-detail__label">🌍 Current World Context</div>
        <div class="advisory-detail__text">${_esc(reasoning.world_context || reasoning.narrative)}</div>
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
  const escHandler = (e) => { if (e.key === "Escape") { close(); document.removeEventListener("keydown", escHandler); } };
  document.addEventListener("keydown", escHandler);
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
  let bestReturn = -Infinity, worstReturn = Infinity;
  for (const h of items) {
    totalEvals += h.outcomes_evaluated || 0;
    totalCorrect += h.outcomes_correct || 0;
    if (h.avg_return_pct != null) {
      allReturns.push(h.avg_return_pct);
      if (h.avg_return_pct > bestReturn) bestReturn = h.avg_return_pct;
      if (h.avg_return_pct < worstReturn) worstReturn = h.avg_return_pct;
    }
  }
  const accuracy = totalEvals > 0 ? ((totalCorrect / totalEvals) * 100).toFixed(1) : "—";
  const avgReturn = allReturns.length > 0 ? (allReturns.reduce((a, b) => a + b, 0) / allReturns.length).toFixed(2) : "—";
  const accCls = totalEvals > 0 ? (totalCorrect / totalEvals >= 0.5 ? "--positive" : "--negative") : "--neutral";
  const retCls = avgReturn !== "—" ? (parseFloat(avgReturn) >= 0 ? "--positive" : "--negative") : "--neutral";

  // Calculate win/loss streak
  let currentStreak = 0, streakType = "";
  for (const h of items) {
    if (h.avg_return_pct == null) continue;
    const win = h.avg_return_pct >= 0;
    if (currentStreak === 0) {
      streakType = win ? "win" : "loss";
      currentStreak = 1;
    } else if ((win && streakType === "win") || (!win && streakType === "loss")) {
      currentStreak++;
    } else {
      break;
    }
  }
  const streakLabel = currentStreak > 0 ? `${currentStreak} ${streakType}${currentStreak > 1 ? "s" : ""}` : "";

  let html = `<div class="track-record">
    <div class="track-record__title">
      <span>📈 Track Record (last ${items.length} advisories)</span>
    </div>
    <div class="track-record__stats">
      <div class="track-record__stat">
        <span class="track-record__stat-label">Accuracy</span>
        <span class="track-record__stat-value track-record__stat-value${accCls}">${accuracy}%</span>
      </div>
      <div class="track-record__stat">
        <span class="track-record__stat-label">Avg Return</span>
        <span class="track-record__stat-value track-record__stat-value${retCls}">${avgReturn !== "—" ? avgReturn + "%" : "—"}</span>
      </div>
      <div class="track-record__stat">
        <span class="track-record__stat-label">Evaluations</span>
        <span class="track-record__stat-value">${totalEvals}</span>
      </div>
      ${bestReturn > -Infinity ? `<div class="track-record__stat">
        <span class="track-record__stat-label">Best</span>
        <span class="track-record__stat-value track-record__stat-value--positive">+${bestReturn.toFixed(2)}%</span>
      </div>` : ""}
      ${worstReturn < Infinity ? `<div class="track-record__stat">
        <span class="track-record__stat-label">Worst</span>
        <span class="track-record__stat-value track-record__stat-value--negative">${worstReturn.toFixed(2)}%</span>
      </div>` : ""}
    </div>`;

  // Streak indicator (dot visualization)
  if (allReturns.length > 0) {
    const streakDots = items.slice(0, 14).map(h => {
      if (h.avg_return_pct == null) return `<span class="streak-dot streak-dot--none" title="${h.date}: pending"></span>`;
      return h.avg_return_pct >= 0
        ? `<span class="streak-dot streak-dot--win" title="${h.date}: +${h.avg_return_pct.toFixed(2)}%"></span>`
        : `<span class="streak-dot streak-dot--loss" title="${h.date}: ${h.avg_return_pct.toFixed(2)}%"></span>`;
    }).join("");
    html += `<div class="track-record__streak">
      <span class="track-record__streak-label">Recent: ${streakDots}</span>
      ${streakLabel ? `<span class="track-record__streak-current ${streakType === "win" ? "track-record__streak-current--win" : "track-record__streak-current--loss"}">${streakLabel} streak</span>` : ""}
    </div>`;
  }

  // Equity curve (cumulative return bar chart)
  if (allReturns.length >= 3) {
    let cumulative = 0;
    const cumReturns = [];
    for (const h of [...items].reverse()) {
      if (h.avg_return_pct != null) {
        cumulative += h.avg_return_pct;
        cumReturns.push({ date: h.date, cum: cumulative, daily: h.avg_return_pct });
      }
    }
    if (cumReturns.length >= 3) {
      const maxAbs = Math.max(...cumReturns.map(c => Math.abs(c.cum)), 0.01);
      const bars = cumReturns.map(c => {
        const pct = Math.abs(c.cum) / maxAbs * 50;
        const cls = c.cum >= 0 ? "equity-bar--pos" : "equity-bar--neg";
        return `<div class="equity-bar ${cls}" style="height:${pct + 2}px" title="${c.date}: ${c.cum >= 0 ? "+" : ""}${c.cum.toFixed(2)}%"></div>`;
      }).join("");
      html += `<div class="track-record__equity">
        <div class="track-record__equity-label">Cumulative Return</div>
        <div class="equity-chart">${bars}</div>
      </div>`;
    }
  }

  // Table of recent advisories
  html += `<table class="track-record__table">
    <thead><tr>
      <th>Date</th><th>Stance</th><th>Bullish</th><th>Bearish</th><th>Evals</th><th>Accuracy</th><th>Avg Return</th>
    </tr></thead><tbody>`;

  for (const h of items.slice(0, 10)) {
    const hDate = esc(h.date || "—");
    const stanceLabel = esc(_stanceLabel(h.market_stance || "neutral"));
    const stanceClass = esc((h.market_stance || "neutral").replace(/[^a-z_]/gi, ""));
    const buyTicks = esc((h.buy_tickers || []).join(", ") || "—");
    const sellTicks = esc((h.sell_tickers || []).join(", ") || "—");
    const evals = h.outcomes_evaluated || 0;
    const acc = h.accuracy != null ? `${(h.accuracy * 100).toFixed(0)}%` : "—";
    const ret = h.avg_return_pct != null ? `${h.avg_return_pct > 0 ? "+" : ""}${h.avg_return_pct.toFixed(2)}%` : "—";
    const retClass = h.avg_return_pct != null ? (h.avg_return_pct >= 0 ? "return--positive" : "return--negative") : "";

    html += `<tr>
      <td>${hDate}</td>
      <td><span class="stance-badge stance-badge--${stanceClass}" style="font-size:0.68rem;padding:2px 8px">${stanceLabel}</span></td>
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
  // Consolidated: delegate to esc() — DOM-based escaping is safer
  return esc(str);
}

function bindPortfolioTabEvents() {
  // MiFID II disclaimer dismiss
  document.querySelector("[data-dismiss-disclaimer]")?.addEventListener("click", () => {
    localStorage.setItem("oc_disclaimer_dismissed", "1");
    document.getElementById("mifid-disclaimer")?.remove();
  });

  // Feed slide-in panel
  document.querySelector("[data-open-feed]")?.addEventListener("click", () => {
    _openFeedPanel();
  });

  // Edit portfolio buttons (multiple on page: empty state, stale banner, header)
  document.querySelectorAll("[data-edit-portfolio]").forEach(btn => {
    btn.addEventListener("click", () => {
      _showPortfolioEditor();
    });
  });

  // Advisory card click → detail popup
  document.querySelectorAll(".advisory-card--clickable").forEach(card => {
    card.addEventListener("click", (e) => {
      // Don't open popup if clicking a ticker link for chart or feedback button
      if (e.target.closest("[data-chart-ticker]")) return;
      if (e.target.closest(".feedback-btn")) return;
      _showAdvisoryDetail(card);
    });
  });

  // Storyline card click → decision tree detail
  document.querySelectorAll("[data-storyline-id]").forEach(el => {
    el.addEventListener("click", () => _showStorylineDetail(el.dataset.storylineId));
  });

  // Portfolio card click → detail popup
  document.querySelectorAll("[data-portfolio-detail]").forEach(card => {
    card.addEventListener("click", (e) => {
      if (e.target.closest("[data-chart-ticker]")) return;
      _showPortfolioDetail(card);
    });
  });

  // Feedback buttons
  document.querySelectorAll(".feedback-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const container = btn.closest(".advisory-card__feedback");
      if (!container) return;
      const ticker = container.dataset.feedbackTicker;
      const action = container.dataset.feedbackAction;
      const rating = btn.dataset.rating;
      const advDate = (state.advisory?.advisory?.generated_at || "").slice(0, 10);
      try {
        await authFetch(`${API_BASE}?_api=advisory-feedback`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ date: advDate, ticker, action, rating }),
        });
        container.innerHTML = `<span class="feedback-confirmed">${{agree: "&#x1F44D; Agreed", disagree: "&#x1F44E; Disagreed", partially_agree: "&#x1F914; Partially"}[rating] || "Noted"}</span>`;
      } catch (err) {
        console.error("Feedback submission failed:", err);
      }
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
    <td><input type="text" class="portfolio-edit__name" value="" placeholder="Name" style="width:150px"></td>
    <td><input type="number" class="portfolio-edit__shares" value="" step="0.01" min="0" style="width:70px"></td>
    <td><input type="number" class="portfolio-edit__avgbuy" value="" step="0.01" min="0" style="width:80px"></td>
    <td><button class="btn btn--xs btn--danger portfolio-edit__remove">✕</button></td>
  </tr>`;

  const modal = document.createElement("div");
  modal.className = "modal-overlay";
  modal.innerHTML = `<div class="modal portfolio-editor-modal">
    <div class="modal__header">
      <h3>💼 Edit Portfolio</h3>
      <button class="modal__close" data-close-modal>✕</button>
    </div>
    <div class="modal__body">
      <div class="stock-search" style="margin-bottom:16px;position:relative">
        <label style="color:var(--text-dim);font-size:0.8rem;display:block;margin-bottom:4px">Search Bunq/Ginmon stocks to add:</label>
        <input type="text" id="stock-search-input" placeholder="Search by name or ticker (e.g. Shell, AAPL, Gold...)"
          style="width:100%;padding:8px 12px;background:var(--bg-inset);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.9rem" autocomplete="off">
        <div id="stock-search-results" style="position:absolute;top:100%;left:0;right:0;z-index:100;max-height:240px;overflow-y:auto;background:var(--bg-card);border:1px solid var(--border);border-radius:0 0 6px 6px;display:none"></div>
      </div>
      <table class="portfolio-edit__table">
        <thead><tr><th>Ticker</th><th>Name</th><th>Shares</th><th>Avg Buy (€)</th><th></th></tr></thead>
        <tbody id="portfolio-edit-rows">${rows}</tbody>
      </table>
    </div>
    <div class="modal__footer">
      <button class="btn btn--sm" data-close-modal>Cancel</button>
      <button class="btn btn--sm btn--primary" id="portfolio-save">Save</button>
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

  // Stock search with debounce
  let _searchTimeout = null;
  const searchInput = document.getElementById("stock-search-input");
  const searchResults = document.getElementById("stock-search-results");

  searchInput?.addEventListener("input", () => {
    clearTimeout(_searchTimeout);
    const q = searchInput.value.trim();
    if (q.length < 2) { searchResults.style.display = "none"; return; }
    _searchTimeout = setTimeout(async () => {
      try {
        const r = await authFetch(`${API_BASE}?_api=portfolio-search&q=${encodeURIComponent(q)}`);
        if (!r.ok) return;
        const data = await r.json();
        if (!data.results || data.results.length === 0) {
          searchResults.innerHTML = `<div style="padding:10px;color:var(--text-dim)">No stocks found for "${_esc(q)}"</div>`;
          searchResults.style.display = "block";
          return;
        }
        searchResults.innerHTML = data.results.map(s => `
          <div class="stock-search-item" data-ticker="${_esc(s.ticker)}" data-name="${_esc(s.name)}"
            style="padding:8px 12px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border)">
            <div>
              <span style="color:var(--accent);font-weight:600">${_esc(s.ticker)}</span>
              <span style="color:var(--text-dim);margin-left:8px">${_esc(s.name)}</span>
            </div>
            ${s.price ? `<span style="color:var(--text);font-size:0.85rem">${s.currency === "EUR" ? "€" : "$"}${formatNum(s.price, 2)} <span style="color:${(s.change_pct||0) >= 0 ? 'var(--green)' : 'var(--red)'};font-size:0.8rem">${(s.change_pct||0) >= 0 ? '+' : ''}${formatNum(s.change_pct||0, 2)}%</span></span>` : ""}
          </div>
        `).join("");
        searchResults.style.display = "block";

        // Click to add
        searchResults.querySelectorAll(".stock-search-item").forEach(item => {
          item.addEventListener("mouseenter", () => item.style.background = "var(--bg-hover)");
          item.addEventListener("mouseleave", () => item.style.background = "");
          item.addEventListener("click", () => {
            const ticker = item.dataset.ticker;
            const name = item.dataset.name;
            // Check if already in table
            const existing = document.querySelectorAll("#portfolio-edit-rows .portfolio-edit__ticker");
            for (const inp of existing) {
              if (inp.value.toUpperCase() === ticker.toUpperCase()) {
                inp.closest("tr").querySelector(".portfolio-edit__shares")?.focus();
                searchResults.style.display = "none";
                searchInput.value = "";
                return;
              }
            }
            // Add new row
            const tbody = document.getElementById("portfolio-edit-rows");
            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td><input type="text" class="portfolio-edit__ticker" value="${_esc(ticker)}" placeholder="XOM" style="width:70px"></td>
              <td><input type="text" class="portfolio-edit__name" value="${_esc(name)}" placeholder="Name" style="width:150px"></td>
              <td><input type="number" class="portfolio-edit__shares" value="" step="0.01" min="0" style="width:70px" autofocus></td>
              <td><input type="number" class="portfolio-edit__avgbuy" value="" step="0.01" min="0" style="width:80px"></td>
              <td><button class="btn btn--xs btn--danger portfolio-edit__remove">✕</button></td>
            `;
            tbody.appendChild(tr);
            _bindRemoveButtons();
            tr.querySelector(".portfolio-edit__shares")?.focus();
            searchResults.style.display = "none";
            searchInput.value = "";
          });
        });
      } catch (err) { console.warn("Stock search failed:", err); }
    }, 300);
  });

  // Hide search results on click outside
  document.addEventListener("click", (e) => {
    if (!e.target.closest(".stock-search")) {
      searchResults.style.display = "none";
    }
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
      const r = await authFetch(`${API_BASE}?_api=portfolio-holdings`, {
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
    case "feed": bindFeedTabEvents(); break;
    case "trees": bindTreesTabEvents(); break;
    case "intel": bindIntelTabEvents(); break;
    case "usage": bindUsageTabEvents(); break;
    case "ml": bindMLTabEvents(); break;
    case "users": bindUsersTabEvents(); break;
    case "settings": bindSettingsTabEvents(); break;
    case "swarm": bindSwarmTabEvents(); break;
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
      await authFetch(`${API_BASE}?_api=signals-refresh`, { method: "POST" });
      await new Promise(r => setTimeout(r, 1500));
      await fetchSignals();
      render();
    } catch (e) { showError("Signal refresh failed: " + e.message); }
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
        stopTreePolling();
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
      stopTreePolling();
      startTreePolling(id);
    });
  });
}

function bindIntelTabEvents() {
  document.querySelector("[data-run-analysis]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-run-analysis]");
    if (btn) { btn.disabled = true; btn.textContent = "Running..."; }
    try {
      await authFetch(`${API_BASE}?_api=analysis-run`, { method: "POST" });
      await new Promise(r => setTimeout(r, 3000));
      await fetchAnalysis();
      render();
    } catch (e) { showError("Analysis run failed: " + e.message); }
  });
}

function bindSettingsTabEvents() {
  // Budget input: show/hide Save button on value change
  const budgetInput = document.getElementById("budget-input");
  if (budgetInput) {
    budgetInput.addEventListener("input", () => {
      const saveBtn = document.getElementById("budget-save-btn");
      const original = parseFloat(budgetInput.dataset.original);
      const current = parseFloat(budgetInput.value);
      if (saveBtn) {
        saveBtn.style.display = (current !== original && current >= 0.5) ? "" : "none";
      }
    });
  }

  // Budget save handler
  document.getElementById("budget-save-btn")?.addEventListener("click", async () => {
    const input = document.getElementById("budget-input");
    const val = parseFloat(input?.value);
    if (isNaN(val) || val < 0.5 || val > 100) return;
    const btn = document.getElementById("budget-save-btn");
    if (btn) { btn.disabled = true; btn.textContent = "Saving..."; }
    try {
      const r = await authFetch(`${API_BASE}?_api=budget`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ daily_budget_eur: val }),
      });
      if (r.ok) {
        await fetchBudget();
        render();
      }
    } catch (err) {
      console.error("Failed to update budget:", err);
    }
  });

  // Swarm interval input: show/hide Save button + cost estimate
  const swarmIntervalInput = document.getElementById("swarm-interval-input");
  if (swarmIntervalInput) {
    swarmIntervalInput.addEventListener("input", () => {
      const saveBtn = document.getElementById("swarm-interval-save-btn");
      const original = parseInt(swarmIntervalInput.dataset.original);
      const current = parseInt(swarmIntervalInput.value);
      if (saveBtn) {
        saveBtn.style.display = (current !== original && current >= 10 && current <= 120) ? "" : "none";
      }
      updateSwarmCostEstimate();
    });
  }

  // Swarm interval save handler
  document.getElementById("swarm-interval-save-btn")?.addEventListener("click", async () => {
    const input = document.getElementById("swarm-interval-input");
    const val = parseInt(input?.value);
    if (isNaN(val) || val < 10 || val > 120) return;
    const btn = document.getElementById("swarm-interval-save-btn");
    if (btn) { btn.disabled = true; btn.textContent = "Saving..."; }
    try {
      const r = await authFetch(`${API_BASE}?_api=swarm/config`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ interval_minutes: val }),
      });
      if (r.ok) {
        await fetchSwarmStatus();
        render();
      }
    } catch (err) {
      console.error("Failed to update swarm interval:", err);
    }
  });

  // Run swarm cycle
  document.querySelector("[data-run-swarm]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-run-swarm]");
    if (btn) { btn.disabled = true; btn.textContent = "Running..."; }
    try {
      await authFetch(`${API_BASE}?_api=swarm-cycle`, { method: "POST" });
      await new Promise(r => setTimeout(r, 2000));
      await fetchSwarmStatus();
      render();
    } catch (e) { showError("Swarm cycle failed: " + e.message); }
  });

  // API Key save handlers
  document.querySelectorAll("[data-save-apikey]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const keyId = btn.dataset.saveApikey;
      const endpoint = btn.dataset.endpoint;
      const field = btn.dataset.field;
      const input = document.getElementById(`apikey-input-${keyId}`);
      const msgEl = document.getElementById(`apikey-msg-${keyId}`);
      const val = (input?.value || "").trim();
      if (!val) { if (msgEl) { msgEl.textContent = "Please enter a key."; msgEl.className = "api-key-msg api-key-msg--error"; } return; }
      btn.disabled = true; btn.textContent = "Saving...";
      try {
        const body = {};
        body[field] = val;
        const r = await authFetch(`${API_BASE}?_api=${endpoint}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        if (r.ok) {
          if (msgEl) { msgEl.textContent = "Key saved successfully."; msgEl.className = "api-key-msg api-key-msg--ok"; }
          if (input) input.value = "";
          await Promise.all([fetchApiKeyStatus(), fetchSwarmStatus()]);
          // Update status inline without full re-render to keep message visible
          const statusEl = document.getElementById(`apikey-status-${keyId}`);
          if (statusEl) { statusEl.textContent = "Configured"; statusEl.className = "settings-row__val settings-row__val--ok"; }
        } else {
          if (msgEl) { msgEl.textContent = "Failed to save key."; msgEl.className = "api-key-msg api-key-msg--error"; }
        }
      } catch (err) {
        if (msgEl) { msgEl.textContent = "Error: " + err.message; msgEl.className = "api-key-msg api-key-msg--error"; }
      }
      btn.disabled = false; btn.textContent = "Save";
    });
  });

  // Feed region collapsible sections
  document.querySelectorAll("[data-toggle-region]").forEach(el => {
    el.addEventListener("click", () => {
      const regionId = el.dataset.toggleRegion;
      const body = document.getElementById(`feed-region-${regionId}`);
      const arrow = document.getElementById(`feed-arrow-${regionId}`);
      if (body) {
        const isOpen = body.style.display !== "none";
        body.style.display = isOpen ? "none" : "block";
        if (arrow) arrow.textContent = isOpen ? "\u25B6" : "\u25BC";
      }
    });
  });

  // Feed toggle (enable/disable)
  document.querySelectorAll("[data-feed-toggle]").forEach(cb => {
    cb.addEventListener("change", async () => {
      const feedId = cb.dataset.feedToggle;
      const enabled = cb.checked;
      try {
        await authFetch(`${API_BASE}?_api=feeds&id=${encodeURIComponent(feedId)}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled }),
        });
        await fetchFeeds();
        render();
      } catch (e) { showError("Failed to update feed: " + e.message); }
    });
  });

  // Add feed inline form toggle
  document.querySelector("[data-add-feed-show]")?.addEventListener("click", () => {
    const form = document.getElementById("feed-add-form");
    if (form) form.style.display = form.style.display === "none" ? "block" : "none";
  });

  // Add feed submit
  document.querySelector("[data-add-feed-submit]")?.addEventListener("click", async () => {
    const url = (document.getElementById("feed-inline-url")?.value || "").trim();
    const region = document.getElementById("feed-inline-region")?.value || "General";
    if (!url) return;
    try {
      await authFetch(`${API_BASE}?_api=feeds`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: url, url, region }),
      });
      await fetchFeeds();
      render();
    } catch (err) {
      showError("Failed to add feed: " + err.message);
    }
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
        await authFetch(`${API_BASE}?_api=feeds&id=${encodeURIComponent(id)}`, { method: "DELETE" });
        await fetchFeeds();
        showFeedsModal(); // Re-render modal
      } catch (e) { showError("Failed to delete feed: " + e.message); }
    });
  });

  // Bind add
  document.getElementById("feed-add-btn")?.addEventListener("click", async () => {
    const name = document.getElementById("feed-name")?.value;
    const url = document.getElementById("feed-url")?.value;
    const region = document.getElementById("feed-region")?.value || "global";
    if (!url) return;
    try {
      await authFetch(`${API_BASE}?_api=feeds`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: name || url, url, region }),
      });
      await fetchFeeds();
      showFeedsModal();
    } catch (err) {
      showError("Failed: " + err.message);
    }
  });
}

/* ══════════════════════════════════════════════════════════════
   FLASH ALERTS (Breaking News)
   ══════════════════════════════════════════════════════════════ */

async function fetchFlashAlerts() {
  try {
    const r = await authFetch(`${API_BASE}?_api=flash-alerts`);
    if (r.ok) {
      const data = await r.json();
      state.flashAlerts = Array.isArray(data) ? data : [];
    }
  } catch (e) { /* non-fatal */ }
}

function renderBreakingBanner() {
  const alerts = (state.flashAlerts || []).filter(a => {
    if (a.status !== "active") return false;
    const age = Date.now() - new Date(a.detected_at).getTime();
    return age < 6 * 3600 * 1000;
  });
  if (!alerts.length) return "";

  const latest = alerts[0];
  const advisory = typeof latest.flash_advisory === "object" ? latest.flash_advisory : {};
  const impact = advisory.market_impact || {};
  const pa = advisory.portfolio_action || {};
  const tickers = Array.isArray(latest.tickers_affected) ? latest.tickers_affected.join(", ") : "";
  const ago = _flashTimeAgo(latest.detected_at);
  const riskClass = (latest.risk_level || "moderate").toLowerCase();

  let html = `<div class="breaking-banner">`;
  html += `<span class="breaking-badge">🔴 BREAKING</span>`;
  html += `<span class="breaking-headline">${_esc(latest.headline)}</span>`;
  if (latest.region) html += `<span class="breaking-region">${_esc(latest.region.toUpperCase())}</span>`;
  if (tickers) html += `<span class="breaking-tickers">Watch: ${_esc(tickers)}</span>`;
  if (pa.recommendation) html += `<span class="breaking-action">⚡ ${_esc(pa.recommendation)}</span>`;
  html += `<span class="breaking-ago">${ago}</span>`;
  html += `</div>`;

  if (alerts.length > 1) {
    html += `<div class="breaking-more">${alerts.length - 1} more alert(s) in last 6h</div>`;
  }

  return html;
}

function renderFlashAlertCards() {
  const alerts = state.flashAlerts || [];
  if (!alerts.length) return "";

  let html = `<div class="flash-section">`;
  html += `<h3 class="section-title">⚡ Flash Alerts</h3>`;
  html += `<div class="flash-grid">`;

  for (const a of alerts.slice(0, 6)) {
    const advisory = typeof a.flash_advisory === "object" ? a.flash_advisory : {};
    const impact = advisory.market_impact || {};
    const tickers = Array.isArray(a.tickers_affected) ? a.tickers_affected : [];
    const sectors = Array.isArray(impact.sectors_affected) ? impact.sectors_affected : [];
    const ago = _flashTimeAgo(a.detected_at);
    const riskClass = a.status === "expired" ? "expired" : (a.risk_level || "moderate").toLowerCase();

    html += `<div class="flash-card flash-card--${riskClass}">`;
    html += `<div class="flash-card__header">`;
    html += `<span class="flash-card__badge">${a.flash_score >= 80 ? "🔴 CRITICAL" : "🟠 ALERT"}</span>`;
    html += `<span class="flash-card__score">${a.flash_score?.toFixed(0)}/100</span>`;
    html += `<span class="flash-card__ago">${ago}</span>`;
    html += `</div>`;
    html += `<div class="flash-card__headline">${_esc(a.headline)}</div>`;

    if (a.region || a.event_type) {
      html += `<div class="flash-card__meta">`;
      if (a.region) html += `<span class="region-badge region-badge--${(a.region || "").replace(/[^a-z-]/gi, "").toLowerCase()}">${_esc(a.region)}</span>`;
      if (a.event_type) html += `<span class="flash-card__event">${_esc(a.event_type.replace(/_/g, " "))}</span>`;
      html += `</div>`;
    }

    if (tickers.length) {
      html += `<div class="flash-card__tickers">Watch: ${tickers.map(t => `<b>${_esc(t)}</b>`).join(", ")}</div>`;
    }
    if (sectors.length) {
      html += `<div class="flash-card__sectors">Sectors: ${_esc(sectors.join(", "))}</div>`;
    }

    // Evaluation badges
    const evals = [];
    for (const h of ["6h", "1d", "4d", "7d"]) {
      const ev = a[`eval_${h}`];
      if (ev && typeof ev === "object") {
        const correct = ev.overall_correct || (ev.results && ev.results[0]?.correct);
        const pct = ev.results?.[0]?.pct_change;
        const icon = correct ? "✅" : "❌";
        evals.push(`<span class="flash-eval-badge flash-eval-badge--${correct ? "ok" : "miss"}">T+${h} ${icon}${pct != null ? ` ${pct > 0 ? "+" : ""}${pct.toFixed(1)}%` : ""}</span>`);
      }
    }
    if (evals.length) {
      html += `<div class="flash-card__evals">${evals.join(" ")}</div>`;
    }

    if (impact.immediate) {
      html += `<div class="flash-card__impact">${_esc(impact.immediate)}</div>`;
    }

    html += `</div>`;
  }

  html += `</div></div>`;
  return html;
}

function _flashTimeAgo(isoStr) {
  if (!isoStr) return "";
  const ms = Date.now() - new Date(isoStr).getTime();
  const mins = Math.floor(ms / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

/* ══════════════════════════════════════════════════════════════
   POLLING & INIT
   ══════════════════════════════════════════════════════════════ */

function startPolling() { pollTimer = setInterval(() => { fetchOverview(); fetchOpportunities(); fetchFlashAlerts(); if (state.activeTab === "portfolio") { fetchPortfolioAlignment().then(() => render()); } }, POLL_INTERVAL); }
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
  } catch (e) { /* theme sync non-fatal */ }
}

// Visibility change handler — fetch fresh data + render once on tab-restore
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    stopPolling();
    stopTreePolling();
  } else {
    // Reset lazy-load flags so tabs re-fetch fresh data on next visit
    _usageLoaded = false;
    _settingsLoaded = false;
    const visFetches = [fetchOverview(), fetchOpportunities(), fetchFocus()];
    if (state.activeTab === "portfolio") { visFetches.push(fetchPortfolioAlignment()); }
    Promise.all(visFetches).then(() => render());
    startPolling();
    if (activeTreeId) startTreePolling(activeTreeId);
  }
});

// Hash routing
function initRoute() {
  if (_embedMode) {
    state.activeTab = "portfolio";
    return;
  }
  const hash = location.hash.replace("#", "");
  if (["signals", "portfolio", "trees", "intel", "usage", "settings"].includes(hash)) {
    state.activeTab = hash;
  }
}

// ══════════════════════════════════════════════════════════════
// Feed Slide-In Panel
// ══════════════════════════════════════════════════════════════

function _openFeedPanel() {
  // Remove existing panel and backdrop if open
  document.getElementById("feed-panel")?.remove();
  document.getElementById("feed-backdrop")?.remove();

  const panel = document.createElement("div");
  panel.id = "feed-panel";
  panel.className = "feed-panel";
  panel.innerHTML = `
    <div class="feed-panel__header">
      <h3>📰 Live Feed</h3>
      <button class="feed-panel__close" data-close-feed>&times;</button>
    </div>
    <div class="feed-panel__filters">
      <select id="fp-region"><option value="">All Regions</option>
        <option value="middle-east">Middle East</option><option value="europe">Europe</option>
        <option value="asia">Asia</option><option value="north-america">North America</option>
        <option value="africa">Africa</option><option value="global">Global</option>
      </select>
      <select id="fp-sentiment"><option value="">All Sentiment</option>
        <option value="negative">Negative</option><option value="neutral">Neutral</option>
        <option value="positive">Positive</option>
      </select>
      <select id="fp-intensity"><option value="">All Intensity</option>
        <option value="critical">Critical</option><option value="high-threat">High Threat</option>
        <option value="moderate">Moderate</option><option value="low">Low</option>
      </select>
    </div>
    <div class="feed-panel__articles" id="fp-articles">
      <div class="feed-panel__loading">Loading articles...</div>
    </div>
  `;
  document.body.appendChild(panel);

  // Create backdrop overlay
  const backdrop = document.createElement("div");
  backdrop.id = "feed-backdrop";
  backdrop.className = "feed-backdrop";
  backdrop.addEventListener("click", () => {
    panel.classList.remove("open");
    backdrop.classList.remove("open");
    setTimeout(() => { panel.remove(); backdrop.remove(); }, 300);
  });
  document.body.appendChild(backdrop);

  // Animate in
  requestAnimationFrame(() => {
    panel.classList.add("open");
    backdrop.classList.add("open");
  });

  // Close handlers
  panel.querySelector("[data-close-feed]").addEventListener("click", () => {
    panel.classList.remove("open");
    backdrop.classList.remove("open");
    setTimeout(() => { panel.remove(); backdrop.remove(); }, 300);
  });

  // Filter handlers
  panel.querySelectorAll("select").forEach(sel => {
    sel.addEventListener("change", () => _loadFeedArticles(panel));
  });

  // Load initial articles
  _loadFeedArticles(panel);
}

async function _loadFeedArticles(panel) {
  const container = panel.querySelector("#fp-articles");
  const region = panel.querySelector("#fp-region").value;
  const sentiment = panel.querySelector("#fp-sentiment").value;
  const intensity = panel.querySelector("#fp-intensity").value;

  container.innerHTML = '<div class="feed-panel__loading">Loading...</div>';

  const params = new URLSearchParams({ limit: "40" });
  if (region) params.set("region", region);

  try {
    const r = await authFetch(`${API_BASE}?_api=briefs&${params}`);
    if (!r.ok) { container.innerHTML = '<div class="feed-panel__empty">Failed to load articles</div>'; return; }
    let articles = await r.json();
    if (!Array.isArray(articles)) articles = articles.briefs || articles.results || [];

    // Client-side filter for sentiment/intensity (API may not support all filters)
    if (sentiment) {
      articles = articles.filter(a => {
        const s = a.sentiment || 0;
        if (sentiment === "negative") return s < -0.2;
        if (sentiment === "positive") return s > 0.2;
        return s >= -0.2 && s <= 0.2;
      });
    }
    if (intensity) articles = articles.filter(a => (a.intensity || "low") === intensity);

    if (articles.length === 0) {
      container.innerHTML = '<div class="feed-panel__empty">No articles match your filters</div>';
      return;
    }

    container.innerHTML = articles.map(a => {
      const sent = (a.sentiment || 0);
      const sentDot = sent < -0.2 ? "🔴" : sent > 0.2 ? "🟢" : "🟡";
      const age = a.processed_at ? Math.round((Date.now() - new Date(a.processed_at).getTime()) / 60000) : null;
      const ageText = age != null ? (age < 60 ? `${age}m` : age < 1440 ? `${Math.floor(age/60)}h` : `${Math.floor(age/1440)}d`) : "";
      const intBadge = a.intensity && a.intensity !== "low" ? `<span class="fp-intensity fp-intensity--${a.intensity.replace(/[^a-z-]/gi,'')}">${a.intensity}</span>` : "";

      return `<div class="fp-article">
        <div class="fp-article__top">
          <span class="fp-article__sent">${sentDot}</span>
          <span class="fp-article__source">${_esc(a.source || "")}</span>
          ${a.region ? `<span class="fp-article__region">${_esc(a.region)}</span>` : ""}
          ${intBadge}
          <span class="fp-article__age">${ageText}</span>
        </div>
        <div class="fp-article__title">${_esc(a.title || "")}</div>
        ${a.summary ? `<div class="fp-article__summary">${_esc((a.summary || "").slice(0, 120))}</div>` : ""}
      </div>`;
    }).join("");
  } catch (e) {
    container.innerHTML = '<div class="feed-panel__empty">Error loading feed</div>';
  }
}


// ══════════════════════════════════════════════════════════════
// Swarm Intelligence Feed
// ══════════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════════
// Recent Activity Ticker
// ══════════════════════════════════════════════════════════════

function renderActivityTicker() {
  const items = [];
  const verdictIcon = { STRONG_BUY: "\u{1F7E2}", BUY: "\u{1F7E2}", HOLD: "\u{1F7E1}", SELL: "\u{1F534}", STRONG_SELL: "\u{1F534}" };

  // Gather swarm verdicts
  const feed = state.swarmFeed;
  if (feed && feed.verdicts && feed.verdicts.length) {
    for (const v of feed.verdicts.slice(0, 6)) {
      const age = v.created_at ? Math.round((Date.now() - new Date(v.created_at).getTime()) / 60000) : null;
      const ageText = age != null ? (age < 60 ? `${age}m` : `${Math.floor(age/60)}h`) : "";
      const confPct = Math.round((v.confidence || 0) * 100);
      const icon = verdictIcon[v.verdict] || "\u26AA";
      const label = (v.verdict || "").replace("STRONG_", "");
      items.push({
        type: "swarm",
        html: `<span class="activity-ticker__pill activity-ticker__pill--${(v.verdict || "HOLD").toLowerCase()}">${icon} ${label}${v.ticker ? " " + _esc(v.ticker) : ""} ${confPct}%${ageText ? " \u00b7 " + ageText : ""}</span>`,
        age: age || 9999
      });
    }
  }

  // Gather flash alerts
  const alerts = (state.flashAlerts || []).filter(a => a.status === "active");
  for (const a of alerts.slice(0, 4)) {
    const age = a.detected_at ? Math.round((Date.now() - new Date(a.detected_at).getTime()) / 60000) : null;
    const ageText = age != null ? (age < 60 ? `${age}m` : `${Math.floor(age/60)}h`) : "";
    const tickers = Array.isArray(a.tickers_affected) ? a.tickers_affected.slice(0, 2).join(", ") : "";
    items.push({
      type: "flash",
      html: `<span class="activity-ticker__pill activity-ticker__pill--flash">\u26A1 ${_esc((a.headline || "Alert").slice(0, 30))}${a.headline && a.headline.length > 30 ? "\u2026" : ""}${tickers ? " " + tickers : ""}${ageText ? " \u00b7 " + ageText : ""}</span>`,
      age: age || 9999
    });
  }

  if (!items.length) return "";

  // Sort by age (newest first), limit to 8
  items.sort((a, b) => a.age - b.age);
  const display = items.slice(0, 8);

  return `<div class="activity-ticker">
    <span class="activity-ticker__label">\u{1F4E1} Recent</span>
    ${display.map(i => i.html).join("")}
  </div>`;
}

async function fetchSwarmFeed() {
  try {
    const r = await authFetch(`${API_BASE}?_api=swarm-feed`);
    if (r.ok) state.swarmFeed = await r.json();
  } catch (e) { /* non-fatal */ }
}

/* ── Investment Swarm V3 Section ──────────────────────────── */
function renderSwarmV3(swarmV3) {
  if (!swarmV3) return "";
  const macro = swarmV3.macro_outlook || {};
  const picks = swarmV3.top_picks || [];
  const confidence = swarmV3.confidence != null ? Math.round(swarmV3.confidence * 100) : null;
  const funnel = swarmV3.funnel || {};

  // Regime color mapping
  const regimeColors = {
    recession: "var(--red)", contraction: "var(--red)",
    expansion: "var(--green)", growth: "var(--green)",
    transition: "var(--orange)", recovery: "var(--orange)"
  };
  const regime = (macro.regime || "").toLowerCase();
  const bias = (macro.bias || "").toLowerCase();
  const sentiment = (macro.sentiment || "").toLowerCase();
  const regimeColor = regimeColors[regime] || "var(--yellow)";
  const biasColor = bias.includes("bull") ? "var(--green)" : bias.includes("bear") ? "var(--red)" : "var(--yellow)";
  const sentColor = sentiment.includes("fear") || sentiment.includes("negative") ? "var(--red)"
    : sentiment.includes("greed") || sentiment.includes("positive") ? "var(--green)" : "var(--yellow)";

  let html = `<div class="swarm-v3-section" style="
    background:rgba(255,255,255,0.03);
    border:1px solid rgba(255,255,255,0.08);
    border-radius:12px;
    padding:16px 20px;
    margin-bottom:16px;
  ">`;

  // Header with funnel
  html += `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
    <div style="display:flex;align-items:center;gap:10px">
      <span style="font-size:1rem;font-weight:600;color:var(--accent)">Investment Swarm V3</span>
      <span style="font-size:0.72rem;padding:2px 8px;border-radius:8px;background:rgba(99,102,241,0.15);color:var(--accent)">AI Consensus</span>
    </div>
    <div style="display:flex;align-items:center;gap:6px;font-size:0.72rem;color:var(--text-dim)">
      <span style="font-weight:500">Swarm verdicts</span>
      <span style="display:inline-flex;align-items:center;gap:3px;font-family:var(--mono)">
        <span style="background:rgba(255,255,255,0.06);padding:1px 5px;border-radius:4px">${funnel.tier1 || 14}</span>
        <span style="opacity:0.4">&rarr;</span>
        <span style="background:rgba(255,255,255,0.08);padding:1px 5px;border-radius:4px">${funnel.tier2 || 6}</span>
        <span style="opacity:0.4">&rarr;</span>
        <span style="background:rgba(99,102,241,0.2);padding:1px 5px;border-radius:4px;color:var(--accent)">${funnel.tier3 || 1}</span>
      </span>
    </div>
  </div>`;

  // Macro summary
  if (macro.summary) {
    html += `<div style="font-size:0.85rem;line-height:1.5;color:var(--text);margin-bottom:12px">${_esc(macro.summary)}</div>`;
  }

  // Macro chips
  html += `<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px">`;
  if (macro.regime) {
    html += `<span style="font-size:0.72rem;padding:3px 10px;border-radius:10px;background:rgba(255,255,255,0.04);border:1px solid ${regimeColor};color:${regimeColor}">Regime: ${_esc(macro.regime)}</span>`;
  }
  if (macro.bias) {
    html += `<span style="font-size:0.72rem;padding:3px 10px;border-radius:10px;background:rgba(255,255,255,0.04);border:1px solid ${biasColor};color:${biasColor}">Bias: ${_esc(macro.bias)}</span>`;
  }
  if (macro.sentiment) {
    html += `<span style="font-size:0.72rem;padding:3px 10px;border-radius:10px;background:rgba(255,255,255,0.04);border:1px solid ${sentColor};color:${sentColor}">Sentiment: ${_esc(macro.sentiment)}</span>`;
  }
  if (confidence != null) {
    html += `<span style="font-size:0.72rem;padding:3px 10px;border-radius:10px;background:rgba(99,102,241,0.12);color:var(--accent);font-family:var(--mono)">Confidence: ${confidence}%</span>`;
  }
  html += `</div>`;

  // Top picks as clickable cards (click to expand full rationale)
  if (picks.length > 0) {
    html += `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px">`;
    const actionColors = { buy: "var(--green)", sell: "var(--red)", hold: "var(--yellow)", strong_buy: "var(--green)", strong_sell: "var(--red)" };
    for (let i = 0; i < picks.length; i++) {
      const pick = picks[i];
      const action = (pick.action || "").toLowerCase();
      const ac = actionColors[action] || "var(--text-dim)";
      const shortRationale = (pick.rationale || "").length > 120 ? pick.rationale.slice(0, 120) + "\u2026" : (pick.rationale || "");
      const pickId = `swarm-pick-${i}`;
      html += `<div class="swarm-pick-card" id="${pickId}" onclick="window._showPickDetail(${i})" style="
        background:rgba(255,255,255,0.03);
        border:1px solid rgba(255,255,255,0.06);
        border-left:3px solid ${ac};
        border-radius:8px;
        padding:10px 12px;
        cursor:pointer;
        transition:background 0.15s,border-color 0.15s;
      " onmouseenter="this.style.background='rgba(255,255,255,0.06)';this.style.borderColor='rgba(255,255,255,0.12)'" onmouseleave="this.style.background='rgba(255,255,255,0.03)';this.style.borderColor='rgba(255,255,255,0.06)'">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">
          <span style="font-weight:600;font-size:0.85rem;color:var(--text)" data-chart-ticker="${_esc(pick.ticker || "")}">${_esc(pick.ticker || "?")}</span>
          <span style="font-size:0.68rem;text-transform:uppercase;font-weight:600;color:${ac}">${_esc(pick.action || "")}</span>
        </div>
        <div style="font-size:0.75rem;line-height:1.4;color:var(--text-dim)">${_esc(shortRationale)}</div>
        <div style="font-size:0.65rem;color:var(--text-muted);margin-top:4px;opacity:0.6">Tap for details \u2192</div>
      </div>`;
    }
    html += `</div>`;

    // Store picks for the detail overlay
    window._swarmV3Picks = picks;
    window._swarmV3Macro = macro;
    window._swarmV3Confidence = confidence;
  }

  html += `</div>`;
  return html;
}

/* ── Swarm Pick Detail Overlay ──────────────────────────── */
window._showPickDetail = function(idx) {
  const picks = window._swarmV3Picks || [];
  const macro = window._swarmV3Macro || {};
  const confidence = window._swarmV3Confidence;
  if (!picks[idx]) return;
  const pick = picks[idx];

  const actionColors = { buy: "var(--green)", sell: "var(--red)", hold: "var(--yellow)", strong_buy: "var(--green)", strong_sell: "var(--red)" };
  const action = (pick.action || "").toLowerCase();
  const ac = actionColors[action] || "var(--text-dim)";

  // Build expectations section from pick metadata
  const target = pick.target_price || pick.target || null;
  const stopLoss = pick.stop_loss || null;
  const timeframe = pick.timeframe || pick.horizon || "1-3 months";
  const score = pick.overall_score || pick.score || null;

  // Find sector rotation info relevant to this pick
  const sectors = pick.sectors || pick.sector || "";

  // Determine if there are sell signals for context
  const sellSignals = (window._swarmV3Picks || []).filter(p => (p.action || "").toLowerCase().includes("sell"));

  let overlay = document.getElementById("swarm-pick-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "swarm-pick-overlay";
    document.body.appendChild(overlay);
  }

  overlay.innerHTML = `
    <div class="swarm-overlay-backdrop" onclick="window._closePickDetail()" style="
      position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:9998;
      animation:fadeIn 0.15s ease;
    "></div>
    <div class="swarm-overlay-panel" style="
      position:fixed;top:0;right:0;bottom:0;width:min(420px,90vw);z-index:9999;
      background:var(--bg, #0f0f14);border-left:1px solid rgba(255,255,255,0.08);
      overflow-y:auto;padding:24px 20px;
      animation:slideInRight 0.2s ease;
    ">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
        <div style="display:flex;align-items:center;gap:12px">
          <span style="font-size:1.4rem;font-weight:700;color:var(--text)">${_esc(pick.ticker || "?")}</span>
          <span style="font-size:0.75rem;text-transform:uppercase;font-weight:600;padding:3px 10px;border-radius:10px;background:rgba(255,255,255,0.04);border:1px solid ${ac};color:${ac}">${_esc(pick.action || "hold")}</span>
          ${score ? `<span style="font-size:0.72rem;padding:3px 8px;border-radius:10px;background:rgba(99,102,241,0.12);color:var(--accent);font-family:var(--mono)">${score}/10</span>` : ""}
        </div>
        <button onclick="window._closePickDetail()" style="
          background:none;border:none;color:var(--text-dim);font-size:1.2rem;cursor:pointer;padding:4px 8px;
        ">&times;</button>
      </div>

      <!-- Why Now Section -->
      <div style="margin-bottom:20px">
        <div style="font-size:0.72rem;font-weight:600;color:var(--accent);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">Why ${_esc(pick.ticker)} now?</div>
        <div style="font-size:0.85rem;line-height:1.6;color:var(--text)">${_esc(pick.rationale || "No rationale available.")}</div>
      </div>

      <!-- Market Context -->
      <div style="margin-bottom:20px;padding:12px 14px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:10px">
        <div style="font-size:0.72rem;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">Market Context</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.78rem">
          <div>
            <span style="color:var(--text-muted)">Regime</span><br/>
            <span style="font-weight:600;color:${actionColors[macro.regime] || "var(--text)"}">${_esc(macro.regime || "—")}</span>
          </div>
          <div>
            <span style="color:var(--text-muted)">Bias</span><br/>
            <span style="font-weight:600">${_esc(macro.bias || "—")}</span>
          </div>
          <div>
            <span style="color:var(--text-muted)">Sentiment</span><br/>
            <span style="font-weight:600">${_esc((macro.sentiment || "—").replace("_", " "))}</span>
          </div>
          <div>
            <span style="color:var(--text-muted)">Confidence</span><br/>
            <span style="font-weight:600;color:var(--accent)">${confidence != null ? confidence + "%" : "—"}</span>
          </div>
        </div>
      </div>

      <!-- Expectations -->
      <div style="margin-bottom:20px;padding:12px 14px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:10px">
        <div style="font-size:0.72rem;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">Expectations</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.78rem">
          ${target ? `<div><span style="color:var(--text-muted)">Target Price</span><br/><span style="font-weight:600;color:var(--green)">${_esc(String(target))}</span></div>` : ""}
          ${stopLoss ? `<div><span style="color:var(--text-muted)">Stop Loss</span><br/><span style="font-weight:600;color:var(--red)">${_esc(String(stopLoss))}</span></div>` : ""}
          <div>
            <span style="color:var(--text-muted)">Timeframe</span><br/>
            <span style="font-weight:600">${_esc(timeframe)}</span>
          </div>
          ${score ? `<div><span style="color:var(--text-muted)">Expert Score</span><br/><span style="font-weight:600;color:var(--accent)">${score}/10</span></div>` : ""}
        </div>
      </div>

      <!-- Expert Consensus Funnel -->
      <div style="margin-bottom:20px;padding:12px 14px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);border-radius:10px">
        <div style="font-size:0.72rem;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">Expert Consensus</div>
        <div style="font-size:0.78rem;color:var(--text)">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
            <div style="flex:1;height:6px;background:rgba(99,102,241,0.15);border-radius:3px;overflow:hidden">
              <div style="width:100%;height:100%;background:var(--accent);border-radius:3px"></div>
            </div>
            <span style="font-size:0.7rem;color:var(--text-dim);white-space:nowrap">14 Domain Specialists</span>
          </div>
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
            <div style="flex:1;height:6px;background:rgba(99,102,241,0.15);border-radius:3px;overflow:hidden">
              <div style="width:70%;height:100%;background:var(--accent);border-radius:3px"></div>
            </div>
            <span style="font-size:0.7rem;color:var(--text-dim);white-space:nowrap">6 Cross-Domain Analysts</span>
          </div>
          <div style="display:flex;align-items:center;gap:8px">
            <div style="flex:1;height:6px;background:rgba(99,102,241,0.15);border-radius:3px;overflow:hidden">
              <div style="width:35%;height:100%;background:var(--accent);border-radius:3px"></div>
            </div>
            <span style="font-size:0.7rem;color:var(--text-dim);white-space:nowrap">1 CIO Synthesis</span>
          </div>
        </div>
      </div>

      <!-- Other Picks Navigation -->
      ${picks.length > 1 ? `
      <div style="border-top:1px solid rgba(255,255,255,0.06);padding-top:14px">
        <div style="font-size:0.72rem;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">Other Picks</div>
        <div style="display:flex;flex-wrap:wrap;gap:6px">
          ${picks.map((p, j) => j === idx ? "" : `
            <button onclick="window._showPickDetail(${j})" style="
              background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
              border-radius:8px;padding:6px 12px;cursor:pointer;color:var(--text);font-size:0.78rem;
              transition:background 0.15s;
            " onmouseenter="this.style.background='rgba(255,255,255,0.08)'" onmouseleave="this.style.background='rgba(255,255,255,0.04)'">
              <span style="font-weight:600">${_esc(p.ticker || "?")}</span>
              <span style="color:${actionColors[(p.action || "").toLowerCase()] || "var(--text-dim)"};font-size:0.68rem;margin-left:4px">${_esc((p.action || "").toUpperCase())}</span>
            </button>
          `).join("")}
        </div>
      </div>` : ""}
    </div>
  `;

  // Add animations if not already present
  if (!document.getElementById("swarm-pick-styles")) {
    const style = document.createElement("style");
    style.id = "swarm-pick-styles";
    style.textContent = `
      @keyframes fadeIn { from { opacity: 0 } to { opacity: 1 } }
      @keyframes slideInRight { from { transform: translateX(100%) } to { transform: translateX(0) } }
      @keyframes slideOutRight { from { transform: translateX(0) } to { transform: translateX(100%) } }
    `;
    document.head.appendChild(style);
  }

  overlay.style.display = "block";
};

window._closePickDetail = function() {
  const overlay = document.getElementById("swarm-pick-overlay");
  if (overlay) {
    const panel = overlay.querySelector(".swarm-overlay-panel");
    if (panel) {
      panel.style.animation = "slideOutRight 0.15s ease";
      setTimeout(() => { overlay.style.display = "none"; }, 150);
    } else {
      overlay.style.display = "none";
    }
  }
};

function renderSwarmFeed() {
  const feed = state.swarmFeed;
  if (!feed || !feed.verdicts || feed.verdicts.length === 0) return "";

  const verdictColor = { STRONG_BUY: "var(--green)", BUY: "var(--green)", HOLD: "var(--yellow)", SELL: "var(--red)", STRONG_SELL: "var(--red)" };
  const verdictIcon = { STRONG_BUY: "🟢", BUY: "🟢", HOLD: "🟡", SELL: "🔴", STRONG_SELL: "🔴" };

  const _hasV3 = state.advisory && state.advisory.swarm_v3;
  const _feedSubtitle = _hasV3
    ? `Swarm verdicts`
    : `${feed.verdicts.length} expert panel verdicts`;

  let html = `<div class="swarm-feed">
    <div class="swarm-feed__header">
      <span>🧠 Swarm Intelligence</span>
      <span class="swarm-feed__subtitle">${_feedSubtitle}</span>
    </div>
    <div class="swarm-feed__list">`;

  for (const v of feed.verdicts.slice(0, 6)) {
    const age = v.created_at ? Math.round((Date.now() - new Date(v.created_at).getTime()) / 60000) : null;
    const ageText = age != null ? (age < 60 ? `${age}m ago` : `${Math.floor(age/60)}h ago`) : "";
    const confPct = Math.round((v.confidence || 0) * 100);
    const color = verdictColor[v.verdict] || "var(--text-dim)";
    const icon = verdictIcon[v.verdict] || "⚪";

    const recentCls = (age != null && age < 30) ? " swarm-verdict-card--recent" : "";
    const verdictType = (v.verdict || "").toLowerCase().replace("strong_", "");
    html += `<div class="swarm-verdict-card swarm-verdict-card--${verdictType}${recentCls}">
      <div class="swarm-verdict-card__top">
        <span class="swarm-verdict-card__verdict" style="color:${color}">${icon} ${v.verdict}</span>
        ${v.ticker ? `<span class="swarm-verdict-card__ticker" data-chart-ticker="${_esc(v.ticker)}">${_esc(v.ticker)}</span>` : ""}
        <span class="swarm-verdict-card__conf">${confPct}% consensus</span>
        <span class="swarm-verdict-card__age">${ageText}</span>
      </div>
      <div class="swarm-verdict-card__reasoning">${_esc(v.entry_reasoning).slice(0, 200)}${v.entry_reasoning.length > 200 ? "…" : ""}</div>
      ${v.dissent_note ? `<div class="swarm-verdict-card__dissent"><span style="opacity:0.5">Dissent:</span> ${_esc(v.dissent_note).slice(0, 150)}${v.dissent_note.length > 150 ? "…" : ""}</div>` : ""}
    </div>`;
  }

  html += `</div></div>`;
  return html;
}




// ══════════════════════════════════════════════════════════════
// Feed Tab
// ══════════════════════════════════════════════════════════════

function renderFeedTab() {
  const items = state.feedData;
  const regions = new Set();
  const sources = new Set();
  if (Array.isArray(items)) {
    items.forEach(b => { if (b.region) regions.add(b.region); if (b.source) sources.add(b.source); });
  }

  let html = `<div class="feed-tab">`;
  html += `<div class="feed-filters">`;
  html += `<select class="feed-filters__select" data-feed-filter="region"><option value="">All Regions</option>`;
  [...regions].sort().forEach(r => { html += `<option value="${esc(r)}">${esc(r)}</option>`; });
  html += `</select>`;
  html += `<select class="feed-filters__select" data-feed-filter="source"><option value="">All Sources</option>`;
  [...sources].sort().forEach(s => { html += `<option value="${esc(s)}">${esc(s)}</option>`; });
  html += `</select>`;
  html += `<select class="feed-filters__select" data-feed-filter="sentiment">
    <option value="">All Sentiment</option>
    <option value="positive">Positive</option>
    <option value="negative">Negative</option>
    <option value="neutral">Neutral</option>
  </select>`;
  html += `<select class="feed-filters__select" data-feed-filter="intensity">
    <option value="">All Intensity</option>
    <option value="low">Low</option>
    <option value="moderate">Moderate</option>
    <option value="high-threat">High Threat</option>
    <option value="critical">Critical</option>
  </select>`;
  html += `</div>`;

  if (!Array.isArray(items) || items.length === 0) {
    html += `<div class="feed-empty">No articles match your filters</div>`;
  } else {
    html += `<div class="feed-list">`;
    items.forEach(b => {
      const sentClass = (b.sentiment || "").toLowerCase();
      const sentDot = sentClass === "positive" ? "var(--green)" : sentClass === "negative" ? "var(--red)" : "var(--yellow)";
      const intLabel = b.intensity || "unknown";
      const intClass = intLabel.toLowerCase().replace(/[\s_]/g, "-");
      const summary = b.summary ? esc(b.summary.slice(0, 100)) + (b.summary.length > 100 ? "..." : "") : "";
      const timeStr = b.processed_at ? ago(new Date(b.processed_at).getTime()) : "";
      html += `<div class="feed-card">
        <div class="feed-card__header">
          <span class="feed-card__title">${esc(b.title || "Untitled")}</span>
          <span class="feed-card__time">${timeStr}</span>
        </div>
        <div class="feed-card__meta">
          <span class="feed-card__sentiment" style="--dot-color:${sentDot}"></span>
          <span class="feed-card__source">${esc(b.source || "")}</span>
          ${b.region ? `<span class="feed-card__region">${esc(b.region)}</span>` : ""}
          <span class="feed-card__intensity feed-card__intensity--${esc(intClass)}">${esc(intLabel)}</span>
        </div>
        ${summary ? `<div class="feed-card__summary">${summary}</div>` : ""}
      </div>`;
    });
    html += `</div>`;
    html += `<div class="feed-load-more-wrap"><button class="btn btn--dim" data-feed-load-more>Load more</button></div>`;
  }
  html += `</div>`;
  return html;
}

function bindFeedTabEvents() {
  document.querySelectorAll("[data-feed-filter]").forEach(sel => {
    sel.addEventListener("change", () => {
      const params = {};
      document.querySelectorAll("[data-feed-filter]").forEach(s => {
        const key = s.dataset.feedFilter;
        const val = s.value;
        if (val) params[key] = val;
      });
      state.feedData = null;
      state.feedOffset = 0;
      fetchFeed(params).then(render);
    });
  });
  document.querySelector("[data-feed-load-more]")?.addEventListener("click", async () => {
    const btn = document.querySelector("[data-feed-load-more]");
    if (btn) { btn.disabled = true; btn.textContent = "Loading..."; }
    state.feedOffset += 50;
    const params = { offset: String(state.feedOffset) };
    document.querySelectorAll("[data-feed-filter]").forEach(s => {
      const val = s.value;
      if (val) params[s.dataset.feedFilter] = val;
    });
    try {
      const qs = new URLSearchParams({ limit: "50", ...params }).toString();
      const r = await authFetch(`${API_BASE}?_api=briefs&${qs}`);
      if (r.ok) {
        const more = await r.json();
        if (Array.isArray(more) && more.length > 0) {
          state.feedData = (state.feedData || []).concat(more);
        }
      }
    } catch (e) { console.warn("[API] fetchFeed (load more) failed:", e.message); }
    render();
  });
}

// ══════════════════════════════════════════════════════════════
// ML Tab (Admin only)
// ══════════════════════════════════════════════════════════════

async function fetchMLData() {
  try {
    const [advHistRes, signalsRes, swarmRes, overviewRes] = await Promise.all([
      authFetch(`${API_BASE}?_api=advisory-history&limit=30`),
      authFetch(`${API_BASE}?_api=signals`),
      authFetch(`${API_BASE}?_api=swarm-status`),
      authFetch(`${API_BASE}?_api=overview`),
    ]);
    state.mlData = {};
    if (advHistRes.ok) state.mlData.advisoryHistory = await advHistRes.json();
    if (signalsRes.ok) state.mlData.signals = await signalsRes.json();
    if (swarmRes.ok) state.mlData.swarmStatus = await swarmRes.json();
    if (overviewRes.ok) state.mlData.overview = await overviewRes.json();
  } catch (e) { console.warn("[API] fetchMLData failed:", e.message); }
}

function renderMLTab() {
  let html = "";

  // Section 1: Data Funnel
  html += renderMLFunnel();

  // Section 2: Prediction Scorecard
  html += renderMLScorecard();

  // Section 3: How We Learn
  html += renderMLHowWeLearn();

  // Section 4: Next Calibration
  html += renderMLNextCalibration();

  return html;
}

function renderMLFunnel() {
  const sysStatus = state.status || {};
  const overview = state.overview || {};
  const mlOverview = (state.mlData && state.mlData.overview) || {};
  const sw = (state.swarmActivity && state.swarmActivity.status) || (state.mlData && state.mlData.swarmStatus) || {};

  const articles = sysStatus.total_articles || sysStatus.total_briefs || mlOverview.article_count || 0;
  const narratives = (state.runups || []).length || mlOverview.narrative_count || 0;
  const verdicts = sw.total_verdicts || sw.active_verdicts || 0;
  const predictions = overview.predictions || 0;

  const stages = [
    { count: articles, label: "Articles Ingested", size: "wide" },
    { count: narratives, label: "Narratives Detected", size: "medium" },
    { count: verdicts, label: "Swarm Verdicts", size: "narrow" },
    { count: predictions, label: "Predictions Made", size: "output" },
  ];

  let html = `<div class="ml-section">
    <div class="ml-section__title">Intelligence Pipeline</div>
    <div class="ml-funnel">`;

  // Calculate swarm countdown progress for the verdicts orb
  const swarmSt = (state.swarmActivity && state.swarmActivity.status) || {};
  let swarmProgressDeg = 0;
  if (swarmSt.next_run) {
    const intervalMs = (swarmSt.interval_minutes || 60) * 60000;
    const diff = new Date(swarmSt.next_run) - Date.now();
    if (diff > 0 && diff < intervalMs) {
      const elapsed = intervalMs - diff;
      swarmProgressDeg = Math.round((elapsed / intervalMs) * 360);
    }
  }

  stages.forEach((s, i) => {
    const countStr = typeof s.count === "number" ? s.count.toLocaleString() : s.count;
    const isVerdicts = s.label === "Swarm Verdicts";
    const orbExtra = isVerdicts ? `<div style="position:absolute;inset:-4px;border-radius:50%;background:conic-gradient(var(--accent) ${swarmProgressDeg}deg, var(--border) ${swarmProgressDeg}deg);z-index:0;opacity:0.6"></div>` : "";
    html += `<div class="ml-funnel__stage ml-funnel__stage--${s.size}">
        <div class="ml-funnel__count" style="${isVerdicts ? 'position:relative;z-index:1' : ''}">${orbExtra}${countStr}</div>
        <div class="ml-funnel__label">${s.label}</div>
      </div>`;
    if (i < stages.length - 1) {
      html += `<div class="ml-funnel__arrow">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none"><path d="M12 5v14m0 0l-5-5m5 5l5-5" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </div>`;
    }
  });

  html += `</div></div>`;
  return html;
}

function renderMLScorecard() {
  const mlOverview = state.mlData && state.mlData.overview;
  const resolved = (mlOverview && (mlOverview.resolved_predictions || (mlOverview.stats && mlOverview.stats.resolved_predictions))) || [];
  const allResolved = resolved.length > 0 ? resolved : ((state.overview && state.overview.resolved_predictions) || []);

  // Use predictions count from overview (same source as funnel) for consistency
  const overviewPredictions = (state.overview && state.overview.predictions) || 0;
  const total = allResolved.length > 0 ? allResolved.length : overviewPredictions;
  const correctCount = allResolved.filter(r => r.correct).length;
  const incorrectCount = allResolved.length > 0 ? allResolved.length - correctCount : 0;
  const accuracy = allResolved.length > 0
    ? Math.round(correctCount / allResolved.length * 100)
    : (state.overview && state.overview.accuracy ? Math.round(state.overview.accuracy) : 0);

  const cards = [
    { value: total, label: "Total Predictions", color: "var(--accent)", bg: "var(--accent-dim)" },
    { value: correctCount, label: "Correct", color: "var(--green)", bg: "var(--green-dim)" },
    { value: incorrectCount, label: "Incorrect", color: "var(--red)", bg: "var(--red-dim)" },
    { value: accuracy + "%", label: "Accuracy", color: accuracy >= 60 ? "var(--green)" : accuracy >= 40 ? "var(--yellow)" : "var(--red)", bg: accuracy >= 60 ? "var(--green-dim)" : accuracy >= 40 ? "var(--yellow-dim)" : "var(--red-dim)" },
  ];

  let html = `<div class="ml-section">
    <div class="ml-section__title">Prediction Scorecard</div>
    <div class="ml-scorecard">`;

  for (const c of cards) {
    html += `<div class="ml-scorecard__card" style="border-color:${c.color};background:${c.bg}">
      <div class="ml-scorecard__value" style="color:${c.color}">${c.value}</div>
      <div class="ml-scorecard__label">${c.label}</div>
    </div>`;
  }

  html += `</div></div>`;
  return html;
}

function renderMLHowWeLearn() {
  const defaultWeights = [
    { name: "Swarm", weight: 20 },
    { name: "Runup", weight: 15 },
    { name: "Polymarket", weight: 15 },
    { name: "News", weight: 15 },
    { name: "Sources", weight: 15 },
    { name: "ML", weight: 20 },
  ];

  const hist = state.advisoryHistory || (state.mlData && state.mlData.advisoryHistory);
  const liveWeights = hist && hist.learning_stats && hist.learning_stats.advisory_weights;

  let weightBars = "";
  const colors = ["var(--accent)", "var(--green)", "var(--purple)", "var(--yellow)", "var(--cyan)", "var(--orange)"];

  if (liveWeights && typeof liveWeights === "object") {
    const entries = Object.entries(liveWeights);
    entries.forEach(([comp, w], i) => {
      const pctVal = (w * 100).toFixed(0);
      const color = colors[i % colors.length];
      weightBars += `<div class="ml-weight-bar">
        <div class="ml-weight-bar__label">${esc(comp)}</div>
        <div class="ml-weight-bar__track">
          <div class="ml-weight-bar__fill" style="width:${Math.min(pctVal, 100)}%;background:${color}"></div>
        </div>
        <div class="ml-weight-bar__pct" style="color:${color}">${pctVal}%</div>
      </div>`;
    });
  } else {
    defaultWeights.forEach((w, i) => {
      const color = colors[i % colors.length];
      weightBars += `<div class="ml-weight-bar">
        <div class="ml-weight-bar__label">${w.name}</div>
        <div class="ml-weight-bar__track">
          <div class="ml-weight-bar__fill" style="width:${w.weight}%;background:${color}"></div>
        </div>
        <div class="ml-weight-bar__pct" style="color:${color}">${w.weight}%</div>
      </div>`;
    });
  }

  return `<div class="ml-section">
    <div class="ml-section__title">How We Learn</div>
    <div class="ml-learn">
      <div class="ml-learn__header">
        <span class="ml-learn__icon">&#x1F504;</span>
        <span class="ml-learn__badge">Self-Improving Algorithm</span>
      </div>
      <p class="ml-learn__desc">Our system tracks every prediction it makes. When outcomes are confirmed, we adjust the weights of each analysis component. Components that predict well get more influence. Components that fail get reduced.</p>
      <div class="ml-learn__weights-title">Current component weights${liveWeights ? " (live)" : ""}:</div>
      <div class="ml-weight-bars">${weightBars}</div>
    </div>
  </div>`;
}

function renderMLNextCalibration() {
  const now = new Date();
  const nextSunday = new Date(now);
  const dayOfWeek = now.getUTCDay();
  const daysUntilSunday = dayOfWeek === 0
    ? (now.getUTCHours() < 7 || (now.getUTCHours() === 7 && now.getUTCMinutes() < 35) ? 0 : 7)
    : (7 - dayOfWeek);
  nextSunday.setUTCDate(now.getUTCDate() + daysUntilSunday);
  nextSunday.setUTCHours(7, 35, 0, 0);

  const diff = nextSunday - now;
  const totalHoursInWeek = 7 * 24;
  const hoursElapsed = totalHoursInWeek - (diff / 3600000);
  const progressPct = Math.min(Math.max((hoursElapsed / totalHoursInWeek) * 100, 0), 100);

  const days = Math.floor(diff / 86400000);
  const hours = Math.floor((diff % 86400000) / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);

  let countdown = "";
  if (days > 0) countdown = `${days}d ${hours}h ${mins}m`;
  else if (hours > 0) countdown = `${hours}h ${mins}m`;
  else countdown = `${mins}m`;

  const dateStr = nextSunday.toLocaleDateString("en-US", { weekday: "long", month: "short", day: "numeric" });

  return `<div class="ml-section">
    <div class="ml-section__title">Next Calibration</div>
    <div class="ml-calibration">
      <div class="ml-calibration__countdown">${countdown}</div>
      <div class="ml-calibration__detail">Weight rebalancing on ${dateStr} at 07:35 UTC</div>
      <div class="ml-calibration__bar">
        <div class="ml-calibration__progress" style="width:${progressPct.toFixed(1)}%"></div>
      </div>
    </div>
  </div>`;
}

function bindMLTabEvents() {
  // Refresh button
  document.querySelector("[data-refresh-ml]")?.addEventListener("click", async () => {
    state.mlData = null;
    await fetchMLData();
    render();
  });

  // M2: Real-time polling every 60s when ML tab is active
  if (window._mlPollInterval) clearInterval(window._mlPollInterval);
  window._mlPollInterval = setInterval(async () => {
    if (state.activeTab !== "ml") { clearInterval(window._mlPollInterval); return; }
    const prevOverview = JSON.stringify(state.overview || {});
    await fetchOverview();
    const newOverview = JSON.stringify(state.overview || {});
    if (prevOverview !== newOverview) {
      // Add pulse animation to changed elements
      document.querySelectorAll(".ml-funnel__count, .ml-scorecard__value").forEach(el => {
        el.style.transition = "transform 0.3s ease";
        el.style.transform = "scale(1.15)";
        setTimeout(() => { el.style.transform = "scale(1)"; }, 300);
      });
      render();
    }
  }, 60000);
}

// ══════════════════════════════════════════════════════════════
// Users Tab (Admin only)
// ══════════════════════════════════════════════════════════════

async function fetchUsers() {
  try {
    const r = await authFetch(`${API_BASE}?_api=admin-users`);
    if (r.ok) state.usersData = await r.json();
  } catch (e) { console.warn("[API] fetchUsers failed:", e.message); }
}

function timeAgo(date) {
  const mins = Math.floor((Date.now() - date.getTime()) / 60000);
  if (mins < 60) return `${mins}m ago`;
  if (mins < 1440) return `${Math.floor(mins/60)}h ago`;
  return `${Math.floor(mins/1440)}d ago`;
}

function renderUsersTab() {
  const data = state.usersData;
  if (!data || !data.users) {
    return `<div class="tab-content"><div class="empty-state"><div class="empty-state__icon">👥</div><div class="empty-state__text">Loading users...</div></div></div>`;
  }

  let html = `<div class="tab-content">
    <div class="section-title">Team <span class="badge">${data.users.length}</span></div>
    <div class="users-grid">`;

  for (const u of data.users) {
    const initials = (u.first_name?.[0] || '') + (u.last_name?.[0] || '');
    const joined = u.created_at ? new Date(u.created_at).toLocaleDateString("nl-NL") : "—";
    const lastLogin = u.last_login ? timeAgo(new Date(u.last_login)) : "Never";

    html += `<div class="user-card">
      <div class="user-card__avatar">${initials.toUpperCase()}</div>
      <div class="user-card__info">
        <div class="user-card__name">${_esc(u.first_name)} ${_esc(u.last_name)}</div>
        <div class="user-card__email">${_esc(u.email)}</div>
        <div class="user-card__meta">
          ${u.is_admin ? '<span class="badge badge--green">Admin</span>' : ''}
          ${u.locked ? '<span class="badge badge--red">Locked</span>' : ''}
          <span>${u.holdings_count} stocks</span>
          <span>Joined ${joined}</span>
        </div>
      </div>
      <div class="user-card__actions">
        ${u.locked ? `<button class="btn btn--xs" data-unlock-user="${u.id}">Unlock</button>` : ''}
        ${!u.is_admin ? `<button class="btn btn--xs btn--danger" data-delete-user="${u.id}" data-username="${_esc(u.username)}">Remove</button>` : ''}
      </div>
    </div>`;
  }
  html += `</div></div>`;
  return html;
}

function bindUsersTabEvents() {
  document.querySelectorAll("[data-delete-user]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const uid = btn.dataset.deleteUser;
      const uname = btn.dataset.username;
      if (!confirm(`Delete user "${uname}"? This cannot be undone.`)) return;
      try {
        const r = await authFetch(`/api/admin/users/${uid}`, { method: "DELETE" });
        if (!r.ok) { const d = await r.json(); showError(d.detail || "Delete failed"); return; }
        await fetchUsers();
        render();
      } catch (e) { showError("Failed to delete user: " + e.message); }
    });
  });

  document.querySelectorAll("[data-unlock-user]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const uid = btn.dataset.unlockUser;
      try {
        const r = await authFetch(`/api/admin/users/${uid}/unlock`, { method: "PUT" });
        if (!r.ok) { const d = await r.json(); showError(d.detail || "Failed"); return; }
        await fetchUsers();
        render();
      } catch (e) { showError("Failed: " + e.message); }
    });
  });
}

/* ══════════════════════════════════════════════════════════════
   Swarm Tab — Real-time swarm activity visualization
   ══════════════════════════════════════════════════════════════ */

async function fetchSwarmActivity() {
  try {
    const [feedRes, statusRes, indicatorsRes] = await Promise.all([
      authFetch(`${API_BASE}?_api=swarm-feed`),
      authFetch(`${API_BASE}?_api=swarm-status`),
      authFetch(`${API_BASE}?_api=indicators`),
    ]);
    state.swarmActivity = {};
    if (feedRes.ok) state.swarmActivity.feed = await feedRes.json();
    if (statusRes.ok) state.swarmActivity.status = await statusRes.json();
    if (indicatorsRes.ok) state.swarmActivity.indicators = await indicatorsRes.json();
    state._swarmFetchedAt = Date.now();
  } catch (e) { console.warn("[API] swarm activity failed:", e.message); }
}

function renderSwarmTab() {
  const sa = state.swarmActivity || {};
  const feedRaw = sa.feed || {};
  const feedArr = feedRaw.verdicts || (Array.isArray(feedRaw) ? feedRaw : []);
  const feed = Array.isArray(feedArr) ? feedArr : [];
  const status = sa.status || {};
  const indicators = sa.indicators || {};

  // Pipeline stage data — pull from correct state sources
  const sysStatus = state.status || {};
  const pipelineStages = [
    { label: "Sources", count: sysStatus.feeds_configured || sysStatus.feeds || 108, icon: "\u{1F4E1}" },
    { label: "NLP", count: sysStatus.total_articles || sysStatus.total_briefs || 0, icon: "\u{1F9E0}" },
    { label: "Narratives", count: (state.runups || []).length || 0, icon: "\u{1F4CA}" },
    { label: "Decision Trees", count: status.total_verdicts || 0, icon: "\u{1F333}" },
    { label: "Swarm Debate", count: status.active_verdicts || 0, icon: "\u{1F41D}" },
    { label: "Signals", count: (state.signals || []).length || 0, icon: "\u{26A1}" },
  ];

  // Section A: Pipeline
  let pipelineHtml = `<div class="swarm-section"><h2 class="swarm-section__title">Data Pipeline</h2><div class="pipeline">`;
  const pipelineStageIds = ["sources", "nlp", "narratives", "trees", "swarm", "signals"];
  pipelineStages.forEach((stage, i) => {
    const active = i <= 4 ? " pipeline-orb--active" : "";
    pipelineHtml += `
      <div class="pipeline-stage" data-pipeline-stage="${pipelineStageIds[i]}" style="cursor:pointer">
        <div class="pipeline-orb${active}">
          <span class="pipeline-orb__icon">${stage.icon}</span>
          <span class="pipeline-orb__count">${typeof stage.count === "number" ? stage.count.toLocaleString() : stage.count}</span>
        </div>
        <div class="pipeline-stage__label">${stage.label}</div>
      </div>`;
    if (i < pipelineStages.length - 1) {
      pipelineHtml += `<div class="pipeline-flow"><div class="pipeline-connector"></div><div class="pipeline-dot"></div><div class="pipeline-dot pipeline-dot--delay1"></div><div class="pipeline-dot pipeline-dot--delay2"></div></div>`;
    }
  });
  pipelineHtml += `</div></div>`;

  // Section B: Countdown (with seconds, live-updating)
  let countdownHtml = "";
  const nextRun = status.next_run;
  let countdownText = "Awaiting schedule";
  if (nextRun) {
    const diff = new Date(nextRun) - new Date();
    if (diff > 0) {
      const totalSecs = Math.floor(diff / 1000);
      const hrs = Math.floor(totalSecs / 3600);
      const mins = Math.floor((totalSecs % 3600) / 60);
      const secs = totalSecs % 60;
      countdownText = hrs > 0 ? `${hrs}h ${mins}m ${secs}s` : `${mins}m ${secs}s`;
    } else {
      countdownText = "In progress...";
    }
  }
  // Store nextRun timestamp for live updates
  if (nextRun) window._swarmNextRun = new Date(nextRun).getTime();
  countdownHtml = `
    <div class="swarm-section">
      <div class="swarm-countdown">
        <div class="swarm-countdown__label">Next Swarm Meeting</div>
        <div class="swarm-countdown__timer" id="swarm-countdown-timer">${countdownText}</div>
        <div class="swarm-countdown__sub">${nextRun ? "Scheduled: " + new Date(nextRun).toLocaleString() : "Every " + (status.interval_minutes || 60) + " minutes"}</div>
      </div>
    </div>`;

  // Section C: Expert Panel
  const experts = [
    { role: "Geopolitical Analyst", icon: "\u{1F30D}" },
    { role: "Energy Trader", icon: "\u{26FD}" },
    { role: "Macro Economist", icon: "\u{1F4B9}" },
    { role: "Sentiment Analyst", icon: "\u{1F4AC}" },
    { role: "Technical Analyst", icon: "\u{1F4C8}" },
    { role: "Risk Manager", icon: "\u{1F6E1}\uFE0F" },
    { role: "Contrarian", icon: "\u{1F504}" },
    { role: "Supply Chain", icon: "\u{1F69A}" },
    { role: "Portfolio Advisor", icon: "\u{1F4BC}" },
    { role: "Military Strategy", icon: "\u{2694}\uFE0F" },
    { role: "Regulatory Analyst", icon: "\u{2696}\uFE0F" },
    { role: "Sector Rotation", icon: "\u{1F504}" },
  ];

  // Try to match expert verdicts from feed
  const expertVerdicts = {};
  feed.forEach(v => {
    if (v.expert_role && !expertVerdicts[v.expert_role]) {
      expertVerdicts[v.expert_role] = v.verdict || v.direction;
    }
  });

  let expertHtml = `<div class="swarm-section"><h2 class="swarm-section__title">Expert Panel</h2><div class="expert-grid">`;
  experts.forEach(exp => {
    const verdict = expertVerdicts[exp.role] || "PENDING";
    const vCls = verdict === "BUY" ? "verdict--buy" : verdict === "SELL" ? "verdict--sell" : verdict === "HOLD" ? "verdict--hold" : "verdict--pending";
    expertHtml += `
      <div class="expert-card" data-expert-role="${_esc(exp.role)}" data-expert-icon="${exp.icon}" style="cursor:pointer">
        <div class="expert-card__icon">${exp.icon}</div>
        <div class="expert-card__role">${exp.role}</div>
        <div class="expert-card__verdict ${vCls}">${verdict}</div>
      </div>`;
  });
  // "+" button to add more experts
  expertHtml += `
    <div class="expert-card expert-card--add" data-add-expert style="cursor:pointer;border:2px dashed var(--border);display:flex;align-items:center;justify-content:center;flex-direction:column;opacity:0.6">
      <div class="expert-card__icon" style="font-size:28px">+</div>
      <div class="expert-card__role" style="font-size:11px">Add Expert</div>
    </div>`;
  expertHtml += `</div></div>`;

  // Section D: Live Verdict Stream
  let verdictHtml = `<div class="swarm-section"><h2 class="swarm-section__title">Live Verdict Stream</h2>`;
  if (feed.length === 0) {
    verdictHtml += `<div class="empty-state">No verdicts yet. The swarm has not debated.</div>`;
  } else {
    verdictHtml += `<div class="verdict-timeline">`;
    feed.slice(0, 30).forEach(v => {
      const verdict = v.verdict || v.direction || "HOLD";
      const vCls = verdict === "BUY" ? "verdict--buy" : verdict === "SELL" ? "verdict--sell" : verdict === "HOLD" ? "verdict--hold" : "verdict--pending";
      const confidence = v.confidence != null ? v.confidence : 0;
      const confPct = Math.round(confidence * 100);
      const reasoning = v.entry_reasoning || v.reasoning || v.summary || v.rationale || "";
      // Try to extract a ticker from reasoning if not provided
      let ticker = v.ticker || v.symbol || "";
      if (!ticker && reasoning) {
        const tickerMatch = reasoning.match(/\b([A-Z]{2,5}(?:\.[A-Z]{2})?)\b/);
        if (tickerMatch && !["THE","AND","FOR","BUT","NOT","HOLD","BUY","SELL","THIS","THAT","WITH","FROM"].includes(tickerMatch[1])) ticker = tickerMatch[1];
      }
      if (!ticker) ticker = "—";
      const timeStr = (v.created_at || v.timestamp) ? new Date(v.created_at || v.timestamp).toLocaleTimeString() : "";
      verdictHtml += `
        <div class="verdict-item" data-verdict-detail='${_esc(JSON.stringify(v))}' style="cursor:pointer">
          <div class="verdict-item__dot ${vCls}"></div>
          <div class="verdict-item__content">
            <div class="verdict-item__header">
              <span class="verdict-item__time">${timeStr}</span>
              <span class="verdict-badge ${vCls}">${verdict}</span>
              <span class="verdict-item__ticker" data-chart-ticker="${_esc(ticker)}">${_esc(ticker)}</span>
            </div>
            <div class="verdict-item__confidence">
              <div class="verdict-item__conf-bar"><div class="verdict-item__conf-fill ${vCls}" style="width:${confPct}%"></div></div>
              <span class="verdict-item__conf-label">${confPct}%</span>
            </div>
            ${reasoning ? `<div class="verdict-item__reasoning">${_esc(reasoning).substring(0, 200)}</div>` : ""}
          </div>
        </div>`;
    });
    verdictHtml += `</div>`;
  }
  verdictHtml += `</div>`;

  // Section E: Consensus Breakdown
  let buyCount = 0, holdCount = 0, sellCount = 0;
  let totalConf = 0;
  feed.forEach(v => {
    const dir = (v.verdict || v.direction || "").toUpperCase();
    if (dir === "BUY") buyCount++;
    else if (dir === "SELL") sellCount++;
    else holdCount++;
    totalConf += (v.confidence || 0);
  });
  const totalVerdicts = feed.length || 1;
  const avgConf = Math.round((totalConf / totalVerdicts) * 100);
  const buyPct = Math.round((buyCount / totalVerdicts) * 100);
  const holdPct = Math.round((holdCount / totalVerdicts) * 100);
  const sellPct = 100 - buyPct - holdPct;

  // CSS conic gradient donut
  const conicGrad = `conic-gradient(var(--green) 0% ${buyPct}%, var(--yellow) ${buyPct}% ${buyPct + holdPct}%, var(--red) ${buyPct + holdPct}% 100%)`;

  let consensusHtml = `
    <div class="swarm-section">
      <h2 class="swarm-section__title">Consensus Breakdown</h2>
      <div class="consensus-grid">
        <div class="consensus-donut-wrap">
          <div class="consensus-donut" style="background: ${conicGrad}">
            <div class="consensus-donut__inner">
              <div class="consensus-donut__total">${feed.length}</div>
              <div class="consensus-donut__label">Verdicts</div>
            </div>
          </div>
        </div>
        <div class="consensus-stats">
          <div class="consensus-stat">
            <span class="consensus-stat__dot" style="background:var(--green)"></span>
            <span class="consensus-stat__label">BUY</span>
            <span class="consensus-stat__count">${buyCount}</span>
            <span class="consensus-stat__pct">${buyPct}%</span>
          </div>
          <div class="consensus-stat">
            <span class="consensus-stat__dot" style="background:var(--yellow)"></span>
            <span class="consensus-stat__label">HOLD</span>
            <span class="consensus-stat__count">${holdCount}</span>
            <span class="consensus-stat__pct">${holdPct}%</span>
          </div>
          <div class="consensus-stat">
            <span class="consensus-stat__dot" style="background:var(--red)"></span>
            <span class="consensus-stat__label">SELL</span>
            <span class="consensus-stat__count">${sellCount}</span>
            <span class="consensus-stat__pct">${sellPct}%</span>
          </div>
          <div class="consensus-stat consensus-stat--avg">
            <span class="consensus-stat__label">Avg Confidence</span>
            <span class="consensus-stat__pct">${avgConf}%</span>
          </div>
        </div>
      </div>
    </div>`;

  return `<div class="swarm-tab">${pipelineHtml}${countdownHtml}${expertHtml}${verdictHtml}${consensusHtml}</div>`;
}

function bindSwarmTabEvents() {
  // Refresh button
  document.querySelector("[data-refresh-swarm]")?.addEventListener("click", async () => {
    await fetchSwarmActivity();
    render();
  });

  // Live countdown with seconds
  if (window._swarmNextRun) {
    if (window._swarmCountdownInterval) clearInterval(window._swarmCountdownInterval);
    window._swarmCountdownInterval = setInterval(() => {
      const el = document.getElementById("swarm-countdown-timer");
      if (!el) { clearInterval(window._swarmCountdownInterval); return; }
      const diff = window._swarmNextRun - Date.now();
      if (diff <= 0) { el.textContent = "In progress..."; return; }
      const totalSecs = Math.floor(diff / 1000);
      const hrs = Math.floor(totalSecs / 3600);
      const mins = Math.floor((totalSecs % 3600) / 60);
      const secs = totalSecs % 60;
      el.textContent = hrs > 0 ? `${hrs}h ${mins}m ${secs}s` : `${mins}m ${secs}s`;
    }, 1000);
  }

  // S1: Pipeline node click → slide-in with stage details
  document.querySelectorAll("[data-pipeline-stage]").forEach(el => {
    el.addEventListener("click", () => _showPipelineDetail(el.dataset.pipelineStage));
  });

  // S3: Expert card click → slide-in with role description
  document.querySelectorAll(".expert-card[data-expert-role]").forEach(card => {
    card.addEventListener("click", () => _showExpertDetail(card.dataset.expertRole, card.dataset.expertIcon));
  });

  // S4: "+" button → add expert slide-in
  document.querySelector("[data-add-expert]")?.addEventListener("click", () => _showAddExpertPanel());

  // S5: Verdict item click → detail popup
  document.querySelectorAll("[data-verdict-detail]").forEach(el => {
    el.addEventListener("click", (e) => {
      if (e.target.closest("[data-chart-ticker]")) return;
      try {
        const v = JSON.parse(el.dataset.verdictDetail);
        _showVerdictDetail(v);
      } catch {}
    });
  });
}

// ── S1: Pipeline detail slide-in ──
function _showPipelineDetail(stage) {
  const sysStatus = state.status || {};
  const sa = state.swarmActivity || {};
  const swarmSt = sa.status || {};
  const runups = state.runups || [];
  const articleCount = sysStatus.total_articles || sysStatus.total_briefs || 0;
  const feedCount = sysStatus.feeds_configured || sysStatus.feeds || 108;
  const narrativeCount = runups.length;

  // Build narrative list for the slide-in
  let narrativeList = "";
  if (runups.length > 0) {
    narrativeList = `<div style="margin-top:12px">` + runups.slice(0, 10).map(r => {
      const name = (r.narrative_name || r.name || "").replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());
      const score = r.current_score || r.score || 0;
      return `<div style="padding:6px 0;border-bottom:1px solid var(--border);display:flex;justify-content:space-between"><span>${_esc(name)}</span><span style="color:var(--text-dim)">${Math.round(score)}pts · ${r.article_count_total || r.article_count || 0} articles</span></div>`;
    }).join("") + `</div>`;
  }

  // Build verdict breakdown
  const vbt = swarmSt.verdicts_by_type || {};
  const verdictBreakdown = Object.keys(vbt).length > 0
    ? Object.entries(vbt).map(([k,v]) => `<span style="margin-right:12px"><strong>${k}:</strong> ${v}</span>`).join("")
    : "No breakdown available";

  const details = {
    sources: { title: "RSS Sources", body: `<p><strong>${feedCount.toLocaleString()}</strong> RSS feeds actively monitored</p><p>Sources include Reuters, AP, BBC, Al Jazeera, TASS, and 100+ geopolitical news outlets.</p><p>Feeds refresh every ${sysStatus.fetch_interval_minutes || 15} minutes to capture breaking developments.</p>` },
    nlp: { title: "NLP Processing", body: `<p><strong>${articleCount.toLocaleString()}</strong> articles processed</p><p>Each article is analyzed with spaCy NER and VADER sentiment scoring.</p><p>Entities (countries, leaders, organizations) and sentiment intensity are extracted to build narrative clusters.</p>` },
    narratives: { title: "Narrative Tracking", body: `<p><strong>${narrativeCount}</strong> active narratives detected</p>${narrativeList}<p style="margin-top:12px">Articles are clustered into narratives based on entity overlap and topic similarity. Narrative momentum determines which stories are escalating.</p>` },
    trees: { title: "Decision Trees", body: `<p><strong>${swarmSt.total_verdicts || 0}</strong> total decision evaluations generated</p><p>Each escalating narrative spawns a game-theory decision tree with Claude Haiku.</p><p>Trees model YES/NO probabilities and their consequences for specific stocks and sectors.</p>` },
    swarm: { title: "Swarm Debate", body: `<p><strong>${swarmSt.total_verdicts || 0}</strong> total verdicts · <strong>${swarmSt.active_verdicts || 0}</strong> active</p><p>${verdictBreakdown}</p><p style="margin-top:8px">Interval: every ${swarmSt.interval_minutes || 60} minutes</p><p>13 experts debate each decision node in 2 rounds before reaching consensus.</p>` },
    signals: { title: "Trading Signals", body: `<p><strong>${(state.signals || []).length}</strong> active trading signals</p><p>Signals combine swarm verdicts, narrative momentum, price momentum, and prediction market data into composite confidence scores.</p>` },
  };

  const d = details[stage] || { title: stage, body: "No details available." };
  _openGenericSlidein(d.title, d.body);
}

// ── S3: Expert detail slide-in ──
function _showExpertDetail(role, icon) {
  const expertDescriptions = {
    "Geopolitical Analyst": "Specializes in power dynamics between nation-states, military alliances, sanctions regimes, and territorial disputes. Assesses how political events translate to market-moving catalysts.",
    "Energy Trader": "Senior energy and commodities trader specializing in oil (WTI/Brent), natural gas, gold, and shipping routes. Understands OPEC dynamics, strategic petroleum reserves, and pipeline politics.",
    "Macro Economist": "Focuses on central bank policy (Fed, ECB, BOJ), inflation dynamics, GDP growth, trade flows, and currency movements. Thinks about second-order effects on interest rates and capital flows.",
    "Sentiment Analyst": "Tracks news narrative intensity, social media momentum, retail investor positioning, and crowd psychology. Detects when markets are pricing in fear vs greed.",
    "Technical Analyst": "Focuses on price patterns, support/resistance levels, moving averages, RSI, MACD, and volume analysis. Identifies optimal entry/exit points based on chart structure.",
    "Risk Manager": "Protects capital by analyzing tail risks, maximum drawdown, correlation breakdowns, liquidity risks, and black swan events. Recommends position sizing and stops.",
    "Contrarian": "Challenges the consensus view. When others are bullish, finds reasons for caution. When others are bearish, finds reasons for optimism. Skeptical of groupthink.",
    "Supply Chain": "Analyzes supply chain cascades, structural shortages, and second-order effects. Finds non-obvious beneficiaries — like Zoom during COVID. Thinks 3-12 months out.",
    "Portfolio Advisor": "Gives advice specific to the user's actual portfolio holdings. Analyzes exposure risk, rebalance signals, and position sizing based on current allocations.",
    "Military Strategy": "Specializes in escalation ladders, defense industry contracts, force posture as a leading indicator, and ammunition production bottlenecks.",
    "Regulatory Analyst": "Tracks EU sanctions packages, US OFAC SDN List, export controls, CHIPS Act restrictions, and regulatory catalysts that create sector rotation opportunities.",
    "Sector Rotation": "Tracks GICS sector rotation, relative strength, earnings cycles, and defensive vs cyclical positioning. Provides specific rotation trades with entry logic.",
  };

  const desc = expertDescriptions[role] || "Expert analyst in the swarm consensus panel.";

  // Get recent verdicts for this expert from feed
  const sa = state.swarmActivity || {};
  const feed = (sa.feed && sa.feed.verdicts) || (Array.isArray(sa.feed) ? sa.feed : []);
  const expertVerdicts = feed.filter(v => v.expert_role === role).slice(0, 5);

  let verdictHistory = "";
  if (expertVerdicts.length > 0) {
    verdictHistory = `<div style="margin-top:16px"><div style="font-weight:600;margin-bottom:8px;color:var(--text-dim);font-size:12px;text-transform:uppercase">Recent Verdicts</div>`;
    for (const v of expertVerdicts) {
      const vCls = v.verdict === "BUY" ? "color:var(--green)" : v.verdict === "SELL" ? "color:var(--red)" : "color:var(--yellow)";
      verdictHistory += `<div style="padding:6px 0;border-bottom:1px solid var(--border);font-size:13px">
        <span style="${vCls};font-weight:700">${v.verdict || "HOLD"}</span>
        <span style="margin-left:8px">${v.ticker || "N/A"}</span>
        <span style="margin-left:8px;color:var(--text-dim)">${v.confidence ? Math.round(v.confidence * 100) + "%" : ""}</span>
      </div>`;
    }
    verdictHistory += `</div>`;
  }

  _openGenericSlidein(`${icon || ""} ${role}`, `<p style="line-height:1.6">${_esc(desc)}</p>${verdictHistory}`);
}

// ── S4: Add Expert panel ──
async function _showAddExpertPanel() {
  let body = `<div style="text-align:center;color:var(--text-dim)">Loading experts...</div>`;
  const panel = _openGenericSlidein("Add Expert", body);

  try {
    const r = await authFetch(`${API_BASE}?_api=swarm-experts`);
    if (!r.ok) throw new Error("Failed to load");
    const experts = await r.json();
    const inactive = experts.filter(e => !e.enabled);
    const active = experts.filter(e => e.enabled);

    let html = `<div style="margin-bottom:12px;color:var(--text-dim);font-size:13px">${active.length} active / ${experts.length} total experts</div>`;
    for (const e of experts) {
      html += `<div style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border)">
        <span style="font-size:20px">${e.emoji}</span>
        <div style="flex:1">
          <div style="font-weight:600;font-size:14px">${_esc(e.name)}</div>
          <div style="font-size:12px;color:var(--text-dim);margin-top:2px">${_esc((e.system || "").substring(0, 100))}...</div>
        </div>
        <button class="btn btn--xs ${e.enabled ? "btn--danger" : ""}" data-toggle-expert="${e.id}" style="min-width:70px">
          ${e.enabled ? "Disable" : "Enable"}
        </button>
      </div>`;
    }

    const contentEl = panel.querySelector("#generic-slidein-content");
    if (contentEl) contentEl.innerHTML = html;

    // Bind toggle buttons
    panel.querySelectorAll("[data-toggle-expert]").forEach(btn => {
      btn.addEventListener("click", async () => {
        const eid = btn.dataset.toggleExpert;
        try {
          const resp = await authFetch(`${API_BASE}?_api=swarm-expert-toggle&id=${eid}`, { method: "PUT" });
          if (resp.ok) {
            const result = await resp.json();
            btn.textContent = result.enabled ? "Disable" : "Enable";
            btn.classList.toggle("btn--danger", result.enabled);
          }
        } catch (err) { console.warn("Toggle failed:", err); }
      });
    });
  } catch (err) {
    const contentEl = panel.querySelector("#generic-slidein-content");
    if (contentEl) contentEl.innerHTML = `<div style="color:var(--red)">Failed to load experts</div>`;
  }
}

// ── S5: Verdict detail popup ──
function _showVerdictDetail(v) {
  const verdict = v.verdict || "HOLD";
  const vColor = verdict.includes("BUY") ? "var(--green)" : verdict.includes("SELL") ? "var(--red)" : "var(--yellow)";
  const confidence = v.confidence != null ? Math.round(v.confidence * 100) : 0;
  const ticker = v.ticker || v.symbol || "N/A";
  const timeStr = (v.created_at || v.timestamp) ? new Date(v.created_at || v.timestamp).toLocaleString() : "";

  let body = `
    <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px">
      <span style="font-size:24px;font-weight:700;color:${vColor}">${verdict}</span>
      <span style="font-size:20px;font-weight:600" data-chart-ticker="${_esc(ticker)}" style="cursor:pointer">${_esc(ticker)}</span>
      <span style="color:var(--text-dim)">${confidence}% confidence</span>
    </div>`;

  if (timeStr) body += `<div style="color:var(--text-dim);font-size:12px;margin-bottom:12px">${timeStr}</div>`;

  if (v.entry_reasoning || v.reasoning) {
    body += `<div style="margin-bottom:12px"><div style="font-weight:600;font-size:12px;text-transform:uppercase;color:var(--text-dim);margin-bottom:4px">Entry Reasoning</div>
      <div style="line-height:1.6;white-space:pre-wrap">${_esc(v.entry_reasoning || v.reasoning || "")}</div></div>`;
  }
  if (v.dissent_note) {
    body += `<div style="margin-bottom:12px"><div style="font-weight:600;font-size:12px;text-transform:uppercase;color:var(--text-dim);margin-bottom:4px">Dissent</div>
      <div style="line-height:1.6;white-space:pre-wrap">${_esc(v.dissent_note)}</div></div>`;
  }
  if (v.risk_note) {
    body += `<div style="margin-bottom:12px"><div style="font-weight:600;font-size:12px;text-transform:uppercase;color:var(--text-dim);margin-bottom:4px">Risk Note</div>
      <div style="line-height:1.6">${_esc(v.risk_note)}</div></div>`;
  }
  if (v.exit_trigger) {
    body += `<div><div style="font-weight:600;font-size:12px;text-transform:uppercase;color:var(--text-dim);margin-bottom:4px">Exit Trigger</div>
      <div style="line-height:1.6">${_esc(v.exit_trigger)}</div></div>`;
  }
  if (v.consensus_strength != null) {
    body += `<div style="margin-top:12px;color:var(--text-dim);font-size:12px">Consensus strength: ${Math.round(v.consensus_strength * 100)}%</div>`;
  }

  _openGenericSlidein(`${_esc(ticker)} — Verdict Detail`, body);
}

// ── Generic slide-in helper ──
function _openGenericSlidein(title, bodyHtml) {
  document.getElementById("generic-slidein")?.remove();
  document.querySelector(".generic-slidein-backdrop")?.remove();

  const backdrop = document.createElement("div");
  backdrop.className = "feed-backdrop generic-slidein-backdrop";
  document.body.appendChild(backdrop);
  requestAnimationFrame(() => backdrop.classList.add("open"));

  const panel = document.createElement("div");
  panel.id = "generic-slidein";
  panel.className = "feed-panel";
  panel.innerHTML = `
    <div class="feed-panel__header">
      <h3>${title}</h3>
      <button class="feed-panel__close" data-close-slidein>&times;</button>
    </div>
    <div class="feed-panel__articles" id="generic-slidein-content" style="padding:16px">${bodyHtml}</div>
  `;
  document.body.appendChild(panel);
  requestAnimationFrame(() => panel.classList.add("open"));

  const closePanel = () => {
    panel.classList.remove("open");
    backdrop.classList.remove("open");
    setTimeout(() => { panel.remove(); backdrop.remove(); }, 300);
  };
  panel.querySelector("[data-close-slidein]").addEventListener("click", closePanel);
  backdrop.addEventListener("click", closePanel);
  document.addEventListener("keydown", function escH(e) {
    if (e.key === "Escape") { closePanel(); document.removeEventListener("keydown", escH); }
  });

  return panel;
}

// Detect embed mode (loaded inside OpenClaw control UI iframe)
const _embedMode = new URLSearchParams(window.location.search).get("embed") === "1";

// Boot
syncTheme();
initRoute();
// Fetch current user info
async function fetchCurrentUser() {
  try {
    const r = await authFetch("/auth/me");
    if (r.ok) state.currentUser = await r.json();
  } catch (e) { /* not logged in */ }
}

const bootFetches = [
  fetchCurrentUser(),
  fetchOverview(),
  fetchSignals(),
  fetchIndicators(),
  fetchOpportunities(),
  fetchFocus(),
  fetchFlashAlerts(),
  fetchAdvisory(),
  fetchAdvisoryHistory(),
  fetchPortfolioAlignment(),
];
// If landing on usage tab, fetch usage data immediately
if (state.activeTab === "usage") {
  bootFetches.push(fetchUsage(_usageDays));
}
// If landing on settings tab, also fetch settings data immediately
if (state.activeTab === "settings") {
  bootFetches.push(fetchFeeds(), fetchBudget(), fetchApiKeyStatus(), fetchSwarmStatus());
}
// Always fetch swarm status (needed for portfolio countdown + ML funnel)
bootFetches.push(fetchSwarmActivity());
Promise.all(bootFetches).then(() => render());

// Refresh indicators every 5 min
setInterval(fetchIndicators, 300000);
startPolling();
