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
  analysis: null,        // Latest deep analysis report
  signals: [],           // Active trading signals
  indicators: null,      // BTC, Gold, VIX prices
  loading: true,
  error: null,
};

let activeTreeId = null; // which run-up is expanded
let pollTimer = null;
let treePollTimer = null;
let feedsExpanded = false;
let _priceModalTicker = null; // currently open price chart ticker
let feedFormVisible = false;
let _lastView = null; // "overview" | "tree" — tracks view to preserve scroll on poll refresh
let _cyInstance = null; // Cytoscape canvas instance for tree visualization

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
      state.autoScorer = data.auto_scorer || null;
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
        // Raw tree nodes for Cytoscape canvas visualization
        tree: nodes,
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

  const currentView = activeTreeId ? "tree" : "overview";
  const preserveScroll = (_lastView === currentView);
  const scrollY = window.scrollY;

  // If a tree is active, show tree detail view
  if (activeTreeId) {
    app.innerHTML = renderTreeView();
    bindTreeEvents();
    // Initialize Cytoscape canvas after DOM is ready
    if (state.activeTree) {
      requestAnimationFrame(() => initTreeCanvas());
    }
    if (preserveScroll) window.scrollTo(0, scrollY);
    _lastView = currentView;
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
    _lastView = currentView;
    return;
  }

  // Market indicators bar (BTC, Gold, VIX)
  html += renderIndicatorBar();

  // Summary cards
  html += renderSummaryCards();

  // Trading Signals panel
  html += renderSignalsPanel();

  // Intelligence Briefing panel
  html += renderBriefingPanel();

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
  bindSignalEvents();
  bindBriefingEvents();
  bindFeedEvents();
  bindBudgetEvents();
  bindGlobalChartClicks();
  if (preserveScroll) window.scrollTo(0, scrollY);
  _lastView = currentView;
}

function bindGlobalChartClicks() {
  document.querySelectorAll("[data-chart-ticker]").forEach(el => {
    if (el._chartBound) return;
    el._chartBound = true;
    el.style.cursor = "pointer";
    el.addEventListener("click", (e) => {
      e.stopPropagation();
      const ticker = el.dataset.chartTicker;
      if (ticker) openPriceChart(ticker);
    });
  });
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

async function fetchAnalysis() {
  try {
    const res = await fetch(`${API_BASE}?_api=analysis`);
    if (res.ok) {
      const data = await res.json();
      if (data && data.data && !data.status) {
        state.analysis = data;
      }
    }
  } catch (err) {
    console.warn("[news-analyzer] fetch analysis error:", err);
  }
}

async function triggerAnalysis() {
  try {
    const res = await fetch(`${API_BASE}?_api=analysis-run`, { method: "POST" });
    if (res.ok) {
      // Wait a moment then re-fetch
      setTimeout(() => { fetchAnalysis().then(render); }, 2000);
    }
  } catch (err) {
    console.warn("[news-analyzer] trigger analysis error:", err);
  }
}

async function fetchSignals() {
  try {
    const res = await fetch(`${API_BASE}?_api=signals`);
    if (res.ok) {
      state.signals = await res.json();
    }
  } catch (err) {
    console.warn("[news-analyzer] fetch signals error:", err);
  }
}

async function triggerSignalRefresh() {
  try {
    await fetch(`${API_BASE}?_api=signals-refresh`, { method: "POST" });
    setTimeout(() => { fetchSignals().then(render); }, 1500);
  } catch (err) {
    console.warn("[news-analyzer] trigger signals error:", err);
  }
}

async function fetchIndicators() {
  try {
    const res = await fetch(`${API_BASE}?_api=indicators`);
    if (res.ok) state.indicators = await res.json();
  } catch (err) {
    console.warn("[news-analyzer] fetch indicators error:", err);
  }
}

/* ── Render: Market Indicator Bar ──────────────────────────── */

function renderIndicatorBar() {
  const ind = state.indicators;
  if (!ind) return "";

  function fmtInd(label, data, prefix, suffix) {
    if (!data || data.error) return `<div class="ind-item"><span class="ind-label">${label}</span><span class="ind-val">--</span></div>`;
    const price = data.price != null ? data.price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "--";
    const chg = data.change_pct != null ? data.change_pct : 0;
    const chgCls = chg >= 0 ? "ind-chg--up" : "ind-chg--down";
    const chgStr = (chg >= 0 ? "+" : "") + chg.toFixed(2) + "%";
    return `<div class="ind-item">
      <span class="ind-label">${label}</span>
      <span class="ind-val">${prefix || ""}${price}${suffix || ""}</span>
      <span class="ind-chg ${chgCls}">${chgStr}</span>
    </div>`;
  }

  return `<div class="indicator-bar">
    ${fmtInd("BTC", ind.bitcoin, "\u20ac")}
    ${fmtInd("Gold", ind.gold, "\u20ac", "/kg")}
    ${fmtInd("Oil", ind.oil, "$")}
    ${fmtInd("VIX", ind.vix, "")}
  </div>`;
}

/* ── Price Chart Modal ─────────────────────────────────────── */

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
  // Remove old modal if any
  let modal = document.getElementById("price-modal");
  if (modal) modal.remove();

  modal = document.createElement("div");
  modal.id = "price-modal";
  modal.className = "price-modal-overlay";
  modal.innerHTML = `<div class="price-modal">
    <div class="price-modal__header">
      <span class="price-modal__ticker">${esc(ticker)}</span>
      <span class="price-modal__price" id="pm-price">Loading...</span>
      <div class="price-modal__periods">
        ${["1mo","3mo","6mo","1y"].map(p =>
          `<button class="price-modal__period-btn${p === period ? " price-modal__period-btn--active" : ""}" data-pm-period="${p}">${p.toUpperCase()}</button>`
        ).join("")}
      </div>
      <button class="price-modal__close" id="pm-close">&times;</button>
    </div>
    <div class="price-modal__chart" id="pm-chart"></div>
  </div>`;
  document.body.appendChild(modal);

  // Close handlers
  document.getElementById("pm-close").addEventListener("click", closePriceChart);
  modal.addEventListener("click", (e) => { if (e.target === modal) closePriceChart(); });
  document.addEventListener("keydown", function _escHandler(e) {
    if (e.key === "Escape") { closePriceChart(); document.removeEventListener("keydown", _escHandler); }
  });

  // Period buttons
  modal.querySelectorAll("[data-pm-period]").forEach(btn => {
    btn.addEventListener("click", () => {
      _priceModalTicker = ticker;
      _renderPriceModal(ticker, btn.dataset.pmPeriod);
    });
  });

  // Fetch quote + chart data
  try {
    const [quoteRes, chartRes] = await Promise.all([
      fetch(`${API_BASE}?_api=price&ticker=${encodeURIComponent(ticker)}`),
      fetch(`${API_BASE}?_api=price-chart&ticker=${encodeURIComponent(ticker)}&period=${period}`),
    ]);
    const quote = quoteRes.ok ? await quoteRes.json() : {};
    const candles = chartRes.ok ? await chartRes.json() : [];

    // Update price display
    const priceEl = document.getElementById("pm-price");
    if (priceEl && quote.price != null) {
      const chg = quote.change_pct || 0;
      const chgCls = chg >= 0 ? "ind-chg--up" : "ind-chg--down";
      priceEl.innerHTML = `$${quote.price.toLocaleString("en-US", {minimumFractionDigits: 2})}
        <span class="${chgCls}">${chg >= 0 ? "+" : ""}${chg.toFixed(2)}%</span>
        <small>${esc(quote.name || "")}</small>`;
    }

    // Render candlestick chart with TradingView Lightweight Charts
    const chartContainer = document.getElementById("pm-chart");
    if (chartContainer && candles.length > 0 && typeof LightweightCharts !== "undefined") {
      chartContainer.innerHTML = "";
      const chart = LightweightCharts.createChart(chartContainer, {
        width: chartContainer.clientWidth,
        height: 400,
        layout: { background: { color: "#1a1d25" }, textColor: "#e0e0e6" },
        grid: { vertLines: { color: "#2a2e38" }, horzLines: { color: "#2a2e38" } },
        crosshair: { mode: 0 },
        timeScale: { borderColor: "#2a2e38", timeVisible: false },
        rightPriceScale: { borderColor: "#2a2e38" },
      });
      const candleSeries = chart.addCandlestickSeries({
        upColor: "#34d399", downColor: "#f87171",
        borderUpColor: "#34d399", borderDownColor: "#f87171",
        wickUpColor: "#34d399", wickDownColor: "#f87171",
      });
      candleSeries.setData(candles);

      // Volume histogram
      if (candles[0] && candles[0].volume != null) {
        const volSeries = chart.addHistogramSeries({
          priceFormat: { type: "volume" },
          priceScaleId: "vol",
        });
        chart.priceScale("vol").applyOptions({
          scaleMargins: { top: 0.85, bottom: 0 },
        });
        volSeries.setData(candles.map(c => ({
          time: c.time,
          value: c.volume,
          color: c.close >= c.open ? "rgba(52,211,153,0.3)" : "rgba(248,113,113,0.3)",
        })));
      }

      chart.timeScale().fitContent();
      // Resize observer
      const ro = new ResizeObserver(() => {
        chart.applyOptions({ width: chartContainer.clientWidth });
      });
      ro.observe(chartContainer);
    } else if (chartContainer) {
      chartContainer.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-dim);">No chart data available</div>`;
    }
  } catch (err) {
    console.warn("[news-analyzer] price chart error:", err);
    const chartContainer = document.getElementById("pm-chart");
    if (chartContainer) chartContainer.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-dim);">Failed to load chart</div>`;
  }
}

/* ── Render: Trading Signals Panel ──────────────────────────── */

function renderSignalsPanel() {
  const sigs = state.signals || [];
  if (!sigs.length) return "";

  const levelOrder = { STRONG_BUY: 0, BUY: 1, ALERT: 2, WATCH: 3 };
  const sorted = [...sigs].sort((a, b) => (levelOrder[a.signal_level] || 9) - (levelOrder[b.signal_level] || 9));

  let rows = "";
  for (const s of sorted.slice(0, 10)) {
    const lvl = s.signal_level || "WATCH";
    const lvlCls = lvl === "STRONG_BUY" || lvl === "BUY" ? "sig--buy"
                 : lvl === "ALERT" ? "sig--alert" : "sig--watch";

    const arrow = s.direction === "bullish" ? "&#9650;" : (s.direction === "bearish" ? "&#9660;" : "");
    const dirCls = s.direction === "bullish" ? "sig-dir--bull" : (s.direction === "bearish" ? "sig-dir--bear" : "");

    const tickerHtml = s.ticker
      ? `<span class="sig-ticker ${dirCls}" data-chart-ticker="${esc(s.ticker)}">${arrow} ${esc(s.ticker)}</span>`
      : "";

    // Build one-line summary with available data points
    const meta = [];
    if (s.news_count > 0) meta.push(`${s.news_count} sources`);
    if (s.polymarket_prob != null) meta.push(`Polymarket ${Math.round(s.polymarket_prob * 100)}%`);
    if (s.x_signal_count > 0) meta.push(`${s.x_signal_count} tweets`);
    const metaStr = meta.length ? meta.join(" &middot; ") : "Monitoring";

    rows += `<div class="sig-row ${lvlCls}">
      <div class="sig-row__main">
        <span class="sig-level">${lvl.replace("_", " ")}</span>
        <span class="sig-narr">${esc(s.narrative_name || "")}</span>
        ${tickerHtml}
      </div>
      <div class="sig-row__meta">${metaStr}</div>
    </div>`;
  }

  return `<div class="signals-panel">
    <div class="signals-header">
      <div class="signals-title">Signals</div>
      <button class="refresh-btn signals-refresh-btn" data-refresh-signals>&#8635;</button>
    </div>
    ${rows}
  </div>`;
}

function bindSignalEvents() {
  const refreshBtn = document.querySelector("[data-refresh-signals]");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", async () => {
      refreshBtn.disabled = true;
      refreshBtn.textContent = "...";
      try {
        const res = await fetch(`${API_BASE}?_api=signals`);
        if (res.ok) {
          state.signals = await res.json();
          renderApp();
        }
      } catch (err) {
        console.warn("[signals] refresh failed:", err);
      }
      refreshBtn.disabled = false;
      refreshBtn.textContent = "↻";
    });
  }
  // Ticker click -> price chart
  document.querySelectorAll("[data-chart-ticker]").forEach((el) => {
    el.addEventListener("click", (e) => {
      e.stopPropagation();
      const ticker = el.dataset.chartTicker;
      if (ticker && typeof openPriceChart === "function") openPriceChart(ticker);
    });
  });
}

/* ── Render: Intelligence Briefing ────────────────────────────── */

let briefingExpanded = false;

function renderBriefingPanel() {
  const a = state.analysis;
  if (!a || !a.data) {
    return `<div class="briefing-panel briefing-panel--empty">
      <div class="briefing-header">
        <div class="briefing-title"><span class="briefing-icon">&#x1F4CA;</span> Intelligence Briefing</div>
        <button class="refresh-btn briefing-run-btn" data-run-analysis>Run Analysis Now</button>
      </div>
      <div class="briefing-empty">No analysis report yet. Click "Run Analysis Now" to generate one.</div>
    </div>`;
  }

  const d = a.data;
  const createdAt = a.created_at ? ago(new Date(a.created_at).getTime()) : "";

  // Trending keywords (objects: {keyword, ratio})
  let trendingHtml = "";
  if (d.vocabulary && d.vocabulary.trending_keywords) {
    const trending = d.vocabulary.trending_keywords.slice(0, 8);
    trendingHtml = trending.map(item => {
      const kw = item.keyword || item[0] || "";
      const ratio = item.ratio || item[1] || 0;
      const r = Math.round(ratio * 100);
      const cls = r > 300 ? "briefing-kw--hot" : r > 200 ? "briefing-kw--warm" : "briefing-kw--mild";
      return `<span class="briefing-kw ${cls}">${esc(kw)} <small>+${r}%</small></span>`;
    }).join(" ");
  }

  // Most active sources
  let sourcesHtml = "";
  if (d.sources && d.sources.source_activity) {
    const top = d.sources.source_activity.slice(0, 5);
    sourcesHtml = top.map(s =>
      `<span class="briefing-source">${esc(s.source)} <small>(${s.article_count})</small></span>`
    ).join(" &middot; ");
  }

  // Regional threat levels
  let regionsHtml = "";
  if (d.regions && d.regions.regions) {
    const regions = d.regions.regions.slice(0, 6);
    regionsHtml = regions.map(r => {
      const tl = r.threat_level || 0;
      const dot = tl >= 0.7 ? "threat-dot--critical" : tl >= 0.4 ? "threat-dot--high" : "threat-dot--low";
      return `<span class="briefing-region"><span class="threat-dot ${dot}"></span>${esc(r.region)} <small>(${(tl * 100).toFixed(0)}%)</small></span>`;
    }).join(" ");
  }

  // Narrative connections
  let narrativeHtml = "";
  if (d.narratives && d.narratives.relationships) {
    const rels = d.narratives.relationships.slice(0, 4);
    narrativeHtml = rels.map(r =>
      `<div class="briefing-relation">${esc(r.narrative_a)} &#8596; ${esc(r.narrative_b)} <small>(${(r.keyword_overlap * 100).toFixed(0)}% overlap)</small></div>`
    ).join("");
  }

  // Temporal patterns
  let temporalHtml = "";
  if (d.temporal) {
    const parts = [];
    if (d.temporal.peak_hours && d.temporal.peak_hours.length > 0) {
      const peaks = d.temporal.peak_hours.slice(0, 3).map(item => {
        const h = item.hour != null ? item.hour : (item[0] != null ? item[0] : 0);
        return `${String(h).padStart(2, "0")}:00`;
      });
      parts.push(`Peak: ${peaks.join(", ")} UTC`);
    }
    if (d.temporal.bursts && d.temporal.bursts.length > 0) {
      const burst = d.temporal.bursts[0];
      parts.push(`Burst: ${burst.date} (${burst.ratio}x)`);
    }
    if (d.temporal.avg_daily_articles) {
      parts.push(`Avg: ${d.temporal.avg_daily_articles} articles/day`);
    }
    temporalHtml = parts.map(p => `<span class="briefing-temporal">${p}</span>`).join(" &middot; ");
  }

  // Threat words (objects: {keyword, avg_sentiment, count})
  let threatHtml = "";
  if (d.vocabulary && d.vocabulary.threat_associated_words && d.vocabulary.threat_associated_words.length > 0) {
    const threats = d.vocabulary.threat_associated_words.slice(0, 6);
    threatHtml = threats.map(item => {
      const kw = item.keyword || item[0] || "";
      const sent = item.avg_sentiment || item[1] || 0;
      return `<span class="briefing-kw briefing-kw--threat">${esc(kw)} <small>${Number(sent).toFixed(2)}</small></span>`;
    }).join(" ");
  }

  // Top keywords (objects: {keyword, count})
  let topKwHtml = "";
  if (d.vocabulary && d.vocabulary.top_keywords) {
    const topKw = d.vocabulary.top_keywords.slice(0, 12);
    topKwHtml = topKw.map(item => {
      const kw = item.keyword || item[0] || "";
      const count = item.count || item[1] || 0;
      return `<span class="briefing-kw briefing-kw--top">${esc(kw)} <small>${count}</small></span>`;
    }).join(" ");
  }

  // Expanded section: narrative trends
  let narrativeTrendsHtml = "";
  if (briefingExpanded && d.narratives && d.narratives.narratives) {
    const narrs = d.narratives.narratives.slice(0, 10);
    narrativeTrendsHtml = `<div class="briefing-section">
      <div class="briefing-label">Narrative Trends</div>
      <div class="briefing-narrative-list">
        ${narrs.map(n => {
          const t = trendArrow(n.trend);
          return `<div class="briefing-narrative-item">
            <span class="${t.cls}">${t.symbol}</span>
            <span class="briefing-narr-name">${esc(n.narrative)}</span>
            <small>${n.total_articles} articles &middot; sent ${n.avg_sentiment.toFixed(2)}</small>
          </div>`;
        }).join("")}
      </div>
    </div>`;
  }

  // Expanded section: source details
  let sourceDetailsHtml = "";
  if (briefingExpanded && d.sources && d.sources.source_activity) {
    const srcs = d.sources.source_activity.slice(0, 10);
    sourceDetailsHtml = `<div class="briefing-section">
      <div class="briefing-label">Source Details</div>
      <div class="briefing-source-list">
        ${srcs.map(s => {
          const cred = s.credibility || 0.6;
          const credCls = cred >= 0.85 ? "cred--high" : cred >= 0.6 ? "cred--mid" : "cred--low";
          return `<div class="briefing-source-item">
            <span class="briefing-src-name">${esc(s.source)}</span>
            <span class="briefing-src-stat">${s.article_count} articles</span>
            <span class="briefing-src-stat">sent ${s.avg_sentiment.toFixed(2)}</span>
            <span class="briefing-cred ${credCls}">${(cred * 100).toFixed(0)}%</span>
          </div>`;
        }).join("")}
      </div>
    </div>`;
  }

  // ── Strategic Outlook: Buy the Rumour, Sell the News ──
  let strategicHtml = "";
  const outlook = d.strategic_outlook;
  const narrative = d.strategic_narrative;

  if (outlook && (outlook.total_signals > 0 || narrative)) {
    let worldHtml = "";
    if (narrative && narrative.world_direction) {
      worldHtml = `<div class="strategic-world">${esc(narrative.world_direction)}</div>`;
    }

    // Buy opportunities
    let buyHtml = "";
    const buyItems = (narrative && narrative.buy_opportunities) || [];
    if (buyItems.length > 0) {
      buyHtml = `<div class="strategic-sub">
        <div class="strategic-sub-label">&#x1F4B0; Buy Opportunities <small>(rumour phase)</small></div>
        ${buyItems.slice(0, 5).map(b => {
          const urgCls = b.urgency === "high" ? "stock-pick--urgent" : "";
          return `<div class="stock-pick stock-pick--buy ${urgCls}" title="${esc(b.reasoning || "")}">
            <span class="stock-pick__arrow">&#9650;</span>
            <span class="stock-pick__ticker" data-chart-ticker="${esc(b.ticker || "")}">${esc(b.ticker || "")}</span>
            <span class="stock-pick__name">${esc(b.name || "")}</span>
            <span class="stock-pick__reason">${esc(b.reasoning || "")}</span>
            ${b.timeframe ? `<span class="stock-pick__time">${esc(b.timeframe)}</span>` : ""}
          </div>`;
        }).join("")}
      </div>`;
    }

    // Sell signals
    let sellHtml = "";
    const sellItems = (narrative && narrative.sell_signals) || [];
    if (sellItems.length > 0) {
      sellHtml = `<div class="strategic-sub">
        <div class="strategic-sub-label">&#x1F4C9; Sell Signals <small>(news phase)</small></div>
        ${sellItems.slice(0, 3).map(s =>
          `<div class="stock-pick stock-pick--sell" title="${esc(s.reasoning || "")}">
            <span class="stock-pick__arrow">&#9660;</span>
            <span class="stock-pick__ticker" data-chart-ticker="${esc(s.ticker || "")}">${esc(s.ticker || "")}</span>
            <span class="stock-pick__name">${esc(s.name || "")}</span>
            <span class="stock-pick__reason">${esc(s.reasoning || "")}</span>
          </div>`
        ).join("")}
      </div>`;
    }

    // Sectors to watch
    let sectorHtml = "";
    const sectors = (narrative && narrative.sectors_to_watch) || [];
    if (sectors.length > 0) {
      sectorHtml = `<div class="strategic-sub">
        <div class="strategic-sub-label">&#x1F3ED; Sectors</div>
        <div class="sector-badges">
          ${sectors.map(s => {
            const cls = s.direction === "bullish" ? "sector-badge--bull" : "sector-badge--bear";
            const arrow = s.direction === "bullish" ? "&#9650;" : "&#9660;";
            return `<span class="sector-badge ${cls}" title="${esc(s.reasoning || "")}">${arrow} ${esc(s.sector || "")}</span>`;
          }).join(" ")}
        </div>
      </div>`;
    }

    // Risk warning
    let riskHtml = "";
    if (narrative && narrative.risk_warning) {
      riskHtml = `<div class="risk-warning">&#x26A0;&#xFE0F; ${esc(narrative.risk_warning)}</div>`;
    }

    // Fallback: pure data stock picks (when no Claude narrative)
    let fallbackPicksHtml = "";
    if (!narrative && outlook.top_picks && outlook.top_picks.length > 0) {
      fallbackPicksHtml = `<div class="strategic-sub">
        <div class="strategic-sub-label">&#x1F4CA; Top Stock Signals</div>
        ${outlook.top_picks.slice(0, 8).map(p => {
          const cls = p.direction === "bullish" ? "stock-pick--buy" : "stock-pick--sell";
          const arrow = p.direction === "bullish" ? "&#9650;" : "&#9660;";
          const reason = (p.top_reasons && p.top_reasons[0])
            ? p.top_reasons[0].reasoning : "";
          return `<div class="stock-pick ${cls}" title="${esc(reason)}">
            <span class="stock-pick__arrow">${arrow}</span>
            <span class="stock-pick__ticker" data-chart-ticker="${esc(p.ticker)}">${esc(p.ticker)}</span>
            <span class="stock-pick__name">${esc(p.name)}</span>
            <span class="stock-pick__score">${p.net_score}</span>
            <span class="stock-pick__narr">${esc((p.narratives || []).slice(0, 2).join(", "))}</span>
          </div>`;
        }).join("")}
      </div>`;
    }

    // Phase indicators
    let phaseHtml = "";
    const rumours = outlook.rumour_phase || [];
    const news = outlook.news_phase || [];
    if (rumours.length > 0 || news.length > 0) {
      const phaseItems = [];
      for (const r of rumours.slice(0, 3)) {
        const accel = r.acceleration > 1 ? " &#8593;&#8593;" : " &#8593;";
        phaseItems.push(`<span class="phase-item phase-item--rumour">${esc(r.narrative)} <small>(${r.days_active}d${accel})</small></span>`);
      }
      for (const n of news.slice(0, 3)) {
        phaseItems.push(`<span class="phase-item phase-item--news">${esc(n.narrative)} <small>(${n.days_active}d, ${(n.confirmed_ratio * 100).toFixed(0)}% confirmed)</small></span>`);
      }
      phaseHtml = `<div class="strategic-sub">
        <div class="strategic-sub-label">&#x1F504; Narrative Phases</div>
        <div class="phase-indicators">${phaseItems.join(" ")}</div>
      </div>`;
    }

    strategicHtml = `<div class="strategic-section">
      <div class="strategic-header">&#x1F3AF; Strategic Outlook &mdash; Buy the Rumour, Sell the News</div>
      ${worldHtml}
      ${buyHtml}
      ${sellHtml}
      ${fallbackPicksHtml}
      ${sectorHtml}
      ${riskHtml}
      ${phaseHtml}
    </div>`;
  }

  return `<div class="briefing-panel">
    <div class="briefing-header">
      <div class="briefing-title">
        <span class="briefing-icon">&#x1F4CA;</span> Intelligence Briefing
        ${createdAt ? `<small class="briefing-age">${createdAt}</small>` : ""}
      </div>
      <div class="briefing-actions">
        <button class="refresh-btn briefing-toggle-btn" data-briefing-toggle>${briefingExpanded ? "Collapse" : "Full Report"}</button>
        <button class="refresh-btn briefing-run-btn" data-run-analysis>&#8635; Refresh</button>
      </div>
    </div>

    ${strategicHtml}

    <div class="briefing-grid">
      ${trendingHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x1F4C8; Trending Keywords</div>
        <div class="briefing-content">${trendingHtml}</div>
      </div>` : ""}

      ${sourcesHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x1F4F0; Most Active Sources</div>
        <div class="briefing-content">${sourcesHtml}</div>
      </div>` : ""}

      ${regionsHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x1F30D; Regional Threat Level</div>
        <div class="briefing-content">${regionsHtml}</div>
      </div>` : ""}

      ${narrativeHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x1F517; Narrative Connections</div>
        <div class="briefing-content">${narrativeHtml}</div>
      </div>` : ""}

      ${temporalHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x1F552; News Patterns</div>
        <div class="briefing-content">${temporalHtml}</div>
      </div>` : ""}

      ${threatHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x26A0; Threat-Associated Words</div>
        <div class="briefing-content">${threatHtml}</div>
      </div>` : ""}

      ${briefingExpanded && topKwHtml ? `<div class="briefing-section">
        <div class="briefing-label">&#x1F511; Top Keywords (7d)</div>
        <div class="briefing-content">${topKwHtml}</div>
      </div>` : ""}

      ${narrativeTrendsHtml}
      ${sourceDetailsHtml}
    </div>
  </div>`;
}

/* ── Render: Run-Up Cards ────────────────────────────────────── */

function renderRunUpSection() {
  if (state.runups.length === 0) {
    return `<div class="section-title">Game Theory Flows</div>
    <div class="empty">
      <div class="empty__icon">&#x1F3AF;</div>
      <div class="empty__text">No decision trees yet</div>
      <div class="empty__hint">Decision trees will appear when narratives are detected and analyzed.</div>
    </div>`;
  }

  let html = `<div class="section-title">Game Theory Flows</div><div class="flow-grid">`;

  for (const r of state.runups) {
    const isActive = r.active !== false;
    const prob = r.root_probability || r.probability || 0;
    const nodeCount = r.node_count || 0;
    const statusBadge = isActive
      ? `<span class="flow-status flow-status--live">LIVE</span>`
      : `<span class="flow-status flow-status--archived">ARCHIVED</span>`;

    // Clean narrative name: replace dashes with spaces, title case
    const rawName = r.narrative || r.name || "Unnamed";
    const cleanName = rawName.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());

    html += `<div class="flow-card ${isActive ? "flow-card--active" : ""}" data-runup-id="${esc(r.id || r.runup_id || "")}">
      <div class="flow-card__top">
        ${statusBadge}
        <span class="flow-card__nodes">${nodeCount} node${nodeCount !== 1 ? "s" : ""}</span>
      </div>
      <div class="flow-card__name">${esc(cleanName)}</div>
      <div class="flow-card__question">${esc(r.root_question || "")}</div>
      <div class="flow-card__footer">
        <span class="flow-card__prob">${pct(prob)}% likely</span>
        <span class="flow-card__articles">${r.article_count || 0} articles</span>
        <span class="flow-card__arrow">&rarr;</span>
      </div>
    </div>`;
  }

  html += `</div>`;
  return html;
}

/* ── Render: Decision Tree Detail (Cytoscape Canvas) ─────────── */

function truncateText(text, maxLen) {
  if (!text) return "";
  return text.length > maxLen ? text.slice(0, maxLen - 1) + "\u2026" : text;
}

function renderTreeView() {
  const tree = state.activeTree;
  const runup = state.runups.find(r => (r.id || r.runup_id) === activeTreeId);

  if (!tree) {
    return `<div class="tree-canvas-view">
      <div class="tree-canvas-header">
        <button class="back-btn" data-back>&#8592; Back</button>
        <h2>Loading\u2026</h2>
        <div class="tree-canvas-controls"></div>
      </div>
      <div class="loading">
        <div class="loading__spinner"></div>
        <div class="loading__text">Loading decision tree\u2026</div>
      </div>
    </div>`;
  }

  const root = tree.root || {};
  const rawName = runup ? (runup.narrative || runup.name) : (root.narrative || "Decision Tree");
  const cleanName = rawName.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());
  const narrativeName = esc(cleanName);
  const nodeCount = (tree.tree || []).length;

  return `<div class="tree-canvas-view">
    <div class="tree-canvas-header">
      <button class="back-btn" data-back>&#8592; All Flows</button>
      <div class="tree-canvas-title">
        <h2>${narrativeName}</h2>
        <span class="tree-canvas-subtitle">${nodeCount} decision node${nodeCount !== 1 ? "s" : ""}</span>
      </div>
      <div class="tree-canvas-controls">
        <button data-tree-fit title="Fit to view">&#8596; Fit</button>
        <button data-tree-zoom-in title="Zoom in">+</button>
        <button data-tree-zoom-out title="Zoom out">&minus;</button>
      </div>
    </div>
    <div id="cy-container" class="tree-canvas-container"></div>
    <div id="node-detail-panel" class="node-detail-panel"></div>
  </div>`;
}

/* ── Cytoscape: Tree Canvas Initialization ───────────────────── */

function initTreeCanvas() {
  // Destroy previous instance if any
  if (_cyInstance) {
    _cyInstance.destroy();
    _cyInstance = null;
  }

  const tree = state.activeTree;
  if (!tree) return;

  const container = document.getElementById("cy-container");
  if (!container) return;

  // Register cytoscape-dagre extension if available and not yet registered
  if (typeof cytoscape === "undefined") {
    console.warn("[news-analyzer] cytoscape not loaded");
    return;
  }
  if (typeof cytoscapeDagre !== "undefined") {
    try { cytoscape.use(cytoscapeDagre); } catch { /* already registered */ }
  }

  const elements = buildTreeElements(tree);

  _cyInstance = cytoscape({
    container: container,
    elements: elements,
    style: getCytoscapeStylesheet(),
    layout: {
      name: "dagre",
      rankDir: "LR",
      nodeSep: 60,
      rankSep: 180,
      padding: 40,
    },
    minZoom: 0.2,
    maxZoom: 3,
    wheelSensitivity: 0.3,
  });

  // Interactions
  bindCytoscapeEvents(_cyInstance);
}

/* ── Cytoscape: Build Elements from Tree Data ────────────────── */

function buildTreeElements(tree) {
  const elements = [];
  const nodes = tree.tree || [];
  const root = tree.root || {};

  // If we have raw tree nodes from the API, use them
  if (nodes.length > 0) {
    const nodeMap = {};
    for (const n of nodes) {
      nodeMap[n.id] = n;
    }

    for (const node of nodes) {
      const nodeId = `node-${node.id}`;
      const status = node.status || "open";
      const yesProb = Math.round((node.yes_probability || 0) * 100);
      const noProb = Math.round((node.no_probability || 0) * 100);
      const prefix = status === "confirmed_yes" ? "\u2713 " : status === "confirmed_no" ? "\u2717 " : "";

      // Decision node
      elements.push({
        data: {
          id: nodeId,
          label: prefix + truncateText(node.question, 60),
          type: "decision",
          status: status,
          yes_prob: yesProb,
          no_prob: noProb,
          fullQuestion: node.question || "",
          timeline: node.timeline_estimate || "",
          fullData: node,
        },
      });

      // Consequences
      const consequences = node.consequences || [];
      for (let ci = 0; ci < consequences.length; ci++) {
        const c = consequences[ci];
        const branch = c.branch || "yes";
        const consId = `cons-${node.id}-${ci}`;
        const effProb = Math.round((c.effective_probability || 0) * 100);

        elements.push({
          data: {
            id: consId,
            label: truncateText(c.description, 50),
            type: "consequence",
            branch: branch,
            probability: effProb,
            fullData: c,
          },
        });

        // Edge: decision -> consequence
        const edgeProb = branch === "yes" ? yesProb : noProb;
        elements.push({
          data: {
            id: `edge-${nodeId}-${consId}`,
            source: nodeId,
            target: consId,
            branch: branch,
            label: `${branch.toUpperCase()} ${edgeProb}%`,
          },
        });

        // Stock impact nodes (only bunq-available)
        const stocks = c.stock_impacts || [];
        for (const si of stocks) {
          if (si.available_on_bunq === false) continue;
          const stockId = `stock-${node.id}-${ci}-${si.ticker}`;
          // Avoid duplicate stock nodes
          if (elements.find(e => e.data.id === stockId)) continue;

          elements.push({
            data: {
              id: stockId,
              label: si.ticker || "",
              type: "stock",
              direction: si.direction || "bullish",
              magnitude: si.magnitude || "moderate",
              fullData: si,
            },
          });

          elements.push({
            data: {
              id: `edge-${consId}-${stockId}`,
              source: consId,
              target: stockId,
              type: "stock",
            },
          });
        }
      }

      // Cascade edges: parent -> child decision nodes
      const childIds = node.children_ids || [];
      for (const childId of childIds) {
        if (nodeMap[childId]) {
          elements.push({
            data: {
              id: `cascade-${node.id}-${childId}`,
              source: nodeId,
              target: `node-${childId}`,
              type: "cascade",
            },
          });
        }
      }
    }
  } else {
    // Fallback: build from the flat root/consequences structure
    const rootId = "node-root";
    const rootProb = root.probability || 0;
    const yesProb = pct(rootProb);
    const noProb = pct(100 - rootProb);

    elements.push({
      data: {
        id: rootId,
        label: truncateText(root.question || root.decision || "Root", 60),
        type: "decision",
        status: "open",
        yes_prob: yesProb,
        no_prob: noProb,
        fullQuestion: root.question || "",
        timeline: root.timeline || "",
        fullData: root,
      },
    });

    // YES consequences
    const yesCons = tree.consequences_yes || [];
    for (let i = 0; i < yesCons.length; i++) {
      const c = yesCons[i];
      const consId = `cons-yes-${i}`;
      elements.push({
        data: {
          id: consId,
          label: truncateText(c.description || c.text, 50),
          type: "consequence",
          branch: "yes",
          probability: pct(c.effective_probability || 0),
          fullData: c,
        },
      });
      elements.push({
        data: {
          id: `edge-root-${consId}`,
          source: rootId,
          target: consId,
          branch: "yes",
          label: `YES ${yesProb}%`,
        },
      });

      const stocks = c.stock_impacts || [];
      for (const si of stocks) {
        if (si.available_on_bunq === false) continue;
        const stockId = `stock-yes-${i}-${si.ticker}`;
        if (elements.find(e => e.data.id === stockId)) continue;
        elements.push({
          data: {
            id: stockId,
            label: si.ticker || "",
            type: "stock",
            direction: si.direction || "bullish",
            magnitude: si.magnitude || "moderate",
            fullData: si,
          },
        });
        elements.push({
          data: {
            id: `edge-${consId}-${stockId}`,
            source: consId,
            target: stockId,
            type: "stock",
          },
        });
      }
    }

    // NO consequences
    const noCons = tree.consequences_no || [];
    for (let i = 0; i < noCons.length; i++) {
      const c = noCons[i];
      const consId = `cons-no-${i}`;
      elements.push({
        data: {
          id: consId,
          label: truncateText(c.description || c.text, 50),
          type: "consequence",
          branch: "no",
          probability: pct(c.effective_probability || 0),
          fullData: c,
        },
      });
      elements.push({
        data: {
          id: `edge-root-${consId}`,
          source: rootId,
          target: consId,
          branch: "no",
          label: `NO ${noProb}%`,
        },
      });

      const stocks = c.stock_impacts || [];
      for (const si of stocks) {
        if (si.available_on_bunq === false) continue;
        const stockId = `stock-no-${i}-${si.ticker}`;
        if (elements.find(e => e.data.id === stockId)) continue;
        elements.push({
          data: {
            id: stockId,
            label: si.ticker || "",
            type: "stock",
            direction: si.direction || "bullish",
            magnitude: si.magnitude || "moderate",
            fullData: si,
          },
        });
        elements.push({
          data: {
            id: `edge-${consId}-${stockId}`,
            source: consId,
            target: stockId,
            type: "stock",
          },
        });
      }
    }
  }

  return elements;
}

/* ── Cytoscape: Stylesheet ───────────────────────────────────── */

function getCytoscapeStylesheet() {
  return [
    // ── Decision nodes (Miro-style clean cards) ──
    {
      selector: "node[type='decision']",
      style: {
        "shape": "round-rectangle",
        "width": 280,
        "height": 80,
        "background-color": "#1e2230",
        "border-color": "#5b8def",
        "border-width": 2,
        "border-opacity": 0.9,
        "label": "data(label)",
        "text-wrap": "wrap",
        "text-max-width": 250,
        "color": "#f0f0f4",
        "font-size": 12,
        "font-weight": "500",
        "text-valign": "center",
        "text-halign": "center",
        "padding": "12px",
        "shadow-blur": 12,
        "shadow-color": "rgba(0,0,0,0.3)",
        "shadow-offset-x": 0,
        "shadow-offset-y": 2,
        "shadow-opacity": 0.5,
      },
    },
    // Confirmed YES — green glow
    {
      selector: "node[type='decision'][status='confirmed_yes']",
      style: {
        "background-color": "#0d3d2e",
        "border-color": "#34d399",
        "border-width": 3,
        "color": "#34d399",
        "shadow-color": "rgba(52,211,153,0.3)",
      },
    },
    // Confirmed NO — red glow
    {
      selector: "node[type='decision'][status='confirmed_no']",
      style: {
        "background-color": "#3d1515",
        "border-color": "#f87171",
        "border-width": 3,
        "color": "#f87171",
        "shadow-color": "rgba(248,113,113,0.3)",
      },
    },
    // ── Consequence nodes ──
    {
      selector: "node[type='consequence']",
      style: {
        "shape": "round-rectangle",
        "width": 240,
        "height": 60,
        "background-color": "#1a1d26",
        "border-width": 1.5,
        "border-opacity": 0.7,
        "label": "data(label)",
        "text-wrap": "wrap",
        "text-max-width": 215,
        "color": "#c8c8d0",
        "font-size": 10.5,
        "text-valign": "center",
        "text-halign": "center",
        "padding": "8px",
        "shadow-blur": 8,
        "shadow-color": "rgba(0,0,0,0.2)",
        "shadow-offset-x": 0,
        "shadow-offset-y": 1,
        "shadow-opacity": 0.4,
      },
    },
    // YES consequence — green left border feel
    {
      selector: "node[type='consequence'][branch='yes']",
      style: {
        "border-color": "#34d399",
        "background-color": "#141e1a",
      },
    },
    // NO consequence — red left border feel
    {
      selector: "node[type='consequence'][branch='no']",
      style: {
        "border-color": "#f87171",
        "background-color": "#1e1414",
      },
    },
    // ── Stock pill nodes ──
    {
      selector: "node[type='stock']",
      style: {
        "shape": "round-rectangle",
        "width": 72,
        "height": 28,
        "label": "data(label)",
        "font-size": 10,
        "font-weight": "bold",
        "text-valign": "center",
        "text-halign": "center",
        "color": "#ffffff",
        "border-width": 0,
        "shadow-blur": 6,
        "shadow-color": "rgba(0,0,0,0.25)",
        "shadow-offset-y": 1,
        "shadow-opacity": 0.4,
      },
    },
    {
      selector: "node[type='stock'][direction='bullish']",
      style: { "background-color": "#059669" },
    },
    {
      selector: "node[type='stock'][direction='bearish']",
      style: { "background-color": "#dc2626" },
    },
    // ── Edges — smooth Miro-style curves ──
    {
      selector: "edge",
      style: {
        "width": 2,
        "curve-style": "bezier",
        "target-arrow-shape": "triangle",
        "arrow-scale": 0.7,
        "line-color": "#333640",
        "target-arrow-color": "#333640",
        "opacity": 0.85,
      },
    },
    // YES branch — green
    {
      selector: "edge[branch='yes']",
      style: {
        "line-color": "#34d399",
        "target-arrow-color": "#34d399",
        "width": 2.5,
        "label": "YES",
        "font-size": 9,
        "font-weight": "bold",
        "color": "#34d399",
        "text-background-color": "#12141a",
        "text-background-opacity": 0.9,
        "text-background-padding": "4px",
        "text-background-shape": "roundrectangle",
      },
    },
    // NO branch — red
    {
      selector: "edge[branch='no']",
      style: {
        "line-color": "#f87171",
        "target-arrow-color": "#f87171",
        "width": 2.5,
        "label": "NO",
        "font-size": 9,
        "font-weight": "bold",
        "color": "#f87171",
        "text-background-color": "#12141a",
        "text-background-opacity": 0.9,
        "text-background-padding": "4px",
        "text-background-shape": "roundrectangle",
      },
    },
    // Stock edges — thin dashed
    {
      selector: "edge[type='stock']",
      style: {
        "width": 1,
        "line-color": "#2a2e38",
        "target-arrow-color": "#2a2e38",
        "line-style": "dashed",
        "opacity": 0.6,
      },
    },
    // Cascade edges — amber dashed
    {
      selector: "edge[type='cascade']",
      style: {
        "width": 2.5,
        "line-style": "dashed",
        "line-color": "#f59e0b",
        "target-arrow-color": "#f59e0b",
        "label": "CASCADE",
        "font-size": 8,
        "color": "#f59e0b",
        "text-background-color": "#12141a",
        "text-background-opacity": 0.9,
        "text-background-padding": "3px",
      },
    },
    // ── Hover / active states ──
    {
      selector: "node:active",
      style: {
        "overlay-opacity": 0.08,
        "overlay-color": "#5b8def",
      },
    },
    // Highlight classes for hover
    {
      selector: ".edge-highlight",
      style: {
        "width": 3.5,
        "opacity": 1,
      },
    },
    {
      selector: ".node-highlight",
      style: {
        "border-width": 3,
        "shadow-blur": 20,
        "shadow-opacity": 0.7,
      },
    },
  ];
}

/* ── Cytoscape: Event Bindings ───────────────────────────────── */

function bindCytoscapeEvents(cy) {
  // Click on decision node -> show detail panel
  cy.on("tap", "node[type='decision']", function (evt) {
    const nodeData = evt.target.data();
    showNodeDetailPanel(nodeData);
  });

  // Click on stock node -> open price chart
  cy.on("tap", "node[type='stock']", function (evt) {
    const ticker = evt.target.data("label");
    if (ticker && typeof openPriceChart === "function") {
      openPriceChart(ticker);
    }
  });

  // Click on consequence node -> show consequence detail
  cy.on("tap", "node[type='consequence']", function (evt) {
    const nodeData = evt.target.data();
    showConsequenceDetailPanel(nodeData);
  });

  // Hover: highlight connected edges
  cy.on("mouseover", "node", function (evt) {
    const node = evt.target;
    node.connectedEdges().addClass("edge-highlight");
    node.connectedEdges().connectedNodes().addClass("node-highlight");
  });

  cy.on("mouseout", "node", function (evt) {
    const node = evt.target;
    node.connectedEdges().removeClass("edge-highlight");
    node.connectedEdges().connectedNodes().removeClass("node-highlight");
  });

  // Click on canvas background -> close detail panel
  cy.on("tap", function (evt) {
    if (evt.target === cy) {
      closeNodeDetailPanel();
    }
  });
}

/* ── Node Detail Panel ───────────────────────────────────────── */

function showNodeDetailPanel(nodeData) {
  const panel = document.getElementById("node-detail-panel");
  if (!panel) return;

  const fullData = nodeData.fullData || {};
  const question = nodeData.fullQuestion || fullData.question || "";
  const yesProb = nodeData.yes_prob || 0;
  const timeline = nodeData.timeline || fullData.timeline_estimate || "";
  const status = nodeData.status || "open";
  const evidence = fullData.evidence || fullData.confirmation_evidence || "";

  const statusLabel = status === "confirmed_yes" ? "Confirmed YES"
    : status === "confirmed_no" ? "Confirmed NO" : "Open";
  const statusCls = status === "confirmed_yes" ? "node-detail-status--yes"
    : status === "confirmed_no" ? "node-detail-status--no" : "node-detail-status--open";

  let html = `<div class="node-detail-panel__inner">
    <button class="node-detail-panel__close" data-panel-close>&times;</button>
    <div class="node-detail-panel__question">${esc(question)}</div>
    <div class="node-detail-panel__meta">
      <span class="node-detail-status ${statusCls}">${esc(statusLabel)}</span>
      <span class="ndp-meta-item">Likelihood: ${yesProb}%</span>
      ${timeline ? `<span class="ndp-meta-item">${esc(timeline)}</span>` : ""}
    </div>`;

  if (evidence) {
    html += `<div class="ndp-evidence">${esc(evidence)}</div>`;
  }

  // Consequences grouped by branch
  const consequences = fullData.consequences || [];
  const yesCons = consequences.filter(c => c.branch === "yes");
  const noCons = consequences.filter(c => c.branch === "no");

  for (const [branchLabel, branchCons, cls] of [["If YES", yesCons, "ndp-branch--yes"], ["If NO", noCons, "ndp-branch--no"]]) {
    if (!branchCons.length) continue;
    html += `<div class="ndp-branch ${cls}"><div class="ndp-branch__label">${branchLabel}</div>`;
    for (const c of branchCons) {
      html += `<div class="ndp-cons"><div class="ndp-cons__desc">${esc(c.description || "")}</div>`;
      if (c.proximity_display) {
        html += `<div class="ndp-proximity">${esc(c.proximity_display)}</div>`;
      }
      const stocks = c.stock_impacts || [];
      if (stocks.length) {
        html += `<div class="ndp-stocks">`;
        for (const si of stocks) {
          const arrow = si.direction === "bullish" ? "\u25B2" : "\u25BC";
          const dc = si.direction === "bullish" ? "stock-pill--bull" : "stock-pill--bear";
          html += `<span class="stock-pill ${dc}" data-chart-ticker="${esc(si.ticker)}" title="${esc(si.reasoning || "")}">${arrow} ${esc(si.ticker)}</span>`;
        }
        html += `</div>`;
      }
      html += `</div>`;
    }
    html += `</div>`;
  }

  // Polymarket
  const polyMatches = state.polymarket || [];
  for (const pm of polyMatches) {
    const pmProb = Math.round((pm.outcome_yes_price || 0) * 100);
    const vol = pm.volume ? ` · $${(pm.volume / 1000).toFixed(0)}K vol` : "";
    html += `<div class="ndp-poly">Polymarket: ${pmProb}%${vol}${pm.polymarket_url ? ` <a href="${esc(pm.polymarket_url)}" target="_blank" rel="noopener">View &rarr;</a>` : ""}</div>`;
  }

  html += `</div>`;
  panel.innerHTML = html;
  panel.classList.add("node-detail-panel--open");

  panel.querySelector("[data-panel-close]")?.addEventListener("click", closeNodeDetailPanel);
  panel.querySelectorAll("[data-chart-ticker]").forEach(el => {
    el.addEventListener("click", () => {
      const t = el.getAttribute("data-chart-ticker");
      if (t) openPriceChart(t);
    });
  });
}

function showConsequenceDetailPanel(nodeData) {
  const panel = document.getElementById("node-detail-panel");
  if (!panel) return;

  const fullData = nodeData.fullData || {};
  const branch = nodeData.branch || "yes";
  const brCls = branch === "yes" ? "ndp-branch--yes" : "ndp-branch--no";

  let html = `<div class="node-detail-panel__inner">
    <button class="node-detail-panel__close" data-panel-close>&times;</button>
    <div class="ndp-branch__label" style="margin-bottom:8px">${branch.toUpperCase()} outcome</div>
    <div class="node-detail-panel__question">${esc(fullData.description || "")}</div>`;

  if (fullData.proximity_display) {
    html += `<div class="ndp-proximity">${esc(fullData.proximity_display)}</div>`;
  }

  const stocks = fullData.stock_impacts || [];
  if (stocks.length) {
    html += `<div class="ndp-stocks" style="margin-top:12px">`;
    for (const si of stocks) {
      const arrow = si.direction === "bullish" ? "\u25B2" : "\u25BC";
      const dc = si.direction === "bullish" ? "stock-pill--bull" : "stock-pill--bear";
      html += `<span class="stock-pill ${dc}" data-chart-ticker="${esc(si.ticker)}" title="${esc(si.reasoning || "")}">${arrow} ${esc(si.ticker)}</span>`;
    }
    html += `</div>`;
  }

  html += `</div>`;
  panel.innerHTML = html;
  panel.classList.add("node-detail-panel--open");

  panel.querySelector("[data-panel-close]")?.addEventListener("click", closeNodeDetailPanel);
  panel.querySelectorAll("[data-chart-ticker]").forEach(el => {
    el.addEventListener("click", () => {
      const t = el.getAttribute("data-chart-ticker");
      if (t) openPriceChart(t);
    });
  });
}

function closeNodeDetailPanel() {
  const panel = document.getElementById("node-detail-panel");
  if (panel) {
    panel.classList.remove("node-detail-panel--open");
    panel.innerHTML = "";
  }
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
      html += `<div class="stock-badge ${dirCls}" title="${esc(si.reasoning)}">
        <span class="stock-badge__arrow">${arrow}</span>
        <span class="stock-badge__ticker" data-chart-ticker="${esc(si.ticker)}">${esc(si.ticker)}</span>
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
      // Destroy Cytoscape instance before navigating away
      if (_cyInstance) {
        _cyInstance.destroy();
        _cyInstance = null;
      }
      activeTreeId = null;
      state.activeTree = null;
      stopTreePolling();
      render();
    });
  }

  // Cytoscape canvas controls
  const fitBtn = document.querySelector("[data-tree-fit]");
  if (fitBtn) {
    fitBtn.addEventListener("click", () => {
      if (_cyInstance) _cyInstance.fit(undefined, 30);
    });
  }

  const zoomInBtn = document.querySelector("[data-tree-zoom-in]");
  if (zoomInBtn) {
    zoomInBtn.addEventListener("click", () => {
      if (_cyInstance) {
        _cyInstance.zoom({
          level: _cyInstance.zoom() * 1.2,
          renderedPosition: {
            x: _cyInstance.width() / 2,
            y: _cyInstance.height() / 2,
          },
        });
      }
    });
  }

  const zoomOutBtn = document.querySelector("[data-tree-zoom-out]");
  if (zoomOutBtn) {
    zoomOutBtn.addEventListener("click", () => {
      if (_cyInstance) {
        _cyInstance.zoom({
          level: _cyInstance.zoom() / 1.2,
          renderedPosition: {
            x: _cyInstance.width() / 2,
            y: _cyInstance.height() / 2,
          },
        });
      }
    });
  }
}

function bindBriefingEvents() {
  const runBtn = document.querySelector("[data-run-analysis]");
  if (runBtn) {
    runBtn.addEventListener("click", () => {
      runBtn.textContent = "Running...";
      runBtn.disabled = true;
      triggerAnalysis();
    });
  }
  const toggleBtn2 = document.querySelector("[data-briefing-toggle]");
  if (toggleBtn2) {
    toggleBtn2.addEventListener("click", () => {
      briefingExpanded = !briefingExpanded;
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
fetchAnalysis();
fetchSignals();
fetchIndicators();
// Refresh indicators every 5 minutes
setInterval(() => { fetchIndicators().then(render); }, 300000);
startPolling();
