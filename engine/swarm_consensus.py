"""Swarm Consensus Module — Expert panel debate for trading decisions.

Multi-provider architecture (€10/month budget, actual ~€0.50/month):
  - Groq (free): Energy, Technical, Supply Chain, Military, Regulatory (5 experts)
  - OpenRouter (paid): Geopolitical, Macro, Contrarian, Portfolio (4 experts)
  - OpenRouter (free): Risk, Sector Rotation (2 experts)
  - OpenRouter (paid, upgraded): Sentiment — Qwen3.5 397B MoE (1 expert)

12 expert agents × 2 debate rounds + 1 synthesis = 25 LLM calls per decision node.
"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

from .bunq_stocks import is_available_on_bunq
from .config import config
from .db import (
    get_session,
    Article,
    ArticleBrief,
    DecisionNode,
    Consequence,
    StockImpact,
    RunUp,
    NarrativeTimeline,
    PolymarketMatch,
    PolymarketPriceHistory,
    TradingSignal,
    ProbabilityUpdate,
    AnalysisReport,
    SwarmVerdict,
    TokenUsage,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provider configuration
# ---------------------------------------------------------------------------

GROQ_BASE_URL = "https://api.groq.com/openai/v1"

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Per-provider rate limiting
GROQ_RPM_LIMIT = 26       # Groq free: 30 req/min → margin
OPENROUTER_RPM_LIMIT = 18  # OpenRouter free: 20 req/min → margin

_groq_timestamps: List[float] = []
_openrouter_timestamps: List[float] = []
_groq_lock = asyncio.Lock()
_openrouter_lock = asyncio.Lock()

# Circuit breaker: track consecutive failures per provider
_provider_failures: Dict[str, int] = {"groq": 0, "openrouter": 0}
_provider_blocked_until: Dict[str, float] = {"groq": 0.0, "openrouter": 0.0}
CIRCUIT_BREAKER_THRESHOLD = 5      # failures before tripping
CIRCUIT_BREAKER_COOLDOWN = 120     # 2 minutes cooldown

MAX_TOKENS = 2048

# Groq models — token limit is PER MODEL, so using multiple models = N× budget!
GROQ_MODELS = {
    "llama-70b": "llama-3.3-70b-versatile",         # 70B — primary workhorse
    "qwen3-32b": "qwen/qwen3-32b",                  # 32B Qwen — different perspective
    "scout-17b": "meta-llama/llama-4-scout-17b-16e-instruct",  # 17B Scout (MoE)
}
GROQ_DEFAULT_MODEL = GROQ_MODELS["llama-70b"]

# OpenRouter models — mix of paid (consistent quality) and free (budget-friendly)
# Updated 2026-03-14 from /api/v1/models
# Total estimated cost: ~$0.50/month — well within €10 budget
OPENROUTER_MODELS = {
    # Paid models (reliable, no rate limits, consistent quality)
    "llama-70b-paid": "meta-llama/llama-3.3-70b-instruct",        # $0.22/mo — strong reasoning
    "gpt-oss-120b": "openai/gpt-oss-120b",                        # $0.17/mo — great analysis
    "qwen3-235b": "qwen/qwen3-235b-a22b-2507",                    # $0.11/mo — 235B MoE diverse
    "qwen35-397b": "qwen/qwen3.5-397b-a17b",                      # 397B MoE — sentiment powerhouse
    # Free fallback models
    "nemotron-120b": "nvidia/nemotron-3-super-120b-a12b:free",     # 120B Nemotron (free)
    "hermes-405b": "nousresearch/hermes-3-llama-3.1-405b:free",    # 405B Hermes (free)
}

# Models that DON'T support system messages → merge into user message
_NO_SYSTEM_MSG_MODELS = {"gemma", "gemini"}

MAX_NODES_PER_CYCLE = 20  # Don't overwhelm in one scheduler run
VERDICT_TTL_HOURS = 2     # Re-evaluate after this many hours
MAX_EVALS_PER_NODE_PER_DAY = 8  # Anti-loop: max evaluations per node per 24h

# ---------------------------------------------------------------------------
# Expert agent definitions
# ---------------------------------------------------------------------------

def get_active_experts() -> List[Dict[str, str]]:
    """Return only enabled experts."""
    return [e for e in EXPERTS if e.get("enabled", True)]


EXPERTS: List[Dict[str, str]] = [
    {
        "id": "geopolitical",
        "name": "Geopolitical Analyst",
        "emoji": "🌍",
        "enabled": True,
        "provider": "openrouter",  # PAID Llama 70B — reliable, no rate limits
        "model": "llama-70b-paid",
        "fallback_provider": "groq",        # Budget fallback: same model, free provider
        "fallback_model": "llama-70b",
        "system": (
            "You are a senior geopolitical analyst at a hedge fund. "
            "You specialize in power dynamics between nation-states, military "
            "alliances, sanctions regimes, and territorial disputes. You assess "
            "how political events translate to market-moving catalysts. "
            "Be specific about which countries, leaders, and institutions matter."
        ),
    },
    {
        "id": "energy",
        "name": "Energy & Commodities Trader",
        "emoji": "⛽",
        "enabled": True,
        "provider": "groq",      # Llama 3.3 70B
        "model": "llama-70b",
        "system": (
            "You are a senior energy and commodities trader. You specialize in "
            "oil (WTI/Brent), natural gas, gold, and shipping routes. You understand "
            "OPEC dynamics, strategic petroleum reserves, pipeline politics, and "
            "commodity supply chains. You think in terms of supply/demand imbalances "
            "and how geopolitical events create trading opportunities in energy markets."
        ),
    },
    {
        "id": "macro",
        "name": "Macro Economist",
        "emoji": "📊",
        "enabled": True,
        "provider": "openrouter",  # PAID GPT-OSS 120B — strong analytical
        "model": "gpt-oss-120b",
        "fallback_provider": "openrouter",   # Budget fallback: free Nemotron
        "fallback_model": "nemotron-120b",
        "system": (
            "You are a macro economist at a major investment bank. You focus on "
            "central bank policy (Fed, ECB, BOJ), inflation dynamics, GDP growth, "
            "trade flows, and currency movements. You understand how geopolitical "
            "shocks propagate through the global financial system. You think about "
            "second-order effects: how events affect interest rates, credit spreads, "
            "and capital flows."
        ),
    },
    {
        "id": "sentiment",
        "name": "Sentiment Analyst",
        "emoji": "📰",
        "enabled": True,
        "provider": "openrouter",  # Qwen3 235B MoE — was qwen35-397b (23x cheaper output)
        "model": "qwen3-235b",
        "fallback_provider": "openrouter",   # Budget fallback: free Hermes 405B
        "fallback_model": "hermes-405b",
        "system": (
            "You are a sentiment and behavioral finance analyst. You track news "
            "narrative intensity, social media momentum, retail investor positioning, "
            "and crowd psychology. You detect when markets are pricing in fear vs "
            "greed, and when sentiment extremes signal reversals. You understand "
            "how media coverage intensity translates to volume spikes and price moves."
        ),
    },
    {
        "id": "technical",
        "name": "Technical Analyst",
        "emoji": "📈",
        "enabled": True,
        "provider": "groq",      # Scout 17B MoE — yet another token budget!
        "model": "scout-17b",
        "system": (
            "You are a technical analyst and quantitative trader. You focus on "
            "price patterns, support/resistance levels, moving averages, RSI, MACD, "
            "and volume analysis. You identify optimal entry/exit points based on "
            "chart structure. When given a geopolitical scenario, you assess which "
            "technical levels would break and what that implies for trade timing."
        ),
    },
    {
        "id": "risk",
        "name": "Risk Manager",
        "emoji": "🛡️",
        "enabled": True,
        "provider": "openrouter",  # FREE Nemotron 120B — fallback to Groq
        "model": "nemotron-120b",
        "system": (
            "You are a portfolio risk manager at a family office. Your job is to "
            "protect capital. You think about tail risks, maximum drawdown, "
            "correlation breakdowns, liquidity risks, and black swan events. "
            "You always ask: what could go wrong? What is the worst case? "
            "How much capital is at risk? You recommend position sizing and stops."
        ),
    },
    {
        "id": "contrarian",
        "name": "Contrarian",
        "emoji": "🔄",
        "enabled": True,
        "provider": "openrouter",  # PAID Qwen3 235B MoE — diverse reasoning
        "model": "qwen3-235b",
        "fallback_provider": "groq",         # Budget fallback: free Qwen3 32B
        "fallback_model": "qwen3-32b",
        "system": (
            "You are a contrarian investor and devil's advocate. Your role is to "
            "challenge the consensus view. When others are bullish, find reasons for "
            "caution. When others are bearish, find reasons for optimism. You look for "
            "over-reaction, crowded trades, and scenarios the market hasn't considered. "
            "You are skeptical of groupthink and media-driven narratives."
        ),
    },
    {
        "id": "supplychain",
        "name": "Supply Chain & Second-Order Analyst",
        "emoji": "🔗",
        "enabled": True,
        "provider": "groq",      # Qwen3 32B — different reasoning than Llama (audit Fix 4)
        "model": "qwen3-32b",
        "system": (
            "You are a supply chain strategist and second-order effects analyst. "
            "You think like a detective: when Event A happens, most people see the "
            "obvious Effect B — but you find the non-obvious C, D, and E. "
            "Your specialty: "
            "1) SUPPLY CHAIN CASCADES: if shipping routes close, who benefits? "
            "If raw materials get scarce, which companies have stockpiles or alternatives? "
            "2) STRUCTURAL SHORTAGES: copper, lithium, rare earths, semiconductors — "
            "you track multi-year supply/demand imbalances that create mid-term holds. "
            "3) THE ZOOM EFFECT: during COVID, Zoom went 5x because remote work was a "
            "non-obvious second-order effect. You find these opportunities: what company "
            "or sector benefits from a crisis in ways nobody is talking about yet? "
            "4) MID-TERM HORIZON: you think 3-12 months out, not day-trading. "
            "You identify structural winners that the market hasn't priced in. "
            "Always name SPECIFIC tickers, not vague sectors. Prefer stocks available "
            "on European exchanges (UCITS ETFs, Xetra, Tradegate)."
        ),
    },
    {
        "id": "portfolio",
        "name": "Portfolio Risk Advisor",
        "emoji": "💼",
        "enabled": True,
        "provider": "openrouter",  # FREE Hermes 405B — deepest reasoning (audit Fix 4)
        "model": "hermes-405b",
        "system": (
            "You are a portfolio risk advisor for a European retail investor "
            "who trades on bunq Stocks (Tradegate/Xetra). You have access to "
            "the user's ACTUAL portfolio holdings and must give advice that is "
            "SPECIFIC to their positions. "
            "Your job: "
            "1) PORTFOLIO IMPACT: How does this geopolitical event affect the "
            "user's specific holdings? Not in theory — in practice, for THEIR "
            "positions. "
            "2) EXPOSURE RISK: Does the user have concentrated risk? E.g., if "
            "they hold an Oil & Gas ETF with heavy Middle East exposure AND "
            "individual oil stocks, that's double exposure to the same risk. "
            "3) REBALANCE SIGNALS: Should the user rotate? Switch an ETF for "
            "individual stocks that have better risk profiles? Reduce a position? "
            "4) POSITION SIZING: Is the user over-allocated to one sector? "
            "Should they take partial profits or add to a position? "
            "Always reference the user's actual tickers and allocation percentages. "
            "Be specific: 'Your IS0D.DE (31.7%) has Middle East exposure — consider "
            "reducing to 20% and adding XOM which produces in Americas.'"
        ),
    },
    # ── New V1 experts (2026-03-15) ─────────────────────────────
    {
        "id": "military",
        "name": "Military Strategy Analyst",
        "emoji": "🎖️",
        "enabled": True,
        "provider": "groq",      # Llama 70B — strong reasoning, free
        "model": "llama-70b",
        "system": (
            "You are a military strategy analyst at a defense-focused hedge fund. "
            "You specialize in: "
            "1) ESCALATION LADDERS: You understand how conflicts escalate from "
            "diplomatic tensions → sanctions → proxy conflicts → direct military "
            "confrontation. Each rung has different market implications. "
            "2) DEFENSE INDUSTRY: You track defense contractors (LMT, RTX, NOC, "
            "GD, LHX, BA, AIR.PA) and understand procurement cycles, contract awards, "
            "and how specific weapons programs translate to revenue. "
            "3) FORCE POSTURE AS LEADING INDICATOR: Military deployments, naval "
            "movements, air defense activations, and troop buildups are leading "
            "indicators before political announcements. You detect these signals. "
            "4) AMMUNITION & PRODUCTION: You track munition production bottlenecks, "
            "drone warfare adoption rates, and which companies benefit from "
            "military modernization programs (EU, NATO, AUKUS). "
            "5) ASYMMETRIC WARFARE: Cyber attacks, drone swarms, electronic "
            "warfare — how do these affect traditional defense stocks vs "
            "emerging defense tech companies? "
            "Always name specific tickers and explain the causal chain: "
            "event → military response → contract/procurement → stock impact."
        ),
    },
    {
        "id": "regulatory",
        "name": "Regulatory & Sanctions Analyst",
        "emoji": "⚖️",
        "enabled": True,
        "provider": "groq",      # Qwen3 32B — good for structured legal analysis
        "model": "qwen3-32b",
        "system": (
            "You are a regulatory and sanctions compliance analyst at a "
            "geopolitical risk consultancy. You specialize in: "
            "1) SANCTIONS REGIMES: EU sanctions packages, US OFAC SDN List, "
            "UK sanctions — you know which entities are targeted and the "
            "knock-on effects for publicly traded companies. "
            "2) EXPORT CONTROLS: CHIPS Act restrictions, Entity List additions, "
            "dual-use technology bans. You understand how export controls "
            "create winners (domestic alternatives) and losers (exposed companies). "
            "3) REGULATORY CATALYSTS: New EU regulations (CBAM, AI Act, DSA), "
            "antitrust actions, ESG mandates — these create sector rotation "
            "opportunities that markets are slow to price in. "
            "4) COMPLIANCE CASCADES: When one country sanctions an entity, "
            "allied nations often follow. You predict which companies will be "
            "affected next in the cascade and which alternatives benefit. "
            "5) TRADE POLICY: Tariffs, trade agreements, WTO disputes — "
            "you track how trade policy shifts create mid-term winners and losers. "
            "Always quantify the impact: which sectors, which tickers, "
            "and the timeline for regulatory impact (immediate vs 3-6 months)."
        ),
    },
    {
        "id": "sector",
        "name": "Sector Rotation Analyst",
        "emoji": "🏭",
        "enabled": True,
        "provider": "openrouter",  # FREE Nemotron 120B — strong reasoning
        "model": "nemotron-120b",
        "system": (
            "You are a sector rotation strategist at a quantitative asset manager. "
            "You specialize in: "
            "1) GICS SECTOR ROTATION: You track money flows between the 11 GICS "
            "sectors (Energy, Materials, Industrials, Consumer Discretionary, "
            "Consumer Staples, Health Care, Financials, IT, Communication Services, "
            "Utilities, Real Estate). You know which sectors lead in each phase "
            "of the economic cycle (early/mid/late/recession). "
            "2) RELATIVE STRENGTH: You compare EU-listed sector ETFs (IS0D.DE for energy, "
            "IQQH.DE for clean energy, WMIN.DE for mining) and broad indices "
            "(IWDA.AS, CSPX.AS, VWRL.AS). You identify sectors gaining momentum vs losing it. "
            "3) EARNINGS CYCLE: You track forward P/E ratios, earnings revision "
            "breadth, and guidance trends by sector to identify where growth "
            "is accelerating or decelerating. "
            "4) DEFENSIVE VS CYCLICAL: Given the geopolitical scenario, should "
            "investors rotate from cyclicals to defensives or vice versa? "
            "You provide specific rotation trades (e.g., 'Rotate from DIS to PG'). "
            "5) EUROPEAN FOCUS: You know European sector ETFs and individual "
            "stocks available on Xetra/Tradegate for the bunq investor. "
            "Always provide specific sector rotation recommendations with "
            "entry logic: 'Rotate INTO [sector] via [ticker] BECAUSE [catalyst]'."
        ),
    },
    # ── 13th expert: Emerging Opportunities (active by default) ──
    {
        "id": "emerging_opportunities",
        "name": "Emerging Opportunities Analyst",
        "emoji": "🔮",
        "enabled": True,
        "provider": "openrouter",
        "model": "qwen3-235b",
        "fallback_provider": "groq",
        "fallback_model": "qwen3-32b",
        "system": (
            "You are an emerging opportunities analyst who looks BEYOND the obvious. "
            "When everyone focuses on oil, you analyze nuclear energy, renewables, and shipping. "
            "When defense is hot, you examine cybersecurity, space tech, and rare earth miners. "
            "Your specialty: "
            "1) SECOND-ORDER BENEFICIARIES: If sector X is in the news, who benefits indirectly? "
            "E.g., if oil spikes → look at nuclear (CCJ, LEU), LNG shippers (FLNG), energy storage. "
            "2) ALTERNATIVE PLAYS: For every obvious trade, find the non-obvious alternative. "
            "3) CROSS-SECTOR CONNECTIONS: A Middle East crisis doesn't just affect oil — "
            "it affects shipping insurance, desalination, food imports, construction. "
            "4) CONTRARIAN OPPORTUNITIES: What's being ignored while everyone watches the headline? "
            "Always name SPECIFIC tickers and explain the non-obvious causal chain."
        ),
    },
    # ── Optional experts (disabled by default — enable via Settings) ──────
    {
        "id": "nuclear",
        "name": "Nuclear Energy Analyst",
        "emoji": "☢️",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are a nuclear energy sector analyst. You track uranium prices (UXC spot/term), enrichment capacity (SWU), reactor construction pipelines, SMR developers (NuScale, X-energy), and fuel cycle companies (CCJ, LEU, UEC, DNN). You understand NRC licensing, geopolitical uranium supply (Kazakhstan, Niger, Australia), and how energy policy shifts create long-term demand catalysts for nuclear.",
    },
    {
        "id": "crypto",
        "name": "Cryptocurrency Strategist",
        "emoji": "₿",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a cryptocurrency and digital assets strategist. You track BTC, ETH, and major altcoins, DeFi protocols, stablecoin flows, on-chain metrics (whale movements, exchange reserves), and regulatory developments (SEC, MiCA). You understand how geopolitical events drive crypto as a safe haven or risk asset, and how mining hash rate shifts signal market moves.",
    },
    {
        "id": "realestate",
        "name": "Real Estate & REITs Analyst",
        "emoji": "🏠",
        "enabled": False,
        "provider": "openrouter",
        "model": "nemotron-120b",
        "system": "You are a real estate and REIT analyst. You track commercial and residential real estate trends, REIT valuations (VNQ, VNQI), interest rate sensitivity, cap rate compression/expansion, and how geopolitical events affect property markets (migration flows, foreign investment, sanctions on oligarch properties). You focus on European REITs and property stocks available on Xetra.",
    },
    {
        "id": "pharma",
        "name": "Pharmaceutical & Biotech Analyst",
        "emoji": "💊",
        "enabled": False,
        "provider": "openrouter",
        "model": "gpt-oss-120b",
        "system": "You are a pharmaceutical and biotech analyst. You track drug pipelines, FDA/EMA approvals, clinical trial results, patent cliffs, and biosimilar competition. You understand how geopolitical events affect pharma supply chains (API sourcing from India/China), drug pricing policy, and pandemic preparedness spending. Name specific tickers and catalysts.",
    },
    {
        "id": "aitech",
        "name": "AI & Technology Analyst",
        "emoji": "🤖",
        "enabled": False,
        "provider": "openrouter",
        "model": "qwen3-235b",
        "system": "You are an AI and technology sector analyst. You track AI chip demand (NVDA, AMD, INTC), cloud infrastructure (AMZN, MSFT, GOOGL), AI model companies, and the broader tech stack. You understand export controls on AI chips, data center power requirements, and how geopolitical tensions affect tech supply chains and market access.",
    },
    {
        "id": "agriculture",
        "name": "Agricultural Commodities Trader",
        "emoji": "🌾",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are an agricultural commodities trader. You track wheat, corn, soybeans, fertilizers (potash, phosphate, nitrogen), and soft commodities. You understand how geopolitical events affect food supply chains — Black Sea grain corridors, fertilizer export bans, drought/flood impacts. You track companies like ADM, BG, MOS, NTR, CF, and agricultural ETFs.",
    },
    {
        "id": "water",
        "name": "Water & Natural Resources Analyst",
        "emoji": "💧",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a water and natural resources analyst. You track water utilities, desalination technology, water infrastructure companies (XYL, AWK, WTS, WTRG), and water scarcity trends. You understand how climate change, population growth, and geopolitical conflicts affect water access and the investment case for water infrastructure.",
    },
    {
        "id": "space",
        "name": "Space & Satellite Industry Analyst",
        "emoji": "🛰️",
        "enabled": False,
        "provider": "openrouter",
        "model": "hermes-405b",
        "system": "You are a space and satellite industry analyst. You track launch providers, satellite operators (IRDM, VSAT), space defense (LMT Space, LDOS), earth observation (MAXR), and commercial space companies. You understand how geopolitical tensions drive demand for satellite communications, surveillance, and GPS alternatives.",
    },
    {
        "id": "cybersecurity",
        "name": "Cybersecurity Threat Analyst",
        "emoji": "🔒",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are a cybersecurity threat and market analyst. You track cyber attacks, ransomware trends, state-sponsored hacking, and how these events drive demand for cybersecurity stocks (CRWD, PANW, FTNT, S, ZS). You understand how geopolitical escalation increases cyber threat levels and creates buying opportunities in security companies.",
    },
    {
        "id": "insurance",
        "name": "Insurance & Catastrophe Risk Analyst",
        "emoji": "🏛️",
        "enabled": False,
        "provider": "openrouter",
        "model": "nemotron-120b",
        "system": "You are an insurance and catastrophe risk analyst. You track reinsurance pricing, catastrophe bonds, war risk insurance premiums, and how geopolitical events affect insurance markets. You follow companies like BRK.B, MKL, RE, RNR, and understand how shipping insurance (P&I clubs), political risk insurance, and trade credit insurance react to geopolitical shocks.",
    },
    {
        "id": "luxury",
        "name": "Luxury & Consumer Goods Analyst",
        "emoji": "👜",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a luxury and consumer goods analyst. You track LVMH (MC.PA), Hermès (RMS.PA), Kering (KER.PA), Richemont (CFR.SW), and consumer sentiment. You understand how geopolitical events, sanctions (Russian oligarch spending), Chinese consumer confidence, and currency movements affect luxury demand and pricing power.",
    },
    {
        "id": "demographics",
        "name": "Demographics & Population Analyst",
        "emoji": "👥",
        "enabled": False,
        "provider": "openrouter",
        "model": "hermes-405b",
        "system": "You are a demographics and population trends analyst. You track aging populations, migration patterns, urbanization, and labor market shifts. You understand how demographic mega-trends create 5-10 year investment themes: healthcare demand, pension systems, housing, education, and consumer behavior shifts across regions.",
    },
    {
        "id": "climate",
        "name": "Climate & ESG Analyst",
        "emoji": "🌡️",
        "enabled": False,
        "provider": "openrouter",
        "model": "gpt-oss-120b",
        "system": "You are a climate and ESG analyst. You track carbon credit markets (EUA), clean energy investments, CBAM implementation, ESG fund flows, and stranded asset risk. You understand how climate policy (EU Green Deal, IRA) creates winners and losers, and how extreme weather events affect commodity prices, insurance costs, and infrastructure spending.",
    },
    {
        "id": "shipping",
        "name": "Shipping & Logistics Analyst",
        "emoji": "🚢",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are a shipping and logistics analyst. You track container rates (SCFI, FBX), bulk shipping (BDI), tanker rates, and chokepoint risks (Suez, Hormuz, Malacca, Panama). You follow companies like MAERSK.B, ZIM, STNG, INSW, and understand how geopolitical disruptions to shipping routes create price spikes and rerouting opportunities.",
    },
    {
        "id": "centralbank",
        "name": "Central Bank Policy Analyst",
        "emoji": "🏦",
        "enabled": False,
        "provider": "openrouter",
        "model": "qwen3-235b",
        "system": "You are a central bank policy analyst. You track Fed, ECB, BOJ, PBOC, and BOE policy decisions, forward guidance, dot plots, and balance sheet operations. You understand how geopolitical shocks force central banks to choose between inflation fighting and financial stability, and how these decisions affect rates, currencies, and equity markets.",
    },
    {
        "id": "emergingmarkets",
        "name": "Emerging Markets Specialist",
        "emoji": "🌏",
        "enabled": False,
        "provider": "openrouter",
        "model": "nemotron-120b",
        "system": "You are an emerging markets specialist. You track EM equities (EEM, VWO), EM bonds, frontier markets, and country-specific risks. You understand how geopolitical events differentially affect EMs — commodity exporters vs importers, dollarized economies, and countries caught in great power competition. You track EM ETFs available on European exchanges.",
    },
    {
        "id": "privateequity",
        "name": "Private Equity & VC Analyst",
        "emoji": "🦄",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a PE and VC market analyst tracking publicly listed alternatives managers (KKR, BX, APO, ARES, CG) and their portfolio exposure to geopolitical risk. You understand how credit conditions, deal flow, and exit markets are affected by geopolitical uncertainty, and how this creates opportunities in listed PE vehicles and BDCs.",
    },
    {
        "id": "credit",
        "name": "Debt & Credit Markets Analyst",
        "emoji": "📋",
        "enabled": False,
        "provider": "openrouter",
        "model": "gpt-oss-120b",
        "system": "You are a credit markets analyst. You track investment grade and high-yield spreads (LQD, HYG), CDS indices (iTraxx, CDX), sovereign debt (CDS spreads for at-risk countries), and distressed debt opportunities. You understand how geopolitical events trigger credit events, rating downgrades, and flight-to-quality flows between sovereign and corporate bonds.",
    },
    {
        "id": "forex",
        "name": "Foreign Exchange Strategist",
        "emoji": "💱",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are a foreign exchange strategist. You track major pairs (EUR/USD, USD/JPY, GBP/USD), EM currencies, and safe-haven flows (CHF, JPY, gold). You understand how geopolitical events trigger currency moves through trade balance effects, capital flows, interest rate differentials, and risk sentiment shifts. You recommend currency-hedged ETFs when appropriate.",
    },
    {
        "id": "politicalrisk",
        "name": "Political Risk Analyst",
        "emoji": "🗳️",
        "enabled": False,
        "provider": "openrouter",
        "model": "hermes-405b",
        "system": "You are a political risk analyst focused on elections, regime changes, and policy shifts. You track global elections calendars, polling data, policy platform analysis, and how political transitions affect market sectors. You understand populism, trade protectionism, and how political ideology shifts create sector rotation opportunities.",
    },
    {
        "id": "sanctions_detection",
        "name": "Sanctions Evasion Detector",
        "emoji": "🔍",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a sanctions evasion detection analyst. You track ship-to-ship transfers, dark fleet movements, front companies, and alternative payment systems used to circumvent sanctions. You understand how sanctions evasion patterns create risk for compliant companies and opportunities for enforcement technology providers.",
    },
    {
        "id": "media",
        "name": "Media & Propaganda Analyst",
        "emoji": "📺",
        "enabled": False,
        "provider": "openrouter",
        "model": "nemotron-120b",
        "system": "You are a media and propaganda analyst. You track state media narratives, information operations, social media amplification campaigns, and how coordinated messaging affects market sentiment. You understand the difference between genuine news events and manufactured narratives, and how to discount propaganda-driven market moves.",
    },
    {
        "id": "infrastructure",
        "name": "Infrastructure & Construction Analyst",
        "emoji": "🏗️",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are an infrastructure and construction analyst. You track government infrastructure spending (US IIJA, EU recovery plans), construction materials (steel, cement, aggregates), and engineering companies (VMC, MLM, AECOM, HOCHTIEF). You understand how geopolitical events drive reconstruction spending, military base construction, and border infrastructure investment.",
    },
    {
        "id": "automotive",
        "name": "Automotive & EV Industry Analyst",
        "emoji": "🚗",
        "enabled": False,
        "provider": "openrouter",
        "model": "qwen3-235b",
        "system": "You are an automotive and EV industry analyst. You track legacy automakers (BMW, VW, STLA), EV pure-plays (TSLA, RIVN, NIO), battery makers (CATL, LG), and charging infrastructure. You understand how geopolitical events affect auto supply chains (chips, rare earths, steel), EV adoption policies, and how energy prices drive the ICE-to-EV transition economics.",
    },
    {
        "id": "semiconductors",
        "name": "Semiconductor & Chip Supply Analyst",
        "emoji": "🔬",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a semiconductor supply chain analyst. You track TSMC, Samsung, Intel, ASML, and the entire chip supply chain. You understand export controls (Entity List, CHIPS Act), fab construction timelines, and how Taiwan Strait tensions create existential risk for the global chip supply. You track inventory cycles, book-to-bill ratios, and end-market demand shifts.",
    },
    {
        "id": "foodsecurity",
        "name": "Food Security & Famine Analyst",
        "emoji": "🍞",
        "enabled": False,
        "provider": "openrouter",
        "model": "hermes-405b",
        "system": "You are a food security analyst. You track global food price indices (FAO), grain trade flows, fertilizer availability, and regions at risk of food crises. You understand how geopolitical conflicts disrupt food supply chains, create refugee flows that strain neighboring economies, and how food insecurity leads to political instability that creates further market risk.",
    },
    {
        "id": "migration",
        "name": "Migration & Refugee Impact Analyst",
        "emoji": "🌐",
        "enabled": False,
        "provider": "groq",
        "model": "llama-70b",
        "system": "You are a migration and refugee impact analyst. You track displacement patterns, border policy changes, and how population movements affect labor markets, housing, healthcare, and social services budgets. You understand how migration creates investment opportunities in housing construction, healthcare expansion, and consumer staples in receiving countries.",
    },
    {
        "id": "telecom",
        "name": "Telecommunications Analyst",
        "emoji": "📡",
        "enabled": False,
        "provider": "openrouter",
        "model": "nemotron-120b",
        "system": "You are a telecommunications analyst. You track 5G deployment, fiber infrastructure, submarine cable routes, and telecom operators (DTE.DE, ORAN, VOD). You understand how geopolitical events affect internet connectivity (cable cuts, sanctions on tech), spectrum auctions, and how conflicts drive demand for satellite communications (Starlink competitors).",
    },
    {
        "id": "mining",
        "name": "Mining & Rare Earth Analyst",
        "emoji": "⛏️",
        "enabled": False,
        "provider": "groq",
        "model": "qwen3-32b",
        "system": "You are a mining and rare earth elements analyst. You track critical minerals (lithium, cobalt, nickel, rare earths), mining jurisdictions, and supply chain concentration risks (China's dominance in REE processing). You follow companies like ALB, LAC, MP, LTHM, and understand how export controls, resource nationalism, and EV demand create structural supply deficits.",
    },
    {
        "id": "tourism",
        "name": "Tourism & Travel Industry Analyst",
        "emoji": "✈️",
        "enabled": False,
        "provider": "openrouter",
        "model": "gpt-oss-120b",
        "system": "You are a tourism and travel industry analyst. You track airlines (LHA.DE, AF.PA, IAG.L), hotel chains (MAR, HLT), online travel (BKNG, ABNB), and cruise lines (RCL, CCL). You understand how geopolitical events affect travel demand, airspace closures, visa restrictions, and travel insurance costs. You identify which travel companies benefit from route disruptions.",
    },
]

# ---------------------------------------------------------------------------
# Round prompts
# ---------------------------------------------------------------------------

# --- Round 1 prompt split for prefix caching ---
# The data context (question, consequences, market data, etc.) goes into the
# first system message.  The expert-specific analysis instruction goes into the
# user message.  Because _trim_context_for_expert blanks out irrelevant fields
# per expert, the shared context is not fully identical across experts — but
# experts that share the same provider/model and similar allowed fields will
# still benefit from partial prefix caching.

ROUND1_SHARED_CONTEXT = """\
DECISION NODE: {question}
Timeline: {timeline}
Current probability (YES): {yes_prob}%

CONSEQUENCES IF YES:
{yes_consequences}

CONSEQUENCES IF NO:
{no_consequences}

AFFECTED STOCKS:
{stock_impacts}

MARKET CONTEXT:
{market_context}

{news_intelligence}

{narrative_momentum}

{polymarket_context}

{trading_signal}

{bayesian_trail}

{strategic_briefing}

{price_momentum}

{military_indicators}

{news_price_correlation}

{forward_outlook}

{portfolio_context}

{swarm_memory}"""

ROUND1_EXPERT_INSTRUCTION = """\
As the {expert_name}, analyze this decision node from your expertise.
Use ALL the data above — news sentiment, narrative momentum, prediction markets, \
probability trail, price momentum, military posture, news-price correlation, and forward outlook.
If portfolio holdings are shown above, consider how this event impacts the user's specific positions.
Cross-reference the news-price correlation data to validate whether narrative shifts actually move these tickers.
If SWARM MEMORY is provided above, use it as baseline context:
- Consider whether new developments confirm or challenge the previous verdict.
- Note probability trajectory trends — is the situation escalating or de-escalating?
- Flag any surprises or unexpected shifts since last evaluation.
- Do NOT blindly anchor to the previous probability — re-evaluate from first principles with new evidence.
Provide your INDEPENDENT assessment in this JSON format:
{{
  "assessment": "Your 2-3 sentence analysis from your domain expertise",
  "yes_probability_estimate": <0-100>,
  "trading_action": "STRONG_BUY | BUY | HOLD | SELL | STRONG_SELL",
  "top_ticker": "TICKER_SYMBOL" or null,
  "ticker_direction": "long | short" or null,
  "confidence": <0-100>,
  "key_risk": "One sentence on the biggest risk",
  "forward_catalyst": "What event in the next 60 days could change everything"
}}
Respond with JSON only."""

# --- Round 2 prompt split for prefix caching ---
# The shared context (assessments, enrichment, disagreement) is IDENTICAL across
# all experts and goes into the first system message.  The expert-specific
# instruction is short and goes into the user message.  This lets the LLM
# provider cache the large shared prefix and bill only incremental tokens for
# each additional expert call (~60% fewer billed input tokens).

ROUND2_SHARED_CONTEXT = """\
ROUND 2 DEBATE CONTEXT

Here are the Round 1 assessments from ALL experts on this decision:

DECISION: {question}

{all_round1_assessments}

SHARED INTELLIGENCE SUMMARY:
{enrichment_summary}

EXPERT DISAGREEMENT MAP:
{disagreement_map}"""

ROUND2_EXPERT_INSTRUCTION = """\
As the {expert_name}, you now see what your colleagues think.
The SHARED INTELLIGENCE SUMMARY contains the key data all experts had access to.
Use it to validate or challenge the other experts' data interpretation.
CHALLENGE or REFINE your position. Where do you DISAGREE? What data point did others misinterpret?
Did anyone ignore the military posture, forward outlook, or news-price correlation data?
IMPORTANT: If your revised probability moves more than 5 points toward the group median compared to your Round 1 estimate, you MUST cite a specific data point or expert argument that justifies the shift. "The other experts convinced me" is NOT valid — only new evidence or a factual correction you overlooked counts.

Update your assessment in this JSON format:
{{
  "revised_assessment": "2-3 sentences — what changed and why",
  "yes_probability_estimate": <0-100>,
  "trading_action": "STRONG_BUY | BUY | HOLD | SELL | STRONG_SELL",
  "top_ticker": "TICKER_SYMBOL" or null,
  "ticker_direction": "long | short" or null,
  "confidence": <0-100>,
  "agrees_with_majority": true/false,
  "dissent_reason": "If you disagree, why?" or null
}}
Respond with JSON only."""

ROUND3_SYNTHESIS = """\
You are a portfolio manager writing the narrative summary for a trading decision.
The quantitative verdict is already computed deterministically — you CANNOT change it.
Your job is ONLY to write human-readable narrative fields explaining the rationale.

DECISION NODE: {question}
Timeline: {timeline}

COMPUTED VERDICT (deterministic — do NOT override):
Direction: {verdict_direction}
Confidence: {verdict_confidence}%
YES probability: {verdict_probability}%
Primary ticker: {verdict_ticker} ({verdict_ticker_direction})

ROUND 2 EXPERT ASSESSMENTS (after debate):
{all_round2_assessments}

---

Respond with this JSON (narrative fields ONLY):
{{
  "entry_reasoning": "2-3 sentences: why enter this trade NOW or why wait — reference the expert assessments",
  "exit_trigger": "What event or price level triggers the exit — be specific with numbers",
  "risk_note": "Key risk the experts flagged — include the concrete invalidation trigger",
  "dissent_note": "Strongest contrarian objection from the panel — name which expert disagreed and why"
}}
Respond with JSON only."""


# ---------------------------------------------------------------------------
# Multi-provider clients
# ---------------------------------------------------------------------------

_groq_client: Optional[OpenAI] = None
_openrouter_client: Optional[OpenAI] = None


def _get_groq_client() -> Optional[OpenAI]:
    """Lazy-init the Groq client using OpenAI SDK."""
    global _groq_client
    if _groq_client is not None:
        return _groq_client

    api_key = config.groq_api_key
    if not api_key:
        logger.warning("Groq API key not configured.")
        return None

    _groq_client = OpenAI(api_key=api_key, base_url=GROQ_BASE_URL, timeout=120.0)
    logger.info("Groq client initialized (models=%s)", list(GROQ_MODELS.keys()))
    return _groq_client


def _get_openrouter_client() -> Optional[OpenAI]:
    """Lazy-init the OpenRouter client using OpenAI SDK."""
    global _openrouter_client
    if _openrouter_client is not None:
        return _openrouter_client

    api_key = config.openrouter_api_key
    if not api_key:
        logger.info("OpenRouter API key not configured — using Groq only.")
        return None

    _openrouter_client = OpenAI(api_key=api_key, base_url=OPENROUTER_BASE_URL, timeout=120.0)
    logger.info("OpenRouter client initialized (free models: %s)", list(OPENROUTER_MODELS.keys()))
    return _openrouter_client


def _get_client_and_model(expert: Dict) -> Tuple[Optional[OpenAI], str]:
    """Return the appropriate client and model for an expert.

    Budget-aware: downgrades paid experts to free fallbacks when budget is tight.
    Falls back to Groq if OpenRouter is not configured.
    """
    provider = expert.get("provider", "groq")
    model_key = expert.get("model", "llama-70b")
    expert_id = expert.get("id", "?")

    # --- Budget gate for paid OpenRouter experts ---
    _FREE_OR_MODELS = {"nemotron-120b", "hermes-405b"}
    is_free = (provider == "groq" or model_key in _FREE_OR_MODELS)

    if not is_free and provider == "openrouter":
        try:
            from .tree_generator import get_budget_tier, BudgetTier, can_spend
            tier = get_budget_tier()

            if tier == BudgetTier.BLOCKED:
                logger.warning("BLOCKED: skipping paid expert %s entirely.", expert_id)
                return None, ""

            if tier in (BudgetTier.ECONOMY, BudgetTier.EMERGENCY):
                # Downgrade to free fallback model
                fb_provider = expert.get("fallback_provider")
                fb_model = expert.get("fallback_model")
                if fb_provider and fb_model:
                    logger.info(
                        "%s tier: downgrading %s → %s/%s (free)",
                        tier.value.upper(), expert_id, fb_provider, fb_model,
                    )
                    return _get_client_and_model({"provider": fb_provider, "model": fb_model})
                logger.info("%s tier: no fallback for %s, skipping.", tier.value.upper(), expert_id)
                return None, ""

            # PREMIUM/STANDARD — pre-flight cost check for paid models
            actual_model = OPENROUTER_MODELS.get(model_key, "")
            pricing = _MODEL_PRICING.get(actual_model)
            if pricing:
                est_cost_usd = 1500 * pricing[0] + 800 * pricing[1]
                est_cost_eur = est_cost_usd * _USD_TO_EUR
                if not can_spend(est_cost_eur):
                    logger.warning(
                        "Pre-flight: %s (€%.4f) would exceed ceiling — using fallback.",
                        expert_id, est_cost_eur,
                    )
                    fb_provider = expert.get("fallback_provider")
                    fb_model = expert.get("fallback_model")
                    if fb_provider and fb_model:
                        return _get_client_and_model({"provider": fb_provider, "model": fb_model})
                    return None, ""
        except ImportError:
            pass  # tree_generator not available — proceed without budget check

    # --- Standard routing (unchanged) ---
    if provider == "openrouter":
        client = _get_openrouter_client()
        if client is not None:
            model_name = OPENROUTER_MODELS.get(model_key, OPENROUTER_MODELS.get("hermes-405b"))
            return client, model_name
        # Fallback to Groq
        logger.debug("OpenRouter unavailable for %s — falling back to Groq", expert_id)

    # Resolve Groq model from mapping
    client = _get_groq_client()
    if client is None:
        return None, ""
    groq_model = GROQ_MODELS.get(model_key, GROQ_DEFAULT_MODEL)
    return client, groq_model


async def _rate_limited_call(
    client: OpenAI,
    model: str,
    messages: List[Dict[str, str]],
    purpose: str = "swarm",
    _is_fallback: bool = False,
) -> Optional[Dict[str, Any]]:
    """Call LLM with per-provider rate limiting.

    Supports both Groq and OpenRouter.
    Returns parsed JSON dict or None on failure.
    """
    global _groq_timestamps, _openrouter_timestamps

    # Determine which provider this is
    is_openrouter = "openrouter" in str(client.base_url)
    timestamps = _openrouter_timestamps if is_openrouter else _groq_timestamps
    rpm_limit = OPENROUTER_RPM_LIMIT if is_openrouter else GROQ_RPM_LIMIT
    provider_name = "OpenRouter" if is_openrouter else "Groq"
    lock = _openrouter_lock if is_openrouter else _groq_lock
    provider_key = "openrouter" if is_openrouter else "groq"

    # --- Circuit breaker check ---
    if time.time() < _provider_blocked_until.get(provider_key, 0):
        remaining = _provider_blocked_until[provider_key] - time.time()
        logger.warning("%s circuit breaker OPEN (%.0fs remaining) — skipping call", provider_name, remaining)
        return None

    # --- Rate limiter (with async lock for thread safety) ---
    async with lock:
        now = time.time()
        timestamps[:] = [t for t in timestamps if now - t < 60]
        if len(timestamps) >= rpm_limit:
            wait_time = 60 - (now - timestamps[0]) + 0.5
            logger.debug("%s rate limit — waiting %.1fs", provider_name, wait_time)
            await asyncio.sleep(wait_time)
            # Re-prune after sleep to account for elapsed time
            now = time.time()
            timestamps[:] = [t for t in timestamps if now - t < 60]

        timestamps.append(time.time())

    # --- API call (sync in thread to not block event loop) ---
    try:
        # Merge system message into user message for models that don't support it
        final_messages = list(messages)
        if any(kw in model.lower() for kw in _NO_SYSTEM_MSG_MODELS):
            system_msgs = [m for m in final_messages if m["role"] == "system"]
            other_msgs = [m for m in final_messages if m["role"] != "system"]
            if system_msgs and other_msgs:
                # Prepend system content to first user message
                system_text = "\n".join(m["content"] for m in system_msgs)
                other_msgs[0] = {
                    "role": "user",
                    "content": f"[INSTRUCTIONS]\n{system_text}\n\n[TASK]\n{other_msgs[0]['content']}",
                }
                final_messages = other_msgs

        kwargs = dict(
            model=model,
            messages=final_messages,
            max_tokens=MAX_TOKENS,
            temperature=0.3,
        )
        # Only add response_format for models that reliably support it
        # Most OpenRouter free models and thinking models don't support json_object
        model_lower = model.lower()
        supports_json_mode = (
            "llama-3.3-70b" in model_lower
            and "openrouter" not in str(client.base_url).lower()
        )
        if supports_json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        # For Qwen3: add /no_think suffix to disable thinking mode
        if "qwen3" in model_lower:
            # Deep copy messages to avoid mutating originals
            import copy
            kwargs["messages"] = copy.deepcopy(kwargs["messages"])
            for msg in reversed(kwargs["messages"]):
                if msg["role"] == "user":
                    msg["content"] += "\n/no_think"
                    break

        response = await asyncio.to_thread(
            client.chat.completions.create,
            **kwargs,
        )

        raw_content = response.choices[0].message.content
        if raw_content is None:
            logger.warning("%s returned None content for %s.", provider_name, purpose)
            return None
        content = raw_content.strip()

        # Log usage
        usage = response.usage
        if usage:
            _log_usage(
                model, usage.prompt_tokens, usage.completion_tokens,
                purpose, provider_name,
            )

        # Parse JSON (handle markdown wrapping and thinking tags)
        parsed = _parse_json_response(content, provider_name, model)
        # Circuit breaker: reset on success
        _provider_failures[provider_key] = 0
        # Sanitize numeric fields to prevent LLM hallucination
        return _sanitize_expert_response(parsed) if parsed else None

    except Exception as e:
        error_str = str(e)
        logger.error("%s API call failed (model=%s): %s", provider_name, model, e)

        # Circuit breaker: track consecutive failures
        _provider_failures[provider_key] = _provider_failures.get(provider_key, 0) + 1
        if _provider_failures[provider_key] >= CIRCUIT_BREAKER_THRESHOLD:
            _provider_blocked_until[provider_key] = time.time() + CIRCUIT_BREAKER_COOLDOWN
            logger.warning(
                "%s circuit breaker TRIPPED after %d consecutive failures — blocking for %ds",
                provider_name, _provider_failures[provider_key], CIRCUIT_BREAKER_COOLDOWN,
            )

        # Don't recurse if we're already in a fallback attempt
        if _is_fallback:
            return None

        # If OpenRouter fails, try Groq as fallback
        if is_openrouter:
            groq = _get_groq_client()
            if groq:
                # Rotate through Groq models until one works
                for fb_model in GROQ_MODELS.values():
                    logger.info("Falling back to Groq/%s ...", fb_model)
                    result = await _rate_limited_call(
                        groq, fb_model, messages, purpose, _is_fallback=True,
                    )
                    if result is not None:
                        return result
        # If Groq fails with rate limit, try other Groq models
        elif "429" in error_str:
            groq = _get_groq_client()
            if groq:
                for alt_model in GROQ_MODELS.values():
                    if alt_model == model:
                        continue
                    logger.info("Trying alternate Groq model: %s", alt_model)
                    result = await _rate_limited_call(
                        groq, alt_model, messages, purpose, _is_fallback=True,
                    )
                    if result is not None:
                        return result
        return None


def _parse_json_response(content: str, provider: str, model: str) -> Optional[Dict]:
    """Parse JSON from various model response formats."""
    # Strip thinking tags (DeepSeek R1, Qwen3, etc.)
    if "<think>" in content:
        think_end = content.rfind("</think>")
        if think_end >= 0:
            content = content[think_end + 8:].strip()
        else:
            # No closing tag — find the first { after <think>
            first_brace = content.find("{")
            if first_brace >= 0:
                content = content[first_brace:]

    # Direct JSON parse
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code blocks
    if "```" in content:
        # Find JSON block
        for marker in ["```json", "```"]:
            start = content.find(marker)
            if start >= 0:
                start = content.find("\n", start) + 1
                end = content.find("```", start)
                if end > start:
                    try:
                        return json.loads(content[start:end].strip())
                    except json.JSONDecodeError:
                        pass

    # Last resort: find first { ... }
    start = content.find("{")
    end = content.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(content[start:end])
        except json.JSONDecodeError:
            pass

    logger.warning("Failed to parse %s/%s response as JSON: %s...", provider, model, content[:200])
    return None


def _sanitize_expert_response(response: Dict) -> Dict:
    """Clamp all numeric fields to valid ranges to prevent LLM hallucination.

    LLMs occasionally return probability=500 or confidence=-10 — this ensures
    the deterministic aggregation receives clean data.
    """
    if response is None:
        return response

    # Clamp yes_probability_estimate to [0, 100]
    if "yes_probability_estimate" in response:
        try:
            val = float(response["yes_probability_estimate"])
            response["yes_probability_estimate"] = max(0, min(100, val))
        except (ValueError, TypeError):
            response["yes_probability_estimate"] = 50

    # Clamp confidence to [0, 100]; detect 0-1 scale confusion
    if "confidence" in response:
        try:
            val = float(response["confidence"])
            if 0 < val < 1.0:
                val = val * 100  # LLM likely used 0-1 scale instead of 0-100
            response["confidence"] = max(0, min(100, val))
        except (ValueError, TypeError):
            response["confidence"] = 50

    # Validate trading_action
    if "trading_action" in response:
        valid_actions = {"STRONG_BUY", "BUY", "HOLD", "SELL", "STRONG_SELL"}
        if response["trading_action"] not in valid_actions:
            response["trading_action"] = "HOLD"

    # Validate primary_ticker (must be string, no spaces, not "null"/"none")
    if "primary_ticker" in response:
        t = str(response["primary_ticker"]).strip().upper()
        if " " in t or len(t) > 12 or t in ("NULL", "NONE", "N/A", "NA", ""):
            response["primary_ticker"] = ""
    if "top_ticker" in response:
        t = str(response["top_ticker"]).strip().upper()
        if " " in t or len(t) > 12 or t in ("NULL", "NONE", "N/A", "NA", ""):
            response["top_ticker"] = ""

    # Validate ticker_direction (must be "long" or "short")
    if "ticker_direction" in response:
        d = str(response["ticker_direction"]).strip().lower()
        if d in ("long", "bullish", "buy", "strong_buy"):
            response["ticker_direction"] = "long"
        elif d in ("short", "bearish", "sell", "strong_sell"):
            response["ticker_direction"] = "short"
        else:
            response["ticker_direction"] = "long"

    return response


# Per-token pricing (USD) for paid models — used for cost tracking & budget gating
# qwen3.5-397b removed: was €1.53/day (23x more expensive output than qwen3-235b)
_MODEL_PRICING = {
    "meta-llama/llama-3.3-70b-instruct": (1e-7, 3.2e-7),
    "openai/gpt-oss-120b": (3.9e-8, 1.9e-7),
    "qwen/qwen3-235b-a22b-2507": (7.1e-8, 1e-7),
}
_USD_TO_EUR = 0.92


def _log_usage(
    model: str, input_tokens: int, output_tokens: int,
    purpose: str, provider: str,
) -> None:
    """Log token usage and cost for analytics."""
    try:
        # Calculate cost for paid models
        pricing = _MODEL_PRICING.get(model)
        if pricing:
            cost_usd = input_tokens * pricing[0] + output_tokens * pricing[1]
            cost_eur = cost_usd * _USD_TO_EUR
        else:
            cost_eur = 0.0  # Free tier

        db = get_session()
        try:
            record = TokenUsage(
                model=f"{provider}/{model}",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_eur=cost_eur,
                purpose=f"swarm_{purpose}",
            )
            db.add(record)
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.debug("Failed to log token usage: %s", e)


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------

def _safe_ctx(fn, *args) -> str:
    """Call a context-enrichment function; return fallback on any error."""
    try:
        result = fn(*args)
        return result if result else ""
    except Exception as e:
        logger.debug("Context enrichment %s failed: %s", fn.__name__, e)
        return ""


def _get_news_intelligence(node: DecisionNode, db) -> str:
    """Aggregate NLP signals from recent ArticleBriefs for this narrative."""
    run_up = db.query(RunUp).get(node.run_up_id)
    if not run_up:
        return ""

    cutoff = datetime.utcnow() - timedelta(hours=72)

    # Parse narrative name for region and topic keywords
    # e.g. "middle-east-iran-conflict" → region="middle-east", keywords=["iran","conflict"]
    parts = run_up.narrative_name.split("-")
    # Common region prefixes (2 segments)
    two_seg_regions = {
        "middle-east", "north-america", "south-asia", "east-asia",
        "southeast-asia", "russia-cis",
    }
    if len(parts) >= 2 and f"{parts[0]}-{parts[1]}" in two_seg_regions:
        region = f"{parts[0]}-{parts[1]}"
        topic_kws = parts[2:]
    else:
        region = parts[0] if parts else ""
        topic_kws = parts[1:]

    # Primary: match by region + recency
    from sqlalchemy import or_, func as sa_func
    briefs = (
        db.query(ArticleBrief)
        .filter(
            ArticleBrief.region == region,
            ArticleBrief.processed_at >= cutoff,
        )
        .order_by(ArticleBrief.urgency_score.desc())
        .limit(100)
        .all()
    )

    # If topic keywords exist, filter further by keyword overlap in summary/entities
    if topic_kws and len(briefs) > 20:
        scored = []
        for b in briefs:
            kws_str = (b.keywords_json or "").lower()
            ent_str = (b.entities_json or "").lower()
            hits = sum(1 for kw in topic_kws if kw in kws_str or kw in ent_str)
            scored.append((hits, b))
        scored.sort(key=lambda x: -x[0])
        # Keep top-scoring briefs (at least half must have a keyword hit)
        with_hits = [b for hits, b in scored if hits > 0]
        if len(with_hits) >= 5:
            briefs = with_hits[:80]

    if not briefs:
        return ""

    # Aggregate statistics
    sentiments = [b.sentiment for b in briefs if b.sentiment is not None]
    avg_sent = sum(sentiments) / len(sentiments) if sentiments else 0
    urgencies = [b.urgency_score for b in briefs if b.urgency_score]
    avg_urg = sum(urgencies) / len(urgencies) if urgencies else 0

    # Intensity distribution
    intensities: Dict[str, int] = {}
    for b in briefs:
        i = b.intensity or "unknown"
        intensities[i] = intensities.get(i, 0) + 1
    int_parts = [f"{cnt} {lvl}" for lvl, cnt in sorted(intensities.items(), key=lambda x: -x[1])]

    # Event type distribution
    events: Dict[str, int] = {}
    for b in briefs:
        if b.event_type:
            events[b.event_type] = events.get(b.event_type, 0) + 1
    evt_parts = [f"{et}({cnt})" for et, cnt in sorted(events.items(), key=lambda x: -x[1])]

    # Top keywords across briefs
    all_kw: Dict[str, int] = {}
    for b in briefs:
        kws = b.keywords if hasattr(b, 'keywords') and callable(getattr(b.__class__.keywords, 'fget', None)) else None
        if kws is None:
            try:
                kws = json.loads(b.keywords_json) if b.keywords_json else []
            except Exception:
                kws = []
        if isinstance(kws, list):
            for kw in kws[:5]:
                k = str(kw).lower().strip()
                if k:
                    all_kw[k] = all_kw.get(k, 0) + 1
    top_kw = sorted(all_kw.items(), key=lambda x: -x[1])[:7]
    kw_parts = [f"{kw}({cnt})" for kw, cnt in top_kw]

    # Source credibility
    creds = [b.source_credibility for b in briefs if b.source_credibility]
    avg_cred = sum(creds) / len(creds) if creds else 0

    lines = [f"NEWS INTELLIGENCE (72h, {len(briefs)} articles):"]
    lines.append(f"Sentiment: {avg_sent:+.2f} avg | {', '.join(int_parts[:4])}")
    if evt_parts:
        lines.append(f"Event types: {', '.join(evt_parts[:5])}")
    if kw_parts:
        lines.append(f"Top keywords: {', '.join(kw_parts)}")

    # Entity aggregation (currently entities_json is never surfaced)
    all_entities = {}
    for b in briefs:
        try:
            ents = json.loads(getattr(b, 'entities_json', '{}') or '{}')
            for category in ("persons", "organizations", "locations"):
                for ent in ents.get(category, []):
                    if ent and len(ent) > 1:
                        all_entities[ent] = all_entities.get(ent, 0) + 1
        except Exception:
            pass
    if all_entities:
        top_ents = sorted(all_entities.items(), key=lambda x: -x[1])[:8]
        lines.append(f"Key actors: {', '.join(f'{e}({c})' for e, c in top_ents)}")

    lines.append(f"Source credibility: {avg_cred:.2f} avg | Urgency: {avg_urg:.2f} avg")

    # Top headlines with summaries (currently summaries are never surfaced to experts)
    top_briefs = sorted(briefs, key=lambda b: getattr(b, 'urgency_score', 0) or 0, reverse=True)[:5]
    headline_lines = []
    for b in top_briefs:
        summary = (getattr(b, 'summary', '') or '')[:120]
        if summary:
            src = ''
            try:
                if b.article:
                    src = f"[{b.article.source}] "
            except Exception:
                pass
            sent = getattr(b, 'sentiment', 0) or 0
            headline_lines.append(f"  • {src}{summary} (sent: {sent:+.2f})")
    if headline_lines:
        lines.append("\nTOP HEADLINES:")
        lines.extend(headline_lines[:5])

    return "\n".join(lines)


def _get_narrative_momentum(node: DecisionNode, db) -> str:
    """7-day narrative trajectory from NarrativeTimeline + RunUp momentum."""
    run_up = db.query(RunUp).get(node.run_up_id)
    if not run_up:
        return ""

    timelines = (
        db.query(NarrativeTimeline)
        .filter(NarrativeTimeline.narrative_name == run_up.narrative_name)
        .order_by(NarrativeTimeline.date.desc())
        .limit(7)
        .all()
    )
    timelines.reverse()  # chronological order

    if not timelines:
        # Minimal info from RunUp alone
        return (
            f"NARRATIVE MOMENTUM:\n"
            f'"{run_up.narrative_name}" — score: {run_up.current_score:.0f}/100, '
            f"acceleration: {run_up.acceleration_rate:+.1f}, "
            f"{run_up.article_count_total} total articles"
        )

    counts = [str(t.article_count) for t in timelines]
    sentiments = [t.avg_sentiment for t in timelines if t.avg_sentiment is not None]
    sent_first = sentiments[0] if sentiments else 0
    sent_last = sentiments[-1] if sentiments else 0
    sent_shift = "worsening" if sent_last < sent_first - 0.05 else "improving" if sent_last > sent_first + 0.05 else "stable"

    sources_first = timelines[0].sources_count if timelines else 0
    sources_last = timelines[-1].sources_count if timelines else 0

    trend = timelines[-1].trend if timelines else "unknown"

    lines = [f"NARRATIVE MOMENTUM:"]
    lines.append(
        f'"{run_up.narrative_name}" — score: {run_up.current_score:.0f}/100, '
        f"acceleration: {run_up.acceleration_rate:+.1f}"
    )
    lines.append(f"7d articles: {' → '.join(counts)} ({trend})")
    lines.append(f"Sentiment shift: {sent_first:+.2f} → {sent_last:+.2f} ({sent_shift})")
    if sources_first or sources_last:
        lines.append(f"Sources: {sources_first} → {sources_last} unique")

    return "\n".join(lines)


def _get_polymarket_context(node: DecisionNode, db) -> str:
    """Polymarket prediction market data + 24h price drift."""
    matches = (
        db.query(PolymarketMatch)
        .filter(
            (PolymarketMatch.decision_node_id == node.id)
            | (PolymarketMatch.run_up_id == node.run_up_id)
        )
        .order_by(PolymarketMatch.match_score.desc())
        .limit(3)
        .all()
    )

    if not matches:
        return ""

    lines = ["PREDICTION MARKETS:"]
    for pm in matches:
        # Get 24h drift
        cutoff_24h = datetime.utcnow() - timedelta(hours=24)
        history = (
            db.query(PolymarketPriceHistory)
            .filter(
                PolymarketPriceHistory.polymarket_id == pm.polymarket_id,
                PolymarketPriceHistory.recorded_at >= cutoff_24h,
            )
            .order_by(PolymarketPriceHistory.recorded_at.asc())
            .all()
        )
        drift_str = ""
        if history and len(history) >= 2:
            drift = pm.outcome_yes_price - history[0].yes_price
            drift_str = f" ({drift:+.2f} 24h drift)"

        # Our model vs market divergence
        our_prob = node.yes_probability
        market_prob = pm.outcome_yes_price
        divergence = (our_prob - market_prob) * 100

        vol_str = f"${pm.volume / 1e6:.1f}M" if pm.volume and pm.volume > 1e6 else f"${pm.volume:,.0f}" if pm.volume else "?"
        lines.append(
            f'"{pm.polymarket_question[:80]}" — YES: ${pm.outcome_yes_price:.2f}{drift_str}'
        )
        lines.append(
            f"  Volume: {vol_str} | Match: {pm.match_score:.0f}% | "
            f"Our model: {our_prob:.0%} vs market: {market_prob:.0%} (edge: {divergence:+.0f}pp)"
        )
        if pm.calibrated_probability:
            lines.append(f"  Calibrated probability: {pm.calibrated_probability:.0%}")

    return "\n".join(lines)


def _get_trading_signal_context(node: DecisionNode, db) -> str:
    """Latest composite trading signal for this run-up."""
    signal = (
        db.query(TradingSignal)
        .filter(
            TradingSignal.run_up_id == node.run_up_id,
            TradingSignal.superseded_by_id.is_(None),
        )
        .order_by(TradingSignal.created_at.desc())
        .first()
    )

    if not signal:
        return ""

    # Check if expired
    if signal.expires_at and signal.expires_at < datetime.utcnow():
        return ""

    lines = [
        f"COMPOSITE TRADING SIGNAL: {signal.signal_level} "
        f"(confidence: {signal.confidence:.0%})"
    ]
    lines.append(
        f"Components: runup={signal.runup_score_component:.2f}, "
        f"x_signal={signal.x_signal_component:.2f}, "
        f"polymarket={signal.polymarket_drift_component:.2f}, "
        f"news_accel={signal.news_acceleration_component:.2f}, "
        f"source_conv={signal.source_convergence_component:.2f}"
    )
    meta = []
    if signal.x_signal_count:
        meta.append(f"OSINT tweets: {signal.x_signal_count}")
    if signal.news_count:
        meta.append(f"News articles: {signal.news_count}")
    if signal.ticker:
        direction = signal.direction or "?"
        meta.append(f"Primary: {signal.ticker} ({direction})")
    if meta:
        lines.append(" | ".join(meta))

    return "\n".join(lines)


def _get_bayesian_trail(node: DecisionNode, db) -> str:
    """Probability evolution from Bayesian updates."""
    updates = (
        db.query(ProbabilityUpdate)
        .filter(
            ProbabilityUpdate.target_type == "node",
            ProbabilityUpdate.target_id == node.id,
        )
        .order_by(ProbabilityUpdate.updated_at.desc())
        .limit(5)
        .all()
    )

    if not updates:
        return ""

    updates.reverse()  # chronological

    trail = " → ".join(f"{u.posterior:.0%}" for u in updates)
    latest = updates[-1]
    evidence_str = ""
    if latest.evidence_summary:
        evidence_str = f"\nLatest evidence ({latest.evidence_count} articles): {latest.evidence_summary[:120]}"

    return f"PROBABILITY TRAIL (last {len(updates)} updates): {trail}{evidence_str}"


def _get_strategic_briefing(node: DecisionNode, db) -> str:
    """Extract relevant section from latest deep analysis report.

    NOTE: Must filter on report_type to avoid reading a daily_advisory
    (or other report type) instead of the intended strategic briefing.
    """
    report = (
        db.query(AnalysisReport)
        .filter(AnalysisReport.report_type.in_(["daily_briefing", "deep_analysis"]))
        .order_by(AnalysisReport.created_at.desc())
        .first()
    )

    if not report or not report.report_json:
        return ""

    try:
        data = json.loads(report.report_json)
    except (json.JSONDecodeError, TypeError):
        return ""

    run_up = db.query(RunUp).get(node.run_up_id)
    narrative = run_up.narrative_name if run_up else ""

    lines = ["STRATEGIC BRIEFING (latest analysis):"]

    # Find matching region
    regions = data.get("regions", {})
    if isinstance(regions, dict):
        regions = regions.get("regions", [])
    if isinstance(regions, list):
        # Match region from narrative name (e.g. "middle-east-iran" → "middle-east")
        narrative_parts = narrative.split("-")
        for r in regions:
            rname = r.get("region", "")
            if any(part in rname for part in narrative_parts[:2]):
                threat = r.get("threat_level", r.get("avg_sentiment", "?"))
                max_int = r.get("max_intensity", "?")
                art_count = r.get("article_count", "?")
                lines.append(f'Region "{rname}": threat={threat}, intensity={max_int}, articles={art_count}')
                break

    # Find matching narrative from report
    narratives = data.get("narratives", {})
    if isinstance(narratives, dict):
        narratives = narratives.get("narratives", [])
    if isinstance(narratives, list):
        for n in narratives:
            nname = n.get("narrative", "")
            if nname == narrative or any(part in nname for part in narrative.split("-")[:2]):
                trend = n.get("trend", "?")
                sent = n.get("avg_sentiment", "?")
                total = n.get("total_articles", "?")
                lines.append(f'Narrative "{nname}": {total} articles, trend={trend}, sentiment={sent}')
                break

    # Trending keywords overlap
    vocab = data.get("vocabulary", {})
    top_kw = vocab.get("top_keywords", []) if isinstance(vocab, dict) else []
    if isinstance(top_kw, list) and narrative:
        n_parts = set(narrative.replace("-", " ").lower().split())
        matching = [kw for kw in top_kw if isinstance(kw, dict) and kw.get("keyword", "").lower() in n_parts]
        if matching:
            kw_str = ", ".join(f'"{k["keyword"]}"({k.get("count","?")}x)' for k in matching[:5])
            lines.append(f"Trending keywords: {kw_str}")

    # Strategic outlook
    outlook = data.get("strategic_outlook", {})
    if isinstance(outlook, dict):
        picks = outlook.get("top_picks", [])
        rumour = outlook.get("rumour_phase", [])
        if picks:
            lines.append(f"Top picks: {', '.join(str(p) for p in picks[:3])}")
        if rumour:
            lines.append(f"Rumour phase: {', '.join(str(r) for r in rumour[:3])}")

    if len(lines) <= 1:
        return ""

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase 2 enrichment helpers
# ---------------------------------------------------------------------------

def _get_price_momentum(node: DecisionNode, db) -> str:
    """2-week price momentum for the node's affected stocks (max 5).

    Shows: 2wk change %, SMA5/10 crossover signal, volume trend.
    """
    from .price_fetcher import get_price_fetcher

    # Collect tickers from StockImpacts
    tickers: List[str] = []
    consequences = (
        db.query(Consequence)
        .filter(Consequence.decision_node_id == node.id)
        .all()
    )
    seen = set()
    for c in consequences:
        for si in db.query(StockImpact).filter(StockImpact.consequence_id == c.id).all():
            t = si.ticker.upper()
            if t not in seen and "=" not in t and "^" not in t:
                tickers.append(t)
                seen.add(t)

    if not tickers:
        return ""

    pf = get_price_fetcher()
    momentum = pf.get_ticker_momentum(tickers[:5])
    if not momentum:
        return ""

    parts = []
    for ticker, data in momentum.items():
        parts.append(
            f"{ticker}: {data['change_2wk_pct']:+.1f}% "
            f"SMA-{data['sma_signal']} vol-{data['volume_trend']}"
        )

    return f"PRICE MOMENTUM (2wk):\n{' | '.join(parts)}"


MILITARY_KEYWORDS = {
    "carrier", "strike group", "b-2", "b-52", "f-22", "f-35",
    "deployment", "naval", "centcom", "eucom", "indopacom",
    "fleet", "bomber", "stealth", "squadron", "mobilization",
    "aircraft carrier", "submarine", "destroyer", "amphibious",
    "military exercise", "carrier strike", "air force",
    "troops", "warship", "missile defense", "patriot", "thaad",
}


def _get_military_indicators(node: DecisionNode, db) -> str:
    """Detect elevated military activity from article metadata (last 7 days).

    Cross-references with NarrativeTimeline for trend vs prior 7 days.
    Returns empty string if <3 military articles.
    """
    import json as _json
    from collections import Counter

    cutoff_7d = datetime.utcnow() - timedelta(days=7)
    cutoff_14d = datetime.utcnow() - timedelta(days=14)

    # Query military articles (last 7 days)
    briefs_7d = (
        db.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= cutoff_7d)
        .all()
    )

    # Filter for military content
    mil_briefs = []
    for b in briefs_7d:
        if b.event_type and "military" in b.event_type.lower():
            mil_briefs.append(b)
            continue
        try:
            kws = _json.loads(b.keywords_json or "[]")
            kw_text = " ".join(k.lower() for k in kws)
            if any(mk in kw_text for mk in MILITARY_KEYWORDS):
                mil_briefs.append(b)
        except Exception:
            pass

    if len(mil_briefs) < 3:
        return ""

    # Count by intensity
    intensity_counts = Counter(b.intensity or "unknown" for b in mil_briefs)
    critical = intensity_counts.get("critical", 0)

    # Top keywords from military articles
    kw_counter: Counter = Counter()
    for b in mil_briefs:
        try:
            kws = _json.loads(b.keywords_json or "[]")
            for k in kws:
                kl = k.lower()
                if any(mk in kl for mk in MILITARY_KEYWORDS) or len(kl) > 3:
                    kw_counter[kl] += 1
        except Exception:
            pass

    top_kws = kw_counter.most_common(5)
    kw_str = ", ".join(f"{k}({v})" for k, v in top_kws)

    # Region distribution
    region_counter = Counter(b.region or "global" for b in mil_briefs)
    region_str = ", ".join(f"{r}({c})" for r, c in region_counter.most_common(3))

    # Compare with prior 7 days for trend
    briefs_prev = (
        db.query(ArticleBrief)
        .filter(
            ArticleBrief.processed_at >= cutoff_14d,
            ArticleBrief.processed_at < cutoff_7d,
        )
        .all()
    )
    prev_mil = 0
    for b in briefs_prev:
        if b.event_type and "military" in b.event_type.lower():
            prev_mil += 1
            continue
        try:
            kws = _json.loads(b.keywords_json or "[]")
            kw_text = " ".join(k.lower() for k in kws)
            if any(mk in kw_text for mk in MILITARY_KEYWORDS):
                prev_mil += 1
        except Exception:
            pass

    trend = "rising" if len(mil_briefs) > prev_mil * 1.3 else (
        "falling" if len(mil_briefs) < prev_mil * 0.7 else "stable"
    )

    lines = [
        f"MILITARY POSTURE (7d):",
        f"{len(mil_briefs)} military articles, {critical} critical-intensity "
        f"(was {prev_mil} prev 7d → {trend})",
        f"Keywords: {kw_str}",
        f"Regions: {region_str}",
    ]
    return "\n".join(lines)


def _get_news_price_correlation(node: DecisionNode, db) -> str:
    """Detect article spikes and align with price movements within 24 hours.

    Needs PriceSnapshot data (populated every 4h). Degrades gracefully
    if insufficient data (<14 days).
    """
    from .db import PriceSnapshot
    import json as _json

    # Get run-up for this node
    run_up = None
    if node.run_up_id:
        run_up = db.query(RunUp).get(node.run_up_id)
    if not run_up:
        return ""

    # Get narrative timeline for last 14 days
    cutoff_14d = datetime.utcnow() - timedelta(days=14)
    timeline = (
        db.query(NarrativeTimeline)
        .filter(
            NarrativeTimeline.narrative_name == run_up.narrative_name,
            NarrativeTimeline.date >= cutoff_14d.date(),
        )
        .order_by(NarrativeTimeline.date)
        .all()
    )

    if len(timeline) < 7:
        return ""

    # Find article count spikes (>2x 7-day average)
    counts = [t.article_count for t in timeline]
    spikes = []
    for i in range(7, len(timeline)):
        avg_7d = sum(counts[max(0, i-7):i]) / min(7, i)
        if avg_7d > 0 and counts[i] > avg_7d * 2:
            pct_above = round(((counts[i] - avg_7d) / avg_7d) * 100, 0)
            spikes.append((timeline[i].date, int(pct_above)))

    if not spikes:
        return ""

    # Collect tickers from StockImpacts
    tickers: List[str] = []
    consequences = (
        db.query(Consequence)
        .filter(Consequence.decision_node_id == node.id)
        .all()
    )
    for c in consequences:
        for si in db.query(StockImpact).filter(StockImpact.consequence_id == c.id).all():
            t = si.ticker.upper()
            if t not in tickers and "=" not in t and "^" not in t:
                tickers.append(t)
    tickers = tickers[:5]

    if not tickers:
        return ""

    # For each spike, look up PriceSnapshot changes
    lines = ["NEWS→PRICE CORRELATION (14d):"]
    hits = 0

    for spike_date, pct_above in spikes[:3]:  # max 3 spikes
        spike_dt = datetime(spike_date.year, spike_date.month, spike_date.day)
        spike_end = spike_dt + timedelta(hours=28)  # 24h + margin

        price_parts = []
        significant = False
        for ticker in tickers[:3]:
            # Find closest snapshot before/at spike and ~24h after
            before = (
                db.query(PriceSnapshot)
                .filter(
                    PriceSnapshot.ticker == ticker,
                    PriceSnapshot.recorded_at >= spike_dt - timedelta(hours=4),
                    PriceSnapshot.recorded_at <= spike_dt + timedelta(hours=4),
                )
                .order_by(PriceSnapshot.recorded_at)
                .first()
            )
            after = (
                db.query(PriceSnapshot)
                .filter(
                    PriceSnapshot.ticker == ticker,
                    PriceSnapshot.recorded_at >= spike_dt + timedelta(hours=20),
                    PriceSnapshot.recorded_at <= spike_end,
                )
                .order_by(PriceSnapshot.recorded_at)
                .first()
            )

            if before and after and before.price > 0:
                change = round(((after.price - before.price) / before.price) * 100, 1)
                price_parts.append(f"{ticker} {change:+.1f}%")
                if abs(change) > 2:
                    significant = True

        date_str = spike_date.strftime("%b %d")
        if price_parts:
            lines.append(
                f"{date_str}: article spike +{pct_above:.0f}% → "
                + ", ".join(price_parts) + " (24h)"
            )
        else:
            lines.append(f"{date_str}: article spike +{pct_above:.0f}% → no price data")

        if significant:
            hits += 1

    total = len(spikes[:3])
    lines.append(f"Hit rate: {hits} of {total} spikes → >2% ticker movement")

    return "\n".join(lines)


def _get_forward_outlook(node: DecisionNode, db) -> str:
    """2-month forward-looking outlook: Polymarket long-bets, narrative trajectory,
    price threshold proximity.
    """
    lines = ["FORWARD OUTLOOK (60d):"]
    has_data = False

    # 1. Polymarket long-dated bets
    try:
        run_up = None
        if node.run_up_id:
            run_up = db.query(RunUp).get(node.run_up_id)

        if run_up:
            matches = (
                db.query(PolymarketMatch)
                .filter(PolymarketMatch.run_up_id == run_up.id)
                .order_by(PolymarketMatch.outcome_yes_price.desc())
                .limit(5)
                .all()
            )
            if matches:
                avg_yes = sum(m.outcome_yes_price for m in matches) / len(matches)
                top = matches[0]
                q_short = (top.polymarket_question or "")[:60]
                lines.append(
                    f"Polymarket: {len(matches)} active bets, avg YES: "
                    f"{avg_yes*100:.0f}%, top: \"{q_short}\" at {top.outcome_yes_price*100:.0f}%"
                )
                has_data = True
    except Exception:
        pass

    # 2. Narrative trajectory projection
    try:
        run_up = None
        if node.run_up_id:
            run_up = db.query(RunUp).get(node.run_up_id)

        if run_up and run_up.current_score and run_up.acceleration_rate:
            projected = min(100, run_up.current_score + run_up.acceleration_rate * 30)
            direction = "accelerating" if run_up.acceleration_rate > 0 else "decelerating"
            lines.append(
                f"Narrative: {direction} {run_up.acceleration_rate:+.1f}/cycle, "
                f"score: {run_up.current_score:.0f}/100 → projected {projected:.0f} in 30d"
            )
            has_data = True
    except Exception:
        pass

    # 3. Price threshold proximity
    try:
        import json as _json

        consequences = (
            db.query(Consequence)
            .filter(Consequence.decision_node_id == node.id)
            .filter(Consequence.proximity_pct.isnot(None))
            .filter(Consequence.proximity_pct > 0)
            .all()
        )

        if consequences:
            close_ones = [c for c in consequences if c.proximity_pct and c.proximity_pct >= 85]
            total = len(consequences)
            if close_ones:
                parts = []
                for c in close_ones[:3]:
                    try:
                        th = _json.loads(c.price_thresholds_json or "[]")
                        if th:
                            asset = th[0].get("asset", "?")
                            target = th[0].get("value", "?")
                            parts.append(f"{asset} ${target} at {c.proximity_pct:.0f}%")
                    except Exception:
                        pass
                if parts:
                    lines.append(
                        f"Thresholds: {len(close_ones)} of {total} within 15% "
                        f"({', '.join(parts)})"
                    )
                    has_data = True
    except Exception:
        pass

    if not has_data:
        return ""

    return "\n".join(lines)


def _get_portfolio_context(node: DecisionNode, db) -> str:
    """Get the user's portfolio holdings for portfolio-aware swarm experts."""
    try:
        from .db import EngineSettings
        import json as _json

        s = db.query(EngineSettings).get("portfolio_holdings")
        if not s or not s.value:
            return ""

        holdings = _json.loads(s.value)
        if not holdings:
            return ""

        # Compute live values for each holding
        from .price_fetcher import get_price_fetcher
        _pf = get_price_fetcher()
        for h in holdings:
            shares = float(h.get("shares", 0))
            if shares > 0:
                q = _pf.get_quote(h["ticker"])
                if "error" not in q:
                    price_eur = _pf.convert_to_eur(q["price"], q.get("currency", "EUR"))
                    h["value_eur"] = round(shares * price_eur, 2)
                else:
                    h["value_eur"] = round(shares * float(h.get("avg_buy_price_eur", 0)), 2)
            elif not h.get("value_eur"):
                h["value_eur"] = 0

        lines = ["USER PORTFOLIO (bunq Stocks):"]
        total = sum(h.get("value_eur", 0) for h in holdings)
        for h in holdings:
            val = h.get("value_eur", 0)
            pct = round(val / total * 100, 1) if total > 0 else 0
            lines.append(f"  {h['ticker']} — {h.get('name', '?')} — €{val:,.0f} ({pct}%)")
        lines.append(f"  Total: €{total:,.0f}")

        # Add known exposure risks
        exposure_notes = {
            "IS0D.DE": "⚠️ Heavy Middle East/OPEC exposure in underlying holdings",
            "WMIN.DE": "Broad mining exposure incl. copper, iron, gold",
            "IS0E.DE": "Gold producers — hedge against uncertainty",
            "ISPA.DE": "Global dividend — defensive income position",
        }
        notes = []
        for h in holdings:
            note = exposure_notes.get(h["ticker"])
            if note:
                notes.append(f"  {h['ticker']}: {note}")
        if notes:
            lines.append("EXPOSURE NOTES:")
            lines.extend(notes)

        return "\n".join(lines)
    except Exception:
        return ""


def _build_swarm_memory(node: DecisionNode, db) -> str:
    """Build the 'Swarm Memory' briefing from previous evaluations.

    Returns a compact text block that gives experts:
    1. Previous verdict summary (probability, confidence, action, reasoning)
    2. Verdict trajectory (how probability evolved over recent evaluations)
    3. New developments since last evaluation (article count + highlights)
    4. Surprise factor (what diverged from expectations)

    Cost: 1 DB query for verdicts + 1 for new articles.  ~200 tokens in prompt.
    """
    try:
        # Get recent verdicts for this node (up to 5, newest first)
        recent_verdicts = (
            db.query(SwarmVerdict)
            .filter(SwarmVerdict.decision_node_id == node.id)
            .order_by(SwarmVerdict.created_at.desc())
            .limit(5)
            .all()
        )

        if not recent_verdicts:
            return ""  # First evaluation — no memory

        latest = recent_verdicts[0]
        hours_ago = (datetime.utcnow() - latest.created_at).total_seconds() / 3600

        lines = ["SWARM MEMORY — PREVIOUS EVALUATION BRIEFING:"]
        lines.append(f"Last evaluated: {hours_ago:.1f}h ago ({latest.created_at.strftime('%Y-%m-%d %H:%M')} UTC)")

        # 1. Previous verdict summary
        lines.append(f"Previous verdict: {latest.verdict} (confidence: {latest.confidence:.0%})")
        lines.append(f"Previous YES probability: {latest.yes_probability:.0%}")
        if latest.entry_reasoning:
            # Truncate to ~150 chars for token efficiency
            reasoning = latest.entry_reasoning[:200]
            if len(latest.entry_reasoning) > 200:
                reasoning += "..."
            lines.append(f"Previous reasoning: {reasoning}")
        if latest.risk_note:
            lines.append(f"Key risk identified: {latest.risk_note[:120]}")
        if latest.dissent_note:
            lines.append(f"Dissent: {latest.dissent_note[:120]}")

        # 2. Verdict trajectory (if multiple evaluations)
        if len(recent_verdicts) > 1:
            trajectory = []
            for v in reversed(recent_verdicts):
                t = v.created_at.strftime("%d/%m %H:%M")
                trajectory.append(f"{t}: {v.yes_probability:.0%} ({v.verdict})")
            lines.append(f"Probability trajectory: {' → '.join(trajectory)}")

            # Detect trend
            first_prob = recent_verdicts[-1].yes_probability
            last_prob = latest.yes_probability
            delta = last_prob - first_prob
            if abs(delta) > 0.05:
                direction = "RISING ↑" if delta > 0 else "FALLING ↓"
                lines.append(f"Trend: {direction} ({delta:+.0%} over {len(recent_verdicts)} evaluations)")

        # 3. New developments since last evaluation
        from .db import Article, ArticleBrief
        run_up = db.query(RunUp).get(node.run_up_id)
        if run_up:
            new_articles = (
                db.query(Article)
                .join(ArticleBrief, ArticleBrief.article_id == Article.id)
                .filter(
                    Article.fetched_at > latest.created_at,
                    ArticleBrief.summary.isnot(None),
                )
                .order_by(ArticleBrief.sentiment.desc())
                .limit(50)
                .all()
            )

            # Filter articles related to this narrative (simple keyword match)
            narrative_words = set(run_up.narrative_name.lower().replace("-", " ").split())
            relevant = []
            for a in new_articles:
                title_words = set(a.title.lower().split())
                if narrative_words & title_words:
                    relevant.append(a)
            relevant = relevant[:5]  # Top 5 most relevant

            if relevant:
                lines.append(f"\nNEW DEVELOPMENTS ({len(new_articles)} articles since last eval, {len(relevant)} relevant):")
                for a in relevant:
                    src = a.source[:25] if a.source else "?"
                    title = a.title[:100] if a.title else "?"
                    # Instead of just title, include sentiment and summary
                    sent_str = ""
                    summary_snip = ""
                    try:
                        if hasattr(a, 'brief') and a.brief:
                            sent_str = f" (sent:{a.brief.sentiment:+.2f})" if a.brief.sentiment else ""
                            summary_snip = f" — {(a.brief.summary or '')[:80]}" if a.brief.summary else ""
                    except Exception:
                        pass
                    lines.append(f"  • [{src}] {title}{sent_str}{summary_snip}")
            else:
                lines.append(f"\nNew articles since last eval: {len(new_articles)} (none directly narrative-related)")

        # 4. Surprise factor
        if latest.entry_reasoning and len(recent_verdicts) >= 2:
            prev = recent_verdicts[1]
            prob_shift = abs(latest.yes_probability - prev.yes_probability)
            if prob_shift > 0.10:
                lines.append(f"\n⚡ SURPRISE: Probability shifted {prob_shift:.0%} between last two evaluations!")
            verdict_changed = latest.verdict != prev.verdict
            if verdict_changed:
                lines.append(f"⚡ VERDICT CHANGED: {prev.verdict} → {latest.verdict}")

        lines.append("")  # trailing newline
        return "\n".join(lines)

    except Exception as e:
        logger.debug("Swarm memory build failed (non-fatal): %s", e)
        return ""


def _build_node_context(node: DecisionNode, db) -> Dict[str, str]:
    """Build the context strings for a decision node.

    Combines: original tree data + NLP intelligence + narrative momentum +
    prediction markets + composite signals + Bayesian trail + strategic briefing +
    price momentum + military indicators + news-price correlation + forward outlook.
    """
    consequences = (
        db.query(Consequence)
        .filter(Consequence.decision_node_id == node.id)
        .order_by(Consequence.branch, Consequence.order)
        .all()
    )

    yes_cons = []
    no_cons = []
    stock_lines = []

    for c in consequences:
        line = f"- {c.description} (p={c.probability:.0%})"
        if c.proximity_pct and c.proximity_pct > 0:
            line += f" [proximity: {c.proximity_pct:.0f}%]"
        if c.branch == "yes":
            yes_cons.append(line)
        else:
            no_cons.append(line)

        # Gather stock impacts
        impacts = (
            db.query(StockImpact)
            .filter(StockImpact.consequence_id == c.id)
            .all()
        )
        for si in impacts:
            arrow = "▲" if si.direction == "bullish" else "▼"
            stock_lines.append(
                f"{arrow} {si.ticker} ({si.direction}, {si.magnitude}) — {si.reasoning[:80]}"
            )

    # Market context from price fetcher
    market_context = _get_market_context()

    # ── Enriched context (each degrades gracefully) ──
    news_intel = _safe_ctx(_get_news_intelligence, node, db)
    momentum = _safe_ctx(_get_narrative_momentum, node, db)
    polymarket = _safe_ctx(_get_polymarket_context, node, db)
    signal = _safe_ctx(_get_trading_signal_context, node, db)
    bayes = _safe_ctx(_get_bayesian_trail, node, db)
    strategic = _safe_ctx(_get_strategic_briefing, node, db)

    # ── Phase 2 enrichment (each degrades gracefully) ──
    price_mom = _safe_ctx(_get_price_momentum, node, db)
    military = _safe_ctx(_get_military_indicators, node, db)
    news_price = _safe_ctx(_get_news_price_correlation, node, db)
    outlook = _safe_ctx(_get_forward_outlook, node, db)

    # ── Portfolio context (for portfolio-aware experts) ──
    portfolio_ctx = _safe_ctx(_get_portfolio_context, node, db)

    # ── Swarm Memory (previous verdict + new developments) ──
    swarm_memory = _build_swarm_memory(node, db)

    return {
        # Original fields
        "question": node.question,
        "timeline": node.timeline_estimate or "unknown",
        "yes_prob": f"{node.yes_probability * 100:.0f}",
        "yes_consequences": "\n".join(yes_cons) or "None specified",
        "no_consequences": "\n".join(no_cons) or "None specified",
        "stock_impacts": "\n".join(stock_lines[:15]) or "None specified",
        "market_context": market_context,
        # Enriched fields (Phase 1)
        "news_intelligence": news_intel,
        "narrative_momentum": momentum,
        "polymarket_context": polymarket,
        "trading_signal": signal,
        "bayesian_trail": bayes,
        "strategic_briefing": strategic,
        # Enriched fields (Phase 2)
        "price_momentum": price_mom,
        "military_indicators": military,
        "news_price_correlation": news_price,
        "forward_outlook": outlook,
        # Portfolio
        "portfolio_context": portfolio_ctx,
        # Swarm Memory
        "swarm_memory": swarm_memory,
    }


def _get_market_context() -> str:
    """Get current market indicators + Fear/Greed + options data."""
    try:
        from .price_fetcher import PriceFetcher

        pf = PriceFetcher()
        indicators = pf.get_market_indicators()

        parts = []
        for key, data in indicators.items():
            if isinstance(data, dict) and "price" in data:
                change = data.get("change_pct", 0)
                arrow = "+" if change >= 0 else ""
                parts.append(f"{key}: {data['price']:,.2f} ({arrow}{change:.1f}%)")

        line = " | ".join(parts) if parts else "Market data unavailable"

        # Fear & Greed Index
        try:
            fg = pf.get_fear_greed()
            if "error" not in fg:
                delta = fg["score"] - fg.get("previous_close", fg["score"])
                line += f" | Fear/Greed: {fg['score']} {fg['label']} ({delta:+d})"
        except Exception:
            pass

        # SPY options put/call ratio
        try:
            opts = pf.get_options_summary("SPY")
            if "error" not in opts and opts.get("put_call_ratio") is not None:
                line += f" | SPY P/C ratio: {opts['put_call_ratio']}"
                if opts.get("implied_vol_put"):
                    line += f", IV(put): {opts['implied_vol_put']:.1%}"
        except Exception:
            pass

        # FRED economic indicators (optional)
        try:
            econ = pf.get_economic_indicators()
            if "error" not in econ:
                spread = econ["yield_spread"]
                curve = "INVERTED" if spread < 0 else "normal"
                line += (
                    f" | 10Y: {econ['treasury_10y']}% 2Y: {econ['treasury_2y']}% "
                    f"spread: {spread:+.2f}% ({curve}) | FFR: {econ['fed_funds_rate']}%"
                )
        except Exception:
            pass

        # Silver & Copper commodities
        try:
            commodities = pf.get_commodities_extended()
            parts_ext = []
            for key in ("silver", "copper"):
                data = commodities.get(key, {})
                if data.get("price") is not None:
                    change = data.get("change_pct", 0)
                    parts_ext.append(f"{key.title()}: ${data['price']:.2f} ({change:+.1f}%)")
            if parts_ext:
                line += " | " + " | ".join(parts_ext)
        except Exception:
            pass

        # Defense & Energy sector snapshot
        try:
            sectors = pf.get_sector_snapshot()
            sector_parts = []
            for key in ("defense_etf", "energy_etf"):
                s = sectors.get(key, {})
                if s.get("price") is not None:
                    sector_parts.append(
                        f"{s['ticker']}: ${s['price']:.2f} ({s.get('change_pct', 0):+.1f}% 24h, "
                        f"{s.get('change_2wk_pct', 0):+.1f}% 2wk {s.get('trend_2wk', '?')})"
                    )
            if sector_parts:
                line += "\nSectors: " + " | ".join(sector_parts)

            movers = sectors.get("defense_movers", [])
            if movers:
                mover_parts = [f"{m['ticker']}: {m['change_2wk_pct']:+.1f}% 2wk" for m in movers]
                line += f"\nDefense stocks: {' | '.join(mover_parts)}"
        except Exception:
            pass

        return line
    except Exception:
        return "Market data unavailable"


# ---------------------------------------------------------------------------
# Cross-expert intelligence helpers (for Round 2 debate)
# ---------------------------------------------------------------------------

def _build_enrichment_summary(context: Dict[str, str]) -> str:
    """Compact summary of key data points for cross-expert reference in debate round.

    Picks the most important signals from each enrichment section (~100 tokens).
    """
    parts = []

    # Fear/Greed from market context
    mc = context.get("market_context", "")
    if "Fear/Greed:" in mc:
        idx = mc.index("Fear/Greed:")
        segment = mc[idx:].split("|")[0].strip()
        parts.append(segment)

    # Narrative momentum headline
    nm = context.get("narrative_momentum", "")
    if nm:
        first_line = nm.split("\n")[0] if "\n" in nm else nm
        parts.append(first_line[:100])

    # Price momentum (first 3 lines for richer signal)
    pm = context.get("price_momentum", "")
    if pm:
        pm_summary = "\n".join(pm.split("\n")[:3])
        parts.append(pm_summary[:300])

    # Military posture headline
    mi = context.get("military_indicators", "")
    if mi:
        lines = mi.split("\n")
        # Get the count line (2nd line usually)
        if len(lines) >= 2:
            parts.append(lines[1][:100])
        else:
            parts.append(lines[0][:100])

    # Forward outlook
    fo = context.get("forward_outlook", "")
    if fo:
        for line in fo.split("\n"):
            if "Narrative:" in line or "Polymarket:" in line:
                parts.append(line.strip()[:100])
                break

    # News-price correlation hit rate
    npc = context.get("news_price_correlation", "")
    if npc:
        for line in npc.split("\n"):
            if "Hit rate:" in line:
                parts.append(line.strip()[:100])
                break

    return "\n".join(parts) if parts else "No enrichment data available"


def _build_disagreement_map(round1_results: List[Dict[str, Any]]) -> str:
    """Identify where experts disagree most on probability and trading action.

    Shows: probability range + spread, most bullish/bearish expert, outlier.
    """
    probs = []
    for r in round1_results:
        resp = r.get("response", {})
        p = resp.get("yes_probability_estimate", 50)
        action = resp.get("trading_action", "HOLD")
        assessment = resp.get("assessment", "")[:60]
        probs.append((r.get("expert_name", "?"), p, action, assessment))

    if not probs:
        return "No Round 1 data available"

    probs.sort(key=lambda x: x[1])
    low = probs[0]
    high = probs[-1]
    spread = high[1] - low[1]

    lines = [f"Probability range: {low[1]}%-{high[1]}% (spread: {spread}pp)"]
    lines.append(f"MOST BULLISH: {high[0]} ({high[1]}% YES, {high[2]}) — \"{high[3]}\"")
    lines.append(f"MOST BEARISH: {low[0]} ({low[1]}% YES, {low[2]}) — \"{low[3]}\"")

    # Outlier detection
    median_p = probs[len(probs) // 2][1]
    max_div = max(probs, key=lambda x: abs(x[1] - median_p))
    if abs(max_div[1] - median_p) > 10:
        lines.append(f"OUTLIER: {max_div[0]} at {max_div[1]}% vs median {median_p}%")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Debate rounds
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Expert-specific context trimming
# ---------------------------------------------------------------------------
# Each expert gets only the context fields relevant to their specialization.
# This reduces token waste (~30-40% fewer tokens per call) and prevents
# experts from hallucinating analysis of data outside their domain.

_EXPERT_CONTEXT_KEYS: Dict[str, List[str]] = {
    # Core fields every expert needs
    "_common": [
        "question", "timeline", "yes_prob",
        "yes_consequences", "no_consequences", "stock_impacts",
        "market_context", "swarm_memory",
    ],
    # Expert-specific additional fields (audit-improved: each expert gets ALL
    # data relevant to their specialty — see audit Fix 1)
    "geopolitical": ["news_intelligence", "narrative_momentum", "military_indicators",
                     "strategic_briefing", "forward_outlook", "polymarket_context"],
    "energy": ["news_intelligence", "narrative_momentum", "price_momentum",
               "news_price_correlation", "forward_outlook", "trading_signal"],
    "macro": ["news_intelligence", "bayesian_trail", "strategic_briefing",
              "forward_outlook", "price_momentum", "polymarket_context",
              "narrative_momentum"],
    "sentiment": ["news_intelligence", "narrative_momentum", "news_price_correlation",
                  "polymarket_context", "trading_signal"],
    "technical": ["price_momentum", "news_price_correlation", "trading_signal",
                  "bayesian_trail", "forward_outlook"],
    "risk": ["news_intelligence", "narrative_momentum", "bayesian_trail",
             "portfolio_context", "forward_outlook", "price_momentum",
             "military_indicators", "news_price_correlation"],
    "contrarian": ["news_intelligence", "narrative_momentum", "polymarket_context",
                   "trading_signal", "price_momentum", "forward_outlook"],
    "supplychain": ["news_intelligence", "narrative_momentum", "price_momentum",
                    "strategic_briefing", "forward_outlook"],
    "portfolio": ["news_intelligence", "trading_signal", "price_momentum",
                  "portfolio_context", "forward_outlook", "bayesian_trail"],
    # V1 new experts (2026-03-15)
    "military": ["news_intelligence", "military_indicators", "narrative_momentum",
                 "strategic_briefing", "forward_outlook", "price_momentum"],
    "regulatory": ["news_intelligence", "narrative_momentum", "strategic_briefing",
                   "forward_outlook", "polymarket_context"],
    "sector": ["news_intelligence", "price_momentum", "narrative_momentum",
               "trading_signal", "news_price_correlation", "forward_outlook"],
}


def _trim_context_for_expert(context: Dict[str, str], expert_id: str) -> Dict[str, str]:
    """Return a copy of context with only the fields relevant to this expert.

    Irrelevant fields are replaced with empty strings so the prompt template
    still formats correctly (no KeyError) but the token count is minimal.
    """
    allowed = set(_EXPERT_CONTEXT_KEYS.get("_common", []))
    allowed.update(_EXPERT_CONTEXT_KEYS.get(expert_id, []))

    trimmed = {}
    for key, value in context.items():
        if key in allowed:
            trimmed[key] = value
        else:
            trimmed[key] = ""  # Keep key for .format() but empty value

    return trimmed


async def _round1_individual(
    context: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Round 1: Each expert independently analyzes the node.

    Each expert may use a different LLM provider/model.
    Context is trimmed per expert to reduce token waste and prevent hallucination.
    Experts are called concurrently per provider (Groq + OpenRouter in parallel).
    """

    async def _call_expert(expert: Dict[str, str]) -> Dict[str, Any]:
        client, model = _get_client_and_model(expert)
        if client is None:
            return {
                "expert_id": expert["id"],
                "expert_name": expert["name"],
                "emoji": expert["emoji"],
                "model": "none",
                "response": {
                    "assessment": "No LLM provider available",
                    "yes_probability_estimate": 50,
                    "trading_action": "HOLD",
                    "confidence": 0,
                },
            }

        expert_context = _trim_context_for_expert(context, expert["id"])
        shared_context = ROUND1_SHARED_CONTEXT.format(**expert_context)
        expert_instruction = ROUND1_EXPERT_INSTRUCTION.format(
            expert_name=expert["name"],
        )

        response = await _rate_limited_call(
            client,
            model,
            messages=[
                {"role": "system", "content": shared_context},
                {"role": "system", "content": f"You are: {expert['name']}. {expert['system']}"},
                {"role": "user", "content": expert_instruction},
            ],
            purpose="round1",
        )

        return {
            "expert_id": expert["id"],
            "expert_name": expert["name"],
            "emoji": expert["emoji"],
            "model": model.split("/")[-1] if "/" in model else model,
            "response": response or {
                "assessment": "Analysis unavailable",
                "yes_probability_estimate": 50,
                "trading_action": "HOLD",
                "confidence": 0,
            },
        }

    # Run all experts concurrently — rate limiter locks handle per-provider pacing
    active_experts = get_active_experts()
    tasks = [_call_expert(expert) for expert in active_experts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to default results
    final = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error("Round 1 expert %s raised: %s", active_experts[i]["id"], result)
            final.append({
                "expert_id": active_experts[i]["id"],
                "expert_name": active_experts[i]["name"],
                "emoji": active_experts[i]["emoji"],
                "model": "error",
                "response": {
                    "assessment": "Analysis failed",
                    "yes_probability_estimate": 50,
                    "trading_action": "HOLD",
                    "confidence": 0,
                },
            })
        else:
            final.append(result)

    return final


async def _round2_debate(
    context: Dict[str, str],
    round1_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Round 2: Experts see all Round 1 assessments and debate.

    Each expert uses the same provider/model as Round 1.
    """
    # Format Round 1 assessments for context
    r1_summary = []
    for r in round1_results:
        resp = r["response"]
        r1_summary.append(
            f"{r['emoji']} {r['expert_name']}:\n"
            f"  Assessment: {resp.get('assessment', 'N/A')}\n"
            f"  YES probability: {resp.get('yes_probability_estimate', 50)}%\n"
            f"  Action: {resp.get('trading_action', 'HOLD')}\n"
            f"  Confidence: {resp.get('confidence', 0)}%\n"
            f"  Risk: {resp.get('key_risk', 'N/A')}"
        )
    all_assessments = "\n\n".join(r1_summary)

    # Build cross-expert intelligence
    enrichment_summary = _build_enrichment_summary(context)
    disagreement_map = _build_disagreement_map(round1_results)

    # Pre-compute shared context ONCE — identical across all experts (prefix caching)
    shared_context = ROUND2_SHARED_CONTEXT.format(
        question=context["question"],
        all_round1_assessments=all_assessments,
        enrichment_summary=enrichment_summary,
        disagreement_map=disagreement_map,
    )

    async def _debate_expert(expert: Dict[str, str]) -> Dict[str, Any]:
        client, model = _get_client_and_model(expert)
        if client is None:
            return {
                "expert_id": expert["id"],
                "expert_name": expert["name"],
                "emoji": expert["emoji"],
                "model": "none",
                "response": {
                    "revised_assessment": "Debate unavailable",
                    "yes_probability_estimate": 50,
                    "trading_action": "HOLD",
                    "confidence": 0,
                    "agrees_with_majority": True,
                },
            }

        expert_instruction = ROUND2_EXPERT_INSTRUCTION.format(
            expert_name=expert["name"],
        )

        response = await _rate_limited_call(
            client,
            model,
            messages=[
                {"role": "system", "content": shared_context},
                {"role": "system", "content": f"You are: {expert['name']}. {expert['system']}"},
                {"role": "user", "content": expert_instruction},
            ],
            purpose="round2",
        )

        return {
            "expert_id": expert["id"],
            "expert_name": expert["name"],
            "emoji": expert["emoji"],
            "model": model.split("/")[-1] if "/" in model else model,
            "response": response or {
                "revised_assessment": "Debate unavailable",
                "yes_probability_estimate": 50,
                "trading_action": "HOLD",
                "confidence": 0,
                "agrees_with_majority": True,
            },
        }

    # Run all experts concurrently — rate limiter locks handle per-provider pacing
    active_experts = get_active_experts()
    tasks = [_debate_expert(expert) for expert in active_experts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    final = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error("Round 2 expert %s raised: %s", active_experts[i]["id"], result)
            final.append({
                "expert_id": active_experts[i]["id"],
                "expert_name": active_experts[i]["name"],
                "emoji": active_experts[i]["emoji"],
                "model": "error",
                "response": {
                    "revised_assessment": "Debate failed",
                    "yes_probability_estimate": 50,
                    "trading_action": "HOLD",
                    "confidence": 0,
                    "agrees_with_majority": True,
                },
            })
        else:
            final.append(result)

    return final


def _deterministic_aggregate(
    round2_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Deterministic Python aggregation of expert opinions — no LLM needed.

    Uses confidence-weighted median for verdict and probability, replacing the
    LLM synthesis that introduced single-model bias into a multi-model debate.

    Method: weighted median of yes_probability and trading_action, where each
    expert's weight = their self-reported confidence.
    """
    # Filter out failed experts before aggregation
    live_results = []
    failed_count = 0
    for r in round2_results:
        resp = r.get("response", {})
        assessment = resp.get("revised_assessment", "") or resp.get("assessment", "")
        is_failed = (
            resp.get("confidence", 50) == 0
            and resp.get("yes_probability_estimate") == 50
            and any(kw in (assessment or "").lower() for kw in ("unavailable", "failed", "no llm provider"))
        )
        if is_failed:
            failed_count += 1
            logger.info("Excluding failed expert from aggregation: %s", r.get("expert_name", "?"))
        else:
            live_results.append(r)

    total_experts = len(round2_results)
    participation_rate = len(live_results) / total_experts if total_experts else 0

    ACTION_ORDER = {"STRONG_SELL": 0, "SELL": 1, "HOLD": 2, "BUY": 3, "STRONG_BUY": 4}
    ORDER_TO_ACTION = {v: k for k, v in ACTION_ORDER.items()}

    probs: List[Tuple[float, float]] = []     # (value, weight)
    actions: List[Tuple[int, float]] = []      # (action_ordinal, weight)
    ticker_votes: Dict[str, Dict[str, int]] = {}  # ticker -> {long: N, short: M}
    risk_notes = []
    dissent_notes = []
    assessments = []

    for r in live_results:
        resp = r.get("response", {})
        conf = max(1.0, float(resp.get("confidence", 50)))  # min weight 1
        weight = conf / 100.0

        # YES probability
        yp = float(resp.get("yes_probability_estimate", 50))
        probs.append((yp, weight))

        # Trading action
        action = resp.get("trading_action", "HOLD")
        action_ord = ACTION_ORDER.get(action, 2)
        actions.append((action_ord, weight))

        # Ticker aggregation
        ticker = resp.get("top_ticker")
        direction = resp.get("ticker_direction", "long")
        if ticker:
            entry = ticker_votes.setdefault(ticker, {"long": 0, "short": 0})
            entry[direction] = entry.get(direction, 0) + 1

        # Qualitative data
        risk = resp.get("key_risk") or ""
        if risk and len(risk) > 10:
            risk_notes.append(risk)
        dissent = resp.get("dissent_reason") or ""
        if dissent and dissent.lower() not in ("none", "null", "n/a") and len(dissent) > 5:
            dissent_notes.append(dissent)

        assessment = resp.get("revised_assessment") or ""
        if assessment:
            assessments.append(f"{r.get('emoji', '')} {r.get('expert_name', '?')}: {assessment[:200]}")

    # --- Weighted median helper ---
    def _weighted_median(pairs: list) -> float:
        if not pairs:
            return 50.0
        sorted_pairs = sorted(pairs, key=lambda x: x[0])
        total_weight = sum(w for _, w in sorted_pairs)
        if total_weight == 0:
            return sorted_pairs[len(sorted_pairs) // 2][0]
        half = total_weight / 2.0
        cumulative = 0.0
        for val, w in sorted_pairs:
            cumulative += w
            if cumulative >= half:
                return val
        return sorted_pairs[-1][0]

    # Compute weighted medians
    median_prob = _weighted_median(probs)
    median_action_ord = round(_weighted_median(actions))
    median_action_ord = max(0, min(4, median_action_ord))
    verdict = ORDER_TO_ACTION.get(median_action_ord, "HOLD")

    # Consensus strength = 1 - normalized spread of expert opinions
    if len(probs) >= 2:
        prob_values = [p[0] for p in probs]
        spread = max(prob_values) - min(prob_values)
        consensus_strength = max(0, round(100 - spread, 1))
    else:
        consensus_strength = 50

    # Average confidence across experts (raw 0-100 scale, not the 0-1 weights)
    avg_confidence = (sum(c * 100 for _, c in probs) / len(probs)) if probs else 50.0
    # Scale confidence by consensus — low consensus = less confident verdict
    effective_confidence = round(avg_confidence * (consensus_strength / 100), 1)

    # Primary ticker = most-voted ticker
    primary_ticker = None
    ticker_direction = None
    if ticker_votes:
        best_ticker = max(
            ticker_votes.items(),
            key=lambda x: x[1]["long"] + x[1]["short"],
        )
        primary_ticker = best_ticker[0]
        ticker_direction = "long" if best_ticker[1]["long"] >= best_ticker[1]["short"] else "short"

    # Format all_ticker_signals
    all_signals = [
        {"ticker": t, "direction": "long" if v["long"] >= v["short"] else "short",
         "votes": v["long"] + v["short"]}
        for t, v in sorted(ticker_votes.items(), key=lambda x: x[1]["long"] + x[1]["short"], reverse=True)
    ]

    return {
        "verdict": verdict,
        "confidence": round(effective_confidence),
        "yes_probability": round(median_prob),
        "primary_ticker": primary_ticker,
        "ticker_direction": ticker_direction,
        "entry_reasoning": assessments[0] if assessments else "Deterministic aggregation of expert panel",
        "exit_trigger": "",
        "risk_note": risk_notes[0] if risk_notes else "",
        "dissent_note": dissent_notes[0] if dissent_notes else "",
        "consensus_strength": round(consensus_strength),
        "all_ticker_signals": all_signals,
        "aggregation_method": "deterministic_weighted_median",
        "participation_rate": participation_rate,
        "experts_responded": len(live_results),
        "experts_total": total_experts,
    }


async def _round3_synthesis(
    context: Dict[str, str],
    round2_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Round 3: Deterministic Python aggregation + optional LLM narrative enrichment.

    The verdict, probability, and confidence are computed deterministically via
    confidence-weighted median (no LLM bias). The LLM call is kept ONLY for
    generating the entry_reasoning and exit_trigger narrative — the LLM cannot
    override the deterministic verdict.
    """
    # Step 1: Deterministic verdict (pure Python, no API cost)
    verdict = _deterministic_aggregate(round2_results)

    # Step 2: Optional LLM narrative enrichment (does NOT change the verdict)
    r2_summary = []
    for r in round2_results:
        resp = r["response"]
        r2_summary.append(
            f"{r['emoji']} {r['expert_name']}:\n"
            f"  Assessment: {resp.get('revised_assessment', 'N/A')}\n"
            f"  YES probability: {resp.get('yes_probability_estimate', 50)}%\n"
            f"  Action: {resp.get('trading_action', 'HOLD')}\n"
            f"  Confidence: {resp.get('confidence', 0)}%\n"
            f"  Agrees with majority: {resp.get('agrees_with_majority', True)}\n"
            f"  Dissent: {resp.get('dissent_reason', 'None')}"
        )
    all_assessments = "\n\n".join(r2_summary)

    prompt = ROUND3_SYNTHESIS.format(
        question=context["question"],
        timeline=context["timeline"],
        verdict_direction=verdict.get("verdict", "HOLD"),
        verdict_confidence=verdict.get("confidence", 0),
        verdict_probability=verdict.get("yes_probability", 50),
        verdict_ticker=verdict.get("primary_ticker", "N/A"),
        verdict_ticker_direction=verdict.get("ticker_direction", "N/A"),
        all_round2_assessments=all_assessments,
    )

    # Try LLM for narrative only
    client = _get_openrouter_client()
    model = OPENROUTER_MODELS.get("gpt-oss-120b", "openai/gpt-oss-120b")
    if client is None:
        client = _get_groq_client()
        model = GROQ_MODELS.get("qwen3-32b", GROQ_DEFAULT_MODEL)

    if client is not None:
        try:
            response = await _rate_limited_call(
                client,
                model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a senior portfolio manager synthesizing expert "
                            "analysis into actionable trading decisions. You weight "
                            "evidence carefully and produce clear entry/exit signals. "
                            "Respond with valid JSON only."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                purpose="synthesis",
            )
            if response:
                # Extract narrative fields from LLM, but KEEP deterministic verdict
                if response.get("entry_reasoning"):
                    verdict["entry_reasoning"] = response["entry_reasoning"]
                if response.get("exit_trigger"):
                    verdict["exit_trigger"] = response["exit_trigger"]
                if response.get("risk_note") and len(response.get("risk_note", "")) > len(verdict.get("risk_note", "")):
                    verdict["risk_note"] = response["risk_note"]
                if response.get("dissent_note") and len(response.get("dissent_note", "")) > len(verdict.get("dissent_note", "")):
                    verdict["dissent_note"] = response["dissent_note"]
        except Exception:
            logger.warning("Swarm: LLM narrative enrichment failed — using deterministic-only verdict.")

    return verdict


# ---------------------------------------------------------------------------
# Main entry point — evaluate one decision node
# ---------------------------------------------------------------------------

def _compute_context_hash(context: Dict[str, str]) -> str:
    """Compute a SHA-256 hash of the swarm context.

    Only hashes data that would change the verdict — skips volatile fields
    like exact market prices (which change every minute).
    We hash: question, yes_prob, consequences, stock_impacts, news_intelligence,
    narrative_momentum, trading_signal, strategic_briefing.
    """
    stable_keys = [
        "question", "yes_prob", "yes_consequences", "no_consequences",
        "stock_impacts", "news_intelligence", "narrative_momentum",
        "trading_signal", "strategic_briefing", "forward_outlook",
    ]
    parts = []
    for key in stable_keys:
        val = context.get(key, "")
        if val:
            parts.append(f"{key}:{val}")
    combined = "\n".join(parts)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


async def evaluate_node(node_id: int) -> Optional[Dict[str, Any]]:
    """Run the full 3-round swarm debate on a single decision node.

    Uses multiple providers (Groq + OpenRouter) for diverse perspectives.
    Skips re-evaluation if the context hasn't materially changed (context-hash check).
    Returns the final verdict dict or None on failure.
    """
    # Need at least one provider
    groq = _get_groq_client()
    openrouter = _get_openrouter_client()
    if groq is None and openrouter is None:
        logger.warning("Swarm: no LLM providers available (need Groq or OpenRouter)")
        return None

    db = get_session()
    try:
        node = db.query(DecisionNode).get(node_id)
        if not node:
            logger.warning("Swarm: node %d not found", node_id)
            return None

        context = _build_node_context(node, db)

        # Context-hash caching: skip re-evaluation if context unchanged
        ctx_hash = _compute_context_hash(context)
        existing_verdict = (
            db.query(SwarmVerdict)
            .filter(
                SwarmVerdict.decision_node_id == node.id,
                SwarmVerdict.superseded_at.is_(None),
            )
            .first()
        )
        if (
            existing_verdict
            and hasattr(existing_verdict, "context_hash")
            and existing_verdict.context_hash == ctx_hash
        ):
            logger.info(
                "Swarm: skipping node %d — context unchanged (hash %s)",
                node_id, ctx_hash[:12],
            )
            return {
                "verdict": existing_verdict.verdict,
                "confidence": round(existing_verdict.confidence * 100),
                "yes_probability": round(existing_verdict.yes_probability * 100),
                "primary_ticker": existing_verdict.primary_ticker,
                "skipped_reason": "context_unchanged",
            }

        providers = []
        if groq: providers.append("Groq")
        if openrouter: providers.append("OpenRouter")
        logger.info(
            "Swarm: evaluating node %d — %s [providers: %s, hash: %s]",
            node_id,
            node.question[:60],
            "+".join(providers),
            ctx_hash[:12],
        )

        # Round 1: Individual analysis (12 calls across providers)
        r1 = await _round1_individual(context)
        logger.info("Swarm: Round 1 complete for node %d (%d agents)", node_id, len(get_active_experts()))

        r1_failed = sum(1 for r in r1 if r.get("model") in ("error", "none")
                        or (r.get("response", {}).get("confidence") == 0
                            and "unavailable" in (r.get("response", {}).get("assessment", "")).lower()))
        if r1_failed > 0:
            logger.warning("Round 1: %d/%d experts failed", r1_failed, len(r1))

        # Round 2: Debate (12 calls across providers)
        r2 = await _round2_debate(context, r1)
        logger.info("Swarm: Round 2 complete for node %d (%d agents)", node_id, len(get_active_experts()))

        r2_failed = sum(1 for r in r2 if r.get("model") in ("error", "none")
                        or (r.get("response", {}).get("confidence") == 0
                            and "unavailable" in (r.get("response", {}).get("assessment", "")).lower()))
        if r2_failed > 0:
            logger.warning("Round 2: %d/%d experts failed", r2_failed, len(r2))

        # Round 3: Deterministic synthesis + optional LLM narrative
        verdict = await _round3_synthesis(context, r2)
        verdict["_context_hash"] = ctx_hash  # Pass to _store_verdict

        MIN_PARTICIPATION = 0.615  # ~8/13 experts must respond
        if verdict.get("participation_rate", 1.0) < MIN_PARTICIPATION:
            logger.warning("LOW PARTICIPATION (%.0f%%) — marking as low_confidence",
                           verdict["participation_rate"] * 100)
            verdict["confidence"] = min(verdict.get("confidence", 0), 0.15)
        logger.info(
            "Swarm: Verdict for node %d — %s (confidence: %s%%)",
            node_id,
            verdict.get("verdict", "HOLD"),
            verdict.get("confidence", 0),
        )

        # Store verdict in DB
        _store_verdict(node, verdict, r1, r2, db)

        return verdict

    except Exception:
        logger.exception("Swarm evaluation failed for node %d", node_id)
        return None
    finally:
        db.close()


def _store_verdict(
    node: DecisionNode,
    verdict: Dict[str, Any],
    round1: List[Dict],
    round2: List[Dict],
    db,
) -> None:
    """Persist the swarm verdict to the database."""
    try:
        # Validate primary_ticker against Bunq whitelist
        primary_ticker = verdict.get("primary_ticker")
        if primary_ticker and not is_available_on_bunq(primary_ticker):
            from .bunq_stocks import get_eu_equivalents
            eu_alts = get_eu_equivalents(primary_ticker)
            if eu_alts:
                logger.info("Swarm: mapped ticker %s → %s", primary_ticker, eu_alts[0]["ticker"])
                # Store original for reference, use mapped for downstream
                verdict["original_ticker"] = primary_ticker
                primary_ticker = eu_alts[0]["ticker"]
            else:
                primary_ticker = None
            verdict["primary_ticker"] = primary_ticker
            if not primary_ticker:
                verdict["ticker_direction"] = None

        # Filter all_ticker_signals to Bunq-available only
        raw_signals = verdict.get("all_ticker_signals", [])
        if raw_signals:
            filtered = [s for s in raw_signals if is_available_on_bunq(s.get("ticker", ""))]
            dropped = len(raw_signals) - len(filtered)
            if dropped:
                logger.info(
                    "Swarm: filtered %d non-Bunq tickers from all_ticker_signals (node %d)",
                    dropped, node.id,
                )
            verdict["all_ticker_signals"] = filtered

        # Check for existing verdict and supersede it
        existing = (
            db.query(SwarmVerdict)
            .filter(
                SwarmVerdict.decision_node_id == node.id,
                SwarmVerdict.superseded_at.is_(None),
            )
            .first()
        )
        if existing:
            existing.superseded_at = datetime.utcnow()

        sv = SwarmVerdict(
            decision_node_id=node.id,
            run_up_id=node.run_up_id,
            verdict=verdict.get("verdict", "HOLD"),
            confidence=verdict.get("confidence", 0) / 100.0,
            yes_probability=verdict.get("yes_probability", 50) / 100.0,
            primary_ticker=verdict.get("primary_ticker"),
            ticker_direction=verdict.get("ticker_direction"),
            entry_reasoning=verdict.get("entry_reasoning", ""),
            exit_trigger=verdict.get("exit_trigger", ""),
            risk_note=verdict.get("risk_note", ""),
            dissent_note=verdict.get("dissent_note", ""),
            consensus_strength=verdict.get("consensus_strength", 0) / 100.0,
            all_ticker_signals_json=json.dumps(
                verdict.get("all_ticker_signals", []), ensure_ascii=False
            ),
            round1_json=json.dumps(
                [{"id": r["expert_id"], "name": r["expert_name"], "r": r["response"]}
                 for r in round1],
                ensure_ascii=False,
            ),
            round2_json=json.dumps(
                [{"id": r["expert_id"], "name": r["expert_name"], "r": r["response"]}
                 for r in round2],
                ensure_ascii=False,
            ),
            model_used="multi-provider",
            context_hash=verdict.get("_context_hash"),
        )
        db.add(sv)
        db.commit()
        logger.info(
            "Swarm verdict stored: node=%d verdict=%s confidence=%.0f%% hash=%s",
            node.id,
            sv.verdict,
            sv.confidence * 100,
            (sv.context_hash or "")[:12],
        )
    except Exception:
        db.rollback()
        logger.exception("Failed to store swarm verdict for node %d", node.id)


# ---------------------------------------------------------------------------
# Scheduler entry point — batch evaluate nodes
# ---------------------------------------------------------------------------

async def swarm_consensus_cycle() -> int:
    """Evaluate decision nodes that need a (re-)assessment.

    Called by the APScheduler job.  Returns the number of nodes evaluated.
    Focused nodes use a shorter TTL (30 min) and get priority.
    """
    groq = _get_groq_client()
    openrouter = _get_openrouter_client()
    if groq is None and openrouter is None:
        logger.info("Swarm: disabled (no Groq or OpenRouter API key)")
        return 0

    # Check if BOTH providers have circuit breakers tripped — skip cycle entirely
    groq_blocked = time.time() < _provider_blocked_until.get("groq", 0)
    or_blocked = time.time() < _provider_blocked_until.get("openrouter", 0)
    if groq_blocked and or_blocked:
        groq_remaining = _provider_blocked_until.get("groq", 0) - time.time()
        or_remaining = _provider_blocked_until.get("openrouter", 0) - time.time()
        logger.error(
            "Swarm: ALL providers circuit-breaker OPEN (Groq: %.0fs, OpenRouter: %.0fs) "
            "— skipping entire cycle to avoid partial verdicts",
            groq_remaining, or_remaining,
        )
        return 0

    # Focus Mode: focused nodes get shorter TTL and priority
    from .focus_manager import get_focused_runup_ids
    focused_ids = set(get_focused_runup_ids())
    FOCUS_TTL_MINUTES = 30

    db = get_session()
    try:
        # Find open decision nodes that need evaluation
        now = datetime.utcnow()
        normal_stale_cutoff = now - timedelta(hours=VERDICT_TTL_HOURS)
        focus_stale_cutoff = now - timedelta(minutes=FOCUS_TTL_MINUTES)

        # Nodes with no verdict, or stale verdicts
        open_nodes = (
            db.query(DecisionNode)
            .filter(
                DecisionNode.status == "open",
            )
            .all()
        )

        nodes_to_evaluate = []
        day_cutoff = now - timedelta(hours=24)
        for node in open_nodes:
            # Check if there's a recent, non-superseded verdict
            latest_verdict = (
                db.query(SwarmVerdict)
                .filter(
                    SwarmVerdict.decision_node_id == node.id,
                    SwarmVerdict.superseded_at.is_(None),
                )
                .first()
            )
            is_focus = node.run_up_id in focused_ids
            cutoff = focus_stale_cutoff if is_focus else normal_stale_cutoff

            if latest_verdict is None or latest_verdict.created_at < cutoff:
                # Anti-loop safeguard: limit evaluations per node per 24h
                evals_today = (
                    db.query(SwarmVerdict)
                    .filter(
                        SwarmVerdict.decision_node_id == node.id,
                        SwarmVerdict.created_at >= day_cutoff,
                    )
                    .count()
                )
                if evals_today >= MAX_EVALS_PER_NODE_PER_DAY:
                    logger.info(
                        "Swarm: node %d hit daily eval limit (%d/%d), skipping.",
                        node.id, evals_today, MAX_EVALS_PER_NODE_PER_DAY,
                    )
                    continue
                nodes_to_evaluate.append((node, is_focus))

        if not nodes_to_evaluate:
            logger.info("Swarm: all nodes up-to-date, nothing to evaluate.")
            return 0

        # Sort: focused nodes first
        nodes_to_evaluate.sort(key=lambda x: (0 if x[1] else 1))
        batch = [n for n, _ in nodes_to_evaluate[:MAX_NODES_PER_CYCLE]]
        logger.info(
            "Swarm: evaluating %d nodes (%d total pending)",
            len(batch),
            len(nodes_to_evaluate),
        )

        evaluated = 0
        for node in batch:
            # Re-check budget before each node (25 LLM calls per node)
            try:
                from .tree_generator import get_budget_tier, BudgetTier
                if get_budget_tier() == BudgetTier.BLOCKED:
                    logger.warning("Swarm: BLOCKED tier reached during cycle — stopping.")
                    break
            except ImportError:
                pass

            try:
                result = await evaluate_node(node.id)
                if result:
                    evaluated += 1
            except Exception:
                logger.exception("Swarm: failed on node %d", node.id)
                # Continue with next node
                continue

        logger.info("Swarm: cycle complete — %d/%d nodes evaluated.", evaluated, len(batch))
        return evaluated

    except Exception:
        logger.exception("Swarm consensus cycle FAILED.")
        return 0
    finally:
        db.close()


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def get_verdicts_for_runup(run_up_id: int) -> List[Dict[str, Any]]:
    """Return all active (non-superseded) swarm verdicts for a run-up."""
    db = get_session()
    try:
        verdicts = (
            db.query(SwarmVerdict)
            .filter(
                SwarmVerdict.run_up_id == run_up_id,
                SwarmVerdict.superseded_at.is_(None),
            )
            .order_by(SwarmVerdict.created_at.desc())
            .all()
        )

        results = []
        for v in verdicts:
            node = db.query(DecisionNode).get(v.decision_node_id)
            results.append({
                "id": v.id,
                "node_id": v.decision_node_id,
                "question": node.question if node else "Unknown",
                "verdict": v.verdict,
                "confidence": round(v.confidence * 100),
                "yes_probability": round(v.yes_probability * 100),
                "primary_ticker": v.primary_ticker,
                "ticker_direction": v.ticker_direction,
                "entry_reasoning": v.entry_reasoning,
                "exit_trigger": v.exit_trigger,
                "risk_note": v.risk_note,
                "dissent_note": v.dissent_note,
                "consensus_strength": round(v.consensus_strength * 100),
                "all_ticker_signals": json.loads(v.all_ticker_signals_json or "[]"),
                "model": v.model_used,
                "created_at": v.created_at.isoformat(),
            })

        return results

    finally:
        db.close()


def get_latest_verdict(node_id: int) -> Optional[Dict[str, Any]]:
    """Return the latest non-superseded verdict for a specific node."""
    db = get_session()
    try:
        v = (
            db.query(SwarmVerdict)
            .filter(
                SwarmVerdict.decision_node_id == node_id,
                SwarmVerdict.superseded_at.is_(None),
            )
            .order_by(SwarmVerdict.created_at.desc())
            .first()
        )

        if not v:
            return None

        node = db.query(DecisionNode).get(v.decision_node_id)
        return {
            "id": v.id,
            "node_id": v.decision_node_id,
            "question": node.question if node else "Unknown",
            "verdict": v.verdict,
            "confidence": round(v.confidence * 100),
            "yes_probability": round(v.yes_probability * 100),
            "primary_ticker": v.primary_ticker,
            "ticker_direction": v.ticker_direction,
            "entry_reasoning": v.entry_reasoning,
            "exit_trigger": v.exit_trigger,
            "risk_note": v.risk_note,
            "dissent_note": v.dissent_note,
            "consensus_strength": round(v.consensus_strength * 100),
            "all_ticker_signals": json.loads(v.all_ticker_signals_json or "[]"),
            "round1": json.loads(v.round1_json or "[]"),
            "round2": json.loads(v.round2_json or "[]"),
            "model": v.model_used,
            "created_at": v.created_at.isoformat(),
        }

    finally:
        db.close()
