"""Daily Investment Advisory engine with multi-horizon feedback loop.

Generates BUY/SELL recommendations for Bunq Stock-available tickers by
combining geopolitical signals, confidence scores, swarm verdicts, price
momentum, and insider-trading OSINT.  A lightweight self-learning system
tracks outcomes at T+1d / T+3d / T+7d / T+14d / T+30d and auto-adjusts
component weights via exponential moving averages.

Cost: ~0.005 EUR/day (one Claude Haiku call for the narrative).

Schedule:
    07:25 UTC  — evaluate_open_advisories()  (score past picks at all horizons)
    07:30 UTC  — generate_daily_advisory()   (produce today's advisory)
    Sunday 07:35 — rebalance_weights()       (auto-adjust component weights)
"""

import json
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import or_

from .bunq_stocks import BUNQ_STOCKS, is_available_on_bunq
from .config import config
from .db import (
    AnalysisReport,
    Article,
    DecisionNode,
    EngineSettings,
    RunUp,
    StockImpact,
    SwarmVerdict,
    TradingSignal,
    get_session,
)

logger = logging.getLogger(__name__)

# ── Horizons (days) ────────────────────────────────────────────────────
EVAL_HORIZONS = [1, 3, 7, 14, 30]

# ── Default component weights (overridden by learned weights) ──────────
DEFAULT_WEIGHTS: Dict[str, float] = {
    "geopolitical": 0.30,
    "confidence": 0.25,
    "swarm": 0.20,
    "momentum": 0.15,
    "insider": 0.10,
}

# ── Swarm verdict → position (reused from confidence_scorer) ───────────
_VERDICT_POS = {
    "STRONG_BUY": 1.0, "BUY": 0.75, "HOLD": 0.5,
    "SELL": 0.25, "STRONG_SELL": 0.0,
}

# ── EMA smoothing factor (higher = more recent data matters) ───────────
EMA_ALPHA = 0.25

# ── Position sizing defaults ──────────────────────────────────────────
KELLY_FRACTION = 0.5          # Half-Kelly (conservative for retail)
MAX_POSITION_PCT = 20.0       # Never >20% of portfolio in one pick
MIN_POSITION_PCT = 3.0        # Minimum 3% to be meaningful
DEFAULT_PORTFOLIO_EUR = 5000  # Assumed portfolio size if not configured

# ── Risk level defaults (ATR multipliers) ─────────────────────────────
STOP_LOSS_ATR_MULT = 2.0     # Stop-loss at 2× ATR below entry
TAKE_PROFIT_ATR_MULT = 3.0   # Take-profit at 3× ATR above entry
TRAILING_STOP_ATR = 1.5      # Trailing stop at 1.5× ATR


# ======================================================================
# Weight management
# ======================================================================

def _load_weights() -> Dict[str, float]:
    """Load learned advisory weights from DB, falling back to defaults."""
    session = get_session()
    try:
        s = session.query(EngineSettings).get("advisory_weights")
        if s and s.value:
            loaded = json.loads(s.value)
            if isinstance(loaded, dict) and len(loaded) == len(DEFAULT_WEIGHTS):
                # Validate: all expected keys present, positive values, sum ≈ 1.0
                if (set(loaded.keys()) == set(DEFAULT_WEIGHTS.keys())
                        and all(v >= 0 for v in loaded.values())
                        and 0.95 <= sum(loaded.values()) <= 1.05):
                    return loaded
                logger.warning("Invalid weights in DB (keys=%s, sum=%.4f), using defaults.",
                               set(loaded.keys()), sum(loaded.values()))
    except Exception:
        pass
    finally:
        session.close()
    return dict(DEFAULT_WEIGHTS)


def _save_weights(weights: Dict[str, float]) -> None:
    session = get_session()
    try:
        s = session.query(EngineSettings).get("advisory_weights")
        val = json.dumps(weights)
        if s:
            s.value = val
        else:
            session.add(EngineSettings(key="advisory_weights", value=val))
        session.commit()
    except Exception:
        logger.exception("Failed to save advisory weights.")
        session.rollback()
    finally:
        session.close()


def _load_component_emas() -> Dict[str, Dict[str, float]]:
    """Load per-component per-horizon EMA accuracy scores.

    Shape: {"geopolitical": {"1": 0.65, "3": 0.70, ...}, ...}
    """
    session = get_session()
    try:
        s = session.query(EngineSettings).get("advisory_component_emas")
        if s and s.value:
            return json.loads(s.value)
    except Exception:
        pass
    finally:
        session.close()
    # Initialise with neutral 0.5
    return {
        comp: {str(h): 0.5 for h in EVAL_HORIZONS}
        for comp in DEFAULT_WEIGHTS
    }


def _save_component_emas(emas: Dict[str, Dict[str, float]]) -> None:
    session = get_session()
    try:
        s = session.query(EngineSettings).get("advisory_component_emas")
        val = json.dumps(emas)
        if s:
            s.value = val
        else:
            session.add(EngineSettings(key="advisory_component_emas", value=val))
        session.commit()
    except Exception:
        logger.exception("Failed to save component EMAs.")
        session.rollback()
    finally:
        session.close()


# ======================================================================
# Candidate collection & scoring
# ======================================================================

def _collect_candidates(session) -> Dict[str, Dict[str, Any]]:
    """Gather candidate tickers from TradingSignals + deep analysis top_picks.

    Returns {ticker: {direction, signal_confidence, net_score, narrative, ...}}
    """
    candidates: Dict[str, Dict[str, Any]] = {}

    # --- 1. Active TradingSignals (non-superseded, non-expired) ---
    now = datetime.utcnow()
    signals = (
        session.query(TradingSignal)
        .filter(
            TradingSignal.superseded_by_id.is_(None),
            or_(
                TradingSignal.expires_at.is_(None),
                TradingSignal.expires_at > now,
            ),
        )
        .all()
    )
    for sig in signals:
        t = sig.ticker
        if not t or not is_available_on_bunq(t):
            continue
        candidates.setdefault(t, {
            "ticker": t,
            "name": BUNQ_STOCKS.get(t.upper(), t),
            "direction": sig.direction or "bullish",
            "signal_confidence": sig.confidence or 0.0,
            "signal_level": sig.signal_level or "WATCH",
            "narrative": sig.narrative_name or "",
            "run_up_id": sig.run_up_id,
            "geopolitical_net_score": 0.0,
            "swarm_verdict": None,
            "swarm_confidence": 0.0,
        })
        # Update with higher confidence if duplicate ticker
        if sig.confidence and sig.confidence > candidates[t].get("signal_confidence", 0):
            candidates[t]["signal_confidence"] = sig.confidence
            candidates[t]["direction"] = sig.direction or "bullish"

    # --- 2. Latest deep analysis strategic outlook ---
    latest_report = (
        session.query(AnalysisReport)
        .filter(AnalysisReport.report_type == "daily_briefing")
        .order_by(AnalysisReport.created_at.desc())
        .first()
    )
    if latest_report and latest_report.report_json:
        try:
            report_data = json.loads(latest_report.report_json)
            outlook = report_data.get("strategic_outlook", {})
            for pick in outlook.get("top_picks", []):
                t = pick.get("ticker", "")
                if not t or not is_available_on_bunq(t):
                    continue
                if t not in candidates:
                    candidates[t] = {
                        "ticker": t,
                        "name": pick.get("name", BUNQ_STOCKS.get(t.upper(), t)),
                        "direction": pick.get("direction", "bullish"),
                        "signal_confidence": 0.0,
                        "signal_level": "OUTLOOK",
                        "narrative": (pick.get("narratives") or [""])[0],
                        "run_up_id": None,
                        "geopolitical_net_score": 0.0,
                        "swarm_verdict": None,
                        "swarm_confidence": 0.0,
                    }
                candidates[t]["geopolitical_net_score"] = abs(pick.get("net_score", 0))
        except Exception:
            logger.exception("Failed to parse strategic outlook.")

    # --- 3. Enrich with swarm verdicts ---
    for t, c in candidates.items():
        if not c.get("run_up_id"):
            continue
        verdict = (
            session.query(SwarmVerdict)
            .filter(
                SwarmVerdict.run_up_id == c["run_up_id"],
                SwarmVerdict.superseded_at.is_(None),
            )
            .order_by(SwarmVerdict.created_at.desc())
            .first()
        )
        if verdict:
            c["swarm_verdict"] = verdict.verdict
            c["swarm_confidence"] = verdict.confidence or 0.0

    return candidates


def _score_insider_signal(ticker: str, session) -> float:
    """Score insider/finance signal from @QuiverQuant, @Insider_Trades,
    @unusual_whales, @NoLimitGains tweets in last 48h."""
    cutoff = datetime.utcnow() - timedelta(hours=48)
    insider_sources = [
        "X/Twitter - Quiver Quantitative",
        "X/Twitter - Insider Trade Alerts",
        "X/Twitter - Unusual Whales",
        "X/Twitter - NoLimit",
        "X/Twitter - Walter Bloomberg",
    ]

    count = 0
    for src in insider_sources:
        matches = (
            session.query(Article)
            .filter(
                Article.source == src,
                Article.pub_date >= cutoff,
                Article.title.ilike(f"%{ticker}%"),
            )
            .count()
        )
        count += matches

    return min(1.0, count / 3.0)


def _compute_composite_score(
    candidate: Dict[str, Any],
    momentum_data: Dict[str, Any],
    insider_score: float,
    weights: Dict[str, float],
) -> Tuple[float, Dict[str, float]]:
    """Compute weighted composite score and return (score, components)."""
    # 1. Geopolitical
    max_net = 20.0  # Typical max net_score from strategic_outlook
    geo_raw = candidate.get("geopolitical_net_score", 0.0)
    geo = min(1.0, geo_raw / max_net) if geo_raw > 0 else 0.0

    # 2. Confidence signal
    conf = candidate.get("signal_confidence", 0.0)

    # 3. Swarm sentiment — direction-aware: bullish verdict boosts bullish
    #    candidates, bearish verdict boosts bearish candidates.
    verdict = candidate.get("swarm_verdict")
    swarm_conf = candidate.get("swarm_confidence", 0.0)
    direction = candidate.get("direction", "bullish")
    if verdict and verdict in _VERDICT_POS:
        pos = _VERDICT_POS[verdict]
        # signed_strength: +1 for STRONG_BUY, -1 for STRONG_SELL, 0 for HOLD
        signed_strength = (pos - 0.5) * 2.0
        # If candidate is bearish, flip sign: a SELL verdict should boost
        if direction == "bearish":
            signed_strength = -signed_strength
        # Only positive alignment contributes; misalignment → 0 (not negative)
        swarm = max(0.0, signed_strength) * swarm_conf
    else:
        swarm = 0.0

    # 4. Momentum
    m = momentum_data.get(candidate["ticker"], {})
    sma = m.get("sma_signal", "neutral")
    vol = m.get("volume_trend", "normal")
    if direction == "bearish":
        mom = {"bullish": 0.2, "neutral": 0.5, "bearish": 0.8}.get(sma, 0.5)
    else:
        mom = {"bullish": 0.8, "neutral": 0.5, "bearish": 0.2}.get(sma, 0.5)
    if vol == "above":
        mom = min(1.0, mom + 0.2)

    # 5. Insider
    ins = insider_score

    components = {
        "geopolitical": round(geo, 4),
        "confidence": round(conf, 4),
        "swarm": round(swarm, 4),
        "momentum": round(mom, 4),
        "insider": round(ins, 4),
    }

    composite = sum(components[k] * weights.get(k, 0) for k in components)

    # Direction adjustment: if bearish, invert the score for sell-side ranking
    direction = candidate.get("direction", "bullish")
    if direction == "bearish":
        composite = -composite

    return round(composite, 4), components


# ======================================================================
# Market context
# ======================================================================

def _get_market_context() -> Dict[str, Any]:
    """Fetch market indicators for the advisory context section."""
    from .price_fetcher import get_price_fetcher
    pf = get_price_fetcher()

    fg = pf.get_fear_greed()
    indicators = pf.get_market_indicators()

    return {
        "fear_greed": {
            "score": fg.get("score", 50),
            "label": fg.get("label", "Neutral"),
        },
        "vix": {
            "price": (indicators.get("vix") or {}).get("price"),
            "change_pct": (indicators.get("vix") or {}).get("change_pct"),
        },
        "oil": {
            "price": (indicators.get("oil") or {}).get("price"),
            "change_pct": (indicators.get("oil") or {}).get("change_pct"),
        },
        "gold": {
            "price": (indicators.get("gold") or {}).get("price"),
            "change_pct": (indicators.get("gold") or {}).get("change_pct"),
        },
    }


def _determine_market_stance(
    context: Dict[str, Any],
    bullish_count: int,
    bearish_count: int,
) -> str:
    """Pure-Python market stance determination."""
    score = 0.0

    # Fear & Greed
    fg = context.get("fear_greed", {}).get("score", 50)
    score += (fg - 50) / 100.0  # -0.5 to +0.5

    # VIX
    vix = (context.get("vix") or {}).get("price") or 20
    if vix > 30:
        score -= 0.3
    elif vix > 25:
        score -= 0.15
    elif vix < 15:
        score += 0.2

    # Signal ratio
    total = max(bullish_count + bearish_count, 1)
    ratio = (bullish_count - bearish_count) / total
    score += ratio * 0.3

    if score > 0.3:
        return "strong_bullish"
    if score > 0.1:
        return "cautious_bullish"
    if score < -0.3:
        return "strong_bearish"
    if score < -0.1:
        return "cautious_bearish"
    return "neutral"


def _robust_json_parse(text: str) -> Dict:
    """Parse JSON robustly — handles extra data, truncated responses, etc."""
    import re

    # 1. Direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2. Find outermost { ... } via brace matching
    # First, find the first JSON start character (skip any preamble text)
    start = -1
    for i, ch in enumerate(text):
        if ch in ('{', '['):
            start = i
            break
    if start == -1:
        # No JSON start character found; skip to repair step
        pass
    else:
        open_ch = text[start]
        close_ch = '}' if open_ch == '{' else ']'
        depth = 0
        end_idx = 0
        in_string = False
        escape_next = False
        for i, ch in enumerate(text[start:], start):
            if escape_next:
                escape_next = False
                continue
            if ch == '\\' and in_string:
                escape_next = True
                continue
            if ch == '"' and not escape_next:
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break
        if end_idx:
            try:
                return json.loads(text[start:end_idx])
            except json.JSONDecodeError:
                pass

    # 3. Truncated JSON repair — close open strings, arrays, objects
    repair = text.rstrip()
    # Close any unterminated string
    quote_count = repair.count('"') - repair.count('\\"')
    if quote_count % 2 == 1:
        repair += '"'
    # Close open arrays and objects
    open_braces = repair.count('{') - repair.count('}')
    open_brackets = repair.count('[') - repair.count(']')
    repair += ']' * max(0, open_brackets)
    repair += '}' * max(0, open_braces)
    try:
        return json.loads(repair)
    except json.JSONDecodeError:
        pass

    # 4. Last resort: extract whatever valid JSON we can find
    # Try to find the first complete { ... } block
    match = re.search(r'\{[^{}]*\}', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    raise json.JSONDecodeError("Could not parse JSON from response", text, 0)


# ======================================================================
# Deep analysis narrative & swarm divergence enrichment (zero-cost)
# ======================================================================

def _get_latest_deep_narrative(session) -> Optional[Dict[str, Any]]:
    """Retrieve the latest deep analysis strategic narrative.

    The deep_analysis module generates a Claude narrative (world_direction,
    buy_opportunities, sell_signals, sectors_to_watch) twice daily. Previously
    this was only shown in the UI. Now we feed it into the advisory so Claude
    can build on the evening analysis when generating the morning advisory.
    """
    try:
        report = (
            session.query(AnalysisReport)
            .filter(AnalysisReport.report_type == "daily_briefing")
            .order_by(AnalysisReport.created_at.desc())
            .first()
        )
        if not report or not report.report_json:
            return None
        data = json.loads(report.report_json)
        narrative = data.get("strategic_narrative")
        if not narrative or not isinstance(narrative, dict):
            return None
        return {
            "world_direction": narrative.get("world_direction", ""),
            "buy_opportunities": narrative.get("buy_opportunities", [])[:5],
            "sell_signals": narrative.get("sell_signals", [])[:3],
            "sectors_to_watch": narrative.get("sectors_to_watch", [])[:4],
            "risk_warning": narrative.get("risk_warning", ""),
            "generated_at": data.get("generated_at", ""),
        }
    except Exception:
        logger.exception("Failed to load deep analysis narrative.")
        return None


def _get_swarm_divergence_risk(session) -> Optional[Dict[str, Any]]:
    """Extract rich risk indicators from recent swarm verdicts.

    Combines:
    1. Divergence: when experts disagree strongly (low consensus_strength)
    2. Debate insights: key arguments from round1/round2 transcripts
    3. Contrarian views: notable dissent notes
    4. Expert ticker signals: aggregated BUY/SELL votes per ticker

    Previously, the rich debate data from 27 LLM calls per node was collapsed
    to just 4 numbers. Now we extract actionable insights that enrich the
    advisory narrative.
    """
    try:
        # Get active (non-superseded) swarm verdicts from the last 24h
        cutoff = datetime.utcnow() - timedelta(hours=24)
        verdicts = (
            session.query(SwarmVerdict)
            .filter(
                SwarmVerdict.superseded_at.is_(None),
                SwarmVerdict.created_at >= cutoff,
            )
            .order_by(SwarmVerdict.created_at.desc())
            .limit(20)
            .all()
        )
        if not verdicts:
            return None

        # Identify high-divergence verdicts (experts strongly disagree)
        high_divergence = []
        dissent_notes = []
        # Aggregate ticker signals from ALL verdicts
        ticker_votes: Dict[str, Dict[str, int]] = {}  # ticker -> {long: N, short: M}
        # Extract key debate arguments from round transcripts
        key_arguments = []

        for v in verdicts:
            if v.consensus_strength < 0.5:
                node = v.decision_node
                narrative = ""
                if node and node.run_up:
                    narrative = node.run_up.narrative_name
                high_divergence.append({
                    "narrative": narrative,
                    "verdict": v.verdict,
                    "confidence": round(v.confidence, 2),
                    "consensus_strength": round(v.consensus_strength, 2),
                    "primary_ticker": v.primary_ticker,
                })
            if v.dissent_note and len(v.dissent_note) > 10:
                dissent_notes.append(v.dissent_note[:200])

            # Aggregate all_ticker_signals across verdicts
            try:
                signals = json.loads(v.all_ticker_signals_json or "[]")
                for sig in signals:
                    t = sig.get("ticker", "")
                    d = sig.get("direction", "long")
                    votes = sig.get("votes", 1)
                    if t:
                        entry = ticker_votes.setdefault(t, {"long": 0, "short": 0})
                        entry[d] = entry.get(d, 0) + votes
            except Exception:
                pass

            # Extract key assessments from round 2 (post-debate)
            try:
                r2 = json.loads(v.round2_json or "[]")
                for expert in r2:
                    resp = expert.get("r", {})
                    assessment = resp.get("revised_assessment", "")
                    if assessment and len(assessment) > 20:
                        name = expert.get("name", "Expert")
                        # Only keep unique, substantive assessments
                        if not any(assessment[:50] in ka.get("text", "") for ka in key_arguments):
                            key_arguments.append({
                                "expert": name,
                                "text": assessment[:250],
                                "action": resp.get("trading_action", "HOLD"),
                                "confidence": resp.get("confidence", 0),
                            })
            except Exception:
                pass

        if not high_divergence and not dissent_notes and not ticker_votes:
            return None

        avg_consensus = sum(v.consensus_strength for v in verdicts) / len(verdicts)

        # Sort ticker votes by conviction (total votes)
        sorted_tickers = sorted(
            ticker_votes.items(),
            key=lambda x: x[1]["long"] + x[1]["short"],
            reverse=True,
        )[:10]

        return {
            "avg_consensus": round(avg_consensus, 2),
            "high_divergence_count": len(high_divergence),
            "high_divergence_examples": high_divergence[:3],
            "notable_dissent": dissent_notes[:3],
            "total_verdicts": len(verdicts),
            "ticker_votes": {t: v for t, v in sorted_tickers},
            "key_arguments": sorted(
                key_arguments,
                key=lambda x: x["confidence"],
                reverse=True,
            )[:5],
        }
    except Exception:
        logger.exception("Failed to compute swarm divergence risk.")
        return None


# ======================================================================
# Claude narrative (optional, single Haiku call)
# ======================================================================

ADVISORY_SONNET_MODEL = "claude-sonnet-4-20250514"


def _select_advisory_model() -> str:
    """Select the best model for the daily advisory narrative.

    The advisory is the end product the user sees — it deserves the best model
    we can afford. Use Sonnet if budget allows (>50% remaining), Haiku otherwise.
    Cost: Sonnet ~€0.03 vs Haiku ~€0.008 per advisory.
    """
    try:
        from .tree_generator import get_daily_budget_eur, get_today_spending_eur
        budget = get_daily_budget_eur()
        spent = get_today_spending_eur()
        remaining = budget - spent
        # Use Sonnet if we have at least 50% budget remaining
        if remaining > budget * 0.5:
            logger.info(
                "Advisory narrative: using Sonnet (€%.3f remaining of €%.2f budget)",
                remaining, budget,
            )
            return ADVISORY_SONNET_MODEL
        else:
            logger.info(
                "Advisory narrative: using Haiku (budget tight: €%.3f remaining)",
                remaining,
            )
    except Exception:
        pass
    return config.tree_generator_model


def _generate_narrative(
    buy_recs: List[Dict],
    sell_recs: List[Dict],
    context: Dict[str, Any],
    stance: str,
) -> Optional[Dict[str, Any]]:
    """Generate advisory narrative via Claude (Sonnet if budget allows, else Haiku)."""
    try:
        from .tree_generator import _check_budget, _log_usage
        if not _check_budget():
            logger.info("Advisory narrative skipped — budget exhausted.")
            return None
    except Exception:
        return None

    api_key = config.anthropic_api_key
    if not api_key:
        return None

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except Exception:
        logger.exception("Failed to create Anthropic client.")
        return None

    def _fmt_rec(r: Dict, direction: str) -> str:
        c = r.get("components", {})
        ccy = r.get("currency", "USD")
        ccy_sym = {"EUR": "\u20ac", "USD": "$", "GBP": "\u00a3", "GBp": "\u00a3", "CHF": "CHF "}.get(ccy, ccy + " ")
        parts = [
            f"  Ticker: {r['ticker']} ({r['name']})",
            f"  Price: {ccy_sym}{r.get('current_price', '?')} ({ccy})",
            f"  Composite: {r['composite_score']:.3f} ({direction})",
            f"  Geopolitical narrative: {r.get('narrative', 'n/a')}",
            f"  Signal level: {r.get('signal_level', '?')}",
            f"  Swarm verdict: {r.get('swarm_verdict', '?')}",
            f"  Components: geo={c.get('geopolitical',0):.0%}, "
            f"conf={c.get('confidence',0):.0%}, "
            f"swarm={c.get('swarm',0):.0%}, "
            f"momentum={c.get('momentum',0):.0%}, "
            f"insider={c.get('insider',0):.0%}",
        ]
        return "\n".join(parts)

    buy_summary = "\n\n".join(_fmt_rec(r, "bullish") for r in buy_recs[:5])
    sell_summary = "\n\n".join(_fmt_rec(r, "bearish") for r in sell_recs[:3])
    fg = context.get("fear_greed", {})
    vix = context.get("vix", {})
    oil = context.get("oil", {})

    # Build deep analysis context block (from previous evening's analysis)
    deep_ctx = ""
    deep = context.get("deep_analysis_narrative")
    if deep:
        deep_ctx = f"""
=== OVERNIGHT STRATEGIC ANALYSIS (from deep analysis engine) ===
World direction: {deep.get('world_direction', 'N/A')}
Sectors to watch: {json.dumps(deep.get('sectors_to_watch', []), default=str)}
Risk assessment: {deep.get('risk_warning', 'N/A')}
Generated: {deep.get('generated_at', '?')}
NOTE: Build on this analysis — confirm or challenge the overnight view with today's data.
"""

    # Build swarm divergence context block (enriched with debate insights)
    divergence_ctx = ""
    div = context.get("swarm_divergence")
    if div:
        divergence_ctx = f"""
=== EXPERT PANEL INTELLIGENCE ({div.get('total_verdicts', 0)} verdicts, {div.get('avg_consensus', 0):.0%} avg consensus) ===
"""
        # Ticker vote aggregation from swarm
        votes = div.get("ticker_votes", {})
        if votes:
            divergence_ctx += "Expert Ticker Signals (aggregated from all swarm debates):\n"
            for ticker, v in list(votes.items())[:6]:
                net = v.get("long", 0) - v.get("short", 0)
                direction = "LONG" if net > 0 else "SHORT" if net < 0 else "MIXED"
                divergence_ctx += f"  {ticker}: {direction} ({v.get('long', 0)} long / {v.get('short', 0)} short votes)\n"

        # Key debate arguments
        args = div.get("key_arguments", [])
        if args:
            divergence_ctx += "\nKey Expert Arguments:\n"
            for a in args[:3]:
                divergence_ctx += f"  {a.get('expert', '?')} ({a.get('action', '?')}, conf {a.get('confidence', 0)}%): {a.get('text', '')[:150]}\n"

        # High-divergence warnings
        if div.get("high_divergence_count", 0) > 0:
            divergence_ctx += f"\n⚠ HIGH DIVERGENCE: {div['high_divergence_count']} narrative(s) with strong expert disagreement:\n"
            for ex in div.get("high_divergence_examples", [])[:2]:
                divergence_ctx += f"  - {ex.get('narrative', '?')}: {ex.get('verdict', '?')} (consensus only {ex.get('consensus_strength', 0):.0%})\n"

        # Contrarian dissent
        for dn in div.get("notable_dissent", [])[:2]:
            divergence_ctx += f"  Contrarian: {dn}\n"

        divergence_ctx += "Use the expert panel intelligence to strengthen your analysis. Where experts disagree, flag this in risk assessment.\n"

    prompt = f"""You are a senior geopolitical investment analyst advising a European retail
investor who trades on bunq Stocks (Tradegate/Xetra, EU exchanges).
Today is {datetime.now().strftime('%A %d %B %Y')}.

=== MARKET CONTEXT ===
Overall stance: {stance}
Fear & Greed Index: {fg.get('score', '?')} ({fg.get('label', '?')})
VIX: {vix.get('price', '?')} ({vix.get('change_pct', '?')}% change)
Oil (WTI): ${oil.get('price', '?')} ({oil.get('change_pct', '?')}% change)
{deep_ctx}{divergence_ctx}
=== BUY CANDIDATES ===
{buy_summary or '(none)'}

=== SELL CANDIDATES ===
{sell_summary or '(none)'}

=== INSTRUCTIONS ===
For EACH BUY, provide:
1. THESIS: The fundamental reason (geopolitical, structural, macro) — WHY this stock.
   Include non-obvious angles (e.g., if a company's production is outside a conflict zone,
   that's a structural advantage even when peers suffer).
2. CATALYST: What is happening RIGHT NOW that makes this urgent? What news/event?
3. TIMING: Best entry approach for Tradegate (opens 08:00 CET). Gap up expected?
   Wait for morning dip? Or buy at open?

For EACH SELL, provide:
1. THESIS: Why sell — what is the risk or the peak signal?
2. TIMING: Optimal sell window in CET.
   Consider: Tradegate opens 08:00 CET, US pre-market 10:00 CET, US open 15:30 CET.
   Sell-the-news gaps often correct after US open.
   Give a specific CET time window recommendation.
3. TARGET: What price level or signal would confirm it's time to sell?

Return STRICT JSON (no text before/after):
{{
  "reasoning": {{
    "<TICKER>": {{
      "thesis": "2-3 sentences on the fundamental investment thesis",
      "catalyst": "What is happening NOW that creates urgency",
      "timing": "Specific entry/exit advice with CET times",
      "risk": "Key risk to watch for this position"
    }}
  }},
  "narrative_summary": "3-4 sentence market outlook connecting geopolitics to portfolio action",
  "risk_warning": "1-2 sentence key risk to watch",
  "sectors_outlook": [
    {{"sector": "...", "direction": "bullish/bearish/mixed", "reasoning": "2-3 sentences with specific analysis"}}
  ]
}}
Only use tickers from the candidates above. Be specific and actionable.
Note: Prices shown are in the stock's native currency (USD for US equities,
EUR for Tradegate-listed). Stop-loss and take-profit levels should always
include the currency label (e.g. "$142.50" or "€128.30").
Write as if briefing a trader before market open."""

    model = _select_advisory_model()

    try:
        resp = client.messages.create(
            model=model,
            max_tokens=4000,
            temperature=0.3,
            system="You are a senior portfolio strategist generating actionable daily investment advisories in JSON format.",
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        # Robust markdown fence stripping (handles nested/mid-text fences)
        import re
        # Try extracting JSON from markdown code block first
        md_match = re.search(r'```(?:json)?\s*\n?(.*?)```', text, re.DOTALL)
        if md_match:
            text = md_match.group(1).strip()
        else:
            # Fallback: simple strip
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            if text.startswith("json"):
                text = text[4:]

        clean = text.strip()
        result = _robust_json_parse(clean)

        _log_usage(
            model=model,
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            purpose="daily_advisory",
        )
        return result
    except Exception:
        logger.exception("Advisory narrative generation failed.")
        return None


def _derive_sectors_outlook(
    buy_recs: list, sell_recs: list
) -> list:
    """Build sectors_outlook from advisory recommendations when Claude fails.

    Maps buy/sell tickers to sectors and infers sector direction.
    """
    TICKER_SECTORS = {
        # Energy
        "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "SLB": "Energy",
        "EOG": "Energy", "MPC": "Energy", "VLO": "Energy", "PSX": "Energy",
        "OXY": "Energy", "HAL": "Energy", "TTE.PA": "Energy",
        "BP.L": "Energy", "SHEL": "Energy", "IS0D.DE": "Energy",
        # Mining & Materials (NOT all precious metals)
        "FCX": "Mining", "SCCO": "Mining",             # copper miners
        "GLEN.L": "Mining", "ANTO.L": "Mining",        # diversified / copper
        "TECK": "Mining", "ALB": "Lithium",            # zinc-copper / lithium
        "RIO": "Mining", "BHP": "Mining",              # diversified mining
        # Defense
        "LMT": "Defense & Aerospace", "RTX": "Defense & Aerospace",
        "NOC": "Defense & Aerospace", "GD": "Defense & Aerospace",
        "LHX": "Defense & Aerospace", "BA": "Defense & Aerospace",
        "AIR.PA": "Defense & Aerospace",
        # Technology
        "NVDA": "Technology", "AMD": "Technology", "INTC": "Technology",
        "AVGO": "Technology", "QCOM": "Technology", "AAPL": "Technology",
        "MSFT": "Technology", "GOOGL": "Technology", "META": "Technology",
        "CRM": "Technology", "ORCL": "Technology", "ADBE": "Technology",
        "ASML": "Technology",
        "SAP": "Technology", "PLTR": "Technology",
        # Emerging Markets
        "IEMA.AS": "Emerging Markets",
    }

    # Count direction per sector
    sector_scores: dict = {}
    for r in buy_recs:
        sector = TICKER_SECTORS.get(r["ticker"])
        if sector:
            entry = sector_scores.setdefault(sector, {"bull": 0, "bear": 0, "tickers": []})
            entry["bull"] += 1
            entry["tickers"].append(r["ticker"])

    for r in sell_recs:
        sector = TICKER_SECTORS.get(r["ticker"])
        if sector:
            entry = sector_scores.setdefault(sector, {"bull": 0, "bear": 0, "tickers": []})
            entry["bear"] += 1
            entry["tickers"].append(r["ticker"])

    result = []
    for sector, data in sector_scores.items():
        direction = "bullish" if data["bull"] >= data["bear"] else "bearish"
        tickers_str = ", ".join(data["tickers"])
        result.append({
            "sector": sector,
            "direction": direction,
            "reasoning": f"Based on {data['bull']} buy and {data['bear']} sell signals ({tickers_str}).",
        })

    return result


# ======================================================================
# Sector diversification
# ======================================================================

# Map tickers → GICS-style sectors for concentration control
_TICKER_TO_SECTOR: Dict[str, str] = {
    # Energy
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "SLB": "Energy",
    "EOG": "Energy", "MPC": "Energy", "VLO": "Energy", "PSX": "Energy",
    "OXY": "Energy", "HAL": "Energy", "TTE.PA": "Energy",
    "BP.L": "Energy", "SHEL": "Energy", "IS0D.DE": "Energy",
    # Precious Metals / Mining
    "FCX": "Mining", "SCCO": "Mining", "GLEN.L": "Mining", "ANTO.L": "Mining",
    "TECK": "Mining", "ALB": "Mining", "RIO": "Mining", "BHP": "Mining",
    "WMIN.DE": "Mining", "IS0E.DE": "Mining",
    # Defense
    "LMT": "Defense", "RTX": "Defense", "NOC": "Defense", "GD": "Defense",
    "LHX": "Defense", "BA": "Defense", "AIR.PA": "Defense",
    # Technology
    "NVDA": "Technology", "AMD": "Technology", "INTC": "Technology",
    "AVGO": "Technology", "QCOM": "Technology", "AAPL": "Technology",
    "MSFT": "Technology", "GOOGL": "Technology", "META": "Technology",
    "CRM": "Technology", "ORCL": "Technology", "ADBE": "Technology",
    "ASML": "Technology",
    "SAP": "Technology", "PLTR": "Technology",
    # Emerging Markets
    "IEMA.AS": "EM",
    # Financials
    "JPM": "Financials", "GS": "Financials", "MS": "Financials",
    "BAC": "Financials", "C": "Financials",
    # V and MA are GICS Information Technology (Transaction Processing)
    "V": "Technology", "MA": "Technology",
    # Healthcare
    "JNJ": "Healthcare", "UNH": "Healthcare", "PFE": "Healthcare",
    "ABBV": "Healthcare", "LLY": "Healthcare", "MRK": "Healthcare",
    # Consumer Discretionary
    "AMZN": "Consumer Discretionary", "TSLA": "Automotive/EV",
    "NKE": "Consumer Discretionary", "DIS": "Consumer Discretionary",
    "MCD": "Consumer Discretionary",
    # Broad Market
    "ISPA.DE": "BroadMarket", "IWDA.AS": "BroadMarket",
    "CSPX.AS": "BroadMarket", "VWRL.AS": "BroadMarket",
}


def _diversify_by_sector(
    candidates: List[Dict[str, Any]],
    max_picks: int = 5,
    max_per_sector: int = 2,
) -> List[Dict[str, Any]]:
    """Select top candidates while enforcing sector concentration limits.

    Prevents the advisory from recommending e.g. 5 oil stocks when there's
    a single geopolitical narrative driving them all.

    Candidates must be pre-sorted by composite_score descending.
    """
    picks: List[Dict[str, Any]] = []
    sector_counts: Dict[str, int] = {}

    for cand in candidates:
        if len(picks) >= max_picks:
            break
        ticker = cand["ticker"]
        sector = _TICKER_TO_SECTOR.get(ticker, "Other")
        count = sector_counts.get(sector, 0)
        if count >= max_per_sector:
            continue  # Skip — sector already has max_per_sector picks
        picks.append(cand)
        sector_counts[sector] = count + 1

    return picks


# ======================================================================
# Position sizing (Half-Kelly criterion)
# ======================================================================

def _compute_atr(ticker: str, period: int = 14) -> Optional[float]:
    """Compute Average True Range for a ticker over `period` days.

    ATR measures daily volatility — used for stop-loss/take-profit sizing
    and Kelly criterion edge estimation.  Returns None if insufficient data.
    """
    try:
        from .price_fetcher import get_price_fetcher
        pf = get_price_fetcher()
        candles = pf.get_chart_data(ticker, period="3mo")
        if len(candles) < period + 1:
            return None

        true_ranges = []
        for i in range(1, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i - 1]["close"]
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
            true_ranges.append(tr)

        # Exponential ATR (Wilder's smoothing)
        atr = sum(true_ranges[:period]) / period
        for tr in true_ranges[period:]:
            atr = (atr * (period - 1) + tr) / period

        return round(atr, 4)
    except Exception:
        logger.debug("ATR calculation failed for %s", ticker)
        return None


def _compute_position_sizing(
    rec: Dict[str, Any],
    atr: Optional[float],
    vix_price: Optional[float],
    portfolio_value: float = DEFAULT_PORTFOLIO_EUR,
) -> Dict[str, Any]:
    """Compute position size using Half-Kelly criterion.

    Kelly fraction f* = (bp − q) / b  where:
      p = win probability (derived from composite_score)
      q = 1 − p
      b = reward/risk ratio (take-profit / stop-loss distance)

    We use half-Kelly for conservative retail sizing, then clamp to
    sensible min/max bounds.  High VIX → reduce position further.
    """
    price = rec.get("current_price")
    score = abs(rec.get("composite_score", 0))
    if not price or price <= 0:
        return {"position_pct": MIN_POSITION_PCT, "shares": 0, "eur_amount": 0,
                "method": "fallback_no_price"}

    # Win probability from composite score (clamp 0.35–0.80)
    win_prob = max(0.35, min(0.80, 0.5 + score * 0.4))

    # Reward/risk ratio from ATR (default 1.5:1 if no ATR)
    if atr and atr > 0:
        risk_per_share = atr * STOP_LOSS_ATR_MULT
        reward_per_share = atr * TAKE_PROFIT_ATR_MULT
        b = reward_per_share / risk_per_share if risk_per_share > 0 else 1.5
    else:
        b = 1.5  # Default R:R

    # Kelly formula
    q = 1.0 - win_prob
    kelly = (b * win_prob - q) / b if b > 0 else 0
    kelly = max(0, kelly)

    # No edge → no position (don't override with MIN_POSITION_PCT)
    if kelly <= 0:
        return {"position_pct": 0, "shares": 0, "eur_amount": 0,
                "kelly_raw": 0, "kelly_half": 0, "method": "kelly_no_edge"}

    # Half-Kelly (conservative)
    half_kelly = kelly * KELLY_FRACTION

    # VIX adjustment: reduce sizing when VIX is elevated
    vix = vix_price or 20
    if vix > 30:
        half_kelly *= 0.5   # Extreme fear → halve position
    elif vix > 25:
        half_kelly *= 0.7   # High fear → reduce 30%
    elif vix < 15:
        half_kelly *= 1.1   # Complacency → slightly larger

    # Convert to portfolio percentage and clamp
    position_pct = round(max(MIN_POSITION_PCT, min(MAX_POSITION_PCT, half_kelly * 100)), 1)
    eur_amount = round(portfolio_value * position_pct / 100, 2)

    # Convert price to EUR for share calculation (Tradegate quotes in EUR)
    price_eur = price
    currency = rec.get("currency", "EUR")
    if currency == "USD" and price > 0:
        try:
            from .price_fetcher import get_price_fetcher
            _pf = get_price_fetcher()
            fx_quote = _pf.get_quote("EURUSD=X")
            if "error" not in fx_quote and fx_quote.get("price", 0) > 0:
                price_eur = price / fx_quote["price"]  # USD price / EURUSD rate = EUR price
        except Exception:
            price_eur = price / 1.08  # Fallback estimate
    elif currency == "GBp" and price > 0:
        # GBp = pence sterling (yfinance returns this for .L stocks)
        # Convert pence → pounds → EUR
        price_gbp = price / 100.0
        try:
            from .price_fetcher import get_price_fetcher
            _pf = get_price_fetcher()
            fx_quote = _pf.get_quote("GBPEUR=X")
            if "error" not in fx_quote and fx_quote.get("price", 0) > 0:
                price_eur = price_gbp * fx_quote["price"]
            else:
                price_eur = price_gbp * 1.17  # Fallback
        except Exception:
            price_eur = price_gbp * 1.17
    elif currency == "GBP" and price > 0:
        try:
            from .price_fetcher import get_price_fetcher
            _pf = get_price_fetcher()
            fx_quote = _pf.get_quote("GBPEUR=X")
            if "error" not in fx_quote and fx_quote.get("price", 0) > 0:
                price_eur = price * fx_quote["price"]
            else:
                price_eur = price * 1.17
        except Exception:
            price_eur = price * 1.17
    elif currency == "CHF" and price > 0:
        try:
            from .price_fetcher import get_price_fetcher
            _pf = get_price_fetcher()
            fx_quote = _pf.get_quote("CHFEUR=X")
            if "error" not in fx_quote and fx_quote.get("price", 0) > 0:
                price_eur = price * fx_quote["price"]
            else:
                price_eur = price * 1.06  # Fallback
        except Exception:
            price_eur = price * 1.06

    shares = int(eur_amount / price_eur) if price_eur > 0 else 0

    return {
        "position_pct": position_pct,
        "shares": shares,
        "eur_amount": round(eur_amount, 2),
        "kelly_raw": round(kelly, 4),
        "kelly_half": round(half_kelly, 4),
        "win_prob": round(win_prob, 3),
        "reward_risk_ratio": round(b, 2),
        "vix_adjustment": round(vix, 1),
        "method": "half_kelly_atr",
    }


# ======================================================================
# Stop-loss / Take-profit levels (ATR-based)
# ======================================================================

def _compute_risk_levels(
    rec: Dict[str, Any],
    atr: Optional[float],
    vix_price: Optional[float],
) -> Dict[str, Any]:
    """Compute stop-loss, take-profit, and trailing stop levels.

    Uses ATR (Average True Range) for volatility-adjusted levels.
    Higher VIX → wider stops to avoid whipsaws.
    Falls back to percentage-based levels if ATR is unavailable.

    Note: prices are in the stock's native currency (typically USD for US
    equities via yfinance, EUR for Tradegate-listed).  The ``currency``
    field in the returned dict indicates which currency the levels are
    denominated in so downstream consumers can convert or label correctly.
    """
    price = rec.get("current_price")
    action = rec.get("action", "BUY")
    currency = rec.get("currency", "USD")  # yfinance default is USD
    if not price or price <= 0:
        return {}

    vix = vix_price or 20

    # VIX-adjusted ATR multipliers: widen in volatile markets
    sl_mult = STOP_LOSS_ATR_MULT
    tp_mult = TAKE_PROFIT_ATR_MULT
    ts_mult = TRAILING_STOP_ATR
    if vix > 30:
        sl_mult *= 1.4
        tp_mult *= 1.3
        ts_mult *= 1.3
    elif vix > 25:
        sl_mult *= 1.2
        tp_mult *= 1.15
        ts_mult *= 1.15

    if atr and atr > 0:
        # ATR-based levels
        if action == "BUY":
            stop_loss = round(price - atr * sl_mult, 2)
            take_profit = round(price + atr * tp_mult, 2)
            trailing_stop = round(price - atr * ts_mult, 2)
        else:  # SELL
            stop_loss = round(price + atr * sl_mult, 2)
            take_profit = round(price - atr * tp_mult, 2)
            trailing_stop = round(price + atr * ts_mult, 2)

        risk_pct = round(abs(price - stop_loss) / price * 100, 1)
        reward_pct = round(abs(take_profit - price) / price * 100, 1)

        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop": trailing_stop,
            "atr": round(atr, 2),
            "atr_pct": round(atr / price * 100, 2),
            "risk_pct": risk_pct,
            "reward_pct": reward_pct,
            "reward_risk": round(reward_pct / risk_pct, 2) if risk_pct > 0 else 0,
            "currency": currency,
            "method": "atr_based",
        }
    else:
        # Fallback: percentage-based (3% SL, 5% TP)
        sl_pct = 3.0 if vix < 25 else 4.5
        tp_pct = 5.0 if vix < 25 else 6.5

        if action == "BUY":
            stop_loss = round(price * (1 - sl_pct / 100), 2)
            take_profit = round(price * (1 + tp_pct / 100), 2)
        else:
            stop_loss = round(price * (1 + sl_pct / 100), 2)
            take_profit = round(price * (1 - tp_pct / 100), 2)

        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop": None,
            "atr": None,
            "risk_pct": sl_pct,
            "reward_pct": tp_pct,
            "reward_risk": round(tp_pct / sl_pct, 2),
            "currency": currency,
            "method": "pct_fallback",
        }


# ======================================================================
# Main advisory generation
# ======================================================================

def generate_daily_advisory() -> Optional[AnalysisReport]:
    """Generate today's daily investment advisory.

    Returns the saved AnalysisReport or None on failure.
    """
    logger.info("Generating daily advisory...")
    session = get_session()

    try:
        # 1. Load learned weights
        weights = _load_weights()
        logger.info("Advisory weights: %s", weights)

        # 2. Collect candidate tickers
        candidates = _collect_candidates(session)
        if not candidates:
            logger.warning("No advisory candidates found.")
            return None
        logger.info("Advisory candidates: %d tickers", len(candidates))

        # 3. Fetch momentum for all candidate tickers
        from .price_fetcher import get_price_fetcher
        pf = get_price_fetcher()
        tickers = list(candidates.keys())
        momentum = pf.get_ticker_momentum(tickers, max_tickers=len(tickers))

        # 4. Score each candidate
        scored: List[Dict[str, Any]] = []
        for t, cand in candidates.items():
            insider = _score_insider_signal(t, session)
            composite, components = _compute_composite_score(
                cand, momentum, insider, weights
            )
            # Fetch current price
            quote = pf.get_quote(t)
            price = quote.get("price") if "error" not in quote else None
            currency = quote.get("currency", "EUR") if "error" not in quote else "EUR"

            scored.append({
                "ticker": t,
                "name": cand["name"],
                "direction": cand["direction"],
                "composite_score": composite,
                "components": components,
                "current_price": price,
                "currency": currency,
                "narrative": cand.get("narrative", ""),
                "signal_level": cand.get("signal_level", ""),
                "swarm_verdict": cand.get("swarm_verdict"),
                "run_up_id": cand.get("run_up_id"),
            })

        # 5. Split into BUY and SELL
        bullish = sorted(
            [s for s in scored if s["composite_score"] > 0],
            key=lambda x: x["composite_score"],
            reverse=True,
        )
        bearish = sorted(
            [s for s in scored if s["composite_score"] < 0],
            key=lambda x: x["composite_score"],
        )

        buy_recs = _diversify_by_sector(bullish, max_picks=5, max_per_sector=2)
        sell_recs = bearish[:3]
        # Remaining notable tickers → watchlist (exclude already-picked buys)
        buy_tickers_set = {r["ticker"] for r in buy_recs}
        watch_tickers = [
            s for s in bullish
            if s["ticker"] not in buy_tickers_set and s["composite_score"] > 0.3
        ][:5]

        # 6. Market context & stance
        context = _get_market_context()
        stance = _determine_market_stance(context, len(bullish), len(bearish))

        # 6b. Enrich context with deep analysis narrative + swarm divergence
        deep_narrative = _get_latest_deep_narrative(session)
        if deep_narrative:
            context["deep_analysis_narrative"] = deep_narrative
        swarm_risk = _get_swarm_divergence_risk(session)
        if swarm_risk:
            context["swarm_divergence"] = swarm_risk

        # 6c. Compute ATR + position sizing + risk levels for all recs
        vix_price = (context.get("vix") or {}).get("price")
        portfolio_val = _get_portfolio_value(session)
        atr_cache: Dict[str, Optional[float]] = {}
        for rec in buy_recs + sell_recs:
            t = rec["ticker"]
            if t not in atr_cache:
                atr_cache[t] = _compute_atr(t)
            atr = atr_cache[t]
            rec["action"] = "BUY" if rec["composite_score"] > 0 else "SELL"
            rec["risk_levels"] = _compute_risk_levels(rec, atr, vix_price)
            rec["position_sizing"] = _compute_position_sizing(
                rec, atr, vix_price, portfolio_val
            )

        # 7. Claude narrative (optional)
        narrative_data = _generate_narrative(buy_recs, sell_recs, context, stance)
        reasoning_map = (narrative_data or {}).get("reasoning", {})

        # Attach reasoning to recs — new format is dict with thesis/catalyst/timing/risk
        for rec in buy_recs + sell_recs:
            claude_reasoning = reasoning_map.get(rec["ticker"])
            if claude_reasoning and isinstance(claude_reasoning, dict):
                # New rich format: keep the full dict
                rec["reasoning"] = claude_reasoning
            elif claude_reasoning and isinstance(claude_reasoning, str):
                # Old string format: wrap in dict for consistency
                rec["reasoning"] = {"thesis": claude_reasoning}
            else:
                rec["reasoning"] = {"thesis": _fallback_reasoning(rec)}

        # 8. Assemble advisory JSON
        advisory_data = {
            "version": 3,
            "generated_at": datetime.utcnow().isoformat(),
            "market_stance": stance,
            "market_context": context,
            "weights_used": weights,
            "buy_recommendations": [
                {
                    "rank": i + 1,
                    "ticker": r["ticker"],
                    "name": r["name"],
                    "action": "BUY",
                    "composite_score": r["composite_score"],
                    "current_price": r["current_price"],
                    "components": r["components"],
                    "narrative": r["narrative"],
                    "reasoning": r["reasoning"],
                    "signal_level": r["signal_level"],
                    "swarm_verdict": r["swarm_verdict"],
                    "risk_levels": r.get("risk_levels", {}),
                    "position_sizing": r.get("position_sizing", {}),
                }
                for i, r in enumerate(buy_recs)
            ],
            "sell_recommendations": [
                {
                    "rank": i + 1,
                    "ticker": r["ticker"],
                    "name": r["name"],
                    "action": "SELL",
                    "composite_score": abs(r["composite_score"]),
                    "current_price": r["current_price"],
                    "components": r["components"],
                    "narrative": r["narrative"],
                    "reasoning": r["reasoning"],
                    "signal_level": r["signal_level"],
                    "swarm_verdict": r["swarm_verdict"],
                    "risk_levels": r.get("risk_levels", {}),
                    "position_sizing": r.get("position_sizing", {}),
                }
                for i, r in enumerate(sell_recs)
            ],
            "hold_watchlist": [
                {"ticker": w["ticker"], "name": w["name"],
                 "note": f"Score {w['composite_score']:.2f} — monitor {w['narrative']}"}
                for w in watch_tickers
            ],
            "sectors_outlook": (narrative_data or {}).get("sectors_outlook", [])
                or _derive_sectors_outlook(buy_recs, sell_recs),
            "risk_warning": (narrative_data or {}).get(
                "risk_warning",
                "Market conditions are volatile. Always use stop-losses.",
            ),
            "narrative_summary": (narrative_data or {}).get(
                "narrative_summary",
                f"Market stance: {stance}. {len(buy_recs)} buy opportunities, "
                f"{len(sell_recs)} sell signals identified.",
            ),
            "sources_used": {
                "active_signals": len([s for s in scored if s["signal_level"] != "OUTLOOK"]),
                "outlook_picks": len([s for s in scored if s["signal_level"] == "OUTLOOK"]),
                "total_candidates": len(scored),
                "deep_analysis_used": bool(deep_narrative),
            },
            "swarm_divergence": swarm_risk,
            # Outcomes will be filled by evaluate_open_advisories()
            "outcomes": {},
        }

        # 8b. Portfolio context — annotate how advisory relates to holdings
        portfolio_ctx = _build_portfolio_context(
            advisory_data, session
        )
        if portfolio_ctx:
            advisory_data["portfolio_context"] = portfolio_ctx

        # 9. Save as AnalysisReport
        report = AnalysisReport(
            report_type="daily_advisory",
            period_start=date.today(),
            period_end=date.today(),
            report_json=json.dumps(advisory_data),
        )
        session.add(report)
        session.commit()

        logger.info(
            "Daily advisory generated: %d BUY, %d SELL, stance=%s (report %d)",
            len(buy_recs), len(sell_recs), stance, report.id,
        )
        return report

    except Exception:
        logger.exception("Daily advisory generation failed.")
        session.rollback()
        return None
    finally:
        session.close()


def _get_portfolio_value(session) -> float:
    """Get total portfolio value from configured holdings using LIVE prices, or default."""
    try:
        s = session.query(EngineSettings).get("portfolio_holdings")
        if s and s.value:
            holdings = json.loads(s.value)
            if not holdings:
                return DEFAULT_PORTFOLIO_EUR
            from .price_fetcher import get_price_fetcher
            pf = get_price_fetcher()
            total = 0.0
            for h in holdings:
                shares = float(h.get("shares", 0))
                if shares > 0:
                    quote = pf.get_quote(h["ticker"])
                    if "error" not in quote:
                        price_eur = pf.convert_to_eur(quote["price"], quote.get("currency", "EUR"))
                        total += shares * price_eur
                    else:
                        total += shares * float(h.get("avg_buy_price_eur", 0))
                elif h.get("value_eur", 0) > 0:
                    total += h["value_eur"]  # legacy format
            if total > 0:
                return total
    except Exception:
        pass
    return DEFAULT_PORTFOLIO_EUR


def _build_portfolio_context(
    advisory: Dict[str, Any],
    session,
) -> Optional[Dict[str, Any]]:
    """Build portfolio-aware context if holdings are configured."""
    try:
        s = session.query(EngineSettings).get("portfolio_holdings")
        if not s or not s.value:
            return None
        holdings = json.loads(s.value)
        if not holdings:
            return None
    except Exception:
        return None

    # ── Holding intelligence: ETF exposure risks & better alternatives ──
    # When a holding's underlying exposure creates risk that individual
    # stocks can avoid, the advisory should flag this.
    HOLDING_INTEL = {
        "IS0D.DE": {
            "description": "iShares Oil & Gas E&P UCITS — heavy Middle East & OPEC exposure",
            "risk_factor": "middle_east",
            "risk_narratives": ["iran", "hormuz", "middle-east", "opec", "gulf"],
            "better_alternatives": ["XOM", "CVX"],
            "alt_reason": "XOM/CVX produceren voornamelijk in Americas, buiten conflict zone. "
                          "Zelfde energie-exposure maar zonder Midden-Oosten risico.",
        },
    }

    buy_tickers = {r["ticker"] for r in advisory.get("buy_recommendations", [])}
    sell_tickers = {r["ticker"] for r in advisory.get("sell_recommendations", [])}
    buy_map = {r["ticker"]: r for r in advisory.get("buy_recommendations", [])}
    sell_map = {r["ticker"]: r for r in advisory.get("sell_recommendations", [])}

    # Collect active narrative slugs for risk matching
    active_narratives = set()
    for rec in advisory.get("buy_recommendations", []) + advisory.get("sell_recommendations", []):
        n = (rec.get("narrative") or "").lower()
        active_narratives.add(n)

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

    total_value = sum(h.get("value_eur", 0) for h in holdings)
    actions = []

    for h in holdings:
        t = h["ticker"]
        val = h.get("value_eur", 0)
        pct = round(val / total_value * 100, 1) if total_value > 0 else 0

        # ── Check holding intelligence for risk overrides ──
        intel = HOLDING_INTEL.get(t)
        if intel:
            risk_triggered = any(
                any(rn in narrative for rn in intel["risk_narratives"])
                for narrative in active_narratives
            )
            if risk_triggered:
                alts = intel.get("better_alternatives", [])
                alts_in_buy = [a for a in alts if a in buy_tickers]
                alt_str = ", ".join(alts_in_buy) if alts_in_buy else ", ".join(alts)
                actions.append({
                    "ticker": t,
                    "name": h.get("name", t),
                    "value_eur": val,
                    "portfolio_pct": pct,
                    "action": "REDUCE",
                    "label": f"Afbouwen — switch naar {alt_str}",
                    "reasoning": intel["alt_reason"],
                    "alternatives": alts,
                })
                continue

        if t in buy_tickers:
            rec = buy_map[t]
            actions.append({
                "ticker": t,
                "name": h.get("name", t),
                "value_eur": val,
                "portfolio_pct": pct,
                "action": "HOLD_ADD",
                "label": "Aanhouden + bijkopen",
                "score": rec.get("composite_score"),
                "reasoning": rec.get("reasoning", ""),
            })
        elif t in sell_tickers:
            rec = sell_map[t]
            actions.append({
                "ticker": t,
                "name": h.get("name", t),
                "value_eur": val,
                "portfolio_pct": pct,
                "action": "REDUCE",
                "label": "Afbouwen",
                "score": rec.get("composite_score"),
                "reasoning": rec.get("reasoning", ""),
            })
        else:
            # Check sector alignment
            _sector_match = _match_sectors(
                t, h.get("name", ""),
                advisory.get("sectors_outlook", []),
            )
            if _sector_match:
                direction = _sector_match.get("direction", "neutral")
                actions.append({
                    "ticker": t,
                    "name": h.get("name", t),
                    "value_eur": val,
                    "portfolio_pct": pct,
                    "action": "HOLD" if direction == "bullish" else "WATCH",
                    "label": f"Aanhouden — {_sector_match['sector']} {direction}"
                        if direction == "bullish"
                        else f"Monitoren — {_sector_match['sector']} {direction}",
                    "reasoning": _sector_match.get("reasoning", ""),
                })
            else:
                # Try to identify the holding's sector even without
                # a matching outlook, so we show a meaningful label.
                detected_sector = _detect_holding_sector(t, h.get("name", ""))
                if detected_sector:
                    actions.append({
                        "ticker": t,
                        "name": h.get("name", t),
                        "value_eur": val,
                        "portfolio_pct": pct,
                        "action": "HOLD",
                        "label": f"Aanhouden — {detected_sector}",
                        "reasoning": "Geen actief signaal voor deze sector in huidig advies.",
                    })
                else:
                    actions.append({
                        "ticker": t,
                        "name": h.get("name", t),
                        "value_eur": val,
                        "portfolio_pct": pct,
                        "action": "NEUTRAL",
                        "label": "Geen signaal",
                        "reasoning": "Niet in huidige advisory",
                    })

    # Missed: BUY recommendations not in portfolio
    held_tickers = {h["ticker"] for h in holdings}
    missed = [
        {"ticker": r["ticker"], "name": r["name"], "score": r["composite_score"]}
        for r in advisory.get("buy_recommendations", [])
        if r["ticker"] not in held_tickers
    ]

    return {
        "total_value": round(total_value, 2),
        "holdings_count": len(holdings),
        "actions": actions,
        "missed_buys": missed,
    }


def _detect_holding_sector(ticker: str, name: str) -> Optional[str]:
    """Detect the sector of a holding based on its ticker/name, independent of
    the sectors_outlook. Returns a human-readable sector label or None."""
    lower = (name + " " + ticker).lower()
    SECTOR_MAP = [
        ("Energy / Oil & Gas", ["oil", "gas", "energy", "petroleum", "crude",
                                 "is0d", "xle", "exxon", "xom"]),
        ("Mining & Grondstoffen", ["mining", "miner", "mineral", "copper",
                                    "wmin", "vaneck", "glen", "bhp", "rio",
                                    "teck", "fcx", "scco"]),
        ("Goud & Edelmetalen", ["gold", "silver", "precious", "producer",
                                 "is0e", "gdx"]),
        ("Dividend", ["dividend", "select dividend", "stoxx", "ispa", "vhyl",
                       "yield", "income"]),
        ("Defensie", ["defense", "defence", "aerospace", "military"]),
        ("Technologie", ["tech", "software", "semiconductor"]),
    ]
    for sector_label, keywords in SECTOR_MAP:
        if any(kw in lower for kw in keywords):
            return sector_label
    return None


def _match_sectors(ticker: str, name: str, sectors: list) -> Optional[Dict]:
    """Match a ticker/name to the best sector outlook."""
    lower = (name + " " + ticker).lower()
    KEYWORDS = {
        "Energy": ["oil", "gas", "energy", "petroleum", "crude", "fuel",
                    "xle", "uso", "xop", "is0d", "exploration", "exxon",
                    "xom", "cvx", "cop", "slb"],
        "Precious Metals": ["gold", "silver", "precious", "mining", "metal", "copper",
                            "gld", "slv", "gdx", "ring", "pick", "is0e",
                            "wmin", "vaneck", "producer", "miner", "mineral"],
        "Defense & Aerospace": ["defense", "defence", "aerospace", "military",
                                "lmt", "rtx", "noc", "ita"],
        "Emerging Markets": ["emerging", "eem", "iema", "developing"],
        "Credit": ["bond", "yield", "credit", "hyg", "tlt", "high yield",
                    "dividend", "ispa", "select dividend", "stoxx"],
        "Technology": ["tech", "software", "semiconductor", "xlk", "qqq"],
    }
    for s in sectors:
        sector_name = s.get("sector", "")
        # Match sector name flexibly — Claude may return "Energy (XOM, XLE)"
        kws = []
        for key, kwlist in KEYWORDS.items():
            if sector_name.lower().startswith(key.lower()):
                kws = kwlist
                break
        if any(kw in lower for kw in kws):
            return s
    return None


def _fallback_reasoning(rec: Dict[str, Any]) -> str:
    """Template-based reasoning when Claude is unavailable."""
    c = rec.get("components", {})
    parts = []
    if c.get("geopolitical", 0) > 0.5:
        parts.append(f"strong geopolitical signal ({c['geopolitical']:.0%})")
    if c.get("swarm", 0) > 0.5:
        parts.append(f"swarm consensus {rec.get('swarm_verdict', 'positive')}")
    if c.get("momentum", 0) > 0.6:
        parts.append("bullish price momentum")
    if c.get("insider", 0) > 0.3:
        parts.append("insider trading activity detected")
    if c.get("confidence", 0) > 0.6:
        parts.append(f"confidence signal at {c['confidence']:.0%}")

    direction = rec.get("direction", "bullish")
    if not parts:
        return f"Composite score {rec['composite_score']:.2f} — {direction} outlook."
    return f"{direction.capitalize()} outlook driven by {', '.join(parts)}."


# ======================================================================
# Multi-horizon outcome evaluation
# ======================================================================

def evaluate_open_advisories() -> Dict[str, Any]:
    """Evaluate ALL open advisories at all horizons.

    For each past advisory, check which horizons (T+1d, T+3d, ..., T+30d)
    have now elapsed and record the actual price vs entry price.
    Updates per-component EMA accuracy scores.

    Returns summary stats.
    """
    logger.info("Evaluating open advisories across all horizons...")
    session = get_session()
    from .price_fetcher import get_price_fetcher
    pf = get_price_fetcher()

    total_checks = 0
    total_hits = 0
    emas = _load_component_emas()

    try:
        # Get all advisories from last 35 days (covers T+30d window)
        cutoff = datetime.utcnow() - timedelta(days=35)
        advisories = (
            session.query(AnalysisReport)
            .filter(
                AnalysisReport.report_type == "daily_advisory",
                AnalysisReport.created_at >= cutoff,
            )
            .order_by(AnalysisReport.created_at.asc())
            .all()
        )

        for report in advisories:
            try:
                data = json.loads(report.report_json)
            except Exception:
                continue

            generated_at = datetime.fromisoformat(data.get("generated_at", ""))
            outcomes = data.get("outcomes", {})
            changed = False

            all_recs = (
                data.get("buy_recommendations", [])
                + data.get("sell_recommendations", [])
            )

            for rec in all_recs:
                ticker = rec.get("ticker")
                entry_price = rec.get("current_price")
                if not ticker or not entry_price:
                    continue

                action = rec.get("action", "BUY")
                ticker_outcomes = outcomes.setdefault(ticker, {})
                components = rec.get("components", {})

                for horizon in EVAL_HORIZONS:
                    h_key = f"T+{horizon}d"
                    if h_key in ticker_outcomes:
                        continue  # Already evaluated

                    target_dt = generated_at + timedelta(days=horizon)
                    now = datetime.utcnow()
                    if now < target_dt:
                        continue  # Not yet reached

                    # Only evaluate within a grace window (horizon + 1 day)
                    # to avoid using a price far in the future as the horizon price
                    grace_dt = target_dt + timedelta(days=1)
                    if now > grace_dt:
                        # Missed the window — use chart data for the target date
                        try:
                            candles = pf.get_chart_data(ticker, period="3mo")
                            target_date = target_dt.date()
                            target_candle = None
                            for c in candles:
                                c_date = datetime.fromisoformat(c["time"]).date() if isinstance(c["time"], str) else c["time"]
                                if c_date >= target_date:
                                    target_candle = c
                                    break
                            if target_candle is None:
                                continue  # No historical data available
                            current_price = target_candle["close"]
                        except Exception:
                            continue  # Cannot get historical price
                    else:
                        # Within grace window — current price is close to target date
                        quote = pf.get_quote(ticker)
                        if "error" in quote:
                            continue
                        current_price = quote["price"]

                    return_pct = ((current_price - entry_price) / entry_price) * 100

                    # Correct if: BUY with meaningful gain (≥1%), or SELL with meaningful drop (≤-1%)
                    # A minimum threshold prevents noise (±0.5% daily drift) from inflating accuracy.
                    # Horizon-adaptive: T+1d needs 1%, T+7d needs 2%, T+30d needs 5%
                    MIN_THRESHOLD_PCT = {1: 1.0, 3: 1.5, 7: 2.0, 14: 3.0, 30: 5.0}
                    threshold = MIN_THRESHOLD_PCT.get(horizon, 1.0)
                    if action == "BUY":
                        correct = return_pct >= threshold
                    else:
                        correct = return_pct <= -threshold

                    ticker_outcomes[h_key] = {
                        "price": round(current_price, 2),
                        "return_pct": round(return_pct, 2),
                        "correct": correct,
                        "evaluated_at": datetime.utcnow().isoformat(),
                    }
                    changed = True
                    total_checks += 1
                    if correct:
                        total_hits += 1

                    # Update per-component EMA
                    outcome_val = 1.0 if correct else 0.0
                    for comp_name, comp_score in components.items():
                        if comp_name not in emas:
                            emas[comp_name] = {str(h): 0.5 for h in EVAL_HORIZONS}
                        h_str = str(horizon)
                        old = emas[comp_name].get(h_str, 0.5)
                        # Weight the EMA update by how strong this component was
                        # Strong signals get more attribution
                        weight = comp_score if comp_score > 0.1 else 0.1
                        effective_alpha = EMA_ALPHA * weight
                        new = old * (1 - effective_alpha) + outcome_val * effective_alpha
                        emas[comp_name][h_str] = round(new, 4)

            if changed:
                data["outcomes"] = outcomes
                report.report_json = json.dumps(data)

                # Also store performance summary
                _update_performance_json(report, data)

        session.commit()
        _save_component_emas(emas)

        accuracy = (total_hits / total_checks * 100) if total_checks > 0 else 0
        logger.info(
            "Advisory evaluation: %d checks, %d hits (%.1f%% accuracy)",
            total_checks, total_hits, accuracy,
        )

        # Accumulate overall stats (load previous totals, add this run's counts)
        prev_checks_row = session.query(EngineSettings).get("advisory_total_checks")
        prev_hits_row = session.query(EngineSettings).get("advisory_total_hits")
        prev_checks = int(prev_checks_row.value) if prev_checks_row and prev_checks_row.value else 0
        prev_hits = int(prev_hits_row.value) if prev_hits_row and prev_hits_row.value else 0
        cumulative_checks = prev_checks + total_checks
        cumulative_hits = prev_hits + total_hits
        cumulative_accuracy = (cumulative_hits / cumulative_checks * 100) if cumulative_checks > 0 else 0
        _save_setting(session, "advisory_total_checks", str(cumulative_checks))
        _save_setting(session, "advisory_total_hits", str(cumulative_hits))
        _save_setting(session, "advisory_accuracy", str(round(cumulative_accuracy, 2)))
        session.commit()

        return {
            "total_checks": total_checks,
            "total_hits": total_hits,
            "accuracy": accuracy,
            "emas": emas,
        }

    except Exception:
        logger.exception("Advisory evaluation failed.")
        session.rollback()
        return {"error": "evaluation failed"}
    finally:
        session.close()


def _update_performance_json(report: AnalysisReport, data: Dict) -> None:
    """Compute and store performance summary in performance_json."""
    outcomes = data.get("outcomes", {})
    buy_perf = []
    sell_perf = []

    for rec in data.get("buy_recommendations", []):
        t = rec.get("ticker", "")
        t_out = outcomes.get(t, {})
        for h in EVAL_HORIZONS:
            h_key = f"T+{h}d"
            if h_key in t_out:
                buy_perf.append({
                    "ticker": t,
                    "horizon": h_key,
                    "entry_price": rec.get("current_price"),
                    "exit_price": t_out[h_key]["price"],
                    "return_pct": t_out[h_key]["return_pct"],
                    "correct": t_out[h_key]["correct"],
                })

    for rec in data.get("sell_recommendations", []):
        t = rec.get("ticker", "")
        t_out = outcomes.get(t, {})
        for h in EVAL_HORIZONS:
            h_key = f"T+{h}d"
            if h_key in t_out:
                sell_perf.append({
                    "ticker": t,
                    "horizon": h_key,
                    "entry_price": rec.get("current_price"),
                    "exit_price": t_out[h_key]["price"],
                    "return_pct": t_out[h_key]["return_pct"],
                    "correct": t_out[h_key]["correct"],
                })

    all_perf = buy_perf + sell_perf
    total = len(all_perf)
    correct = sum(1 for p in all_perf if p["correct"])

    perf = {
        "advisory_date": data.get("generated_at", "")[:10],
        "buy_performance": buy_perf,
        "sell_performance": sell_perf,
        "overall_accuracy": round(correct / total, 4) if total > 0 else None,
        "avg_return_pct": (
            round(sum(p["return_pct"] for p in buy_perf) / len(buy_perf), 2)
            if buy_perf else None
        ),
        "evaluated_at": datetime.utcnow().isoformat(),
    }

    if hasattr(report, "performance_json"):
        report.performance_json = json.dumps(perf)


def _save_setting(session, key: str, value: str) -> None:
    """Upsert an EngineSettings value."""
    s = session.query(EngineSettings).get(key)
    if s:
        s.value = value
    else:
        session.add(EngineSettings(key=key, value=value))


# ======================================================================
# Weekly weight rebalancing
# ======================================================================

def rebalance_weights() -> Dict[str, float]:
    """Auto-adjust advisory weights based on per-component accuracy EMAs.

    Called weekly (Sunday 07:35 UTC). Components with higher accuracy
    across horizons get proportionally more weight.
    """
    logger.info("Rebalancing advisory weights...")
    emas = _load_component_emas()
    current = _load_weights()

    # Compute average accuracy across all horizons per component
    avg_accuracy: Dict[str, float] = {}
    for comp in DEFAULT_WEIGHTS:
        horizons = emas.get(comp, {})
        if horizons:
            # Weight longer horizons more (they matter more for investments)
            weighted_sum = 0.0
            weight_sum = 0.0
            for h_str, acc in horizons.items():
                h = int(h_str)
                # Horizon weight: longer = more important
                hw = math.log2(h + 1)
                weighted_sum += acc * hw
                weight_sum += hw
            avg_accuracy[comp] = weighted_sum / weight_sum if weight_sum > 0 else 0.5
        else:
            avg_accuracy[comp] = 0.5

    # Normalize to weights that sum to 1.0
    total_acc = sum(avg_accuracy.values())
    if total_acc <= 0:
        logger.warning("All component accuracies are zero — keeping current weights.")
        return current

    new_weights = {
        comp: round(acc / total_acc, 4)
        for comp, acc in avg_accuracy.items()
    }

    # Iterative clamp-and-renormalize to ensure bounds [0.05, 0.50] hold
    FLOOR, CEIL = 0.05, 0.50
    for _ in range(5):  # converges in 2-3 iterations
        clamped = {k: max(FLOOR, min(CEIL, v)) for k, v in new_weights.items()}
        total = sum(clamped.values())
        new_weights = {k: round(v / total, 4) for k, v in clamped.items()}
        if all(FLOOR <= v <= CEIL for v in new_weights.values()):
            break

    _save_weights(new_weights)

    logger.info("Weights rebalanced: %s → %s", current, new_weights)
    logger.info("Component avg accuracies: %s", avg_accuracy)

    return new_weights


# ======================================================================
# Brier score calculation
# ======================================================================

def calculate_brier_scores() -> Dict[str, float]:
    """Calculate Brier scores for probability calibration.

    Brier = mean((predicted_probability - actual_outcome)^2)
    Lower is better: 0.0 = perfect, 0.25 = random, 1.0 = always wrong.
    """
    session = get_session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=35)
        advisories = (
            session.query(AnalysisReport)
            .filter(
                AnalysisReport.report_type == "daily_advisory",
                AnalysisReport.created_at >= cutoff,
            )
            .all()
        )

        scores_by_horizon: Dict[str, List[float]] = {
            f"T+{h}d": [] for h in EVAL_HORIZONS
        }
        component_brier: Dict[str, List[float]] = {
            comp: [] for comp in DEFAULT_WEIGHTS
        }

        for report in advisories:
            try:
                data = json.loads(report.report_json)
            except Exception:
                continue

            outcomes = data.get("outcomes", {})
            all_recs = (
                data.get("buy_recommendations", [])
                + data.get("sell_recommendations", [])
            )

            for rec in all_recs:
                ticker = rec.get("ticker")
                if not ticker or ticker not in outcomes:
                    continue

                # Use calibrated win probability as predicted probability for Brier score.
                # Composite score is NOT a probability — map it through the same
                # transform used in position sizing to get a proper [0, 1] probability.
                score = min(1.0, max(0.0, abs(rec.get("composite_score", 0.0))))
                pred_prob = max(0.35, min(0.80, 0.5 + score * 0.4))
                components = rec.get("components", {})

                for h_key, outcome in outcomes[ticker].items():
                    actual = 1.0 if outcome.get("correct") else 0.0
                    brier = (pred_prob - actual) ** 2
                    if h_key in scores_by_horizon:
                        scores_by_horizon[h_key].append(brier)

                    # Per-component Brier
                    for comp_name, comp_score in components.items():
                        if comp_name in component_brier:
                            comp_pred = min(1.0, max(0.0, comp_score))
                            comp_brier = (comp_pred - actual) ** 2
                            component_brier[comp_name].append(comp_brier)

        result = {}
        for h_key, scores in scores_by_horizon.items():
            if scores:
                result[h_key] = round(sum(scores) / len(scores), 4)

        for comp, scores in component_brier.items():
            if scores:
                result[f"component_{comp}"] = round(sum(scores) / len(scores), 4)

        # Save to DB
        _save_setting_standalone("advisory_brier_scores", json.dumps(result))

        logger.info("Brier scores calculated: %s", result)
        return result

    except Exception:
        logger.exception("Brier score calculation failed.")
        return {}
    finally:
        session.close()


def _save_setting_standalone(key: str, value: str) -> None:
    session = get_session()
    try:
        s = session.query(EngineSettings).get(key)
        if s:
            s.value = value
        else:
            session.add(EngineSettings(key=key, value=value))
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
