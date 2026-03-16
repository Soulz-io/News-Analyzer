"""Confidence scorer -- composite multi-source signal generator.

Combines six independent signal sources into a single confidence score
per active run-up, then generates TradingSignal records when the composite
crosses pre-defined thresholds.

Signal sources and weights (V2.0):
  1. Run-up momentum score  (W_RUNUP = 0.15)
  2. Swarm verdict strength (W_SWARM = 0.20)
  3. Polymarket price drift (W_POLYMARKET = 0.15)
  4. News article accel     (W_NEWS_ACCEL = 0.15)
  5. Source convergence     (W_SOURCE_CONV = 0.15)
  6. ML signal predictor    (W_ML_PRED = 0.20)

Signal levels (mapped to SELL-side for bearish direction):
  STRONG_BUY / STRONG_SELL: >= 0.80
  BUY / SELL:               >= 0.70
  ALERT:                    >= 0.55
  WATCH:                    >= 0.35

No Claude API calls -- pure Python computation.  Zero additional cost.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from .db import (
    get_session,
    Article,
    ArticleBrief,
    NarrativeTimeline,
    RunUp,
    DecisionNode,
    Consequence,
    StockImpact,
    TradingSignal,
    PolymarketMatch,
    PolymarketPriceHistory,
    SwarmVerdict,
)
from .bunq_stocks import is_available_on_bunq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Signal level thresholds (checked top-down; first match wins)
# ---------------------------------------------------------------------------

SIGNAL_THRESHOLDS = [
    (0.80, "STRONG_BUY"),
    (0.70, "BUY"),
    (0.55, "ALERT"),
    (0.35, "WATCH"),
]

# Component weights (must sum to 1.0)
# V2.0: added ML predictor, rebalanced existing weights
W_RUNUP = 0.15
W_SWARM = 0.20        # swarm verdict confidence
W_POLYMARKET = 0.15   # keep but reduced (sparse matches)
W_NEWS_ACCEL = 0.15   # reliable
W_SOURCE_CONV = 0.15  # reliable
W_ML_PRED = 0.20      # ML signal predictor (autoresearch-optimized XGBoost)


def _classify_level(confidence: float) -> str:
    """Map a composite confidence score to the appropriate signal level."""
    for threshold, level in SIGNAL_THRESHOLDS:
        if confidence >= threshold:
            return level
    return "NONE"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_narrative_keywords(narrative_name: str, session: Session) -> set:
    """Collect keywords associated with a narrative.

    Sources:
      - Parse the narrative_name itself (split on '-')
      - Pull keywords_json from NarrativeTimeline-related ArticleBriefs
    """
    # Name-derived keywords (e.g. "middle-east-iran-war" -> {middle, east, iran, war})
    name_parts = set(narrative_name.lower().replace("-", " ").split())
    name_parts.discard("")

    # Try to gather richer keywords from ArticleBriefs linked to this narrative's
    # timeline entries (via topic_cluster_id or region matching).
    try:
        timeline_entries = (
            session.query(NarrativeTimeline)
            .filter(NarrativeTimeline.narrative_name == narrative_name)
            .all()
        )
        cluster_ids = {
            t.topic_cluster_id for t in timeline_entries
            if t.topic_cluster_id is not None
        }

        if cluster_ids:
            briefs = (
                session.query(ArticleBrief)
                .filter(ArticleBrief.topic_cluster_id.in_(cluster_ids))
                .limit(200)
                .all()
            )
            for b in briefs:
                try:
                    kws = json.loads(b.keywords_json) if b.keywords_json else []
                    if isinstance(kws, list):
                        for kw in kws:
                            name_parts.add(str(kw).lower().strip())
                except Exception:
                    pass
    except Exception:
        pass  # fall back to name-parts only

    return name_parts


def _brief_keywords(brief: ArticleBrief) -> set:
    """Extract the keyword set from an ArticleBrief's keywords_json."""
    try:
        kws = json.loads(brief.keywords_json) if brief.keywords_json else []
        if isinstance(kws, list):
            return {str(kw).lower().strip() for kw in kws}
    except Exception:
        pass
    return set()


# ---------------------------------------------------------------------------
# Component 1: X/Twitter Signal Strength
# ---------------------------------------------------------------------------

def _calculate_x_signal(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Score based on OSINT tweet density and urgency in the last 48 hours.

    Matching logic:
      - A tweet matches if it shares >= 1 keyword with the narrative
        (from ArticleBrief.keywords_json), OR if the narrative_name parts
        appear in the tweet title/description.

    Score formula:
      score = min(1.0, matched_count / 10) * max(avg_urgency, 0.5)

    Returns dict with score, count, and list of unique source accounts.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(hours=48)

        # Narrative keywords (from name + timeline briefs)
        narrative_kws = _get_narrative_keywords(run_up.narrative_name, session)
        name_parts = set(run_up.narrative_name.lower().replace("-", " ").split())
        name_parts.discard("")

        # Find X/Twitter articles in the last 48h
        x_articles = (
            session.query(Article)
            .filter(
                Article.source.like("X/Twitter%"),
                Article.pub_date >= cutoff,
            )
            .all()
        )

        if not x_articles:
            return {"score": 0.0, "count": 0, "accounts": []}

        # Preload briefs for these articles
        article_ids = [a.id for a in x_articles]
        briefs_by_article: Dict[int, ArticleBrief] = {}
        if article_ids:
            briefs = (
                session.query(ArticleBrief)
                .filter(ArticleBrief.article_id.in_(article_ids))
                .all()
            )
            briefs_by_article = {b.article_id: b for b in briefs}

        matched_articles: List[Article] = []
        urgency_scores: List[float] = []

        for art in x_articles:
            matched = False

            # Method 1: keyword overlap via ArticleBrief
            brief = briefs_by_article.get(art.id)
            if brief:
                tweet_kws = _brief_keywords(brief)
                if tweet_kws & narrative_kws:
                    matched = True

            # Method 2: narrative_name parts appear in title/description
            if not matched:
                text = f"{art.title or ''} {art.description or ''}".lower()
                significant_parts = {p for p in name_parts if len(p) > 2}
                if significant_parts and any(p in text for p in significant_parts):
                    matched = True

            if matched:
                matched_articles.append(art)
                if brief and brief.urgency_score is not None:
                    urgency_scores.append(brief.urgency_score)

        matched_count = len(matched_articles)
        if matched_count == 0:
            return {"score": 0.0, "count": 0, "accounts": []}

        avg_urgency = (
            sum(urgency_scores) / len(urgency_scores)
            if urgency_scores
            else 0.5
        )

        score = min(1.0, matched_count / 10.0) * max(avg_urgency, 0.5)
        score = min(1.0, score)

        unique_accounts = list({a.source for a in matched_articles})

        return {
            "score": round(score, 4),
            "count": matched_count,
            "accounts": unique_accounts,
        }

    except Exception:
        logger.exception(
            "X signal calculation failed for run-up %s", run_up.narrative_name
        )
        return {"score": 0.0, "count": 0, "accounts": []}


# ---------------------------------------------------------------------------
# Component 2: Polymarket Price Drift
# ---------------------------------------------------------------------------

def _calculate_polymarket_drift(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Score based on Polymarket price movement over the last 24 hours.

    For each matched market, finds the oldest price snapshot within 24h
    and calculates drift = current_price - oldest_price.  Takes the max
    absolute drift across all matched markets.

    Score formula:
      score = min(1.0, abs(max_drift) / 0.20)   (20% drift = max)

    Returns dict with score, drift, current probability, and market count.
    """
    try:
        matches = (
            session.query(PolymarketMatch)
            .filter(PolymarketMatch.run_up_id == run_up.id)
            .all()
        )

        if not matches:
            return {"score": 0.0, "drift": 0.0, "prob": None, "market_count": 0}

        cutoff = datetime.utcnow() - timedelta(hours=24)
        max_drift = 0.0
        best_prob = None
        found_history = False

        for match in matches:
            current_price = match.outcome_yes_price or 0.5

            # Find the oldest snapshot within the last 24h
            oldest = (
                session.query(PolymarketPriceHistory)
                .filter(
                    PolymarketPriceHistory.polymarket_id == match.polymarket_id,
                    PolymarketPriceHistory.recorded_at >= cutoff,
                )
                .order_by(PolymarketPriceHistory.recorded_at.asc())
                .first()
            )

            if oldest:
                found_history = True
                drift = current_price - oldest.yes_price
                if abs(drift) > abs(max_drift):
                    max_drift = drift
                    best_prob = current_price
            else:
                # No history for this market; track current price if nothing better
                if best_prob is None:
                    best_prob = current_price

        if not found_history:
            # No price history at all -- weak signal from current price
            if best_prob is not None:
                weak_score = max(0.0, (best_prob - 0.5) * 0.5)
            else:
                weak_score = 0.0
            return {
                "score": round(min(1.0, weak_score), 4),
                "drift": 0.0,
                "prob": round(best_prob, 4) if best_prob is not None else None,
                "market_count": len(matches),
            }

        score = min(1.0, abs(max_drift) / 0.20)

        return {
            "score": round(score, 4),
            "drift": round(max_drift, 4),
            "prob": round(best_prob, 4) if best_prob is not None else None,
            "market_count": len(matches),
        }

    except Exception:
        logger.exception(
            "Polymarket drift calculation failed for run-up %s",
            run_up.narrative_name,
        )
        return {"score": 0.0, "drift": 0.0, "prob": None, "market_count": 0}


# ---------------------------------------------------------------------------
# Component 3: News Acceleration
# ---------------------------------------------------------------------------

def _calculate_news_acceleration(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Score based on article growth rate: last 2 days vs preceding 5 days.

    Score formula:
      ratio  = recent_daily / max(baseline_daily, 1)
      score  = min(1.0, max(0, ratio - 1.0))

    A ratio of 2.0 (double the baseline) yields score 1.0.
    A ratio of 1.0 (flat) yields score 0.0.

    Returns dict with score, article count (recent), and ratio.
    """
    try:
        today = date.today()
        week_ago = today - timedelta(days=7)

        timelines = (
            session.query(NarrativeTimeline)
            .filter(
                NarrativeTimeline.narrative_name == run_up.narrative_name,
                NarrativeTimeline.date >= week_ago,
            )
            .order_by(NarrativeTimeline.date)
            .all()
        )

        if not timelines:
            return {"score": 0.0, "count": 0, "ratio": 0.0}

        # Split into recent (last 2 days) and baseline (preceding 5 days)
        recent_cutoff = today - timedelta(days=2)
        recent = [t for t in timelines if t.date >= recent_cutoff]
        baseline = [t for t in timelines if t.date < recent_cutoff]

        recent_count = sum(t.article_count for t in recent)
        recent_days = max(1, len(set(t.date for t in recent)))
        recent_daily = recent_count / recent_days

        if baseline:
            baseline_count = sum(t.article_count for t in baseline)
            baseline_days = max(1, len(set(t.date for t in baseline)))
            baseline_daily = baseline_count / baseline_days
        else:
            baseline_daily = 0.0

        ratio = recent_daily / max(baseline_daily, 1.0)
        score = min(1.0, max(0.0, ratio - 1.0))

        return {
            "score": round(score, 4),
            "count": recent_count,
            "ratio": round(ratio, 4),
        }

    except Exception:
        logger.exception(
            "News acceleration calculation failed for run-up %s",
            run_up.narrative_name,
        )
        return {"score": 0.0, "count": 0, "ratio": 0.0}


# ---------------------------------------------------------------------------
# Component 4: Source Convergence
# ---------------------------------------------------------------------------

def _calculate_source_convergence(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Score based on number and QUALITY of independent sources covering this narrative
    in the last 48 hours.

    Matching logic:
      - Region overlap with narrative name parts, OR
      - Keyword overlap via ArticleBrief

    Score formula (credibility-weighted):
      raw_score = min(1.0, unique_sources / 8)
      credibility_bonus = (avg_credibility - 0.6) × 0.5   (max +0.2 for high-quality sources)
      score = clamp(raw_score + credibility_bonus, 0, 1)

    A narrative covered by Reuters + FT + BBC scores higher than one covered
    by 3 obscure blogs, even with the same source count.

    Returns dict with score, unique source count, source list, and avg credibility.
    """
    from .deep_analysis import SOURCE_CREDIBILITY, DEFAULT_CREDIBILITY

    try:
        cutoff = datetime.utcnow() - timedelta(hours=48)

        name_parts = set(run_up.narrative_name.lower().replace("-", " ").split())
        name_parts.discard("")
        narrative_kws = _get_narrative_keywords(run_up.narrative_name, session)

        # Get all recent articles with their briefs
        articles = (
            session.query(Article)
            .filter(Article.pub_date >= cutoff)
            .all()
        )

        if not articles:
            return {"score": 0.0, "unique_sources": 0, "sources": [],
                    "avg_credibility": 0.0}

        # Preload briefs
        article_ids = [a.id for a in articles]
        briefs_by_article: Dict[int, ArticleBrief] = {}
        if article_ids:
            briefs = (
                session.query(ArticleBrief)
                .filter(ArticleBrief.article_id.in_(article_ids))
                .all()
            )
            briefs_by_article = {b.article_id: b for b in briefs}

        matched_sources: set = set()

        for art in articles:
            matched = False

            # Method 1: narrative name parts in title/description (region overlap)
            text = f"{art.title or ''} {art.description or ''}".lower()
            significant_parts = {p for p in name_parts if len(p) > 2}
            if significant_parts:
                overlap_count = sum(1 for p in significant_parts if p in text)
                if overlap_count >= 2:
                    matched = True

            # Method 2: keyword overlap via ArticleBrief
            if not matched:
                brief = briefs_by_article.get(art.id)
                if brief:
                    tweet_kws = _brief_keywords(brief)
                    if tweet_kws & narrative_kws:
                        matched = True

            if matched:
                matched_sources.add(art.source)

        unique_count = len(matched_sources)
        raw_score = min(1.0, unique_count / 8.0)

        # Credibility-weighted bonus: high-quality sources boost the score
        if matched_sources:
            avg_cred = sum(
                SOURCE_CREDIBILITY.get(s, DEFAULT_CREDIBILITY)
                for s in matched_sources
            ) / len(matched_sources)
            # Bonus: if avg credibility > 0.6 baseline, boost score by up to 0.2
            credibility_bonus = max(0.0, (avg_cred - DEFAULT_CREDIBILITY)) * 0.5
        else:
            avg_cred = 0.0
            credibility_bonus = 0.0

        score = min(1.0, max(0.0, raw_score + credibility_bonus))

        return {
            "score": round(score, 4),
            "unique_sources": unique_count,
            "sources": sorted(matched_sources),
            "avg_credibility": round(avg_cred, 3),
        }

    except Exception:
        logger.exception(
            "Source convergence calculation failed for run-up %s",
            run_up.narrative_name,
        )
        return {"score": 0.0, "unique_sources": 0, "sources": [],
                "avg_credibility": 0.0}


# ---------------------------------------------------------------------------
# Component 5: Swarm Verdict Consensus
# ---------------------------------------------------------------------------

# Map swarm verdicts to a 0-1 scale (how far from neutral)
_VERDICT_POSITION = {
    "STRONG_BUY": 1.0,
    "BUY": 0.75,
    "HOLD": 0.5,
    "SELL": 0.25,
    "STRONG_SELL": 0.0,
}


def _calculate_swarm_component(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Score based on swarm consensus verdict strength.

    Uses the most recent (non-superseded) SwarmVerdict for any decision node
    in this run-up. The signal strength is:

        |deviation from neutral| × confidence

    Examples:
      STRONG_BUY with 80% confidence → |1.0-0.5|×2×0.8 = 0.80
      BUY with 70% confidence → |0.75-0.5|×2×0.7 = 0.35
      HOLD with 90% confidence → |0.5-0.5|×2×0.9 = 0.00

    Returns dict with score, verdict, and confidence.
    """
    try:
        # Get the most recent non-superseded verdict for this run-up
        verdict = (
            session.query(SwarmVerdict)
            .filter(
                SwarmVerdict.run_up_id == run_up.id,
                SwarmVerdict.superseded_at.is_(None),
            )
            .order_by(SwarmVerdict.created_at.desc())
            .first()
        )

        if not verdict:
            return {"score": 0.0, "verdict": None, "confidence": 0.0}

        position = _VERDICT_POSITION.get(verdict.verdict, 0.5)
        deviation = abs(position - 0.5) * 2.0  # scale to 0-1
        score = deviation * verdict.confidence

        return {
            "score": round(min(1.0, score), 4),
            "verdict": verdict.verdict,
            "confidence": round(verdict.confidence, 4),
            "consensus_strength": round(verdict.consensus_strength or 0.0, 4),
        }

    except Exception:
        logger.exception(
            "Swarm component calculation failed for run-up %s",
            run_up.narrative_name,
        )
        return {"score": 0.0, "verdict": None, "confidence": 0.0}


# ---------------------------------------------------------------------------
# ML signal predictor component (V2.0)
# ---------------------------------------------------------------------------

def _calculate_ml_component(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Calculate the ML signal predictor component.

    Uses a trained XGBoost model to predict the probability that this run-up
    leads to a profitable trade. Returns 0.5 (neutral) if model is not available.
    """
    try:
        from .ml.inference import predict_signal, get_model_status

        model_status = get_model_status()
        ml_score = predict_signal(run_up.id)

        # If ML model is not available, contribute nothing (truly neutral)
        if model_status.get("status") != "active" or abs(ml_score - 0.5) < 0.001:
            return {"score": 0.0, "model_status": model_status.get("status", "unavailable"), "raw_prediction": ml_score}

        return {
            "score": round(ml_score, 4),
            "model_status": model_status.get("status", "unknown"),
            "model_trained_at": model_status.get("trained_at"),
        }
    except ImportError:
        return {"score": 0.0, "model_status": "not_installed"}
    except Exception:
        logger.exception("ML prediction failed for %s", run_up.narrative_name)
        return {"score": 0.0, "model_status": "error"}


# ---------------------------------------------------------------------------
# Composite confidence calculation
# ---------------------------------------------------------------------------

def calculate_confidence(run_up: RunUp, session: Session) -> Dict[str, Any]:
    """Calculate the composite confidence score for a single run-up.

    Calls all five component scorers plus ML predictor, and normalizes
    the run-up's own
    current_score (0-100 scale) to 0-1.

    Returns a dict containing:
      - confidence: float (0-1)
      - signal_level: str
      - components: dict with runup_score, x_signal, polymarket_drift,
                     news_acceleration, source_convergence sub-dicts
    """
    # Normalize run-up score: 0-100 -> 0-1
    runup_norm = min(1.0, max(0.0, (run_up.current_score or 0.0) / 100.0))

    # Each component is wrapped in its own try/except internally,
    # but we also guard here so a broken component returns 0 without
    # crashing the entire scorer.
    try:
        swarm = _calculate_swarm_component(run_up, session)
    except Exception:
        logger.exception("Swarm component failed for %s, defaulting to 0", run_up.narrative_name)
        swarm = {"score": 0.0, "verdict": None, "confidence": 0.0}

    try:
        poly = _calculate_polymarket_drift(run_up, session)
    except Exception:
        logger.exception("Polymarket drift failed for %s, defaulting to 0", run_up.narrative_name)
        poly = {"score": 0.0, "drift": 0.0, "prob": None, "market_count": 0}

    try:
        news = _calculate_news_acceleration(run_up, session)
    except Exception:
        logger.exception("News accel failed for %s, defaulting to 0", run_up.narrative_name)
        news = {"score": 0.0, "count": 0, "ratio": 0.0}

    try:
        src = _calculate_source_convergence(run_up, session)
    except Exception:
        logger.exception("Source convergence failed for %s, defaulting to 0", run_up.narrative_name)
        src = {"score": 0.0, "unique_sources": 0, "sources": []}

    # Also calculate X signal for display/reasoning (kept at 0 weight)
    try:
        x = _calculate_x_signal(run_up, session)
    except Exception:
        x = {"score": 0.0, "count": 0, "accounts": []}

    # ML signal predictor (V2.0 — autoresearch-optimized XGBoost)
    try:
        ml = _calculate_ml_component(run_up, session)
    except Exception:
        logger.exception("ML component failed for %s, defaulting to neutral", run_up.narrative_name)
        ml = {"score": 0.0, "model_status": "error"}

    confidence = (
        W_RUNUP * runup_norm
        + W_SWARM * swarm["score"]
        + W_POLYMARKET * poly["score"]
        + W_NEWS_ACCEL * news["score"]
        + W_SOURCE_CONV * src["score"]
        + W_ML_PRED * ml["score"]
    )

    signal_level = _classify_level(confidence)

    return {
        "confidence": round(confidence, 4),
        "signal_level": signal_level,
        "components": {
            "runup_score": round(runup_norm, 4),
            "swarm_verdict": swarm,
            "x_signal": x,
            "polymarket_drift": poly,
            "news_acceleration": news,
            "source_convergence": src,
            "ml_prediction": ml,
        },
    }


# ---------------------------------------------------------------------------
# Primary ticker identification
# ---------------------------------------------------------------------------

def _find_primary_ticker(
    run_up: RunUp, session: Session
) -> Tuple[Optional[str], Optional[str]]:
    """Walk DecisionNode -> Consequence -> StockImpact for this run-up.

    Finds the ticker with the highest (magnitude x branch_probability)
    score, preferring tickers available on bunq.

    Returns (ticker, direction) or (None, None).
    """
    try:
        magnitude_weight = {"low": 1, "moderate": 2, "high": 4, "extreme": 8}

        ticker_scores: Dict[str, float] = defaultdict(float)
        ticker_direction: Dict[str, float] = defaultdict(float)  # net direction weight

        # Batch-load all relationships to avoid N+1 queries
        nodes = (
            session.query(DecisionNode)
            .options(
                joinedload(DecisionNode.consequences).joinedload(Consequence.stock_impacts)
            )
            .filter(DecisionNode.run_up_id == run_up.id)
            .all()
        )

        for node in nodes:
            if node.status != "open":
                continue

            yes_prob = node.yes_probability or 0.5

            for cons in node.consequences:
                branch_prob = (
                    yes_prob if cons.branch == "yes" else (1.0 - yes_prob)
                )

                for si in cons.stock_impacts:
                    mag = magnitude_weight.get(si.magnitude, 1)
                    weight = mag * branch_prob
                    ticker_scores[si.ticker] += weight

                    if si.direction == "bullish":
                        ticker_direction[si.ticker] += weight
                    else:
                        ticker_direction[si.ticker] -= weight

        if not ticker_scores:
            return None, None

        # Only consider bunq-available tickers
        bunq_tickers = {t for t in ticker_scores if is_available_on_bunq(t)}
        if not bunq_tickers:
            return None, None

        best = max(bunq_tickers, key=lambda t: ticker_scores[t])
        direction = "bullish" if ticker_direction[best] > 0 else "bearish"
        return best, direction

    except Exception:
        logger.exception(
            "Primary ticker lookup failed for run-up %s", run_up.narrative_name
        )
        return None, None


# ---------------------------------------------------------------------------
# Reasoning template (no Claude API call)
# ---------------------------------------------------------------------------

def _generate_reasoning(
    run_up: RunUp,
    result: Dict[str, Any],
    ticker: Optional[str],
    signal_level: Optional[str] = None,
) -> str:
    """Generate a human-readable reasoning string from component scores.

    Format example:
      [BUY] middle-east-iran-war: 8 OSINT tweets in 48h; Polymarket drift
      +12.5%; 45 articles accelerating; 11 sources; Primary: XOM
    """
    level = signal_level or result["signal_level"]
    comps = result["components"]
    parts: List[str] = []

    # Swarm verdict
    swarm = comps.get("swarm_verdict", {})
    if swarm.get("verdict"):
        parts.append(f"Swarm: {swarm['verdict']} ({swarm['confidence']:.0%} conf)")

    # X/Twitter signal (informational — 0 weight)
    x = comps.get("x_signal", {})
    if x.get("count", 0) > 0:
        parts.append(f"{x['count']} OSINT tweets")

    # Polymarket drift
    poly = comps["polymarket_drift"]
    if poly["drift"] != 0:
        parts.append(f"Polymarket drift {poly['drift']:+.1%}")
    elif poly.get("prob") is not None:
        parts.append(f"Polymarket prob {poly['prob']:.0%}")

    # News acceleration
    news = comps["news_acceleration"]
    if news["count"] > 0:
        parts.append(f"{news['count']} articles accelerating")

    # Source convergence
    src = comps["source_convergence"]
    if src["unique_sources"] > 0:
        parts.append(f"{src['unique_sources']} sources")

    # Primary ticker
    if ticker:
        parts.append(f"Primary: {ticker}")

    detail = "; ".join(parts) if parts else "low activity"
    return f"[{level}] {run_up.narrative_name}: {detail}"


# ---------------------------------------------------------------------------
# Main entry point: update trading signals for all active run-ups
# ---------------------------------------------------------------------------

def update_trading_signals() -> List[TradingSignal]:
    """Score all active run-ups and create/update TradingSignal records.

    Called every 30 minutes by the scheduler.

    Logic:
      - Query all active RunUps (status='active', merged_into_id IS NULL)
      - For each: calculate_confidence()
      - Skip if signal_level is 'NONE' (below WATCH threshold)
      - Check the last non-superseded TradingSignal for this run_up:
          * If same level and confidence hasn't shifted >5%, skip (no noise)
          * Otherwise create a new signal and supersede the old one
      - Find primary ticker via _find_primary_ticker()
      - Generate reasoning
      - Create TradingSignal record with all component scores, context,
        expires_at = now + 24h
      - Commit and return list of new signals

    Returns list of newly created TradingSignal records.
    """
    session = get_session()
    signals: List[TradingSignal] = []

    try:
        active_runups = (
            session.query(RunUp)
            .filter(
                RunUp.status == "active",
                RunUp.merged_into_id.is_(None),
            )
            .all()
        )

        # Filter out catch-all "general" narratives (noise, not actionable signals)
        active_runups = [ru for ru in active_runups if not ru.narrative_name.endswith("-general")]

        if not active_runups:
            logger.info("Confidence scorer: no active run-ups to score.")
            return []

        logger.info(
            "Confidence scorer: scoring %d active run-ups.", len(active_runups)
        )

        for ru in active_runups:
            try:
                result = calculate_confidence(ru, session)
            except Exception:
                logger.exception(
                    "Confidence calculation failed for run-up '%s' (id=%d), skipping.",
                    ru.narrative_name,
                    ru.id,
                )
                continue

            if result["signal_level"] == "NONE":
                continue

            # Check if signal meaningfully changed from the last one
            last_signal = (
                session.query(TradingSignal)
                .filter(
                    TradingSignal.run_up_id == ru.id,
                    TradingSignal.superseded_by_id.is_(None),
                )
                .order_by(TradingSignal.created_at.desc())
                .first()
            )

            # Find the primary ticker for this run-up
            ticker, direction = _find_primary_ticker(ru, session)

            # Map signal level to direction-aware label BEFORE dedup comparison
            signal_level = result["signal_level"]
            if direction == "bearish":
                _SELL_MAP = {
                    "STRONG_BUY": "STRONG_SELL",
                    "BUY": "SELL",
                    "ALERT": "ALERT",
                    "WATCH": "WATCH",
                }
                signal_level = _SELL_MAP.get(signal_level, signal_level)

            # Suppress noise: skip if level and confidence barely changed
            if last_signal:
                level_same = last_signal.signal_level == signal_level
                conf_shift = abs(last_signal.confidence - result["confidence"])
                if level_same and conf_shift < 0.05:
                    continue  # No meaningful change -- suppress noise

            # Build human-readable reasoning (use mapped signal_level)
            reasoning = _generate_reasoning(ru, result, ticker, signal_level=signal_level)

            # Create the new TradingSignal record
            comps = result["components"]
            swarm_comp = comps.get("swarm_verdict", {})
            signal = TradingSignal(
                run_up_id=ru.id,
                narrative_name=ru.narrative_name,
                ticker=ticker,
                direction=direction,
                confidence=result["confidence"],
                signal_level=signal_level,
                # Component scores for transparency
                # NOTE: x_signal_component repurposed to store swarm score (was always 0)
                runup_score_component=comps["runup_score"],
                x_signal_component=swarm_comp.get("score", 0.0),
                polymarket_drift_component=comps["polymarket_drift"]["score"],
                news_acceleration_component=comps["news_acceleration"]["score"],
                source_convergence_component=comps["source_convergence"]["score"],
                ml_prediction_component=comps.get("ml_prediction", {}).get("score", 0.0),
                # Context
                x_signal_count=comps.get("x_signal", {}).get("count", 0),
                news_count=comps["news_acceleration"]["count"],
                polymarket_prob=comps["polymarket_drift"]["prob"],
                reasoning=reasoning,
                # Expires in 24 hours
                created_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(hours=24),
            )

            session.add(signal)
            session.flush()  # Get the auto-generated ID

            # Supersede the previous signal for this run-up
            if last_signal:
                last_signal.superseded_by_id = signal.id

            signals.append(signal)

            logger.info(
                "Signal: [%s] %s conf=%.2f ticker=%s",
                result["signal_level"],
                ru.narrative_name,
                result["confidence"],
                ticker or "none",
            )

        session.commit()

        buy_plus = sum(
            1 for s in signals if s.signal_level in ("BUY", "STRONG_BUY")
        )
        logger.info(
            "Confidence scorer complete: %d run-ups scored, %d signals generated, %d BUY+.",
            len(active_runups),
            len(signals),
            buy_plus,
        )

        return signals

    except Exception:
        session.rollback()
        logger.exception("Confidence scoring run failed.")
        return []
    finally:
        session.close()
