"""Deep analysis module — generates intelligence reports from database patterns.

Runs 2x daily (08:00 and 20:00 UTC) and produces an AnalysisReport with:
  - Vocabulary analysis: keyword frequencies, trending terms, threat-associated words
  - Source analysis: per-source activity, sentiment, credibility ratings
  - Narrative analysis: growth/decline, inter-narrative relationships
  - Regional analysis: geopolitical heatmap with threat levels
  - Temporal analysis: publication patterns, burst detection
  - Strategic outlook: "buy the rumour, sell the news" stock analysis
"""

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta
from statistics import mean
from typing import Dict, List, Any, Optional

from .db import (
    get_session,
    Article,
    ArticleBrief,
    NarrativeTimeline,
    RunUp,
    DecisionNode,
    Consequence,
    StockImpact,
    AnalysisReport,
    TradingSignal,
)
from .bunq_stocks import is_available_on_bunq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source credibility ratings (0-1 scale)
# ---------------------------------------------------------------------------

SOURCE_CREDIBILITY: Dict[str, float] = {
    # Major wire services
    "Reuters": 0.95,
    "AP News": 0.95,
    "AFP": 0.90,
    # Western broadsheets
    "BBC": 0.90,
    "The Guardian": 0.88,
    "NPR": 0.88,
    "PBS NewsHour": 0.88,
    "Financial Times": 0.90,
    "The Economist": 0.88,
    "New York Times": 0.88,
    "Washington Post": 0.85,
    "Wall Street Journal": 0.88,
    "Bloomberg": 0.88,
    # TV / cable news
    "CNN": 0.78,
    "CNBC": 0.80,
    "Fox News": 0.60,
    # Regional quality outlets
    "Al Jazeera English": 0.82,
    "Al Jazeera": 0.82,
    "South China Morning Post": 0.78,
    "The Japan Times": 0.80,
    "DW (Deutsche Welle)": 0.85,
    "France24": 0.83,
    # Defense / specialist
    "Defense One": 0.82,
    "The War Zone": 0.80,
    "Defense News": 0.80,
    "Jane's Defence": 0.88,
    "SIPRI": 0.90,
    "OilPrice.com": 0.72,
    "Rigzone": 0.72,
    # Government
    "White House": 0.70,
    "US State Department": 0.70,
    "NATO": 0.72,
    "European Council": 0.72,
    # State-affiliated media (lower credibility)
    "RT (Russia Today)": 0.30,
    "TASS": 0.35,
    "Xinhua": 0.40,
    "CGTN": 0.38,
    "PressTV Iran": 0.35,
    "IRNA": 0.40,
    "Press TV": 0.35,
    # Think tanks
    "CSIS": 0.85,
    "Brookings": 0.85,
    "RAND": 0.85,
    "CFR": 0.85,
    "IISS": 0.85,
    "Carnegie": 0.85,
    "Chatham House": 0.85,
    # X/Twitter OSINT accounts
    "X/Twitter - @OSINTdefender": 0.72,
    "X/Twitter - @sentdefender": 0.70,
    "X/Twitter - @IntelCrab": 0.70,
    "X/Twitter - @Nrg8000": 0.68,
    "X/Twitter - @AuroraIntel": 0.72,
    "X/Twitter - @Faytuks": 0.68,
    "X/Twitter - @AircraftSpots": 0.74,
    "X/Twitter - @RALee85": 0.78,
    "X/Twitter - @oryxspioenkop": 0.80,
    "X/Twitter - @Conflicts": 0.70,
    "X/Twitter - @christaborowski": 0.68,
    "X/Twitter - @ggreenwald": 0.75,
    "X/Twitter - @MaxBlumenthal": 0.65,
    "X/Twitter - @zerohedge": 0.55,
    "X/Twitter - @wikileaks": 0.70,
    "X/Twitter - @mtracey": 0.62,
    "X/Twitter - @BenjaminNorton": 0.60,
    "X/Twitter - @caitoz": 0.55,
    "X/Twitter - @TheGrayzoneNews": 0.60,
}

DEFAULT_CREDIBILITY = 0.60


# ---------------------------------------------------------------------------
# A. Vocabulary Analysis
# ---------------------------------------------------------------------------

def _analyze_vocabulary(session, since_date) -> Dict[str, Any]:
    """Top keywords, trending terms, sentiment-laden language."""
    briefs = (
        session.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= datetime.combine(since_date, datetime.min.time()))
        .all()
    )

    if not briefs:
        return {"top_keywords": [], "trending_keywords": [], "threat_associated_words": []}

    # 1. Overall keyword frequency
    keyword_freq: Counter = Counter()
    for b in briefs:
        try:
            kws = json.loads(b.keywords_json or "[]")
            keyword_freq.update(kw.lower() for kw in kws if isinstance(kw, str))
        except (json.JSONDecodeError, TypeError):
            pass

    # 2. Trending: keywords that appear >2x more today than weekly average
    today = date.today()
    today_briefs = [b for b in briefs if b.processed_at.date() == today]
    today_freq: Counter = Counter()
    for b in today_briefs:
        try:
            kws = json.loads(b.keywords_json or "[]")
            today_freq.update(kw.lower() for kw in kws if isinstance(kw, str))
        except (json.JSONDecodeError, TypeError):
            pass

    period_days = max((today - since_date).days, 1)
    trending = {}
    for kw, count in today_freq.items():
        if count < 2:
            continue
        baseline_daily = keyword_freq.get(kw, 0) / period_days
        if baseline_daily > 0:
            ratio = count / baseline_daily
            if ratio > 1.5:
                trending[kw] = round(ratio, 1)

    # 3. Threat-associated words: keywords correlated with negative sentiment
    kw_sentiment: Dict[str, List[float]] = defaultdict(list)
    for b in briefs:
        try:
            kws = json.loads(b.keywords_json or "[]")
            for kw in kws:
                if isinstance(kw, str):
                    kw_sentiment[kw.lower()].append(b.sentiment)
        except (json.JSONDecodeError, TypeError):
            pass

    threat_words = []
    for kw, sentiments in kw_sentiment.items():
        if len(sentiments) >= 3:
            avg = mean(sentiments)
            if avg < -0.3:
                threat_words.append({"keyword": kw, "avg_sentiment": round(avg, 3), "count": len(sentiments)})

    threat_words.sort(key=lambda x: x["avg_sentiment"])

    return {
        "top_keywords": [{"keyword": k, "count": c} for k, c in keyword_freq.most_common(30)],
        "trending_keywords": sorted(
            [{"keyword": k, "ratio": r} for k, r in trending.items()],
            key=lambda x: -x["ratio"],
        )[:15],
        "threat_associated_words": threat_words[:15],
    }


# ---------------------------------------------------------------------------
# B. Source Analysis
# ---------------------------------------------------------------------------

def _analyze_sources(session, since_date) -> Dict[str, Any]:
    """Per-source activity, sentiment, credibility ratings."""
    articles = (
        session.query(Article)
        .filter(Article.pub_date >= datetime.combine(since_date, datetime.min.time()))
        .all()
    )
    briefs_by_article = {
        b.article_id: b
        for b in session.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= datetime.combine(since_date, datetime.min.time()))
        .all()
    }

    source_stats: Dict[str, Dict] = defaultdict(lambda: {
        "count": 0, "sentiments": [], "intensities": [], "regions": set(),
        "keywords": Counter(),
    })

    for a in articles:
        s = source_stats[a.source]
        s["count"] += 1
        brief = briefs_by_article.get(a.id)
        if brief:
            s["sentiments"].append(brief.sentiment)
            s["intensities"].append(brief.intensity)
            s["regions"].add(brief.region)
            try:
                for kw in json.loads(brief.keywords_json or "[]"):
                    if isinstance(kw, str):
                        s["keywords"][kw.lower()] += 1
            except (json.JSONDecodeError, TypeError):
                pass

    source_activity = []
    for name, stats in sorted(source_stats.items(), key=lambda x: -x[1]["count"]):
        intensity_counts = Counter(stats["intensities"])
        dominant = intensity_counts.most_common(1)[0][0] if intensity_counts else "low"
        source_activity.append({
            "source": name,
            "article_count": stats["count"],
            "avg_sentiment": round(mean(stats["sentiments"]) if stats["sentiments"] else 0, 3),
            "dominant_intensity": dominant,
            "regions_covered": sorted(stats["regions"]),
            "top_topics": [{"keyword": k, "count": c} for k, c in stats["keywords"].most_common(5)],
            "credibility": SOURCE_CREDIBILITY.get(name, DEFAULT_CREDIBILITY),
        })

    most_active = source_activity[0]["source"] if source_activity else None
    most_negative = None
    if source_activity:
        neg_sources = [(s["source"], s["avg_sentiment"]) for s in source_activity if s["avg_sentiment"] != 0]
        if neg_sources:
            most_negative = min(neg_sources, key=lambda x: x[1])[0]

    return {
        "source_activity": source_activity,
        "most_active": most_active,
        "most_negative": most_negative,
        "total_sources": len(source_stats),
    }


# ---------------------------------------------------------------------------
# C. Narrative Analysis
# ---------------------------------------------------------------------------

def _analyze_narratives(session, since_date) -> Dict[str, Any]:
    """Narrative overview: growth, decline, inter-narrative relationships."""
    timelines = (
        session.query(NarrativeTimeline)
        .filter(NarrativeTimeline.date >= since_date)
        .order_by(NarrativeTimeline.date)
        .all()
    )

    # 1. Narrative trends
    narrative_data: Dict[str, Dict] = defaultdict(lambda: {
        "dates": [], "counts": [], "sentiments": [],
    })
    for t in timelines:
        n = narrative_data[t.narrative_name]
        n["dates"].append(str(t.date))
        n["counts"].append(t.article_count)
        n["sentiments"].append(t.avg_sentiment)

    narratives = []
    for name, data in narrative_data.items():
        total = sum(data["counts"])
        recent = sum(data["counts"][-2:])
        older = sum(data["counts"][:-2]) if len(data["counts"]) > 2 else 0
        if older > 0:
            trend = "rising" if recent > older * 0.8 else ("falling" if recent < older * 0.3 else "stable")
        else:
            trend = "rising" if recent > 0 else "stable"

        narratives.append({
            "narrative": name,
            "total_articles": total,
            "trend": trend,
            "avg_sentiment": round(mean(data["sentiments"]) if data["sentiments"] else 0, 3),
            "timeline": [{"date": d, "count": c} for d, c in zip(data["dates"], data["counts"])],
        })

    # 2. Narrative relationships via keyword overlap from brief data
    # Collect keywords per narrative by matching briefs to narratives
    narrative_keywords: Dict[str, set] = defaultdict(set)
    briefs = (
        session.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= datetime.combine(since_date, datetime.min.time()))
        .all()
    )

    for b in briefs:
        try:
            kws = json.loads(b.keywords_json or "[]")
            kw_set = {kw.lower() for kw in kws if isinstance(kw, str)}
        except (json.JSONDecodeError, TypeError):
            kw_set = set()

        region = (b.region or "global").lower().strip()
        # Match brief to narratives by region prefix
        for name in narrative_data:
            name_lower = name.lower()
            if name_lower.startswith(region) or region in name_lower:
                narrative_keywords[name].update(kw_set)

    # Compute overlap matrix
    relationships = []
    names = list(narrative_data.keys())
    for i, na in enumerate(names):
        for nb in names[i + 1:]:
            kws_a = narrative_keywords.get(na, set())
            kws_b = narrative_keywords.get(nb, set())
            if not kws_a or not kws_b:
                continue
            intersection = kws_a & kws_b
            overlap = len(intersection) / min(len(kws_a), len(kws_b))
            if overlap > 0.15:
                relationships.append({
                    "narrative_a": na,
                    "narrative_b": nb,
                    "keyword_overlap": round(overlap, 3),
                    "shared_keywords": sorted(list(intersection))[:10],
                })

    return {
        "narratives": sorted(narratives, key=lambda x: -x["total_articles"]),
        "relationships": sorted(relationships, key=lambda x: -x["keyword_overlap"]),
    }


# ---------------------------------------------------------------------------
# D. Regional Analysis
# ---------------------------------------------------------------------------

def _analyze_regions(session, since_date) -> Dict[str, Any]:
    """Activity per region with sentiment and intensity."""
    briefs = (
        session.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= datetime.combine(since_date, datetime.min.time()))
        .all()
    )

    intensity_map = {"low": 1, "moderate": 2, "high-threat": 3, "critical": 4}

    region_stats: Dict[str, Dict] = defaultdict(lambda: {
        "count": 0, "sentiments": [], "intensities": [],
        "top_keywords": Counter(),
    })

    for b in briefs:
        r = region_stats[b.region or "global"]
        r["count"] += 1
        r["sentiments"].append(b.sentiment)
        r["intensities"].append(b.intensity)
        try:
            for kw in json.loads(b.keywords_json or "[]"):
                if isinstance(kw, str):
                    r["top_keywords"][kw.lower()] += 1
        except (json.JSONDecodeError, TypeError):
            pass

    regions = []
    for name, stats in sorted(region_stats.items(), key=lambda x: -x[1]["count"]):
        intensities = stats["intensities"]
        max_intensity = max(intensities, key=lambda x: intensity_map.get(x, 0)) if intensities else "low"
        avg_threat = mean(intensity_map.get(i, 1) for i in intensities) / 4 if intensities else 0

        regions.append({
            "region": name,
            "article_count": stats["count"],
            "avg_sentiment": round(mean(stats["sentiments"]) if stats["sentiments"] else 0, 3),
            "max_intensity": max_intensity,
            "threat_level": round(avg_threat, 2),
            "top_topics": [{"keyword": k, "count": c} for k, c in stats["top_keywords"].most_common(5)],
        })

    return {"regions": regions}


# ---------------------------------------------------------------------------
# E. Temporal Analysis
# ---------------------------------------------------------------------------

def _analyze_temporal(session, since_date) -> Dict[str, Any]:
    """Publication patterns: peak hours, busiest days, burst detection."""
    articles = (
        session.query(Article)
        .filter(Article.pub_date >= datetime.combine(since_date, datetime.min.time()))
        .all()
    )

    if not articles:
        return {"peak_hours": [], "busiest_days": [], "avg_daily_articles": 0, "bursts": []}

    valid_articles = [a for a in articles if a.pub_date]
    if not valid_articles:
        return {"peak_hours": [], "busiest_days": [], "avg_daily_articles": 0, "bursts": []}

    hour_counts = Counter(a.pub_date.hour for a in valid_articles)
    day_counts = Counter(a.pub_date.strftime("%A") for a in valid_articles)

    # Daily article counts for burst detection
    daily_counts = Counter(a.pub_date.date() for a in valid_articles)
    avg_daily = mean(daily_counts.values()) if daily_counts else 0

    bursts = []
    for d, c in daily_counts.items():
        if avg_daily > 0 and c > avg_daily * 2:
            bursts.append({
                "date": str(d),
                "count": c,
                "ratio": round(c / avg_daily, 1),
            })

    return {
        "peak_hours": [{"hour": h, "count": c} for h, c in hour_counts.most_common(5)],
        "busiest_days": [{"day": d, "count": c} for d, c in day_counts.most_common(3)],
        "avg_daily_articles": round(avg_daily, 1),
        "bursts": sorted(bursts, key=lambda x: -x["ratio"]),
        "total_articles": len(valid_articles),
    }


# ---------------------------------------------------------------------------
# F. Strategic Outlook — "Buy the Rumour, Sell the News"
# ---------------------------------------------------------------------------

MAGNITUDE_WEIGHT = {"low": 1, "moderate": 2, "high": 4, "extreme": 8}


def _analyze_strategic_outlook(session, since_date) -> Dict[str, Any]:
    """Aggregate stock signals from active run-ups and classify rumour/news phases.

    Walks the chain: RunUp → DecisionNode → Consequence → StockImpact
    to build a weighted signal per ticker, then classifies each run-up
    as rumour (early/buy), news (late/sell), or developing.
    """
    active_runups = (
        session.query(RunUp)
        .filter(RunUp.status == "active")
        .all()
    )

    if not active_runups:
        return {
            "top_picks": [],
            "rumour_phase": [],
            "news_phase": [],
            "total_signals": 0,
        }

    # --- A. Stock Impact Aggregation ---
    stock_signals: Dict[str, Dict] = defaultdict(lambda: {
        "bullish_score": 0.0, "bearish_score": 0.0,
        "narratives": set(), "reasons": [],
        "name": "", "asset_type": "",
    })

    for runup in active_runups:
        runup_weight = (runup.current_score or 0) / 100.0
        if runup_weight <= 0:
            continue

        for node in runup.decision_nodes:
            if node.status != "open":
                continue
            yes_prob = node.yes_probability or 0.5
            no_prob = node.no_probability or (1 - yes_prob)

            for cons in node.consequences:
                branch_prob = yes_prob if cons.branch == "yes" else no_prob
                for si in cons.stock_impacts:
                    s = stock_signals[si.ticker]
                    s["name"] = si.name
                    s["asset_type"] = si.asset_type or "stock"
                    weight = (
                        MAGNITUDE_WEIGHT.get(si.magnitude, 1)
                        * branch_prob
                        * runup_weight
                    )
                    if si.direction == "bullish":
                        s["bullish_score"] += weight
                    else:
                        s["bearish_score"] += weight
                    s["narratives"].add(runup.narrative_name)
                    s["reasons"].append({
                        "narrative": runup.narrative_name,
                        "direction": si.direction,
                        "magnitude": si.magnitude,
                        "reasoning": si.reasoning or "",
                        "branch_prob": round(branch_prob, 2),
                    })

    # --- B. Rumour vs News Phase Detection ---
    rumour_phase: List[Dict] = []
    news_phase: List[Dict] = []

    for runup in active_runups:
        total_nodes = len(runup.decision_nodes)
        confirmed_nodes = sum(
            1 for n in runup.decision_nodes
            if n.status in ("confirmed_yes", "confirmed_no")
        )
        days_active = (
            (date.today() - runup.start_date).days
            if runup.start_date else 0
        )
        acceleration = runup.acceleration_rate or 0.0
        confirmed_ratio = confirmed_nodes / max(total_nodes, 1)

        phase_info = {
            "narrative": runup.narrative_name,
            "score": runup.current_score or 0,
            "acceleration": round(acceleration, 2),
            "days_active": days_active,
            "article_count": runup.article_count_total or 0,
            "confirmed_ratio": round(confirmed_ratio, 2),
        }

        # Rumour: early stage, high acceleration, few confirmed outcomes
        if acceleration > 0.5 and confirmed_ratio < 0.3 and days_active < 7:
            phase_info["phase"] = "rumour"
            rumour_phase.append(phase_info)
        # News: broadly covered, many confirmed nodes
        elif confirmed_ratio > 0.5 or days_active > 10:
            phase_info["phase"] = "news"
            news_phase.append(phase_info)
        else:
            phase_info["phase"] = "developing"
            rumour_phase.append(phase_info)  # Still a potential buy opportunity

    # --- C. Top Picks ---
    top_picks = []
    for ticker, data in stock_signals.items():
        net = data["bullish_score"] - data["bearish_score"]
        direction = "bullish" if net > 0 else "bearish"
        # Deduplicate reasons by narrative
        seen_narratives = set()
        unique_reasons = []
        for r in sorted(
            data["reasons"],
            key=lambda r: MAGNITUDE_WEIGHT.get(r["magnitude"], 1),
            reverse=True,
        ):
            if r["narrative"] not in seen_narratives:
                seen_narratives.add(r["narrative"])
                unique_reasons.append(r)
            if len(unique_reasons) >= 3:
                break

        top_picks.append({
            "ticker": ticker,
            "name": data["name"],
            "asset_type": data["asset_type"],
            "direction": direction,
            "net_score": round(abs(net), 2),
            "bullish_signals": round(data["bullish_score"], 2),
            "bearish_signals": round(data["bearish_score"], 2),
            "narratives": sorted(data["narratives"]),
            "top_reasons": unique_reasons,
            "available_on_bunq": is_available_on_bunq(ticker),
        })

    # Sort: bunq-available first, then by signal strength
    top_picks.sort(key=lambda x: (-int(x["available_on_bunq"]), -x["net_score"]))

    logger.info(
        "Strategic outlook: %d stock signals, %d rumour-phase, %d news-phase narratives.",
        len(top_picks),
        len(rumour_phase),
        len(news_phase),
    )

    return {
        "top_picks": top_picks[:15],
        "rumour_phase": sorted(rumour_phase, key=lambda x: -x["acceleration"])[:10],
        "news_phase": sorted(news_phase, key=lambda x: -x["confirmed_ratio"])[:10],
        "total_signals": len(stock_signals),
    }


# ---------------------------------------------------------------------------
# G. Claude Strategic Narrative
# ---------------------------------------------------------------------------

STRATEGIC_SYSTEM = """\
You are a senior geopolitical investment strategist.
You write concise, actionable market outlooks based on news intelligence data.
Your philosophy: "Buy the rumour, sell the news" — identify opportunities
BEFORE events fully materialize. Focus on bunq Stocks-available tickers.
Write in Dutch. Be direct, no fluff. Use bullet points where helpful.
Respond with valid JSON only. No markdown, no explanation outside JSON."""

STRATEGIC_USER_TEMPLATE = """\
Based on this intelligence data, write a strategic outlook.

## Active Narratives (rumour phase — early, buy opportunities):
{rumour_data}

## Maturing Narratives (news phase — consider selling/avoiding):
{news_data}

## Top Stock Signals (aggregated from all decision trees):
{stock_data}

## Regional Threat Levels:
{region_data}

## Trending Keywords (what's accelerating):
{trending_data}

## Active Trading Signals (multi-source confidence scoring):
{signals_data}

Write a JSON response:
{{
  "world_direction": "2-3 zinnen over waar de wereld naartoe gaat — de grote trends",
  "buy_opportunities": [
    {{
      "ticker": "XOM",
      "name": "ExxonMobil",
      "reasoning": "Waarom nu kopen — 1-2 zinnen",
      "narrative": "welk narratief drijft dit",
      "urgency": "high",
      "timeframe": "korte termijn"
    }}
  ],
  "sell_signals": [
    {{
      "ticker": "SPY",
      "name": "S&P 500 ETF",
      "reasoning": "Waarom overwegen te verkopen — 1 zin",
      "narrative": "welk narratief",
      "urgency": "medium"
    }}
  ],
  "sectors_to_watch": [
    {{
      "sector": "Energy",
      "direction": "bullish",
      "reasoning": "Waarom — 1 zin"
    }}
  ],
  "risk_warning": "Belangrijkste risico's — 1-2 zinnen"
}}

IMPORTANT: Only include tickers that appear in the stock data above.
Keep buy_opportunities to max 5 and sell_signals to max 3.
sectors_to_watch: max 4 sectors."""


def _generate_strategic_narrative(
    outlook_data: Dict[str, Any],
    regions_data: Dict[str, Any],
    vocabulary_data: Dict[str, Any],
    signals_data: Optional[List[Dict]] = None,
) -> Optional[Dict[str, Any]]:
    """Generate a strategic narrative via Claude Haiku.

    Cost: ~€0.005-0.01 per call. Returns None if no API key,
    budget exhausted, or on any error.
    """
    from .config import config

    if not config.anthropic_api_key:
        logger.info("No API key — skipping strategic narrative generation.")
        return None

    # Budget check (reuse from tree_generator)
    try:
        from .tree_generator import _check_budget, _log_usage
    except ImportError:
        logger.warning("Cannot import tree_generator budget functions.")
        return None

    if not _check_budget():
        logger.info("Budget exhausted — skipping strategic narrative.")
        return None

    # Build prompt data
    rumour_data = json.dumps(
        outlook_data.get("rumour_phase", [])[:5], default=str, ensure_ascii=False
    )
    news_data = json.dumps(
        outlook_data.get("news_phase", [])[:5], default=str, ensure_ascii=False
    )
    stock_data = json.dumps(
        outlook_data.get("top_picks", [])[:10], default=str, ensure_ascii=False
    )
    region_data = json.dumps(
        regions_data.get("regions", [])[:5], default=str, ensure_ascii=False
    )
    trending_data = json.dumps(
        vocabulary_data.get("trending_keywords", [])[:10], default=str, ensure_ascii=False
    )
    signals_text = json.dumps(
        signals_data[:5] if signals_data else [], default=str, ensure_ascii=False
    )

    user_prompt = STRATEGIC_USER_TEMPLATE.format(
        rumour_data=rumour_data,
        news_data=news_data,
        stock_data=stock_data,
        region_data=region_data,
        trending_data=trending_data,
        signals_data=signals_text,
    )

    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed — pip install anthropic")
        return None

    try:
        client = anthropic.Anthropic(api_key=config.anthropic_api_key)
        response = client.messages.create(
            model=config.tree_generator_model,
            max_tokens=2000,
            system=STRATEGIC_SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )

        # Log token usage
        usage = response.usage
        _log_usage(
            model=config.tree_generator_model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            purpose="strategic_outlook",
        )

        text = response.content[0].text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3].strip()

        result = json.loads(text)
        logger.info(
            "Strategic narrative generated: %d buy ops, %d sell signals, %d sectors.",
            len(result.get("buy_opportunities", [])),
            len(result.get("sell_signals", [])),
            len(result.get("sectors_to_watch", [])),
        )
        return result

    except json.JSONDecodeError as e:
        logger.error("Failed to parse strategic narrative JSON: %s", e)
        return None
    except Exception:
        logger.exception("Strategic narrative generation failed.")
        return None


# ---------------------------------------------------------------------------
# H. Main entry point
# ---------------------------------------------------------------------------

def run_deep_analysis(period_days: int = 7) -> Optional[AnalysisReport]:
    """Run complete deep analysis and store as AnalysisReport.

    Parameters
    ----------
    period_days:
        Number of days to analyse (default: 7).

    Returns
    -------
    AnalysisReport or None on failure.
    """
    session = get_session()
    since = date.today() - timedelta(days=period_days)

    try:
        logger.info("Starting deep analysis for period %s to %s...", since, date.today())

        vocabulary_data = _analyze_vocabulary(session, since)
        regions_data = _analyze_regions(session, since)

        # Strategic outlook: stock signals + rumour/news phases
        outlook_data = _analyze_strategic_outlook(session, since)

        # Query active trading signals for strategic narrative context
        active_signals = (
            session.query(TradingSignal)
            .filter(
                TradingSignal.superseded_by_id.is_(None),
                TradingSignal.expires_at >= datetime.utcnow(),
            )
            .order_by(TradingSignal.confidence.desc())
            .limit(10)
            .all()
        )
        signals_data = [
            {
                "narrative": s.narrative_name,
                "confidence": s.confidence,
                "level": s.signal_level,
                "ticker": s.ticker,
                "direction": s.direction,
                "reasoning": s.reasoning,
            }
            for s in active_signals
        ]

        # Claude narrative (optional — gracefully None if no API key or budget)
        strategic_narrative = None
        if outlook_data.get("total_signals", 0) > 0:
            strategic_narrative = _generate_strategic_narrative(
                outlook_data, regions_data, vocabulary_data,
                signals_data=signals_data,
            )

        report_data = {
            "period": {"start": str(since), "end": str(date.today()), "days": period_days},
            "vocabulary": vocabulary_data,
            "sources": _analyze_sources(session, since),
            "narratives": _analyze_narratives(session, since),
            "regions": regions_data,
            "temporal": _analyze_temporal(session, since),
            "strategic_outlook": outlook_data,
            "strategic_narrative": strategic_narrative,
            "generated_at": datetime.utcnow().isoformat(),
        }

        report = AnalysisReport(
            report_type="daily_briefing",
            period_start=since,
            period_end=date.today(),
            report_json=json.dumps(report_data, ensure_ascii=False, default=str),
        )
        session.add(report)
        session.commit()

        logger.info(
            "Deep analysis complete: %d keywords, %d sources, %d narratives, %d regions, %d stock signals.",
            len(report_data["vocabulary"].get("top_keywords", [])),
            report_data["sources"].get("total_sources", 0),
            len(report_data["narratives"].get("narratives", [])),
            len(report_data["regions"].get("regions", [])),
            outlook_data.get("total_signals", 0),
        )
        return report

    except Exception:
        logger.exception("Deep analysis failed.")
        session.rollback()
        return None
    finally:
        session.close()
