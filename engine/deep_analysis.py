"""Deep analysis module — generates intelligence reports from database patterns.

Runs 2x daily (08:00 and 20:00 UTC) and produces an AnalysisReport with:
  - Vocabulary analysis: keyword frequencies, trending terms, threat-associated words
  - Source analysis: per-source activity, sentiment, credibility ratings
  - Narrative analysis: growth/decline, inter-narrative relationships
  - Regional analysis: geopolitical heatmap with threat levels
  - Temporal analysis: publication patterns, burst detection
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
    AnalysisReport,
)

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
# F. Main entry point
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

        report_data = {
            "period": {"start": str(since), "end": str(date.today()), "days": period_days},
            "vocabulary": _analyze_vocabulary(session, since),
            "sources": _analyze_sources(session, since),
            "narratives": _analyze_narratives(session, since),
            "regions": _analyze_regions(session, since),
            "temporal": _analyze_temporal(session, since),
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
            "Deep analysis complete: %d keywords, %d sources, %d narratives, %d regions.",
            len(report_data["vocabulary"].get("top_keywords", [])),
            report_data["sources"].get("total_sources", 0),
            len(report_data["narratives"].get("narratives", [])),
            len(report_data["regions"].get("regions", [])),
        )
        return report

    except Exception:
        logger.exception("Deep analysis failed.")
        session.rollback()
        return None
    finally:
        session.close()
