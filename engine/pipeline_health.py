"""Pipeline Health Monitor — zero LLM tokens.

Computes data quality metrics across every pipeline stage.
Called periodically or on-demand via API.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from sqlalchemy import func

from .db import (
    Article, ArticleBrief, NarrativeTimeline, RunUp,
    DecisionNode, SwarmVerdict, TradingSignal, AnalysisReport,
    get_session,
)

logger = logging.getLogger(__name__)


def check_pipeline_health() -> Dict[str, Any]:
    """Compute pipeline health metrics. Returns dict with scores and alerts."""
    session = get_session()
    try:
        now = datetime.utcnow()
        h6 = now - timedelta(hours=6)
        h24 = now - timedelta(hours=24)

        # --- Ingestion ---
        articles_6h = session.query(func.count(Article.id)).filter(
            Article.fetched_at >= h6
        ).scalar() or 0

        # --- Brief quality ---
        total_briefs = session.query(func.count(ArticleBrief.id)).scalar() or 0
        empty_summaries = session.query(func.count(ArticleBrief.id)).filter(
            (ArticleBrief.summary == None) | (ArticleBrief.summary == "")
        ).scalar() or 0
        summary_coverage = round((total_briefs - empty_summaries) / total_briefs * 100, 1) if total_briefs else 0

        # --- Narrative specificity ---
        total_narratives = session.query(func.count(NarrativeTimeline.id)).filter(
            NarrativeTimeline.date >= (now.date() - timedelta(days=7))
        ).scalar() or 0
        general_narratives = session.query(func.count(NarrativeTimeline.id)).filter(
            NarrativeTimeline.date >= (now.date() - timedelta(days=7)),
            NarrativeTimeline.narrative_name.like("%-general"),
        ).scalar() or 0
        specificity = round((total_narratives - general_narratives) / total_narratives * 100, 1) if total_narratives else 0

        # --- Signal quality ---
        total_signals = session.query(func.count(TradingSignal.id)).filter(
            TradingSignal.created_at >= h24
        ).scalar() or 0
        signals_with_ticker = session.query(func.count(TradingSignal.id)).filter(
            TradingSignal.created_at >= h24,
            TradingSignal.ticker.isnot(None),
        ).scalar() or 0
        ticker_rate = round(signals_with_ticker / total_signals * 100, 1) if total_signals else 0

        buy_sell_signals = session.query(func.count(TradingSignal.id)).filter(
            TradingSignal.created_at >= h24,
            TradingSignal.signal_level.in_(["BUY", "STRONG_BUY", "SELL", "STRONG_SELL"]),
        ).scalar() or 0
        action_rate = round(buy_sell_signals / total_signals * 100, 1) if total_signals else 0

        # --- Advisory yield ---
        latest_advisory = (
            session.query(AnalysisReport)
            .filter(AnalysisReport.report_type == "daily_advisory")
            .order_by(AnalysisReport.created_at.desc())
            .first()
        )
        advisory_recs = 0
        if latest_advisory and latest_advisory.report_json:
            try:
                data = json.loads(latest_advisory.report_json)
                advisory_recs = len(data.get("buy_recommendations", [])) + len(data.get("sell_recommendations", []))
            except Exception:
                pass

        # --- Swarm participation ---
        recent_verdicts = (
            session.query(SwarmVerdict)
            .filter(SwarmVerdict.created_at >= h24)
            .all()
        )
        avg_participation = 0
        if recent_verdicts:
            rates = [v.participation_rate or 1.0 for v in recent_verdicts]
            avg_participation = round(sum(rates) / len(rates) * 100, 1)

        # --- Build report ---
        metrics = {
            "timestamp": now.isoformat(),
            "ingestion_rate_6h": articles_6h,
            "summary_coverage_pct": summary_coverage,
            "narrative_specificity_pct": specificity,
            "signal_ticker_rate_pct": ticker_rate,
            "signal_action_rate_pct": action_rate,
            "advisory_recommendations": advisory_recs,
            "swarm_avg_participation_pct": avg_participation,
        }

        # --- Health status ---
        alerts = []
        if articles_6h < 20:
            alerts.append(f"Low ingestion: {articles_6h} articles/6h (threshold: 20)")
        if summary_coverage < 95:
            alerts.append(f"Low summary coverage: {summary_coverage}% (threshold: 95%)")
        if specificity < 30:
            alerts.append(f"Low narrative specificity: {specificity}% (threshold: 30%)")
        if total_signals > 0 and ticker_rate < 50:
            alerts.append(f"Low signal ticker rate: {ticker_rate}% (threshold: 50%)")
        if total_signals > 0 and action_rate < 5:
            alerts.append(f"No BUY/SELL signals: {action_rate}% (threshold: 5%)")
        if advisory_recs == 0:
            alerts.append("Advisory has 0 recommendations")

        metrics["status"] = "healthy" if not alerts else "degraded"
        metrics["alerts"] = alerts
        metrics["alert_count"] = len(alerts)

        logger.info("Pipeline health: %s (%d alerts)", metrics["status"], len(alerts))
        return metrics

    finally:
        session.close()
