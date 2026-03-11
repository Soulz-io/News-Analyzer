"""Narrative tracking and run-up detection (pure Python, no LLM).

Responsibilities:
  - Group article briefs by topic cluster and update NarrativeTimeline records.
  - Calculate a composite run-up score (0-100) per narrative.
  - Detect narratives whose score exceeds a threshold and manage RunUp records.
"""

import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .config import config
from .db import get_session, ArticleBrief, NarrativeTimeline, RunUp, Article

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Weights for the composite run-up score (must sum to 100)
W_VOLUME_DELTA = 25
W_SOURCE_SPREAD = 20
W_SENTIMENT_SHIFT = 30
W_SPIKE = 25

# Look-back windows
BASELINE_DAYS = 7
RECENT_DAYS = 2


# ---------------------------------------------------------------------------
# Narrative naming
# ---------------------------------------------------------------------------

def _narrative_name_for_cluster(cluster_id: Optional[int], briefs: List[ArticleBrief]) -> str:
    """Derive a human-readable narrative name from a cluster's briefs.

    Uses the most frequent keyword across the cluster.  Falls back to
    ``cluster_<id>`` when no keywords are available.
    """
    if cluster_id is None:
        return "unclustered"

    keyword_freq: Dict[str, int] = defaultdict(int)
    for b in briefs:
        try:
            import json
            kws = json.loads(b.keywords_json) if b.keywords_json else []
        except Exception:
            kws = []
        for kw in kws:
            keyword_freq[kw.lower()] = keyword_freq.get(kw.lower(), 0) + 1

    if keyword_freq:
        top_kw = max(keyword_freq, key=keyword_freq.get)  # type: ignore[arg-type]
        return top_kw.replace(" ", "-")

    return f"cluster_{cluster_id}"


# ---------------------------------------------------------------------------
# Timeline updates
# ---------------------------------------------------------------------------

def update_narratives(briefs: List[ArticleBrief]) -> List[NarrativeTimeline]:
    """Group *briefs* by topic cluster and upsert NarrativeTimeline rows.

    Each call appends/updates a row for today per narrative.

    Returns the list of upserted NarrativeTimeline records.
    """
    if not briefs:
        return []

    today = date.today()

    # Group briefs by topic_cluster_id
    clusters: Dict[Optional[int], List[ArticleBrief]] = defaultdict(list)
    for b in briefs:
        clusters[b.topic_cluster_id].append(b)

    session = get_session()
    updated: List[NarrativeTimeline] = []

    try:
        for cluster_id, cluster_briefs in clusters.items():
            narrative_name = _narrative_name_for_cluster(cluster_id, cluster_briefs)

            article_count = len(cluster_briefs)
            sources = {b.article.source for b in cluster_briefs if b.article}
            regions = {b.region for b in cluster_briefs}
            avg_sentiment = (
                sum(b.sentiment for b in cluster_briefs) / article_count
                if article_count > 0
                else 0.0
            )

            # Intensity score: weighted average mapped to 0-100
            intensity_map = {"low": 10, "moderate": 40, "high-threat": 70, "critical": 95}
            avg_intensity = (
                sum(intensity_map.get(b.intensity, 10) for b in cluster_briefs) / article_count
                if article_count > 0
                else 0.0
            )

            # Upsert
            existing: Optional[NarrativeTimeline] = (
                session.query(NarrativeTimeline)
                .filter(
                    NarrativeTimeline.narrative_name == narrative_name,
                    NarrativeTimeline.date == today,
                )
                .first()
            )

            if existing:
                existing.article_count += article_count
                existing.sources_count = max(existing.sources_count, len(sources))
                existing.unique_regions = max(existing.unique_regions, len(regions))
                existing.avg_sentiment = (existing.avg_sentiment + avg_sentiment) / 2
                existing.intensity_score = (existing.intensity_score + avg_intensity) / 2
                existing.topic_cluster_id = cluster_id
                row = existing
            else:
                row = NarrativeTimeline(
                    narrative_name=narrative_name,
                    topic_cluster_id=cluster_id,
                    date=today,
                    article_count=article_count,
                    sources_count=len(sources),
                    unique_regions=len(regions),
                    avg_sentiment=avg_sentiment,
                    intensity_score=avg_intensity,
                    trend="stable",
                )
                session.add(row)

            updated.append(row)

        # Compute trend for each narrative
        for row in updated:
            row.trend = _compute_trend(session, row.narrative_name, today)

        session.commit()
        logger.info("Updated %d narrative timelines for %s.", len(updated), today)

    except Exception:
        logger.exception("Failed to update narrative timelines.")
        session.rollback()
        return []
    finally:
        session.close()

    return updated


def _compute_trend(session: Session, narrative_name: str, today: date) -> str:
    """Determine whether a narrative is rising, stable, or falling.

    Compares the article count over the last RECENT_DAYS against the
    BASELINE_DAYS preceding that window.
    """
    recent_start = today - timedelta(days=RECENT_DAYS)
    baseline_start = today - timedelta(days=BASELINE_DAYS + RECENT_DAYS)

    recent_count = (
        session.query(func.coalesce(func.sum(NarrativeTimeline.article_count), 0))
        .filter(
            NarrativeTimeline.narrative_name == narrative_name,
            NarrativeTimeline.date >= recent_start,
            NarrativeTimeline.date <= today,
        )
        .scalar()
    ) or 0

    baseline_count = (
        session.query(func.coalesce(func.sum(NarrativeTimeline.article_count), 0))
        .filter(
            NarrativeTimeline.narrative_name == narrative_name,
            NarrativeTimeline.date >= baseline_start,
            NarrativeTimeline.date < recent_start,
        )
        .scalar()
    ) or 0

    if baseline_count == 0:
        return "rising" if recent_count > 0 else "stable"

    ratio = recent_count / max(baseline_count / max(BASELINE_DAYS, 1) * RECENT_DAYS, 1)

    if ratio > 1.5:
        return "rising"
    if ratio < 0.5:
        return "falling"
    return "stable"


# ---------------------------------------------------------------------------
# Run-up scoring
# ---------------------------------------------------------------------------

def calculate_runup_score(narrative_name: str, session: Optional[Session] = None) -> float:
    """Compute a composite run-up score (0-100) for *narrative_name*.

    Score components (summing to 100 max):
      - volume_delta  (25): % change in article count recent vs baseline
      - source_spread (20): number of distinct sources covering the narrative
      - sentiment_shift (30): magnitude of sentiment change toward negative
      - spike         (25): ratio of today's count vs 7-day daily average

    Returns
    -------
    float  0-100
    """
    close_after = session is None
    if session is None:
        session = get_session()

    try:
        today = date.today()
        recent_start = today - timedelta(days=RECENT_DAYS)
        baseline_start = today - timedelta(days=BASELINE_DAYS + RECENT_DAYS)

        # Recent rows
        recent_rows: List[NarrativeTimeline] = (
            session.query(NarrativeTimeline)
            .filter(
                NarrativeTimeline.narrative_name == narrative_name,
                NarrativeTimeline.date >= recent_start,
            )
            .all()
        )
        # Baseline rows
        baseline_rows: List[NarrativeTimeline] = (
            session.query(NarrativeTimeline)
            .filter(
                NarrativeTimeline.narrative_name == narrative_name,
                NarrativeTimeline.date >= baseline_start,
                NarrativeTimeline.date < recent_start,
            )
            .all()
        )

        # --- volume_delta ---
        recent_vol = sum(r.article_count for r in recent_rows) if recent_rows else 0
        baseline_vol = sum(r.article_count for r in baseline_rows) if baseline_rows else 0
        if baseline_vol > 0:
            vol_change = (recent_vol - baseline_vol) / baseline_vol
        else:
            vol_change = 1.0 if recent_vol > 0 else 0.0
        volume_score = min(max(vol_change, 0), 1.0) * W_VOLUME_DELTA

        # --- source_spread ---
        max_sources = max((r.sources_count for r in recent_rows), default=0)
        spread_score = min(max_sources / 10.0, 1.0) * W_SOURCE_SPREAD

        # --- sentiment_shift ---
        recent_sent = (
            sum(r.avg_sentiment for r in recent_rows) / len(recent_rows)
            if recent_rows
            else 0.0
        )
        baseline_sent = (
            sum(r.avg_sentiment for r in baseline_rows) / len(baseline_rows)
            if baseline_rows
            else 0.0
        )
        shift = baseline_sent - recent_sent  # positive = sentiment got more negative
        sentiment_score = min(max(shift, 0), 1.0) * W_SENTIMENT_SHIFT

        # --- spike ---
        today_row = next((r for r in recent_rows if r.date == today), None)
        today_count = today_row.article_count if today_row else 0
        all_rows = recent_rows + baseline_rows
        if all_rows:
            daily_avg = sum(r.article_count for r in all_rows) / len(all_rows)
        else:
            daily_avg = 0
        if daily_avg > 0:
            spike_ratio = today_count / daily_avg
        else:
            spike_ratio = 1.0 if today_count > 0 else 0.0
        spike_score = min(max(spike_ratio - 1.0, 0), 1.0) * W_SPIKE

        total = volume_score + spread_score + sentiment_score + spike_score
        return round(min(total, 100.0), 2)

    finally:
        if close_after:
            session.close()


# ---------------------------------------------------------------------------
# Run-up detection
# ---------------------------------------------------------------------------

def detect_runups(threshold: Optional[float] = None) -> List[RunUp]:
    """Detect narratives whose run-up score exceeds *threshold*.

    Creates new ``RunUp`` records for newly detected narratives and updates
    existing active ones.

    Returns
    -------
    list[RunUp]
        All active RunUp records at or above threshold.
    """
    if threshold is None:
        threshold = config.runup_threshold

    session = get_session()
    try:
        today = date.today()

        # Get all distinct narrative names that have recent activity
        recent_cutoff = today - timedelta(days=RECENT_DAYS + BASELINE_DAYS)
        names = [
            row[0]
            for row in session.query(NarrativeTimeline.narrative_name)
            .filter(NarrativeTimeline.date >= recent_cutoff)
            .distinct()
            .all()
        ]

        detected: List[RunUp] = []

        for name in names:
            score = calculate_runup_score(name, session)
            if score < threshold:
                continue

            # Check for existing active RunUp
            existing: Optional[RunUp] = (
                session.query(RunUp)
                .filter(
                    RunUp.narrative_name == name,
                    RunUp.status == "active",
                )
                .first()
            )

            # Total article count for this narrative
            total_articles = (
                session.query(func.coalesce(func.sum(NarrativeTimeline.article_count), 0))
                .filter(NarrativeTimeline.narrative_name == name)
                .scalar()
            ) or 0

            if existing:
                # Update
                prev_score = existing.current_score
                existing.current_score = score
                existing.acceleration_rate = round(score - prev_score, 2)
                existing.article_count_total = total_articles
                detected.append(existing)
            else:
                # Create new RunUp
                run_up = RunUp(
                    narrative_name=name,
                    detected_at=datetime.utcnow(),
                    start_date=today,
                    current_score=score,
                    acceleration_rate=0.0,
                    article_count_total=total_articles,
                    status="active",
                )
                session.add(run_up)
                detected.append(run_up)

        # Expire stale run-ups (score dropped below threshold * 0.6)
        active_runups = (
            session.query(RunUp).filter(RunUp.status == "active").all()
        )
        for ru in active_runups:
            if ru not in detected:
                score = calculate_runup_score(ru.narrative_name, session)
                if score < threshold * 0.6:
                    ru.status = "expired"
                    logger.info(
                        "RunUp '%s' expired (score %.1f < %.1f).",
                        ru.narrative_name,
                        score,
                        threshold * 0.6,
                    )

        session.commit()
        logger.info(
            "Run-up detection complete: %d active run-ups (threshold=%.1f).",
            len(detected),
            threshold,
        )
        return detected

    except Exception:
        logger.exception("Run-up detection failed.")
        session.rollback()
        return []
    finally:
        session.close()
