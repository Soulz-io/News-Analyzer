"""Narrative tracking and run-up detection (pure Python, no LLM).

Responsibilities:
  - Group article briefs by topic cluster and update NarrativeTimeline records.
  - Calculate a composite run-up score (0-100) per narrative.
  - Detect narratives whose score exceeds a threshold and manage RunUp records.
"""

import json
import logging
import time
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

# Score cache: narrative_name -> (score, timestamp)
_runup_score_cache: Dict[str, Tuple[float, float]] = {}
CACHE_TTL = 300  # 5 minutes


# ---------------------------------------------------------------------------
# Narrative naming
# ---------------------------------------------------------------------------

def _narrative_name_for_cluster(
    cluster_id: Optional[int],
    briefs: List[ArticleBrief],
    pseudo_key: Optional[str] = None,
) -> str:
    """Derive a human-readable narrative name from a cluster's briefs.

    Uses the most frequent keyword across the cluster.  Falls back to
    ``cluster_<id>`` when no keywords are available.

    If *pseudo_key* is provided (e.g. ``"middle-east-military"``), it is
    used directly as the narrative name -- this supports the keyword-based
    grouping for unclustered briefs.
    """
    if pseudo_key is not None:
        return pseudo_key

    if cluster_id is None:
        # Attempt to build a name from keywords even for unclustered briefs
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
        return "unclustered"

    keyword_freq = defaultdict(int)
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

def _get_brief_keywords(brief: ArticleBrief) -> List[str]:
    """Extract keywords from a brief's keywords_json field."""
    try:
        kws = json.loads(brief.keywords_json) if brief.keywords_json else []
        return [str(kw).lower().strip() for kw in kws] if isinstance(kws, list) else []
    except Exception:
        return []


def _group_unclustered_by_region_keyword(
    unclustered_briefs: List[ArticleBrief],
) -> Dict[str, List[ArticleBrief]]:
    """Group unclustered briefs into pseudo-clusters by region + shared keyword.

    Strategy:
      1. Bucket briefs by region.
      2. Within each region, find the most frequent keyword across all briefs.
      3. For each brief, create a key = ``"{region}-{top_keyword}"`` using
         the top keyword from the brief's own keywords that overlaps with
         the region's keyword pool.
      4. Merge small groups (< 3 articles) into ``"{region}-general"``.

    Returns a dict mapping pseudo-cluster key -> list of briefs.
    """
    # Step 1: Bucket by region
    region_buckets: Dict[str, List[ArticleBrief]] = defaultdict(list)
    for b in unclustered_briefs:
        region = (b.region or "unknown").lower().strip()
        region_buckets[region].append(b)

    pseudo_clusters: Dict[str, List[ArticleBrief]] = defaultdict(list)

    for region, region_briefs in region_buckets.items():
        # Step 2: Count keyword frequencies across all briefs in this region
        region_kw_freq: Dict[str, int] = defaultdict(int)
        brief_kw_map: Dict[int, List[str]] = {}  # brief.id -> keywords
        for b in region_briefs:
            kws = _get_brief_keywords(b)[:3]  # top 3 keywords per brief
            brief_kw_map[b.id] = kws
            for kw in kws:
                region_kw_freq[kw] += 1

        # Step 3: Assign each brief to a pseudo-cluster key
        for b in region_briefs:
            kws = brief_kw_map.get(b.id, [])
            if kws:
                # Pick the keyword from this brief that is most frequent
                # in the region overall
                best_kw = max(kws, key=lambda k: region_kw_freq.get(k, 0))
                key = f"{region}-{best_kw.replace(' ', '-')}"
            else:
                key = f"{region}-general"
            pseudo_clusters[key].append(b)

        # Step 4: Merge small groups (< 3 articles) into "{region}-general"
        general_key = f"{region}-general"
        small_keys = [
            k for k, v in pseudo_clusters.items()
            if k.startswith(f"{region}-") and k != general_key and len(v) < 3
        ]
        for k in small_keys:
            pseudo_clusters[general_key].extend(pseudo_clusters.pop(k))

    return dict(pseudo_clusters)


def update_narratives(briefs: List[ArticleBrief]) -> List[NarrativeTimeline]:
    """Group *briefs* by topic cluster and upsert NarrativeTimeline rows.

    When briefs have ``topic_cluster_id = None`` (e.g. BERTopic is not
    installed), they are grouped by **region + top keyword** instead of
    being lumped into one ``"unclustered"`` narrative.  This produces
    meaningful pseudo-clusters such as ``"middle-east-military"``,
    ``"europe-sanctions"``, ``"east-asia-trade"``, etc.

    Each call appends/updates a row for today per narrative.

    Returns the list of upserted NarrativeTimeline records.
    """
    if not briefs:
        return []

    today = date.today()

    # ---- Step 1: Separate clustered and unclustered briefs ----
    clustered_briefs: Dict[int, List[ArticleBrief]] = defaultdict(list)
    unclustered_briefs: List[ArticleBrief] = []

    for b in briefs:
        if b.topic_cluster_id is not None:
            clustered_briefs[b.topic_cluster_id].append(b)
        else:
            unclustered_briefs.append(b)

    # ---- Step 2: Build a unified work-list of (name, cluster_id, briefs) ----
    #  Each entry is (narrative_name, topic_cluster_id, list_of_briefs)
    work_items: List[Tuple[str, Optional[int], List[ArticleBrief]]] = []

    # Clustered briefs: use existing logic (group by topic_cluster_id)
    for cluster_id, cluster_briefs in clustered_briefs.items():
        name = _narrative_name_for_cluster(cluster_id, cluster_briefs)
        work_items.append((name, cluster_id, cluster_briefs))

    # Unclustered briefs: group by region + top keyword
    if unclustered_briefs:
        pseudo_clusters = _group_unclustered_by_region_keyword(unclustered_briefs)
        for pseudo_key, pseudo_briefs in pseudo_clusters.items():
            name = _narrative_name_for_cluster(
                None, pseudo_briefs, pseudo_key=pseudo_key,
            )
            work_items.append((name, None, pseudo_briefs))
        logger.info(
            "Grouped %d unclustered briefs into %d pseudo-clusters by region+keyword.",
            len(unclustered_briefs),
            len(pseudo_clusters),
        )

    # ---- Step 3: Upsert NarrativeTimeline rows ----
    session = get_session()
    updated: List[NarrativeTimeline] = []

    try:
        for narrative_name, cluster_id, cluster_briefs in work_items:
            article_count = len(cluster_briefs)
            # Safely extract sources — briefs may be detached from their original session
            sources = set()
            for b in cluster_briefs:
                try:
                    if b.article:
                        sources.add(b.article.source)
                except Exception:
                    # Brief is detached from session; skip
                    pass
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
                # Weighted average: combine old and new by article count
                old_n = existing.article_count
                new_n = article_count
                total_n = old_n + new_n
                if total_n > 0:
                    existing.avg_sentiment = (existing.avg_sentiment * old_n + avg_sentiment * new_n) / total_n
                    existing.intensity_score = (existing.intensity_score * old_n + avg_intensity * new_n) / total_n
                existing.article_count = total_n
                existing.sources_count = existing.sources_count + len(sources)  # Accumulate (upper bound of distinct sources)
                existing.unique_regions = max(existing.unique_regions, len(regions))
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
    # Check cache first
    cached = _runup_score_cache.get(narrative_name)
    if cached and (time.time() - cached[1]) < CACHE_TTL:
        return cached[0]

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
        # Store in cache before returning
        _runup_score_cache[narrative_name] = (total, time.time())
        return round(min(total, 100.0), 2)

    finally:
        if close_after:
            session.close()


# ---------------------------------------------------------------------------
# Run-up detection
# ---------------------------------------------------------------------------

def detect_runups(threshold: Optional[float] = None, changed_narratives: Optional[List[str]] = None) -> List[RunUp]:
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

        # If we know which narratives changed, invalidate their cache and only score those
        if changed_narratives:
            for cn in changed_narratives:
                _runup_score_cache.pop(cn, None)
            # Still check all names for threshold, but prioritize changed ones
            # (cache will handle the non-changed ones efficiently)

        detected: List[RunUp] = []

        for name in names:
            score = calculate_runup_score(name, session)
            if score < threshold:
                continue

            # Check for existing RunUp (active OR merged — don't recreate merged ones)
            existing: Optional[RunUp] = (
                session.query(RunUp)
                .filter(
                    RunUp.narrative_name == name,
                    RunUp.status.in_(["active", "merged"]),
                )
                .first()
            )

            # If already merged into another run-up, skip — the primary handles it
            if existing and existing.status == "merged":
                continue

            # Total article count for this narrative
            total_articles = (
                session.query(func.coalesce(func.sum(NarrativeTimeline.article_count), 0))
                .filter(NarrativeTimeline.narrative_name == name)
                .scalar()
            ) or 0

            if existing:
                # Update existing active run-up
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


def clear_score_cache() -> None:
    """Clear the run-up score cache (e.g. after a full analysis cycle)."""
    _runup_score_cache.clear()


# ---------------------------------------------------------------------------
# Run-up consolidation
# ---------------------------------------------------------------------------

def _runup_keywords(narrative_name: str, session: Session) -> set:
    """Collect all keywords associated with a narrative's briefs."""
    rows = (
        session.query(NarrativeTimeline)
        .filter(NarrativeTimeline.narrative_name == narrative_name)
        .all()
    )
    # Get all brief keywords for articles tracked under this narrative
    from .db import ArticleBrief, Article
    keywords: set = set()
    briefs = (
        session.query(ArticleBrief)
        .join(Article)
        .filter(
            ArticleBrief.region.isnot(None),
        )
        .all()
    )
    # Match briefs to this narrative by checking if the narrative_name
    # appears in the brief's region+keyword combo
    parts = narrative_name.lower().split("-")
    region_part = parts[0] if parts else ""
    kw_parts = set(parts[1:]) if len(parts) > 1 else set()

    for b in briefs:
        b_region = (b.region or "").lower().strip()
        if b_region != region_part and region_part not in b_region:
            continue
        b_kws = set(_get_brief_keywords(b))
        if kw_parts and not kw_parts.intersection(b_kws):
            continue
        keywords.update(b_kws)
    return keywords


def _keyword_overlap(kws_a: set, kws_b: set) -> float:
    """Return Jaccard-like overlap ratio between two keyword sets."""
    if not kws_a or not kws_b:
        return 0.0
    intersection = kws_a & kws_b
    smaller = min(len(kws_a), len(kws_b))
    return len(intersection) / smaller if smaller else 0.0


def _extract_region(narrative_name: str) -> str:
    """Extract the region prefix from a narrative name like 'middle-east-iran'."""
    # Known multi-word regions
    known_regions = [
        "middle-east", "east-asia", "south-asia", "southeast-asia",
        "central-asia", "sub-saharan-africa", "north-africa", "latin-america",
        "north-america", "eastern-europe", "western-europe",
    ]
    name_lower = narrative_name.lower()
    for r in known_regions:
        if name_lower.startswith(r):
            return r
    # Single-word region
    return name_lower.split("-")[0] if "-" in name_lower else name_lower


def consolidate_runups() -> List[RunUp]:
    """Merge overlapping active run-ups into consolidated topics.

    Run-ups with the same region AND ≥40% keyword overlap are merged.
    The highest-scoring run-up becomes the primary; others get
    ``status='merged'`` and ``merged_into_id`` set.

    Returns the list of primary (non-merged) active run-ups.
    """
    session = get_session()
    try:
        active = (
            session.query(RunUp)
            .filter(RunUp.status == "active")
            .order_by(RunUp.current_score.desc())
            .all()
        )

        # Focus Mode: also include expired (non-merged) run-ups so they can
        # be consolidated into a focused primary narrative.
        from .focus_manager import get_focused_runup_ids
        focused_ids = set(get_focused_runup_ids())

        expired_candidates: list = []
        if focused_ids:
            expired_candidates = (
                session.query(RunUp)
                .filter(
                    RunUp.status == "expired",
                    RunUp.merged_into_id.is_(None),
                )
                .order_by(RunUp.current_score.desc())
                .all()
            )

        all_candidates = active + expired_candidates

        if len(all_candidates) <= 1:
            return active

        # Gather keywords per run-up
        ru_keywords: Dict[int, set] = {}
        ru_regions: Dict[int, str] = {}
        for ru in all_candidates:
            ru_keywords[ru.id] = _runup_keywords(ru.narrative_name, session)
            ru_regions[ru.id] = _extract_region(ru.narrative_name)

        # Build merge groups using union-find
        parent: Dict[int, int] = {ru.id: ru.id for ru in all_candidates}

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra

        OVERLAP_THRESHOLD = 0.40
        FOCUS_OVERLAP_THRESHOLD = 0.25  # lower bar for focused narratives

        for i, ru_a in enumerate(all_candidates):
            for ru_b in all_candidates[i + 1:]:
                either_focused = ru_a.id in focused_ids or ru_b.id in focused_ids

                if not either_focused:
                    # Standard merge: require same region, active only
                    if ru_a.status != "active" or ru_b.status != "active":
                        continue
                    if ru_regions[ru_a.id] != ru_regions[ru_b.id]:
                        continue
                    threshold = OVERLAP_THRESHOLD
                else:
                    # Focus merge: cross-region, include expired, lower threshold
                    threshold = FOCUS_OVERLAP_THRESHOLD

                overlap = _keyword_overlap(
                    ru_keywords[ru_a.id], ru_keywords[ru_b.id]
                )
                if overlap >= threshold:
                    union(ru_a.id, ru_b.id)

        # Group by root — include all candidates (active + expired in focus)
        groups: Dict[int, List[RunUp]] = defaultdict(list)
        for ru in all_candidates:
            groups[find(ru.id)].append(ru)

        primaries: List[RunUp] = []
        merged_count = 0

        for root_id, members in groups.items():
            # If any member is focused, it becomes primary (regardless of score)
            focused_members = [m for m in members if m.id in focused_ids]
            if focused_members:
                members.sort(key=lambda r: (r.id not in focused_ids, -r.current_score))
            else:
                members.sort(key=lambda r: r.current_score, reverse=True)
            primary = members[0]

            # Aggregate article counts from merged members
            total_articles = sum(
                getattr(m, "article_count_total", 0) or 0 for m in members
            )
            primary.article_count_total = total_articles

            # Take the best score
            best_score = max(m.current_score for m in members)
            primary.current_score = best_score

            # Ensure primary is active (reactivate if it was expired)
            if primary.status == "expired" and primary.id in focused_ids:
                primary.status = "active"

            primaries.append(primary)

            # Merge the rest
            for child in members[1:]:
                child.status = "merged"
                child.merged_into_id = primary.id
                merged_count += 1

        session.commit()
        logger.info(
            "Consolidation: %d candidates (%d active + %d expired) → %d topics (%d merged).",
            len(all_candidates),
            len(active),
            len(expired_candidates),
            len(primaries),
            merged_count,
        )
        return primaries

    except Exception:
        logger.exception("Run-up consolidation failed.")
        session.rollback()
        return []
    finally:
        session.close()
