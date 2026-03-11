"""Bayesian probability update engine.

For every open DecisionNode and Consequence, the engine:
  1. Finds newly arrived ArticleBriefs whose keywords overlap.
  2. Scores evidence as supporting yes-branch or no-branch.
  3. Performs a dampened Bayesian update (max shift per cycle capped).
  4. Logs each update as a ProbabilityUpdate record.
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set

from sqlalchemy.orm import Session

from .config import config
from .db import (
    get_session,
    ArticleBrief,
    DecisionNode,
    Consequence,
    ProbabilityUpdate,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keyword helpers
# ---------------------------------------------------------------------------

def _parse_keywords(raw_json: Optional[str]) -> List[str]:
    """Parse a JSON list of keywords, returning lowercase strings."""
    if not raw_json:
        return []
    try:
        kws = json.loads(raw_json)
        if isinstance(kws, list):
            return [str(k).lower().strip() for k in kws if k]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def _brief_keywords(brief: ArticleBrief) -> Set[str]:
    """Extract the lowercase keyword set from a brief."""
    try:
        kws = json.loads(brief.keywords_json) if brief.keywords_json else []
        if isinstance(kws, list):
            return {str(k).lower().strip() for k in kws if k}
    except (json.JSONDecodeError, TypeError):
        pass
    return set()


# ---------------------------------------------------------------------------
# Evidence matching
# ---------------------------------------------------------------------------

def find_relevant_briefs(
    keywords: List[str],
    briefs: List[ArticleBrief],
    min_overlap: int = 0,
) -> List[ArticleBrief]:
    """Return briefs whose keywords overlap with *keywords* by at least
    *min_overlap* terms.

    Parameters
    ----------
    keywords:
        Target keyword list (already lowercase).
    briefs:
        Candidate briefs to search.
    min_overlap:
        Minimum number of shared keywords.  Defaults to
        ``config.min_keyword_overlap``.
    """
    if min_overlap <= 0:
        min_overlap = config.min_keyword_overlap

    if not keywords:
        return []

    target_set = set(keywords)
    matches: List[ArticleBrief] = []

    for brief in briefs:
        bkw = _brief_keywords(brief)
        overlap = target_set & bkw
        if len(overlap) >= min_overlap:
            matches.append(brief)

    return matches


def score_evidence(brief: ArticleBrief, yes_keywords: List[str], no_keywords: List[str]) -> float:
    """Score a single brief as positive (yes evidence) or negative (no evidence).

    Returns a float in roughly [-1, +1]:
      - Positive values support the *yes* branch.
      - Negative values support the *no* branch.

    The magnitude depends on:
      - keyword overlap count with each set
      - brief sentiment (negative sentiment amplifies threat-related evidence)
      - brief intensity (higher intensity = stronger signal)
    """
    bkw = _brief_keywords(brief)
    yes_overlap = len(bkw & set(yes_keywords))
    no_overlap = len(bkw & set(no_keywords))

    if yes_overlap == 0 and no_overlap == 0:
        return 0.0

    # Base directional score
    direction = yes_overlap - no_overlap  # positive favours yes

    # Intensity multiplier
    intensity_mult = {
        "low": 0.5,
        "moderate": 0.8,
        "high-threat": 1.0,
        "critical": 1.2,
    }.get(brief.intensity, 0.6)

    # Sentiment modifier (negative sentiment boosts threat evidence)
    sent_boost = 1.0
    if brief.sentiment < -0.3:
        sent_boost = 1.0 + abs(brief.sentiment) * 0.5  # up to ~1.5x

    raw = direction * intensity_mult * sent_boost

    # Normalise to roughly [-1, +1]
    return max(min(raw / 3.0, 1.0), -1.0)


# ---------------------------------------------------------------------------
# Dampened Bayesian update
# ---------------------------------------------------------------------------

def _bayesian_update(prior: float, evidence_score: float, max_shift: float) -> float:
    """Perform a dampened Bayesian-style update.

    Parameters
    ----------
    prior:
        Current probability in [0, 1].
    evidence_score:
        Aggregated evidence score (positive = towards 1, negative = towards 0).
    max_shift:
        Maximum allowed change per cycle (e.g. 0.15).

    Returns
    -------
    float
        Updated probability clamped to [0.01, 0.99].
    """
    # Convert evidence to a likelihood ratio approximation
    # Positive evidence_score -> shift prior upward
    shift = evidence_score * max_shift

    posterior = prior + shift
    return round(max(min(posterior, 0.99), 0.01), 4)


# ---------------------------------------------------------------------------
# Main update loop
# ---------------------------------------------------------------------------

def update_probabilities(new_briefs: List[ArticleBrief]) -> List[ProbabilityUpdate]:
    """Update probabilities for all open decision nodes and consequences.

    Parameters
    ----------
    new_briefs:
        Freshly created ArticleBriefs from the current processing cycle.

    Returns
    -------
    list[ProbabilityUpdate]
        All probability update audit records created.
    """
    if not new_briefs:
        return []

    session = get_session()
    updates: List[ProbabilityUpdate] = []

    try:
        # Fetch open decision nodes
        open_nodes: List[DecisionNode] = (
            session.query(DecisionNode)
            .filter(DecisionNode.status == "open")
            .all()
        )

        # Fetch predicted consequences
        open_consequences: List[Consequence] = (
            session.query(Consequence)
            .filter(Consequence.status == "predicted")
            .all()
        )

        logger.info(
            "Updating probabilities: %d open nodes, %d predicted consequences, "
            "%d new briefs.",
            len(open_nodes),
            len(open_consequences),
            len(new_briefs),
        )

        # --- Decision Nodes ---
        for node in open_nodes:
            yes_kw = _parse_keywords(node.yes_keywords_json)
            no_kw = _parse_keywords(node.no_keywords_json)
            all_kw = yes_kw + no_kw

            relevant = find_relevant_briefs(all_kw, new_briefs)
            if not relevant:
                continue

            # Aggregate evidence
            total_score = 0.0
            for brief in relevant:
                total_score += score_evidence(brief, yes_kw, no_kw)

            avg_score = total_score / len(relevant) if relevant else 0.0

            prior_yes = node.yes_probability
            posterior_yes = _bayesian_update(
                prior_yes, avg_score, config.max_probability_shift
            )
            posterior_no = round(1.0 - posterior_yes, 4)

            # Only record if there was a meaningful change
            if abs(posterior_yes - prior_yes) < 0.001:
                continue

            node.yes_probability = posterior_yes
            node.no_probability = posterior_no
            node.updated_at = datetime.utcnow()

            update = ProbabilityUpdate(
                target_type="node",
                target_id=node.id,
                prior=prior_yes,
                posterior=posterior_yes,
                evidence_count=len(relevant),
                evidence_briefs_json=json.dumps(
                    [b.id for b in relevant], ensure_ascii=False
                ),
                evidence_summary=_build_evidence_summary(relevant, yes_kw, no_kw),
                updated_at=datetime.utcnow(),
            )
            session.add(update)
            updates.append(update)

            logger.debug(
                "Node %d: %.4f -> %.4f (%d evidence briefs)",
                node.id,
                prior_yes,
                posterior_yes,
                len(relevant),
            )

        # --- Consequences ---
        for cons in open_consequences:
            cons_kw = _parse_keywords(cons.keywords_json)
            if not cons_kw:
                continue

            relevant = find_relevant_briefs(cons_kw, new_briefs)
            if not relevant:
                continue

            # For consequences, evidence matching keywords raises probability
            total_score = 0.0
            for brief in relevant:
                bkw = _brief_keywords(brief)
                overlap = len(bkw & set(cons_kw))
                intensity_mult = {
                    "low": 0.4, "moderate": 0.7, "high-threat": 1.0, "critical": 1.3,
                }.get(brief.intensity, 0.5)
                total_score += (overlap / max(len(cons_kw), 1)) * intensity_mult

            avg_score = total_score / len(relevant)
            prior = cons.probability
            posterior = _bayesian_update(prior, avg_score, config.max_probability_shift)

            if abs(posterior - prior) < 0.001:
                continue

            cons.probability = posterior

            update = ProbabilityUpdate(
                target_type="consequence",
                target_id=cons.id,
                prior=prior,
                posterior=posterior,
                evidence_count=len(relevant),
                evidence_briefs_json=json.dumps(
                    [b.id for b in relevant], ensure_ascii=False
                ),
                evidence_summary=_build_consequence_summary(relevant, cons_kw),
                updated_at=datetime.utcnow(),
            )
            session.add(update)
            updates.append(update)

            logger.debug(
                "Consequence %d: %.4f -> %.4f (%d evidence briefs)",
                cons.id,
                prior,
                posterior,
                len(relevant),
            )

        session.commit()
        logger.info("Probability engine: %d updates recorded.", len(updates))

    except Exception:
        logger.exception("Probability update cycle failed.")
        session.rollback()
        return []
    finally:
        session.close()

    return updates


# ---------------------------------------------------------------------------
# Significant shift detection
# ---------------------------------------------------------------------------

def get_significant_shifts(
    threshold: Optional[float] = None,
    session: Optional[Session] = None,
) -> List[ProbabilityUpdate]:
    """Return ProbabilityUpdate records where the shift exceeds *threshold*.

    Parameters
    ----------
    threshold:
        Minimum absolute shift to be considered significant.
        Defaults to ``config.significant_shift_threshold``.
    session:
        Optional existing session.
    """
    if threshold is None:
        threshold = config.significant_shift_threshold

    close_after = session is None
    if session is None:
        session = get_session()

    try:
        all_updates: List[ProbabilityUpdate] = session.query(ProbabilityUpdate).all()
        significant = [
            u for u in all_updates if abs(u.posterior - u.prior) >= threshold
        ]
        return significant
    finally:
        if close_after:
            session.close()


# ---------------------------------------------------------------------------
# Summary builders
# ---------------------------------------------------------------------------

def _build_evidence_summary(
    briefs: List[ArticleBrief],
    yes_kw: List[str],
    no_kw: List[str],
) -> str:
    """Build a short textual summary of the evidence found."""
    yes_count = 0
    no_count = 0
    for b in briefs:
        s = score_evidence(b, yes_kw, no_kw)
        if s > 0:
            yes_count += 1
        elif s < 0:
            no_count += 1

    parts = [f"{len(briefs)} relevant articles found."]
    if yes_count:
        parts.append(f"{yes_count} support YES branch.")
    if no_count:
        parts.append(f"{no_count} support NO branch.")
    return " ".join(parts)


def _build_consequence_summary(
    briefs: List[ArticleBrief],
    keywords: List[str],
) -> str:
    """Build a short textual summary for consequence evidence."""
    parts = [f"{len(briefs)} articles match consequence keywords."]
    avg_sent = sum(b.sentiment for b in briefs) / len(briefs) if briefs else 0
    parts.append(f"Avg sentiment: {avg_sent:.2f}.")
    intensities = [b.intensity for b in briefs]
    if "critical" in intensities:
        parts.append("Contains critical-intensity articles.")
    elif "high-threat" in intensities:
        parts.append("Contains high-threat articles.")
    return " ".join(parts)
