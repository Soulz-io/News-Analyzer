"""Polymarket integration: fetch prediction markets and match to run-ups.

Public API:
  GET https://gamma-api.polymarket.com/markets?closed=false&limit=50

Matching uses rapidfuzz (already a dependency) for fuzzy string matching.
No extra Claude API calls are needed.
"""

import json
import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import httpx
from rapidfuzz import fuzz
from sqlalchemy.orm import Session

from .db import (
    get_session,
    RunUp,
    DecisionNode,
    PolymarketMatch,
    PolymarketPriceHistory,
)

logger = logging.getLogger(__name__)

POLYMARKET_EVENTS_API = "https://gamma-api.polymarket.com/events"
MATCH_THRESHOLD = 45  # minimum fuzzy match score (0-100)
FETCH_TIMEOUT = 15

# ---------------------------------------------------------------------------
# Geopolitical market filter
# ---------------------------------------------------------------------------

GEOPOLITICAL_INCLUDE = {
    "war", "military", "sanctions", "election", "president", "nato", "nuclear",
    "invasion", "ceasefire", "troops", "missile", "conflict", "regime", "coup",
    "alliance", "embargo", "territorial", "sovereignty", "genocide", "humanitarian",
    "refugee", "terrorism", "insurgency", "strike", "bombing", "deployment",
    "iran", "russia", "ukraine", "china", "taiwan", "israel", "gaza", "palestine",
    "north korea", "syria", "yemen", "sudan", "myanmar", "houthi",
    "oil", "energy", "opec", "commodity", "tariff", "trade war", "pipeline",
    "diplomat", "treaty", "annexation", "mobilization", "escalation",
    "federal reserve", "rate cut", "rate hike", "recession", "inflation",
}

GEOPOLITICAL_EXCLUDE = {
    "super bowl", "nfl", "nba", "mlb", "premier league", "champions league",
    "world cup", "basketball", "football score", "touchdown", "playoffs",
    "world series", "stanley cup", "formula 1", "ufc", "boxing",
    "oscars", "grammy", "emmy", "bachelor", "reality tv", "netflix",
    "bitcoin etf", "memecoin", "nft", "solana price", "dogecoin",
    "tiktok ban", "youtube", "twitch", "streaming", "spotify",
}


def is_geopolitical(market: Dict) -> bool:
    """Check if a Polymarket market is geopolitically relevant."""
    text = f"{market.get('question', '')} {market.get('_event_title', '')}".lower()
    if any(kw in text for kw in GEOPOLITICAL_EXCLUDE):
        return False
    return any(kw in text for kw in GEOPOLITICAL_INCLUDE)


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_polymarket_markets(
    keywords: Optional[List[str]] = None,
    limit: int = 100,
) -> List[Dict]:
    """Fetch open markets from Polymarket's public events API.

    Uses the /events endpoint sorted by 24h volume (most active first),
    then extracts individual markets from each event.
    The gamma-api /markets endpoint does not support text search,
    so we fetch top-volume events and match client-side.
    """
    params = {
        "closed": "false",
        "limit": str(min(limit, 100)),
        "order": "volume24hr",
        "ascending": "false",
    }

    all_markets: List[Dict] = []

    try:
        with httpx.Client(timeout=FETCH_TIMEOUT) as client:
            resp = client.get(POLYMARKET_EVENTS_API, params=params)
            if resp.status_code == 200:
                events = resp.json()
                if isinstance(events, list):
                    for event in events:
                        event_title = event.get("title", "")
                        event_slug = event.get("slug", "")
                        for market in (event.get("markets") or []):
                            # Enrich market with event-level info
                            market["_event_title"] = event_title
                            market["_event_slug"] = event_slug
                            all_markets.append(market)
    except Exception as e:
        logger.warning("Polymarket API fetch failed: %s", e)

    logger.info("Fetched %d markets from %d events.", len(all_markets),
                len(set(m.get("_event_slug", "") for m in all_markets)))

    # Filter to geopolitically relevant markets only
    geo_markets = [m for m in all_markets if is_geopolitical(m)]
    logger.info(
        "Polymarket: %d markets fetched, %d geopolitically relevant.",
        len(all_markets), len(geo_markets),
    )
    return geo_markets


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_market_to_question(
    market_question: str,
    node_question: str,
    node_keywords: List[str],
    event_title: str = "",
) -> float:
    """Score how well a Polymarket question matches a decision node question.

    Uses fuzzy string similarity + keyword overlap + event title matching.
    Returns a score 0-100.
    """
    # Fuzzy question similarity (0-100)
    # Use token_set_ratio — handles asymmetric lengths well
    # (short Polymarket questions vs long decision-node questions)
    q_score = fuzz.token_set_ratio(
        market_question.lower(),
        node_question.lower(),
    )

    # Also check event title similarity
    if event_title:
        et_score = fuzz.token_set_ratio(
            event_title.lower(),
            node_question.lower(),
        )
        q_score = max(q_score, et_score)

    # Keyword overlap — check if multi-word keyword phrases appear in combined text
    combined_text = f"{market_question} {event_title}".lower()

    kw_score = 0
    if node_keywords:
        kw_hits = 0
        for kw in node_keywords:
            kw_lower = kw.lower().strip()
            if not kw_lower:
                continue
            # Check full phrase first
            if kw_lower in combined_text:
                kw_hits += 1
                continue
            # Check individual words of multi-word keywords
            parts = kw_lower.split()
            if len(parts) > 1 and any(p in combined_text for p in parts if len(p) > 3):
                kw_hits += 0.5

        kw_score = min(100, kw_hits * 25)

    return q_score * 0.6 + kw_score * 0.4


def find_matches_for_runup(
    run_up: RunUp,
    nodes: List[DecisionNode],
    markets: List[Dict],
) -> List[Tuple[DecisionNode, Dict, float]]:
    """Find Polymarket markets that match a run-up's decision nodes.

    Returns list of (node, market, score) tuples above MATCH_THRESHOLD.
    """
    matches: List[Tuple[DecisionNode, Dict, float]] = []

    for node in nodes:
        try:
            yes_kw = json.loads(node.yes_keywords_json) if node.yes_keywords_json else []
        except Exception:
            yes_kw = []
        try:
            no_kw = json.loads(node.no_keywords_json) if node.no_keywords_json else []
        except Exception:
            no_kw = []

        all_kw = yes_kw + no_kw

        for market in markets:
            mq = market.get("question", "")
            if not mq:
                continue

            et = market.get("_event_title", "")
            score = match_market_to_question(mq, node.question, all_kw, event_title=et)

            if score >= MATCH_THRESHOLD:
                matches.append((node, market, score))

    # Sort by score descending, keep best match per node
    matches.sort(key=lambda x: x[2], reverse=True)

    seen_nodes: set = set()
    best_matches: List[Tuple[DecisionNode, Dict, float]] = []
    for node, market, score in matches:
        if node.id not in seen_nodes:
            seen_nodes.add(node.id)
            best_matches.append((node, market, score))

    return best_matches


# ---------------------------------------------------------------------------
# Probability calibration
# ---------------------------------------------------------------------------

def calibrate_probability(
    bayesian_prob: float,
    polymarket_prob: float,
    polymarket_volume: float,
    polymarket_liquidity: float,
) -> float:
    """Combine Bayesian estimate with Polymarket odds.

    Volume-weighted blend: 70/30 at low volume, shifting to 40/60
    for high-volume (>$1M) markets.
    """
    volume_factor = min(1.0, math.log1p(polymarket_volume) / math.log1p(1_000_000))
    liquidity_factor = min(1.0, math.log1p(polymarket_liquidity) / math.log1p(100_000))

    market_confidence = (volume_factor + liquidity_factor) / 2.0
    poly_weight = 0.30 + (market_confidence * 0.30)
    bayes_weight = 1.0 - poly_weight

    calibrated = bayesian_prob * bayes_weight + polymarket_prob * poly_weight
    return round(max(0.01, min(0.99, calibrated)), 4)


# ---------------------------------------------------------------------------
# Price drift detection
# ---------------------------------------------------------------------------

def detect_price_drift(
    session: Session,
    market_id: str,
    current_price: float,
    threshold: float = 0.05,
) -> Optional[float]:
    """Check if a Polymarket price moved >threshold over 24h.

    Returns the drift magnitude (can be negative), or None if below threshold.
    """
    cutoff = datetime.utcnow() - timedelta(hours=24)
    oldest = (
        session.query(PolymarketPriceHistory)
        .filter(
            PolymarketPriceHistory.polymarket_id == market_id,
            PolymarketPriceHistory.recorded_at >= cutoff,
        )
        .order_by(PolymarketPriceHistory.recorded_at.asc())
        .first()
    )
    if oldest is None:
        return None
    drift = current_price - oldest.yes_price
    return drift if abs(drift) >= threshold else None


# ---------------------------------------------------------------------------
# Main update loop
# ---------------------------------------------------------------------------

def update_polymarket_matches() -> int:
    """Fetch Polymarket data and update matches for all active run-ups.

    Returns the number of matches created or updated.
    """
    session = get_session()
    match_count = 0

    try:
        active_runups = (
            session.query(RunUp)
            .filter(RunUp.status == "active", RunUp.merged_into_id.is_(None))
            .all()
        )

        if not active_runups:
            return 0

        # Collect keywords from all run-ups for bulk fetching
        all_keywords: set = set()
        runup_nodes: Dict[int, List[DecisionNode]] = {}
        for ru in active_runups:
            nodes = (
                session.query(DecisionNode)
                .filter(DecisionNode.run_up_id == ru.id, DecisionNode.status == "open")
                .all()
            )
            runup_nodes[ru.id] = nodes

            name_parts = ru.narrative_name.lower().replace("-", " ").split()
            all_keywords.update(p for p in name_parts if len(p) > 3)

        markets = fetch_polymarket_markets(
            keywords=list(all_keywords)[:10]
        )

        if not markets:
            return 0

        for ru in active_runups:
            nodes = runup_nodes.get(ru.id, [])
            if not nodes:
                continue

            matches = find_matches_for_runup(ru, nodes, markets)

            for node, market, score in matches:
                outcome_prices = market.get("outcomePrices", [])
                try:
                    if isinstance(outcome_prices, str):
                        outcome_prices = json.loads(outcome_prices)
                    yes_price = float(outcome_prices[0]) if outcome_prices else 0.5
                    no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else (1.0 - yes_price)
                except (ValueError, IndexError, TypeError):
                    yes_price = 0.5
                    no_price = 0.5

                volume = float(market.get("volume", 0) or 0)
                liquidity = float(market.get("liquidity", 0) or 0)

                calibrated = calibrate_probability(
                    node.yes_probability,
                    yes_price,
                    volume,
                    liquidity,
                )

                end_date_str = market.get("endDate")
                end_date = None
                if end_date_str:
                    try:
                        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                    except Exception:
                        pass

                slug = market.get("slug", "")
                poly_url = f"https://polymarket.com/event/{slug}" if slug else ""

                existing = (
                    session.query(PolymarketMatch)
                    .filter(
                        PolymarketMatch.run_up_id == ru.id,
                        PolymarketMatch.polymarket_id == str(market.get("id", "")),
                    )
                    .first()
                )

                if existing:
                    existing.outcome_yes_price = yes_price
                    existing.outcome_no_price = no_price
                    existing.volume = volume
                    existing.liquidity = liquidity
                    existing.match_score = score
                    existing.calibrated_probability = calibrated
                    existing.updated_at = datetime.utcnow()
                else:
                    pm = PolymarketMatch(
                        run_up_id=ru.id,
                        decision_node_id=node.id,
                        polymarket_id=str(market.get("id", "")),
                        polymarket_slug=slug,
                        polymarket_question=market.get("question", ""),
                        polymarket_url=poly_url,
                        outcome_yes_price=yes_price,
                        outcome_no_price=no_price,
                        volume=volume,
                        liquidity=liquidity,
                        end_date=end_date,
                        match_score=score,
                        match_method="keyword",
                        calibrated_probability=calibrated,
                    )
                    session.add(pm)

                match_count += 1

        session.commit()
        logger.info("Polymarket: %d matches updated.", match_count)

        # Record price history for drift detection
        try:
            for match in session.query(PolymarketMatch).filter(
                PolymarketMatch.run_up_id.in_([ru.id for ru in active_runups])
            ).all():
                snapshot = PolymarketPriceHistory(
                    polymarket_id=match.polymarket_id,
                    question=match.polymarket_question[:500],
                    yes_price=match.outcome_yes_price,
                    volume=match.volume,
                )
                session.add(snapshot)
            session.commit()
            logger.info("Polymarket: recorded %d price snapshots.", len(active_runups))
        except Exception:
            session.rollback()
            logger.exception("Failed to record Polymarket price history.")

        # Auto-confirm nodes where Polymarket has resolved (≥97%)
        try:
            _auto_confirm_resolved_markets(session)
        except Exception:
            session.rollback()
            logger.exception("Auto-confirmation pass failed (non-fatal).")

    except Exception:
        logger.exception("Polymarket update cycle failed.")
        session.rollback()
    finally:
        session.close()

    return match_count


# ---------------------------------------------------------------------------
# Auto-confirmation of resolved markets
# ---------------------------------------------------------------------------

def _auto_confirm_resolved_markets(session: Session) -> int:
    """Auto-confirm decision nodes when their matched Polymarket reaches >=97%.

    For YES confirmation: outcome_yes_price >= 0.97
    For NO confirmation:  outcome_yes_price <= 0.03 (i.e. outcome_no_price >= 0.97)

    After confirming a node, attempts to generate a cascading child tree for
    the next level of analysis.

    Returns the number of nodes confirmed.
    """
    confirmed_count = 0

    # --- YES branch: market strongly says YES ---
    yes_matches = (
        session.query(PolymarketMatch)
        .filter(PolymarketMatch.outcome_yes_price >= 0.97)
        .filter(PolymarketMatch.decision_node_id.isnot(None))
        .all()
    )

    for match in yes_matches:
        node = (
            session.query(DecisionNode)
            .filter(DecisionNode.id == match.decision_node_id)
            .first()
        )
        if node is None or node.status != "open":
            continue

        node.status = "confirmed_yes"
        node.confirmed_at = datetime.utcnow()
        node.evidence = (
            f"Polymarket '{match.polymarket_question}' reached "
            f"{match.outcome_yes_price:.0%} — auto-confirmed"
        )
        confirmed_count += 1
        logger.info(
            "Auto-confirmed YES: node %d (question=%r) — Polymarket at %.0f%%",
            node.id, node.question[:80], match.outcome_yes_price * 100,
        )

    # --- NO branch: market strongly says NO ---
    no_matches = (
        session.query(PolymarketMatch)
        .filter(PolymarketMatch.outcome_yes_price <= 0.03)
        .filter(PolymarketMatch.decision_node_id.isnot(None))
        .all()
    )

    for match in no_matches:
        node = (
            session.query(DecisionNode)
            .filter(DecisionNode.id == match.decision_node_id)
            .first()
        )
        if node is None or node.status != "open":
            continue

        node.status = "confirmed_no"
        node.confirmed_at = datetime.utcnow()
        node.evidence = (
            f"Polymarket '{match.polymarket_question}' reached "
            f"{match.outcome_no_price:.0%} NO — auto-confirmed"
        )
        confirmed_count += 1
        logger.info(
            "Auto-confirmed NO: node %d (question=%r) — Polymarket NO at %.0f%%",
            node.id, node.question[:80], match.outcome_no_price * 100,
        )

    if confirmed_count > 0:
        session.commit()
        logger.info("Auto-confirmed %d decision nodes from Polymarket.", confirmed_count)

        # Trigger cascading child-tree generation for each confirmed node
        confirmed_nodes = (
            session.query(DecisionNode)
            .filter(DecisionNode.status.in_(["confirmed_yes", "confirmed_no"]))
            .filter(DecisionNode.confirmed_at.isnot(None))
            .all()
        )
        for node in confirmed_nodes:
            # Only generate children if this node doesn't already have child nodes
            has_children = (
                session.query(DecisionNode)
                .filter(DecisionNode.parent_node_id == node.id)
                .first()
            )
            if has_children:
                continue
            try:
                from .tree_generator import generate_child_tree
                result = generate_child_tree(node.id)
                if result:
                    logger.info(
                        "Generated child tree for confirmed node %d: %s",
                        node.id, result.get("status", "?"),
                    )
            except Exception:
                logger.exception(
                    "Child tree generation failed for node %d (non-fatal).",
                    node.id,
                )

    return confirmed_count


# ---------------------------------------------------------------------------
# Manual Polymarket linking (Focus Mode)
# ---------------------------------------------------------------------------

def create_manual_polymarket_match(
    run_up_id: int,
    polymarket_url: str,
    db: Optional[Session] = None,
) -> Optional[Dict]:
    """Create a PolymarketMatch from a user-provided Polymarket URL.

    Fetches the event/market data from the Polymarket Gamma API using the
    URL slug, then creates a PolymarketMatch with match_method='manual'
    and match_score=100.

    Returns a summary dict on success, None on failure.
    """
    close_session = False
    if db is None:
        db = get_session()
        close_session = True

    try:
        # Extract slug from URL: https://polymarket.com/event/<slug>
        slug = polymarket_url.rstrip("/").split("/")[-1]
        if not slug:
            logger.warning("Manual PM link: no slug extracted from %s", polymarket_url)
            return None

        # Fetch market data
        resp = httpx.get(
            POLYMARKET_EVENTS_API,
            params={"slug": slug},
            timeout=FETCH_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(
                "Manual PM link: API returned %d for slug %s", resp.status_code, slug
            )
            return None

        events = resp.json()
        if not events:
            logger.warning("Manual PM link: no events found for slug %s", slug)
            return None

        event = events[0]
        markets = event.get("markets", [])
        if not markets:
            logger.warning("Manual PM link: no markets in event %s", slug)
            return None

        market = markets[0]
        polymarket_id = str(market.get("id", slug))
        question = market.get("question", event.get("title", ""))
        yes_price = None
        volume = None

        try:
            yes_price = float(market.get("outcomePrices", "[0.5]").strip("[]").split(",")[0])
        except (ValueError, IndexError):
            yes_price = 0.5

        try:
            volume = float(market.get("volume", 0))
        except (ValueError, TypeError):
            volume = 0

        # Find root decision node for this run-up
        root_node = (
            db.query(DecisionNode)
            .filter(DecisionNode.run_up_id == run_up_id, DecisionNode.depth == 0)
            .first()
        )

        # Create or update PolymarketMatch
        existing = (
            db.query(PolymarketMatch)
            .filter(
                PolymarketMatch.run_up_id == run_up_id,
                PolymarketMatch.polymarket_id == polymarket_id,
            )
            .first()
        )

        if existing:
            existing.outcome_yes_price = yes_price
            existing.volume = volume
            existing.match_score = 100.0
            existing.match_method = "manual"
            existing.polymarket_question = question
            existing.polymarket_url = polymarket_url
            existing.updated_at = datetime.utcnow()
            match = existing
        else:
            match = PolymarketMatch(
                run_up_id=run_up_id,
                decision_node_id=root_node.id if root_node else None,
                polymarket_id=polymarket_id,
                polymarket_question=question,
                polymarket_url=polymarket_url,
                outcome_yes_price=yes_price,
                volume=volume,
                match_score=100.0,
                match_method="manual",
            )
            db.add(match)

        # Record price history
        history = PolymarketPriceHistory(
            polymarket_id=polymarket_id,
            question=question,
            yes_price=yes_price,
            volume=volume,
            recorded_at=datetime.utcnow(),
        )
        db.add(history)
        db.commit()

        logger.info(
            "Manual PM link: linked run-up %d to %s (YES=%.2f, vol=%.0f)",
            run_up_id, question[:50], yes_price or 0, volume or 0,
        )
        return {
            "status": "linked",
            "polymarket_id": polymarket_id,
            "question": question,
            "yes_price": yes_price,
            "volume": volume,
        }

    except Exception:
        db.rollback()
        logger.exception("Manual Polymarket link failed for run-up %d", run_up_id)
        return None
    finally:
        if close_session:
            db.close()
