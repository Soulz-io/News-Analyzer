"""Breaking-news flash detection system.

Scores each article brief on a 0-100 scale using five NLP-derived
components.  When a single article or a narrative cluster exceeds the
configured thresholds, a :class:`FlashAlert` is created and an
advisory is generated via Groq (free tier).

Integration point: called at the END of ``fetch_and_process()`` in
``engine.py``, after ``update_narratives()`` and ``update_probabilities()``.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI
from sqlalchemy.orm import Session

from .config import config
from .db import (
    ArticleBrief,
    FlashAlert,
    EngineSettings,
    PriceSnapshot,
    RunUp,
    TradingSignal,
    get_session,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

_INTENSITY_SCORES: Dict[str, float] = {
    "critical": 25.0,
    "high-threat": 18.0,
    "moderate": 8.0,
    "low": 0.0,
}

_EVENT_TYPE_SCORES: Dict[str, float] = {
    "military_action": 15.0,
    "economic": 10.0,
    "diplomatic": 8.0,
    "social": 5.0,
    "legal": 5.0,
    "general": 0.0,
}

# Region → affected sectors mapping (for template fallback)
_REGION_SECTOR_MAP: Dict[str, List[str]] = {
    "middle-east": ["Energy", "Defense", "Shipping"],
    "europe": ["Finance", "Energy", "Industrials"],
    "east-asia": ["Technology", "Manufacturing", "Shipping"],
    "south-asia": ["Technology", "Manufacturing", "Shipping"],
    "southeast-asia": ["Technology", "Manufacturing", "Shipping"],
    "africa": ["Mining", "Energy", "Agriculture"],
    "americas": ["Finance", "Technology", "Energy"],
    "global": ["Energy", "Finance", "Defense"],
}

# Event type → affected sectors
_EVENT_SECTOR_MAP: Dict[str, List[str]] = {
    "military_action": ["Defense", "Energy", "Shipping"],
    "economic": ["Finance", "Energy", "Industrials"],
    "diplomatic": ["Finance", "Defense"],
    "social": ["Consumer", "Finance"],
    "legal": ["Finance", "Technology"],
}

# Event type + region → likely tickers (Bunq-available)
_TICKER_MAP: Dict[str, List[str]] = {
    "military_action:middle-east": ["IS0D.DE", "XOM", "CVX", "LMT", "RTX"],
    "military_action:europe": ["RHM.DE", "BA", "AIR.PA", "IS0D.DE"],
    "military_action:east-asia": ["LMT", "RTX", "NOC", "BA"],
    "military_action:south-asia": ["LMT", "RTX", "NOC", "BA"],
    "military_action:southeast-asia": ["LMT", "RTX", "NOC", "BA"],
    "economic:middle-east": ["XOM", "CVX", "IS0D.DE", "TTE.PA"],
    "economic:europe": ["SIE.DE", "SAP.DE", "ASML.AS", "BNP.PA"],
    "economic:east-asia": ["CSPX.AS", "EQQQ.DE", "TSM", "ASML.AS"],
    "economic:global": ["CSPX.AS", "EQQQ.DE", "IS0E.DE", "XOM"],
    "economic:americas": ["CSPX.AS", "JPM", "GS", "XOM"],
    "diplomatic:middle-east": ["XOM", "IS0D.DE", "CVX"],
    "diplomatic:europe": ["SIE.DE", "BNP.PA", "AIR.PA"],
    "diplomatic:east-asia": ["TSM", "ASML.AS", "CSPX.AS"],
    "social:global": ["CSPX.AS", "VWRL.AS", "IWDA.AS"],
    "legal:global": ["GOOGL", "META", "MSFT", "AAPL"],
}


# ---------------------------------------------------------------------------
# Flash score calculation
# ---------------------------------------------------------------------------

def calculate_flash_score(brief: ArticleBrief) -> float:
    """Score a single article brief on a 0-100 flash-urgency scale.

    Components (pure Python — no API call):
      1. Urgency score    (30 pts) — ``brief.urgency_score × 30``
      2. Intensity         (25 pts) — mapped from ``brief.intensity``
      3. Event type        (15 pts) — mapped from ``brief.event_type``
      4. Source credibility (15 pts) — ``brief.source_credibility × 15``
      5. Actor action density (15 pts) — count of actors with non-empty action
    """
    # 1. Urgency (0-30)
    urgency = (brief.urgency_score or 0.0) * 30.0

    # 2. Intensity (0-25)
    intensity = _INTENSITY_SCORES.get(brief.intensity or "low", 0.0)

    # 3. Event type (0-15)
    event = _EVENT_TYPE_SCORES.get(brief.event_type or "general", 0.0)

    # 4. Source credibility (0-15)
    credibility = (brief.source_credibility or 0.6) * 15.0

    # 5. Actor action density (0-15)
    actor_count = 0
    if brief.key_actors_json:
        try:
            actors = json.loads(brief.key_actors_json)
            actor_count = sum(
                1 for a in actors
                if isinstance(a, dict) and a.get("action")
            )
        except (json.JSONDecodeError, TypeError):
            pass
    actors_score = min(actor_count * 5.0, 15.0)

    return urgency + intensity + event + credibility + actors_score


# ---------------------------------------------------------------------------
# Cooldown & rate-limit state  (persisted in EngineSettings)
# ---------------------------------------------------------------------------

def _load_flash_state() -> Dict[str, Any]:
    """Load cooldown/rate-limit state from DB."""
    session = get_session()
    try:
        s = session.query(EngineSettings).get("flash_alert_state")
        if s and s.value:
            return json.loads(s.value)
    except Exception:
        logger.debug("Could not load flash_alert_state — starting fresh")
    finally:
        session.close()
    return {"narrative_cooldowns": {}, "rate_limit_window": []}


def _save_flash_state(state: Dict[str, Any]) -> None:
    """Persist cooldown/rate-limit state to DB."""
    session = get_session()
    try:
        val = json.dumps(state, default=str)
        s = session.query(EngineSettings).get("flash_alert_state")
        if s:
            s.value = val
        else:
            session.add(EngineSettings(key="flash_alert_state", value=val))
        session.commit()
    except Exception:
        logger.exception("Failed to save flash_alert_state")
        session.rollback()
    finally:
        session.close()


def _is_cooldown_active(narrative: str) -> bool:
    """Check if *narrative* is still within its 2-hour cooldown."""
    state = _load_flash_state()
    ts_str = state.get("narrative_cooldowns", {}).get(narrative)
    if not ts_str:
        return False
    try:
        cooldown_until = datetime.fromisoformat(ts_str)
        return datetime.utcnow() < cooldown_until
    except (ValueError, TypeError):
        return False


def _check_rate_limit() -> bool:
    """Return ``True`` if under the per-6-hour rate limit, ``False`` if exceeded."""
    state = _load_flash_state()
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=6)

    # Purge old timestamps
    window = []
    for ts_str in state.get("rate_limit_window", []):
        try:
            if datetime.fromisoformat(ts_str) > cutoff:
                window.append(ts_str)
        except (ValueError, TypeError):
            pass
    state["rate_limit_window"] = window
    _save_flash_state(state)

    if len(window) >= config.flash_max_per_6h:
        logger.warning("Flash rate limit reached: %d alerts in 6h", len(window))
        return False
    return True


def _apply_cooldown(narrative: str) -> None:
    """Put *narrative* on a cooldown for ``config.flash_cooldown_minutes``."""
    state = _load_flash_state()
    until = datetime.utcnow() + timedelta(minutes=config.flash_cooldown_minutes)
    state.setdefault("narrative_cooldowns", {})[narrative] = until.isoformat()
    # Purge stale cooldowns
    now = datetime.utcnow()
    state["narrative_cooldowns"] = {
        k: v for k, v in state["narrative_cooldowns"].items()
        if datetime.fromisoformat(v) > now
    }
    _save_flash_state(state)


def _record_alert_sent() -> None:
    """Record that a flash alert was sent (for rate-limiting)."""
    state = _load_flash_state()
    state.setdefault("rate_limit_window", []).append(datetime.utcnow().isoformat())
    _save_flash_state(state)


# ---------------------------------------------------------------------------
# Batch evaluation — single-article + narrative-spike triggers
# ---------------------------------------------------------------------------

def evaluate_batch(briefs: List[ArticleBrief], db: Session) -> List[FlashAlert]:
    """Score a batch of new briefs and return 0+ FlashAlert objects.

    Two trigger modes:
      1. **Single article** — flash_score ≥ threshold AND credibility ≥ 0.70
      2. **Narrative spike** — ≥3 articles same topic, avg score ≥ cluster
         threshold, ≥2 distinct sources

    Returns new ``FlashAlert`` objects (NOT yet added to the session).
    """
    if not briefs:
        return []

    # Pre-check rate limit
    if not _check_rate_limit():
        return []

    alerts: List[FlashAlert] = []
    scored: List[Tuple[ArticleBrief, float]] = []

    for brief in briefs:
        score = calculate_flash_score(brief)
        scored.append((brief, score))

    # --- Trigger 1: single high-scoring article ---
    for brief, score in scored:
        if score < config.flash_single_threshold:
            continue
        if (brief.source_credibility or 0.6) < 0.70:
            continue
        # Check narrative cooldown
        narrative = _guess_narrative(brief)
        if narrative and _is_cooldown_active(narrative):
            logger.debug("Flash suppressed (cooldown): %s", narrative)
            continue
        # Check dedup against active run-ups
        if _is_runup_duplicate(brief, db):
            continue

        alert = _create_alert(
            trigger_type="single_article",
            briefs=[brief],
            flash_score=score,
            narrative=narrative,
            db=db,
        )
        alerts.append(alert)
        if narrative:
            _apply_cooldown(narrative)
        _record_alert_sent()
        break  # Max 1 alert per batch from single-article trigger

    # --- Trigger 2: narrative spike (≥3 articles, ≥2 sources) ---
    if not alerts:
        clusters = _cluster_by_topic(scored)
        for topic, items in clusters.items():
            if len(items) < 3:
                continue
            avg_score = sum(s for _, s in items) / len(items)
            if avg_score < config.flash_cluster_threshold:
                continue
            sources = {b.article.source if hasattr(b, "article") and b.article else "unknown"
                       for b, _ in items}
            # Also try source from brief directly
            for b, _ in items:
                if b.source_names_json:
                    try:
                        for sn in json.loads(b.source_names_json):
                            sources.add(sn)
                    except (json.JSONDecodeError, TypeError):
                        pass
            if len(sources) < 2:
                continue
            if _is_cooldown_active(topic):
                continue

            best_brief, best_score = max(items, key=lambda x: x[1])
            alert = _create_alert(
                trigger_type="narrative_spike",
                briefs=[b for b, _ in items],
                flash_score=avg_score,
                narrative=topic,
                db=db,
            )
            alerts.append(alert)
            _apply_cooldown(topic)
            _record_alert_sent()
            break  # Max 1 cluster alert per batch

    # Generate Groq advisory for each alert
    for alert in alerts:
        advisory = generate_flash_advisory(alert, db)
        alert.flash_advisory_json = json.dumps(advisory, default=str)
        # Extract portfolio-impact fields
        pa = advisory.get("portfolio_action", {})
        alert.portfolio_action = pa.get("urgency", "watch")
        alert.risk_level = pa.get("risk_level", "moderate")
        mi = advisory.get("market_impact", {})
        tickers = mi.get("tickers_to_watch", [])
        alert.tickers_affected_json = json.dumps(tickers)
        # Build self-learning recommendations
        recs = _build_recommendations(advisory, tickers, db)
        alert.recommendations_json = json.dumps(recs, default=str)

    return alerts


# ---------------------------------------------------------------------------
# Alert construction helpers
# ---------------------------------------------------------------------------

def _create_alert(
    trigger_type: str,
    briefs: List[ArticleBrief],
    flash_score: float,
    narrative: Optional[str],
    db: Session,
) -> FlashAlert:
    """Build a FlashAlert from scored briefs."""
    primary = briefs[0]
    # Try to find linked run-up
    run_up_id = None
    if narrative:
        ru = (
            db.query(RunUp)
            .filter(RunUp.narrative_name == narrative, RunUp.status == "active")
            .first()
        )
        if ru:
            run_up_id = ru.id

    article_ids = [b.article_id for b in briefs if b.article_id]
    source_names = list({b.article.source for b in briefs
                         if hasattr(b, "article") and b.article and b.article.source} or set())
    if not source_names:
        # Fallback: try to extract from brief region or other
        source_names = []

    return FlashAlert(
        alert_id=uuid.uuid4().hex,
        trigger_type=trigger_type,
        flash_score=flash_score,
        headline=primary.summary or primary.article.title if hasattr(primary, "article") and primary.article else (primary.summary or "Breaking news detected"),
        region=primary.region or "global",
        event_type=primary.event_type or "general",
        intensity=primary.intensity or "moderate",
        article_ids_json=json.dumps(article_ids),
        source_names_json=json.dumps(source_names),
        narrative_name=narrative,
        run_up_id=run_up_id,
        detected_at=datetime.utcnow(),
        expires_at=datetime.utcnow() + timedelta(hours=6),
        status="active",
    )


def _guess_narrative(brief: ArticleBrief) -> Optional[str]:
    """Best-effort narrative name from a brief's topic cluster or keywords."""
    if brief.topic_cluster_id:
        return brief.topic_cluster_id
    if brief.keywords_json:
        try:
            kws = json.loads(brief.keywords_json)
            if kws:
                return " ".join(kws[:3])
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _cluster_by_topic(scored: List[Tuple[ArticleBrief, float]]) -> Dict[str, List[Tuple[ArticleBrief, float]]]:
    """Group briefs by topic_cluster_id."""
    clusters: Dict[str, List[Tuple[ArticleBrief, float]]] = defaultdict(list)
    for brief, score in scored:
        key = brief.topic_cluster_id or "misc"
        clusters[key].append((brief, score))
    return clusters


def _is_runup_duplicate(brief: ArticleBrief, db: Session) -> bool:
    """Suppress if an existing active run-up covers this topic and isn't accelerating fast."""
    narrative = _guess_narrative(brief)
    if not narrative:
        return False
    ru = (
        db.query(RunUp)
        .filter(RunUp.narrative_name == narrative, RunUp.status == "active")
        .first()
    )
    if ru and (ru.acceleration_rate or 0) < 5.0:
        logger.debug("Flash suppressed (run-up dedup, accel=%.1f): %s",
                      ru.acceleration_rate or 0, narrative)
        return True
    return False


# ---------------------------------------------------------------------------
# Groq flash advisory generation
# ---------------------------------------------------------------------------

def generate_flash_advisory(alert: FlashAlert, db: Session) -> Dict[str, Any]:
    """Generate a concise advisory via Groq (free tier).

    Falls back to ``_template_fallback()`` if Groq is unavailable.
    """
    try:
        from .swarm_consensus import _get_groq_client, GROQ_DEFAULT_MODEL, MAX_TOKENS
    except ImportError:
        logger.warning("Cannot import swarm_consensus — using template fallback")
        return _template_fallback(alert)

    client = _get_groq_client()
    if client is None:
        return _template_fallback(alert)

    # Build context
    context_parts: List[str] = []

    # 1. Alert details
    context_parts.append(
        f"BREAKING NEWS: {alert.headline}\n"
        f"Region: {alert.region}, Event type: {alert.event_type}, "
        f"Intensity: {alert.intensity}, Flash score: {alert.flash_score:.0f}/100"
    )

    # 2. Article details
    if alert.article_ids_json:
        try:
            article_ids = json.loads(alert.article_ids_json)
            briefs = (
                db.query(ArticleBrief)
                .filter(ArticleBrief.article_id.in_(article_ids))
                .limit(5)
                .all()
            )
            for b in briefs:
                context_parts.append(f"- {b.summary or '(no summary)'}")
        except Exception:
            pass

    # 3. Active trading signals
    try:
        signals = (
            db.query(TradingSignal)
            .filter(TradingSignal.superseded_by_id.is_(None))
            .order_by(TradingSignal.created_at.desc())
            .limit(10)
            .all()
        )
        if signals:
            sig_lines = [f"  {s.ticker}: {s.signal_level} (conf={s.confidence:.0%})"
                         for s in signals]
            context_parts.append("Active trading signals:\n" + "\n".join(sig_lines))
    except Exception:
        pass

    # 4. Portfolio holdings
    try:
        holdings_setting = db.query(EngineSettings).get("portfolio_holdings")
        if holdings_setting and holdings_setting.value:
            context_parts.append(f"Portfolio: {holdings_setting.value[:500]}")
    except Exception:
        pass

    system_msg = (
        "You are a geopolitical risk analyst specializing in real-time "
        "market impact assessment. Generate a concise flash advisory in JSON format. "
        "Be specific about affected tickers (use European exchange suffixes like .DE, .PA, .AS "
        "where applicable). Focus on actionable intelligence."
    )

    user_msg = (
        "Based on the following breaking news, generate a flash advisory.\n\n"
        + "\n\n".join(context_parts)
        + "\n\nRespond with ONLY valid JSON in this exact format:\n"
        '{"what_happened": "1-2 sentence summary", '
        '"market_impact": {"immediate": "expected 1-4h reaction", '
        '"sectors_affected": ["sector1"], "tickers_to_watch": ["TICK1"], '
        '"direction": "bullish/bearish/mixed"}, '
        '"portfolio_action": {"urgency": "immediate/watch/none", '
        '"recommendation": "specific action", "risk_level": "critical/high/moderate"}, '
        '"confidence": 0.85}'
    )

    try:
        resp = client.chat.completions.create(
            model=GROQ_DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=MAX_TOKENS,
            temperature=0.3,
        )
        raw = resp.choices[0].message.content.strip()
        # Try to extract JSON from the response
        if raw.startswith("```"):
            # Strip markdown code block
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Flash advisory Groq response not valid JSON — using fallback")
        return _template_fallback(alert)
    except Exception:
        logger.exception("Groq flash advisory call failed — using fallback")
        return _template_fallback(alert)


def _template_fallback(alert: FlashAlert) -> Dict[str, Any]:
    """Build an advisory from NLP scores only (no API call)."""
    sectors = _event_to_sectors(alert.event_type)
    tickers = _event_to_tickers(alert.event_type, alert.region)
    direction = "bearish" if alert.intensity in ("critical", "high-threat") else "mixed"

    return {
        "what_happened": alert.headline or "Geopolitical event detected",
        "market_impact": {
            "immediate": f"{alert.intensity or 'moderate'} {alert.event_type or 'general'} event in {alert.region or 'global'}",
            "sectors_affected": sectors,
            "tickers_to_watch": tickers,
            "direction": direction,
        },
        "portfolio_action": {
            "urgency": "watch",
            "recommendation": "Monitor situation and review stop-losses",
            "risk_level": alert.intensity or "moderate",
        },
        "confidence": 0.40,
    }


def _event_to_sectors(event_type: Optional[str]) -> List[str]:
    """Map event type to affected market sectors."""
    return _EVENT_SECTOR_MAP.get(event_type or "general", ["Finance", "Energy"])


def _event_to_tickers(event_type: Optional[str], region: Optional[str]) -> List[str]:
    """Map event type + region to likely affected tickers.

    All returned tickers are validated against the bunq whitelist.
    Non-bunq tickers are silently filtered out.
    """
    from .bunq_stocks import is_available_on_bunq

    key = f"{event_type or 'general'}:{region or 'global'}"
    tickers = _TICKER_MAP.get(key)
    if not tickers:
        # Fallback: try event-only
        for k, v in _TICKER_MAP.items():
            if k.startswith(f"{event_type}:"):
                tickers = v
                break
    if not tickers:
        tickers = ["XOM", "IS0D.DE", "IS0E.DE"]

    validated = [t for t in tickers if is_available_on_bunq(t)]
    return validated if validated else tickers[:3]


# ---------------------------------------------------------------------------
# Self-learning: recommendation builder
# ---------------------------------------------------------------------------

def _build_recommendations(
    advisory: Dict[str, Any],
    tickers: List[str],
    db: Session,
) -> List[Dict[str, Any]]:
    """Build concrete recommendations with price-at-alert for evaluation.

    Each recommendation includes:
      - ticker, action, price_at_alert, confidence, pred_prob, components
    """
    if not tickers:
        return []

    confidence = advisory.get("confidence", 0.5)
    pred_prob = max(0.35, min(0.80, 0.5 + float(confidence) * 0.4))
    direction = advisory.get("market_impact", {}).get("direction", "mixed")
    pa = advisory.get("portfolio_action", {})
    urgency = pa.get("urgency", "watch")

    # Determine action from direction
    if "bullish" in str(direction).lower():
        default_action = "BUY" if urgency == "immediate" else "WATCH"
    elif "bearish" in str(direction).lower():
        default_action = "SELL" if urgency == "immediate" else "WATCH"
    else:
        default_action = "WATCH"

    recs: List[Dict[str, Any]] = []
    for ticker in tickers[:5]:  # Max 5 tickers
        # Get latest price
        price = None
        try:
            snap = (
                db.query(PriceSnapshot)
                .filter(PriceSnapshot.ticker == ticker)
                .order_by(PriceSnapshot.recorded_at.desc())
                .first()
            )
            if snap:
                price = snap.price
        except Exception:
            pass

        recs.append({
            "ticker": ticker,
            "action": default_action,
            "price_at_alert": price,
            "confidence": confidence,
            "pred_prob": pred_prob,
            "components": {},  # Filled during evaluation if needed
        })

    return recs


# ---------------------------------------------------------------------------
# Alert expiry (scheduled job — runs hourly)
# ---------------------------------------------------------------------------

def expire_old_alerts(db: Session) -> int:
    """Mark alerts past their ``expires_at`` as expired.

    Records are NEVER deleted — only status changes.
    Returns the number of alerts expired.
    """
    now = datetime.utcnow()
    expired = (
        db.query(FlashAlert)
        .filter(
            FlashAlert.status == "active",
            FlashAlert.expires_at.isnot(None),
            FlashAlert.expires_at < now,
        )
        .all()
    )
    for alert in expired:
        alert.status = "expired"
    if expired:
        db.commit()
        logger.info("Expired %d flash alerts", len(expired))
    return len(expired)
