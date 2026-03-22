"""
Arabella — Geopolitical Risk Analyst Persona & Big-News Notifier
================================================================
Monitors swarm verdicts, flash alerts, and trading signals.
Only sends Telegram notifications when truly significant events occur.

Integration: called from engine.py after swarm_consensus_job and
flash detection completes. Uses Claude Haiku for implication analysis.

Cost: ~€0.001 per notification (1 Haiku call for implications).
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .config import config
from .db import (
    AnalysisReport,
    EngineSettings,
    FlashAlert,
    SwarmVerdict,
    TradingSignal,
    get_session,
)

logger = logging.getLogger(__name__)

# ── Thresholds for "big news" ────────────────────────────────────

# Swarm verdict must be strong AND confident
MIN_VERDICT_CONFIDENCE = 0.70
BIG_VERDICTS = {"STRONG_BUY", "STRONG_SELL"}

# Flash alert must score high
MIN_FLASH_SCORE = 70
MIN_FLASH_SCORE_CLUSTER = 55

# Trading signal must be strong
BIG_SIGNAL_LEVELS = {"STRONG_BUY", "STRONG_SELL"}
MIN_SIGNAL_CONFIDENCE = 0.70

# ── Rate limiting ────────────────────────────────────────────────

# Per-narrative cooldown
NARRATIVE_COOLDOWN_HOURS = 6
# Global limits
MAX_PER_6H = 3
MAX_PER_DAY = 6
# Quiet hours (UTC)
QUIET_HOUR_START = 22
QUIET_HOUR_END = 6

# ── Internal state (persisted in EngineSettings) ─────────────────

_SETTINGS_KEY_COOLDOWNS = "arabella_cooldowns"
_SETTINGS_KEY_SENT_LOG = "arabella_sent_log"


# ══════════════════════════════════════════════════════════════════
# Core: evaluate whether swarm update qualifies as "big news"
# ══════════════════════════════════════════════════════════════════

def evaluate_swarm_update(verdicts: List[SwarmVerdict]) -> Optional[Dict[str, Any]]:
    """Check if recent swarm verdicts contain big news worth notifying about.

    Returns a notification dict if big news detected, None otherwise.
    """
    if not verdicts:
        return None

    big_verdicts = []
    for v in verdicts:
        verdict_str = getattr(v, "verdict", "HOLD") or "HOLD"
        confidence = getattr(v, "confidence", 0) or 0

        if verdict_str in BIG_VERDICTS and confidence >= MIN_VERDICT_CONFIDENCE:
            big_verdicts.append({
                "ticker": getattr(v, "primary_ticker", None) or "?",
                "verdict": verdict_str,
                "confidence": round(confidence, 2),
                "direction": getattr(v, "ticker_direction", "") or "",
                "reasoning": getattr(v, "entry_reasoning", "") or "",
                "exit_trigger": getattr(v, "exit_trigger", "") or "",
                "risk_note": getattr(v, "risk_note", "") or "",
                "dissent": getattr(v, "dissent_note", "") or "",
                "consensus_strength": round(getattr(v, "consensus_strength", 0) or 0, 2),
            })

    if not big_verdicts:
        return None

    return {
        "type": "swarm_verdict",
        "verdicts": big_verdicts,
        "timestamp": datetime.utcnow().isoformat(),
    }


def evaluate_flash_alert(alert: FlashAlert) -> Optional[Dict[str, Any]]:
    """Check if a flash alert qualifies as big news.

    Returns a notification dict if big news, None otherwise.
    """
    score = getattr(alert, "flash_score", 0) or 0
    trigger_type = getattr(alert, "trigger_type", "single_article") or "single_article"

    threshold = MIN_FLASH_SCORE if trigger_type == "single_article" else MIN_FLASH_SCORE_CLUSTER
    if score < threshold:
        return None

    risk_level = getattr(alert, "risk_level", "moderate") or "moderate"
    if risk_level not in ("critical", "high"):
        return None

    tickers = []
    try:
        tickers_json = getattr(alert, "tickers_affected_json", None)
        if tickers_json:
            tickers = json.loads(tickers_json) if isinstance(tickers_json, str) else tickers_json
    except (json.JSONDecodeError, TypeError):
        pass

    recommendations = []
    try:
        recs_json = getattr(alert, "recommendations_json", None)
        if recs_json:
            recommendations = json.loads(recs_json) if isinstance(recs_json, str) else recs_json
    except (json.JSONDecodeError, TypeError):
        pass

    return {
        "type": "flash_alert",
        "headline": getattr(alert, "headline", "") or "",
        "region": getattr(alert, "region", "") or "",
        "flash_score": score,
        "risk_level": risk_level,
        "event_type": getattr(alert, "event_type", "") or "",
        "intensity": getattr(alert, "intensity", "") or "",
        "portfolio_action": getattr(alert, "portfolio_action", "") or "",
        "tickers": tickers,
        "recommendations": recommendations,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ══════════════════════════════════════════════════════════════════
# Rate limiting & deduplication
# ══════════════════════════════════════════════════════════════════

def _is_quiet_hours() -> bool:
    """Check if current UTC time is in quiet hours."""
    hour = datetime.utcnow().hour
    if QUIET_HOUR_START > QUIET_HOUR_END:  # wraps midnight
        return hour >= QUIET_HOUR_START or hour < QUIET_HOUR_END
    return QUIET_HOUR_START <= hour < QUIET_HOUR_END


def _load_sent_log() -> List[Dict]:
    """Load recent sent notifications from DB."""
    session = get_session()
    try:
        s = session.query(EngineSettings).get(_SETTINGS_KEY_SENT_LOG)
        if s and s.value:
            entries = json.loads(s.value)
            # Prune entries older than 24h
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            return [e for e in entries if e.get("sent_at", "") > cutoff]
        return []
    except Exception:
        return []
    finally:
        session.close()


def _save_sent_log(entries: List[Dict]) -> None:
    """Persist sent log to DB."""
    session = get_session()
    try:
        s = session.query(EngineSettings).get(_SETTINGS_KEY_SENT_LOG)
        value = json.dumps(entries[-50:], default=str)  # keep last 50
        if s:
            s.value = value
            s.updated_at = datetime.utcnow()
        else:
            s = EngineSettings(key=_SETTINGS_KEY_SENT_LOG, value=value)
            session.add(s)
        session.commit()
    except Exception:
        session.rollback()
        logger.warning("Failed to save Arabella sent log", exc_info=True)
    finally:
        session.close()


def _compute_content_hash(notification: Dict) -> str:
    """Create hash for deduplication."""
    ntype = notification.get("type", "")
    if ntype == "swarm_verdict":
        # Hash on tickers + verdicts
        tickers = sorted(v["ticker"] for v in notification.get("verdicts", []))
        verdicts = sorted(v["verdict"] for v in notification.get("verdicts", []))
        raw = f"swarm:{','.join(tickers)}:{','.join(verdicts)}"
    elif ntype == "flash_alert":
        raw = f"flash:{notification.get('headline', '')[:100]}"
    else:
        raw = json.dumps(notification, sort_keys=True, default=str)[:200]
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _check_rate_limits(notification: Dict) -> Optional[str]:
    """Check if notification passes rate limits.

    Returns None if OK, or a reason string if blocked.
    """
    # Quiet hours (critical flash alerts bypass)
    if _is_quiet_hours():
        is_critical = (
            notification.get("type") == "flash_alert"
            and notification.get("risk_level") == "critical"
        )
        if not is_critical:
            return "quiet_hours"

    sent_log = _load_sent_log()
    now = datetime.utcnow()

    # Deduplication (same content hash in last 6h)
    content_hash = _compute_content_hash(notification)
    six_h_ago = (now - timedelta(hours=6)).isoformat()
    for entry in sent_log:
        if entry.get("hash") == content_hash and entry.get("sent_at", "") > six_h_ago:
            return f"duplicate (hash={content_hash[:8]})"

    # Max per 6h
    count_6h = sum(1 for e in sent_log if e.get("sent_at", "") > six_h_ago)
    if count_6h >= MAX_PER_6H:
        return f"rate_limit_6h ({count_6h}/{MAX_PER_6H})"

    # Max per day
    day_ago = (now - timedelta(hours=24)).isoformat()
    count_24h = sum(1 for e in sent_log if e.get("sent_at", "") > day_ago)
    if count_24h >= MAX_PER_DAY:
        return f"rate_limit_24h ({count_24h}/{MAX_PER_DAY})"

    # Narrative cooldown (for swarm verdicts)
    if notification.get("type") == "swarm_verdict":
        tickers = sorted(v["ticker"] for v in notification.get("verdicts", []))
        ticker_key = ",".join(tickers)
        cooldown_cutoff = (now - timedelta(hours=NARRATIVE_COOLDOWN_HOURS)).isoformat()
        for entry in sent_log:
            if (entry.get("ticker_key") == ticker_key
                    and entry.get("sent_at", "") > cooldown_cutoff):
                return f"narrative_cooldown ({ticker_key})"

    return None


def _record_sent(notification: Dict) -> None:
    """Record that a notification was sent."""
    sent_log = _load_sent_log()
    entry = {
        "type": notification.get("type"),
        "hash": _compute_content_hash(notification),
        "sent_at": datetime.utcnow().isoformat(),
    }
    if notification.get("type") == "swarm_verdict":
        entry["ticker_key"] = ",".join(
            sorted(v["ticker"] for v in notification.get("verdicts", []))
        )
    elif notification.get("type") == "flash_alert":
        entry["headline"] = notification.get("headline", "")[:80]

    sent_log.append(entry)
    _save_sent_log(sent_log)


# ══════════════════════════════════════════════════════════════════
# Implication analysis (1 LLM call)
# ══════════════════════════════════════════════════════════════════

def _generate_implications(notification: Dict) -> str:
    """Generate implication analysis using Groq (free) or Claude Haiku.

    Returns a Dutch-language implications summary.
    """
    if notification.get("type") == "swarm_verdict":
        verdicts_text = "\n".join(
            f"- {v['ticker']}: {v['verdict']} ({v['confidence']:.0%} confidence) — {v['reasoning'][:150]}"
            for v in notification.get("verdicts", [])
        )
        context = f"Swarm consensus update:\n{verdicts_text}"
    elif notification.get("type") == "flash_alert":
        context = (
            f"Breaking news: {notification.get('headline', '')}\n"
            f"Region: {notification.get('region', 'Global')}\n"
            f"Severity: {notification.get('risk_level', 'high')}\n"
            f"Event type: {notification.get('event_type', '')}\n"
            f"Tickers affected: {', '.join(str(t) for t in notification.get('tickers', []))}"
        )
    else:
        return ""

    prompt = (
        f"Analyseer dit nieuws en geef de IMPLICATIES in het Nederlands.\n\n"
        f"{context}\n\n"
        f"Geef per dimensie 1-2 zinnen:\n"
        f"1. GEOPOLITIEK: Welke machtsverhoudingen verschuiven?\n"
        f"2. MARKT: Welke sectors/tickers profiteren of lijden? Geef richting.\n"
        f"3. ECONOMISCH: Supply chain, inflatie, centrale bank reactie?\n"
        f"4. ACTIE: Wat is de concrete trade? Entry, exit, stop-loss.\n\n"
        f"Wees direct en specifiek. Noem tickers en prijsniveaus waar mogelijk.\n"
        f"Max 200 woorden."
    )

    # Try Groq first (free), fall back to no implications
    if not config.groq_api_key:
        logger.debug("Arabella: no Groq key — skipping implications")
        return ""

    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=config.groq_api_key,
            base_url="https://api.groq.com/openai/v1",
            timeout=30.0,
        )
        response = client.chat.completions.create(
            model=config.groq_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Je bent Arabella, een geopolitiek risico-analist voor actieve traders. "
                        "Je schrijft in het Nederlands, bent direct, data-gedreven, en skeptisch "
                        "tegenover mainstream narratieven. Gebruik Engelse tickers en technische "
                        "termen. Elke analyse bevat concrete acties met invalidatie-triggers."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=400,
            temperature=0.3,
        )
        content = response.choices[0].message.content
        return content.strip() if content else ""
    except Exception as e:
        logger.warning("Arabella implications generation failed: %s", e)
        return ""


# ══════════════════════════════════════════════════════════════════
# Telegram message formatting
# ══════════════════════════════════════════════════════════════════

_VERDICT_EMOJI = {
    "STRONG_BUY": "🟢🟢",
    "BUY": "🟢",
    "HOLD": "⚪",
    "SELL": "🔴",
    "STRONG_SELL": "🔴🔴",
}


def _format_arabella_message(notification: Dict, implications: str) -> str:
    """Format an Arabella notification as HTML for Telegram."""
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines: List[str] = []

    if notification.get("type") == "flash_alert":
        risk = notification.get("risk_level", "high").upper()
        badge = "🚨🚨🚨" if risk == "CRITICAL" else "🚨"
        lines.append(f"{badge} <b>ARABELLA — {risk}</b>")
        lines.append(f"{now_str}")
        lines.append("")
        lines.append(f"<b>{notification.get('headline', '')}</b>")

        region = notification.get("region", "")
        event_type = notification.get("event_type", "")
        if region or event_type:
            lines.append(f"Regio: {region} | Type: {event_type}")

        tickers = notification.get("tickers", [])
        if tickers:
            lines.append(f"Tickers: {', '.join(str(t) for t in tickers[:8])}")

        portfolio_action = notification.get("portfolio_action", "")
        if portfolio_action and portfolio_action != "none":
            lines.append(f"⚡ Portfolio actie: <b>{portfolio_action.upper()}</b>")

        recs = notification.get("recommendations", [])
        if recs:
            lines.append("")
            for r in recs[:3]:
                ticker = r.get("ticker", "?")
                action = r.get("action", "watch")
                conf = r.get("confidence", 0)
                lines.append(f"  • <b>{ticker}</b> → {action} ({conf:.0%})")

    elif notification.get("type") == "swarm_verdict":
        lines.append(f"🧠 <b>ARABELLA — Swarm Update</b>")
        lines.append(f"{now_str}")
        lines.append("")

        for v in notification.get("verdicts", []):
            emoji = _VERDICT_EMOJI.get(v["verdict"], "⚪")
            lines.append(
                f"  {emoji} <b>{v['ticker']}</b> — {v['verdict']}"
                f" ({v.get('direction', '')}) | {v['confidence']:.0%} conf"
            )
            if v.get("reasoning"):
                snippet = v["reasoning"][:150]
                if len(v["reasoning"]) > 150:
                    snippet += "..."
                lines.append(f"    <i>{snippet}</i>")
            if v.get("exit_trigger"):
                lines.append(f"    Exit: {v['exit_trigger'][:100]}")
            if v.get("risk_note"):
                lines.append(f"    ⚠️ {v['risk_note'][:100]}")
            lines.append("")

    # Add implications
    if implications:
        lines.append("")
        lines.append("<b>📊 IMPLICATIES:</b>")
        lines.append(implications)

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# Main entry points (called from engine.py)
# ══════════════════════════════════════════════════════════════════

def notify_if_big_news_swarm(verdicts: list) -> bool:
    """Evaluate swarm verdicts and send Arabella notification if big news.

    Called from engine.py after swarm_consensus_job completes.
    Returns True if a notification was sent.
    """
    notification = evaluate_swarm_update(verdicts)
    if not notification:
        logger.debug("Arabella: swarm update — no big news detected.")
        return False

    return _send_notification(notification)


def notify_if_big_news_flash(alert) -> bool:
    """Evaluate a flash alert and send Arabella notification if big news.

    Called from engine.py after flash detection.
    Returns True if a notification was sent.
    """
    notification = evaluate_flash_alert(alert)
    if not notification:
        logger.debug("Arabella: flash alert score too low — skipping.")
        return False

    return _send_notification(notification)


def _send_notification(notification: Dict) -> bool:
    """Common send logic: rate limit → implications → format → Telegram."""
    # Rate limit check
    blocked = _check_rate_limits(notification)
    if blocked:
        logger.info("Arabella: notification blocked (%s).", blocked)
        return False

    # Generate implications (1 Groq call)
    implications = _generate_implications(notification)

    # Format message
    msg = _format_arabella_message(notification, implications)

    # Send via Telegram
    from .telegram_notifier import is_telegram_configured, send_message
    if not is_telegram_configured():
        logger.warning("Arabella: Telegram not configured — notification logged only.")
        return False

    ok = send_message(msg, parse_mode="HTML")
    if ok:
        _record_sent(notification)
        ntype = notification.get("type", "unknown")
        logger.info("Arabella: %s notification sent via Telegram.", ntype)
        return True
    else:
        logger.warning("Arabella: Telegram send failed.")
        return False
