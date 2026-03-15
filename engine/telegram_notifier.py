"""Telegram push notifications for daily advisory and alerts.

Sends the morning advisory as a formatted Telegram message via the Bot API.
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to be configured
(via env vars or DB EngineSettings).

Cost: free (Telegram Bot API has no usage fees).

Setup:
    1. Create a bot via @BotFather → get token
    2. Start a chat with the bot (or add to group)
    3. Get chat_id via https://api.telegram.org/bot<TOKEN>/getUpdates
    4. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in settings
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Telegram Bot API base URL
_API_BASE = "https://api.telegram.org/bot{token}"


def _get_telegram_config() -> tuple:
    """Get Telegram bot token and chat ID from env or DB.

    Returns (token, chat_id) or (None, None) if not configured.
    """
    import os
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if not token or not chat_id:
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                if not token:
                    s = session.query(EngineSettings).get("telegram_bot_token")
                    if s and s.value:
                        token = s.value
                if not chat_id:
                    s = session.query(EngineSettings).get("telegram_chat_id")
                    if s and s.value:
                        chat_id = s.value
            finally:
                session.close()
        except Exception:
            pass

    return (token or None, chat_id or None)


def is_telegram_configured() -> bool:
    """Check if Telegram notifications are configured."""
    token, chat_id = _get_telegram_config()
    return bool(token and chat_id)


def send_message(text: str, parse_mode: str = "HTML") -> bool:
    """Send a message via Telegram Bot API.

    Returns True on success, False on failure.
    """
    token, chat_id = _get_telegram_config()
    if not token or not chat_id:
        logger.debug("Telegram not configured — skipping notification.")
        return False

    try:
        import httpx
        url = f"{_API_BASE.format(token=token)}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        resp = httpx.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            logger.info("Telegram message sent successfully.")
            return True
        else:
            logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text[:200])
            return False
    except Exception:
        logger.exception("Telegram notification failed.")
        return False


def send_advisory_notification(advisory_data: Dict[str, Any]) -> bool:
    """Format and send the daily advisory as a Telegram message.

    Builds a concise, actionable message with BUY/SELL picks,
    risk levels, and position sizing.
    """
    if not is_telegram_configured():
        return False

    try:
        msg = _format_advisory_message(advisory_data)
        return send_message(msg)
    except Exception:
        logger.exception("Failed to format advisory for Telegram.")
        return False


def send_test_message() -> bool:
    """Send a test message to verify Telegram configuration."""
    return send_message(
        "<b>OpenClaw Advisory</b> — Telegram test OK\n"
        f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )


def _format_advisory_message(data: Dict[str, Any]) -> str:
    """Format advisory data into a compact Telegram HTML message."""
    stance = data.get("market_stance", "neutral")
    stance_emoji = {
        "strong_bullish": "🟢🟢",
        "cautious_bullish": "🟢",
        "neutral": "⚪",
        "cautious_bearish": "🔴",
        "strong_bearish": "🔴🔴",
    }.get(stance, "⚪")

    ctx = data.get("market_context", {})
    fg = ctx.get("fear_greed", {})
    vix = ctx.get("vix", {})

    gen_at = data.get("generated_at", "")[:16]

    lines = [
        f"<b>📊 Daily Advisory — {gen_at}</b>",
        f"Markt: {stance_emoji} {stance.replace('_', ' ').title()}",
        "",
    ]

    # Market context
    ctx_parts = []
    if fg.get("score") is not None:
        ctx_parts.append(f"F&G: {fg['score']}")
    if vix.get("price") is not None:
        ctx_parts.append(f"VIX: {vix['price']}")
    if ctx_parts:
        lines.append(" | ".join(ctx_parts))
        lines.append("")

    # BUY recommendations
    buys = data.get("buy_recommendations", [])
    if buys:
        lines.append("<b>🟢 BUY</b>")
        for rec in buys:
            ticker = rec.get("ticker", "?")
            price = rec.get("current_price")
            score = rec.get("composite_score", 0)
            risk = rec.get("risk_levels", {})
            sizing = rec.get("position_sizing", {})

            price_str = f"${price:.2f}" if price else "?"
            line = f"  <b>{ticker}</b> {price_str} (score {score:.2f})"

            # Add risk levels
            if risk.get("stop_loss") and risk.get("take_profit"):
                line += f"\n    SL ${risk['stop_loss']:.2f} → TP ${risk['take_profit']:.2f} ({risk.get('reward_risk', '?')}R)"

            # Add sizing
            if sizing.get("position_pct"):
                line += f"\n    Positie: {sizing['position_pct']}%"
                if sizing.get("eur_amount"):
                    line += f" (€{sizing['eur_amount']:.0f})"

            # Add thesis teaser
            reasoning = rec.get("reasoning", {})
            thesis = reasoning.get("thesis", "") if isinstance(reasoning, dict) else str(reasoning)
            if thesis:
                line += f"\n    <i>{thesis[:100]}{'...' if len(thesis) > 100 else ''}</i>"

            lines.append(line)
        lines.append("")

    # SELL recommendations
    sells = data.get("sell_recommendations", [])
    if sells:
        lines.append("<b>🔴 SELL</b>")
        for rec in sells:
            ticker = rec.get("ticker", "?")
            price = rec.get("current_price")
            score = rec.get("composite_score", 0)
            risk = rec.get("risk_levels", {})

            price_str = f"${price:.2f}" if price else "?"
            line = f"  <b>{ticker}</b> {price_str} (score {score:.2f})"

            if risk.get("stop_loss") and risk.get("take_profit"):
                line += f"\n    SL ${risk['stop_loss']:.2f} → TP ${risk['take_profit']:.2f}"

            reasoning = rec.get("reasoning", {})
            thesis = reasoning.get("thesis", "") if isinstance(reasoning, dict) else str(reasoning)
            if thesis:
                line += f"\n    <i>{thesis[:100]}{'...' if len(thesis) > 100 else ''}</i>"

            lines.append(line)
        lines.append("")

    # Risk warning
    risk_warning = data.get("risk_warning", "")
    if risk_warning:
        lines.append(f"⚠️ {risk_warning[:200]}")

    # Narrative summary
    summary = data.get("narrative_summary", "")
    if summary:
        lines.append("")
        lines.append(f"<i>{summary[:300]}</i>")

    return "\n".join(lines)
