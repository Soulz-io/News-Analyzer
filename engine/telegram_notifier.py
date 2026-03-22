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
            from .crypto import decrypt_value
            session = get_session()
            try:
                if not token:
                    s = session.query(EngineSettings).get("telegram_bot_token")
                    if s and s.value:
                        token = decrypt_value(s.value)
                if not chat_id:
                    s = session.query(EngineSettings).get("telegram_chat_id")
                    if s and s.value:
                        chat_id = decrypt_value(s.value)
            finally:
                session.close()
        except Exception:
            logger.warning("Failed to load Telegram config from DB.", exc_info=True)

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


def send_swarm_notification(verdicts) -> bool:
    """Format and send swarm consensus results as a Telegram message.

    Args:
        verdicts: list of SwarmVerdict ORM objects (or dicts with matching keys).

    Returns True on success, False on failure.
    """
    if not is_telegram_configured():
        return False

    try:
        msg = _format_swarm_message(verdicts)
        return send_message(msg)
    except Exception:
        logger.exception("Failed to format/send swarm notification via Telegram.")
        return False


def send_test_message() -> bool:
    """Send a test message to verify Telegram configuration."""
    return send_message(
        "<b>OpenClaw Advisory</b> — Telegram test OK\n"
        f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )


# ---------------------------------------------------------------------------
# Flash alert notifications
# ---------------------------------------------------------------------------

def send_flash_alert(alert, suppress_in_quiet_hours: bool = True) -> bool:
    """Send a breaking-news flash alert via Telegram.

    Args:
        alert: FlashAlert DB object.
        suppress_in_quiet_hours: If True, queue non-CRITICAL during 23:00-06:00 UTC.

    Returns True if sent immediately, False if queued or failed.
    """
    if not is_telegram_configured():
        return False

    if suppress_in_quiet_hours and _should_suppress(alert.flash_score):
        _queue_suppressed(alert)
        return False

    try:
        tier = "CRITICAL" if alert.flash_score >= 80 else "ALERT"
        msg = _format_flash_message(alert, tier)
        return send_message(msg)
    except Exception:
        logger.exception("Failed to send flash alert")
        return False


def send_advisory_refresh_notification(refresh_data: Dict[str, Any]) -> bool:
    """Send an advisory refresh notification (lightweight).

    Reuses ``_format_advisory_message()`` with a REFRESH badge.
    Suppressed during quiet hours.
    """
    if not is_telegram_configured():
        return False

    if _should_suppress(50):  # advisory refresh = niet-critical
        return False

    try:
        base = _format_advisory_message(refresh_data)
        msg = "🔄 <b>Advisory Refresh</b>\n\n" + base
        return send_message(msg)
    except Exception:
        logger.exception("Failed to send advisory refresh notification")
        return False


def batch_send_suppressed() -> int:
    """Send batch summary of alerts suppressed during quiet hours.

    Called by scheduler at 06:05 UTC daily.
    Returns count of alerts included in batch.
    """
    if not is_telegram_configured():
        return 0

    try:
        from .db import get_session, SuppressedNotification
        session = get_session()
        try:
            queued = (
                session.query(SuppressedNotification)
                .filter(SuppressedNotification.batch_sent_at.is_(None))
                .order_by(SuppressedNotification.created_at.asc())
                .all()
            )
            if not queued:
                return 0

            lines = [
                "<b>📋 Overnight Alert Summary</b>",
                f"{len(queued)} alert(s) suppressed during quiet hours (23:00-06:00 UTC)",
                "",
            ]

            for q in queued[:8]:
                tier_emoji = "🔴" if q.tier == "CRITICAL" else "🟠"
                lines.append(f"  {tier_emoji} {q.message_preview}")

            if len(queued) > 8:
                lines.append(f"  … and {len(queued) - 8} more")

            msg = "\n".join(lines)
            if send_message(msg):
                now = datetime.utcnow()
                for q in queued:
                    q.batch_sent_at = now
                session.commit()
                logger.info("Batch sent %d suppressed notifications", len(queued))
                return len(queued)
            return 0
        finally:
            session.close()
    except Exception:
        logger.exception("batch_send_suppressed failed")
        return 0


def _should_suppress(flash_score: float) -> bool:
    """Return True if current time is in quiet hours and alert is non-CRITICAL."""
    hour = datetime.utcnow().hour
    in_quiet = hour >= 23 or hour < 6
    return in_quiet and flash_score < 80


def _queue_suppressed(alert) -> None:
    """Store a SuppressedNotification for later batch delivery."""
    try:
        from .db import get_session, SuppressedNotification
        session = get_session()
        try:
            existing = (
                session.query(SuppressedNotification)
                .filter(SuppressedNotification.flash_alert_id == alert.id)
                .first()
            )
            if not existing:
                tier = "CRITICAL" if alert.flash_score >= 80 else "ALERT"
                session.add(SuppressedNotification(
                    flash_alert_id=alert.id,
                    tier=tier,
                    message_preview=(alert.headline or "")[:200],
                ))
                session.commit()
                logger.info("Flash alert %s queued (quiet hours)", alert.alert_id)
        finally:
            session.close()
    except Exception:
        logger.exception("Failed to queue suppressed notification")


def _format_flash_message(alert, tier: str) -> str:
    """Format a FlashAlert into an HTML Telegram message."""
    tier_emoji = {"CRITICAL": "🔴🔴🔴", "ALERT": "🟠🟠"}.get(tier, "⚠️")
    region = (alert.region or "global").upper()

    lines = [
        f"{tier_emoji} BREAKING — {region}",
        "",
        f"<b>{alert.headline}</b>",
        "",
    ]

    # Parse advisory JSON
    advisory: Dict[str, Any] = {}
    if alert.flash_advisory_json:
        try:
            advisory = json.loads(alert.flash_advisory_json)
        except (json.JSONDecodeError, TypeError):
            pass

    # Market impact
    impact = advisory.get("market_impact", {})
    if impact.get("immediate"):
        lines.append(f"Impact: {impact['immediate']}")

    # Tickers
    if alert.tickers_affected_json:
        try:
            tickers = json.loads(alert.tickers_affected_json)
            if tickers:
                lines.append(f"Watch: {', '.join(tickers[:6])}")
        except (json.JSONDecodeError, TypeError):
            pass

    # Sectors
    sectors = impact.get("sectors_affected", [])
    if sectors:
        lines.append(f"Sectors: {', '.join(sectors[:4])}")

    lines.append("")

    # Portfolio action
    pa = advisory.get("portfolio_action", {})
    rec = pa.get("recommendation", "Monitor situation")
    if rec:
        lines.append(f"⚡ {rec}")

    # Risk & confidence
    risk = (alert.risk_level or "moderate").upper()
    conf = advisory.get("confidence", 0)
    lines.append(f"Risk: {risk} | Confidence: {conf:.0%}" if isinstance(conf, (int, float)) else f"Risk: {risk}")

    # Sources
    if alert.source_names_json:
        try:
            sources = json.loads(alert.source_names_json)
            if sources:
                lines.append(f"\nSources: {', '.join(sources[:3])}")
        except (json.JSONDecodeError, TypeError):
            pass

    # Timestamp
    if alert.detected_at:
        lines.append(alert.detected_at.strftime("%H:%M UTC"))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Advisory message formatting
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Swarm consensus message formatting
# ---------------------------------------------------------------------------

def _format_swarm_message(verdicts) -> str:
    """Format swarm verdicts into a compact Telegram HTML message.

    Accepts a list of SwarmVerdict ORM objects. Shows top 5 by confidence.
    """
    verdict_emoji = {
        "STRONG_BUY": "🟢🟢",
        "BUY": "🟢",
        "HOLD": "⚪",
        "SELL": "🔴",
        "STRONG_SELL": "🔴🔴",
    }

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "<b>🧠 Swarm Consensus Update</b>",
        f"{now_str}",
        "",
    ]

    # Sort by confidence descending, take top 5
    sorted_verdicts = sorted(
        verdicts,
        key=lambda v: getattr(v, "confidence", 0) or 0,
        reverse=True,
    )[:5]

    if not sorted_verdicts:
        lines.append("No verdicts produced this cycle.")
        return "\n".join(lines)

    for v in sorted_verdicts:
        verdict_str = getattr(v, "verdict", "HOLD") or "HOLD"
        emoji = verdict_emoji.get(verdict_str, "⚪")
        confidence = getattr(v, "confidence", 0) or 0
        ticker = getattr(v, "primary_ticker", None) or "?"
        direction = getattr(v, "ticker_direction", "") or ""
        reasoning = getattr(v, "entry_reasoning", "") or ""

        line = f"  {emoji} <b>{ticker}</b> — {verdict_str}"
        if direction:
            line += f" ({direction})"
        line += f" | {confidence:.0%} conf"

        # Add brief reasoning snippet
        if reasoning:
            snippet = reasoning[:120]
            if len(reasoning) > 120:
                snippet += "..."
            line += f"\n    <i>{snippet}</i>"

        lines.append(line)

    lines.append("")
    lines.append(f"Total verdicts: {len(verdicts)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Morning Briefing — server-side formatted, zero LLM tokens
# ---------------------------------------------------------------------------

def send_morning_briefing() -> bool:
    """Format and send the complete morning briefing via Telegram.

    Assembles all data server-side (same as /api/morning-digest),
    formats into Arabella's Telegram style, and sends directly.
    Zero LLM tokens — pure template formatting.
    """
    if not is_telegram_configured():
        logger.warning("Morning briefing: Telegram not configured.")
        return False

    try:
        msg = _format_morning_briefing()
        if not msg:
            logger.warning("Morning briefing: empty message generated.")
            return False
        return send_message(msg)
    except Exception:
        logger.exception("Morning briefing failed.")
        return False


def _format_morning_briefing() -> str:
    """Build the complete morning briefing message from DB data + weather."""
    from datetime import timedelta
    from .db import (
        get_session, RunUp, SwarmVerdict, TradingSignal,
        AnalysisReport, EngineSettings, NarrativeTimeline, FlashAlert,
    )
    from .api_routes import _get_daily_quotes
    from sqlalchemy import desc

    session = get_session()
    try:
        now = datetime.utcnow()
        twenty_four_hours_ago = now - timedelta(hours=24)

        # --- 1. Portfolio ---
        portfolio_lines = []
        total_str = ""
        pnl_str = ""
        s = session.query(EngineSettings).get("portfolio_holdings")
        if s and s.value:
            try:
                from .price_fetcher import get_price_fetcher
                holdings = json.loads(s.value)
                pf = get_price_fetcher()
                total_value = 0
                total_cost = 0
                holding_data = []
                for h in holdings:
                    ticker = h["ticker"]
                    shares = float(h.get("shares", 0))
                    avg_buy = float(h.get("avg_buy_price_eur", 0))
                    if shares <= 0 and h.get("value_eur", 0) <= 0:
                        continue
                    quote = pf.get_quote(ticker)
                    if "error" not in quote:
                        live_eur = round(pf.convert_to_eur(quote["price"], quote.get("currency", "EUR")), 4)
                        current_val = round(shares * live_eur, 2)
                        cost_basis = round(shares * avg_buy, 2)
                        change_pct = quote.get("change_pct", 0)
                        total_value += current_val
                        total_cost += cost_basis
                        holding_data.append({
                            "ticker": ticker,
                            "value": current_val,
                            "change_pct": change_pct,
                        })

                if total_cost > 0:
                    pnl_pct = round(((total_value - total_cost) / total_cost) * 100, 2)
                    pnl_emoji = "📈" if pnl_pct >= 0 else "📉"
                    total_str = f"€{total_value:,.0f}"
                    pnl_str = f"{pnl_emoji} {pnl_pct:+.1f}%"

                # Sort by absolute change for top movers
                holding_data.sort(key=lambda x: abs(x.get("change_pct", 0) or 0), reverse=True)
                for hd in holding_data[:6]:
                    chg = hd.get("change_pct", 0) or 0
                    chg_emoji = "🟢" if chg >= 0 else "🔴"
                    portfolio_lines.append(
                        f"  {chg_emoji} {hd['ticker']} · €{hd['value']:,.0f} · {chg:+.1f}%"
                    )
            except Exception as e:
                logger.warning("Morning briefing portfolio failed: %s", e)

        # --- 2. Swarm verdicts ---
        swarm_lines = []
        active_runups = (
            session.query(RunUp)
            .filter(RunUp.status == "active")
            .order_by(desc(RunUp.current_score))
            .limit(3)
            .all()
        )
        for ru in active_runups:
            verdict = (
                session.query(SwarmVerdict)
                .filter(SwarmVerdict.run_up_id == ru.id, SwarmVerdict.superseded_at.is_(None))
                .order_by(desc(SwarmVerdict.created_at))
                .first()
            )
            v_str = ""
            if verdict:
                v_emoji = {"STRONG_BUY": "🟢🟢", "BUY": "🟢", "HOLD": "⚪", "SELL": "🔴", "STRONG_SELL": "🔴🔴"}.get(verdict.verdict, "⚪")
                v_str = f" · {v_emoji} {verdict.verdict} ({verdict.confidence:.0%})"
            swarm_lines.append(f"  {ru.narrative_name} (score {ru.current_score}){v_str}")

        # --- 3. Trending narratives ---
        trending_lines = []
        from .db import NarrativeTimeline
        from datetime import date
        timelines = (
            session.query(NarrativeTimeline)
            .filter(NarrativeTimeline.date >= (date.today() - timedelta(days=1)))
            .order_by(desc(NarrativeTimeline.intensity_score))
            .limit(3)
            .all()
        )
        for t in timelines:
            sent_emoji = "🟢" if (t.avg_sentiment or 0) > 0.1 else ("🔴" if (t.avg_sentiment or 0) < -0.1 else "⚪")
            trend_emoji = "↗️" if t.trend == "rising" else ("↘️" if t.trend == "falling" else "→")
            trending_lines.append(f"  {sent_emoji} {t.narrative_name} · {trend_emoji} · {t.article_count} artikelen")

        # --- 4. Advisory ---
        advisory_line = ""
        advisory_report = (
            session.query(AnalysisReport)
            .filter(AnalysisReport.report_type.in_(["daily_advisory", "advisory_refresh"]))
            .order_by(desc(AnalysisReport.created_at))
            .first()
        )
        if advisory_report:
            try:
                data = json.loads(advisory_report.report_json)
                stance = (data.get("market_stance") or "neutral").replace("_", " ").title()
                buys = [r.get("ticker", "?") for r in data.get("buy_recommendations", [])[:3]]
                sells = [r.get("ticker", "?") for r in data.get("sell_recommendations", [])[:3]]
                parts = [stance]
                if buys:
                    parts.append(f"BUY: {', '.join(buys)}")
                if sells:
                    parts.append(f"SELL: {', '.join(sells)}")
                advisory_line = " | ".join(parts)
            except Exception:
                pass

        # --- 5. Actions ---
        action_now = []
        flashes = (
            session.query(FlashAlert)
            .filter(FlashAlert.detected_at >= twenty_four_hours_ago, FlashAlert.status != "expired")
            .order_by(desc(FlashAlert.flash_score))
            .limit(3)
            .all()
        )
        for f in flashes:
            if f.flash_score and f.flash_score >= 7:
                action_now.append(f"⚡ {(f.headline or '')[:80]}")

        signals = (
            session.query(TradingSignal)
            .filter(TradingSignal.created_at >= twenty_four_hours_ago)
            .order_by(desc(TradingSignal.confidence))
            .limit(5)
            .all()
        )
        for sig in signals:
            if sig.signal_level in ("STRONG_BUY", "BUY", "ALERT") and (sig.confidence or 0) >= 0.6:
                action_now.append(f"📊 {sig.narrative_name}: {sig.signal_level} ({sig.confidence:.0%})")

        # --- 6. Quotes ---
        quotes = _get_daily_quotes(now)

        # --- 7. Weather ---
        weather_line = ""
        try:
            import httpx
            resp = httpx.get("https://wttr.in/Alkmaar?format=j1", timeout=5)
            if resp.status_code == 200:
                w = resp.json()
                current = w.get("current_condition", [{}])[0]
                temp = current.get("temp_C", "?")
                desc_nl = current.get("lang_nl", [{}])[0].get("value", current.get("weatherDesc", [{}])[0].get("value", ""))
                weather_line = f"🌤 Alkmaar: {temp}°C, {desc_nl}"
        except Exception:
            weather_line = "🌤 Weer niet beschikbaar"

        # --- BUILD MESSAGE ---
        lines = []
        day_str = now.strftime("%A %d %B").capitalize()
        lines.append(f"<b>Goedemorgen Joost ☀️</b>")
        lines.append(f"<i>{day_str}</i>")
        lines.append("")

        # Actions
        if action_now:
            lines.append("🚨 <b>NU</b>")
            lines.extend(action_now[:3])
            lines.append("")

        # Portfolio
        if portfolio_lines:
            lines.append(f"💰 <b>Portfolio: {total_str}</b> ({pnl_str})")
            lines.extend(portfolio_lines)
            lines.append("")

        # Swarm
        if swarm_lines:
            lines.append("🌍 <b>Swarm</b>")
            lines.extend(swarm_lines)
            lines.append("")

        # Trending
        if trending_lines:
            lines.append("🔥 <b>Trending</b>")
            lines.extend(trending_lines)
            lines.append("")

        # Advisory
        if advisory_line:
            lines.append(f"📈 <b>Advisory:</b> {advisory_line}")
            lines.append("")

        # Quotes
        if isinstance(quotes, dict) and "error" not in quotes:
            bijbels = quotes.get("bijbels")
            if bijbels:
                lines.append(f'📖 <i>"{bijbels.get("text", "")}"</i>')
                lines.append(f"— {bijbels.get('author', '')}")
                lines.append("")

            spiritueel = quotes.get("spiritueel")
            if spiritueel:
                lines.append(f'🧘 <i>"{spiritueel.get("text", "")}"</i>')
                lines.append(f"— {spiritueel.get('author', '')}")
                lines.append("")

            # Pick one from entrepreneur/filosofisch/motiverend (rotate by day)
            for cat in ["ondernemer", "filosofisch", "motiverend"]:
                q = quotes.get(cat)
                if q and (now.timetuple().tm_yday % 3 == ["ondernemer", "filosofisch", "motiverend"].index(cat)):
                    lines.append(f'💬 <i>"{q.get("text", "")}"</i>')
                    lines.append(f"— {q.get('author', '')}")
                    lines.append("")
                    break

        # Weather
        if weather_line:
            lines.append(weather_line)
            lines.append("")

        # Closing
        if action_now:
            lines.append(f"<b>Moet je vandaag iets doen?</b> Ja: check de alerts hierboven.")
        else:
            lines.append(f"<b>Moet je vandaag iets doen?</b> Nee, rustige dag. ☕")

        return "\n".join(lines)
    finally:
        session.close()
