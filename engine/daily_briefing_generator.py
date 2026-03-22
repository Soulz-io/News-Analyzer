"""
Daily Briefing Generator — Self-Evaluation & Improvement Suggestions
====================================================================
Generates a daily markdown briefing at briefings/YYYY-MM-DD.md

Purpose: Give the developer (using Claude Code) a clear picture of:
1. What the system predicted yesterday and whether it was right
2. Where the biggest errors are in the model
3. One concrete improvement to implement today

Scheduled: 06:00 UTC daily (before advisory generation at 06:30)
Cost: 1 Groq call (~free) for improvement suggestion
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import config
from .db import (
    AnalysisReport,
    Article,
    FlashAlert,
    RunUp,
    SwarmVerdict,
    TradingSignal,
    TokenUsage,
    EngineSettings,
    get_session,
)

logger = logging.getLogger(__name__)

BRIEFING_DIR = Path(__file__).resolve().parent.parent / "briefings"


def generate_daily_briefing() -> Optional[str]:
    """Generate the daily self-evaluation briefing.

    Returns the file path of the generated briefing, or None on failure.
    """
    BRIEFING_DIR.mkdir(exist_ok=True)
    today = date.today()
    filepath = BRIEFING_DIR / f"{today.isoformat()}.md"

    # Don't regenerate if already exists
    if filepath.exists():
        logger.info("Daily briefing already exists: %s", filepath)
        return str(filepath)

    session = get_session()
    try:
        # Gather data
        picks_report = _evaluate_recent_picks(session)
        flash_report = _evaluate_flash_alerts(session)
        pipeline_stats = _get_pipeline_stats(session)
        swarm_stats = _get_swarm_stats(session)
        cost_stats = _get_cost_stats(session)
        biggest_errors = _find_biggest_errors(session)
        improvement = _generate_improvement_suggestion(
            picks_report, flash_report, biggest_errors
        )

        # Build markdown
        md = _format_briefing(
            today, picks_report, flash_report, pipeline_stats,
            swarm_stats, cost_stats, biggest_errors, improvement,
        )

        filepath.write_text(md, encoding="utf-8")
        logger.info("Daily briefing generated: %s", filepath)
        return str(filepath)

    except Exception:
        logger.exception("Daily briefing generation failed")
        return None
    finally:
        session.close()


# ══════════════════════════════════════════════════════════════════
# Data Collection
# ══════════════════════════════════════════════════════════════════

def _evaluate_recent_picks(session) -> Dict[str, Any]:
    """Evaluate BUY/SELL picks from the last 7 days."""
    from .price_fetcher import get_price_fetcher
    pf = get_price_fetcher()

    cutoff = datetime.utcnow() - timedelta(days=7)
    advisories = (
        session.query(AnalysisReport)
        .filter(
            AnalysisReport.report_type == "daily_advisory",
            AnalysisReport.created_at >= cutoff,
        )
        .order_by(AnalysisReport.created_at.desc())
        .all()
    )

    picks: List[Dict] = []
    total = 0
    correct = 0
    incorrect = 0
    pending = 0
    total_return = 0.0

    for report in advisories:
        try:
            data = json.loads(report.report_json)
        except Exception:
            continue

        generated_at = report.created_at
        outcomes = data.get("outcomes", {})
        all_recs = data.get("buy_recommendations", []) + data.get("sell_recommendations", [])

        for rec in all_recs:
            ticker = rec.get("ticker", "?")
            entry_price = rec.get("current_price")
            action = rec.get("action", "BUY")
            score = rec.get("composite_score", 0)

            if not entry_price:
                continue

            total += 1
            ticker_outcomes = outcomes.get(ticker, {})

            # Find best evaluated horizon
            best_horizon = None
            best_outcome = None
            for h in [30, 14, 7, 3, 1]:
                h_key = f"T+{h}d"
                if h_key in ticker_outcomes:
                    best_horizon = h
                    best_outcome = ticker_outcomes[h_key]
                    break

            if best_outcome:
                is_correct = best_outcome.get("correct", False)
                return_pct = best_outcome.get("return_pct", 0)
                if is_correct:
                    correct += 1
                else:
                    incorrect += 1
                total_return += return_pct

                picks.append({
                    "ticker": ticker,
                    "action": action,
                    "entry_price": entry_price,
                    "outcome_price": best_outcome.get("price"),
                    "return_pct": round(return_pct, 2),
                    "correct": is_correct,
                    "horizon": f"T+{best_horizon}d",
                    "score": round(score, 3),
                    "date": generated_at.strftime("%Y-%m-%d"),
                })
            else:
                # Try to get current price for pending picks
                pending += 1
                try:
                    quote = pf.get_quote(ticker)
                    if "error" not in quote:
                        current = quote["price"]
                        ret = ((current - entry_price) / entry_price) * 100
                        picks.append({
                            "ticker": ticker,
                            "action": action,
                            "entry_price": entry_price,
                            "outcome_price": current,
                            "return_pct": round(ret, 2),
                            "correct": None,  # pending
                            "horizon": "pending",
                            "score": round(score, 3),
                            "date": generated_at.strftime("%Y-%m-%d"),
                        })
                except Exception:
                    pass

    evaluated = correct + incorrect
    accuracy = round((correct / evaluated) * 100, 1) if evaluated > 0 else 0
    avg_return = round(total_return / evaluated, 2) if evaluated > 0 else 0

    return {
        "total": total,
        "correct": correct,
        "incorrect": incorrect,
        "pending": pending,
        "accuracy": accuracy,
        "avg_return": avg_return,
        "picks": sorted(picks, key=lambda p: p.get("return_pct", 0)),
    }


def _evaluate_flash_alerts(session) -> Dict[str, Any]:
    """Evaluate flash alert accuracy over last 7 days."""
    cutoff = datetime.utcnow() - timedelta(days=7)
    alerts = (
        session.query(FlashAlert)
        .filter(FlashAlert.detected_at >= cutoff)
        .order_by(FlashAlert.detected_at.desc())
        .all()
    )

    total = len(alerts)
    evaluated = 0
    correct = 0

    for alert in alerts:
        for field in ["eval_1d_json", "eval_4d_json", "eval_7d_json"]:
            raw = getattr(alert, field, None)
            if raw:
                try:
                    eval_data = json.loads(raw) if isinstance(raw, str) else raw
                    if eval_data.get("overall_correct") is not None:
                        evaluated += 1
                        if eval_data["overall_correct"]:
                            correct += 1
                        break  # Use first available horizon
                except Exception:
                    pass

    accuracy = round((correct / evaluated) * 100, 1) if evaluated > 0 else 0
    return {"total": total, "evaluated": evaluated, "correct": correct, "accuracy": accuracy}


def _get_pipeline_stats(session) -> Dict[str, Any]:
    """Get 24h pipeline throughput."""
    from sqlalchemy import func
    cutoff = datetime.utcnow() - timedelta(hours=24)
    articles = session.query(func.count(Article.id)).filter(
        Article.fetched_at >= cutoff
    ).scalar() or 0
    signals = session.query(func.count(TradingSignal.id)).filter(
        TradingSignal.created_at >= cutoff
    ).scalar() or 0
    verdicts = session.query(func.count(SwarmVerdict.id)).filter(
        SwarmVerdict.created_at >= cutoff
    ).scalar() or 0
    active_runups = session.query(func.count(RunUp.id)).filter(
        RunUp.status == "active"
    ).scalar() or 0

    return {
        "articles_24h": articles,
        "signals_24h": signals,
        "verdicts_24h": verdicts,
        "active_runups": active_runups,
    }


def _get_swarm_stats(session) -> Dict[str, Any]:
    """Get swarm consensus stats over last 24h."""
    cutoff = datetime.utcnow() - timedelta(hours=24)
    verdicts = (
        session.query(SwarmVerdict)
        .filter(SwarmVerdict.created_at >= cutoff)
        .all()
    )

    distribution = {"STRONG_BUY": 0, "BUY": 0, "HOLD": 0, "SELL": 0, "STRONG_SELL": 0}
    avg_confidence = 0
    for v in verdicts:
        verdict_str = getattr(v, "verdict", "HOLD") or "HOLD"
        distribution[verdict_str] = distribution.get(verdict_str, 0) + 1
        avg_confidence += getattr(v, "confidence", 0) or 0

    if verdicts:
        avg_confidence /= len(verdicts)

    return {
        "total": len(verdicts),
        "distribution": distribution,
        "avg_confidence": round(avg_confidence, 3),
    }


def _get_cost_stats(session) -> Dict[str, Any]:
    """Get LLM cost over last 24h and 7d."""
    from sqlalchemy import func
    cutoff_24h = datetime.utcnow() - timedelta(hours=24)
    cutoff_7d = datetime.utcnow() - timedelta(days=7)

    cost_24h = session.query(func.sum(TokenUsage.cost_eur)).filter(
        TokenUsage.timestamp >= cutoff_24h
    ).scalar() or 0.0
    cost_7d = session.query(func.sum(TokenUsage.cost_eur)).filter(
        TokenUsage.timestamp >= cutoff_7d
    ).scalar() or 0.0

    return {
        "cost_24h": round(cost_24h, 4),
        "cost_7d": round(cost_7d, 4),
        "projected_monthly": round(cost_7d / 7 * 30, 2),
    }


def _find_biggest_errors(session) -> List[Dict]:
    """Find the picks with the biggest errors (wrong direction + large loss)."""
    cutoff = datetime.utcnow() - timedelta(days=14)
    advisories = (
        session.query(AnalysisReport)
        .filter(
            AnalysisReport.report_type == "daily_advisory",
            AnalysisReport.created_at >= cutoff,
        )
        .all()
    )

    errors: List[Dict] = []
    for report in advisories:
        try:
            data = json.loads(report.report_json)
        except Exception:
            continue

        outcomes = data.get("outcomes", {})
        all_recs = data.get("buy_recommendations", []) + data.get("sell_recommendations", [])

        for rec in all_recs:
            ticker = rec.get("ticker", "?")
            action = rec.get("action", "BUY")
            entry_price = rec.get("current_price", 0)
            components = rec.get("components", {})
            ticker_outcomes = outcomes.get(ticker, {})

            for h in [7, 14, 30, 3, 1]:
                h_key = f"T+{h}d"
                if h_key in ticker_outcomes:
                    outcome = ticker_outcomes[h_key]
                    if not outcome.get("correct", True):
                        errors.append({
                            "ticker": ticker,
                            "action": action,
                            "entry_price": entry_price,
                            "return_pct": outcome.get("return_pct", 0),
                            "horizon": h_key,
                            "date": report.created_at.strftime("%Y-%m-%d"),
                            "components": components,
                            "reasoning": rec.get("reasoning", {}),
                        })
                    break

    # Sort by absolute return (worst first)
    errors.sort(key=lambda e: e.get("return_pct", 0))
    return errors[:5]  # Top 5 worst


def _generate_improvement_suggestion(
    picks: Dict, flash: Dict, errors: List[Dict]
) -> str:
    """Use Groq to generate one concrete improvement suggestion."""
    if not config.groq_api_key:
        return _fallback_suggestion(picks, errors)

    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=config.groq_api_key,
            base_url="https://api.groq.com/openai/v1",
            timeout=30.0,
        )

        error_text = ""
        if errors:
            error_text = "Grootste fouten:\n" + "\n".join(
                f"- {e['ticker']} {e['action']}: {e['return_pct']:+.1f}% op {e['horizon']} "
                f"(components: {json.dumps(e.get('components', {}), default=str)[:100]})"
                for e in errors[:3]
            )

        response = client.chat.completions.create(
            model=config.groq_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Je bent een quant developer die een trading prediction systeem verbetert. "
                        "Het systeem voorspelt stock price movements op basis van geopolitiek nieuws, "
                        "swarm consensus (12 AI experts), en insider signalen. "
                        "Geef PRECIES 1 concrete, implementeerbare verbetering. "
                        "Noem het specifieke bestand en de functie die aangepast moet worden. "
                        "Max 150 woorden. Schrijf in het Nederlands."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Advisory accuracy: {picks['accuracy']}% ({picks['correct']}/{picks['correct']+picks['incorrect']})\n"
                        f"Flash alert accuracy: {flash['accuracy']}%\n"
                        f"Avg return on picks: {picks['avg_return']}%\n"
                        f"{error_text}\n\n"
                        "Wat is de #1 verbetering die we vandaag kunnen implementeren "
                        "om de prediction accuracy te verhogen?"
                    ),
                },
            ],
            max_tokens=300,
            temperature=0.4,
        )
        content = response.choices[0].message.content
        return content.strip() if content else _fallback_suggestion(picks, errors)
    except Exception as e:
        logger.warning("Improvement suggestion generation failed: %s", e)
        return _fallback_suggestion(picks, errors)


def _fallback_suggestion(picks: Dict, errors: List[Dict]) -> str:
    """Generate a basic suggestion without LLM."""
    if picks["accuracy"] < 50:
        return (
            "Accuracy is onder 50%. Focus op het verlagen van de composite_score "
            "threshold in daily_advisory.py — alleen picks met score > 0.65 doorlaten. "
            "Dit filtert low-conviction picks die de accuracy drukken."
        )
    if errors:
        worst = errors[0]
        return (
            f"Grootste fout: {worst['ticker']} ({worst['return_pct']:+.1f}%). "
            f"Check de {worst['action']} thesis en evalueer welke component "
            f"(geopolitical/confidence/swarm/momentum/insider) het meest bijdroeg. "
            f"Overweeg die component weight te verlagen in de EMA."
        )
    return "Systeem draait stabiel. Overweeg meer Polymarket data te integreren voor betere probability calibratie."


# ══════════════════════════════════════════════════════════════════
# Formatting
# ══════════════════════════════════════════════════════════════════

def _format_briefing(
    today: date,
    picks: Dict,
    flash: Dict,
    pipeline: Dict,
    swarm: Dict,
    cost: Dict,
    errors: List[Dict],
    improvement: str,
) -> str:
    """Format everything into a markdown briefing."""
    lines = [
        f"# Daily Briefing — {today.isoformat()}",
        f"Generated: {datetime.utcnow().strftime('%H:%M UTC')}",
        "",
        "---",
        "",
        "## 1. Prediction Accuracy (afgelopen 7 dagen)",
        "",
        f"| Metric | Waarde |",
        f"|--------|--------|",
        f"| Total picks | {picks['total']} |",
        f"| Correct | {picks['correct']} |",
        f"| Incorrect | {picks['incorrect']} |",
        f"| Pending | {picks['pending']} |",
        f"| **Accuracy** | **{picks['accuracy']}%** |",
        f"| Avg return | {picks['avg_return']}% |",
        "",
    ]

    # Recent picks table
    if picks["picks"]:
        lines.extend([
            "### Recente picks",
            "",
            "| Datum | Ticker | Actie | Entry | Outcome | Return | Status | Horizon |",
            "|-------|--------|-------|-------|---------|--------|--------|---------|",
        ])
        for p in picks["picks"][-10:]:
            status = "pending" if p["correct"] is None else ("correct" if p["correct"] else "FOUT")
            status_icon = "⏳" if p["correct"] is None else ("✅" if p["correct"] else "❌")
            lines.append(
                f"| {p['date']} | {p['ticker']} | {p['action']} | "
                f"${p['entry_price']:.2f} | ${p.get('outcome_price', 0):.2f} | "
                f"{p['return_pct']:+.1f}% | {status_icon} {status} | {p['horizon']} |"
            )
        lines.append("")

    # Flash alerts
    lines.extend([
        "## 2. Flash Alert Accuracy",
        "",
        f"| Metric | Waarde |",
        f"|--------|--------|",
        f"| Total alerts (7d) | {flash['total']} |",
        f"| Evaluated | {flash['evaluated']} |",
        f"| Correct | {flash['correct']} |",
        f"| **Accuracy** | **{flash['accuracy']}%** |",
        "",
    ])

    # Biggest errors
    if errors:
        lines.extend([
            "## 3. Grootste Fouten (leren van)",
            "",
        ])
        for i, e in enumerate(errors, 1):
            lines.extend([
                f"### Fout {i}: {e['ticker']} ({e['action']}) — {e['return_pct']:+.1f}%",
                f"- Datum: {e['date']}, Horizon: {e['horizon']}",
                f"- Entry: ${e['entry_price']:.2f}",
                f"- Components: {json.dumps(e.get('components', {}), default=str)}",
            ])
            reasoning = e.get("reasoning", {})
            if isinstance(reasoning, dict):
                thesis = reasoning.get("thesis", "")
                if thesis:
                    lines.append(f"- Thesis: {thesis[:200]}")
            lines.append("")

    # Pipeline stats
    lines.extend([
        "## 4. Pipeline Health (24h)",
        "",
        f"| Metric | Waarde |",
        f"|--------|--------|",
        f"| Articles processed | {pipeline['articles_24h']} |",
        f"| Trading signals | {pipeline['signals_24h']} |",
        f"| Swarm verdicts | {pipeline['verdicts_24h']} |",
        f"| Active run-ups | {pipeline['active_runups']} |",
        "",
    ])

    # Swarm stats
    lines.extend([
        "## 5. Swarm Consensus (24h)",
        "",
        f"- Verdicts: {swarm['total']}",
        f"- Avg confidence: {swarm['avg_confidence']:.1%}",
        f"- Distribution: {json.dumps(swarm['distribution'])}",
        "",
    ])

    # Cost
    lines.extend([
        "## 6. Kosten",
        "",
        f"| Periode | Kosten |",
        f"|---------|--------|",
        f"| 24h | EUR {cost['cost_24h']} |",
        f"| 7d | EUR {cost['cost_7d']} |",
        f"| Projectie/maand | EUR {cost['projected_monthly']} |",
        "",
    ])

    # Improvement suggestion
    lines.extend([
        "## 7. Verbetering van Vandaag",
        "",
        improvement,
        "",
        "---",
        "",
        "## Hoe te gebruiken",
        "",
        "Open Claude Code in `/home/opposite/openclaw-news-analyzer/` en zeg:",
        "",
        f'```',
        f'Lees briefings/{today.isoformat()}.md en implementeer de verbetering',
        f'```',
        "",
    ])

    return "\n".join(lines)
