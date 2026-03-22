"""
Watchdog AI — Platform Health Checker
======================================
Runs 2× daily (07:00 + 19:00 UTC).  Performs 14 pure-Python pipeline
checks, and if issues are found, makes exactly ONE Groq call for
root-cause diagnosis.  Reports via Telegram.

Cost: effectively zero (all checks are DB queries; ≤1 free Groq call).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import func

from .config import config
from .db import (
    Article,
    ArticleBrief,
    AnalysisReport,
    DecisionNode,
    FlashAlert,
    PriceSnapshot,
    RunUp,
    SwarmVerdict,
    TradingSignal,
    TokenUsage,
    get_session,
)

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────

_T = {
    # RSS Feeds
    "rss_fail_2h": 0,          # 0 articles in 2h => FAIL
    "rss_warn_2h": 20,         # <20 articles in 2h => WARN
    "rss_source_warn": 5,      # <5 distinct sources in 6h => WARN
    # GDELT
    "gdelt_fail_2h": 0,
    "gdelt_warn_2h": 5,
    # Twitter
    "twitter_fail_mult": 3,    # no tweets in interval×3 => FAIL
    "twitter_warn_mult": 2,    # no tweets in interval×2 => WARN
    # NLP
    "nlp_fail_pct": 50,        # <50% articles briefed => FAIL
    "nlp_warn_pct": 85,
    "nlp_stale_hours": 2,      # newest brief older than 2h => FAIL
    # Signals
    "signal_warn_hours": 6,
    "signal_fail_hours": 12,
    # Swarm
    "swarm_fail_hours": 4,
    "swarm_warn_hours": 2,
    # Deep analysis
    "deep_fail_hours": 26,
    "deep_warn_hours": 14,
    # Daily advisory
    "advisory_fail_hours": 36,
    "advisory_warn_hours": 26,
    # Advisory refresh
    "refresh_fail_hours": 24,
    "refresh_warn_hours": 12,
    # Trees
    "tree_fail_hours": 24,
    "tree_warn_hours": 6,
    # Price snapshots
    "price_fail_hours": 10,
    "price_warn_hours": 6,
    # API
    "api_warn_ms": 5000,
    # Investment Swarm V3 (news-analyzer cross-DB)
    "swarm_v3_fail_hours": 18,     # No consensus in 18h => FAIL (should run 3×/day)
    "swarm_v3_warn_hours": 8,      # No consensus in 8h => WARN
    "swarm_v3_stale_picks": 36,    # Picks older than 36h => FAIL (stale advice)
    # News-analyzer Flask web service
    "flask_port": 5000,
    "flask_warn_ms": 8000,
}


# ── Data structures ───────────────────────────────────────────────

@dataclass
class HealthCheck:
    name: str
    status: str = "OK"       # OK | WARN | FAIL
    detail: str = ""
    metric: Optional[float] = None


# ── Individual checks ─────────────────────────────────────────────

def _check_rss(session) -> HealthCheck:
    cutoff_2h = datetime.utcnow() - timedelta(hours=2)
    cutoff_6h = datetime.utcnow() - timedelta(hours=6)
    count = session.query(func.count(Article.id)).filter(
        Article.fetched_at >= cutoff_2h
    ).scalar() or 0
    sources = session.query(func.count(func.distinct(Article.source))).filter(
        Article.fetched_at >= cutoff_6h
    ).scalar() or 0

    if count <= _T["rss_fail_2h"]:
        return HealthCheck("RSS Feeds", "FAIL", f"0 articles in 2h ({sources} sources/6h)", count)
    if count < _T["rss_warn_2h"]:
        return HealthCheck("RSS Feeds", "WARN", f"Only {count} articles/2h ({sources} sources/6h)", count)
    if sources < _T["rss_source_warn"]:
        return HealthCheck("RSS Feeds", "WARN", f"{count} articles but only {sources} sources/6h", count)
    return HealthCheck("RSS Feeds", "OK", f"{count} articles/2h, {sources} sources/6h", count)


def _check_gdelt(session) -> HealthCheck:
    cutoff = datetime.utcnow() - timedelta(hours=2)
    count = session.query(func.count(Article.id)).filter(
        Article.source.like("GDELT%"),
        Article.fetched_at >= cutoff,
    ).scalar() or 0

    if count <= _T["gdelt_fail_2h"]:
        return HealthCheck("GDELT", "FAIL", "0 GDELT articles in 2h", count)
    if count < _T["gdelt_warn_2h"]:
        return HealthCheck("GDELT", "WARN", f"Only {count} GDELT articles/2h", count)
    return HealthCheck("GDELT", "OK", f"{count} articles/2h", count)


def _check_twitter(session) -> HealthCheck:
    if not config.twitter_enabled:
        return HealthCheck("Twitter OSINT", "OK", "Disabled", 0)
    window = timedelta(minutes=config.twitter_fetch_interval_minutes * _T["twitter_fail_mult"])
    cutoff = datetime.utcnow() - window
    count = session.query(func.count(Article.id)).filter(
        Article.source.like("X/Twitter%"),
        Article.fetched_at >= cutoff,
    ).scalar() or 0

    if count == 0:
        return HealthCheck("Twitter OSINT", "FAIL", f"0 tweets in {window}", count)
    if count < 2:
        return HealthCheck("Twitter OSINT", "WARN", f"Only {count} tweets in {window}", count)
    return HealthCheck("Twitter OSINT", "OK", f"{count} tweets", count)


def _check_nlp(session) -> HealthCheck:
    cutoff = datetime.utcnow() - timedelta(hours=6)
    total = session.query(func.count(Article.id)).filter(
        Article.fetched_at >= cutoff
    ).scalar() or 0
    if total == 0:
        return HealthCheck("NLP Pipeline", "WARN", "No articles to process in 6h", 0)

    briefed = session.query(func.count(ArticleBrief.id)).join(Article).filter(
        Article.fetched_at >= cutoff
    ).scalar() or 0
    pct = round(briefed / total * 100, 1) if total else 0

    # Also check freshness
    newest = session.query(func.max(ArticleBrief.processed_at)).scalar()
    stale = newest and (datetime.utcnow() - newest).total_seconds() > _T["nlp_stale_hours"] * 3600

    if pct < _T["nlp_fail_pct"] or stale:
        age = f", last brief {_age_str(newest)}" if newest else ", no briefs found"
        return HealthCheck("NLP Pipeline", "FAIL", f"{pct}% processed ({briefed}/{total}){age}", pct)
    if pct < _T["nlp_warn_pct"]:
        return HealthCheck("NLP Pipeline", "WARN", f"{pct}% processed ({briefed}/{total})", pct)
    return HealthCheck("NLP Pipeline", "OK", f"{pct}% processed ({briefed}/{total})", pct)


def _check_signals(session) -> HealthCheck:
    active_runups = session.query(func.count(RunUp.id)).filter(RunUp.status == "active").scalar() or 0
    if active_runups == 0:
        return HealthCheck("Trading Signals", "OK", "No active run-ups", 0)

    cutoff_6h = datetime.utcnow() - timedelta(hours=_T["signal_warn_hours"])
    cutoff_12h = datetime.utcnow() - timedelta(hours=_T["signal_fail_hours"])
    count_6h = session.query(func.count(TradingSignal.id)).filter(
        TradingSignal.created_at >= cutoff_6h
    ).scalar() or 0
    count_12h = session.query(func.count(TradingSignal.id)).filter(
        TradingSignal.created_at >= cutoff_12h
    ).scalar() or 0

    if active_runups > 3 and count_12h == 0:
        return HealthCheck("Trading Signals", "FAIL", f"{active_runups} active run-ups but 0 signals/12h", 0)
    if count_6h == 0:
        return HealthCheck("Trading Signals", "WARN", f"0 signals/6h ({active_runups} active run-ups)", 0)
    return HealthCheck("Trading Signals", "OK", f"{count_6h} signals/6h, {active_runups} run-ups", count_6h)


def _check_swarm(session) -> HealthCheck:
    if not config.swarm_enabled:
        return HealthCheck("Swarm Consensus", "OK", "Disabled", 0)
    latest = session.query(func.max(SwarmVerdict.created_at)).scalar()
    if not latest:
        return HealthCheck("Swarm Consensus", "FAIL", "No verdicts ever recorded", 0)

    age_h = (datetime.utcnow() - latest).total_seconds() / 3600
    if age_h > _T["swarm_fail_hours"]:
        return HealthCheck("Swarm Consensus", "FAIL", f"Last verdict {_age_str(latest)}", round(age_h, 1))
    if age_h > _T["swarm_warn_hours"]:
        return HealthCheck("Swarm Consensus", "WARN", f"Last verdict {_age_str(latest)}", round(age_h, 1))
    return HealthCheck("Swarm Consensus", "OK", f"Last verdict {_age_str(latest)}", round(age_h, 1))


def _check_deep_analysis(session) -> HealthCheck:
    latest = session.query(func.max(AnalysisReport.created_at)).filter(
        AnalysisReport.report_type == "daily_briefing"
    ).scalar()
    if not latest:
        return HealthCheck("Deep Analysis", "FAIL", "No reports ever", 0)
    age_h = (datetime.utcnow() - latest).total_seconds() / 3600
    if age_h > _T["deep_fail_hours"]:
        return HealthCheck("Deep Analysis", "FAIL", f"Last report {_age_str(latest)}", round(age_h, 1))
    if age_h > _T["deep_warn_hours"]:
        return HealthCheck("Deep Analysis", "WARN", f"Last report {_age_str(latest)}", round(age_h, 1))
    return HealthCheck("Deep Analysis", "OK", f"Last report {_age_str(latest)}", round(age_h, 1))


def _check_advisory(session) -> HealthCheck:
    latest = session.query(func.max(AnalysisReport.created_at)).filter(
        AnalysisReport.report_type == "daily_advisory"
    ).scalar()
    if not latest:
        return HealthCheck("Daily Advisory", "FAIL", "No advisory ever generated", 0)
    age_h = (datetime.utcnow() - latest).total_seconds() / 3600
    if age_h > _T["advisory_fail_hours"]:
        return HealthCheck("Daily Advisory", "FAIL", f"Last advisory {_age_str(latest)}", round(age_h, 1))
    if age_h > _T["advisory_warn_hours"]:
        return HealthCheck("Daily Advisory", "WARN", f"Last advisory {_age_str(latest)}", round(age_h, 1))
    return HealthCheck("Daily Advisory", "OK", f"Last advisory {_age_str(latest)}", round(age_h, 1))


def _check_advisory_refresh(session) -> HealthCheck:
    if not config.advisory_refresh_enabled:
        return HealthCheck("Advisory Refresh", "OK", "Disabled", 0)
    latest = session.query(func.max(AnalysisReport.created_at)).filter(
        AnalysisReport.report_type == "advisory_refresh"
    ).scalar()
    if not latest:
        return HealthCheck("Advisory Refresh", "WARN", "No refresh ever recorded", 0)
    age_h = (datetime.utcnow() - latest).total_seconds() / 3600
    if age_h > _T["refresh_fail_hours"]:
        return HealthCheck("Advisory Refresh", "FAIL", f"Last refresh {_age_str(latest)}", round(age_h, 1))
    if age_h > _T["refresh_warn_hours"]:
        return HealthCheck("Advisory Refresh", "WARN", f"Last refresh {_age_str(latest)}", round(age_h, 1))
    return HealthCheck("Advisory Refresh", "OK", f"Last refresh {_age_str(latest)}", round(age_h, 1))


def _check_flash_alerts(session) -> HealthCheck:
    if not config.flash_alert_enabled:
        return HealthCheck("Flash Alerts", "OK", "Disabled", 0)
    cutoff = datetime.utcnow() - timedelta(days=7)
    count = session.query(func.count(FlashAlert.id)).filter(
        FlashAlert.detected_at >= cutoff
    ).scalar() or 0
    if count == 0:
        return HealthCheck("Flash Alerts", "WARN", "No alerts in 7 days (may be quiet period)", count)
    return HealthCheck("Flash Alerts", "OK", f"{count} alerts in 7d", count)


def _check_trees(session) -> HealthCheck:
    active = session.query(func.count(RunUp.id)).filter(RunUp.status == "active").scalar() or 0
    if active == 0:
        return HealthCheck("Tree Generation", "OK", "No active run-ups", 0)
    latest = session.query(func.max(DecisionNode.created_at)).scalar()
    if not latest:
        return HealthCheck("Tree Generation", "WARN", f"{active} active run-ups but no tree nodes", 0)
    age_h = (datetime.utcnow() - latest).total_seconds() / 3600
    if age_h > _T["tree_fail_hours"]:
        return HealthCheck("Tree Generation", "FAIL", f"Last node {_age_str(latest)} ({active} run-ups)", round(age_h, 1))
    if age_h > _T["tree_warn_hours"]:
        return HealthCheck("Tree Generation", "WARN", f"Last node {_age_str(latest)} ({active} run-ups)", round(age_h, 1))
    return HealthCheck("Tree Generation", "OK", f"Last node {_age_str(latest)}", round(age_h, 1))


def _check_prices(session) -> HealthCheck:
    latest = session.query(func.max(PriceSnapshot.recorded_at)).scalar()
    if not latest:
        return HealthCheck("Price Snapshots", "FAIL", "No snapshots ever", 0)
    age_h = (datetime.utcnow() - latest).total_seconds() / 3600
    if age_h > _T["price_fail_hours"]:
        return HealthCheck("Price Snapshots", "FAIL", f"Last snapshot {_age_str(latest)}", round(age_h, 1))
    if age_h > _T["price_warn_hours"]:
        return HealthCheck("Price Snapshots", "WARN", f"Last snapshot {_age_str(latest)}", round(age_h, 1))
    return HealthCheck("Price Snapshots", "OK", f"Last snapshot {_age_str(latest)}", round(age_h, 1))


def _check_telegram() -> HealthCheck:
    from .telegram_notifier import is_telegram_configured
    if not is_telegram_configured():
        return HealthCheck("Telegram", "FAIL", "Not configured (token/chat_id missing)", 0)
    return HealthCheck("Telegram", "OK", "Configured", 1)


def _check_swarm_v3() -> HealthCheck:
    """Check Investment Swarm V3 consensus freshness (cross-DB to news-analyzer)."""
    import sqlite3 as _sqlite3
    _NA_DB = "/home/opposite/news-analyzer/news.db"
    try:
        conn = _sqlite3.connect(_NA_DB, timeout=5)
        conn.row_factory = _sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT created_at, confidence, macro_summary, top_picks "
            "FROM swarm_consensus "
            "WHERE error IS NULL AND confidence IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            return HealthCheck("Swarm V3", "FAIL", "No V3 consensus ever generated", 0)

        created = datetime.fromisoformat(row["created_at"])
        age_h = (datetime.utcnow() - created).total_seconds() / 3600

        # Check data quality
        issues = []
        if not row["macro_summary"]:
            issues.append("no summary")
        if not row["top_picks"]:
            issues.append("no picks")
        if row["confidence"] and row["confidence"] < 0.3:
            issues.append(f"low confidence {row['confidence']}")

        detail = f"Last V3 consensus {_age_str(created)}"
        if issues:
            detail += f" ({', '.join(issues)})"

        if age_h > _T["swarm_v3_fail_hours"] or (not row["top_picks"] and age_h > 4):
            return HealthCheck("Swarm V3", "FAIL", detail, round(age_h, 1))
        if age_h > _T["swarm_v3_warn_hours"] or issues:
            return HealthCheck("Swarm V3", "WARN", detail, round(age_h, 1))
        return HealthCheck("Swarm V3", "OK", detail, round(age_h, 1))
    except FileNotFoundError:
        return HealthCheck("Swarm V3", "FAIL", "news-analyzer DB not found", 0)
    except Exception as e:
        return HealthCheck("Swarm V3", "WARN", f"DB read error: {e}", 0)


def _check_flask_web() -> HealthCheck:
    """Check that the news-analyzer Flask web service is alive on port 5000."""
    import urllib.request
    url = f"http://127.0.0.1:{_T['flask_port']}/api/swarm/consensus/latest"
    try:
        t0 = time.time()
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ms = round((time.time() - t0) * 1000)
            # 401 = running but needs auth (OK), 200 = running with data
            if resp.status in (200, 401):
                if ms > _T["flask_warn_ms"]:
                    return HealthCheck("Flask Web", "WARN", f"Slow: {ms}ms", ms)
                return HealthCheck("Flask Web", "OK", f"{ms}ms", ms)
            return HealthCheck("Flask Web", "WARN", f"HTTP {resp.status}", ms)
    except urllib.request.HTTPError as e:
        # 401/403 means the server IS running (just needs auth)
        if e.code in (401, 403):
            return HealthCheck("Flask Web", "OK", f"Running (auth required)", e.code)
        return HealthCheck("Flask Web", "FAIL", f"HTTP {e.code}", e.code)
    except Exception as e:
        return HealthCheck("Flask Web", "FAIL", f"Unreachable: {e}", 0)


def _check_swarm_v3_api_integration() -> HealthCheck:
    """Verify that this engine's advisory API returns swarm_v3 data."""
    import urllib.request
    url = f"http://127.0.0.1:{config.engine_port}/api/status"
    try:
        # We can't call /api/advisory/latest without auth, but we can check
        # if the cross-DB file is readable
        import sqlite3 as _sqlite3
        _NA_DB = "/home/opposite/news-analyzer/news.db"
        conn = _sqlite3.connect(_NA_DB, timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM swarm_consensus WHERE confidence IS NOT NULL")
        count = cur.fetchone()[0]
        conn.close()
        if count == 0:
            return HealthCheck("V3 Integration", "WARN", "Cross-DB OK but 0 valid consensus records", count)
        return HealthCheck("V3 Integration", "OK", f"Cross-DB OK, {count} records", count)
    except Exception as e:
        return HealthCheck("V3 Integration", "FAIL", f"Cross-DB broken: {e}", 0)


def _check_arabella() -> HealthCheck:
    """Check Arabella notification system health."""
    try:
        s = get_session()
        try:
            setting = s.query(EngineSettings).get("arabella_sent_log")
            if not setting or not setting.value:
                return HealthCheck("Arabella", "OK", "No notifications sent yet (may be quiet)", 0)
            entries = json.loads(setting.value)
            recent = [e for e in entries if e.get("sent_at", "") > (datetime.utcnow() - timedelta(days=7)).isoformat()]
            return HealthCheck("Arabella", "OK", f"{len(recent)} notifications in 7d", len(recent))
        finally:
            s.close()
    except Exception as e:
        return HealthCheck("Arabella", "WARN", f"Check failed: {e}", 0)


def _check_api() -> HealthCheck:
    import urllib.request
    url = f"http://127.0.0.1:{config.engine_port}/api/briefs?limit=1"
    try:
        t0 = time.time()
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ms = round((time.time() - t0) * 1000)
            if resp.status == 200:
                if ms > _T["api_warn_ms"]:
                    return HealthCheck("API", "WARN", f"Slow: {ms}ms", ms)
                return HealthCheck("API", "OK", f"{ms}ms", ms)
            return HealthCheck("API", "FAIL", f"HTTP {resp.status}", ms)
    except Exception as e:
        return HealthCheck("API", "FAIL", f"Unreachable: {e}", 0)


# ── Pipeline throughput summary ───────────────────────────────────

def _throughput(session) -> dict:
    cutoff = datetime.utcnow() - timedelta(hours=24)
    articles = session.query(func.count(Article.id)).filter(Article.fetched_at >= cutoff).scalar() or 0
    briefs = session.query(func.count(ArticleBrief.id)).join(Article).filter(Article.fetched_at >= cutoff).scalar() or 0
    signals = session.query(func.count(TradingSignal.id)).filter(TradingSignal.created_at >= cutoff).scalar() or 0
    verdicts = session.query(func.count(SwarmVerdict.id)).filter(SwarmVerdict.created_at >= cutoff).scalar() or 0
    spending = session.query(func.sum(TokenUsage.cost_eur)).filter(TokenUsage.timestamp >= cutoff).scalar() or 0.0
    return {
        "articles_24h": articles,
        "briefs_24h": briefs,
        "signals_24h": signals,
        "verdicts_24h": verdicts,
        "spending_24h": round(spending, 4),
    }


# ── Groq diagnosis (max 1 call) ──────────────────────────────────

def _diagnose_with_groq(issues: List[HealthCheck], throughput: dict) -> str:
    """One Groq call to diagnose root cause of issues. Returns diagnosis text."""
    if not config.groq_api_key:
        return "(Groq niet beschikbaar — geen API key)"

    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=config.groq_api_key,
            base_url="https://api.groq.com/openai/v1",
            timeout=30.0,
        )

        issue_lines = "\n".join(
            f"- [{c.status}] {c.name}: {c.detail}" for c in issues
        )

        response = client.chat.completions.create(
            model=config.groq_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a DevOps diagnostician for OpenClaw, a geopolitical news "
                        "analysis platform. Two subsystems: "
                        "1) OpenClaw engine: RSS→NLP→Signals→Swarm→Advisory→Telegram. "
                        "2) Investment Swarm V3 (news-analyzer): 22 LLM experts in 3 tiers "
                        "(GROQ T1→Anthropic T2→Sonnet synthesis), Flask web on port 5000, "
                        "SQLite cross-DB integration via /home/opposite/news-analyzer/news.db. "
                        "Common V3 issues: Flask web crashed (port 5000 dead), GROQ rate "
                        "limits (6k TPM), Anthropic key not loading (needs load_dotenv override), "
                        "user_id filter mismatch (scheduled runs have user_id=NULL). "
                        "Diagnose root cause(s) and suggest 1-3 specific fixes. "
                        "Be concise (max 200 words). Respond in Dutch."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Pipeline health issues:\n{issue_lines}\n\n"
                        f"24h throughput: {throughput['articles_24h']} articles → "
                        f"{throughput['briefs_24h']} briefs → {throughput['signals_24h']} signals → "
                        f"{throughput['verdicts_24h']} verdicts\n"
                        f"Spending 24h: €{throughput['spending_24h']}"
                    ),
                },
            ],
            max_tokens=300,
            temperature=0.3,
        )
        content = response.choices[0].message.content
        return content.strip() if content else "(Geen diagnose)"
    except Exception as e:
        logger.warning("Groq diagnosis failed: %s", e)
        return f"(Diagnose mislukt: {e})"


# ── Auto-remediation ─────────────────────────────────────────────

def _auto_remediate(checks: List[HealthCheck]) -> List[str]:
    """Attempt to fix detected failures. Returns list of remediation actions taken."""
    actions: List[str] = []

    check_map = {c.name: c for c in checks}

    # --- Stale daily advisory (>36h) → regenerate ---
    adv = check_map.get("Daily Advisory")
    if adv and adv.status == "FAIL":
        try:
            logger.info("Auto-remediation: regenerating daily advisory...")
            from .daily_advisory import generate_daily_advisory
            result = generate_daily_advisory()
            if result:
                actions.append("Daily Advisory: regenerated successfully")
                logger.info("Auto-remediation: daily advisory regenerated.")
            else:
                actions.append("Daily Advisory: regeneration returned None")
                logger.warning("Auto-remediation: daily advisory returned None.")
        except Exception as e:
            actions.append(f"Daily Advisory: regeneration failed — {e}")
            logger.error("Auto-remediation: daily advisory failed: %s", e, exc_info=True)

    # --- Stale price snapshots (>6h) → re-fetch ---
    prices = check_map.get("Price Snapshots")
    if prices and prices.status in ("FAIL", "WARN"):
        try:
            logger.info("Auto-remediation: storing price snapshots...")
            from .engine import store_price_snapshots
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(store_price_snapshots())
                actions.append("Price Snapshots: scheduled async re-fetch")
            else:
                loop.run_until_complete(store_price_snapshots())
                actions.append("Price Snapshots: re-fetched successfully")
            logger.info("Auto-remediation: price snapshots refreshed.")
        except Exception as e:
            actions.append(f"Price Snapshots: re-fetch failed — {e}")
            logger.error("Auto-remediation: price snapshots failed: %s", e, exc_info=True)

    # --- Missing trading signals → run confidence scorer ---
    signals = check_map.get("Trading Signals")
    if signals and signals.status in ("FAIL", "WARN"):
        try:
            logger.info("Auto-remediation: running confidence scorer / signal update...")
            from .confidence_scorer import update_trading_signals
            new_signals = update_trading_signals()
            actions.append(f"Trading Signals: scorer produced {len(new_signals)} signal(s)")
            logger.info("Auto-remediation: confidence scorer produced %d signals.", len(new_signals))
        except Exception as e:
            actions.append(f"Trading Signals: scorer failed — {e}")
            logger.error("Auto-remediation: confidence scorer failed: %s", e, exc_info=True)

    # --- Flask web service down → restart ---
    flask = check_map.get("Flask Web")
    if flask and flask.status == "FAIL":
        try:
            import subprocess
            logger.info("Auto-remediation: restarting news-analyzer Flask web...")
            # Kill stale process if PID file exists
            pid_file = "/home/opposite/news-analyzer/logs/web.pid"
            try:
                with open(pid_file) as f:
                    old_pid = int(f.read().strip())
                os.kill(old_pid, 9)  # SIGKILL stale process
                logger.info("Killed stale Flask web PID %d", old_pid)
            except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
                pass
            # Start fresh
            proc = subprocess.Popen(
                ["python3", "-m", "src.web"],
                cwd="/home/opposite/news-analyzer",
                stdout=open("/home/opposite/news-analyzer/logs/web.log", "a"),
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            with open(pid_file, "w") as f:
                f.write(str(proc.pid))
            actions.append(f"Flask Web: restarted (PID {proc.pid})")
            logger.info("Auto-remediation: Flask web restarted with PID %d", proc.pid)
        except Exception as e:
            actions.append(f"Flask Web: restart failed — {e}")
            logger.error("Auto-remediation: Flask web restart failed: %s", e, exc_info=True)

    # --- Swarm V3 stale → trigger light consensus ---
    swarm_v3 = check_map.get("Swarm V3")
    if swarm_v3 and swarm_v3.status == "FAIL":
        try:
            import subprocess
            logger.info("Auto-remediation: triggering Swarm V3 light consensus...")
            # Run light consensus via the news-analyzer CLI
            result = subprocess.run(
                [
                    "python3", "-c",
                    "import asyncio; from src.investment_swarm import run_consensus; "
                    "asyncio.run(run_consensus(consensus_type='light'))",
                ],
                cwd="/home/opposite/news-analyzer",
                capture_output=True,
                text=True,
                timeout=900,  # 15 min max
            )
            if result.returncode == 0:
                actions.append("Swarm V3: light consensus triggered successfully")
                logger.info("Auto-remediation: Swarm V3 light consensus completed.")
            else:
                actions.append(f"Swarm V3: consensus failed (rc={result.returncode})")
                logger.error("Auto-remediation: Swarm V3 failed: %s", result.stderr[:300])
        except subprocess.TimeoutExpired:
            actions.append("Swarm V3: consensus timed out (15min)")
            logger.error("Auto-remediation: Swarm V3 timed out after 15 minutes.")
        except Exception as e:
            actions.append(f"Swarm V3: trigger failed — {e}")
            logger.error("Auto-remediation: Swarm V3 failed: %s", e, exc_info=True)

    # --- V3 Integration broken → just log (DB path issue, needs manual fix) ---
    v3_int = check_map.get("V3 Integration")
    if v3_int and v3_int.status == "FAIL":
        actions.append("V3 Integration: cross-DB connection broken — check /home/opposite/news-analyzer/news.db exists")

    # --- API unresponsive → self-restart ---
    api = check_map.get("API")
    if api and api.status == "FAIL":
        logger.critical("Engine unresponsive — triggering self-restart via sys.exit(1)")
        os._exit(1)  # Hard exit — systemd will restart

    # --- Stale scheduler detection ---
    try:
        from .engine import scheduler
        if not scheduler.running:
            logger.critical("Scheduler is dead — triggering restart")
            os._exit(1)
    except Exception as e:
        actions.append(f"Scheduler check: import/check failed — {e}")
        logger.error("Auto-remediation: scheduler check failed: %s", e, exc_info=True)

    return actions


# ── Telegram report formatting ────────────────────────────────────

_EMOJI = {"OK": "✅", "WARN": "⚠️", "FAIL": "❌"}


def _format_report(checks: List[HealthCheck], throughput: dict, diagnosis: str,
                    remediation_actions: Optional[List[str]] = None) -> str:
    fails = sum(1 for c in checks if c.status == "FAIL")
    warns = sum(1 for c in checks if c.status == "WARN")
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    if fails == 0 and warns == 0:
        header = f"<b>🟢 OpenClaw Watchdog — All Systems OK</b>\n{ts}\n"
    elif fails > 0:
        header = f"<b>🔴 OpenClaw Watchdog — {fails} FAIL, {warns} WARN</b>\n{ts}\n"
    else:
        header = f"<b>🟡 OpenClaw Watchdog — {warns} WARN</b>\n{ts}\n"

    lines = []
    for c in checks:
        lines.append(f"{_EMOJI[c.status]} {c.name}: {c.detail}")

    # Throughput line
    tp = throughput
    lines.append("")
    lines.append(
        f"📊 24h: {tp['articles_24h']} articles → {tp['briefs_24h']} briefs → "
        f"{tp['signals_24h']} signals → {tp['verdicts_24h']} verdicts  |  €{tp['spending_24h']}"
    )

    if diagnosis:
        lines.append("")
        lines.append(f"<b>🤖 AI Diagnose:</b>\n{diagnosis}")

    if remediation_actions:
        lines.append("")
        lines.append("<b>🔧 Auto-remediation:</b>")
        for action in remediation_actions:
            lines.append(f"  • {action}")

    return header + "\n".join(lines)


# ── Main entry point ──────────────────────────────────────────────

def run_watchdog() -> None:
    """Run all pipeline health checks, optionally diagnose, report via Telegram."""
    logger.info("Watchdog health check starting...")
    session = get_session()
    try:
        checks: List[HealthCheck] = [
            _check_rss(session),
            _check_gdelt(session),
            _check_twitter(session),
            _check_nlp(session),
            _check_signals(session),
            _check_swarm(session),
            _check_swarm_v3(),
            _check_deep_analysis(session),
            _check_advisory(session),
            _check_advisory_refresh(session),
            _check_flash_alerts(session),
            _check_trees(session),
            _check_prices(session),
            _check_telegram(),
            _check_arabella(),
            _check_api(),
            _check_flask_web(),
            _check_swarm_v3_api_integration(),
        ]

        tp = _throughput(session)

        # Decide if AI diagnosis is needed
        fails = [c for c in checks if c.status == "FAIL"]
        warns = [c for c in checks if c.status == "WARN"]
        diagnosis = ""
        if fails or len(warns) >= 2:
            issues = fails + warns
            logger.info("Watchdog found %d FAIL + %d WARN — requesting Groq diagnosis.", len(fails), len(warns))
            diagnosis = _diagnose_with_groq(issues, tp)

        # Auto-remediate any failures (after diagnosis, before report)
        remediation_actions: List[str] = []
        if fails or warns:
            logger.info("Watchdog attempting auto-remediation for %d FAIL + %d WARN...", len(fails), len(warns))
            try:
                remediation_actions = _auto_remediate(checks)
                if remediation_actions:
                    logger.info("Watchdog auto-remediation actions: %s", remediation_actions)
                else:
                    logger.info("Watchdog auto-remediation: no actionable remediations.")
            except Exception:
                logger.exception("Watchdog auto-remediation crashed unexpectedly.")

        report = _format_report(checks, tp, diagnosis, remediation_actions)

        # Send via Telegram ONLY if there are issues (FAIL or WARN)
        # When everything is ✅, skip Telegram to avoid noise.
        has_issues = bool(fails or warns)
        from .telegram_notifier import is_telegram_configured, send_message
        if has_issues and is_telegram_configured():
            ok = send_message(report, parse_mode="HTML")
            if ok:
                logger.info("Watchdog report sent via Telegram (%d FAIL, %d WARN).", len(fails), len(warns))
            else:
                logger.warning("Watchdog report Telegram send failed.")
        elif not has_issues:
            logger.info("Watchdog: all checks OK — skipping Telegram notification.")
        else:
            logger.warning("Telegram not configured — watchdog report logged only.")

        # Always log summary
        fail_count = len(fails)
        warn_count = len(warns)
        logger.info(
            "Watchdog complete: %d OK, %d WARN, %d FAIL | 24h: %d articles, %d briefs, %d signals",
            len(checks) - fail_count - warn_count,
            warn_count,
            fail_count,
            tp["articles_24h"],
            tp["briefs_24h"],
            tp["signals_24h"],
        )
    except Exception:
        logger.exception("Watchdog health check failed unexpectedly.")
    finally:
        session.close()


# ── Helpers ───────────────────────────────────────────────────────

def _age_str(dt: Optional[datetime]) -> str:
    """Human-readable age string like '2h 15m ago'."""
    if not dt:
        return "never"
    delta = datetime.utcnow() - dt
    total_min = int(delta.total_seconds() / 60)
    if total_min < 1:
        return "just now"
    if total_min < 60:
        return f"{total_min}m ago"
    hours = total_min // 60
    mins = total_min % 60
    if hours < 24:
        return f"{hours}h {mins}m ago"
    days = hours // 24
    return f"{days}d {hours % 24}h ago"
