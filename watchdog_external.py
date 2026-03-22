#!/usr/bin/env python3
"""External Watchdog for OpenClaw News Analyzer.

Runs independently via systemd timer (every 15 minutes).
Performs deterministic health checks — zero LLM tokens.
Sends alerts via Telegram when issues are detected.
Can restart the engine if it's down.

Usage:
    python3 watchdog_external.py           # Run all checks
    python3 watchdog_external.py --restart  # Also auto-restart engine on failure
"""

import json
import os
import subprocess
import sys
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# --- Configuration ---
ENGINE_PORT = 9121
DB_PATH = Path("/home/opposite/openclaw-news-analyzer/engine/data/news_analyzer.db")
ENV_PATH = Path("/home/opposite/openclaw-news-analyzer/.env")
SERVICE_NAME = "openclaw-news-analyzer.service"
CONSECUTIVE_FAIL_FILE = Path("/tmp/openclaw-watchdog-failures.json")

# Thresholds
MAX_VERDICT_AGE_HOURS = 4
MAX_ADVISORY_AGE_HOURS = 36
MAX_CONSECUTIVE_FAILURES_BEFORE_RESTART = 3
MAX_CONSECUTIVE_FAILURES_BEFORE_GROQ = 3  # reserved for future use


def _load_env():
    """Load .env file for Telegram credentials."""
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip()
    return env


def _send_telegram(text: str) -> bool:
    """Send alert via Telegram Bot API."""
    env = _load_env()
    token = env.get("TELEGRAM_BOT_TOKEN")
    chat_id = env.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[WARN] Telegram not configured in .env")
        return False

    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")
        return False


def check_port_open() -> dict:
    """Check if engine is listening on expected port."""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(("127.0.0.1", ENGINE_PORT))
        sock.close()
        if result == 0:
            return {"name": "Port", "status": "OK", "detail": f"Port {ENGINE_PORT} open"}
        return {"name": "Port", "status": "FAIL", "detail": f"Port {ENGINE_PORT} closed"}
    except Exception as e:
        return {"name": "Port", "status": "FAIL", "detail": str(e)}


def check_api_health() -> dict:
    """Check /api/status endpoint."""
    try:
        import urllib.request
        url = f"http://127.0.0.1:{ENGINE_PORT}/api/status"
        t0 = time.time()
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ms = round((time.time() - t0) * 1000)
            if resp.status == 200:
                data = json.loads(resp.read())
                if data.get("healthy"):
                    return {"name": "API", "status": "OK", "detail": f"Healthy ({ms}ms)"}
                return {"name": "API", "status": "WARN", "detail": f"Degraded ({ms}ms)"}
            return {"name": "API", "status": "FAIL", "detail": f"HTTP {resp.status}"}
    except Exception as e:
        return {"name": "API", "status": "FAIL", "detail": f"Unreachable: {e}"}


def check_last_verdict() -> dict:
    """Check if swarm verdicts are fresh."""
    if not DB_PATH.exists():
        return {"name": "Swarm", "status": "FAIL", "detail": "Database not found"}
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        cursor = conn.execute("SELECT MAX(created_at) FROM swarm_verdicts")
        row = cursor.fetchone()
        conn.close()
        if not row or not row[0]:
            return {"name": "Swarm", "status": "WARN", "detail": "No verdicts found"}
        last = datetime.fromisoformat(row[0])
        age_h = (datetime.utcnow() - last).total_seconds() / 3600
        if age_h > MAX_VERDICT_AGE_HOURS:
            return {"name": "Swarm", "status": "WARN", "detail": f"Last verdict {age_h:.1f}h ago"}
        return {"name": "Swarm", "status": "OK", "detail": f"Last verdict {age_h:.1f}h ago"}
    except Exception as e:
        return {"name": "Swarm", "status": "FAIL", "detail": str(e)}


def check_last_advisory() -> dict:
    """Check if daily advisory is fresh."""
    if not DB_PATH.exists():
        return {"name": "Advisory", "status": "FAIL", "detail": "Database not found"}
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        cursor = conn.execute(
            "SELECT MAX(created_at) FROM analysis_reports WHERE report_type = 'daily_advisory'"
        )
        row = cursor.fetchone()
        conn.close()
        if not row or not row[0]:
            return {"name": "Advisory", "status": "WARN", "detail": "No advisory found"}
        last = datetime.fromisoformat(row[0])
        age_h = (datetime.utcnow() - last).total_seconds() / 3600
        if age_h > MAX_ADVISORY_AGE_HOURS:
            return {"name": "Advisory", "status": "FAIL", "detail": f"Last advisory {age_h:.1f}h ago"}
        return {"name": "Advisory", "status": "OK", "detail": f"Last advisory {age_h:.1f}h ago"}
    except Exception as e:
        return {"name": "Advisory", "status": "FAIL", "detail": str(e)}


def check_telegram_config() -> dict:
    """Check if Telegram credentials are set."""
    env = _load_env()
    token = env.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = env.get("TELEGRAM_CHAT_ID", "")
    if token and chat_id:
        return {"name": "Telegram", "status": "OK", "detail": "Configured"}
    missing = []
    if not token:
        missing.append("BOT_TOKEN")
    if not chat_id:
        missing.append("CHAT_ID")
    return {"name": "Telegram", "status": "FAIL", "detail": f"Missing: {', '.join(missing)}"}


def check_innovation_metric(metric_type: str, label: str) -> dict:
    """Generic check for any innovation roadmap metric freshness."""
    na_db = Path("/home/opposite/news-analyzer/news.db")
    if not na_db.exists():
        return {"name": label, "status": "WARN", "detail": "news.db not found"}
    try:
        conn = sqlite3.connect(str(na_db), timeout=5)
        cursor = conn.execute(
            "SELECT MAX(calculated_at) FROM dashboard_metrics WHERE metric_type = ?",
            (metric_type,)
        )
        row = cursor.fetchone()
        conn.close()
        if not row or not row[0]:
            return {"name": label, "status": "WARN", "detail": "No data yet"}
        last = datetime.fromisoformat(row[0])
        age_min = (datetime.utcnow() - last).total_seconds() / 60
        if age_min > 20:
            return {"name": label, "status": "WARN", "detail": f"Stale ({age_min:.0f}m ago)"}
        return {"name": label, "status": "OK", "detail": f"Fresh ({age_min:.0f}m ago)"}
    except Exception as e:
        return {"name": label, "status": "WARN", "detail": str(e)}


def check_sentiment_divergence_metric() -> dict:
    """Check if sentiment divergence metric is being calculated (Feature 1)."""
    na_db = Path("/home/opposite/news-analyzer/news.db")
    if not na_db.exists():
        return {"name": "SentDivergence", "status": "WARN", "detail": "news.db not found"}
    try:
        conn = sqlite3.connect(str(na_db), timeout=5)
        cursor = conn.execute(
            "SELECT MAX(calculated_at) FROM dashboard_metrics WHERE metric_type = 'sentiment_divergence'"
        )
        row = cursor.fetchone()
        conn.close()
        if not row or not row[0]:
            return {"name": "SentDivergence", "status": "WARN", "detail": "No data yet"}
        last = datetime.fromisoformat(row[0])
        age_min = (datetime.utcnow() - last).total_seconds() / 60
        if age_min > 20:
            return {"name": "SentDivergence", "status": "WARN", "detail": f"Stale ({age_min:.0f}m ago)"}
        return {"name": "SentDivergence", "status": "OK", "detail": f"Fresh ({age_min:.0f}m ago)"}
    except Exception as e:
        return {"name": "SentDivergence", "status": "WARN", "detail": str(e)}


def check_systemd_service() -> dict:
    """Check if engine systemd service is active."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", SERVICE_NAME],
            capture_output=True, text=True, timeout=5
        )
        status = result.stdout.strip()
        if status == "active":
            return {"name": "Service", "status": "OK", "detail": f"{SERVICE_NAME} active"}
        return {"name": "Service", "status": "FAIL", "detail": f"{SERVICE_NAME}: {status}"}
    except Exception as e:
        return {"name": "Service", "status": "FAIL", "detail": str(e)}


def _load_failure_count() -> int:
    """Load consecutive failure count from temp file."""
    try:
        if CONSECUTIVE_FAIL_FILE.exists():
            data = json.loads(CONSECUTIVE_FAIL_FILE.read_text())
            return data.get("count", 0)
    except Exception:
        pass
    return 0


def _save_failure_count(count: int):
    """Save consecutive failure count to temp file."""
    try:
        CONSECUTIVE_FAIL_FILE.write_text(json.dumps({"count": count, "last": datetime.utcnow().isoformat()}))
    except Exception:
        pass


def main():
    auto_restart = "--restart" in sys.argv

    checks = [
        check_systemd_service(),
        check_port_open(),
        check_api_health(),
        check_last_verdict(),
        check_last_advisory(),
        check_telegram_config(),
        check_sentiment_divergence_metric(),
        check_innovation_metric("swarm_vs_market", "SwarmVsMarket"),
        check_innovation_metric("expert_leaderboard", "ExpertBoard"),
        check_innovation_metric("flash_alert_roi", "FlashROI"),
        check_innovation_metric("market_heatmap", "Heatmap"),
        check_innovation_metric("prediction_accuracy", "PredAccuracy"),
        check_innovation_metric("source_credibility", "SrcCredibility"),
        check_innovation_metric("morning_briefing", "Briefing"),
        check_innovation_metric("portfolio_alerts", "PortAlerts"),
        check_innovation_metric("news_volume_impact", "NewsVolImpact"),
        check_innovation_metric("geopolitical_sectors", "GeopolSectors"),
        check_innovation_metric("decision_tree_accuracy", "TreeAccuracy"),
        check_innovation_metric("confidence_weights", "ConfWeights"),
        check_innovation_metric("track_record", "TrackRecord"),
    ]

    fails = [c for c in checks if c["status"] == "FAIL"]
    warns = [c for c in checks if c["status"] == "WARN"]

    # Print report
    emoji = {"OK": "\u2705", "WARN": "\u26a0\ufe0f", "FAIL": "\u274c"}
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"OpenClaw External Watchdog \u2014 {ts}")
    for c in checks:
        print(f"  {emoji[c['status']]} {c['name']}: {c['detail']}")

    if not fails and not warns:
        _save_failure_count(0)
        print("All systems OK.")
        return 0

    # Track consecutive failures
    prev_count = _load_failure_count()
    if fails:
        new_count = prev_count + 1
        _save_failure_count(new_count)
    else:
        _save_failure_count(0)
        new_count = 0

    # Send Telegram alert
    if fails:
        lines = [f"<b>\U0001f534 External Watchdog \u2014 {len(fails)} FAIL</b>", ts, ""]
        for c in checks:
            lines.append(f"{emoji[c['status']]} {c['name']}: {c['detail']}")

        # Auto-restart if enabled and threshold reached
        if auto_restart and new_count >= MAX_CONSECUTIVE_FAILURES_BEFORE_RESTART:
            # Only restart if port/API/service failed (not just stale data)
            critical_fails = [c for c in fails if c["name"] in ("Port", "API", "Service")]
            if critical_fails:
                lines.append("")
                lines.append(f"\U0001f504 Auto-restart triggered ({new_count} consecutive failures)")
                try:
                    subprocess.run(
                        ["systemctl", "restart", SERVICE_NAME],
                        capture_output=True, timeout=30
                    )
                    lines.append("\u2705 Service restart command sent")
                    _save_failure_count(0)
                except Exception as e:
                    lines.append(f"\u274c Restart failed: {e}")

        _send_telegram("\n".join(lines))
    elif warns:
        # Only alert on warnings if there are 2+ consecutive
        if prev_count >= 1:
            lines = [f"<b>\U0001f7e1 External Watchdog \u2014 {len(warns)} WARN</b>", ts, ""]
            for c in checks:
                if c["status"] != "OK":
                    lines.append(f"{emoji[c['status']]} {c['name']}: {c['detail']}")
            _send_telegram("\n".join(lines))

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
