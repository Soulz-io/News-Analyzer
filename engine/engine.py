"""Main FastAPI application for the News Analyzer engine.

Responsibilities:
  - Create and configure the FastAPI app with CORS.
  - On startup: initialise the database, schedule recurring jobs.
  - APScheduler jobs:
      * ``fetch_and_process`` -- runs every FETCH_INTERVAL_MINUTES.
  - Expose all API routes via the ``api_routes`` router.
  - Serve via uvicorn.
"""

import asyncio
import logging
import os
import signal
import socket
import time as _time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncGenerator

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from .config import config
from .db import create_all, get_session
from .api_routes import router as api_router

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

scheduler = AsyncIOScheduler()


async def _fetch_and_process_impl() -> None:
    """Inner implementation of fetch_and_process (wrapped with timeout)."""
    # --- 1. Fetch ---
    from .fetcher import RSSFetcher

    async with RSSFetcher() as fetcher:
        new_articles = await fetcher.fetch_and_save()

    if not new_articles:
        logger.info("No new articles this cycle -- skipping NLP & analysis.")
        return

    logger.info("Fetched %d new articles.", len(new_articles))

    # --- 2. NLP pipeline (CPU-heavy: run in thread to avoid blocking event loop) ---
    from .nlp_pipeline import process_batch

    briefs = await asyncio.to_thread(process_batch, new_articles)
    logger.info("Produced %d article briefs.", len(briefs))

    if not briefs:
        return

    # --- 3. Narrative tracking (DB-heavy: run in thread) ---
    from .narrative_tracker import update_narratives, detect_runups

    updated_timelines = await asyncio.to_thread(update_narratives, briefs)
    changed_names = [t.narrative_name for t in updated_timelines]
    runups = await asyncio.to_thread(detect_runups, changed_narratives=changed_names)
    logger.info("Active run-ups after detection: %d", len(runups))

    # --- 4. Probability updates (DB-heavy: run in thread) ---
    from .probability_engine import update_probabilities

    updates = await asyncio.to_thread(update_probabilities, briefs)
    logger.info("Probability updates recorded: %d", len(updates))

    # --- 5. Flash detection (after all NLP, before confidence scoring) ---
    if config.flash_alert_enabled:
        try:
            from .flash_detector import evaluate_batch
            from .telegram_notifier import send_flash_alert
            from .db import get_session as _get_flash_session

            _fdb = _get_flash_session()
            try:
                flash_alerts = await asyncio.to_thread(evaluate_batch, briefs, _fdb)
                for alert in flash_alerts:
                    _fdb.add(alert)
                    if not alert.telegram_sent:
                        sent = await asyncio.to_thread(send_flash_alert, alert)
                        alert.telegram_sent = bool(sent)
                    # Arabella: check if this flash alert is big enough for implications
                    try:
                        from .arabella import notify_if_big_news_flash
                        await asyncio.to_thread(notify_if_big_news_flash, alert)
                    except Exception:
                        logger.debug("Arabella flash check failed (non-fatal).", exc_info=True)
                _fdb.commit()
                if flash_alerts:
                    logger.info("Flash detector: %d alert(s) generated", len(flash_alerts))
            finally:
                _fdb.close()
        except Exception:
            logger.exception("Flash detection failed (non-fatal).")


async def fetch_and_process() -> None:
    """Recurring job: fetch RSS feeds, run NLP, update narratives & probs.

    This is the engine's heartbeat.  It is deliberately written as a
    standalone coroutine so that APScheduler can invoke it.
    """
    logger.info("=== fetch_and_process cycle START ===")
    try:
        await asyncio.wait_for(_fetch_and_process_impl(), timeout=300)  # 5 min
    except asyncio.TimeoutError:
        logger.error("fetch_and_process TIMED OUT after 300s")
    except Exception:
        logger.exception("fetch_and_process cycle FAILED.")
    logger.info("=== fetch_and_process cycle END ===")


async def prediction_scoring() -> None:
    """Recurring job: auto-score predictions using resolved Polymarket markets."""
    logger.info("=== prediction_scoring cycle START ===")
    try:
        async def _prediction_scoring_impl():
            from .prediction_scorer import score_predictions
            count = await asyncio.to_thread(score_predictions)
            logger.info("Prediction scoring: %d resolved.", count)
        await asyncio.wait_for(_prediction_scoring_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("prediction_scoring TIMED OUT after 180s")
    except Exception:
        logger.exception("prediction_scoring cycle FAILED.")
    finally:
        logger.info("=== prediction_scoring cycle END ===")


async def deep_analysis_job() -> None:
    """Recurring job: run deep database analysis (2x daily)."""
    logger.info("=== deep_analysis cycle START ===")
    try:
        async def _deep_analysis_impl():
            from .deep_analysis import run_deep_analysis
            report = await asyncio.to_thread(run_deep_analysis, period_days=7)
            if report:
                logger.info("Deep analysis complete: report %d created.", report.id)
            else:
                logger.warning("Deep analysis returned None — check logs for errors.")
        await asyncio.wait_for(_deep_analysis_impl(), timeout=600)  # 10 min
    except asyncio.TimeoutError:
        logger.error("deep_analysis TIMED OUT after 600s")
    except Exception:
        logger.exception("deep_analysis cycle FAILED.")
    finally:
        logger.info("=== deep_analysis cycle END ===")


async def anomaly_detection_job() -> None:
    """Recurring job: detect anomalies in article volume, sentiment, feed health."""
    logger.info("=== anomaly_detection cycle START ===")
    try:
        async def _anomaly_detection_impl():
            from .confidence_scorer import detect_anomalies
            anomalies = await asyncio.to_thread(detect_anomalies)
            if anomalies:
                logger.warning("Anomaly detection: %d anomalies found.", len(anomalies))
                # Store for API access
                from .db import get_session, EngineSettings
                session = get_session()
                try:
                    import json as _json
                    s = session.query(EngineSettings).get("last_anomalies")
                    val = _json.dumps(anomalies)
                    if s:
                        s.value = val
                    else:
                        session.add(EngineSettings(key="last_anomalies", value=val))
                    session.commit()
                finally:
                    session.close()
            else:
                logger.info("Anomaly detection: no anomalies detected.")
        await asyncio.wait_for(_anomaly_detection_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("anomaly_detection TIMED OUT after 180s")
    except Exception:
        logger.exception("anomaly_detection cycle FAILED.")
    finally:
        logger.info("=== anomaly_detection cycle END ===")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Startup / shutdown lifecycle hook."""
    # --- Startup ---

    # --- PID lockfile mechanism ---
    port = config.engine_port
    run_dir = Path("/tmp/openclaw")
    run_dir.mkdir(parents=True, exist_ok=True)
    pid_file = run_dir / f"engine-{port}.pid"
    ready_file = run_dir / f"engine-{port}.ready"

    if pid_file.exists():
        try:
            old_pid = int(pid_file.read_text().strip())
            os.kill(old_pid, 0)  # check if alive
            logger.warning("Stale engine detected (PID %d) — sending SIGTERM...", old_pid)
            os.kill(old_pid, signal.SIGTERM)
            _time.sleep(5)
            try:
                os.kill(old_pid, 0)
                logger.warning("PID %d still alive after SIGTERM — sending SIGKILL...", old_pid)
                os.kill(old_pid, signal.SIGKILL)
            except OSError:
                pass  # already dead
        except (OSError, ValueError):
            pass  # process not running or bad PID

    pid_file.write_text(str(os.getpid()))
    logger.info("PID lockfile written: %s (PID %d)", pid_file, os.getpid())

    # --- Port availability check ---
    def _check_port(p: int) -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("0.0.0.0", p))
            return True
        except OSError:
            return False
        finally:
            sock.close()

    if not _check_port(port):
        logger.warning("Port %d in use — attempting fuser -k %d/tcp ...", port, port)
        os.system(f"fuser -k {port}/tcp")
        _time.sleep(2)
        if not _check_port(port):
            logger.error("Port %d still blocked after fuser -k. Proceeding anyway.", port)
        else:
            logger.info("Port %d freed successfully.", port)

    config.configure_logging()
    from .auth import log_auth_warning_if_needed
    log_auth_warning_if_needed()
    logger.info("Starting News Analyzer engine on port %d...", config.engine_port)

    # Initialise database tables
    create_all()

    # Schedule recurring fetch-and-process job
    scheduler.add_job(
        fetch_and_process,
        trigger=IntervalTrigger(minutes=config.fetch_interval_minutes),
        id="fetch_and_process",
        name="Fetch RSS & process",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    # Schedule Polymarket data refresh (every hour)
    scheduler.add_job(
        polymarket_refresh,
        trigger=IntervalTrigger(hours=1),
        id="polymarket_refresh",
        name="Polymarket data refresh",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )

    # Schedule prediction scoring (every 2 hours)
    scheduler.add_job(
        prediction_scoring,
        trigger=IntervalTrigger(hours=2),
        id="prediction_scoring",
        name="Auto-score predictions",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    # Schedule deep analysis (2x daily at 08:00 and 20:00 UTC)
    scheduler.add_job(
        deep_analysis_job,
        trigger=CronTrigger(hour="8,20", minute=0),
        id="deep_analysis",
        name="Deep database analysis",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )

    # Schedule confidence scoring (every 30 min — pure Python, no API cost)
    scheduler.add_job(
        confidence_scoring,
        trigger=IntervalTrigger(minutes=30),
        id="confidence_scoring",
        name="Confidence scoring & signal generation",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )

    # Schedule GDELT global events fetch (every 30 min)
    scheduler.add_job(
        gdelt_fetch_and_process,
        trigger=IntervalTrigger(minutes=30),
        id="gdelt_fetch",
        name="GDELT global events fetch",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )

    # Schedule proximity tracking update (every 5 min)
    scheduler.add_job(
        proximity_update,
        trigger=IntervalTrigger(minutes=5),
        id="proximity_update",
        name="Proximity threshold tracking",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )

    # Schedule X/Twitter OSINT fetch (if bearer token configured)
    if config.twitter_enabled:
        scheduler.add_job(
            twitter_fetch_and_process,
            trigger=IntervalTrigger(minutes=config.twitter_fetch_interval_minutes),
            id="twitter_fetch",
            name="X/Twitter OSINT fetch",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
            coalesce=True,
        )
        logger.info(
            "X/Twitter OSINT enabled — fetching every %d min.",
            config.twitter_fetch_interval_minutes,
        )
    else:
        logger.info("X/Twitter OSINT disabled — no bearer token configured.")

    # Schedule swarm consensus (if Groq API key is configured)
    if config.swarm_enabled:
        scheduler.add_job(
            swarm_consensus_job,
            trigger=IntervalTrigger(minutes=config.swarm_interval_minutes),
            id="swarm_consensus",
            name="Swarm consensus expert panel",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
            coalesce=True,
        )
        logger.info(
            "Swarm consensus enabled — running every %d min (Groq/Llama).",
            config.swarm_interval_minutes,
        )
    else:
        logger.info("Swarm consensus disabled — no Groq API key configured.")

    # Schedule price snapshot storage (every 1 hour — for news→price correlation)
    scheduler.add_job(
        store_price_snapshots,
        trigger=IntervalTrigger(hours=1),
        id="price_snapshots",
        name="Price snapshot storage for news-price correlation",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Price snapshot job scheduled — every 1 hour.")

    # Focused narratives get hourly price snapshots
    scheduler.add_job(
        store_focused_price_snapshots,
        trigger=IntervalTrigger(hours=1),
        id="focused_price_snapshots",
        name="Hourly price snapshots for focused narratives",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Focused price snapshot job scheduled — every 1 hour.")

    # Schedule automatic tree generation (every 2 hours)
    scheduler.add_job(
        tree_generation_job,
        trigger=IntervalTrigger(hours=2),
        id="tree_generation",
        name="Generate decision trees for active narratives",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Tree generation job scheduled — every 2 hours.")

    # Schedule anomaly detection (daily 06:00 UTC — before evaluation + advisory)
    scheduler.add_job(
        anomaly_detection_job,
        trigger=CronTrigger(hour=6, minute=0),
        id="anomaly_detection",
        name="Z-score anomaly detection (volume/sentiment/feeds)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Anomaly detection job scheduled — daily 06:00 UTC.")

    # Schedule daily advisory evaluation (06:25 UTC — before new advisory)
    scheduler.add_job(
        advisory_evaluation_job,
        trigger=CronTrigger(hour=6, minute=25),
        id="advisory_evaluation",
        name="Multi-horizon advisory evaluation",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Advisory evaluation job scheduled — daily 06:25 UTC.")

    # Schedule daily advisory generation (06:30 + 14:30 UTC)
    scheduler.add_job(
        daily_advisory_job,
        trigger=CronTrigger(hour=6, minute=30),
        id="daily_advisory",
        name="Daily investment advisory (morning)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    # NOTE: Afternoon advisory removed — 14:30 is handled by advisory_refresh only.
    # This saves 1 Claude call/day (~€0.03) without losing freshness.
    logger.info("Daily advisory job scheduled — 06:30 UTC only (afternoon = refresh only).")

    # Schedule weekly weight rebalancing (Sunday 07:35 UTC)
    scheduler.add_job(
        advisory_rebalance_job,
        trigger=CronTrigger(day_of_week="sun", hour=7, minute=35),
        id="advisory_rebalance",
        name="Weekly advisory weight rebalancing",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Advisory weight rebalance job scheduled — Sunday 07:35 UTC.")

    # --- Flash alert & advisory refresh scheduler jobs ---

    # Flash evaluation: every hour at :15
    scheduler.add_job(
        flash_evaluation_job,
        trigger=CronTrigger(minute=15),
        id="flash_evaluation",
        name="Flash alert evaluation (self-learning)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Flash evaluation job scheduled — hourly at :15.")

    # Flash alert expiry: every hour at :45
    scheduler.add_job(
        flash_expiry_job,
        trigger=CronTrigger(minute=45),
        id="flash_expiry",
        name="Flash alert expiry",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Flash expiry job scheduled — hourly at :45.")

    # Flash weight rebalance: Sunday 07:40 UTC (after advisory rebalance at 07:35)
    scheduler.add_job(
        flash_rebalance_job,
        trigger=CronTrigger(day_of_week="sun", hour=7, minute=40),
        id="flash_rebalance",
        name="Weekly flash weight rebalancing",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Flash weight rebalance job scheduled — Sunday 07:40 UTC.")

    # Advisory refresh: 3x daily (10:00, 14:30, 18:00 UTC)
    for _h, _m in [(10, 0), (16, 0)]:
        scheduler.add_job(
            advisory_refresh_job,
            trigger=CronTrigger(hour=_h, minute=_m),
            id=f"advisory_refresh_{_h:02d}{_m:02d}",
            name=f"Advisory refresh ({_h:02d}:{_m:02d} UTC)",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
            coalesce=True,
        )
    logger.info("Advisory refresh jobs scheduled — 10:00, 16:00 UTC.")

    # Batch send suppressed notifications: daily 06:05 UTC
    scheduler.add_job(
        batch_send_suppressed_job,
        trigger=CronTrigger(hour=6, minute=5),
        id="batch_send_suppressed",
        name="Batch send quiet-hours suppressed alerts",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Batch send suppressed job scheduled — daily 06:05 UTC.")

    # Daily self-evaluation briefing: 06:00 UTC (before advisory at 06:30)
    scheduler.add_job(
        daily_briefing_job,
        trigger=CronTrigger(hour=6, minute=0),
        id="daily_briefing",
        name="Daily self-evaluation briefing (briefings/*.md)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
        coalesce=True,
    )
    logger.info("Daily briefing job scheduled — 06:00 UTC.")

    # Morning briefing via Telegram (07:30 Amsterdam = 06:30 UTC pre-DST)
    from pytz import timezone as _pytz_tz
    scheduler.add_job(
        morning_briefing_job,
        trigger=CronTrigger(hour=7, minute=30, timezone=_pytz_tz("Europe/Amsterdam")),
        id="morning_briefing",
        name="Morning briefing to Telegram (zero LLM tokens)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Morning briefing job scheduled — 07:30 Amsterdam time.")

    # Deterministic heartbeat check (every 2 hours)
    scheduler.add_job(
        heartbeat_check_job,
        trigger=IntervalTrigger(hours=2),
        id="heartbeat_check",
        name="Deterministic heartbeat (gold/portfolio threshold check)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Heartbeat check job scheduled — every 2 hours.")

    # Schedule proactive child tree branching (every 4 hours)
    scheduler.add_job(
        proactive_children_job,
        trigger=IntervalTrigger(hours=4),
        id="proactive_children",
        name="Proactive scenario branches for strong-signal narratives",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Proactive children job scheduled — every 4 hours.")

    # Schedule ML model retrain (daily at 03:00 UTC — before 07:30 advisory)
    if os.environ.get("ML_ENABLED", "").lower() in ("1", "true", "yes"):
        scheduler.add_job(
            ml_retrain_job,
            trigger=CronTrigger(hour=3, minute=0),
            id="ml_retrain",
            name="ML model retrain (autoresearch)",
            replace_existing=True,
            max_instances=1,
        )
        logger.info("ML retrain job scheduled — daily 03:00 UTC.")
    else:
        logger.info("ML disabled — set ML_ENABLED=true to activate.")

    # Watchdog AI health checker — 2x daily at 07:00 and 19:00 UTC
    scheduler.add_job(
        watchdog_health_check,
        trigger=CronTrigger(hour="7,19", minute=0),
        id="watchdog_health_check",
        name="Watchdog AI platform health checker",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Watchdog health check scheduled — 07:00 and 19:00 UTC.")

    # Pipeline health check — every 6 hours
    scheduler.add_job(
        pipeline_health_job,
        trigger=CronTrigger(hour="6,12,18,0", minute=10),
        id="pipeline_health",
        name="Pipeline data quality health check (zero LLM tokens)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Pipeline health check scheduled — every 6 hours.")

    # Schedule daily data cleanup (04:00 UTC)
    scheduler.add_job(
        cleanup_old_data,
        trigger=CronTrigger(hour=4, minute=0),
        id="cleanup_old_data",
        name="Daily old data cleanup + vacuum",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
    )
    logger.info("Daily data cleanup scheduled — 04:00 UTC.")

    scheduler.start()
    logger.info(
        "Scheduler started -- fetch %dmin, polymarket 1h, scoring 2h, confidence 30min, GDELT 30min, swarm %s, analysis 08:00+20:00 UTC, trees 2h, ML %s.",
        config.fetch_interval_minutes,
        f"{config.swarm_interval_minutes}min" if config.swarm_enabled else "disabled",
        "enabled" if os.environ.get("ML_ENABLED", "").lower() in ("1", "true", "yes") else "disabled",
    )

    # --- Startup smoke test ---
    try:
        from sqlalchemy import text as _sa_text
        _smoke_session = get_session()
        _smoke_session.execute(_sa_text("SELECT 1"))
        _smoke_session.close()
        logger.info("Smoke test: database connection OK.")
    except Exception as _smoke_err:
        logger.error("Smoke test: database connection FAILED — %s", _smoke_err)

    _job_count = len(scheduler.get_jobs())
    if _job_count > 0:
        logger.info("Smoke test: scheduler has %d jobs — OK.", _job_count)
    else:
        logger.warning("Smoke test: scheduler has 0 jobs!")

    logger.info("Engine ready — all smoke tests passed")
    try:
        ready_file.write_text("READY")
        logger.info("Ready file written: %s", ready_file)
    except Exception as _rf_err:
        logger.warning("Could not write ready file: %s", _rf_err)

    # Generate child trees for any already-confirmed nodes missing children
    asyncio.create_task(_generate_pending_child_trees_async())

    # Run one immediate cycle in the background so the engine is pre-warmed
    asyncio.create_task(_initial_fetch())

    yield

    # --- Graceful Shutdown ---
    # 1. Stop scheduler (wait up to 10s for running jobs to finish)
    logger.info("Shutting down scheduler (waiting up to 10s)...")
    try:
        await asyncio.wait_for(
            asyncio.to_thread(scheduler.shutdown, wait=True),
            timeout=10,
        )
    except asyncio.TimeoutError:
        logger.warning("Scheduler did not stop within 10s — forcing shutdown.")
        scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped.")

    # 2. Give pending background tasks 5s to finish, then cancel stragglers
    logger.info("Waiting for background tasks to finish (up to 5s)...")
    current = asyncio.current_task()
    pending = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
    if pending:
        done, still_pending = await asyncio.wait(pending, timeout=5)
        if still_pending:
            logger.info("Cancelling %d remaining tasks after timeout.", len(still_pending))
            for task in still_pending:
                task.cancel()
            await asyncio.gather(*still_pending, return_exceptions=True)
        logger.info("Resolved %d background tasks (%d finished, %d cancelled).",
                     len(pending), len(done), len(still_pending) if still_pending else 0)

    # 3. Dispose SQLAlchemy engine (close all DB connections)
    from .db import _get_engine
    try:
        db_engine = _get_engine()
        db_engine.dispose()
        logger.info("Database connections disposed.")
    except Exception:
        logger.exception("Failed to dispose database engine.")

    # 4. Remove PID lockfile and ready file
    try:
        pid_file.unlink(missing_ok=True)
        logger.info("PID lockfile removed: %s", pid_file)
    except Exception:
        pass
    try:
        ready_file.unlink(missing_ok=True)
        logger.info("Ready file removed: %s", ready_file)
    except Exception:
        pass

    logger.info("Shutdown complete — all resources released.")


async def polymarket_refresh() -> None:
    """Recurring job: fetch and match Polymarket prediction market data."""
    logger.info("=== polymarket_refresh cycle START ===")
    try:
        async def _polymarket_refresh_impl():
            from .polymarket import update_polymarket_matches
            count = await asyncio.to_thread(update_polymarket_matches)
            logger.info("Polymarket refresh: %d matches updated.", count)
        await asyncio.wait_for(_polymarket_refresh_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("polymarket_refresh TIMED OUT after 180s")
    except Exception:
        logger.exception("polymarket_refresh cycle FAILED.")
    finally:
        logger.info("=== polymarket_refresh cycle END ===")


async def twitter_fetch_and_process() -> None:
    """Recurring job: fetch tweets from OSINT accounts and process them."""
    logger.info("=== twitter_fetch_and_process cycle START ===")
    try:
        async def _twitter_fetch_impl():
            from .twitter_fetcher import TwitterFetcher

            fetcher = TwitterFetcher()
            if not fetcher.is_configured:
                logger.info("X: Twitter fetcher not configured — skipping.")
                return
            new_articles = await asyncio.to_thread(fetcher.fetch_and_save)

            if not new_articles:
                logger.info("X: no new tweets this cycle.")
                return

            logger.info("X: fetched %d new tweet-articles.", len(new_articles))

            # Run through the same pipeline as RSS articles (in thread to avoid blocking event loop)
            from .nlp_pipeline import process_batch

            briefs = await asyncio.to_thread(process_batch, new_articles)
            logger.info("X: produced %d briefs.", len(briefs))

            if briefs:
                from .narrative_tracker import update_narratives, detect_runups

                updated = await asyncio.to_thread(update_narratives, briefs)
                changed = [t.narrative_name for t in updated]
                await asyncio.to_thread(detect_runups, changed_narratives=changed)

                from .probability_engine import update_probabilities

                await asyncio.to_thread(update_probabilities, briefs)

        await asyncio.wait_for(_twitter_fetch_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("twitter_fetch_and_process TIMED OUT after 180s")
    except Exception:
        logger.exception("twitter_fetch_and_process cycle FAILED.")
    finally:
        logger.info("=== twitter_fetch_and_process cycle END ===")


async def gdelt_fetch_and_process() -> None:
    """Recurring job: fetch articles from GDELT global events database."""
    logger.info("=== gdelt_fetch_and_process cycle START ===")
    try:
        async def _gdelt_fetch_impl():
            from .gdelt_fetcher import GdeltFetcher

            fetcher = GdeltFetcher()
            new_articles = await asyncio.to_thread(fetcher.fetch_and_save)

            if not new_articles:
                logger.info("GDELT: no new articles this cycle.")
                return

            logger.info("GDELT: fetched %d new articles.", len(new_articles))

            # Run through the same NLP pipeline as RSS articles (in thread to avoid blocking event loop)
            from .nlp_pipeline import process_batch

            briefs = await asyncio.to_thread(process_batch, new_articles)
            logger.info("GDELT: produced %d briefs.", len(briefs))

            if briefs:
                from .narrative_tracker import update_narratives, detect_runups

                updated = await asyncio.to_thread(update_narratives, briefs)
                changed = [t.narrative_name for t in updated]
                await asyncio.to_thread(detect_runups, changed_narratives=changed)

                from .probability_engine import update_probabilities

                await asyncio.to_thread(update_probabilities, briefs)

        await asyncio.wait_for(_gdelt_fetch_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("gdelt_fetch_and_process TIMED OUT after 180s")
    except Exception:
        logger.exception("gdelt_fetch_and_process cycle FAILED.")
    finally:
        logger.info("=== gdelt_fetch_and_process cycle END ===")


async def proximity_update() -> None:
    """Recurring job: update proximity percentages for consequence price thresholds."""
    logger.info("=== proximity_update cycle START ===")
    try:
        async def _proximity_update_impl():
            import json as _json
            from .db import Consequence
            from .price_fetcher import get_price_fetcher

            session = get_session()
            try:
                fetcher = get_price_fetcher()

                # Find consequences with price thresholds
                consequences = (
                    session.query(Consequence)
                    .filter(Consequence.price_thresholds_json.isnot(None))
                    .filter(Consequence.status == "predicted")
                    .all()
                )

                updated = 0
                for cons in consequences:
                    try:
                        thresholds = _json.loads(cons.price_thresholds_json or "[]")
                        if not thresholds:
                            continue

                        for th in thresholds:
                            asset = th.get("asset", "")
                            direction = th.get("direction", "above")
                            target = float(th.get("value", 0))
                            if not asset or target <= 0:
                                continue

                            # Fetch current price
                            quote = await asyncio.to_thread(fetcher.get_quote, asset)
                            if "error" in quote:
                                continue

                            current = quote["price"]
                            if direction == "above":
                                pct = min(100.0, max(0, (current / target) * 100))
                            else:  # "below"
                                pct = min(100.0, max(0, (target / current) * 100)) if current > 0 else 0

                            cons.proximity_pct = round(pct, 1)
                            updated += 1
                            break  # Use first threshold

                    except Exception:
                        continue

                if updated:
                    session.commit()
                    logger.info("Proximity: updated %d consequences.", updated)
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        await asyncio.wait_for(_proximity_update_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("proximity_update TIMED OUT after 180s")
    except Exception:
        logger.exception("proximity_update cycle FAILED.")
    finally:
        logger.info("=== proximity_update cycle END ===")


async def store_price_snapshots() -> None:
    """Recurring job: snapshot prices for tickers in active StockImpacts + key macro assets.

    Runs every 4 hours. Enables news→price correlation by storing historical data.
    Cleans up snapshots older than 30 days.
    """
    logger.info("=== store_price_snapshots cycle START ===")
    try:
        async def _store_price_snapshots_impl():
            from .db import (
                get_session, RunUp, DecisionNode, Consequence, StockImpact,
                PriceSnapshot, cleanup_old_price_snapshots,
            )
            from .price_fetcher import get_price_fetcher

            session = get_session()
            try:
                fetcher = get_price_fetcher()

                # Collect unique tickers from active StockImpacts
                active_tickers: set = set()
                active_runups = (
                    session.query(RunUp)
                    .filter(RunUp.status == "active", RunUp.merged_into_id.is_(None))
                    .all()
                )
                for ru in active_runups:
                    for node in ru.decision_nodes:
                        if node.status != "open":
                            continue
                        for cons in node.consequences:
                            for si in cons.stock_impacts:
                                active_tickers.add(si.ticker.upper())

                # Always track key macro tickers
                always_track = {"GC=F", "SI=F", "HG=F", "CL=F", "^VIX", "ITA", "XLE", "SPY"}
                active_tickers.update(always_track)

                logger.info("Price snapshots: tracking %d tickers", len(active_tickers))

                stored = 0
                for ticker in active_tickers:
                    try:
                        quote = await asyncio.to_thread(fetcher.get_quote, ticker)
                        if "error" in quote:
                            continue

                        snapshot = PriceSnapshot(
                            ticker=ticker,
                            price=quote["price"],
                            recorded_at=datetime.utcnow(),
                        )
                        session.add(snapshot)
                        stored += 1
                    except Exception:
                        continue

                if stored:
                    session.commit()
                    logger.info("Price snapshots: stored %d snapshots.", stored)

                # Cleanup old snapshots (>30 days)
                cleaned = cleanup_old_price_snapshots(max_age_days=30)
                if cleaned:
                    logger.info("Price snapshots: cleaned %d old records.", cleaned)

                # Cleanup old articles + briefs (>90 days)
                from .db import Article, ArticleBrief
                cutoff = datetime.utcnow() - timedelta(days=90)
                old_briefs = session.query(ArticleBrief).filter(
                    ArticleBrief.processed_at < cutoff
                ).delete(synchronize_session=False)
                old_articles = session.query(Article).filter(
                    Article.pub_date < cutoff
                ).delete(synchronize_session=False)
                if old_articles or old_briefs:
                    session.commit()
                    logger.info(
                        "Article retention: cleaned %d articles + %d briefs older than 90 days.",
                        old_articles, old_briefs,
                    )
            finally:
                session.close()

        await asyncio.wait_for(_store_price_snapshots_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("store_price_snapshots TIMED OUT after 180s")
    except Exception:
        logger.exception("store_price_snapshots cycle FAILED.")
    finally:
        logger.info("=== store_price_snapshots cycle END ===")


async def cleanup_old_data() -> None:
    """Daily cleanup job: delete old articles (>90 days), old price snapshots (>30 days), and vacuum the database."""
    logger.info("=== cleanup_old_data cycle START ===")
    try:
        from .db import (
            get_session, Article, ArticleBrief, cleanup_old_price_snapshots,
        )
        from sqlalchemy import text as _sa_text

        session = get_session()
        try:
            # Delete articles and briefs older than 90 days
            cutoff_90 = datetime.utcnow() - timedelta(days=90)
            old_briefs = session.query(ArticleBrief).filter(
                ArticleBrief.processed_at < cutoff_90
            ).delete(synchronize_session=False)
            old_articles = session.query(Article).filter(
                Article.pub_date < cutoff_90
            ).delete(synchronize_session=False)
            if old_articles or old_briefs:
                session.commit()
                logger.info(
                    "cleanup_old_data: removed %d articles + %d briefs older than 90 days.",
                    old_articles, old_briefs,
                )

            # Delete price snapshots older than 30 days
            cleaned = cleanup_old_price_snapshots(max_age_days=30)
            if cleaned:
                logger.info("cleanup_old_data: removed %d old price snapshots.", cleaned)

            # Vacuum the database
            session.execute(_sa_text("VACUUM"))
            logger.info("cleanup_old_data: database vacuumed.")
        finally:
            session.close()
    except Exception:
        logger.exception("cleanup_old_data cycle FAILED.")
    finally:
        logger.info("=== cleanup_old_data cycle END ===")


async def store_focused_price_snapshots() -> None:
    """Recurring job: hourly price snapshots for tickers in focused narratives only.

    Focused narratives get 4x more frequent price data (hourly vs 4-hourly)
    for better news-price correlation analysis.
    """
    from .focus_manager import get_focused_runup_ids

    focused_ids = get_focused_runup_ids()
    if not focused_ids:
        return  # No focused narratives — skip silently

    logger.info("=== store_focused_price_snapshots cycle START ===")
    try:
        async def _store_focused_impl():
            from .db import (
                get_session, RunUp, DecisionNode, Consequence, StockImpact,
                PriceSnapshot,
            )
            from .price_fetcher import get_price_fetcher

            session = get_session()
            try:
                fetcher = get_price_fetcher()

                # Collect tickers from focused run-ups' StockImpacts
                focus_tickers: set = set()
                for ru_id in focused_ids:
                    ru = session.query(RunUp).get(ru_id)
                    if not ru:
                        continue
                    for node in ru.decision_nodes:
                        if node.status != "open":
                            continue
                        for cons in node.consequences:
                            for si in cons.stock_impacts:
                                focus_tickers.add(si.ticker.upper())

                if not focus_tickers:
                    logger.info("Focused price snapshots: no tickers to track.")
                    return

                logger.info("Focused price snapshots: tracking %d tickers", len(focus_tickers))

                stored = 0
                for ticker in focus_tickers:
                    try:
                        quote = await asyncio.to_thread(fetcher.get_quote, ticker)
                        if "error" in quote:
                            continue
                        snapshot = PriceSnapshot(
                            ticker=ticker,
                            price=quote["price"],
                            recorded_at=datetime.utcnow(),
                        )
                        session.add(snapshot)
                        stored += 1
                    except Exception:
                        continue

                if stored:
                    session.commit()
                    logger.info("Focused price snapshots: stored %d snapshots.", stored)
            finally:
                session.close()

        await asyncio.wait_for(_store_focused_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("store_focused_price_snapshots TIMED OUT after 180s")
    except Exception:
        logger.exception("store_focused_price_snapshots cycle FAILED.")
    finally:
        logger.info("=== store_focused_price_snapshots cycle END ===")


async def confidence_scoring() -> None:
    """Recurring job: score active run-ups and generate trading signals."""
    logger.info("=== confidence_scoring cycle START ===")
    try:
        async def _confidence_scoring_impl():
            from .confidence_scorer import update_trading_signals

            signals = await asyncio.to_thread(update_trading_signals)
            buy_plus = sum(1 for s in signals if s.signal_level in ("BUY", "STRONG_BUY"))
            logger.info(
                "Confidence scoring: %d signals (%d BUY+).", len(signals), buy_plus
            )
        await asyncio.wait_for(_confidence_scoring_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("confidence_scoring TIMED OUT after 180s")
    except Exception:
        logger.exception("confidence_scoring cycle FAILED.")
    finally:
        logger.info("=== confidence_scoring cycle END ===")


async def ml_retrain_job() -> None:
    """Recurring job: retrain ML signal predictor on latest data."""
    logger.info("=== ml_retrain cycle START ===")
    try:
        async def _ml_retrain_impl():
            from .ml.prepare import extract_all
            from .ml.train import train

            # Extract fresh features from DB
            features, labels = extract_all()
            if features.empty:
                logger.warning("ML retrain skipped — no features available.")
                return

            # Train model
            metadata = train()
            if metadata:
                metrics = metadata.get("metrics", {})
                logger.info(
                    "ML retrain complete: sharpe=%.4f, hit_rate=%.4f, n_samples=%d",
                    metrics.get("sharpe_ratio", 0),
                    metrics.get("hit_rate", 0),
                    metadata.get("n_samples", 0),
                )
            else:
                logger.warning("ML retrain produced no model (insufficient data?).")
        await asyncio.wait_for(_ml_retrain_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("ml_retrain TIMED OUT after 180s")
    except Exception:
        logger.exception("ml_retrain cycle FAILED.")
    finally:
        logger.info("=== ml_retrain cycle END ===")


async def swarm_consensus_job() -> None:
    """Recurring job: run expert swarm debate on decision nodes (Groq/Llama)."""
    logger.info("=== swarm_consensus cycle START ===")
    try:
        async def _swarm_consensus_impl():
            from .swarm_consensus import swarm_consensus_cycle

            count = await swarm_consensus_cycle()
            logger.info("Swarm consensus: %d nodes evaluated.", count)

            # Send Telegram notification with latest verdicts
            if count and count > 0:
                try:
                    from .telegram_notifier import send_swarm_notification
                    from .db import SwarmVerdict
                    from datetime import timedelta
                    db_sw = get_session()
                    try:
                        cutoff = datetime.utcnow() - timedelta(hours=2)
                        recent_verdicts = (
                            db_sw.query(SwarmVerdict)
                            .filter(
                                SwarmVerdict.created_at >= cutoff,
                                SwarmVerdict.superseded_at.is_(None),
                            )
                            .order_by(SwarmVerdict.confidence.desc())
                            .limit(10)
                            .all()
                        )
                        if recent_verdicts:
                            logger.info("Attempting to send swarm results via Telegram (%d verdicts)...", len(recent_verdicts))
                            sent = await asyncio.to_thread(send_swarm_notification, recent_verdicts)
                            logger.info("Telegram swarm send result: %s", sent)
                    finally:
                        db_sw.close()
                except Exception:
                    logger.warning("Telegram swarm notification failed.", exc_info=True)

            # After successful swarm, check if Arabella should notify (big news only)
            if count and count > 0:
                try:
                    from .arabella import notify_if_big_news_swarm
                    await asyncio.to_thread(notify_if_big_news_swarm, recent_verdicts if recent_verdicts else [])
                except Exception:
                    logger.warning("Arabella swarm notification check failed.", exc_info=True)

                # Also trigger advisory refresh
                try:
                    from .daily_advisory import generate_refresh_advisory
                    report = await asyncio.to_thread(generate_refresh_advisory)
                    if report:
                        logger.info("Advisory refreshed after swarm consensus: report %d", report.id)
                    else:
                        logger.info("Advisory refresh after swarm: no significant changes.")
                except Exception as e:
                    logger.warning("Advisory refresh after swarm failed: %s", e)
        await asyncio.wait_for(_swarm_consensus_impl(), timeout=600)  # 10 min
    except asyncio.TimeoutError:
        logger.error("swarm_consensus TIMED OUT after 600s")
    except Exception:
        logger.exception("swarm_consensus cycle FAILED.")
    finally:
        logger.info("=== swarm_consensus cycle END ===")


async def tree_generation_job() -> None:
    """Recurring job: generate decision trees for active run-ups missing trees."""
    logger.info("=== tree_generation cycle START ===")
    try:
        async def _tree_generation_impl():
            from .tree_generator import generate_trees_for_new_runups

            results = await asyncio.to_thread(generate_trees_for_new_runups)
            created = sum(1 for r in results if r.get("status") == "created")
            logger.info("Tree generation: %d trees created out of %d candidates.", created, len(results))
        await asyncio.wait_for(_tree_generation_impl(), timeout=600)  # 10 min
    except asyncio.TimeoutError:
        logger.error("tree_generation TIMED OUT after 600s")
    except Exception:
        logger.exception("tree_generation cycle FAILED.")
    finally:
        logger.info("=== tree_generation cycle END ===")


async def proactive_children_job() -> None:
    """Recurring job: generate depth-1 child trees for strong-signal root nodes."""
    logger.info("=== proactive_children cycle START ===")
    try:
        async def _proactive_children_impl():
            from .tree_generator import generate_proactive_children

            results = await asyncio.to_thread(generate_proactive_children)
            logger.info("Proactive children: %d child trees generated.", len(results))
        await asyncio.wait_for(_proactive_children_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("proactive_children TIMED OUT after 180s")
    except Exception:
        logger.exception("proactive_children cycle FAILED.")
    finally:
        logger.info("=== proactive_children cycle END ===")


async def daily_advisory_job() -> None:
    """Recurring job: generate daily investment advisory (07:30 UTC)."""
    logger.info("=== daily_advisory cycle START ===")
    try:
        async def _daily_advisory_impl():
            from .daily_advisory import generate_daily_advisory
            report = await asyncio.to_thread(generate_daily_advisory)
            if report:
                logger.info("Daily advisory generated: report %d", report.id)
                # Send Telegram notification
                try:
                    import json
                    from .telegram_notifier import send_advisory_notification
                    logger.info("Attempting to send advisory via Telegram...")
                    data = json.loads(report.report_json) if report.report_json else {}
                    sent = await asyncio.to_thread(send_advisory_notification, data)
                    logger.info("Telegram advisory send result: %s", sent)
                except Exception:
                    logger.warning("Telegram advisory notification failed.", exc_info=True)
            else:
                logger.warning("Daily advisory returned None — check logs.")
        await asyncio.wait_for(_daily_advisory_impl(), timeout=600)  # 10 min
    except asyncio.TimeoutError:
        logger.error("daily_advisory TIMED OUT after 600s")
    except Exception:
        logger.exception("daily_advisory cycle FAILED.")
    finally:
        logger.info("=== daily_advisory cycle END ===")


async def advisory_evaluation_job() -> None:
    """Recurring job: evaluate past advisories at all horizons (07:25 UTC)."""
    logger.info("=== advisory_evaluation cycle START ===")
    try:
        async def _advisory_evaluation_impl():
            from .daily_advisory import evaluate_open_advisories, calculate_brier_scores
            result = await asyncio.to_thread(evaluate_open_advisories)
            logger.info(
                "Advisory evaluation: %d checks, %.1f%% accuracy",
                result.get("total_checks", 0),
                result.get("accuracy", 0),
            )
            # Also update Brier scores
            await asyncio.to_thread(calculate_brier_scores)
        await asyncio.wait_for(_advisory_evaluation_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("advisory_evaluation TIMED OUT after 180s")
    except Exception:
        logger.exception("advisory_evaluation cycle FAILED.")
    finally:
        logger.info("=== advisory_evaluation cycle END ===")


async def advisory_rebalance_job() -> None:
    """Recurring job: weekly weight rebalancing (Sunday 07:35 UTC)."""
    logger.info("=== advisory_rebalance cycle START ===")
    try:
        async def _advisory_rebalance_impl():
            from .daily_advisory import rebalance_weights
            new_weights = await asyncio.to_thread(rebalance_weights)
            logger.info("Advisory weights rebalanced: %s", new_weights)
        await asyncio.wait_for(_advisory_rebalance_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("advisory_rebalance TIMED OUT after 180s")
    except Exception:
        logger.exception("advisory_rebalance cycle FAILED.")
    finally:
        logger.info("=== advisory_rebalance cycle END ===")


# ---------------------------------------------------------------------------
# Flash alert & advisory refresh jobs
# ---------------------------------------------------------------------------

async def flash_evaluation_job() -> None:
    """Recurring job: evaluate flash alert predictions (hourly at :15)."""
    logger.info("=== flash_evaluation cycle START ===")
    try:
        async def _flash_evaluation_impl():
            from .daily_advisory import evaluate_flash_alerts
            result = await asyncio.to_thread(evaluate_flash_alerts)
            logger.info("Flash evaluation: %d new evals", result.get("new_evals", 0))
        await asyncio.wait_for(_flash_evaluation_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("flash_evaluation TIMED OUT after 180s")
    except Exception:
        logger.exception("flash_evaluation cycle FAILED.")
    finally:
        logger.info("=== flash_evaluation cycle END ===")


async def flash_expiry_job() -> None:
    """Recurring job: expire old flash alerts (hourly at :45)."""
    logger.info("=== flash_expiry cycle START ===")
    try:
        async def _flash_expiry_impl():
            from .flash_detector import expire_old_alerts
            from .db import get_session as _get_exp_session
            _edb = _get_exp_session()
            try:
                count = await asyncio.to_thread(expire_old_alerts, _edb)
                if count:
                    logger.info("Expired %d flash alerts", count)
            finally:
                _edb.close()
        await asyncio.wait_for(_flash_expiry_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("flash_expiry TIMED OUT after 180s")
    except Exception:
        logger.exception("flash_expiry cycle FAILED.")
    finally:
        logger.info("=== flash_expiry cycle END ===")


async def flash_rebalance_job() -> None:
    """Recurring job: weekly flash weight rebalancing (Sunday 07:40 UTC)."""
    logger.info("=== flash_rebalance cycle START ===")
    try:
        async def _flash_rebalance_impl():
            from .daily_advisory import rebalance_flash_weights
            new_weights = await asyncio.to_thread(rebalance_flash_weights)
            logger.info("Flash weights rebalanced: %s", new_weights)
        await asyncio.wait_for(_flash_rebalance_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("flash_rebalance TIMED OUT after 180s")
    except Exception:
        logger.exception("flash_rebalance cycle FAILED.")
    finally:
        logger.info("=== flash_rebalance cycle END ===")


async def advisory_refresh_job() -> None:
    """Recurring job: lightweight advisory refresh (10:00, 14:30, 18:00 UTC)."""
    if not config.advisory_refresh_enabled:
        return
    logger.info("=== advisory_refresh cycle START ===")
    try:
        async def _advisory_refresh_impl():
            from .daily_advisory import generate_refresh_advisory
            report = await asyncio.to_thread(generate_refresh_advisory)
            if report:
                logger.info("Advisory refresh generated: report %d", report.id)
            else:
                logger.info("Advisory refresh: no significant changes.")
        await asyncio.wait_for(_advisory_refresh_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("advisory_refresh TIMED OUT after 180s")
    except Exception:
        logger.exception("advisory_refresh cycle FAILED.")
    finally:
        logger.info("=== advisory_refresh cycle END ===")


async def batch_send_suppressed_job() -> None:
    """Recurring job: batch send quiet-hours suppressed alerts (06:05 UTC)."""
    logger.info("=== batch_send_suppressed cycle START ===")
    try:
        async def _batch_send_impl():
            from .telegram_notifier import batch_send_suppressed
            count = await asyncio.to_thread(batch_send_suppressed)
            if count:
                logger.info("Batch sent %d suppressed notifications", count)
        await asyncio.wait_for(_batch_send_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("batch_send_suppressed TIMED OUT after 180s")
    except Exception:
        logger.exception("batch_send_suppressed cycle FAILED.")
    finally:
        logger.info("=== batch_send_suppressed cycle END ===")


async def daily_briefing_job() -> None:
    """Generate the daily self-evaluation briefing (briefings/YYYY-MM-DD.md)."""
    logger.info("=== daily_briefing START ===")
    try:
        from .daily_briefing_generator import generate_daily_briefing
        filepath = await asyncio.to_thread(generate_daily_briefing)
        logger.info("Daily briefing generated: %s", filepath)
    except Exception:
        logger.exception("daily_briefing FAILED.")
    finally:
        logger.info("=== daily_briefing END ===")


async def morning_briefing_job() -> None:
    """Send the morning briefing via Telegram — zero LLM tokens."""
    logger.info("=== morning_briefing START ===")
    try:
        from .telegram_notifier import send_morning_briefing
        sent = await asyncio.to_thread(send_morning_briefing)
        logger.info("Morning briefing sent: %s", sent)
    except Exception:
        logger.exception("morning_briefing FAILED.")
    finally:
        logger.info("=== morning_briefing END ===")


async def heartbeat_check_job() -> None:
    """Deterministic heartbeat — check gold/portfolio thresholds, alert only if breached."""
    logger.info("=== heartbeat_check START ===")
    try:
        async def _heartbeat_impl():
            import httpx
            try:
                resp = httpx.get(f"http://127.0.0.1:{config.engine_port}/api/heartbeat", timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "ALERT":
                        from .telegram_notifier import send_message
                        alert_lines = ["⚡ <b>Heartbeat Alert</b>", ""]
                        for a in data.get("alerts", []):
                            alert_lines.append(f"• {a['message']}")
                        await asyncio.to_thread(send_message, "\n".join(alert_lines))
                        logger.info("Heartbeat alert sent (%d alerts)", len(data.get("alerts", [])))
                    else:
                        logger.info("Heartbeat OK — no alerts")
                else:
                    logger.warning("Heartbeat endpoint returned %s", resp.status_code)
            except Exception as e:
                logger.warning("Heartbeat check failed: %s", e)

        await asyncio.wait_for(_heartbeat_impl(), timeout=30)
    except asyncio.TimeoutError:
        logger.error("heartbeat_check TIMED OUT")
    except Exception:
        logger.exception("heartbeat_check FAILED.")
    finally:
        logger.info("=== heartbeat_check END ===")


async def pipeline_health_job() -> None:
    """Periodic pipeline health check — zero LLM tokens."""
    logger.info("=== pipeline_health START ===")
    try:
        from .pipeline_health import check_pipeline_health
        from .telegram_notifier import send_message

        health = await asyncio.to_thread(check_pipeline_health)

        # Alert via Telegram if degraded
        if health.get("alert_count", 0) > 2:
            lines = ["\u26a0\ufe0f <b>Pipeline Health Alert</b>", ""]
            for alert in health.get("alerts", []):
                lines.append(f"\u2022 {alert}")
            lines.append("")
            lines.append(f"Signals w/ ticker: {health.get('signal_ticker_rate_pct', 0)}%")
            lines.append(f"Advisory recs: {health.get('advisory_recommendations', 0)}")
            await asyncio.to_thread(send_message, "\n".join(lines))

        logger.info("Pipeline health: %s (%d alerts)", health.get("status"), health.get("alert_count", 0))
    except Exception:
        logger.exception("pipeline_health FAILED.")
    finally:
        logger.info("=== pipeline_health END ===")


async def watchdog_health_check() -> None:
    """Recurring job: run Watchdog AI health check on all pipeline elements (2x daily)."""
    logger.info("=== watchdog_health_check cycle START ===")
    try:
        async def _watchdog_impl():
            from .watchdog import run_watchdog
            await asyncio.to_thread(run_watchdog)
        await asyncio.wait_for(_watchdog_impl(), timeout=180)  # 3 min
    except asyncio.TimeoutError:
        logger.error("watchdog_health_check TIMED OUT after 180s")
    except Exception:
        logger.exception("watchdog_health_check cycle FAILED.")
    finally:
        logger.info("=== watchdog_health_check cycle END ===")


async def _generate_pending_child_trees_async() -> None:
    """One-time startup task: generate child trees for confirmed nodes missing children."""
    await asyncio.sleep(15)  # let other startup tasks settle
    logger.info("=== pending_child_trees check START ===")
    try:
        from .db import DecisionNode

        session = get_session()
        try:
            confirmed = (
                session.query(DecisionNode)
                .filter(
                    DecisionNode.status.in_(["confirmed_yes", "confirmed_no"]),
                    DecisionNode.depth < 4,
                )
                .all()
            )

            pending = []
            for node in confirmed:
                has_children = (
                    session.query(DecisionNode)
                    .filter(DecisionNode.parent_node_id == node.id)
                    .first()
                )
                if not has_children:
                    pending.append(node.id)
        finally:
            session.close()

        if not pending:
            logger.info("No confirmed nodes missing child trees.")
            return

        logger.info(
            "Found %d confirmed nodes without child trees — generating...",
            len(pending),
        )

        from .tree_generator import generate_child_tree

        for node_id in pending:
            try:
                result = await asyncio.to_thread(generate_child_tree, node_id)
                if result:
                    logger.info(
                        "Generated child tree for node %d: %s",
                        node_id, result.get("status", "?"),
                    )
            except Exception:
                logger.exception("Failed to generate child tree for node %d.", node_id)

    except Exception:
        logger.exception("pending_child_trees check FAILED.")
    finally:
        logger.info("=== pending_child_trees check END ===")


async def _initial_fetch() -> None:
    """Run a single fetch-and-process cycle shortly after startup."""
    await asyncio.sleep(5)  # small delay to let the app fully start
    logger.info("Running initial fetch_and_process cycle...")
    await fetch_and_process()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="OpenClaw News Analyzer Engine",
    description=(
        "Real-time geopolitical news analysis engine with RSS ingestion, "
        "NLP enrichment, narrative tracking, Bayesian probability updates, "
        "and decision-tree management."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS -- restrict to configured origins in production; same-origin only if empty.
_cors_origins = config.cors_allowed_origins
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
        max_age=600,
    )
else:
    # No CORS middleware = only same-origin requests allowed (secure default).
    logger.warning(
        "CORS_ALLOWED_ORIGINS not set -- only same-origin requests allowed. "
        "Set CORS_ALLOWED_ORIGINS in .env for cross-origin access."
    )

# Rate limiting (slowapi)
from .rate_limit import limiter, rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# Security headers (CSP, HSTS, X-Frame-Options, etc.)
from .security_headers import SecurityHeadersMiddleware

app.add_middleware(SecurityHeadersMiddleware)

# Session authentication middleware (protects all routes except /auth/*, /login)
from .session_middleware import SessionAuthMiddleware

app.add_middleware(SessionAuthMiddleware)

# Include the API router (all routes are under /api)
app.include_router(api_router)

# Include auth routes (login, signup, logout — public)
from .auth_routes import router as auth_router

app.include_router(auth_router)

# ---------------------------------------------------------------------------
# Standalone UI serving (when not behind OpenClaw gateway)
# Translates gateway-style ?_api=X queries to internal engine routes, and
# serves a bundled HTML dashboard at the root URL.
# ---------------------------------------------------------------------------
_UI_DIR = Path(__file__).resolve().parent.parent / "ui"

# ── API dispatch map: _api value → (method, engine_path_template) ────
# Mirrors the TypeScript http-handler.ts routing table.
_API_DISPATCH: dict[str, str] = {
    "overview": "/api/dashboard/overview",
    "status": "/api/status",
    "signals": "/api/signals",
    "signals-history": "/api/signals/history",
    "signals-refresh": "/api/signals/refresh",
    "indicators": "/api/indicators",
    "analysis": "/api/analysis/latest",
    "analysis-run": "/api/analysis/run",
    "advisory": "/api/advisory/latest",
    "advisory-generate": "/api/advisory/generate",
    "advisory-history": "/api/advisory/history",
    "usage-breakdown": "/api/usage/breakdown",
    "opportunities": "/api/dashboard/opportunities",
    "feeds": "/api/feeds",
    "budget": "/api/budget",
    "apikey": "/api/settings/api-key",
    "swarm-status": "/api/swarm/status",
    "swarm-cycle": "/api/swarm/run-cycle",
    "polymarket-refresh": "/api/polymarket/refresh",
    "prediction-score": "/api/predictions/score",
    "telegram-status": "/api/telegram/status",
    "telegram-configure": "/api/telegram/configure",
    "telegram-test": "/api/telegram/test",
    "telegram-send-advisory": "/api/telegram/send-advisory",
    "advisory-feedback": "/api/advisory/feedback",
    "portfolio-holdings": "/api/portfolio/holdings",
    "portfolio-holdings-upsert": "/api/portfolio/holdings/upsert",
    "portfolio-search": "/api/portfolio/search",
    "portfolio-size": "/api/portfolio/size",
    "portfolio-alignment": "/api/portfolio/alignment",
    "focus": "/api/focus",
    "focus-polymarket-link": "/api/focus/polymarket-link",
    # Flash alerts (V2)
    "flash-alerts": "/api/flash/alerts",
    # ML endpoints (V2)
    "ml-status": "/api/ml/status",
    "ml-predictions": "/api/ml/predictions",
    "ml-retrain": "/api/ml/retrain",
    "morning-digest": "/api/morning-digest",
    "audit-log": "/api/audit/log",
    "ml-experiments": "/api/ml/experiments",
    "admin-users": "/api/admin/users",
    "swarm-feed": "/api/swarm/feed",
    "swarm-status": "/api/swarm/status",
    "swarm-verdicts": "/api/swarm/verdicts",
    "swarm-experts": "/api/swarm/experts",
    "briefs": "/api/briefs",
}
# Parameterised routes: _api value → (query_param, path_template)
_API_PARAM_DISPATCH: dict[str, tuple[str, str]] = {
    "tree": ("id", "/api/dashboard/tree/{val}"),
    "polymarket": ("id", "/api/polymarket/{val}"),
    "swarm-verdict": ("nodeId", "/api/swarm/verdict/{val}"),
    "swarm-verdicts": ("runUpId", "/api/swarm/verdicts/{val}"),
    "price": ("ticker", "/api/price/{val}"),
    "price-chart": ("ticker", "/api/price/{val}/chart"),
    "focus-regenerate-tree": ("id", "/api/focus/regenerate-tree/{val}"),
    "swarm-expert-toggle": ("id", "/api/swarm/experts/{val}/toggle"),
}

if _UI_DIR.is_dir():
    from starlette.responses import HTMLResponse, RedirectResponse
    from starlette.requests import Request as StarletteRequest
    import re as _re

    _ui_cache: dict = {"html": None, "mtime": 0.0}

    def _build_bundle() -> str:
        """Build a self-contained HTML bundle like the gateway does."""
        css_path = _UI_DIR / "app.css"
        js_path = _UI_DIR / "app.js"
        css = css_path.read_text("utf-8") if css_path.exists() else ""
        js = js_path.read_text("utf-8") if js_path.exists() else ""
        # Rewrite API_BASE to use the same path (gateway compat)
        # API_BASE points to root "/" — the gateway compat handler works on both / and /plugins/...
        js = _re.sub(
            r'const API_BASE\s*=\s*["\'][^"\']*["\']',
            'const API_BASE = ""',
            js,
        )
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>W25 — Intelligence</title>
  <style>{css}</style>
  <script src="https://unpkg.com/cytoscape@3.28.1/dist/cytoscape.min.js"></script>
  <script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"></script>
  <script src="https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
  <script src="https://unpkg.com/lightweight-charts@4/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body>
  <div id="app"></div>
  <script type="module">{js}</script>
</body>
</html>"""

    def _get_bundle() -> str:
        js_path = _UI_DIR / "app.js"
        mtime = js_path.stat().st_mtime if js_path.exists() else 0
        if _ui_cache["html"] is None or mtime > _ui_cache["mtime"]:
            _ui_cache["html"] = _build_bundle()
            _ui_cache["mtime"] = mtime
        return _ui_cache["html"]

    @app.get("/plugins/openclaw-news-analyzer", include_in_schema=False)
    @app.post("/plugins/openclaw-news-analyzer", include_in_schema=False)
    @app.put("/plugins/openclaw-news-analyzer", include_in_schema=False)
    @app.delete("/plugins/openclaw-news-analyzer", include_in_schema=False)
    async def _gateway_compat(request: StarletteRequest):
        """Handle gateway-style _api queries and serve the bundled dashboard.

        Instead of 307 redirects (which cause browsers to drop auth headers
        and can fail when accessed via SSH tunnels), we internally proxy the
        request to the target API route using ASGI scope rewriting.
        """
        import httpx
        api_action = request.query_params.get("_api")

        if not api_action:
            # No _api param → serve the dashboard HTML
            return HTMLResponse(_get_bundle())

        # Resolve the target path
        target = None
        if api_action in _API_PARAM_DISPATCH:
            qp, tpl = _API_PARAM_DISPATCH[api_action]
            val = request.query_params.get(qp, "")
            target = tpl.replace("{val}", val)
            extra = {k: v for k, v in request.query_params.items() if k not in ("_api", qp)}
            if extra:
                qs = "&".join(f"{k}={v}" for k, v in extra.items())
                target = f"{target}?{qs}"
        elif api_action in _API_DISPATCH:
            target = _API_DISPATCH[api_action]
            extra = {k: v for k, v in request.query_params.items() if k != "_api"}
            if extra:
                qs = "&".join(f"{k}={v}" for k, v in extra.items())
                target = f"{target}?{qs}"

        if not target:
            return HTMLResponse(
                f'{{"error": "Unknown _api action: {api_action}"}}',
                status_code=404,
                media_type="application/json",
            )

        # Internal proxy: forward request to the engine's own API
        port = config.engine_port
        method = request.method
        url = f"http://127.0.0.1:{port}{target}"
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in ("host", "content-length")}
        body = await request.body() if method in ("POST", "PUT", "DELETE") else None

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.request(method, url, headers=headers, content=body)
                from starlette.responses import Response
                return Response(
                    content=resp.content,
                    status_code=resp.status_code,
                    headers=dict(resp.headers),
                    media_type=resp.headers.get("content-type"),
                )
        except Exception as exc:
            logger.error("Internal proxy failed for %s: %s", target, exc)
            return HTMLResponse(
                f'{{"error": "Internal proxy failed: {exc}"}}',
                status_code=502,
                media_type="application/json",
            )

    @app.get("/", include_in_schema=False)
    @app.post("/", include_in_schema=False)
    @app.put("/", include_in_schema=False)
    @app.delete("/", include_in_schema=False)
    async def _root_dashboard(request: StarletteRequest):
        """Serve dashboard at root URL — same handler as /plugins/openclaw-news-analyzer."""
        return await _gateway_compat(request)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the engine with uvicorn."""
    import uvicorn

    config.configure_logging()
    uvicorn.run(
        "engine.engine:app",
        host=config.host,
        port=config.engine_port,
        log_level=config.log_level.lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()
