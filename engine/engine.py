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
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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


async def fetch_and_process() -> None:
    """Recurring job: fetch RSS feeds, run NLP, update narratives & probs.

    This is the engine's heartbeat.  It is deliberately written as a
    standalone coroutine so that APScheduler can invoke it.
    """
    logger.info("=== fetch_and_process cycle START ===")

    try:
        # --- 1. Fetch ---
        from .fetcher import RSSFetcher

        async with RSSFetcher() as fetcher:
            new_articles = await fetcher.fetch_and_save()

        if not new_articles:
            logger.info("No new articles this cycle -- skipping NLP & analysis.")
            return

        logger.info("Fetched %d new articles.", len(new_articles))

        # --- 2. NLP pipeline ---
        from .nlp_pipeline import process_batch

        briefs = process_batch(new_articles)
        logger.info("Produced %d article briefs.", len(briefs))

        if not briefs:
            return

        # --- 3. Narrative tracking ---
        from .narrative_tracker import update_narratives, detect_runups

        updated_timelines = update_narratives(briefs)
        changed_names = [t.narrative_name for t in updated_timelines]
        runups = detect_runups(changed_narratives=changed_names)
        logger.info("Active run-ups after detection: %d", len(runups))

        # --- 4. Probability updates ---
        from .probability_engine import update_probabilities

        updates = update_probabilities(briefs)
        logger.info("Probability updates recorded: %d", len(updates))

    except Exception:
        logger.exception("fetch_and_process cycle FAILED.")
    finally:
        logger.info("=== fetch_and_process cycle END ===")


async def prediction_scoring() -> None:
    """Recurring job: auto-score predictions using resolved Polymarket markets."""
    logger.info("=== prediction_scoring cycle START ===")
    try:
        from .prediction_scorer import score_predictions
        count = score_predictions()
        logger.info("Prediction scoring: %d resolved.", count)
    except Exception:
        logger.exception("prediction_scoring cycle FAILED.")
    finally:
        logger.info("=== prediction_scoring cycle END ===")


async def deep_analysis_job() -> None:
    """Recurring job: run deep database analysis (2x daily)."""
    logger.info("=== deep_analysis cycle START ===")
    try:
        from .deep_analysis import run_deep_analysis
        report = run_deep_analysis(period_days=7)
        if report:
            logger.info("Deep analysis complete: report %d created.", report.id)
        else:
            logger.warning("Deep analysis returned None — check logs for errors.")
    except Exception:
        logger.exception("deep_analysis cycle FAILED.")
    finally:
        logger.info("=== deep_analysis cycle END ===")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Startup / shutdown lifecycle hook."""
    # --- Startup ---
    config.configure_logging()
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
    )
    # Schedule Polymarket data refresh (every hour)
    scheduler.add_job(
        polymarket_refresh,
        trigger=IntervalTrigger(hours=1),
        id="polymarket_refresh",
        name="Polymarket data refresh",
        replace_existing=True,
        max_instances=1,
    )

    # Schedule prediction scoring (every 2 hours)
    scheduler.add_job(
        prediction_scoring,
        trigger=IntervalTrigger(hours=2),
        id="prediction_scoring",
        name="Auto-score predictions",
        replace_existing=True,
        max_instances=1,
    )
    # Schedule deep analysis (2x daily at 08:00 and 20:00 UTC)
    scheduler.add_job(
        deep_analysis_job,
        trigger=CronTrigger(hour="8,20", minute=0),
        id="deep_analysis",
        name="Deep database analysis",
        replace_existing=True,
        max_instances=1,
    )

    # Schedule confidence scoring (every 30 min — pure Python, no API cost)
    scheduler.add_job(
        confidence_scoring,
        trigger=IntervalTrigger(minutes=30),
        id="confidence_scoring",
        name="Confidence scoring & signal generation",
        replace_existing=True,
        max_instances=1,
    )

    # Schedule GDELT global events fetch (every 30 min)
    scheduler.add_job(
        gdelt_fetch_and_process,
        trigger=IntervalTrigger(minutes=30),
        id="gdelt_fetch",
        name="GDELT global events fetch",
        replace_existing=True,
        max_instances=1,
    )

    # Schedule proximity tracking update (every 5 min)
    scheduler.add_job(
        proximity_update,
        trigger=IntervalTrigger(minutes=5),
        id="proximity_update",
        name="Proximity threshold tracking",
        replace_existing=True,
        max_instances=1,
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
        )
        logger.info(
            "X/Twitter OSINT enabled — fetching every %d min.",
            config.twitter_fetch_interval_minutes,
        )
    else:
        logger.info("X/Twitter OSINT disabled — no bearer token configured.")

    scheduler.start()
    logger.info(
        "Scheduler started -- fetch %dmin, polymarket 1h, scoring 2h, confidence 30min, GDELT 30min, analysis 08:00+20:00 UTC.",
        config.fetch_interval_minutes,
    )

    # Run one immediate cycle in the background so the engine is pre-warmed
    asyncio.create_task(_initial_fetch())

    yield

    # --- Shutdown ---
    scheduler.shutdown(wait=False)
    logger.info("Scheduler shut down.")


async def polymarket_refresh() -> None:
    """Recurring job: fetch and match Polymarket prediction market data."""
    logger.info("=== polymarket_refresh cycle START ===")
    try:
        from .polymarket import update_polymarket_matches
        count = update_polymarket_matches()
        logger.info("Polymarket refresh: %d matches updated.", count)
    except Exception:
        logger.exception("polymarket_refresh cycle FAILED.")
    finally:
        logger.info("=== polymarket_refresh cycle END ===")


async def twitter_fetch_and_process() -> None:
    """Recurring job: fetch tweets from OSINT accounts and process them."""
    logger.info("=== twitter_fetch_and_process cycle START ===")
    try:
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

        # Run through the same pipeline as RSS articles
        from .nlp_pipeline import process_batch

        briefs = process_batch(new_articles)
        logger.info("X: produced %d briefs.", len(briefs))

        if briefs:
            from .narrative_tracker import update_narratives, detect_runups

            updated = update_narratives(briefs)
            changed = [t.narrative_name for t in updated]
            detect_runups(changed_narratives=changed)

            from .probability_engine import update_probabilities

            update_probabilities(briefs)

    except Exception:
        logger.exception("twitter_fetch_and_process cycle FAILED.")
    finally:
        logger.info("=== twitter_fetch_and_process cycle END ===")


async def gdelt_fetch_and_process() -> None:
    """Recurring job: fetch articles from GDELT global events database."""
    logger.info("=== gdelt_fetch_and_process cycle START ===")
    try:
        from .gdelt_fetcher import GdeltFetcher

        fetcher = GdeltFetcher()
        new_articles = await asyncio.to_thread(fetcher.fetch_and_save)

        if not new_articles:
            logger.info("GDELT: no new articles this cycle.")
            return

        logger.info("GDELT: fetched %d new articles.", len(new_articles))

        # Run through the same NLP pipeline as RSS articles
        from .nlp_pipeline import process_batch

        briefs = process_batch(new_articles)
        logger.info("GDELT: produced %d briefs.", len(briefs))

        if briefs:
            from .narrative_tracker import update_narratives, detect_runups

            updated = update_narratives(briefs)
            changed = [t.narrative_name for t in updated]
            detect_runups(changed_narratives=changed)

            from .probability_engine import update_probabilities

            update_probabilities(briefs)

    except Exception:
        logger.exception("gdelt_fetch_and_process cycle FAILED.")
    finally:
        logger.info("=== gdelt_fetch_and_process cycle END ===")


async def proximity_update() -> None:
    """Recurring job: update proximity percentages for consequence price thresholds."""
    logger.info("=== proximity_update cycle START ===")
    try:
        import json as _json
        from .db import Consequence
        from .price_fetcher import get_price_fetcher

        session = get_session()
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
                        pct = min(100.0, (current / target) * 100)
                    else:  # "below"
                        pct = min(100.0, (target / current) * 100) if current > 0 else 0

                    cons.proximity_pct = round(pct, 1)
                    updated += 1
                    break  # Use first threshold

            except Exception:
                continue

        if updated:
            session.commit()
            logger.info("Proximity: updated %d consequences.", updated)
        session.close()

    except Exception:
        logger.exception("proximity_update cycle FAILED.")
    finally:
        logger.info("=== proximity_update cycle END ===")


async def confidence_scoring() -> None:
    """Recurring job: score active run-ups and generate trading signals."""
    logger.info("=== confidence_scoring cycle START ===")
    try:
        from .confidence_scorer import update_trading_signals

        signals = update_trading_signals()
        buy_plus = sum(1 for s in signals if s.signal_level in ("BUY", "STRONG_BUY"))
        logger.info(
            "Confidence scoring: %d signals (%d BUY+).", len(signals), buy_plus
        )
    except Exception:
        logger.exception("confidence_scoring cycle FAILED.")
    finally:
        logger.info("=== confidence_scoring cycle END ===")


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

# CORS -- allow all origins during development; tighten for production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the API router (all routes are under /api)
app.include_router(api_router)


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
