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

        update_narratives(briefs)
        runups = detect_runups()
        logger.info("Active run-ups after detection: %d", len(runups))

        # --- 4. Probability updates ---
        from .probability_engine import update_probabilities

        updates = update_probabilities(briefs)
        logger.info("Probability updates recorded: %d", len(updates))

    except Exception:
        logger.exception("fetch_and_process cycle FAILED.")
    finally:
        logger.info("=== fetch_and_process cycle END ===")


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

    scheduler.start()
    logger.info(
        "Scheduler started -- fetch_and_process every %d min, polymarket every 1h.",
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
