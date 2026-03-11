"""Configuration loader for the News Analyzer engine.

Reads from environment variables with sensible defaults.
Loads RSS feed definitions from default_feeds.yaml.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Optional

import yaml

logger = logging.getLogger(__name__)

# Base directory for the engine package
_ENGINE_DIR = Path(__file__).resolve().parent


class EngineConfig:
    """Central configuration object for the News Analyzer engine."""

    def __init__(self) -> None:
        # Server
        self.engine_port: int = int(os.getenv("ENGINE_PORT", "9120"))
        self.host: str = os.getenv("ENGINE_HOST", "0.0.0.0")

        # Data
        self.data_dir: Path = Path(os.getenv("DATA_DIR", str(_ENGINE_DIR / "data")))
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Database
        self.database_uri: str = os.getenv(
            "DATABASE_URI",
            f"sqlite:///{self.data_dir / 'news_analyzer.db'}",
        )

        # Fetch settings
        self.fetch_interval_minutes: int = int(
            os.getenv("FETCH_INTERVAL_MINUTES", "30")
        )
        self.feed_timeout_seconds: int = int(
            os.getenv("FEED_TIMEOUT_SECONDS", "30")
        )
        self.dedup_similarity_threshold: float = float(
            os.getenv("DEDUP_SIMILARITY_THRESHOLD", "0.82")
        )

        # NLP settings
        self.spacy_model: str = os.getenv("SPACY_MODEL", "en_core_web_lg")
        self.yake_top_keywords: int = int(os.getenv("YAKE_TOP_KEYWORDS", "8"))
        self.summary_sentences: int = int(os.getenv("SUMMARY_SENTENCES", "2"))

        # Narrative / run-up detection
        self.runup_threshold: float = float(os.getenv("RUNUP_THRESHOLD", "50.0"))

        # Probability engine
        self.max_probability_shift: float = float(
            os.getenv("MAX_PROBABILITY_SHIFT", "0.15")
        )
        self.min_keyword_overlap: int = int(os.getenv("MIN_KEYWORD_OVERLAP", "2"))
        self.significant_shift_threshold: float = float(
            os.getenv("SIGNIFICANT_SHIFT_THRESHOLD", "0.10")
        )

        # Logging
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

        # Feeds (loaded lazily)
        self._feeds: Optional[List[Dict]] = None

    # ------------------------------------------------------------------
    # Feed loading
    # ------------------------------------------------------------------
    @property
    def feeds(self) -> List[Dict]:
        """Return the list of RSS feed definitions.

        Loaded once from ``default_feeds.yaml`` (or an override path set via
        the ``FEEDS_YAML`` environment variable).
        """
        if self._feeds is None:
            self._feeds = self._load_feeds()
        return self._feeds

    def _load_feeds(self) -> List[Dict]:
        feeds_path = Path(
            os.getenv("FEEDS_YAML", str(_ENGINE_DIR / "default_feeds.yaml"))
        )

        if not feeds_path.exists():
            logger.warning(
                "Feeds YAML not found at %s -- starting with an empty feed list.",
                feeds_path,
            )
            return []

        try:
            with open(feeds_path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            feeds = data.get("feeds", [])
            logger.info("Loaded %d feeds from %s", len(feeds), feeds_path)
            return feeds
        except Exception:
            logger.exception("Failed to load feeds from %s", feeds_path)
            return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def configure_logging(self) -> None:
        """Apply the configured log level to the root logger."""
        logging.basicConfig(
            level=getattr(logging, self.log_level, logging.INFO),
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def __repr__(self) -> str:
        return (
            f"EngineConfig(port={self.engine_port}, "
            f"data_dir={self.data_dir}, "
            f"db={self.database_uri}, "
            f"feeds={len(self.feeds)})"
        )


# Module-level singleton ------------------------------------------------
config = EngineConfig()
