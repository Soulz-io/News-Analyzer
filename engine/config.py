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
            os.getenv("FETCH_INTERVAL_MINUTES", "15")
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

        # Tree generator (Claude API)
        # Try .env file first, then environment variable
        self._anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
        self.tree_generator_model: str = os.getenv(
            "TREE_GENERATOR_MODEL", "claude-haiku-4-5-20251001"
        )
        self.max_trees_per_cycle: int = int(
            os.getenv("MAX_TREES_PER_CYCLE", "5")
        )

        # Twitter / X API
        self.twitter_bearer_token: str = os.getenv(
            "X_BEARER_TOKEN", os.getenv("TWITTER_BEARER_TOKEN", "")
        )
        self.twitter_fetch_interval_minutes: int = int(
            os.getenv("TWITTER_FETCH_INTERVAL_MINUTES", "120")
        )
        self.twitter_enabled: bool = bool(self.twitter_bearer_token)

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
    # Combined feed list (default + user)
    # ------------------------------------------------------------------
    def get_all_active_feeds(self) -> List[Dict]:
        """Return all active feeds: defaults from YAML plus enabled user feeds
        from the database.

        This performs a late import of ``db`` to avoid circular-import issues
        (since ``db.py`` imports ``config``).
        """
        # Start with all default feeds (always considered active)
        combined: List[Dict] = list(self.feeds)

        # Add enabled user feeds from the database
        try:
            from .db import get_session, UserFeed

            session = get_session()
            try:
                user_feeds = (
                    session.query(UserFeed)
                    .filter(UserFeed.enabled == True)
                    .all()
                )
                for uf in user_feeds:
                    combined.append({
                        "name": uf.name,
                        "url": uf.url,
                        "region": uf.region or "global",
                        "lang": uf.lang or "en",
                    })
                logger.info(
                    "Combined feeds: %d default + %d user = %d total",
                    len(self.feeds),
                    len(user_feeds),
                    len(combined),
                )
            finally:
                session.close()
        except Exception:
            logger.exception(
                "Failed to load user feeds from DB -- returning defaults only."
            )

        return combined

    # ------------------------------------------------------------------
    # Anthropic API key (from env, .env file, or DB settings)
    # ------------------------------------------------------------------
    @property
    def anthropic_api_key(self) -> str:
        """Return API key from env → DB settings → empty."""
        if self._anthropic_api_key:
            return self._anthropic_api_key
        # Try loading from DB settings
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("anthropic_api_key")
                if setting and setting.value:
                    return setting.value
            finally:
                session.close()
        except Exception:
            pass
        return ""

    @anthropic_api_key.setter
    def anthropic_api_key(self, value: str) -> None:
        """Store API key in DB settings for persistence."""
        self._anthropic_api_key = value
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("anthropic_api_key")
                if setting:
                    setting.value = value
                else:
                    setting = EngineSettings(key="anthropic_api_key", value=value)
                    session.add(setting)
                session.commit()
            finally:
                session.close()
        except Exception:
            pass

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
