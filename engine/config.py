"""Configuration loader for the News Analyzer engine.

Reads from environment variables with sensible defaults.
Loads RSS feed definitions from default_feeds.yaml.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Optional

import yaml

# Load .env file BEFORE any os.getenv() calls
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=False)
except ImportError:
    pass  # python-dotenv not installed; rely on env vars and DB fallback

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

        # Groq API (for swarm consensus — free tier)
        self._groq_api_key: str = os.getenv("GROQ_API_KEY", "")
        self.groq_model: str = os.getenv(
            "GROQ_MODEL", "llama-3.3-70b-versatile"
        )
        # OpenRouter API (for diverse free models — Google Gemini, DeepSeek, etc.)
        self._openrouter_api_key: str = os.getenv("OPENROUTER_API_KEY", "")
        self.swarm_interval_minutes: int = int(
            os.getenv("SWARM_INTERVAL_MINUTES", "60")
        )
        # FRED API (optional — free registration at fred.stlouisfed.org)
        self._fred_api_key: str = os.getenv("FRED_API_KEY", "")

        # Telegram Bot API (for push notifications)
        self._telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

        # Twitter / X API
        self._twitter_bearer_token: str = os.getenv(
            "X_BEARER_TOKEN", os.getenv("TWITTER_BEARER_TOKEN", "")
        )
        self.twitter_fetch_interval_minutes: int = int(
            os.getenv("TWITTER_FETCH_INTERVAL_MINUTES", "120")
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
    # Swarm enabled (dynamic — checks if any API key is available)
    # ------------------------------------------------------------------
    @property
    def swarm_enabled(self) -> bool:
        """True if at least one swarm LLM provider (Groq or OpenRouter) is configured."""
        return bool(self.groq_api_key) or bool(self.openrouter_api_key)

    @swarm_enabled.setter
    def swarm_enabled(self, value: bool) -> None:
        """No-op setter to avoid AttributeError from old code."""
        pass  # Property is dynamically computed

    # ------------------------------------------------------------------
    # Groq API key (from env or DB settings)
    # ------------------------------------------------------------------
    @property
    def groq_api_key(self) -> str:
        """Return Groq API key from env → DB settings → empty."""
        if self._groq_api_key:
            return self._groq_api_key
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("groq_api_key")
                if setting and setting.value:
                    return setting.value
            finally:
                session.close()
        except Exception:
            pass
        return ""

    @groq_api_key.setter
    def groq_api_key(self, value: str) -> None:
        """Store Groq API key in DB settings for persistence."""
        self._groq_api_key = value
        self.swarm_enabled = bool(value)
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("groq_api_key")
                if setting:
                    setting.value = value
                else:
                    setting = EngineSettings(key="groq_api_key", value=value)
                    session.add(setting)
                session.commit()
            finally:
                session.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # OpenRouter API key (from env or DB settings)
    # ------------------------------------------------------------------
    @property
    def openrouter_api_key(self) -> str:
        """Return OpenRouter API key from env → DB settings → empty."""
        if self._openrouter_api_key:
            return self._openrouter_api_key
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("openrouter_api_key")
                if setting and setting.value:
                    return setting.value
            finally:
                session.close()
        except Exception:
            pass
        return ""

    @openrouter_api_key.setter
    def openrouter_api_key(self, value: str) -> None:
        """Store OpenRouter API key in DB settings."""
        self._openrouter_api_key = value
        self.swarm_enabled = bool(self._groq_api_key) or bool(value)
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("openrouter_api_key")
                if setting:
                    setting.value = value
                else:
                    setting = EngineSettings(key="openrouter_api_key", value=value)
                    session.add(setting)
                session.commit()
            finally:
                session.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Twitter / X bearer token (from env or DB settings)
    # ------------------------------------------------------------------
    @property
    def twitter_bearer_token(self) -> str:
        """Return X/Twitter bearer token from env → DB settings → empty."""
        if self._twitter_bearer_token:
            return self._twitter_bearer_token
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("twitter_bearer_token")
                if setting and setting.value:
                    return setting.value
            finally:
                session.close()
        except Exception:
            pass
        return ""

    @twitter_bearer_token.setter
    def twitter_bearer_token(self, value: str) -> None:
        """Store X/Twitter bearer token in DB settings for persistence."""
        self._twitter_bearer_token = value
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("twitter_bearer_token")
                if setting:
                    setting.value = value
                else:
                    setting = EngineSettings(key="twitter_bearer_token", value=value)
                    session.add(setting)
                session.commit()
            finally:
                session.close()
        except Exception:
            pass

    @property
    def twitter_enabled(self) -> bool:
        """True if X/Twitter bearer token is configured."""
        return bool(self.twitter_bearer_token)

    # ------------------------------------------------------------------
    # FRED API key (optional — from env or DB settings)
    # ------------------------------------------------------------------
    @property
    def fred_api_key(self) -> str:
        """Return FRED API key from env → DB settings → empty."""
        if self._fred_api_key:
            return self._fred_api_key
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("fred_api_key")
                if setting and setting.value:
                    return setting.value
            finally:
                session.close()
        except Exception:
            pass
        return ""

    @fred_api_key.setter
    def fred_api_key(self, value: str) -> None:
        """Store FRED API key in DB settings."""
        self._fred_api_key = value
        try:
            from .db import get_session, EngineSettings
            session = get_session()
            try:
                setting = session.query(EngineSettings).get("fred_api_key")
                if setting:
                    setting.value = value
                else:
                    setting = EngineSettings(key="fred_api_key", value=value)
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
