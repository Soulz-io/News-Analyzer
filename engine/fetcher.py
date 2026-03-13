"""Async RSS feed fetcher with two-level deduplication.

Fetches all configured RSS feeds concurrently, deduplicates articles by
exact link match and fuzzy title similarity, and persists new articles to
the database.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import aiohttp
import feedparser
from rapidfuzz import fuzz

from .config import config
from .db import get_session, Article

logger = logging.getLogger(__name__)


class RSSFetcher:
    """Asynchronous RSS feed fetcher with deduplication."""

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "RSSFetcher":
        timeout = aiohttp.ClientTimeout(total=config.feed_timeout_seconds)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; OpenClawNewsAnalyzer/1.0; "
                    "+https://github.com/openclaw)"
                ),
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Single-feed fetch
    # ------------------------------------------------------------------
    async def fetch_feed(self, feed: Dict) -> List[Dict]:
        """Fetch and parse a single RSS feed.

        Parameters
        ----------
        feed:
            A dict with at least ``name``, ``url``, and ``region`` keys
            (as defined in default_feeds.yaml).

        Returns
        -------
        list[dict]
            Parsed article dicts ready for deduplication / DB insertion.
        """
        name = feed["name"]
        url = feed["url"]
        region = feed.get("region", "global")
        lang = feed.get("lang", "en")

        try:
            logger.info("Fetching feed: %s (%s)", name, url)

            if self._session is None:
                raise RuntimeError("RSSFetcher must be used as an async context manager")

            async with self._session.get(url) as response:
                if response.status != 200:
                    logger.warning(
                        "Feed %s returned HTTP %d -- skipping.", name, response.status
                    )
                    return []
                raw = await response.text()

            parsed = feedparser.parse(raw)

            if parsed.bozo:
                logger.warning(
                    "Feed %s has parsing issues: %s", name, parsed.bozo_exception
                )

            articles: List[Dict] = []
            for entry in parsed.entries:
                try:
                    title = (entry.get("title") or "").strip()
                    link = (entry.get("link") or "").strip()
                    if not title or not link:
                        continue

                    description = (entry.get("description") or "").strip()

                    # Parse publication date
                    pub_date = self._parse_entry_date(entry)

                    articles.append(
                        {
                            "title": title,
                            "description": description,
                            "link": link,
                            "source": name,
                            "region": region,
                            "lang": lang,
                            "pub_date": pub_date,
                        }
                    )
                except Exception:
                    logger.exception("Error processing entry from %s", name)
                    continue

            logger.info("Fetched %d articles from %s", len(articles), name)
            return articles

        except asyncio.TimeoutError:
            logger.warning("Timeout fetching feed %s (%s)", name, url)
            return []
        except aiohttp.ClientError as exc:
            logger.warning("HTTP error fetching feed %s: %s", name, exc)
            return []
        except Exception:
            logger.exception("Unexpected error fetching feed %s", name)
            return []

    # ------------------------------------------------------------------
    # All-feed fetch
    # ------------------------------------------------------------------
    async def fetch_all_feeds(self) -> List[Dict]:
        """Fetch every active feed (default + user) concurrently.

        Returns
        -------
        list[dict]
            All raw article dicts from every feed (before dedup).
        """
        feeds = config.get_all_active_feeds()
        if not feeds:
            logger.warning("No feeds configured -- nothing to fetch.")
            return []

        logger.info("Fetching %d feeds concurrently...", len(feeds))

        tasks = [self.fetch_feed(feed) for feed in feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_articles: List[Dict] = []
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    "Feed %s raised an exception: %s",
                    feeds[idx].get("name", idx),
                    result,
                )
            elif isinstance(result, list):
                all_articles.extend(result)

        logger.info("Total raw articles fetched: %d", len(all_articles))
        return all_articles

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------
    def deduplicate(self, articles: List[Dict], session) -> List[Dict]:
        """Two-level deduplication: exact link, then fuzzy title match.

        Parameters
        ----------
        articles:
            Raw article dicts from ``fetch_all_feeds``.
        session:
            An active SQLAlchemy session for querying existing articles.

        Returns
        -------
        list[dict]
            Articles that survived both dedup stages.
        """
        if not articles:
            return []

        # --- Level 1: exact link match against DB ---
        existing_links = {
            row[0]
            for row in session.query(Article.link).all()
        }

        unique_by_link = [a for a in articles if a["link"] not in existing_links]
        logger.info(
            "Link dedup: %d -> %d articles",
            len(articles),
            len(unique_by_link),
        )

        if not unique_by_link:
            return []

        # Also deduplicate within the current batch (same link appearing
        # in multiple feeds).
        seen_links: set = set()
        batch_unique: List[Dict] = []
        for art in unique_by_link:
            if art["link"] not in seen_links:
                seen_links.add(art["link"])
                batch_unique.append(art)
        unique_by_link = batch_unique

        # --- Level 2: fuzzy title similarity ---
        cutoff = datetime.utcnow() - timedelta(days=7)
        recent_titles = {
            row[0].lower()
            for row in session.query(Article.title)
            .filter(Article.pub_date >= cutoff)
            .all()
            if row[0]
        }

        threshold = config.dedup_similarity_threshold * 100  # rapidfuzz uses 0-100

        # Convert set to list for rapidfuzz batch API (C-optimized, 10-50x faster)
        from rapidfuzz import process as rfprocess
        recent_titles_list = list(recent_titles) if recent_titles else []

        final: List[Dict] = []
        for art in unique_by_link:
            title_lower = art["title"].lower()

            # Exact title match
            if title_lower in recent_titles:
                continue

            # Fuzzy match using rapidfuzz batch API (C++ optimized)
            if recent_titles_list:
                match = rfprocess.extractOne(
                    title_lower,
                    recent_titles_list,
                    scorer=fuzz.ratio,
                    score_cutoff=threshold,
                )
                if match is not None:
                    logger.debug(
                        "Fuzzy dup: %r ~ %r (score=%.0f)",
                        art["title"][:60],
                        match[0][:60],
                        match[1],
                    )
                    continue

            final.append(art)

        logger.info(
            "Title dedup: %d -> %d articles",
            len(unique_by_link),
            len(final),
        )
        return final

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
    def save_articles(self, articles: List[Dict], session) -> List[Article]:
        """Insert deduplicated articles into the database.

        Returns
        -------
        list[Article]
            The newly created Article ORM objects (with IDs assigned).
        """
        if not articles:
            return []

        new_objs: List[Article] = []
        for art in articles:
            obj = Article(
                title=art["title"],
                description=art.get("description"),
                link=art["link"],
                source=art["source"],
                pub_date=art.get("pub_date") or datetime.utcnow(),
                fetched_at=datetime.utcnow(),
                original_lang=art.get("lang"),
            )
            session.add(obj)
            new_objs.append(obj)

        try:
            session.commit()
            logger.info("Saved %d new articles to the database.", len(new_objs))
        except Exception:
            logger.exception("Failed to commit new articles -- rolling back.")
            session.rollback()
            return []

        return new_objs

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------
    async def fetch_and_save(self) -> List[Article]:
        """End-to-end: fetch all feeds, deduplicate, persist.

        Returns
        -------
        list[Article]
            Newly saved Article ORM objects.
        """
        raw_articles = await self.fetch_all_feeds()
        if not raw_articles:
            logger.info("No articles fetched this cycle.")
            return []

        session = get_session()
        try:
            unique = self.deduplicate(raw_articles, session)
            saved = self.save_articles(unique, session)
            return saved
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_entry_date(entry) -> datetime:
        """Extract a datetime from a feedparser entry."""
        for attr in ("published_parsed", "updated_parsed"):
            parsed = getattr(entry, attr, None)
            if parsed:
                try:
                    return datetime(*parsed[:6])
                except Exception:
                    pass
        return datetime.utcnow()
