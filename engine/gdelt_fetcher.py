"""GDELT global events fetcher for the OpenClaw News Analyzer.

Queries the GDELT Project's free API for global news articles matching
geopolitically relevant keywords. Articles are stored as standard Article
records and processed by the NLP pipeline identically to RSS-sourced articles.

GDELT updates every 15 minutes and contains 250M+ articles.
No API key required.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keyword queries -- each targets a geopolitical theme / region
# ---------------------------------------------------------------------------

GDELT_QUERIES: List[Dict[str, str]] = [
    {"query": "iran military OR nuclear", "region": "middle-east"},
    {"query": "china taiwan strait", "region": "east-asia"},
    {"query": "russia ukraine war", "region": "europe"},
    {"query": "oil opec energy crisis", "region": "global"},
    {"query": "NATO military deployment", "region": "europe"},
    {"query": "sanctions embargo", "region": "global"},
    {"query": "missile strike bombing", "region": "global"},
    {"query": "coup regime change", "region": "global"},
    {"query": "ceasefire negotiations peace", "region": "global"},
    {"query": "troop mobilization deployment", "region": "global"},
]


# ---------------------------------------------------------------------------
# GdeltFetcher
# ---------------------------------------------------------------------------

class GdeltFetcher:
    """Fetches global news articles from the GDELT Project's free API.

    Articles are converted to :class:`~engine.db.Article` records and saved
    to the database so the NLP pipeline processes them automatically.

    All public methods are **synchronous** because httpx is used in
    blocking mode.  The async scheduler should call ``fetch_and_save``
    via ``asyncio.to_thread`` or an equivalent wrapper.
    """

    GDELT_API_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self) -> None:
        self._timeout = 15  # seconds

    # ------------------------------------------------------------------
    # Single-query fetch
    # ------------------------------------------------------------------

    def fetch_articles(
        self,
        query: str,
        timespan: str = "60min",
        max_records: int = 75,
    ) -> List[Dict[str, Any]]:
        """Fetch articles from the GDELT API for a single keyword query.

        Parameters
        ----------
        query:
            A keyword search string (e.g. ``"russia ukraine war"``).
        timespan:
            How far back to look.  Defaults to ``"60min"``.
        max_records:
            Maximum number of articles to return per query.

        Returns
        -------
        list[dict]
            Raw article dicts straight from the GDELT response, filtered
            to English-language results only.
        """
        params = {
            "query": query,
            "mode": "ArtList",
            "maxrecords": str(max_records),
            "format": "json",
            "timespan": timespan,
        }

        try:
            logger.info("GDELT: querying %r (timespan=%s)", query, timespan)

            response = httpx.get(
                self.GDELT_API_BASE,
                params=params,
                timeout=self._timeout,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; OpenClawNewsAnalyzer/1.0; "
                        "+https://github.com/openclaw)"
                    ),
                },
            )

            if response.status_code != 200:
                logger.warning(
                    "GDELT: query %r returned HTTP %d -- skipping.",
                    query,
                    response.status_code,
                )
                return []

            data = response.json()

        except httpx.TimeoutException:
            logger.warning("GDELT: timeout querying %r", query)
            return []
        except httpx.HTTPError as exc:
            logger.warning("GDELT: HTTP error querying %r: %s", query, exc)
            return []
        except ValueError:
            # json() decoding failed
            logger.warning("GDELT: invalid JSON response for query %r", query)
            return []
        except Exception:
            logger.exception("GDELT: unexpected error querying %r", query)
            return []

        raw_articles = data.get("articles") or []

        # Filter to English-language articles only
        english_articles = [
            a for a in raw_articles
            if (a.get("language") or "").strip() in ("English", "")
        ]

        logger.info(
            "GDELT: query %r returned %d articles (%d English).",
            query,
            len(raw_articles),
            len(english_articles),
        )

        return english_articles

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_seendate(seendate: str) -> datetime:
        """Parse a GDELT ``seendate`` string into a :class:`datetime`.

        GDELT returns dates in the format ``"20240315T120000Z"``.
        Falls back to :func:`datetime.utcnow` if parsing fails.
        """
        if not seendate:
            return datetime.utcnow()
        try:
            return datetime.strptime(seendate, "%Y%m%dT%H%M%SZ")
        except (ValueError, TypeError):
            logger.debug("GDELT: could not parse seendate %r", seendate)
            return datetime.utcnow()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def fetch_and_save(self) -> List:
        """Fetch articles for every configured query, deduplicate, and persist.

        This is a **synchronous** method.  The async scheduler should call
        it via ``asyncio.to_thread(fetcher.fetch_and_save)`` or an
        equivalent wrapper.

        Workflow:
            1. Iterate over :data:`GDELT_QUERIES` and fetch articles.
            2. Convert raw GDELT article dicts to Article-compatible format.
            3. Deduplicate against existing ``Article.link`` values.
            4. Save new articles to the database.

        Returns
        -------
        list[Article]
            Newly created :class:`~engine.db.Article` objects, or an
            empty list when there is nothing new (or the API is down).
        """
        from .db import get_session, Article

        # --- 1. fetch from all queries ---
        raw_articles: List[Dict[str, Any]] = []
        for qdef in GDELT_QUERIES:
            query_str = qdef["query"]
            region = qdef.get("region", "global")

            fetched = self.fetch_articles(query_str)
            for art in fetched:
                url = (art.get("url") or "").strip()
                title = (art.get("title") or "").strip()
                if not url or not title:
                    continue

                domain = (art.get("domain") or "unknown").strip()
                seendate = (art.get("seendate") or "").strip()

                raw_articles.append({
                    "title": title,
                    "description": title,  # GDELT doesn't always have description
                    "link": url,
                    "source": f"GDELT - {domain}",
                    "region": region,
                    "lang": "en",
                    "pub_date": self._parse_seendate(seendate),
                })

        logger.info(
            "GDELT: collected %d candidate articles from %d queries.",
            len(raw_articles),
            len(GDELT_QUERIES),
        )

        if not raw_articles:
            return []

        # --- 2. deduplicate within the batch (same URL from multiple queries) ---
        seen_links: set = set()
        unique_articles: List[Dict[str, Any]] = []
        for art in raw_articles:
            if art["link"] not in seen_links:
                seen_links.add(art["link"])
                unique_articles.append(art)

        logger.info(
            "GDELT: batch dedup: %d -> %d articles.",
            len(raw_articles),
            len(unique_articles),
        )

        # --- 3. deduplicate against database ---
        session = get_session()
        try:
            candidate_links = [a["link"] for a in unique_articles]
            existing_links: set = set()

            # Query in chunks to avoid overly large IN clauses
            chunk_size = 500
            for i in range(0, len(candidate_links), chunk_size):
                chunk = candidate_links[i : i + chunk_size]
                rows = (
                    session.query(Article.link)
                    .filter(Article.link.in_(chunk))
                    .all()
                )
                existing_links.update(row[0] for row in rows)

            new_articles = [
                a for a in unique_articles if a["link"] not in existing_links
            ]

            skipped = len(unique_articles) - len(new_articles)
            if skipped:
                logger.info(
                    "GDELT: skipped %d articles already in DB.", skipped
                )

            if not new_articles:
                logger.info("GDELT: no new articles to save this cycle.")
                return []

            # --- 4. save ---
            saved: List = []
            for a in new_articles:
                article = Article(
                    title=a["title"][:512],
                    description=a.get("description"),
                    link=a["link"],
                    source=a["source"],
                    pub_date=a.get("pub_date"),
                    fetched_at=datetime.utcnow(),
                    original_lang=a.get("lang", "en"),
                )
                session.add(article)
                saved.append(article)

            session.commit()

            # Refresh so that auto-generated ``id`` values are available.
            for article in saved:
                session.refresh(article)

            logger.info(
                "GDELT: saved %d new articles to DB.", len(saved)
            )
            return saved

        except Exception:
            session.rollback()
            logger.exception("GDELT: failed to save articles to DB.")
            return []
        finally:
            session.close()
