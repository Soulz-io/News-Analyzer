"""X/Twitter OSINT collector for the OpenClaw News Analyzer.

Fetches tweets from a curated list of prioritised OSINT, military-tracking,
investigative-journalism, and geopolitical-commentary accounts via the
Twitter API v2 (tweepy).  Tweets are converted to Article records and
persisted through the existing ORM so that the downstream NLP pipeline
(entity extraction, sentiment scoring, narrative clustering) processes
them identically to RSS-sourced articles.

Rate-budget management
~~~~~~~~~~~~~~~~~~~~~~
X Basic plan provides ~10 000 tweet reads / month.  A four-tier priority
system ensures the most operationally valuable accounts are polled every
cycle while lower-priority commentary accounts are only checked once a
day.  Approximate monthly cost at one cycle every two hours:

    Tier 1 (every cycle)   ~6 accts x 12 cycles/day x 30 = 2 160 calls
    Tier 2 (every 2nd)     ~5 accts x  6 cycles/day x 30 =   900 calls
    Tier 3 (every 3rd)     ~4 accts x  4 cycles/day x 30 =   480 calls
    Tier 4 (every 12th)    ~4 accts x  1 cycle /day x 30 =   120 calls
                                                   Total   ~3 660 calls

Authentication
~~~~~~~~~~~~~~
Requires either ``X_BEARER_TOKEN`` or ``TWITTER_BEARER_TOKEN`` in the
environment.  If neither is set the module logs a warning and every public
method returns an empty list rather than crashing the scheduler.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OSINT Account Configuration (4 priority tiers)
# ---------------------------------------------------------------------------

OSINT_ACCOUNTS: Dict[str, Dict[str, Any]] = {
    # === Tier 1: Military OSINT Trackers (every cycle) =====================
    "OSINTdefender": {
        "name": "OSINT Defender",
        "region": "global",
        "category": "osint_military",
        "priority": 1,
        "credibility": 0.72,
    },
    "sentdefender": {
        "name": "Sentinel OSINT",
        "region": "global",
        "category": "osint_military",
        "priority": 1,
        "credibility": 0.70,
    },
    "IntelCrab": {
        "name": "IntelCrab",
        "region": "global",
        "category": "osint_military",
        "priority": 1,
        "credibility": 0.70,
    },
    "Nrg8000": {
        "name": "Nrg8000",
        "region": "global",
        "category": "osint_military",
        "priority": 1,
        "credibility": 0.68,
    },
    "AuroraIntel": {
        "name": "Aurora Intel",
        "region": "global",
        "category": "osint_military",
        "priority": 1,
        "credibility": 0.72,
    },
    "Faytuks": {
        "name": "Faytuks",
        "region": "middle-east",
        "category": "osint_military",
        "priority": 1,
        "credibility": 0.68,
    },

    # === Tier 2: Trackers + Defense Analysts (every 2nd cycle) =============
    "AircraftSpots": {
        "name": "Aircraft Spots",
        "region": "global",
        "category": "tracking",
        "priority": 2,
        "credibility": 0.74,
    },
    "RALee85": {
        "name": "Rob Lee",
        "region": "europe",
        "category": "defense_analyst",
        "priority": 2,
        "credibility": 0.78,
    },
    "oryxspioenkop": {
        "name": "Oryx",
        "region": "europe",
        "category": "defense_analyst",
        "priority": 2,
        "credibility": 0.80,
    },
    "Conflicts": {
        "name": "Conflict News",
        "region": "global",
        "category": "osint",
        "priority": 2,
        "credibility": 0.70,
    },
    "christaborowski": {
        "name": "Chris Borowski",
        "region": "europe",
        "category": "regional",
        "priority": 2,
        "credibility": 0.68,
    },

    "NoLimitGains": {
        "name": "NoLimit",
        "region": "global",
        "category": "macro_finance",
        "priority": 2,
        "credibility": 0.68,
    },
    "QuiverQuant": {
        "name": "Quiver Quantitative",
        "region": "north-america",
        "category": "insider_trading",
        "priority": 1,
        "credibility": 0.82,
    },
    "unusual_whales": {
        "name": "Unusual Whales",
        "region": "north-america",
        "category": "options_flow",
        "priority": 1,
        "credibility": 0.78,
    },
    "DeItaone": {
        "name": "Walter Bloomberg",
        "region": "global",
        "category": "breaking_market",
        "priority": 1,
        "credibility": 0.85,
    },
    "BNONews": {
        "name": "BNO News",
        "region": "global",
        "category": "breaking_news",
        "priority": 1,
        "credibility": 0.80,
    },

    # === Tier 2: Finance + Macro (every 2nd cycle) ==========================
    "FirstSquawk": {
        "name": "First Squawk",
        "region": "global",
        "category": "breaking_market",
        "priority": 2,
        "credibility": 0.76,
    },
    "Fxhedgers": {
        "name": "Fxhedgers",
        "region": "global",
        "category": "macro_finance",
        "priority": 2,
        "credibility": 0.68,
    },
    "Insider_Trades": {
        "name": "Insider Trade Alerts",
        "region": "north-america",
        "category": "insider_trading",
        "priority": 2,
        "credibility": 0.75,
    },

    # === Tier 3: Geopolitical Journalists (every 3rd cycle) ================
    "ggreenwald": {
        "name": "Glenn Greenwald",
        "region": "global",
        "category": "investigative",
        "priority": 3,
        "credibility": 0.75,
    },
    "MaxBlumenthal": {
        "name": "Max Blumenthal",
        "region": "global",
        "category": "investigative",
        "priority": 3,
        "credibility": 0.65,
    },
    "zerohedge": {
        "name": "ZeroHedge",
        "region": "global",
        "category": "finance",
        "priority": 3,
        "credibility": 0.55,
    },
    "wikileaks": {
        "name": "WikiLeaks",
        "region": "global",
        "category": "transparency",
        "priority": 3,
        "credibility": 0.70,
    },

    # === Tier 4: Commentary / Analysis (every 12th cycle, ~1x/day) =========
    "mtracey": {
        "name": "Michael Tracey",
        "region": "global",
        "category": "analysis",
        "priority": 4,
        "credibility": 0.62,
    },
    "BenjaminNorton": {
        "name": "Ben Norton",
        "region": "global",
        "category": "investigative",
        "priority": 4,
        "credibility": 0.60,
    },
    "caitoz": {
        "name": "Caitlin Johnstone",
        "region": "global",
        "category": "analysis",
        "priority": 4,
        "credibility": 0.55,
    },
    "TheGrayzoneNews": {
        "name": "The Grayzone",
        "region": "global",
        "category": "media",
        "priority": 4,
        "credibility": 0.60,
    },
}


def get_account_credibility(username: str) -> float:
    """Return the credibility rating for an X account (0.0 - 1.0).

    Falls back to 0.50 for unknown accounts.
    """
    acct = OSINT_ACCOUNTS.get(username)
    return acct["credibility"] if acct else 0.50


# ---------------------------------------------------------------------------
# Sentence-splitting helper
# ---------------------------------------------------------------------------

def _first_sentence(text: str, max_len: int = 200) -> str:
    """Extract the first sentence from *text* for use as a title.

    Splits on common sentence-ending punctuation (``. ! ?``).  If no
    sentence boundary is found the first line (up to *max_len* chars) is
    returned instead.
    """
    # Normalise whitespace so we work on a single logical line first.
    flat = " ".join(text.split())
    for sep in (". ", "! ", "? "):
        idx = flat.find(sep)
        if 0 < idx < max_len:
            return flat[: idx + 1]
    # Fallback: first line, truncated.
    first_line = text.split("\n", 1)[0]
    if len(first_line) > max_len:
        return first_line[:max_len].rsplit(" ", 1)[0] + "..."
    return first_line


# ---------------------------------------------------------------------------
# TwitterFetcher
# ---------------------------------------------------------------------------

class TwitterFetcher:
    """Fetches tweets from OSINT accounts using Twitter API v2 (tweepy).

    Tweets are converted to :class:`~engine.db.Article` records and saved
    to the database so the NLP pipeline processes them automatically.
    A four-tier priority system controls the per-cycle rate budget.

    All public methods are **synchronous** because the tweepy v2
    :class:`tweepy.Client` uses blocking HTTP.  The async scheduler in
    ``engine.py`` wraps calls via ``asyncio.to_thread`` or similar.
    """

    def __init__(self):
        # Try env vars first, then fall back to config (DB settings)
        from .config import config as _cfg
        self.bearer_token: str = (
            os.getenv("X_BEARER_TOKEN")
            or os.getenv("TWITTER_BEARER_TOKEN")
            or _cfg.twitter_bearer_token
            or ""
        )
        self._client = None
        self._user_id_cache: Dict[str, str] = {}
        self._cycle_counter: int = 0

        if not self.bearer_token:
            logger.warning(
                "X/Twitter bearer token not configured "
                "(set X_BEARER_TOKEN or TWITTER_BEARER_TOKEN). "
                "Twitter fetching will be skipped."
            )
            return

        try:
            import tweepy
        except ImportError:
            logger.error(
                "tweepy is not installed. "
                "Install it with: pip install tweepy>=4.14.0"
            )
            return

        self._client = tweepy.Client(
            bearer_token=self.bearer_token,
            wait_on_rate_limit=False,
        )
        logger.info("TwitterFetcher initialised (%d accounts configured).",
                     len(OSINT_ACCOUNTS))

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_configured(self) -> bool:
        """Return True if the fetcher has a working tweepy client."""
        return self._client is not None

    # ------------------------------------------------------------------
    # Priority scheduling
    # ------------------------------------------------------------------

    def _should_fetch(self, priority: int) -> bool:
        """Decide whether an account at *priority* tier runs this cycle.

        Tier 1 -- every cycle
        Tier 2 -- every 2nd cycle
        Tier 3 -- every 3rd cycle
        Tier 4 -- every 12th cycle (~once per day with 2-hour intervals)
        """
        if priority == 1:
            return True
        if priority == 2:
            return self._cycle_counter % 2 == 0
        if priority == 3:
            return self._cycle_counter % 3 == 0
        if priority == 4:
            return self._cycle_counter % 12 == 0
        return False

    # ------------------------------------------------------------------
    # User ID lookup (cached, synchronous)
    # ------------------------------------------------------------------

    def _get_user_id(self, username: str) -> Optional[str]:
        """Resolve a Twitter *username* to a numeric user ID.

        Results are cached for the lifetime of this ``TwitterFetcher``
        instance to avoid burning rate-limited user-lookup calls.
        """
        if username in self._user_id_cache:
            return self._user_id_cache[username]

        import tweepy

        try:
            user = self._client.get_user(username=username.lstrip("@"))
            if user and user.data:
                uid = str(user.data.id)
                self._user_id_cache[username] = uid
                return uid
            logger.warning("X user not found: @%s", username)
        except tweepy.TooManyRequests:
            logger.warning("X rate limit hit on user lookup: @%s", username)
        except tweepy.Unauthorized:
            logger.error("X unauthorised -- check bearer token")
        except Exception:
            logger.exception("X user lookup failed: @%s", username)
        return None

    # ------------------------------------------------------------------
    # Tweet fetching
    # ------------------------------------------------------------------

    def _fetch_user_tweets(
        self,
        username: str,
        max_results: int = 10,
        hours_back: int = 24,
    ) -> List[Dict[str, Any]]:
        """Fetch recent original tweets from *username*.

        Retweets are filtered out so only original content and quote
        tweets flow into the analysis pipeline.  Each returned dict is
        ready for conversion to an :class:`~engine.db.Article`.

        Keys per dict:
            title, description, link, source, region, lang, pub_date
        """
        import tweepy

        user_id = self._get_user_id(username)
        if not user_id:
            return []

        account_info = OSINT_ACCOUNTS.get(username, {})
        region = account_info.get("region", "global")

        start_time = (
            datetime.utcnow() - timedelta(hours=hours_back)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            resp = self._client.get_users_tweets(
                id=user_id,
                max_results=min(max_results, 100),
                start_time=start_time,
                tweet_fields=[
                    "created_at",
                    "text",
                    "public_metrics",
                    "entities",
                    "referenced_tweets",
                ],
            )

            if not resp or not resp.data:
                logger.debug("X: no recent tweets from @%s", username)
                return []

            articles: List[Dict[str, Any]] = []
            for tweet in resp.data:
                # --- Skip retweets (keep replies and quote tweets) ---
                if tweet.referenced_tweets:
                    is_retweet = any(
                        ref.type == "retweeted"
                        for ref in tweet.referenced_tweets
                    )
                    if is_retweet:
                        continue

                text: str = tweet.text or ""
                title = _first_sentence(text)
                pub_date = (
                    tweet.created_at if tweet.created_at else datetime.utcnow()
                )

                articles.append({
                    "title": title,
                    "description": text,
                    "link": f"https://twitter.com/{username}/status/{tweet.id}",
                    "source": f"X/Twitter - @{username}",
                    "region": region,
                    "lang": "en",
                    "pub_date": pub_date,
                })

            logger.info(
                "X: fetched %d original tweets from @%s", len(articles), username
            )
            return articles

        except tweepy.TooManyRequests:
            logger.warning("X rate limit hit on tweets for @%s", username)
            return []
        except tweepy.Unauthorized:
            logger.error("X unauthorised fetching tweets for @%s", username)
            return []
        except Exception:
            logger.exception("X tweet fetch failed for @%s", username)
            return []

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def fetch_and_save(self) -> List:
        """Fetch tweets for this cycle, deduplicate, and persist as Articles.

        This is a **synchronous** method.  The async scheduler should call
        it via ``asyncio.to_thread(fetcher.fetch_and_save)`` or an
        equivalent wrapper.

        Workflow:
            1. Increment the internal cycle counter.
            2. Determine which account tiers are due this cycle.
            3. Fetch tweets from each scheduled account.
            4. Check ``Article.link`` uniqueness to avoid duplicates.
            5. Save new articles to the database.

        Returns:
            A list of newly created :class:`~engine.db.Article` objects,
            or an empty list if there is nothing new (or the bearer token
            is not configured).
        """
        if not self.is_configured:
            logger.warning("X: bearer token not configured -- skipping fetch.")
            return []

        from .db import get_session, Article

        # --- 1. advance cycle ---
        self._cycle_counter += 1

        accounts_this_cycle = [
            (username, info)
            for username, info in OSINT_ACCOUNTS.items()
            if self._should_fetch(info["priority"])
        ]

        tier_counts = {}
        for _, info in accounts_this_cycle:
            t = info["priority"]
            tier_counts[t] = tier_counts.get(t, 0) + 1
        tier_summary = ", ".join(
            f"T{t}={c}" for t, c in sorted(tier_counts.items())
        )
        logger.info(
            "X fetch cycle #%d: %d accounts scheduled (%s).",
            self._cycle_counter,
            len(accounts_this_cycle),
            tier_summary,
        )

        # --- 2. fetch tweets ---
        raw_articles: List[Dict[str, Any]] = []
        accounts_fetched = 0
        for username, _info in accounts_this_cycle:
            tweets = self._fetch_user_tweets(
                username, max_results=10, hours_back=24,
            )
            if tweets:
                accounts_fetched += 1
            raw_articles.extend(tweets)

        logger.info(
            "X: collected %d tweets from %d / %d accounts.",
            len(raw_articles),
            accounts_fetched,
            len(accounts_this_cycle),
        )

        if not raw_articles:
            return []

        # --- 3. deduplicate and save ---
        session = get_session()
        try:
            candidate_links = [a["link"] for a in raw_articles]
            existing_links = set(
                row[0]
                for row in session.query(Article.link)
                .filter(Article.link.in_(candidate_links))
                .all()
            )

            new_articles = [
                a for a in raw_articles if a["link"] not in existing_links
            ]

            if not new_articles:
                logger.info("X: all %d tweets already in DB -- nothing new.",
                            len(raw_articles))
                return []

            saved: List = []
            for a in new_articles:
                article = Article(
                    title=a["title"][:512],
                    description=a.get("description"),
                    link=a["link"],
                    source=a["source"],
                    pub_date=a.get("pub_date"),
                    original_lang=a.get("lang", "en"),
                )
                session.add(article)
                saved.append(article)

            session.commit()

            # Refresh so that auto-generated ``id`` values are available.
            for article in saved:
                session.refresh(article)

            logger.info(
                "X: saved %d new tweet-articles to DB "
                "(skipped %d duplicates).",
                len(saved),
                len(raw_articles) - len(new_articles),
            )
            return saved

        except Exception:
            session.rollback()
            logger.exception("X: failed to save tweet-articles to DB.")
            return []
        finally:
            session.close()
