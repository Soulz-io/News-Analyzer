"""X/Twitter OSINT collector for the OpenClaw News Analyzer.

Fetches tweets from a curated list of prioritised OSINT, military-tracking,
investigative-journalism, and geopolitical-commentary accounts via the
Twitter API v2 (tweepy).  Tweets are converted to Article records and
persisted through the existing ORM so that the downstream NLP pipeline
(entity extraction, sentiment scoring, narrative clustering) processes
them identically to RSS-sourced articles.

Adaptive rate-budget management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
X Basic plan provides ~10 000 tweet reads / month.  An **adaptive**
four-tier system defines **target polling intervals** per tier.  The
scheduler base interval (default 30 min) is combined with these targets
to auto-calculate the cycle-skip count for each tier.

    Tier   Target   Accounts   @30min base        Monthly
    ----   ------   --------   ---------------    -------
      1     30 min      3      48 cycles/day      4 320
      2    120 min     12      12 cycles/day      4 320
      3    360 min     24       4 cycles/day      2 880
      4   1440 min     36       1 cycle /day      1 080
                                          Total  ~12 600

At 120 min base interval the same targets yield ~9 360 calls/month.
Slight overshoot on Basic is tolerated -- tweepy handles HTTP 429
gracefully and calls returning 0 tweets don't count as reads.

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

from sqlalchemy import func

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OSINT Account Configuration (4 priority tiers)
# ---------------------------------------------------------------------------

OSINT_ACCOUNTS: Dict[str, Dict[str, Any]] = {

    # ═══════════════════════════════════════════════════════════════════════
    #  TIER 1 — BREAKING (target: every 30 min)
    #  Fastest, most reliable breaking-news sources.
    #  Budget: 3 accts × 48 cycles/day × 30 days = 4 320 calls/month
    # ═══════════════════════════════════════════════════════════════════════
    "DeItaone":        {"name": "Walter Bloomberg",       "region": "global",        "category": "breaking_market",  "priority": 1, "credibility": 0.85},
    "BNONews":         {"name": "BNO News",               "region": "global",        "category": "breaking_news",    "priority": 1, "credibility": 0.80},
    "OSINTdefender":   {"name": "OSINT Defender",         "region": "global",        "category": "osint_military",   "priority": 1, "credibility": 0.72},

    # ═══════════════════════════════════════════════════════════════════════
    #  TIER 2 — FAST OSINT + MARKETS (target: every 2h)
    #  High-value OSINT trackers, breaking market feeds, insider flow.
    #  Budget: 12 accts × 12 cycles/day × 30 days = 4 320 calls/month
    # ═══════════════════════════════════════════════════════════════════════
    # -- Military / Conflict OSINT --
    "sentdefender":    {"name": "Sentinel OSINT",         "region": "global",        "category": "osint_military",   "priority": 2, "credibility": 0.70},
    "IntelCrab":       {"name": "IntelCrab",              "region": "global",        "category": "osint_military",   "priority": 2, "credibility": 0.70},
    "AuroraIntel":     {"name": "Aurora Intel",           "region": "global",        "category": "osint_military",   "priority": 2, "credibility": 0.72},
    "WarMonitors":     {"name": "War Monitors",           "region": "global",        "category": "osint_military",   "priority": 2, "credibility": 0.72},
    "detresfa_":       {"name": "detresfa (OSINT Legend)", "region": "global",       "category": "osint_military",   "priority": 2, "credibility": 0.78},
    "Osint613":        {"name": "OSINT 613",              "region": "middle-east",   "category": "osint_military",   "priority": 2, "credibility": 0.72},
    "Faytuks":         {"name": "Faytuks",                "region": "middle-east",   "category": "osint_military",   "priority": 2, "credibility": 0.68},
    # -- Breaking Market / Insider Flow --
    "unusual_whales":  {"name": "Unusual Whales",         "region": "north-america", "category": "options_flow",     "priority": 2, "credibility": 0.78},
    "QuiverQuant":     {"name": "Quiver Quantitative",    "region": "north-america", "category": "insider_trading",  "priority": 2, "credibility": 0.82},
    "FirstSquawk":     {"name": "First Squawk",           "region": "global",        "category": "breaking_market",  "priority": 2, "credibility": 0.76},
    "NoLimitGains":    {"name": "NoLimit",                "region": "global",        "category": "macro_finance",    "priority": 2, "credibility": 0.68},
    "Nrg8000":         {"name": "Nrg8000",                "region": "global",        "category": "osint_military",   "priority": 2, "credibility": 0.68},

    # ═══════════════════════════════════════════════════════════════════════
    #  TIER 3 — ANALYSTS + INSTITUTIONAL (target: every 6h)
    #  Defense analysts, geopolitical thinkers, data-driven finance.
    #  Budget: 24 accts × 4 cycles/day × 30 days = 2 880 calls/month
    # ═══════════════════════════════════════════════════════════════════════
    # -- Defense / Conflict Analysts --
    "Tatarigami_UA":   {"name": "Tatarigami UA",          "region": "europe",        "category": "osint_military",   "priority": 3, "credibility": 0.75},
    "calibreobscura":  {"name": "Calibre Obscura",        "region": "global",        "category": "osint_military",   "priority": 3, "credibility": 0.74},
    "RALee85":         {"name": "Rob Lee",                "region": "europe",        "category": "defense_analyst",  "priority": 3, "credibility": 0.78},
    "oryxspioenkop":   {"name": "Oryx",                   "region": "europe",        "category": "defense_analyst",  "priority": 3, "credibility": 0.80},
    "AircraftSpots":   {"name": "Aircraft Spots",         "region": "global",        "category": "tracking",         "priority": 3, "credibility": 0.74},
    "War_Monitoring":  {"name": "War Monitoring",         "region": "global",        "category": "osint_military",   "priority": 3, "credibility": 0.68},
    "Conflicts":       {"name": "Conflict News",          "region": "global",        "category": "osint",            "priority": 3, "credibility": 0.70},
    "LongWarJournal":  {"name": "Long War Journal",       "region": "global",        "category": "conflict_analysis","priority": 3, "credibility": 0.80},
    # -- Institutional Intel --
    "IABOROWITZ":      {"name": "IISS",                   "region": "global",        "category": "defense_institutional","priority": 3, "credibility": 0.82},
    "ABOROWITZ":       {"name": "CSIS",                   "region": "global",        "category": "think_tank",       "priority": 3, "credibility": 0.80},
    "ICG_Updates":     {"name": "Crisis Group",           "region": "global",        "category": "conflict_analysis","priority": 3, "credibility": 0.82},
    "DefenseIntel":    {"name": "Defense Intelligence",   "region": "global",        "category": "defense_institutional","priority": 3, "credibility": 0.75},
    # -- Geopolitical Analysts --
    "ianbremmer":      {"name": "Ian Bremmer (Eurasia Group)","region": "global",    "category": "geopolitical_analyst","priority": 3, "credibility": 0.85},
    "christaborowski": {"name": "Chris Borowski",         "region": "europe",        "category": "regional",         "priority": 3, "credibility": 0.68},
    # -- Finance Data / Macro --
    "LizAnnSonders":   {"name": "Liz Ann Sonders (Schwab)","region": "north-america","category": "macro_finance",    "priority": 3, "credibility": 0.82},
    "charliebilello":  {"name": "Charlie Bilello",        "region": "north-america", "category": "macro_finance",    "priority": 3, "credibility": 0.80},
    "elerianm":        {"name": "Mohamed El-Erian",       "region": "global",        "category": "macro_finance",    "priority": 3, "credibility": 0.85},
    "bespokeinvest":   {"name": "Bespoke Invest",         "region": "north-america", "category": "market_data",      "priority": 3, "credibility": 0.80},
    "WSJmarkets":      {"name": "WSJ Markets",            "region": "global",        "category": "breaking_market",  "priority": 3, "credibility": 0.88},
    "Fxhedgers":       {"name": "Fxhedgers",              "region": "global",        "category": "macro_finance",    "priority": 3, "credibility": 0.68},
    "Insider_Trades":  {"name": "Insider Trade Alerts",   "region": "north-america", "category": "insider_trading",  "priority": 3, "credibility": 0.75},
    # -- Investigative / Transparency --
    "ggreenwald":      {"name": "Glenn Greenwald",        "region": "global",        "category": "investigative",    "priority": 3, "credibility": 0.75},
    "zerohedge":       {"name": "ZeroHedge",              "region": "global",        "category": "finance",          "priority": 3, "credibility": 0.55},
    "wikileaks":       {"name": "WikiLeaks",              "region": "global",        "category": "transparency",     "priority": 3, "credibility": 0.70},

    # ═══════════════════════════════════════════════════════════════════════
    #  TIER 4 — COMMENTARY + DEEP ANALYSIS (target: every 24h)
    #  Long-form thinkers, institutional accounts, education.
    #  Budget: 36 accts × 1 cycle/day × 30 days = 1 080 calls/month
    # ═══════════════════════════════════════════════════════════════════════
    # -- Geopolitical Commentary --
    "Natsecjeff":      {"name": "NatSecJeff",             "region": "north-america", "category": "defense_analyst",  "priority": 4, "credibility": 0.72},
    "Mr_Andrew_Fox":   {"name": "Andrew Fox",             "region": "global",        "category": "osint_military",   "priority": 4, "credibility": 0.70},
    "DD_Geopolitics":  {"name": "DD Geopolitics",         "region": "global",        "category": "geopolitical_analyst","priority": 4, "credibility": 0.72},
    "GeoPWatch":       {"name": "Geopolitical Watch",     "region": "global",        "category": "geopolitical_analyst","priority": 4, "credibility": 0.70},
    "InsightGL":       {"name": "Insight Global",         "region": "asia",          "category": "geopolitical_analyst","priority": 4, "credibility": 0.68},
    "GIS_Reports":     {"name": "GIS Reports",            "region": "global",        "category": "geopolitical_analyst","priority": 4, "credibility": 0.78},
    "Overton_news":    {"name": "Overton News",           "region": "global",        "category": "breaking_news",    "priority": 4, "credibility": 0.65},
    "weewoono":        {"name": "weewoono",               "region": "global",        "category": "geopolitical_analyst","priority": 4, "credibility": 0.65},
    "HelenHet20":      {"name": "Helen Thompson",         "region": "europe",        "category": "political_economy","priority": 4, "credibility": 0.78},
    "MaxBlumenthal":   {"name": "Max Blumenthal",         "region": "global",        "category": "investigative",    "priority": 4, "credibility": 0.65},
    "mtracey":         {"name": "Michael Tracey",         "region": "global",        "category": "analysis",         "priority": 4, "credibility": 0.62},
    "BenjaminNorton":  {"name": "Ben Norton",             "region": "global",        "category": "investigative",    "priority": 4, "credibility": 0.60},
    "caitoz":          {"name": "Caitlin Johnstone",      "region": "global",        "category": "analysis",         "priority": 4, "credibility": 0.55},
    "TheGrayzoneNews": {"name": "The Grayzone",           "region": "global",        "category": "media",            "priority": 4, "credibility": 0.60},
    "PrometheanActn":  {"name": "Promethean Action",      "region": "north-america", "category": "geopolitical_analyst","priority": 4, "credibility": 0.60},
    # -- Institutional Finance / Macro --
    "RayDalio":        {"name": "Ray Dalio",              "region": "global",        "category": "macro_finance",    "priority": 4, "credibility": 0.82},
    "paulkrugman":     {"name": "Paul Krugman",           "region": "north-america", "category": "economics",        "priority": 4, "credibility": 0.75},
    "TheEconomist":    {"name": "The Economist",          "region": "global",        "category": "economics",        "priority": 4, "credibility": 0.85},
    "YardeniResearch": {"name": "Yardeni Research",       "region": "north-america", "category": "macro_finance",    "priority": 4, "credibility": 0.80},
    "GoldmanSachs":    {"name": "Goldman Sachs",          "region": "global",        "category": "institutional_finance","priority": 4, "credibility": 0.82},
    "CNBC":            {"name": "CNBC",                   "region": "global",        "category": "breaking_market",  "priority": 4, "credibility": 0.72},
    "MarketWatch":     {"name": "MarketWatch",            "region": "north-america", "category": "breaking_market",  "priority": 4, "credibility": 0.72},
    "Nasdaq":          {"name": "Nasdaq",                 "region": "north-america", "category": "institutional_finance","priority": 4, "credibility": 0.80},
    "AswathDamodaran": {"name": "Aswath Damodaran (NYU)", "region": "north-america", "category": "valuation",        "priority": 4, "credibility": 0.85},
    "JustinWolfers":   {"name": "Justin Wolfers",         "region": "north-america", "category": "economics",        "priority": 4, "credibility": 0.78},
    "Steve_Hanke":     {"name": "Steve Hanke",            "region": "global",        "category": "macro_finance",    "priority": 4, "credibility": 0.75},
    # -- Finance Education / Investing --
    "morganhousel":    {"name": "Morgan Housel",          "region": "north-america", "category": "behavioral_finance","priority": 4, "credibility": 0.78},
    "BrianFeroldi":    {"name": "Brian Feroldi",          "region": "north-america", "category": "fundamentals",     "priority": 4, "credibility": 0.72},
    "awealthofcs":     {"name": "Ben Carlson",            "region": "north-america", "category": "investing",        "priority": 4, "credibility": 0.78},
    "Trader_Dante":    {"name": "Trader Dante",           "region": "global",        "category": "trading",          "priority": 4, "credibility": 0.65},
    "DeepakShenoy":    {"name": "Deepak Shenoy",          "region": "asia",          "category": "investing",        "priority": 4, "credibility": 0.70},
    "AndrewLokenauth": {"name": "Andrew Lokenauth",       "region": "north-america", "category": "finance_education","priority": 4, "credibility": 0.65},
    "IanCassel":       {"name": "Ian Cassel",             "region": "north-america", "category": "micro_cap",        "priority": 4, "credibility": 0.70},
    # -- Regional (Asia-Pacific) --
    "elitepredatorss": {"name": "Elite Predators",        "region": "asia",          "category": "geopolitical_analyst","priority": 4, "credibility": 0.58},
    "alpha_defense":   {"name": "Alpha Defense",          "region": "asia",          "category": "defense_analyst",  "priority": 4, "credibility": 0.62},
    "BharatAlphaint":  {"name": "Bharat Alpha",           "region": "asia",          "category": "geopolitical_analyst","priority": 4, "credibility": 0.58},
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
        self._cycle_counter: int = self._load_cycle_counter()

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

        # Log budget estimation
        self._log_budget_estimate()
        logger.info("TwitterFetcher initialised (%d accounts configured).",
                     len(OSINT_ACCOUNTS))

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_configured(self) -> bool:
        """Return True if the fetcher has a working tweepy client."""
        return self._client is not None

    def _log_budget_estimate(self) -> None:
        """Log estimated monthly API call budget for current config."""
        from .config import config as _cfg
        base = max(_cfg.twitter_fetch_interval_minutes, 1)
        cycles_day = (24 * 60) / base
        tier_counts: Dict[int, int] = {}
        for info in OSINT_ACCOUNTS.values():
            t = info["priority"]
            tier_counts[t] = tier_counts.get(t, 0) + 1
        total_day = 0.0
        parts = []
        for tier in sorted(tier_counts):
            target = self._TIER_TARGET_MINUTES.get(tier, 1440)
            skip = max(1, round(target / base))
            calls = tier_counts[tier] * (cycles_day / skip)
            total_day += calls
            parts.append(f"T{tier}({tier_counts[tier]})={calls:.0f}/day")
        total_month = total_day * 30
        summary = ", ".join(parts)
        level = "warning" if total_month > 10000 else "info"
        getattr(logger, level)(
            "X budget estimate @ %dmin interval: %s → "
            "%.0f/day, %.0f/month (X Basic quota: 10K/month %s)",
            base, summary, total_day, total_month,
            "✅" if total_month <= 10000 else f"⚠️ {total_month/10000:.1f}×",
        )

    # ------------------------------------------------------------------
    # Cycle counter persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _load_cycle_counter() -> int:
        """Load persisted cycle counter from DB (survives restarts)."""
        try:
            from .db import get_session, EngineSettings
            with get_session() as session:
                setting = session.query(EngineSettings).get("twitter_cycle_counter")
                if setting:
                    return int(setting.value)
        except Exception as e:
            logger.warning("Failed to load twitter cycle counter: %s", e)
        return 0

    def _save_cycle_counter(self) -> None:
        """Persist cycle counter to DB."""
        try:
            from .db import get_session, EngineSettings
            with get_session() as session:
                setting = session.query(EngineSettings).get("twitter_cycle_counter")
                if setting:
                    setting.value = str(self._cycle_counter)
                else:
                    session.add(EngineSettings(key="twitter_cycle_counter", value=str(self._cycle_counter)))
                session.commit()
        except Exception as e:
            logger.warning("Failed to persist twitter cycle counter: %s", e)

    # ------------------------------------------------------------------
    # Adaptive priority scheduling
    # ------------------------------------------------------------------

    # Target polling interval per tier (in minutes).
    # The actual cycle-skip count is auto-calculated from the base interval.
    _TIER_TARGET_MINUTES = {1: 30, 2: 120, 3: 360, 4: 1440}

    # Monthly quota auto-throttle settings
    _MONTHLY_QUOTA = 10_000      # X Basic plan
    _THROTTLE_WARN_PCT = 0.70    # At 70% → skip T4
    _THROTTLE_HARD_PCT = 0.85    # At 85% → skip T3+T4
    _THROTTLE_CRIT_PCT = 0.95    # At 95% → only T1

    def _get_monthly_calls(self) -> int:
        """Count X API calls this calendar month (from DB)."""
        try:
            from .db import get_session, Article
            from datetime import date
            session = get_session()
            try:
                first_of_month = datetime(datetime.utcnow().year, datetime.utcnow().month, 1)
                count = session.query(
                    func.count(func.distinct(Article.source))
                ).filter(
                    Article.source.like("X/Twitter%"),
                    Article.fetched_at >= first_of_month,
                ).scalar() or 0
                # Each unique source fetched = ~1 API call per occurrence
                # Better metric: count distinct (source, date) pairs
                from sqlalchemy import cast, Date
                calls = session.query(
                    func.count()
                ).select_from(
                    session.query(
                        Article.source,
                        cast(Article.fetched_at, Date),
                    ).filter(
                        Article.source.like("X/Twitter%"),
                        Article.fetched_at >= first_of_month,
                    ).group_by(
                        Article.source,
                        func.strftime("%Y-%m-%d %H", Article.fetched_at),
                    ).subquery()
                ).scalar() or 0
                return calls
            finally:
                session.close()
        except Exception:
            return 0

    def _should_fetch(self, priority: int) -> bool:
        """Decide whether an account at *priority* tier runs this cycle.

        Uses **target polling intervals** per tier so the system auto-adapts
        to whatever base interval is configured (30 min, 60 min, 120 min…).
        Also applies **auto-throttle** when monthly quota usage is high.

            Tier 1 — target  30 min  (breaking OSINT/markets)
            Tier 2 — target 120 min  (fast OSINT + market flow)
            Tier 3 — target 360 min  (analysts + institutional, ~6h)
            Tier 4 — target 1440 min (commentary + education, ~24h)
        """
        from .config import config as _cfg
        target = self._TIER_TARGET_MINUTES.get(priority, 1440)
        base = max(_cfg.twitter_fetch_interval_minutes, 1)
        skip = max(1, round(target / base))

        if self._cycle_counter % skip != 0:
            return False

        # Auto-throttle check (only run DB query once per cycle for T3/T4)
        if priority >= 3 and hasattr(self, "_throttle_level"):
            if priority == 4 and self._throttle_level >= 1:
                return False  # Skip T4 at 70%+ quota
            if priority == 3 and self._throttle_level >= 2:
                return False  # Skip T3 at 85%+ quota
        if priority >= 2 and hasattr(self, "_throttle_level"):
            if self._throttle_level >= 3:
                return False  # Only T1 at 95%+ quota

        return True

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
                # Strip timezone info for consistency with naive UTC datetimes used elsewhere
                pub_date = (
                    tweet.created_at.replace(tzinfo=None) if tweet.created_at else datetime.utcnow()
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

        # --- 0. auto-throttle check ---
        monthly_calls = self._get_monthly_calls()
        quota_pct = monthly_calls / self._MONTHLY_QUOTA if self._MONTHLY_QUOTA else 0
        if quota_pct >= self._THROTTLE_CRIT_PCT:
            self._throttle_level = 3
            logger.warning("X auto-throttle: CRITICAL (%.0f%% quota) — T1 only.", quota_pct * 100)
        elif quota_pct >= self._THROTTLE_HARD_PCT:
            self._throttle_level = 2
            logger.warning("X auto-throttle: HARD (%.0f%% quota) — T1+T2 only.", quota_pct * 100)
        elif quota_pct >= self._THROTTLE_WARN_PCT:
            self._throttle_level = 1
            logger.info("X auto-throttle: SOFT (%.0f%% quota) — skipping T4.", quota_pct * 100)
        else:
            self._throttle_level = 0

        # --- 1. advance cycle ---
        self._cycle_counter += 1
        self._save_cycle_counter()

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
