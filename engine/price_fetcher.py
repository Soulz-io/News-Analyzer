"""Stock/ETF price fetching via yfinance + Bitcoin via CoinGecko API.

Provides on-demand price data with in-memory TTL caching.
Zero API key required -- uses free public APIs only.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


class PriceFetcher:
    """Fetches live price data for stocks, ETFs, and crypto.

    Uses yfinance for equities and CoinGecko for Bitcoin.  All results
    are cached in-memory with configurable TTL to avoid hammering
    upstream APIs on repeated calls.
    """

    # ------------------------------------------------------------------
    # TTL constants (seconds)
    # ------------------------------------------------------------------
    QUOTE_TTL = 300       # 5 minutes
    CHART_TTL = 900       # 15 minutes
    INDICATOR_TTL = 300   # 5 minutes

    def __init__(self) -> None:
        self._quote_cache: Dict[str, Tuple[float, Dict]] = {}
        self._chart_cache: Dict[str, Tuple[float, List]] = {}
        self._indicator_cache: Optional[Tuple[float, Dict]] = None

    # ------------------------------------------------------------------
    # Quote (single ticker)
    # ------------------------------------------------------------------
    def get_quote(self, ticker: str) -> Dict[str, Any]:
        """Get current price and 24-hour change for *ticker*.

        Returns a dict with keys: ticker, price, previous_close,
        change_abs, change_pct, currency, name.  On failure the dict
        contains a single ``error`` key instead.
        """
        ticker = ticker.upper().strip()
        now = time.time()

        # --- cache check ---
        if ticker in self._quote_cache:
            ts, data = self._quote_cache[ticker]
            if now - ts < self.QUOTE_TTL:
                logger.debug("Quote cache HIT for %s", ticker)
                return data
            logger.debug("Quote cache EXPIRED for %s", ticker)

        logger.info("Fetching quote for %s via yfinance", ticker)
        try:
            import yfinance as yf

            t = yf.Ticker(ticker)
            info = t.fast_info

            current_price = float(info["lastPrice"])
            previous_close = float(info["previousClose"])
            change_abs = round(current_price - previous_close, 2)
            change_pct = (
                round((change_abs / previous_close) * 100, 2)
                if previous_close
                else 0.0
            )

            # Attempt to get the human-readable name; fast_info may not
            # have it, so fall back gracefully.
            name = ticker
            try:
                name = t.info.get("shortName") or t.info.get("longName") or ticker
            except Exception:
                pass

            currency = "USD"
            try:
                currency = info.get("currency", "USD") or "USD"
            except Exception:
                pass

            result: Dict[str, Any] = {
                "ticker": ticker,
                "price": round(current_price, 2),
                "previous_close": round(previous_close, 2),
                "change_abs": change_abs,
                "change_pct": change_pct,
                "currency": currency,
                "name": name,
            }
            self._quote_cache[ticker] = (now, result)
            return result

        except Exception as exc:
            logger.error("Failed to fetch quote for %s: %s", ticker, exc)
            return {"error": f"Failed to fetch quote for {ticker}: {exc}"}

    # ------------------------------------------------------------------
    # Chart / OHLCV candle data
    # ------------------------------------------------------------------
    def get_chart_data(
        self, ticker: str, period: str = "3mo"
    ) -> List[Dict[str, Any]]:
        """Get daily OHLCV candle data for *ticker*.

        Returns a list of dicts suitable for TradingView Lightweight
        Charts: ``[{"time": "2024-01-15", "open", "high", "low",
        "close", "volume"}, ...]``.

        On failure an empty list is returned.
        """
        ticker = ticker.upper().strip()
        cache_key = f"{ticker}:{period}"
        now = time.time()

        # --- cache check ---
        if cache_key in self._chart_cache:
            ts, data = self._chart_cache[cache_key]
            if now - ts < self.CHART_TTL:
                logger.debug("Chart cache HIT for %s", cache_key)
                return data
            logger.debug("Chart cache EXPIRED for %s", cache_key)

        logger.info("Fetching chart data for %s (period=%s)", ticker, period)
        try:
            import yfinance as yf

            hist = yf.Ticker(ticker).history(period=period, interval="1d")

            if hist.empty:
                logger.warning("No chart data returned for %s", ticker)
                return []

            candles: List[Dict[str, Any]] = []
            for idx, row in hist.iterrows():
                candles.append(
                    {
                        "time": idx.strftime("%Y-%m-%d"),
                        "open": round(float(row["Open"]), 2),
                        "high": round(float(row["High"]), 2),
                        "low": round(float(row["Low"]), 2),
                        "close": round(float(row["Close"]), 2),
                        "volume": int(row["Volume"]),
                    }
                )

            self._chart_cache[cache_key] = (now, candles)
            return candles

        except Exception as exc:
            logger.error("Failed to fetch chart data for %s: %s", ticker, exc)
            return []

    # ------------------------------------------------------------------
    # Bitcoin price (CoinGecko primary, yfinance fallback)
    # ------------------------------------------------------------------
    def get_bitcoin_price(self) -> Dict[str, Any]:
        """Fetch current BTC/USD price via CoinGecko free API.

        Falls back to yfinance ``BTC-USD`` if CoinGecko is unavailable.
        Returns: ``{"price": float, "change_24h_pct": float,
        "currency": "USD"}``.
        """
        # --- try CoinGecko first ---
        try:
            import httpx

            url = (
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
            )
            logger.info("Fetching Bitcoin price from CoinGecko")
            resp = httpx.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            btc = data["bitcoin"]
            result: Dict[str, Any] = {
                "price": float(btc["usd"]),
                "change_24h_pct": round(float(btc.get("usd_24h_change", 0)), 2),
                "currency": "USD",
            }
            return result

        except Exception as cg_exc:
            logger.warning(
                "CoinGecko request failed (%s); falling back to yfinance",
                cg_exc,
            )

        # --- yfinance fallback ---
        try:
            import yfinance as yf

            logger.info("Fetching Bitcoin price from yfinance (BTC-USD)")
            t = yf.Ticker("BTC-USD")
            info = t.fast_info

            current_price = float(info["lastPrice"])
            previous_close = float(info["previousClose"])
            change_pct = (
                round(((current_price - previous_close) / previous_close) * 100, 2)
                if previous_close
                else 0.0
            )

            return {
                "price": current_price,
                "change_24h_pct": change_pct,
                "currency": "USD",
            }

        except Exception as yf_exc:
            logger.error("Bitcoin price fetch failed entirely: %s", yf_exc)
            return {"error": f"Failed to fetch Bitcoin price: {yf_exc}"}

    # ------------------------------------------------------------------
    # Gold price (via GLD ETF)
    # ------------------------------------------------------------------
    def get_gold_price(self) -> Dict[str, Any]:
        """Get gold price via the GLD ETF.

        Returns the same shape as :meth:`get_quote` but with the name
        set to ``"Gold (GLD)"``.
        """
        quote = self.get_quote("GLD")
        if "error" in quote:
            return quote
        quote["name"] = "Gold (GLD)"
        return quote

    # ------------------------------------------------------------------
    # Combined market indicators (BTC + Gold + VIX)
    # ------------------------------------------------------------------
    def get_market_indicators(self) -> Dict[str, Any]:
        """Fetch BTC, Gold, and VIX in one call.

        Returns::

            {
                "bitcoin": {"price": ..., "change_pct": ..., "currency": ...},
                "gold":    {"price": ..., "change_pct": ..., "currency": ...},
                "vix":     {"price": ..., "change_pct": ..., "currency": ...},
                "fetched_at": "2024-06-15T12:34:56+00:00",
            }
        """
        now = time.time()

        # --- cache check ---
        if self._indicator_cache is not None:
            ts, data = self._indicator_cache
            if now - ts < self.INDICATOR_TTL:
                logger.debug("Indicator cache HIT")
                return data
            logger.debug("Indicator cache EXPIRED")

        logger.info("Fetching combined market indicators")

        # -- Bitcoin --
        btc_raw = self.get_bitcoin_price()
        if "error" in btc_raw:
            bitcoin = {"price": None, "change_pct": None, "currency": "USD"}
        else:
            bitcoin = {
                "price": btc_raw["price"],
                "change_pct": btc_raw.get("change_24h_pct", 0.0),
                "currency": btc_raw["currency"],
            }

        # -- Gold (GLD) --
        gold_raw = self.get_quote("GLD")
        if "error" in gold_raw:
            gold = {"price": None, "change_pct": None, "currency": "USD"}
        else:
            gold = {
                "price": gold_raw["price"],
                "change_pct": gold_raw.get("change_pct", 0.0),
                "currency": gold_raw.get("currency", "USD"),
            }

        # -- VIX --
        vix_raw = self.get_quote("^VIX")
        if "error" in vix_raw:
            vix = {"price": None, "change_pct": None, "currency": "USD"}
        else:
            vix = {
                "price": vix_raw["price"],
                "change_pct": vix_raw.get("change_pct", 0.0),
                "currency": vix_raw.get("currency", "USD"),
            }

        result: Dict[str, Any] = {
            "bitcoin": bitcoin,
            "gold": gold,
            "vix": vix,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        self._indicator_cache = (now, result)
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_fetcher: Optional[PriceFetcher] = None


def get_price_fetcher() -> PriceFetcher:
    """Return (and lazily create) the module-level PriceFetcher singleton."""
    global _fetcher
    if _fetcher is None:
        _fetcher = PriceFetcher()
    return _fetcher
