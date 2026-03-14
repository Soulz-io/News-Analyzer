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
        """Fetch current BTC/EUR price via CoinGecko free API.

        Falls back to yfinance ``BTC-EUR`` if CoinGecko is unavailable.
        Returns: ``{"price": float, "change_24h_pct": float,
        "currency": "EUR"}``.
        """
        # --- try CoinGecko first ---
        try:
            import httpx

            url = (
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=bitcoin&vs_currencies=eur&include_24hr_change=true"
            )
            logger.info("Fetching Bitcoin price from CoinGecko")
            resp = httpx.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            btc = data["bitcoin"]
            result: Dict[str, Any] = {
                "price": float(btc["eur"]),
                "change_24h_pct": round(float(btc.get("eur_24h_change", 0)), 2),
                "currency": "EUR",
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

            logger.info("Fetching Bitcoin price from yfinance (BTC-EUR)")
            t = yf.Ticker("BTC-EUR")
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
                "currency": "EUR",
            }

        except Exception as yf_exc:
            logger.error("Bitcoin price fetch failed entirely: %s", yf_exc)
            return {"error": f"Failed to fetch Bitcoin price: {yf_exc}"}

    # ------------------------------------------------------------------
    # Gold price in EUR per kilogram
    # ------------------------------------------------------------------
    def get_gold_price(self) -> Dict[str, Any]:
        """Get gold price in EUR per kilogram.

        Tries CoinGecko first, then falls back to yfinance Gold Futures
        (GC=F) converted from USD/oz to EUR/kg using the EUR/USD rate.

        Returns: ``{"price": float, "change_pct": float,
        "currency": "EUR", "unit": "kg"}``.
        """
        # 1 kg = 32.1507 troy ounces
        OZ_PER_KG = 32.1507

        # --- try CoinGecko first ---
        try:
            import httpx

            url = (
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=gold&vs_currencies=eur&include_24hr_change=true"
            )
            logger.info("Fetching gold price from CoinGecko")
            resp = httpx.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            gold = data["gold"]
            # CoinGecko gold price is per troy ounce; convert to per kg
            price_eur_per_oz = float(gold["eur"])
            price_eur_per_kg = round(price_eur_per_oz * OZ_PER_KG, 2)
            change_pct = round(float(gold.get("eur_24h_change", 0)), 2)

            return {
                "price": price_eur_per_kg,
                "change_pct": change_pct,
                "currency": "EUR",
                "unit": "kg",
            }

        except Exception as cg_exc:
            logger.warning(
                "CoinGecko gold request failed (%s); falling back to yfinance",
                cg_exc,
            )

        # --- yfinance fallback: GC=F (USD/oz) + EURUSD=X ---
        try:
            import yfinance as yf

            logger.info("Fetching gold price from yfinance (GC=F + EURUSD=X)")

            # Gold futures in USD per troy ounce
            gold_ticker = yf.Ticker("GC=F")
            gold_info = gold_ticker.fast_info
            gold_price_usd = float(gold_info["lastPrice"])
            gold_prev_close = float(gold_info["previousClose"])

            # EUR/USD exchange rate
            eur_ticker = yf.Ticker("EURUSD=X")
            eur_info = eur_ticker.fast_info
            eurusd_rate = float(eur_info["lastPrice"])

            # Convert: USD per oz -> EUR per kg
            price_eur_per_kg = round((gold_price_usd * OZ_PER_KG) / eurusd_rate, 2)

            # Calculate change % from gold futures
            change_pct = (
                round(((gold_price_usd - gold_prev_close) / gold_prev_close) * 100, 2)
                if gold_prev_close
                else 0.0
            )

            return {
                "price": price_eur_per_kg,
                "change_pct": change_pct,
                "currency": "EUR",
                "unit": "kg",
            }

        except Exception as yf_exc:
            logger.error("Gold price fetch failed entirely: %s", yf_exc)
            return {"error": f"Failed to fetch gold price: {yf_exc}"}

    # ------------------------------------------------------------------
    # Oil price (WTI Crude Futures)
    # ------------------------------------------------------------------
    def get_oil_price(self) -> Dict[str, Any]:
        """Get WTI Crude Oil price via yfinance (CL=F).

        Returns the same shape as :meth:`get_quote`.
        """
        quote = self.get_quote("CL=F")
        if "error" in quote:
            return quote
        quote["name"] = "Oil (WTI)"
        return quote

    # ------------------------------------------------------------------
    # Combined market indicators (BTC + Gold + Oil + VIX)
    # ------------------------------------------------------------------
    def get_market_indicators(self) -> Dict[str, Any]:
        """Fetch BTC, Gold, Oil, and VIX in one call.

        Returns::

            {
                "bitcoin": {"price": ..., "change_pct": ..., "currency": "EUR"},
                "gold":    {"price": ..., "change_pct": ..., "currency": "EUR", "unit": "kg"},
                "oil":     {"price": ..., "change_pct": ..., "currency": "USD"},
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
            bitcoin = {"price": None, "change_pct": None, "currency": "EUR"}
        else:
            bitcoin = {
                "price": btc_raw["price"],
                "change_pct": btc_raw.get("change_24h_pct", 0.0),
                "currency": btc_raw["currency"],
            }

        # -- Gold (EUR/kg) --
        gold_raw = self.get_gold_price()
        if "error" in gold_raw:
            gold = {"price": None, "change_pct": None, "currency": "EUR", "unit": "kg"}
        else:
            gold = {
                "price": gold_raw["price"],
                "change_pct": gold_raw.get("change_pct", 0.0),
                "currency": gold_raw.get("currency", "EUR"),
                "unit": gold_raw.get("unit", "kg"),
            }

        # -- Oil (WTI Crude) --
        oil_raw = self.get_oil_price()
        if "error" in oil_raw:
            oil = {"price": None, "change_pct": None, "currency": "USD"}
        else:
            oil = {
                "price": oil_raw["price"],
                "change_pct": oil_raw.get("change_pct", 0.0),
                "currency": oil_raw.get("currency", "USD"),
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
            "oil": oil,
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
