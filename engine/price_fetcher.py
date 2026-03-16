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

    FEAR_GREED_TTL = 1800   # 30 minutes
    OPTIONS_TTL = 900       # 15 minutes
    ECONOMIC_TTL = 3600     # 1 hour

    MAX_CACHE_SIZE = 200  # Max entries per cache dict

    def __init__(self) -> None:
        import threading
        self._lock = threading.Lock()
        self._quote_cache: Dict[str, Tuple[float, Dict]] = {}
        self._chart_cache: Dict[str, Tuple[float, List]] = {}
        self._indicator_cache: Optional[Tuple[float, Dict]] = None
        self._fg_cache: Optional[Tuple[float, Dict]] = None
        self._options_cache: Dict[str, Tuple[float, Dict]] = {}
        self._econ_cache: Optional[Tuple[float, Dict]] = None
        self._commodity_ext_cache: Optional[Tuple[float, Dict]] = None
        self._sector_cache_data: Optional[Tuple[float, Dict]] = None

    # ------------------------------------------------------------------
    # Cache eviction
    # ------------------------------------------------------------------
    def _evict_stale(self, cache: dict, ttl: float) -> None:
        """Remove expired entries from a cache dict. Must be called with self._lock held."""
        now = time.time()
        expired = [k for k, (ts, _) in cache.items() if now - ts >= ttl]
        for k in expired:
            del cache[k]
        # If still over limit, remove oldest entries
        if len(cache) > self.MAX_CACHE_SIZE:
            sorted_keys = sorted(cache.keys(), key=lambda k: cache[k][0])
            for k in sorted_keys[:len(cache) - self.MAX_CACHE_SIZE]:
                del cache[k]

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

        # --- cache check (thread-safe) ---
        with self._lock:
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
            with self._lock:
                self._evict_stale(self._quote_cache, self.QUOTE_TTL)
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

        # --- cache check (thread-safe) ---
        with self._lock:
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

            with self._lock:
                self._evict_stale(self._chart_cache, self.CHART_TTL)
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

        # --- cache check (thread-safe) ---
        with self._lock:
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

        with self._lock:
            self._indicator_cache = (now, result)
        return result

    # ------------------------------------------------------------------
    # Fear & Greed Index (CNN — free, no API key)
    # ------------------------------------------------------------------
    def get_fear_greed(self) -> Dict[str, Any]:
        """Fetch CNN Fear & Greed Index (0-100 scale).

        Returns: {"score": int, "label": str, "previous_close": int}
        On failure: {"error": "..."}
        """
        now = time.time()
        with self._lock:
            if self._fg_cache is not None:
                ts, data = self._fg_cache
                if now - ts < self.FEAR_GREED_TTL:
                    return data

        try:
            import httpx

            resp = httpx.get(
                "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/",
                timeout=10,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                    "Referer": "https://www.cnn.com/markets/fear-and-greed",
                },
            )
            resp.raise_for_status()
            raw = resp.json()

            fg = raw.get("fear_and_greed", {})
            score = int(fg.get("score", 50))
            rating = fg.get("rating", "Neutral")
            previous = int(fg.get("previous_close", score))

            result: Dict[str, Any] = {
                "score": score,
                "label": rating,
                "previous_close": previous,
            }
            with self._lock:
                self._fg_cache = (now, result)
            return result
        except Exception as exc:
            logger.debug("Fear & Greed fetch failed: %s", exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # Options summary (yfinance — no API key)
    # ------------------------------------------------------------------
    def get_options_summary(self, ticker: str) -> Dict[str, Any]:
        """Get options-derived signals for a ticker.

        Returns: implied_vol_call, implied_vol_put, put_call_ratio, near_expiry
        On failure: {"error": "..."}
        """
        ticker = ticker.upper().strip()
        cache_key = f"opts:{ticker}"
        now = time.time()

        with self._lock:
            if cache_key in self._options_cache:
                ts, data = self._options_cache[cache_key]
                if now - ts < self.OPTIONS_TTL:
                    return data

        try:
            import yfinance as yf

            t = yf.Ticker(ticker)
            expirations = t.options
            if not expirations:
                return {"error": "No options data available"}

            # Use nearest expiry
            nearest = expirations[0]
            chain = t.option_chain(nearest)

            calls = chain.calls
            puts = chain.puts

            # Average implied volatility (filter zero/NaN)
            call_ivs = calls["impliedVolatility"].dropna()
            call_ivs = call_ivs[call_ivs > 0]
            put_ivs = puts["impliedVolatility"].dropna()
            put_ivs = put_ivs[put_ivs > 0]

            avg_call_iv = float(call_ivs.mean()) if len(call_ivs) > 0 else None
            avg_put_iv = float(put_ivs.mean()) if len(put_ivs) > 0 else None

            # Put/call ratio by volume
            call_vol = float(calls["volume"].sum()) if "volume" in calls else 0
            put_vol = float(puts["volume"].sum()) if "volume" in puts else 0
            pc_ratio = round(put_vol / call_vol, 2) if call_vol > 0 else None

            result: Dict[str, Any] = {
                "implied_vol_call": round(avg_call_iv, 4) if avg_call_iv else None,
                "implied_vol_put": round(avg_put_iv, 4) if avg_put_iv else None,
                "put_call_ratio": pc_ratio,
                "near_expiry": nearest,
            }

            with self._lock:
                self._evict_stale(self._options_cache, self.OPTIONS_TTL)
                self._options_cache[cache_key] = (now, result)
            return result

        except Exception as exc:
            logger.debug("Options summary failed for %s: %s", ticker, exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # Economic indicators (FRED — optional, free API key)
    # ------------------------------------------------------------------
    def get_economic_indicators(self) -> Dict[str, Any]:
        """Fetch key economic indicators from FRED.

        Requires FRED_API_KEY env var or config.  Returns gracefully if
        fredapi is not installed or key is not configured.

        Returns: treasury_10y, treasury_2y, yield_spread, fed_funds_rate
        On failure: {"error": "..."}
        """
        now = time.time()
        if self._econ_cache is not None:
            ts, data = self._econ_cache
            if now - ts < self.ECONOMIC_TTL:
                return data

        try:
            from .config import config
            fred_key = config.fred_api_key
            if not fred_key:
                return {"error": "FRED API key not configured"}

            from fredapi import Fred
            fred = Fred(api_key=fred_key)

            t10y = fred.get_series("DGS10").dropna().iloc[-1]
            t2y = fred.get_series("DGS2").dropna().iloc[-1]
            ffr = fred.get_series("FEDFUNDS").dropna().iloc[-1]

            result: Dict[str, Any] = {
                "treasury_10y": round(float(t10y), 2),
                "treasury_2y": round(float(t2y), 2),
                "yield_spread": round(float(t10y - t2y), 2),
                "fed_funds_rate": round(float(ffr), 2),
            }

            self._econ_cache = (now, result)
            return result

        except ImportError:
            return {"error": "fredapi not installed"}
        except Exception as exc:
            logger.debug("FRED fetch failed: %s", exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # Extended commodities: Silver + Copper futures
    # ------------------------------------------------------------------
    COMMODITY_EXT_TTL = 900  # 15 minutes

    def get_commodities_extended(self) -> Dict[str, Any]:
        """Fetch silver (SI=F) and copper (HG=F) prices via yfinance futures.

        Returns::

            {
                "silver": {"price": 33.45, "change_pct": -0.8, "currency": "USD"},
                "copper": {"price": 4.12, "change_pct": 1.2, "currency": "USD"},
                "fetched_at": "..."
            }
        """
        now = time.time()
        cache_key = "_commodities_ext"
        if self._commodity_ext_cache is not None:
            ts, data = self._commodity_ext_cache
            if now - ts < self.COMMODITY_EXT_TTL:
                return data

        result: Dict[str, Any] = {"fetched_at": datetime.now(timezone.utc).isoformat()}

        for name, ticker in [("silver", "SI=F"), ("copper", "HG=F")]:
            try:
                q = self.get_quote(ticker)
                if "error" not in q:
                    result[name] = {
                        "price": q["price"],
                        "change_pct": q.get("change_pct", 0.0),
                        "currency": q.get("currency", "USD"),
                    }
                else:
                    result[name] = {"price": None, "change_pct": None, "error": q["error"]}
            except Exception as exc:
                result[name] = {"price": None, "change_pct": None, "error": str(exc)}

        self._commodity_ext_cache = (now, result)
        return result

    # ------------------------------------------------------------------
    # Sector snapshot: Defense (ITA) + Energy (XLE) + defense movers
    # ------------------------------------------------------------------
    SECTOR_TTL = 900  # 15 minutes

    def get_sector_snapshot(self) -> Dict[str, Any]:
        """Fetch defense & energy sector ETFs with 2-week trends, plus top defense movers.

        Returns::

            {
                "defense_etf": {"ticker": "ITA", "price": 142.3, "change_pct": 1.1,
                                "trend_2wk": "up", "change_2wk_pct": 3.2},
                "energy_etf":  {"ticker": "XLE", ...},
                "defense_movers": [
                    {"ticker": "LMT", "change_2wk_pct": 4.1},
                    {"ticker": "RTX", "change_2wk_pct": 2.8},
                    {"ticker": "NOC", "change_2wk_pct": 3.5},
                ],
            }
        """
        now = time.time()
        if self._sector_cache_data is not None:
            ts, data = self._sector_cache_data
            if now - ts < self.SECTOR_TTL:
                return data

        result: Dict[str, Any] = {}

        def _etf_data(ticker: str) -> Dict[str, Any]:
            q = self.get_quote(ticker)
            entry: Dict[str, Any] = {
                "ticker": ticker,
                "price": q.get("price"),
                "change_pct": q.get("change_pct", 0.0),
            }
            # 2-week trend from chart data
            candles = self.get_chart_data(ticker, period="1mo")
            if len(candles) >= 10:
                close_10d = candles[-10]["close"]
                close_now = candles[-1]["close"]
                pct = round(((close_now - close_10d) / close_10d) * 100, 1) if close_10d else 0
                entry["change_2wk_pct"] = pct
                entry["trend_2wk"] = "up" if pct > 1.5 else ("down" if pct < -1.5 else "flat")
            else:
                entry["change_2wk_pct"] = 0
                entry["trend_2wk"] = "unknown"
            return entry

        # Sector ETFs
        try:
            result["defense_etf"] = _etf_data("ITA")
        except Exception:
            result["defense_etf"] = {"ticker": "ITA", "price": None, "error": "unavailable"}

        try:
            result["energy_etf"] = _etf_data("XLE")
        except Exception:
            result["energy_etf"] = {"ticker": "XLE", "price": None, "error": "unavailable"}

        # Top defense movers (2-week change)
        movers = []
        for ticker in ("LMT", "RTX", "NOC"):
            try:
                candles = self.get_chart_data(ticker, period="1mo")
                if len(candles) >= 10:
                    close_10d = candles[-10]["close"]
                    close_now = candles[-1]["close"]
                    pct = round(((close_now - close_10d) / close_10d) * 100, 1) if close_10d else 0
                    movers.append({"ticker": ticker, "change_2wk_pct": pct})
            except Exception:
                pass
        result["defense_movers"] = movers

        self._sector_cache_data = (now, result)
        return result

    # ------------------------------------------------------------------
    # Ticker momentum: 2-week SMA crossover + volume trend
    # ------------------------------------------------------------------
    MOMENTUM_TTL = 1800  # 30 minutes

    def get_ticker_momentum(self, tickers: List[str], max_tickers: int = 5) -> Dict[str, Dict[str, Any]]:
        """Calculate 2-week momentum for a list of tickers.

        For each ticker (max 5) returns: change_2wk_pct, sma_signal
        (bullish/bearish/neutral), volume_trend (above/below/normal), price.

        Returns: {"XOM": {...}, "LMT": {...}, ...}
        """
        now = time.time()
        result: Dict[str, Dict[str, Any]] = {}

        for ticker in tickers[:max_tickers]:
            ticker = ticker.upper().strip()

            # Per-ticker cache
            cache_key = f"_mom:{ticker}"
            if cache_key in self._quote_cache:
                ts, data = self._quote_cache[cache_key]
                if now - ts < self.MOMENTUM_TTL:
                    result[ticker] = data
                    continue

            try:
                candles = self.get_chart_data(ticker, period="1mo")
                if len(candles) < 10:
                    continue

                closes = [c["close"] for c in candles]
                volumes = [c["volume"] for c in candles]

                # 2-week change
                close_10d = closes[-10]
                close_now = closes[-1]
                change_2wk = round(((close_now - close_10d) / close_10d) * 100, 1) if close_10d else 0

                # SMA5 vs SMA10 crossover
                sma5 = sum(closes[-5:]) / 5
                sma10 = sum(closes[-10:]) / 10
                if sma5 > sma10 * 1.005:
                    sma_signal = "bullish"
                elif sma5 < sma10 * 0.995:
                    sma_signal = "bearish"
                else:
                    sma_signal = "neutral"

                # Volume trend: last 2 days avg vs 10-day avg
                vol_recent = sum(volumes[-2:]) / 2 if len(volumes) >= 2 else 0
                vol_10d = sum(volumes[-10:]) / 10 if len(volumes) >= 10 else 1
                if vol_10d > 0 and vol_recent > vol_10d * 1.3:
                    vol_trend = "above"
                elif vol_10d > 0 and vol_recent < vol_10d * 0.7:
                    vol_trend = "below"
                else:
                    vol_trend = "normal"

                entry = {
                    "price": close_now,
                    "change_2wk_pct": change_2wk,
                    "sma_signal": sma_signal,
                    "volume_trend": vol_trend,
                }
                result[ticker] = entry
                self._quote_cache[cache_key] = (now, entry)

            except Exception as exc:
                logger.debug("Momentum calc failed for %s: %s", ticker, exc)

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
