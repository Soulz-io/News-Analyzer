"""BUNQ Stocks whitelist — curated list of stocks/ETFs available on bunq.

bunq Stocks uses Ginmon (broker) + Upvest (principal broker & custodian)
executing on Tradegate, Xetra, Quotrix, Gettex, and Lang & Schwarz.

Because all execution happens on German/EU exchanges, only instruments with
a European listing are available.  Pure US-listed commodity ETFs (USO, GLD,
SLV) and the VIX index are NOT available to European retail investors
(MiFID II / PRIIPs KID requirement).

bunq offers ~400+ curated popular US & EU large-caps and ETFs.
This list is compiled from bunq's public communications, user-confirmed
holdings, and the typical Upvest/Ginmon universe.

Source: https://press.bunq.com/241839-bunq-lanceert-beleggingsproduct/
Partner info: https://upvest.co/blog/bunq-ginmon-and-upvest-join-forces-to-launch-investment-products
Upvest docs: https://docs.upvest.co/products/tol/guides/instruments/concept
"""

# ── Confirmed by user (actually in their bunq portfolio) ─────────────
CONFIRMED = {
    "AAPL", "TSLA", "NVDA", "META", "MSFT", "HPQ", "V",
    # User-confirmed holdings (March 2026):
    "XOM", "IS0D.DE", "WMIN.DE", "IS0E.DE", "ISPA.DE",
}

# ── NOT available on bunq (verified) ─────────────────────────────────
# These are US-only instruments that lack EU cross-listings required
# by Upvest's execution venues.
NOT_AVAILABLE = {
    "USO",   # United States Oil Fund — US-only commodity ETF
    "GLD",   # SPDR Gold Shares — US-only (use IGLN.L / EWG2.DE instead)
    "SLV",   # iShares Silver Trust — US-only (use ISLN.L instead)
    "VIX",   # CBOE Volatility Index — not a tradeable security
    "GDX",   # VanEck Gold Miners — US-only (use IS0E.DE instead)
    "XOP",   # SPDR Oil & Gas E&P — US-only
    "HDV",   # iShares Core High Dividend — US-only (use VHYL.AS)
    "SCHD",  # Schwab US Dividend — US-only
    # US-domiciled ETFs — no UCITS KID, blocked for EU retail under MiFID II/PRIIPs
    "XLE",   # Energy Select SPDR — use IS0D.DE instead
    "XLF",   # Financial Select SPDR
    "XLV",   # Health Care Select SPDR
    "XLK",   # Technology Select SPDR
    "XLI",   # Industrial Select SPDR
    "ITA",   # iShares US Aerospace & Defense
    "VNQ",   # Vanguard Real Estate ETF
    "EEM",   # iShares MSCI EM — use IEMA.AS instead
    "EFA",   # iShares MSCI EAFE
    "FXI",   # iShares China Large-Cap
    "TLT",   # iShares 20+ Year Treasury
    "HYG",   # iShares High Yield Corporate Bond
    "SPY",   # SPDR S&P 500 — use CSPX.AS instead
    "QQQ",   # Invesco QQQ — use EQQQ.DE instead
    "VTI",   # Vanguard Total Stock Market — use IWDA.AS instead
    "VGK",   # Vanguard FTSE Europe — use VEUR.AS instead
}

# ── Full whitelist ───────────────────────────────────────────────────
BUNQ_STOCKS: dict[str, str] = {
    # ═══ US MEGA-CAPS (confirmed + very likely) ═══
    "AAPL":  "Apple",
    "MSFT":  "Microsoft",
    "AMZN":  "Amazon",
    "GOOGL": "Alphabet (Google)",
    "META":  "Meta Platforms",
    "NVDA":  "NVIDIA",
    "TSLA":  "Tesla",
    "BRK.B": "Berkshire Hathaway",
    "V":     "Visa",
    "MA":    "Mastercard",
    "JPM":   "JPMorgan Chase",
    "JNJ":   "Johnson & Johnson",
    "UNH":   "UnitedHealth Group",
    "XOM":   "Exxon Mobil",
    "CVX":   "Chevron",
    "PG":    "Procter & Gamble",
    "HD":    "Home Depot",
    "KO":    "Coca-Cola",
    "PEP":   "PepsiCo",
    "MRK":   "Merck & Co",
    "ABBV":  "AbbVie",
    "PFE":   "Pfizer",
    "COST":  "Costco",
    "WMT":   "Walmart",
    "DIS":   "Walt Disney",
    "NFLX":  "Netflix",
    "CRM":   "Salesforce",
    "AMD":   "Advanced Micro Devices",
    "INTC":  "Intel",
    "CSCO":  "Cisco Systems",
    "BA":    "Boeing",
    "CAT":   "Caterpillar",
    "GE":    "GE Aerospace",
    "NKE":   "Nike",
    "MCD":   "McDonald's",
    "ABT":   "Abbott Laboratories",
    "TMO":   "Thermo Fisher Scientific",
    "AVGO":  "Broadcom",
    "ORCL":  "Oracle",
    "ADBE":  "Adobe",
    "QCOM":  "Qualcomm",
    "TXN":   "Texas Instruments",
    "PYPL":  "PayPal",
    "SQ":    "Block (Square)",
    "UBER":  "Uber Technologies",
    "ABNB":  "Airbnb",
    "HPQ":   "HP Inc",
    "IBM":   "IBM",
    "GS":    "Goldman Sachs",
    "MS":    "Morgan Stanley",
    "BAC":   "Bank of America",
    "C":     "Citigroup",
    "WFC":   "Wells Fargo",
    "AXP":   "American Express",
    "BLK":   "BlackRock",
    "SCHW":  "Charles Schwab",
    "T":     "AT&T",
    "VZ":    "Verizon",
    "CMCSA": "Comcast",
    "LLY":   "Eli Lilly",
    "BMY":   "Bristol-Myers Squibb",

    # ═══ US DEFENSE & AEROSPACE ═══
    "LMT":   "Lockheed Martin",
    "RTX":   "RTX (Raytheon)",
    "NOC":   "Northrop Grumman",
    "GD":    "General Dynamics",
    "LHX":   "L3Harris Technologies",

    # ═══ US ENERGY ═══
    "COP":   "ConocoPhillips",
    "SLB":   "Schlumberger",
    "EOG":   "EOG Resources",
    "MPC":   "Marathon Petroleum",
    "VLO":   "Valero Energy",
    "PSX":   "Phillips 66",
    "OXY":   "Occidental Petroleum",
    "HAL":   "Halliburton",

    # ═══ US CYBERSECURITY & TECH ═══
    "CRWD":  "CrowdStrike",
    "PANW":  "Palo Alto Networks",
    "ZS":    "Zscaler",
    "FTNT":  "Fortinet",
    "NET":   "Cloudflare",
    "PLTR":  "Palantir Technologies",
    "SNOW":  "Snowflake",

    # ═══ EUROPEAN STOCKS ═══
    "ASML":  "ASML Holding",
    "SHEL":  "Shell",
    "MC.PA": "LVMH",
    "OR.PA": "L'Oréal",
    "SAN.PA": "Sanofi",
    "AIR.PA": "Airbus",
    "BNP.PA": "BNP Paribas",
    "TTE.PA": "TotalEnergies",
    "SAP":   "SAP",
    "SIE.DE": "Siemens",
    "ALV.DE": "Allianz",
    "DTE.DE": "Deutsche Telekom",
    "ADS.DE": "Adidas",
    "NOVO-B.CO": "Novo Nordisk",
    "PHIA.AS": "Philips",
    "UNA.AS": "Unilever",
    "INGA.AS": "ING Group",
    "AD.AS":  "Ahold Delhaize",
    "HEIA.AS": "Heineken",
    "WKL.AS": "Wolters Kluwer",
    "RELX.AS": "RELX",
    "PRX.AS": "Prosus",
    "NESN.SW": "Nestlé",
    "ROG.SW": "Roche",
    "NOVN.SW": "Novartis",
    "BARC.L": "Barclays",
    "AZN.L":  "AstraZeneca",
    "HSBA.L": "HSBC Holdings",
    "BP.L":   "BP",

    # ═══ ETFs — EU-listed (UCITS compliant, on Xetra/Tradegate) ═══
    "IWDA.AS": "iShares Core MSCI World",
    "VWRL.AS": "Vanguard FTSE All-World",
    "CSPX.AS": "iShares Core S&P 500",
    "EUNL.DE": "iShares Core MSCI World (DE)",
    "VUSA.AS": "Vanguard S&P 500",
    "IEMA.AS": "iShares MSCI EM",
    "ISAC.AS": "iShares MSCI ACWI",
    "VHYL.AS": "Vanguard FTSE All-World High Dividend Yield",
    "IQQH.DE": "iShares Global Clean Energy",

    # ═══ COMMODITY / MINING / GOLD (EU-listed UCITS, user-confirmed) ═══
    "IS0D.DE": "iShares Oil & Gas Exploration & Production UCITS ETF",
    "WMIN.DE": "VanEck S&P Global Mining UCITS ETF",
    "IS0E.DE": "iShares Gold Producers UCITS ETF",
    "ISPA.DE": "iShares STOXX Global Select Dividend 100 UCITS ETF (DE)",

    # ═══ COPPER / CRITICAL MINERALS (mid-term structural plays) ═══
    "FCX":     "Freeport-McMoRan (copper/gold mining)",
    "SCCO":    "Southern Copper Corp",
    "GLEN.L":  "Glencore (copper/commodities)",
    "ANTO.L":  "Antofagasta (copper mining)",
    "TECK":    "Teck Resources (copper/zinc)",
    "ALB":     "Albemarle (lithium)",
    "RIO":     "Rio Tinto (copper/iron/aluminium)",
    "BHP":     "BHP Group (copper/iron/commodities)",

    # ═══ INFRASTRUCTURE / INDUSTRIALS ═══
    # Note: CAT already listed under US MEGA-CAPS
    "DE":      "Deere & Co",

}

# Simple lookup: normalize ticker to just uppercase base
_TICKER_SET = {t.split(".")[0].upper() for t in BUNQ_STOCKS}
_TICKER_SET.update(t.upper() for t in BUNQ_STOCKS)


def is_available_on_bunq(ticker: str) -> bool:
    """Check if a ticker is likely available on bunq Stocks."""
    t = ticker.strip().upper()
    # Explicitly blocked tickers
    if t in NOT_AVAILABLE:
        return False
    if t in BUNQ_STOCKS:
        return True
    # Check base ticker without exchange suffix
    base = t.split(".")[0]
    return base in _TICKER_SET


def get_bunq_ticker_list() -> list[str]:
    """Return sorted list of all known bunq tickers."""
    return sorted(BUNQ_STOCKS.keys())


def get_bunq_stocks_prompt_snippet() -> str:
    """Return a compact string listing available tickers for Claude prompts."""
    us_tickers = [t for t in BUNQ_STOCKS if "." not in t]
    eu_tickers = [t for t in BUNQ_STOCKS if "." in t]

    return (
        f"US stocks: {', '.join(sorted(us_tickers))}\n"
        f"EU stocks/ETFs: {', '.join(sorted(eu_tickers))}"
    )
