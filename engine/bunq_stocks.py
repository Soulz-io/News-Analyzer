"""BUNQ Stocks whitelist — curated list of stocks/ETFs available on bunq.

bunq Stocks uses Ginmon (broker) + Upvest (principal broker & custodian)
executing on Börse Stuttgart and other EU exchanges.

bunq offers ~200+ curated popular US & EU large-caps and ETFs.
This list is compiled from bunq's public communications and the typical
Upvest/Ginmon universe. Update this list periodically as bunq adds more.

Source: https://press.bunq.com/241839-bunq-lanceert-beleggingsproduct/
Partner info: https://upvest.co/blog/bunq-ginmon-and-upvest-join-forces-to-launch-investment-products
"""

# Confirmed available (mentioned in bunq press releases / reviews)
CONFIRMED = {
    "AAPL", "TSLA", "NVDA", "META", "MSFT", "HPQ", "V",
}

# Highly likely available — standard large-cap US & EU stocks
# offered by Upvest to all partners (Revolut, N26, bunq)
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
    "REN.AS": "RELX",
    "PRX.AS": "Prosus",
    "NESN.SW": "Nestlé",
    "ROG.SW": "Roche",
    "NOVN.SW": "Novartis",
    "BARC.L": "Barclays",
    "AZN.L":  "AstraZeneca",
    "HSBA.L": "HSBC Holdings",
    "BP.L":   "BP",

    # ═══ ETFs (known bunq/Ginmon offerings) ═══
    "IWDA.AS": "iShares Core MSCI World",
    "VWRL.AS": "Vanguard FTSE All-World",
    "CSPX.AS": "iShares Core S&P 500",
    "EUNL.DE": "iShares Core MSCI World (DE)",
    "VUSA.AS": "Vanguard S&P 500",
    "IEMA.AS": "iShares MSCI EM",
    "ISAC.AS": "iShares MSCI ACWI",

    # ═══ COMMODITY ETFs ═══
    "GLD":   "SPDR Gold Shares",
    "USO":   "United States Oil Fund",
    "SLV":   "iShares Silver Trust",

    # ═══ SECTOR ETFs ═══
    "XLE":   "Energy Select Sector SPDR",
    "XLF":   "Financial Select Sector SPDR",
    "XLV":   "Health Care Select Sector SPDR",
    "XLK":   "Technology Select Sector SPDR",
    "XLI":   "Industrial Select Sector SPDR",
    "ITA":   "iShares US Aerospace & Defense",
    "VNQ":   "Vanguard Real Estate ETF",
    "EEM":   "iShares MSCI Emerging Markets",
    "EFA":   "iShares MSCI EAFE",
    "FXI":   "iShares China Large-Cap",
    "TLT":   "iShares 20+ Year Treasury Bond",
    "HYG":   "iShares High Yield Corporate Bond",
    "SPY":   "SPDR S&P 500 ETF Trust",
    "QQQ":   "Invesco QQQ Trust",
    "VTI":   "Vanguard Total Stock Market",
    "VGK":   "Vanguard FTSE Europe",
    "IQQH.DE": "iShares Global Clean Energy",

    # ═══ VOLATILITY ═══
    "VIX":   "CBOE Volatility Index",
}

# Simple lookup: normalize ticker to just uppercase base
_TICKER_SET = {t.split(".")[0].upper() for t in BUNQ_STOCKS}
_TICKER_SET.update(t.upper() for t in BUNQ_STOCKS)


def is_available_on_bunq(ticker: str) -> bool:
    """Check if a ticker is likely available on bunq Stocks."""
    t = ticker.strip().upper()
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
    # Group by category for readability
    us_tickers = [t for t in BUNQ_STOCKS if "." not in t and t not in ("GLD", "USO", "SLV", "XLE", "XLF", "XLV", "XLK", "XLI", "ITA", "VNQ", "EEM", "EFA", "FXI", "TLT", "HYG", "SPY", "QQQ", "VTI", "VGK", "VIX")]
    eu_tickers = [t for t in BUNQ_STOCKS if "." in t]
    etf_tickers = [t for t in BUNQ_STOCKS if t in ("GLD", "USO", "SLV", "XLE", "XLF", "XLV", "XLK", "XLI", "ITA", "VNQ", "EEM", "EFA", "FXI", "TLT", "HYG", "SPY", "QQQ", "VTI", "VGK", "VIX")]

    return (
        f"US stocks: {', '.join(sorted(us_tickers))}\n"
        f"EU stocks: {', '.join(sorted(eu_tickers))}\n"
        f"ETFs: {', '.join(sorted(etf_tickers))}"
    )
