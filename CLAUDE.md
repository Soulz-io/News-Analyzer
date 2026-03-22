# OpenClaw / W25.ai — Claude Code Project Guide

## Mission
Build a self-improving stock prediction platform based on "buy the rumour, sell the news".
The system analyses world news, insider signals, and Polymarket flow to predict stock movements
BEFORE the market prices them in. Every day it evaluates its own accuracy and improves.

## Architecture
- **Engine**: FastAPI (Python) on port 9121, systemd managed
- **Frontend**: Vanilla JS SPA (`ui/app.js` + `ui/app.css`)
- **Database**: SQLite at `engine/data/news_analyzer.db`
- **Dashboard**: https://w25.ai (Nginx reverse proxy)
- **Cross-DB**: Also reads from `/home/opposite/news-analyzer/news.db` (Investment Swarm V3)

## Key Files
| File | Purpose |
|------|---------|
| `engine/engine.py` | FastAPI app, scheduler (27+ jobs), pipeline |
| `engine/api_routes.py` | All API endpoints (100+) |
| `engine/swarm_consensus.py` | 12-expert swarm debate (Groq + OpenRouter) |
| `engine/daily_advisory.py` | BUY/SELL recommendations with Kelly sizing |
| `engine/flash_detector.py` | Breaking news detection (0-100 flash score) |
| `engine/deep_analysis.py` | "Buy the rumour, sell the news" phase detection |
| `engine/confidence_scorer.py` | Composite signal scoring |
| `engine/price_fetcher.py` | yfinance price data with caching |
| `engine/arabella.py` | Big-news notification persona |
| `engine/catalyst_scanner.py` | Undervalued stock + catalyst detection |
| `engine/watchdog.py` | Health monitoring (only notifies on failures) |
| `engine/telegram_notifier.py` | Telegram delivery |
| `engine/twitter_fetcher.py` | 160+ X accounts monitoring (incl. QuiverQuant) |
| `ui/app.js` | Entire frontend SPA (~5000 lines) |
| `ui/app.css` | All styles (~4700 lines) |

## Data Pipeline
```
108 RSS feeds + GDELT + 160 X accounts → NLP (spaCy/VADER)
→ 24K+ articles → Narrative Tracking → Run-ups
→ Decision Trees (Claude Haiku) → Swarm Consensus (12 experts)
→ Confidence Scoring → Trading Signals → Daily Advisory
→ Evaluation at T+1/3/7/14/30d → Weight rebalancing (EMA)
```

## Daily Briefing Workflow
The system generates a daily briefing at `briefings/YYYY-MM-DD.md`.
To work on improvements:
1. Read the briefing: it tells you what went wrong and what to fix
2. Implement the suggested improvement
3. Test with `python3 -c "import py_compile; py_compile.compile('engine/FILE.py', doraise=True)"`
4. Restart: `sudo systemctl restart openclaw-news-analyzer`
5. Check logs: `journalctl -u openclaw-news-analyzer --since "5 min ago" --no-pager`

## Development Rules
- Frontend is Vanilla JS — no React/Vue/Svelte
- API calls use `?_api=X` query params (dispatch map in engine.py)
- CSS uses dark theme variables (--bg, --accent, --green, --red, --yellow)
- Multiple agents writing to app.js WILL conflict — one agent per file
- Always verify Python syntax after edits
- Advisory evaluation horizons: T+1d, T+3d, T+7d, T+14d, T+30d
- Thresholds per horizon: 1%, 1.5%, 2%, 3%, 5%
- Bunq-available tickers only (210+ in bunq_stocks.py)
- Cost budget: ~EUR 10/month for LLM calls

## Testing
```bash
# Login
curl -s -c cookies.txt -X POST http://127.0.0.1:9121/auth/login \
  -H "Content-Type: application/json" \
  -d '{"login":"joost","password":"Nederland05!!"}'

# Fetch overview
curl -s -b cookies.txt 'http://127.0.0.1:9121/?_api=overview' | python3 -m json.tool

# Check stock scorecard
curl -s -b cookies.txt 'http://127.0.0.1:9121/?_api=stock-scorecard' | python3 -m json.tool

# JS syntax
node --check ui/app.js

# Python syntax
python3 -c "import py_compile; py_compile.compile('engine/daily_advisory.py', doraise=True)"

# Restart service
sudo systemctl restart openclaw-news-analyzer

# Logs
journalctl -u openclaw-news-analyzer --since "5 min ago" --no-pager
```

## Grand Plan Phases
1. **Stock Scorecard** — Track if BUY picks actually grew within predicted horizon
2. **Catalyst Scanner** — Detect undervalued stocks with upcoming catalysts
3. **Polymarket Flow** — Detect large bets on low-volume markets as early signals
4. **Daily Self-Evaluation** — Auto-generate briefings with what went wrong + fix suggestions
5. **Execution Engine** — Connect stock exchange API for automated small trades
