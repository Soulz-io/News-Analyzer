# News Analyzer - OpenClaw Plugin

AI newsroom that detects narrative run-ups in global news and builds game theory decision trees to predict events.

## How it works

```
60 RSS feeds → Python NLP Pipeline → Article Briefs (~80 tokens each)
                                           ↓
                                   Narrative Tracker
                                   (frequency, acceleration, run-up scoring)
                                           ↓
                              OpenClaw Sub-Agents (Newsroom)
                              ├─ Narrative Scanner (haiku, 12x/day)
                              ├─ Run-Up Detector (haiku, 4x/day)
                              ├─ Game Theory Analyst (sonnet, on-demand)
                              ├─ Probability Updater (haiku, per evidence batch)
                              └─ Truth Analyst (sonnet, 1-2x/day)
                                           ↓
                              Decision Trees with Ja/Nee branches
                              5 consequences per branch, Bayesian probability updates
                                           ↓
                              Dashboard in OpenClaw Control UI
```

## Core Concept: Game Theory on News

Every detected narrative run-up becomes a decision tree:

```
"Iran tensions escalating" (run-up: +300% coverage in 3 weeks)
│
├─ Military action? ── P(yes)=0.65 ← [updated from 0.45 after 12 new articles]
│   ├─ YES → 5 near-certain consequences:
│   │   1. Oil price surge (95%)
│   │   2. Strait of Hormuz under pressure (90%)
│   │   3. Defense stocks rise (92%)
│   │   4. Refugee crisis (85%) → EU border pressure? P(yes)=60%
│   │   5. Cyber attacks escalate (80%)
│   └─ NO → 5 consequences: sanctions, diplomacy, ...
```

Probabilities are **live** — they update automatically as new articles come in via Bayesian inference.

## Architecture

- **TypeScript plugin** — integrates with OpenClaw (tab injection, HTTP proxy, agent spawning)
- **Python engine** — RSS fetching, NLP preprocessing, narrative tracking, probability engine
- **OpenClaw sub-agents** — the "newsroom" that analyzes and predicts (no external LLM APIs)
- **Dashboard** — decision trees, run-up cards, prediction scoreboard

## Prerequisites

- OpenClaw 2026.2+
- Python 3.10+
- ~2GB disk for NLP models (spaCy, sentence-transformers)

## Installation

1. Clone this repo to your OpenClaw plugins directory
2. Add to `openclaw.json`:
```json
{
  "plugins": {
    "entries": {
      "openclaw-news-analyzer": {
        "enabled": true,
        "path": "/path/to/openclaw-news-analyzer",
        "config": {
          "fetchIntervalMinutes": 30,
          "scanIntervalHours": 2,
          "scannerModel": "anthropic/claude-haiku-4-5-20251001",
          "analystModel": "anthropic/claude-sonnet-4-6"
        }
      }
    }
  }
}
```
3. Restart OpenClaw gateway
4. Python dependencies auto-install on first launch
5. Navigate to Control UI → Monitoring → News Analyzer

## Token Usage

~$0.50/day, ~$15/month (mix of haiku scanning + sonnet analysis)

Python NLP extracts entities, sentiment, keywords, and summaries from articles so agents receive compressed briefs (~80 tokens) instead of raw text (~500 tokens) — 6x reduction.

## License

MIT
