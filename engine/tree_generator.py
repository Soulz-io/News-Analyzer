"""Decision tree generator using Claude API.

Given a consolidated run-up, calls Claude Haiku to produce a structured
decision tree with:
  - One specific yes/no question with a timeline
  - Probability estimate
  - Yes/no tracking keywords
  - 3-5 consequences per branch with impacts and probabilities

The output is stored as DecisionNode + Consequence records in the DB.
"""

import json
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from .config import config
from .db import (
    get_session,
    RunUp,
    DecisionNode,
    Consequence,
    StockImpact,
    NarrativeTimeline,
    ArticleBrief,
    Article,
    TokenUsage,
    EngineSettings,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pricing table (EUR per 1M tokens)  — updated March 2025
# ---------------------------------------------------------------------------

MODEL_PRICING = {
    # model_name: (input_per_1M, output_per_1M) in EUR
    "claude-haiku-4-5-20251001": (0.80, 4.00),
    "claude-sonnet-4-20250514": (3.00, 15.00),
    "claude-opus-4-20250514": (15.00, 75.00),
}
DEFAULT_PRICING = (1.00, 5.00)  # fallback

DEFAULT_DAILY_BUDGET_EUR = 1.00

# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a geopolitical analyst specializing in game theory and scenario planning.
You analyze news narratives and formulate concrete, time-bound yes/no decision
questions with consequences for each outcome.

You ALWAYS respond with valid JSON only. No markdown, no explanation outside JSON."""

USER_PROMPT_TEMPLATE = """\
Analyze this news run-up and create a decision tree.

## Run-up: {narrative_name}
- Score: {score}/100 (escalation level)
- Articles tracked: {article_count}
- Days active: {days_active}
- Region: {region}

## Recent article summaries:
{article_summaries}

## Instructions:

1. Formulate ONE specific yes/no question with a clear timeline (e.g. "within 2 weeks", "within 1 month")
2. Estimate the probability of YES (between 0.05 and 0.95)
3. List 5 keywords that would indicate YES is happening, and 5 for NO
4. For each branch (YES and NO), describe 3-5 consequences with:
   - Description (1-2 sentences)
   - Probability of this consequence occurring IF the branch happens (0.0-1.0)
   - Economic impact (1 sentence)
   - Geopolitical impact (1 sentence)
   - Social impact (1 sentence)
   - 3 tracking keywords (for automated monitoring)
   - 2-4 affected stocks, ETFs, or commodities with ticker, direction (bullish/bearish), magnitude (low/moderate/high/extreme), and reasoning

IMPORTANT: Only use tickers from this list (user's broker: bunq Stocks via Upvest/Ginmon):
{available_tickers}

Respond with this exact JSON structure:
{{
  "question": "Will X happen within Y?",
  "timeline_estimate": "2 weeks",
  "yes_probability": 0.65,
  "yes_keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "no_keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "consequences": {{
    "yes": [
      {{
        "description": "What happens if YES",
        "probability": 0.8,
        "impact_economic": "Economic effect",
        "impact_geopolitical": "Geopolitical effect",
        "impact_social": "Social effect",
        "keywords": ["track1", "track2", "track3"],
        "stock_impacts": [
          {{
            "ticker": "XOM",
            "name": "ExxonMobil",
            "asset_type": "stock",
            "direction": "bullish",
            "magnitude": "high",
            "reasoning": "Oil supply disruption fears drive energy stocks up"
          }}
        ]
      }}
    ],
    "no": [
      {{
        "description": "What happens if NO",
        "probability": 0.7,
        "impact_economic": "Economic effect",
        "impact_geopolitical": "Geopolitical effect",
        "impact_social": "Social effect",
        "keywords": ["track1", "track2", "track3"],
        "stock_impacts": [
          {{
            "ticker": "SPY",
            "name": "S&P 500 ETF",
            "asset_type": "etf",
            "direction": "bullish",
            "magnitude": "low",
            "reasoning": "Market relief on de-escalation"
          }}
        ]
      }}
    ]
  }}
}}"""


# ---------------------------------------------------------------------------
# Brief collection helpers
# ---------------------------------------------------------------------------

def _collect_briefs_for_runup(
    run_up: RunUp, session: Session, max_briefs: int = 30
) -> List[ArticleBrief]:
    """Collect the most relevant article briefs for a run-up's narrative.

    Matches briefs by region + keyword overlap with the narrative name.
    Returns at most *max_briefs* briefs, sorted by recency.
    """
    parts = run_up.narrative_name.lower().split("-")
    region_part = parts[0] if parts else ""
    kw_parts = set(parts[1:]) if len(parts) > 1 else set()

    # Known multi-word regions
    known_regions = [
        "middle-east", "east-asia", "south-asia", "southeast-asia",
        "central-asia", "sub-saharan-africa", "north-africa", "latin-america",
    ]
    for r in known_regions:
        if run_up.narrative_name.lower().startswith(r):
            region_part = r
            kw_parts = set(run_up.narrative_name.lower()[len(r) + 1:].split("-"))
            break

    # Query briefs matching the region
    briefs = (
        session.query(ArticleBrief)
        .join(Article)
        .filter(ArticleBrief.region.isnot(None))
        .order_by(Article.pub_date.desc().nullslast())
        .limit(500)
        .all()
    )

    scored: List[tuple] = []
    for b in briefs:
        b_region = (b.region or "").lower().strip()
        if region_part not in b_region and b_region not in region_part:
            continue
        # Keyword match score
        try:
            b_kws = set(
                kw.lower()
                for kw in (json.loads(b.keywords_json) if b.keywords_json else [])
            )
        except Exception:
            b_kws = set()
        overlap = len(kw_parts & b_kws) if kw_parts else 1
        if kw_parts and overlap == 0:
            # Still include if region matches but with lower score
            overlap = 0.1
        scored.append((overlap, b))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [b for _, b in scored[:max_briefs]]


def _briefs_to_summary(briefs: List[ArticleBrief]) -> str:
    """Convert briefs to a compact text summary for the prompt."""
    lines = []
    for i, b in enumerate(briefs, 1):
        title = ""
        if b.article:
            title = (b.article.title or "")[:100]
        summary = (b.summary or "")[:150]
        sentiment = b.sentiment or 0.0
        intensity = b.intensity or "unknown"
        try:
            kws = json.loads(b.keywords_json) if b.keywords_json else []
        except Exception:
            kws = []
        kw_str = ", ".join(kws[:5])
        lines.append(
            f"{i}. [{intensity}] {title}\n"
            f"   {summary}\n"
            f"   Keywords: {kw_str} | Sentiment: {sentiment:.2f}"
        )
    return "\n".join(lines) if lines else "(no articles available)"


# ---------------------------------------------------------------------------
# Token budget management
# ---------------------------------------------------------------------------

def get_daily_budget_eur() -> float:
    """Get the daily token budget in EUR from settings (default €1.00)."""
    session = get_session()
    try:
        setting = session.query(EngineSettings).get("daily_budget_eur")
        if setting:
            return float(setting.value)
        return DEFAULT_DAILY_BUDGET_EUR
    except Exception:
        return DEFAULT_DAILY_BUDGET_EUR
    finally:
        session.close()


def set_daily_budget_eur(amount: float) -> float:
    """Set the daily token budget in EUR. Returns the new value."""
    amount = max(0.0, round(amount, 2))
    session = get_session()
    try:
        setting = session.query(EngineSettings).get("daily_budget_eur")
        if setting:
            setting.value = str(amount)
        else:
            setting = EngineSettings(key="daily_budget_eur", value=str(amount))
            session.add(setting)
        session.commit()
        logger.info("Daily budget set to €%.2f", amount)
        return amount
    except Exception:
        session.rollback()
        return DEFAULT_DAILY_BUDGET_EUR
    finally:
        session.close()


def get_today_spending_eur() -> float:
    """Get total EUR spent today on Claude API calls."""
    from datetime import date as date_type
    session = get_session()
    try:
        today_start = datetime.combine(date_type.today(), datetime.min.time())
        total = (
            session.query(func.coalesce(func.sum(TokenUsage.cost_eur), 0.0))
            .filter(TokenUsage.timestamp >= today_start)
            .scalar()
        ) or 0.0
        return float(total)
    except Exception:
        return 0.0
    finally:
        session.close()


def _calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate EUR cost for a Claude API call."""
    input_rate, output_rate = MODEL_PRICING.get(model, DEFAULT_PRICING)
    cost = (input_tokens / 1_000_000) * input_rate + (output_tokens / 1_000_000) * output_rate
    return round(cost, 6)


def _check_budget() -> bool:
    """Check if we still have budget left for today. Returns True if OK."""
    budget = get_daily_budget_eur()
    spent = get_today_spending_eur()
    remaining = budget - spent
    if remaining <= 0:
        logger.warning(
            "Daily token budget exhausted: €%.4f spent of €%.2f budget.",
            spent, budget,
        )
        return False
    logger.info("Budget check OK: €%.4f spent, €%.4f remaining of €%.2f.",
                spent, remaining, budget)
    return True


def _log_usage(
    model: str,
    input_tokens: int,
    output_tokens: int,
    purpose: str = "tree_generation",
    run_up_id: Optional[int] = None,
) -> TokenUsage:
    """Log a Claude API call and its cost."""
    cost = _calculate_cost(model, input_tokens, output_tokens)
    session = get_session()
    try:
        usage = TokenUsage(
            timestamp=datetime.utcnow(),
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_eur=cost,
            purpose=purpose,
            run_up_id=run_up_id,
        )
        session.add(usage)
        session.commit()
        logger.info(
            "Token usage logged: %s in=%d out=%d cost=€%.4f purpose=%s",
            model, input_tokens, output_tokens, cost, purpose,
        )
        return usage
    except Exception:
        session.rollback()
        logger.exception("Failed to log token usage.")
        return None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

def _call_claude(
    prompt_context: Dict[str, Any],
    run_up_id: Optional[int] = None,
) -> Optional[Dict]:
    """Call Claude API and return the parsed JSON decision tree.

    Checks the daily EUR budget before calling. Logs token usage after.
    """
    if not config.anthropic_api_key:
        logger.error("ANTHROPIC_API_KEY not set -- cannot generate decision trees.")
        return None

    # Budget gate
    if not _check_budget():
        return None

    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed. Run: pip install anthropic")
        return None

    user_prompt = USER_PROMPT_TEMPLATE.format(**prompt_context)

    try:
        client = anthropic.Anthropic(api_key=config.anthropic_api_key)
        response = client.messages.create(
            model=config.tree_generator_model,
            max_tokens=6000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        # Log token usage
        usage = response.usage
        _log_usage(
            model=config.tree_generator_model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            purpose="tree_generation",
            run_up_id=run_up_id,
        )

        # Extract text from response
        text = response.content[0].text.strip()

        # Handle potential markdown code fences
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3].strip()

        tree_data = json.loads(text)
        logger.info(
            "Claude generated tree: question=%r, yes_prob=%.2f",
            tree_data.get("question", "?")[:80],
            tree_data.get("yes_probability", 0.5),
        )
        return tree_data

    except json.JSONDecodeError as e:
        logger.error("Failed to parse Claude response as JSON: %s", e)
        return None
    except Exception as e:
        logger.exception("Claude API call failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Tree storage
# ---------------------------------------------------------------------------

def _store_tree(run_up_id: int, tree_data: Dict, session: Session) -> DecisionNode:
    """Store a generated decision tree as DecisionNode + Consequence records."""
    now = datetime.utcnow()
    yes_prob = max(0.05, min(0.95, tree_data.get("yes_probability", 0.5)))

    # Create root decision node
    root = DecisionNode(
        run_up_id=run_up_id,
        parent_node_id=None,
        branch="root",
        question=tree_data.get("question", ""),
        yes_probability=yes_prob,
        no_probability=round(1.0 - yes_prob, 4),
        yes_keywords_json=json.dumps(tree_data.get("yes_keywords", [])),
        no_keywords_json=json.dumps(tree_data.get("no_keywords", [])),
        depth=0,
        timeline_estimate=tree_data.get("timeline_estimate", ""),
        status="open",
        created_at=now,
        updated_at=now,
    )
    session.add(root)
    session.flush()  # get root.id

    # Create consequences for YES and NO branches
    consequences = tree_data.get("consequences", {})
    for branch_name in ("yes", "no"):
        branch_cons = consequences.get(branch_name, [])
        for i, cons_data in enumerate(branch_cons, 1):
            cons = Consequence(
                decision_node_id=root.id,
                branch=branch_name,
                order=i,
                description=cons_data.get("description", ""),
                probability=max(0.0, min(1.0, cons_data.get("probability", 0.5))),
                impact_economic=cons_data.get("impact_economic"),
                impact_geopolitical=cons_data.get("impact_geopolitical"),
                impact_social=cons_data.get("impact_social"),
                keywords_json=json.dumps(cons_data.get("keywords", [])),
                status="predicted",
            )
            session.add(cons)
            session.flush()  # need cons.id for stock impacts

            # Store stock impacts for this consequence
            for si_data in cons_data.get("stock_impacts", []):
                ticker = si_data.get("ticker", "").strip().upper()
                if not ticker:
                    continue
                si = StockImpact(
                    consequence_id=cons.id,
                    ticker=ticker,
                    name=si_data.get("name", ticker),
                    asset_type=si_data.get("asset_type", "stock"),
                    direction=si_data.get("direction", "bullish"),
                    magnitude=si_data.get("magnitude", "moderate"),
                    reasoning=si_data.get("reasoning", ""),
                )
                session.add(si)

    session.flush()
    logger.info(
        "Stored decision tree for run-up %d: root_id=%d, question=%r",
        run_up_id,
        root.id,
        root.question[:80],
    )
    return root


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_tree(run_up_id: int) -> Optional[Dict]:
    """Generate a decision tree for a single run-up.

    Returns a dict with tree info on success, None on failure.
    """
    session = get_session()
    try:
        run_up = session.query(RunUp).get(run_up_id)
        if run_up is None:
            logger.error("RunUp %d not found.", run_up_id)
            return None

        # Check if tree already exists
        existing = (
            session.query(DecisionNode)
            .filter(DecisionNode.run_up_id == run_up_id, DecisionNode.depth == 0)
            .first()
        )
        if existing:
            logger.info(
                "RunUp %d already has a decision tree (node %d). Skipping.",
                run_up_id,
                existing.id,
            )
            return {"status": "exists", "root_node_id": existing.id}

        # Collect briefs
        briefs = _collect_briefs_for_runup(run_up, session)
        summaries = _briefs_to_summary(briefs)

        # Derive region from narrative name
        from .narrative_tracker import _extract_region
        region = _extract_region(run_up.narrative_name)

        # Days active
        from datetime import date
        days_active = (date.today() - run_up.start_date).days if run_up.start_date else 0

        # Build prompt context
        from .bunq_stocks import get_bunq_stocks_prompt_snippet
        prompt_context = {
            "narrative_name": run_up.narrative_name,
            "score": run_up.current_score,
            "article_count": run_up.article_count_total,
            "days_active": days_active,
            "region": region,
            "article_summaries": summaries,
            "available_tickers": get_bunq_stocks_prompt_snippet(),
        }

        # Call Claude (budget-aware)
        tree_data = _call_claude(prompt_context, run_up_id=run_up_id)
        if tree_data is None:
            return None

        # Store in DB
        root = _store_tree(run_up_id, tree_data, session)
        session.commit()

        return {
            "status": "created",
            "run_up_id": run_up_id,
            "root_node_id": root.id,
            "question": root.question,
            "yes_probability": root.yes_probability,
            "timeline": root.timeline_estimate,
        }

    except Exception:
        logger.exception("Failed to generate tree for run-up %d.", run_up_id)
        session.rollback()
        return None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Child tree generation (cascading from confirmed nodes)
# ---------------------------------------------------------------------------

CHILD_TREE_SYSTEM = """\
You are a geopolitical game theory analyst. A previous decision point has been
CONFIRMED — it actually happened. Your job is to analyze the NEXT critical
yes/no decision that follows from this confirmation.

What must we watch for now? What is the next domino that could fall?

You ALWAYS respond with valid JSON only. No markdown, no explanation outside JSON."""

CHILD_TREE_USER_TEMPLATE = """\
A previous decision point has been CONFIRMED. Analyze what comes next.

## Confirmed Decision
- Question: {confirmed_question}
- Outcome: {confirmed_outcome}
- Evidence: {evidence}

## Key Consequence Now In Play
{consequence_text}

## Parent Run-up: {narrative_name}
- Score: {score}/100 (escalation level)
- Region: {region}

## Recent article summaries:
{article_summaries}

## Instructions:

Based on the confirmed outcome and its key consequence, formulate the NEXT
critical yes/no question. What is the next decision point to watch?

1. Formulate ONE specific yes/no question with a clear timeline
2. Estimate the probability of YES (between 0.05 and 0.95)
3. List 5 keywords that would indicate YES is happening, and 5 for NO
4. For each branch (YES and NO), describe 3-5 consequences with:
   - Description (1-2 sentences)
   - Probability of this consequence occurring IF the branch happens (0.0-1.0)
   - Economic impact (1 sentence)
   - Geopolitical impact (1 sentence)
   - Social impact (1 sentence)
   - 3 tracking keywords (for automated monitoring)
   - 2-4 affected stocks, ETFs, or commodities with ticker, direction (bullish/bearish), magnitude (low/moderate/high/extreme), and reasoning

IMPORTANT: Only use tickers from this list (user's broker: bunq Stocks via Upvest/Ginmon):
{available_tickers}

Respond with this exact JSON structure:
{{
  "question": "Will X happen within Y?",
  "timeline_estimate": "2 weeks",
  "yes_probability": 0.65,
  "yes_keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "no_keywords": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "consequences": {{
    "yes": [
      {{
        "description": "What happens if YES",
        "probability": 0.8,
        "impact_economic": "Economic effect",
        "impact_geopolitical": "Geopolitical effect",
        "impact_social": "Social effect",
        "keywords": ["track1", "track2", "track3"],
        "stock_impacts": [
          {{
            "ticker": "XOM",
            "name": "ExxonMobil",
            "asset_type": "stock",
            "direction": "bullish",
            "magnitude": "high",
            "reasoning": "Oil supply disruption fears drive energy stocks up"
          }}
        ]
      }}
    ],
    "no": [
      {{
        "description": "What happens if NO",
        "probability": 0.7,
        "impact_economic": "Economic effect",
        "impact_geopolitical": "Geopolitical effect",
        "impact_social": "Social effect",
        "keywords": ["track1", "track2", "track3"],
        "stock_impacts": [
          {{
            "ticker": "SPY",
            "name": "S&P 500 ETF",
            "asset_type": "etf",
            "direction": "bullish",
            "magnitude": "low",
            "reasoning": "Market relief on de-escalation"
          }}
        ]
      }}
    ]
  }}
}}"""


def generate_child_tree(parent_node_id: int) -> Optional[Dict]:
    """Generate child decision tree after a node is auto-confirmed.

    When a decision node is confirmed (via Polymarket or manual), this creates
    the next level of the game theory tree — asking "what happens next?"

    Returns a dict with tree info on success, None on failure.
    """
    session = get_session()
    try:
        # 1. Load the confirmed parent node
        parent = session.query(DecisionNode).get(parent_node_id)
        if parent is None:
            logger.error("Parent node %d not found.", parent_node_id)
            return None

        if parent.status not in ("confirmed_yes", "confirmed_no"):
            logger.warning(
                "Node %d is not confirmed (status=%s). Skipping child tree.",
                parent_node_id, parent.status,
            )
            return None

        # Check if child tree already exists
        existing_child = (
            session.query(DecisionNode)
            .filter(DecisionNode.parent_node_id == parent_node_id)
            .first()
        )
        if existing_child:
            logger.info(
                "Node %d already has child tree (node %d). Skipping.",
                parent_node_id, existing_child.id,
            )
            return {"status": "exists", "root_node_id": existing_child.id}

        # 2. Load the parent's run-up
        run_up = session.query(RunUp).get(parent.run_up_id)
        if run_up is None:
            logger.error("RunUp %d not found for node %d.", parent.run_up_id, parent_node_id)
            return None

        # 3. Determine the confirmed branch and pick the highest-impact consequence
        confirmed_branch = "yes" if parent.status == "confirmed_yes" else "no"
        confirmed_outcome = "YES" if confirmed_branch == "yes" else "NO"

        consequences = (
            session.query(Consequence)
            .filter(
                Consequence.decision_node_id == parent.id,
                Consequence.branch == confirmed_branch,
            )
            .order_by(Consequence.probability.desc())
            .all()
        )

        if consequences:
            top_consequence = consequences[0]
            consequence_text = (
                f"- Description: {top_consequence.description}\n"
                f"- Probability: {top_consequence.probability:.0%}\n"
                f"- Economic impact: {top_consequence.impact_economic or 'N/A'}\n"
                f"- Geopolitical impact: {top_consequence.impact_geopolitical or 'N/A'}\n"
                f"- Social impact: {top_consequence.impact_social or 'N/A'}"
            )
        else:
            consequence_text = "(No specific consequences were predicted for this branch)"

        # 4. Collect recent articles (reuse existing logic)
        briefs = _collect_briefs_for_runup(run_up, session)
        summaries = _briefs_to_summary(briefs)

        # Derive region from narrative name
        from .narrative_tracker import _extract_region
        region = _extract_region(run_up.narrative_name)

        # 5. Build prompt context for the child tree
        from .bunq_stocks import get_bunq_stocks_prompt_snippet
        prompt_context = {
            "confirmed_question": parent.question,
            "confirmed_outcome": confirmed_outcome,
            "evidence": parent.evidence or "Auto-confirmed via Polymarket",
            "consequence_text": consequence_text,
            "narrative_name": run_up.narrative_name,
            "score": run_up.current_score,
            "region": region,
            "article_summaries": summaries,
            "available_tickers": get_bunq_stocks_prompt_snippet(),
        }

        # 6. Budget check
        if not _check_budget():
            logger.warning("Budget exhausted — cannot generate child tree for node %d.", parent_node_id)
            return None

        # 7. Call Claude with the child-tree prompt
        tree_data = _call_claude_child(prompt_context, run_up_id=run_up.id)
        if tree_data is None:
            return None

        # 8. Store the child tree, reusing _store_tree but overriding parent/depth
        child_root = _store_child_tree(
            run_up_id=run_up.id,
            tree_data=tree_data,
            parent_node_id=parent.id,
            parent_depth=parent.depth,
            session=session,
        )
        session.commit()

        logger.info(
            "Generated child tree for node %d: child_root=%d, question=%r",
            parent_node_id, child_root.id, child_root.question[:80],
        )
        return {
            "status": "created",
            "run_up_id": run_up.id,
            "parent_node_id": parent_node_id,
            "root_node_id": child_root.id,
            "question": child_root.question,
            "yes_probability": child_root.yes_probability,
            "timeline": child_root.timeline_estimate,
            "depth": child_root.depth,
        }

    except Exception:
        logger.exception("Failed to generate child tree for node %d.", parent_node_id)
        session.rollback()
        return None
    finally:
        session.close()


def _call_claude_child(
    prompt_context: Dict[str, Any],
    run_up_id: Optional[int] = None,
) -> Optional[Dict]:
    """Call Claude API with the child-tree prompt. Returns parsed JSON."""
    if not config.anthropic_api_key:
        logger.error("ANTHROPIC_API_KEY not set — cannot generate child trees.")
        return None

    if not _check_budget():
        return None

    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed. Run: pip install anthropic")
        return None

    user_prompt = CHILD_TREE_USER_TEMPLATE.format(**prompt_context)

    try:
        client = anthropic.Anthropic(api_key=config.anthropic_api_key)
        response = client.messages.create(
            model=config.tree_generator_model,
            max_tokens=6000,
            system=CHILD_TREE_SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )

        usage = response.usage
        _log_usage(
            model=config.tree_generator_model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            purpose="child_tree_generation",
            run_up_id=run_up_id,
        )

        text = response.content[0].text.strip()

        # Handle potential markdown code fences
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3].strip()

        tree_data = json.loads(text)
        logger.info(
            "Claude generated child tree: question=%r, yes_prob=%.2f",
            tree_data.get("question", "?")[:80],
            tree_data.get("yes_probability", 0.5),
        )
        return tree_data

    except json.JSONDecodeError as e:
        logger.error("Failed to parse Claude child-tree response as JSON: %s", e)
        return None
    except Exception as e:
        logger.exception("Claude API call failed for child tree: %s", e)
        return None


def _store_child_tree(
    run_up_id: int,
    tree_data: Dict,
    parent_node_id: int,
    parent_depth: int,
    session: Session,
) -> DecisionNode:
    """Store a child decision tree, setting parent_node_id and depth correctly.

    Reuses the same consequence/stock-impact storage logic as _store_tree.
    """
    now = datetime.utcnow()
    yes_prob = max(0.05, min(0.95, tree_data.get("yes_probability", 0.5)))

    child_root = DecisionNode(
        run_up_id=run_up_id,
        parent_node_id=parent_node_id,
        branch="root",
        question=tree_data.get("question", ""),
        yes_probability=yes_prob,
        no_probability=round(1.0 - yes_prob, 4),
        yes_keywords_json=json.dumps(tree_data.get("yes_keywords", [])),
        no_keywords_json=json.dumps(tree_data.get("no_keywords", [])),
        depth=parent_depth + 1,
        timeline_estimate=tree_data.get("timeline_estimate", ""),
        status="open",
        created_at=now,
        updated_at=now,
    )
    session.add(child_root)
    session.flush()

    # Create consequences for YES and NO branches (same logic as _store_tree)
    consequences = tree_data.get("consequences", {})
    for branch_name in ("yes", "no"):
        branch_cons = consequences.get(branch_name, [])
        for i, cons_data in enumerate(branch_cons, 1):
            cons = Consequence(
                decision_node_id=child_root.id,
                branch=branch_name,
                order=i,
                description=cons_data.get("description", ""),
                probability=max(0.0, min(1.0, cons_data.get("probability", 0.5))),
                impact_economic=cons_data.get("impact_economic"),
                impact_geopolitical=cons_data.get("impact_geopolitical"),
                impact_social=cons_data.get("impact_social"),
                keywords_json=json.dumps(cons_data.get("keywords", [])),
                status="predicted",
            )
            session.add(cons)
            session.flush()

            for si_data in cons_data.get("stock_impacts", []):
                ticker = si_data.get("ticker", "").strip().upper()
                if not ticker:
                    continue
                si = StockImpact(
                    consequence_id=cons.id,
                    ticker=ticker,
                    name=si_data.get("name", ticker),
                    asset_type=si_data.get("asset_type", "stock"),
                    direction=si_data.get("direction", "bullish"),
                    magnitude=si_data.get("magnitude", "moderate"),
                    reasoning=si_data.get("reasoning", ""),
                )
                session.add(si)

    session.flush()
    logger.info(
        "Stored child tree for run-up %d: child_root_id=%d, parent=%d, depth=%d, question=%r",
        run_up_id, child_root.id, parent_node_id, child_root.depth, child_root.question[:80],
    )
    return child_root


def generate_trees_for_new_runups() -> List[Dict]:
    """Generate decision trees for active, non-merged run-ups that don't have one yet.

    Respects ``config.max_trees_per_cycle`` to limit API costs.

    Returns a list of result dicts from ``generate_tree()``.
    """
    session = get_session()
    try:
        # Find active, non-merged run-ups without decision trees
        from sqlalchemy import and_, not_, exists
        has_tree = (
            session.query(DecisionNode.run_up_id)
            .filter(DecisionNode.depth == 0)
            .subquery()
        )

        candidates = (
            session.query(RunUp)
            .filter(
                RunUp.status == "active",
                RunUp.merged_into_id.is_(None),
                ~RunUp.id.in_(session.query(has_tree.c.run_up_id)),
            )
            .order_by(RunUp.current_score.desc())
            .limit(config.max_trees_per_cycle)
            .all()
        )

        run_up_ids = [ru.id for ru in candidates]
    finally:
        session.close()

    if not run_up_ids:
        logger.info("No run-ups need decision trees.")
        return []

    logger.info(
        "Generating decision trees for %d run-ups (max %d per cycle).",
        len(run_up_ids),
        config.max_trees_per_cycle,
    )

    results = []
    for ru_id in run_up_ids:
        result = generate_tree(ru_id)
        if result:
            results.append(result)

    logger.info("Generated %d decision trees.", len(results))
    return results
