"""FastAPI router with all API endpoints for the News Analyzer engine.

Two audiences:
  - **Agents** (read + write): briefs, narratives, run-ups, decision trees,
    probability shifts, predictions.
  - **Dashboard** (read-only): overview, tree visualisation, timeline,
    scoreboard, world map data, status.
"""

import json
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Any, Dict

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, desc, case
from sqlalchemy.orm import Session

from .config import config
from .db import (
    get_session,
    Article,
    ArticleBrief,
    NarrativeTimeline,
    RunUp,
    DecisionNode,
    Consequence,
    StockImpact,
    PolymarketMatch,
    ProbabilityUpdate,
    Prediction,
    UserFeed,
    TokenUsage,
    EngineSettings,
    AnalysisReport,
    TradingSignal,
    SwarmVerdict,
)
from .narrative_tracker import update_narratives, detect_runups, consolidate_runups
from .probability_engine import get_significant_shifts
from .tree_generator import generate_tree, generate_trees_for_new_runups
from .focus_manager import (
    get_focused_runup_ids, set_focus, clear_focus, is_focused,
    get_focus_polymarket_links, add_polymarket_link,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")

# ---------------------------------------------------------------------------
# Dependency: DB session
# ---------------------------------------------------------------------------

def _get_db():
    """FastAPI dependency that yields a database session and closes it."""
    session = get_session()
    try:
        yield session
    finally:
        session.close()


def _get_engine_setting(db: Session, key: str, default: str = "") -> str:
    """Read a value from EngineSettings."""
    setting = db.query(EngineSettings).filter(EngineSettings.key == key).first()
    return setting.value if setting and setting.value else default


# ---------------------------------------------------------------------------
# Pydantic request/response models
# ---------------------------------------------------------------------------

class NarrativeCreate(BaseModel):
    narrative_name: str
    topic_cluster_id: Optional[int] = None
    date: Optional[str] = None  # ISO date string
    article_count: int = 0
    sources_count: int = 0
    unique_regions: int = 0
    avg_sentiment: float = 0.0
    intensity_score: float = 0.0
    trend: str = "stable"


class RunUpCreate(BaseModel):
    narrative_name: str
    start_date: Optional[str] = None
    current_score: float = 0.0
    acceleration_rate: float = 0.0
    article_count_total: int = 0
    status: str = "active"


class DecisionTreeCreate(BaseModel):
    """Payload for POST /api/decision-tree.

    Expects the full tree JSON produced by the Game Theory Analyst agent.
    """
    run_up_id: int
    nodes: List[Dict[str, Any]]


class NodeUpdate(BaseModel):
    yes_probability: Optional[float] = None
    no_probability: Optional[float] = None
    status: Optional[str] = None
    timeline_estimate: Optional[str] = None


class NodeConfirm(BaseModel):
    branch: str  # "yes" or "no"
    evidence: str = ""


class PredictionCreate(BaseModel):
    run_up_id: int
    decision_node_id: Optional[int] = None
    consequence_id: Optional[int] = None
    prediction_text: str
    confidence: float = 0.5
    branch: str = "yes"
    deadline: Optional[str] = None  # ISO datetime


class PredictionVerify(BaseModel):
    outcome: str  # "correct", "incorrect", "partial"
    evidence: str = ""


class UserFeedCreate(BaseModel):
    name: str
    url: str
    region: str = "global"
    lang: str = "en"


class FocusUpdate(BaseModel):
    runup_ids: List[int] = Field(default_factory=list)


class PolymarketLinkCreate(BaseModel):
    run_up_id: int
    polymarket_url: str


# ===========================================================================
# FOCUS MODE ENDPOINTS
# ===========================================================================

@router.get("/focus")
def get_focus(db: Session = Depends(_get_db)):
    """Return current focus state."""
    ids = get_focused_runup_ids()
    links = get_focus_polymarket_links()

    focused_runups = []
    if ids:
        runups = db.query(RunUp).filter(RunUp.id.in_(ids)).all()
        for ru in runups:
            root_node = (
                db.query(DecisionNode)
                .filter(DecisionNode.run_up_id == ru.id, DecisionNode.depth == 0)
                .first()
            )
            focused_runups.append({
                "id": ru.id,
                "narrative_name": ru.narrative_name,
                "score": ru.current_score,
                "article_count": ru.article_count_total,
                "status": ru.status,
                "has_tree": root_node is not None,
                "root_question": root_node.question if root_node else None,
                "root_probability": round((root_node.yes_probability or 0.5) * 100, 1) if root_node else None,
            })

    return {
        "focused_runup_ids": ids,
        "focused_runups": focused_runups,
        "polymarket_links": links,
    }


@router.put("/focus")
def update_focus(payload: FocusUpdate, db: Session = Depends(_get_db)):
    """Set focused run-ups (max 3). Triggers consolidation."""
    ids = set_focus(payload.runup_ids)
    if ids:
        consolidate_runups()
    return {"status": "ok", "focused_runup_ids": ids}


@router.delete("/focus")
def delete_focus():
    """Clear all focus selections."""
    clear_focus()
    return {"status": "ok", "focused_runup_ids": []}


@router.post("/focus/polymarket-link")
def focus_polymarket_link(payload: PolymarketLinkCreate, db: Session = Depends(_get_db)):
    """Manually link a Polymarket URL to a run-up."""
    from .polymarket import create_manual_polymarket_match

    result = create_manual_polymarket_match(
        run_up_id=payload.run_up_id,
        polymarket_url=payload.polymarket_url,
        db=db,
    )
    if result is None:
        from fastapi import HTTPException as HE
        raise HE(status_code=400, detail="Failed to link Polymarket URL")
    return result


@router.post("/focus/regenerate-tree/{run_up_id}")
def focus_regenerate_tree(run_up_id: int):
    """Force regenerate a decision tree for a focused run-up."""
    if not is_focused(run_up_id):
        from fastapi import HTTPException as HE
        raise HE(status_code=403, detail="Run-up is not in focus mode")

    from .tree_generator import regenerate_tree
    result = regenerate_tree(run_up_id)
    if result is None:
        from fastapi import HTTPException as HE
        raise HE(status_code=500, detail="Tree regeneration failed")
    return result


# ===========================================================================
# AGENT ENDPOINTS (read + write)
# ===========================================================================

# ---- Briefs ---------------------------------------------------------------

@router.get("/briefs")
def get_briefs(
    since: Optional[str] = Query(None, description="ISO datetime cutoff"),
    region: Optional[str] = Query(None),
    topic: Optional[int] = Query(None, description="topic_cluster_id"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(_get_db),
):
    """Return article briefs with optional filters."""
    q = db.query(ArticleBrief).join(Article)

    if since:
        try:
            cutoff = datetime.fromisoformat(since)
            q = q.filter(ArticleBrief.processed_at >= cutoff)
        except ValueError:
            raise HTTPException(400, detail="Invalid 'since' datetime format.")

    if region:
        q = q.filter(ArticleBrief.region == region)

    if topic is not None:
        q = q.filter(ArticleBrief.topic_cluster_id == topic)

    briefs = q.order_by(desc(ArticleBrief.processed_at)).limit(limit).all()

    return [_brief_to_dict(b) for b in briefs]


@router.get("/briefs/for-node/{node_id}")
def get_briefs_for_node(node_id: int, db: Session = Depends(_get_db)):
    """Return briefs relevant to a specific decision node's keywords."""
    node: Optional[DecisionNode] = db.query(DecisionNode).get(node_id)
    if node is None:
        raise HTTPException(404, detail="Decision node not found.")

    yes_kw = _parse_kw(node.yes_keywords_json)
    no_kw = _parse_kw(node.no_keywords_json)
    all_kw = set(yes_kw + no_kw)

    if not all_kw:
        return []

    # Fetch recent briefs and filter by keyword overlap
    cutoff = datetime.utcnow() - timedelta(days=14)
    briefs = (
        db.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= cutoff)
        .all()
    )

    relevant = []
    for b in briefs:
        bkw = set(_parse_kw(b.keywords_json))
        if len(all_kw & bkw) >= config.min_keyword_overlap:
            relevant.append(b)

    return [_brief_to_dict(b) for b in relevant[:100]]


# ---- Narratives -----------------------------------------------------------

@router.get("/narratives")
def get_narratives(db: Session = Depends(_get_db)):
    """Return all distinct narratives with their latest timeline entry."""
    subq = (
        db.query(
            NarrativeTimeline.narrative_name,
            func.max(NarrativeTimeline.date).label("latest_date"),
        )
        .group_by(NarrativeTimeline.narrative_name)
        .subquery()
    )

    rows = (
        db.query(NarrativeTimeline)
        .join(
            subq,
            (NarrativeTimeline.narrative_name == subq.c.narrative_name)
            & (NarrativeTimeline.date == subq.c.latest_date),
        )
        .order_by(desc(NarrativeTimeline.intensity_score))
        .all()
    )

    return [_timeline_to_dict(r) for r in rows]


@router.get("/narratives/{name}/timeline")
def get_narrative_timeline(name: str, db: Session = Depends(_get_db)):
    """Return the full timeline for a given narrative."""
    rows = (
        db.query(NarrativeTimeline)
        .filter(NarrativeTimeline.narrative_name == name)
        .order_by(NarrativeTimeline.date)
        .all()
    )
    if not rows:
        raise HTTPException(404, detail="Narrative not found.")
    return [_timeline_to_dict(r) for r in rows]


@router.post("/narratives")
def create_narrative(payload: NarrativeCreate, db: Session = Depends(_get_db)):
    """Create or update a NarrativeTimeline entry (agent-authored)."""
    target_date = date.fromisoformat(payload.date) if payload.date else date.today()

    existing = (
        db.query(NarrativeTimeline)
        .filter(
            NarrativeTimeline.narrative_name == payload.narrative_name,
            NarrativeTimeline.date == target_date,
        )
        .first()
    )

    if existing:
        existing.article_count = payload.article_count
        existing.sources_count = payload.sources_count
        existing.unique_regions = payload.unique_regions
        existing.avg_sentiment = payload.avg_sentiment
        existing.intensity_score = payload.intensity_score
        existing.trend = payload.trend
        existing.topic_cluster_id = payload.topic_cluster_id
        row = existing
    else:
        row = NarrativeTimeline(
            narrative_name=payload.narrative_name,
            topic_cluster_id=payload.topic_cluster_id,
            date=target_date,
            article_count=payload.article_count,
            sources_count=payload.sources_count,
            unique_regions=payload.unique_regions,
            avg_sentiment=payload.avg_sentiment,
            intensity_score=payload.intensity_score,
            trend=payload.trend,
        )
        db.add(row)

    db.commit()
    db.refresh(row)
    return _timeline_to_dict(row)


# ---- Run-ups --------------------------------------------------------------

@router.get("/runups")
def get_runups(db: Session = Depends(_get_db)):
    """Return all run-ups, newest first."""
    rows = db.query(RunUp).order_by(desc(RunUp.detected_at)).all()
    return [_runup_to_dict(r) for r in rows]


@router.get("/runups/{runup_id}/evidence")
def get_runup_evidence(runup_id: int, db: Session = Depends(_get_db)):
    """Return briefs and probability updates relevant to a run-up."""
    ru: Optional[RunUp] = db.query(RunUp).get(runup_id)
    if ru is None:
        raise HTTPException(404, detail="RunUp not found.")

    # Gather decision node IDs
    node_ids = [n.id for n in ru.decision_nodes]

    # Probability updates for these nodes
    updates = (
        db.query(ProbabilityUpdate)
        .filter(
            ProbabilityUpdate.target_type == "node",
            ProbabilityUpdate.target_id.in_(node_ids),
        )
        .order_by(desc(ProbabilityUpdate.updated_at))
        .all()
    ) if node_ids else []

    # Collect brief IDs referenced in evidence
    brief_ids: set = set()
    for u in updates:
        try:
            ids = json.loads(u.evidence_briefs_json) if u.evidence_briefs_json else []
            brief_ids.update(ids)
        except Exception:
            pass

    briefs = (
        db.query(ArticleBrief).filter(ArticleBrief.id.in_(brief_ids)).all()
        if brief_ids
        else []
    )

    return {
        "run_up": _runup_to_dict(ru),
        "probability_updates": [_prob_update_to_dict(u) for u in updates],
        "evidence_briefs": [_brief_to_dict(b) for b in briefs],
    }


@router.post("/runups")
def create_runup(payload: RunUpCreate, db: Session = Depends(_get_db)):
    """Create a RunUp record (agent-authored)."""
    start = date.fromisoformat(payload.start_date) if payload.start_date else date.today()
    ru = RunUp(
        narrative_name=payload.narrative_name,
        detected_at=datetime.utcnow(),
        start_date=start,
        current_score=payload.current_score,
        acceleration_rate=payload.acceleration_rate,
        article_count_total=payload.article_count_total,
        status=payload.status,
    )
    db.add(ru)
    db.commit()
    db.refresh(ru)
    return _runup_to_dict(ru)


# ---- Manual analysis trigger ----------------------------------------------

@router.post("/analyze")
def run_analysis(db: Session = Depends(_get_db)):
    """Manually trigger narrative analysis and run-up detection.

    Fetches all existing briefs (with article eagerly loaded), runs
    update_narratives and detect_runups using the narrative tracker's
    own session management.
    """
    from sqlalchemy.orm import joinedload

    briefs = (
        db.query(ArticleBrief)
        .options(joinedload(ArticleBrief.article))
        .all()
    )
    if not briefs:
        return {
            "status": "ok",
            "briefs_processed": 0,
            "narratives_updated": 0,
            "runups_detected": 0,
        }

    brief_count = len(briefs)

    # Expunge briefs so they can be used outside this session
    for b in briefs:
        db.expunge(b)
    db.close()

    narratives = update_narratives(briefs)
    runups = detect_runups()

    # Consolidate overlapping run-ups into topics
    primaries = consolidate_runups()

    # Generate decision trees for new consolidated topics
    trees = generate_trees_for_new_runups()

    return {
        "status": "ok",
        "briefs_processed": brief_count,
        "narratives_updated": len(narratives),
        "runups_detected": len(runups),
        "runups_consolidated": len(primaries),
        "trees_generated": len(trees),
        "trees": trees,
    }


# ---- Tree generation (Claude API) -----------------------------------------

@router.post("/generate-tree/{run_up_id}")
def api_generate_tree(run_up_id: int):
    """Generate a decision tree for a specific run-up using Claude API."""
    result = generate_tree(run_up_id)
    if result is None:
        raise HTTPException(500, detail="Tree generation failed. Check logs.")
    return result


@router.post("/generate-trees")
def api_generate_trees():
    """Consolidate run-ups and generate decision trees for top-N without trees."""
    primaries = consolidate_runups()
    trees = generate_trees_for_new_runups()
    return {
        "status": "ok",
        "consolidated_topics": len(primaries),
        "trees_generated": len(trees),
        "trees": trees,
    }


# ---- Token budget ---------------------------------------------------------

@router.get("/budget")
def get_budget():
    """Get current token budget status."""
    from .tree_generator import get_daily_budget_eur, get_today_spending_eur
    budget = get_daily_budget_eur()
    spent = get_today_spending_eur()
    return {
        "daily_budget_eur": budget,
        "spent_today_eur": round(spent, 4),
        "remaining_eur": round(max(0, budget - spent), 4),
        "percentage_used": round((spent / budget * 100) if budget > 0 else 0, 1),
    }


class BudgetUpdate(BaseModel):
    daily_budget_eur: float = Field(..., ge=0.0, le=100.0)


@router.put("/budget")
def update_budget(payload: BudgetUpdate):
    """Update the daily token budget in EUR."""
    from .tree_generator import set_daily_budget_eur
    new_budget = set_daily_budget_eur(payload.daily_budget_eur)
    return {
        "status": "ok",
        "daily_budget_eur": new_budget,
    }


class ApiKeyUpdate(BaseModel):
    api_key: str = Field(..., min_length=1)


@router.put("/settings/api-key")
def update_api_key(payload: ApiKeyUpdate):
    """Set the Anthropic API key (stored in DB, persists across restarts)."""
    config.anthropic_api_key = payload.api_key
    masked = payload.api_key[:8] + "..." + payload.api_key[-4:] if len(payload.api_key) > 12 else "****"
    return {"status": "ok", "api_key_set": True, "masked": masked}


@router.get("/settings/api-key")
def get_api_key_status():
    """Check if an Anthropic API key is configured."""
    key = config.anthropic_api_key
    has_key = bool(key)
    masked = key[:8] + "..." + key[-4:] if key and len(key) > 12 else ("****" if key else "")
    return {"has_key": has_key, "masked": masked}


@router.get("/budget/history")
def budget_history(days: int = Query(7, ge=1, le=90), db: Session = Depends(_get_db)):
    """Return daily token spending for the last N days."""
    from .db import TokenUsage
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = (
        db.query(
            func.date(TokenUsage.timestamp).label("day"),
            func.sum(TokenUsage.cost_eur).label("total_cost"),
            func.sum(TokenUsage.input_tokens).label("total_input"),
            func.sum(TokenUsage.output_tokens).label("total_output"),
            func.count(TokenUsage.id).label("calls"),
        )
        .filter(TokenUsage.timestamp >= cutoff)
        .group_by(func.date(TokenUsage.timestamp))
        .order_by(func.date(TokenUsage.timestamp).desc())
        .all()
    )
    return [
        {
            "date": str(r.day),
            "cost_eur": round(float(r.total_cost), 4),
            "input_tokens": int(r.total_input),
            "output_tokens": int(r.total_output),
            "api_calls": int(r.calls),
        }
        for r in rows
    ]


@router.get("/usage/breakdown")
def usage_breakdown(
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(_get_db),
):
    """Comprehensive token usage breakdown by platform, purpose, and model."""
    from .tree_generator import get_daily_budget_eur, get_today_spending_eur

    cutoff = datetime.utcnow() - timedelta(days=days)
    budget = get_daily_budget_eur()
    spent_today = get_today_spending_eur()

    # ---------- helpers to classify provider ----------
    def _provider(model: str) -> str:
        m = (model or "").lower()
        if m.startswith("openrouter/"):
            return "OpenRouter"
        if m.startswith("groq/"):
            return "Groq"
        if "llama" in m or "qwen" in m or "scout" in m or "nemotron" in m or "hermes" in m or "gpt-oss" in m or "step-" in m:
            # Legacy rows logged without Groq/OR prefix
            return "Groq"
        return "Claude"

    # ---------- raw rows for the requested window ----------
    rows = (
        db.query(TokenUsage)
        .filter(TokenUsage.timestamp >= cutoff)
        .order_by(TokenUsage.timestamp.desc())
        .all()
    )

    # ---------- per-platform totals ----------
    platform_totals: Dict[str, Any] = {}
    purpose_totals: Dict[str, Any] = {}
    model_totals: Dict[str, Any] = {}
    daily_by_platform: Dict[str, Dict[str, float]] = {}  # date -> {platform -> cost}
    daily_totals: Dict[str, Dict[str, Any]] = {}  # date -> aggregated

    for r in rows:
        prov = _provider(r.model or "")
        purp = r.purpose or "unknown"
        model = r.model or "unknown"
        day = r.timestamp.strftime("%Y-%m-%d") if r.timestamp else "unknown"
        cost = float(r.cost_eur or 0)
        inp = int(r.input_tokens or 0)
        out = int(r.output_tokens or 0)

        # Platform
        if prov not in platform_totals:
            platform_totals[prov] = {"cost_eur": 0.0, "input_tokens": 0, "output_tokens": 0, "calls": 0}
        platform_totals[prov]["cost_eur"] += cost
        platform_totals[prov]["input_tokens"] += inp
        platform_totals[prov]["output_tokens"] += out
        platform_totals[prov]["calls"] += 1

        # Purpose
        if purp not in purpose_totals:
            purpose_totals[purp] = {"cost_eur": 0.0, "input_tokens": 0, "output_tokens": 0, "calls": 0}
        purpose_totals[purp]["cost_eur"] += cost
        purpose_totals[purp]["input_tokens"] += inp
        purpose_totals[purp]["output_tokens"] += out
        purpose_totals[purp]["calls"] += 1

        # Model
        if model not in model_totals:
            model_totals[model] = {"cost_eur": 0.0, "input_tokens": 0, "output_tokens": 0, "calls": 0, "provider": prov}
        model_totals[model]["cost_eur"] += cost
        model_totals[model]["input_tokens"] += inp
        model_totals[model]["output_tokens"] += out
        model_totals[model]["calls"] += 1

        # Daily by platform
        if day not in daily_by_platform:
            daily_by_platform[day] = {}
        daily_by_platform[day][prov] = daily_by_platform[day].get(prov, 0.0) + cost

        # Daily totals
        if day not in daily_totals:
            daily_totals[day] = {"cost_eur": 0.0, "input_tokens": 0, "output_tokens": 0, "calls": 0}
        daily_totals[day]["cost_eur"] += cost
        daily_totals[day]["input_tokens"] += inp
        daily_totals[day]["output_tokens"] += out
        daily_totals[day]["calls"] += 1

    # Round costs
    for v in platform_totals.values():
        v["cost_eur"] = round(v["cost_eur"], 4)
    for v in purpose_totals.values():
        v["cost_eur"] = round(v["cost_eur"], 4)
    for v in model_totals.values():
        v["cost_eur"] = round(v["cost_eur"], 4)

    # Build daily history sorted desc
    daily_history = []
    for day in sorted(daily_totals.keys(), reverse=True):
        dt = daily_totals[day]
        entry = {
            "date": day,
            "cost_eur": round(dt["cost_eur"], 4),
            "input_tokens": dt["input_tokens"],
            "output_tokens": dt["output_tokens"],
            "calls": dt["calls"],
            "by_platform": {k: round(v, 4) for k, v in daily_by_platform.get(day, {}).items()},
        }
        daily_history.append(entry)

    # Grand total
    total_cost = sum(v["cost_eur"] for v in platform_totals.values())
    total_calls = sum(v["calls"] for v in platform_totals.values())
    total_input = sum(v["input_tokens"] for v in platform_totals.values())
    total_output = sum(v["output_tokens"] for v in platform_totals.values())

    return {
        "budget": {
            "daily_budget_eur": budget,
            "spent_today_eur": round(spent_today, 4),
            "remaining_today_eur": round(max(0, budget - spent_today), 4),
            "pct_used_today": round((spent_today / budget * 100) if budget > 0 else 0, 1),
        },
        "period_days": days,
        "totals": {
            "cost_eur": round(total_cost, 4),
            "calls": total_calls,
            "input_tokens": total_input,
            "output_tokens": total_output,
        },
        "by_platform": platform_totals,
        "by_purpose": purpose_totals,
        "by_model": {
            k: v for k, v in sorted(model_totals.items(), key=lambda x: -x[1]["cost_eur"])
        },
        "daily_history": daily_history,
    }


# ---- Decision trees -------------------------------------------------------

@router.post("/decision-tree")
def create_decision_tree(payload: DecisionTreeCreate, db: Session = Depends(_get_db)):
    """Ingest a full decision tree JSON from the Game Theory Analyst.

    Expected node shape::

        {
            "temp_id": ...,
            "parent_temp_id": null | ...,
            "branch": "root" | "yes" | "no",
            "question": "...",
            "yes_probability": 0.6,
            "no_probability": 0.4,
            "yes_keywords": [...],
            "no_keywords": [...],
            "depth": 0,
            "timeline_estimate": "1-2 weeks",
            "consequences": [
                {
                    "branch": "yes",
                    "order": 1,
                    "description": "...",
                    "probability": 0.7,
                    "impact_economic": "...",
                    "impact_geopolitical": "...",
                    "impact_social": "...",
                    "keywords": [...]
                }, ...
            ]
        }
    """
    # Verify run-up exists
    ru: Optional[RunUp] = db.query(RunUp).get(payload.run_up_id)
    if ru is None:
        raise HTTPException(404, detail="RunUp not found.")

    # Map temp_id -> real DecisionNode id for parent linking
    temp_to_real: Dict[Any, int] = {}
    created_nodes: List[DecisionNode] = []

    # Sort nodes by depth so parents are created first
    sorted_nodes = sorted(payload.nodes, key=lambda n: n.get("depth", 0))

    for node_data in sorted_nodes:
        parent_id = None
        parent_temp = node_data.get("parent_temp_id")
        if parent_temp is not None and parent_temp in temp_to_real:
            parent_id = temp_to_real[parent_temp]

        dn = DecisionNode(
            run_up_id=payload.run_up_id,
            parent_node_id=parent_id,
            branch=node_data.get("branch", "root"),
            question=node_data.get("question", ""),
            yes_probability=node_data.get("yes_probability", 0.5),
            no_probability=node_data.get("no_probability", 0.5),
            yes_keywords_json=json.dumps(node_data.get("yes_keywords", [])),
            no_keywords_json=json.dumps(node_data.get("no_keywords", [])),
            depth=node_data.get("depth", 0),
            timeline_estimate=node_data.get("timeline_estimate"),
            status="open",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(dn)
        db.flush()  # assigns dn.id

        temp_id = node_data.get("temp_id")
        if temp_id is not None:
            temp_to_real[temp_id] = dn.id

        # Create consequences
        for cons_data in node_data.get("consequences", []):
            cons = Consequence(
                decision_node_id=dn.id,
                branch=cons_data.get("branch", "yes"),
                order=cons_data.get("order", 1),
                description=cons_data.get("description", ""),
                probability=cons_data.get("probability", 0.5),
                impact_economic=cons_data.get("impact_economic"),
                impact_geopolitical=cons_data.get("impact_geopolitical"),
                impact_social=cons_data.get("impact_social"),
                keywords_json=json.dumps(cons_data.get("keywords", [])),
                status="predicted",
            )
            db.add(cons)

        created_nodes.append(dn)

    db.commit()

    return {
        "status": "ok",
        "run_up_id": payload.run_up_id,
        "nodes_created": len(created_nodes),
    }


@router.get("/decision-tree/{run_up_id}")
def get_decision_tree(run_up_id: int, db: Session = Depends(_get_db)):
    """Return the full decision tree for a run-up."""
    nodes = (
        db.query(DecisionNode)
        .filter(DecisionNode.run_up_id == run_up_id)
        .order_by(DecisionNode.depth, DecisionNode.id)
        .all()
    )
    if not nodes:
        raise HTTPException(404, detail="No decision tree found for this run-up.")

    result = []
    for n in nodes:
        nd = _node_to_dict(n)
        # Attach consequences
        consequences = (
            db.query(Consequence)
            .filter(Consequence.decision_node_id == n.id)
            .order_by(Consequence.order)
            .all()
        )
        nd["consequences"] = [_consequence_to_dict(c) for c in consequences]
        result.append(nd)

    return result


@router.put("/decision-tree/{node_id}")
def update_decision_node(
    node_id: int,
    payload: NodeUpdate,
    db: Session = Depends(_get_db),
):
    """Update a decision node's probability, status, or timeline."""
    node: Optional[DecisionNode] = db.query(DecisionNode).get(node_id)
    if node is None:
        raise HTTPException(404, detail="Decision node not found.")

    if payload.yes_probability is not None:
        node.yes_probability = payload.yes_probability
        node.no_probability = round(1.0 - payload.yes_probability, 4)
    if payload.no_probability is not None:
        node.no_probability = payload.no_probability
        node.yes_probability = round(1.0 - payload.no_probability, 4)
    if payload.status is not None:
        node.status = payload.status
    if payload.timeline_estimate is not None:
        node.timeline_estimate = payload.timeline_estimate

    node.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(node)
    return _node_to_dict(node)


@router.put("/decision-tree/{node_id}/confirm")
def confirm_decision_node(
    node_id: int,
    payload: NodeConfirm,
    db: Session = Depends(_get_db),
):
    """Confirm a yes/no branch outcome on a decision node."""
    node: Optional[DecisionNode] = db.query(DecisionNode).get(node_id)
    if node is None:
        raise HTTPException(404, detail="Decision node not found.")

    if payload.branch not in ("yes", "no"):
        raise HTTPException(400, detail="branch must be 'yes' or 'no'.")

    node.status = f"confirmed_{payload.branch}"
    node.confirmed_at = datetime.utcnow()
    node.evidence = payload.evidence
    node.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(node)
    return _node_to_dict(node)


# ---- Probability shifts ---------------------------------------------------

@router.get("/probability-shifts")
def get_probability_shifts(
    min_shift: float = Query(0.10, ge=0.0, le=1.0),
    db: Session = Depends(_get_db),
):
    """Return probability updates where the shift exceeds min_shift."""
    shifts = get_significant_shifts(threshold=min_shift, session=db)
    return [_prob_update_to_dict(u) for u in shifts]


# ---- Predictions ----------------------------------------------------------

@router.post("/predictions")
def create_prediction(payload: PredictionCreate, db: Session = Depends(_get_db)):
    """Record a prediction by an agent."""
    pred = Prediction(
        run_up_id=payload.run_up_id,
        decision_node_id=payload.decision_node_id,
        consequence_id=payload.consequence_id,
        prediction_text=payload.prediction_text,
        confidence=payload.confidence,
        branch=payload.branch,
        created_at=datetime.utcnow(),
        deadline=(
            datetime.fromisoformat(payload.deadline) if payload.deadline else None
        ),
        outcome="pending",
    )
    db.add(pred)
    db.commit()
    db.refresh(pred)
    return _prediction_to_dict(pred)


@router.put("/predictions/{pred_id}/verify")
def verify_prediction(
    pred_id: int,
    payload: PredictionVerify,
    db: Session = Depends(_get_db),
):
    """Verify a prediction's outcome."""
    pred: Optional[Prediction] = db.query(Prediction).get(pred_id)
    if pred is None:
        raise HTTPException(404, detail="Prediction not found.")

    if payload.outcome not in ("correct", "incorrect", "partial"):
        raise HTTPException(400, detail="outcome must be correct/incorrect/partial.")

    pred.outcome = payload.outcome
    pred.verified_at = datetime.utcnow()
    pred.evidence = payload.evidence
    db.commit()
    db.refresh(pred)
    return _prediction_to_dict(pred)


# ---- Feed management ------------------------------------------------------

@router.get("/feeds")
def get_feeds(db: Session = Depends(_get_db)):
    """Return all feeds (default from config + user-added from DB).

    Default feeds carry ``source: "default"`` and an ``id`` derived from their
    index in default_feeds.yaml (prefixed ``"default-"``).  User feeds carry
    ``source: "user"`` and their real database ``id``.
    """
    # Default feeds from config
    default_feeds = config.feeds
    result: List[Dict] = []
    for idx, feed in enumerate(default_feeds):
        result.append({
            "id": f"default-{idx}",
            "name": feed.get("name", ""),
            "url": feed.get("url", ""),
            "region": feed.get("region", "global"),
            "lang": feed.get("lang", "en"),
            "enabled": True,
            "source": "default",
        })

    # User feeds from DB
    user_feeds = db.query(UserFeed).order_by(UserFeed.added_at).all()
    for uf in user_feeds:
        result.append({
            "id": uf.id,
            "name": uf.name,
            "url": uf.url,
            "region": uf.region,
            "lang": uf.lang,
            "enabled": uf.enabled,
            "source": "user",
            "added_at": uf.added_at.isoformat() if uf.added_at else None,
        })

    return result


@router.post("/feeds")
def create_feed(payload: UserFeedCreate, db: Session = Depends(_get_db)):
    """Add a new user-defined RSS feed."""
    # Check for duplicate URL among user feeds
    existing = db.query(UserFeed).filter(UserFeed.url == payload.url).first()
    if existing:
        raise HTTPException(409, detail="A user feed with this URL already exists.")

    # Also check against default feeds
    for feed in config.feeds:
        if feed.get("url") == payload.url:
            raise HTTPException(
                409, detail="This URL is already present in the default feeds."
            )

    uf = UserFeed(
        name=payload.name,
        url=payload.url,
        region=payload.region,
        lang=payload.lang,
        enabled=True,
    )
    db.add(uf)
    db.commit()
    db.refresh(uf)

    return {
        "id": uf.id,
        "name": uf.name,
        "url": uf.url,
        "region": uf.region,
        "lang": uf.lang,
        "enabled": uf.enabled,
        "source": "user",
        "added_at": uf.added_at.isoformat() if uf.added_at else None,
    }


@router.delete("/feeds/{feed_id}")
def delete_feed(feed_id: int, db: Session = Depends(_get_db)):
    """Remove a user-added feed.  Default feeds cannot be deleted."""
    uf = db.query(UserFeed).get(feed_id)
    if uf is None:
        raise HTTPException(
            404,
            detail="User feed not found. Only user-added feeds can be deleted.",
        )

    db.delete(uf)
    db.commit()
    return {"status": "deleted", "id": feed_id}


@router.put("/feeds/{feed_id}/toggle")
def toggle_feed(feed_id: int, db: Session = Depends(_get_db)):
    """Enable or disable a user-added feed."""
    uf = db.query(UserFeed).get(feed_id)
    if uf is None:
        raise HTTPException(
            404,
            detail="User feed not found. Only user-added feeds can be toggled.",
        )

    uf.enabled = not uf.enabled
    db.commit()
    db.refresh(uf)

    return {
        "id": uf.id,
        "name": uf.name,
        "url": uf.url,
        "region": uf.region,
        "lang": uf.lang,
        "enabled": uf.enabled,
        "source": "user",
        "added_at": uf.added_at.isoformat() if uf.added_at else None,
    }


# ===========================================================================
# DASHBOARD ENDPOINTS (read-only)
# ===========================================================================

@router.get("/dashboard/overview")
def dashboard_overview(db: Session = Depends(_get_db)):
    """High-level stats for the dashboard landing page."""
    total_articles = db.query(func.count(Article.id)).scalar() or 0
    total_briefs = db.query(func.count(ArticleBrief.id)).scalar() or 0

    # All run-ups with decision trees — active first, then expired
    # Exclude merged ones (only show consolidated topics)
    active_runup_rows = (
        db.query(RunUp)
        .filter(RunUp.merged_into_id.is_(None))
        .order_by(
            # Active first, then expired
            case((RunUp.status == "active", 0), else_=1),
            desc(RunUp.current_score),
        )
        .all()
    )
    # Only include run-ups that actually have decision nodes
    active_runup_rows = [
        ru for ru in active_runup_rows
        if db.query(DecisionNode).filter(DecisionNode.run_up_id == ru.id).count() > 0
    ]
    # Filter out expired run-ups with very few articles (noise)
    active_runup_rows = [
        ru for ru in active_runup_rows
        if not (ru.status == "expired" and (ru.article_count_total or 0) < 10)
    ]

    # Enrich run-ups with root decision node info
    runups_out = []
    for ru in active_runup_rows:
        rd = _runup_to_dict(ru)
        # Find root decision node for this run-up
        root_node = (
            db.query(DecisionNode)
            .filter(DecisionNode.run_up_id == ru.id, DecisionNode.depth == 0)
            .first()
        )
        if root_node:
            rd["root_question"] = root_node.question
            rd["root_probability"] = round((root_node.yes_probability or 0.5) * 100, 1)
        else:
            rd["root_question"] = ""
            rd["root_probability"] = 0
        # Days active
        if ru.start_date:
            rd["days_active"] = (date.today() - ru.start_date).days
        else:
            rd["days_active"] = 0
        rd["name"] = ru.narrative_name
        rd["narrative"] = ru.narrative_name
        rd["score"] = ru.current_score
        rd["article_count"] = ru.article_count_total
        rd["active"] = ru.status == "active"
        rd["status"] = ru.status
        rd["node_count"] = db.query(DecisionNode).filter(
            DecisionNode.run_up_id == ru.id
        ).count()
        # Latest swarm verdict for this run-up
        sv = (
            db.query(SwarmVerdict)
            .filter(SwarmVerdict.run_up_id == ru.id, SwarmVerdict.superseded_at.is_(None))
            .order_by(SwarmVerdict.created_at.desc())
            .first()
        )
        rd["swarm_verdict"] = sv.verdict if sv else None
        rd["swarm_confidence"] = round(sv.confidence * 100) if sv else None
        runups_out.append(rd)

    # Prediction scoreboard stats
    predictions = db.query(Prediction).all()
    total_preds = len(predictions)
    verified = [p for p in predictions if p.outcome != "pending"]
    correct = sum(1 for p in verified if p.outcome == "correct")
    incorrect = sum(1 for p in verified if p.outcome == "incorrect")
    partial = sum(1 for p in verified if p.outcome == "partial")
    accuracy = (correct + partial * 0.5) / len(verified) * 100 if verified else 0.0

    # Recent intensity distribution
    cutoff = datetime.utcnow() - timedelta(hours=24)
    recent_briefs = (
        db.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= cutoff)
        .all()
    )
    intensity_dist = {"low": 0, "moderate": 0, "high-threat": 0, "critical": 0}
    for b in recent_briefs:
        if b.intensity in intensity_dist:
            intensity_dist[b.intensity] += 1

    # Resolved predictions: confirmed decision nodes (our track record)
    resolved_nodes = (
        db.query(DecisionNode)
        .filter(DecisionNode.status.in_(["confirmed_yes", "confirmed_no"]))
        .order_by(DecisionNode.updated_at.desc())
        .limit(20)
        .all()
    )
    resolved_out = []
    for node in resolved_nodes:
        ru = db.query(RunUp).get(node.run_up_id) if node.run_up_id else None
        resolved_out.append({
            "node_id": node.id,
            "question": node.question,
            "predicted_probability": round((node.yes_probability or 0.5) * 100, 1),
            "outcome": "YES" if node.status == "confirmed_yes" else "NO",
            "correct": (
                (node.status == "confirmed_yes" and (node.yes_probability or 0.5) >= 0.5) or
                (node.status == "confirmed_no" and (node.yes_probability or 0.5) < 0.5)
            ),
            "narrative_name": ru.narrative_name if ru else "unknown",
            "article_count": ru.article_count_total if ru else 0,
            "days_active": (date.today() - ru.start_date).days if ru and ru.start_date else 0,
            "resolved_at": node.updated_at.isoformat() if node.updated_at else None,
            "depth": node.depth,
        })

    return {
        "runups": runups_out,
        "resolved_predictions": resolved_out,
        "stats": {
            "predictions": total_preds,
            "correct": correct,
            "incorrect": incorrect,
            "partial": partial,
            "accuracy": round(accuracy, 1),
            "recent_outcomes": [
                {
                    "description": p.prediction_text,
                    "probability": round((p.confidence or 0) * 100, 1),
                    "outcome": p.outcome,
                    "date": p.verified_at.isoformat() if p.verified_at else (p.created_at.isoformat() if p.created_at else None),
                }
                for p in sorted(predictions, key=lambda x: x.created_at or datetime.min, reverse=True)[:10]
            ],
        },
        "total_articles": total_articles,
        "total_briefs": total_briefs,
        "active_runups": len(active_runup_rows),
        "last_24h_briefs": len(recent_briefs),
        "intensity_distribution": intensity_dist,
        # Auto-scorer accuracy (from prediction_scorer)
        "auto_scorer": {
            "total": int(_get_engine_setting(db, "predictions_total", "0")),
            "correct": int(_get_engine_setting(db, "predictions_correct", "0")),
            "accuracy": float(_get_engine_setting(db, "prediction_accuracy", "0")),
        },
    }


@router.get("/dashboard/tree/{run_up_id}")
def dashboard_tree(run_up_id: int, db: Session = Depends(_get_db)):
    """Return a dashboard-friendly tree structure for visualisation."""
    ru: Optional[RunUp] = db.query(RunUp).get(run_up_id)
    if ru is None:
        raise HTTPException(404, detail="RunUp not found.")

    nodes = (
        db.query(DecisionNode)
        .filter(DecisionNode.run_up_id == run_up_id)
        .order_by(DecisionNode.depth, DecisionNode.id)
        .all()
    )

    tree_nodes = []
    for n in nodes:
        consequences = (
            db.query(Consequence)
            .filter(Consequence.decision_node_id == n.id)
            .order_by(Consequence.order)
            .all()
        )
        cons_dicts = []
        for c in consequences:
            cd = _consequence_to_dict(c)
            # Add branch_probability and effective_probability
            branch_prob = n.yes_probability if c.branch == "yes" else n.no_probability
            cd["branch_probability"] = round(branch_prob, 4)
            cd["effective_probability"] = round(branch_prob * c.probability, 4)
            # Proximity tracking
            cd["proximity_pct"] = c.proximity_pct
            if c.proximity_pct is not None and c.price_thresholds_json:
                try:
                    thresholds = json.loads(c.price_thresholds_json or "[]")
                    if thresholds:
                        th = thresholds[0]
                        asset = th.get("asset", "?")
                        direction = th.get("direction", "above")
                        target = th.get("value", 0)
                        arrow = "\u2192"
                        cd["proximity_display"] = f"{asset} {arrow} ${target:.0f} ({c.proximity_pct:.0f}%)"
                except Exception:
                    cd["proximity_display"] = None
            else:
                cd["proximity_display"] = None
            # Attach stock impacts
            impacts = (
                db.query(StockImpact)
                .filter(StockImpact.consequence_id == c.id)
                .all()
            )
            cd["stock_impacts"] = [_stock_impact_to_dict(si) for si in impacts]
            cons_dicts.append(cd)

        # Swarm verdict for this node (latest non-superseded)
        sv = (
            db.query(SwarmVerdict)
            .filter(
                SwarmVerdict.decision_node_id == n.id,
                SwarmVerdict.superseded_at.is_(None),
            )
            .order_by(SwarmVerdict.created_at.desc())
            .first()
        )
        swarm_data = None
        if sv:
            swarm_data = {
                "verdict": sv.verdict,
                "confidence": round(sv.confidence * 100),
                "yes_probability": round(sv.yes_probability * 100),
                "primary_ticker": sv.primary_ticker,
                "ticker_direction": sv.ticker_direction,
                "entry_reasoning": sv.entry_reasoning,
                "exit_trigger": sv.exit_trigger,
                "risk_note": sv.risk_note,
                "dissent_note": sv.dissent_note,
                "consensus_strength": round(sv.consensus_strength * 100),
                "all_ticker_signals": json.loads(sv.all_ticker_signals_json or "[]"),
                "created_at": sv.created_at.isoformat(),
            }

        tree_nodes.append({
            **_node_to_dict(n),
            "consequences": cons_dicts,
            "children_ids": [ch.id for ch in n.children] if n.children else [],
            "swarm_verdict": swarm_data,
        })

    # Polymarket matches for this run-up
    poly_matches = (
        db.query(PolymarketMatch)
        .filter(PolymarketMatch.run_up_id == run_up_id)
        .order_by(PolymarketMatch.match_score.desc())
        .all()
    )

    return {
        "run_up": _runup_to_dict(ru),
        "tree": tree_nodes,
        "polymarket": [_polymarket_to_dict(m) for m in poly_matches],
    }


@router.get("/dashboard/timeline")
def dashboard_timeline(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(_get_db),
):
    """Return narrative timeline data for the last N days."""
    cutoff = date.today() - timedelta(days=days)
    rows = (
        db.query(NarrativeTimeline)
        .filter(NarrativeTimeline.date >= cutoff)
        .order_by(NarrativeTimeline.date)
        .all()
    )
    return [_timeline_to_dict(r) for r in rows]


@router.get("/dashboard/scoreboard")
def dashboard_scoreboard(db: Session = Depends(_get_db)):
    """Return prediction accuracy scoreboard."""
    predictions = db.query(Prediction).all()

    total = len(predictions)
    verified = [p for p in predictions if p.outcome != "pending"]
    correct = sum(1 for p in verified if p.outcome == "correct")
    incorrect = sum(1 for p in verified if p.outcome == "incorrect")
    partial = sum(1 for p in verified if p.outcome == "partial")
    pending = total - len(verified)

    accuracy = (
        (correct + partial * 0.5) / len(verified) if verified else 0.0
    )

    return {
        "total_predictions": total,
        "verified": len(verified),
        "correct": correct,
        "incorrect": incorrect,
        "partial": partial,
        "pending": pending,
        "accuracy": round(accuracy, 4),
    }


@router.get("/dashboard/worldmap")
def dashboard_worldmap(db: Session = Depends(_get_db)):
    """Return per-region aggregation for a world map visualisation."""
    cutoff = datetime.utcnow() - timedelta(hours=48)
    briefs = (
        db.query(ArticleBrief)
        .filter(ArticleBrief.processed_at >= cutoff)
        .all()
    )

    regions: Dict[str, Dict] = {}
    for b in briefs:
        r = b.region
        if r not in regions:
            regions[r] = {
                "region": r,
                "article_count": 0,
                "avg_sentiment": 0.0,
                "intensity_counts": {"low": 0, "moderate": 0, "high-threat": 0, "critical": 0},
                "sentiments": [],
            }
        regions[r]["article_count"] += 1
        regions[r]["sentiments"].append(b.sentiment)
        if b.intensity in regions[r]["intensity_counts"]:
            regions[r]["intensity_counts"][b.intensity] += 1

    # Compute averages
    result = []
    for data in regions.values():
        sents = data.pop("sentiments")
        data["avg_sentiment"] = round(sum(sents) / len(sents), 4) if sents else 0.0
        result.append(data)

    return sorted(result, key=lambda x: x["article_count"], reverse=True)


@router.get("/status")
def get_status(db: Session = Depends(_get_db)):
    """Health-check / status endpoint."""
    try:
        article_count = db.query(func.count(Article.id)).scalar() or 0
        brief_count = db.query(func.count(ArticleBrief.id)).scalar() or 0
        db_ok = True
    except Exception:
        article_count = 0
        brief_count = 0
        db_ok = False

    return {
        "status": "ok" if db_ok else "degraded",
        "engine": "running" if db_ok else "stopped",
        "engine_port": config.engine_port,
        "database": config.database_uri,
        "feeds_configured": len(config.feeds),
        "feeds": len(config.feeds),
        "total_articles": article_count,
        "total_briefs": brief_count,
        "fetch_interval_minutes": config.fetch_interval_minutes,
    }


# ===========================================================================
# Polymarket endpoints
# ===========================================================================


@router.get("/polymarket/{run_up_id}")
def get_polymarket_matches(run_up_id: int, db: Session = Depends(_get_db)):
    """Return Polymarket matches for a run-up."""
    matches = (
        db.query(PolymarketMatch)
        .filter(PolymarketMatch.run_up_id == run_up_id)
        .order_by(PolymarketMatch.match_score.desc())
        .all()
    )
    return [_polymarket_to_dict(m) for m in matches]


@router.post("/polymarket/refresh")
def trigger_polymarket_refresh():
    """Manually trigger a Polymarket data refresh."""
    from .polymarket import update_polymarket_matches
    count = update_polymarket_matches()
    return {"status": "ok", "matches_updated": count}


# ===========================================================================
# Analysis & Scoring endpoints
# ===========================================================================


@router.get("/analysis/latest")
def get_latest_analysis(db: Session = Depends(_get_db)):
    """Return the most recent deep analysis report."""
    from .db import AnalysisReport
    report = (
        db.query(AnalysisReport)
        .order_by(desc(AnalysisReport.created_at))
        .first()
    )
    if not report:
        return {"status": "no_report", "message": "No analysis report available yet."}

    try:
        data = json.loads(report.report_json)
    except Exception:
        data = {}

    return {
        "id": report.id,
        "report_type": report.report_type,
        "period_start": str(report.period_start),
        "period_end": str(report.period_end),
        "created_at": report.created_at.isoformat() if report.created_at else None,
        "data": data,
    }


@router.post("/analysis/run")
def trigger_analysis():
    """Manually trigger a deep analysis run."""
    from .deep_analysis import run_deep_analysis
    try:
        report = run_deep_analysis(period_days=7)
        if report is None:
            raise HTTPException(500, detail="Analysis returned no report — check engine logs.")
        return {"status": "ok", "report_id": report.id}
    except Exception as e:
        logger.exception("Manual analysis trigger failed.")
        raise HTTPException(500, detail=str(e))


@router.post("/predictions/score")
def trigger_prediction_scoring():
    """Manually trigger prediction auto-scoring."""
    from .prediction_scorer import score_predictions
    try:
        count = score_predictions()
        return {"status": "ok", "resolved": count}
    except Exception as e:
        logger.exception("Manual prediction scoring failed.")
        raise HTTPException(500, detail=str(e))


# ===========================================================================
# Trading Signals
# ===========================================================================


@router.get("/signals")
def get_trading_signals(
    level: str = Query(None, description="WATCH, ALERT, BUY, STRONG_BUY"),
    active_only: bool = Query(True),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(_get_db),
):
    """Return active trading signals sorted by confidence."""
    q = db.query(TradingSignal)
    if active_only:
        from sqlalchemy import or_
        q = q.filter(
            TradingSignal.superseded_by_id.is_(None),
            or_(
                TradingSignal.expires_at.is_(None),
                TradingSignal.expires_at >= datetime.utcnow(),
            ),
        )
    if level:
        q = q.filter(TradingSignal.signal_level == level.upper())

    signals = q.order_by(TradingSignal.confidence.desc()).limit(limit).all()

    return [
        {
            "id": s.id,
            "run_up_id": s.run_up_id,
            "narrative_name": s.narrative_name,
            "ticker": s.ticker,
            "direction": s.direction,
            "confidence": s.confidence,
            "signal_level": s.signal_level,
            "components": {
                "runup_score": s.runup_score_component,
                "x_signal": s.x_signal_component,
                "polymarket_drift": s.polymarket_drift_component,
                "news_acceleration": s.news_acceleration_component,
                "source_convergence": s.source_convergence_component,
            },
            "x_signal_count": s.x_signal_count,
            "news_count": s.news_count,
            "polymarket_prob": s.polymarket_prob,
            "reasoning": s.reasoning,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "expires_at": s.expires_at.isoformat() if s.expires_at else None,
        }
        for s in signals
    ]


@router.get("/signals/history")
def get_signal_history(
    narrative: str = Query(None),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(_get_db),
):
    """Return historical trading signals for analysis."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    q = db.query(TradingSignal).filter(TradingSignal.created_at >= cutoff)
    if narrative:
        q = q.filter(TradingSignal.narrative_name == narrative)
    signals = q.order_by(TradingSignal.created_at.desc()).limit(limit).all()
    return [
        {
            "id": s.id,
            "narrative_name": s.narrative_name,
            "ticker": s.ticker,
            "direction": s.direction,
            "confidence": s.confidence,
            "signal_level": s.signal_level,
            "reasoning": s.reasoning,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in signals
    ]


@router.post("/signals/refresh")
def trigger_confidence_scoring():
    """Manually trigger confidence scoring for all active run-ups."""
    from .confidence_scorer import update_trading_signals

    signals = update_trading_signals()
    return {
        "status": "ok",
        "signals_created": len(signals),
        "buy_signals": sum(1 for s in signals if s.signal_level in ("BUY", "STRONG_BUY")),
    }


# ===========================================================================
# Swarm Consensus (Expert Panel — Groq/Llama)
# ===========================================================================


@router.get("/swarm/verdicts/{run_up_id}")
def get_swarm_verdicts(run_up_id: int):
    """Get all active swarm verdicts for a run-up."""
    from .swarm_consensus import get_verdicts_for_runup

    verdicts = get_verdicts_for_runup(run_up_id)
    return {"run_up_id": run_up_id, "verdicts": verdicts, "count": len(verdicts)}


@router.get("/swarm/verdict/{node_id}")
def get_swarm_verdict(node_id: int):
    """Get the latest swarm verdict for a specific decision node."""
    from .swarm_consensus import get_latest_verdict

    verdict = get_latest_verdict(node_id)
    if not verdict:
        return {"node_id": node_id, "verdict": None}
    return {"node_id": node_id, "verdict": verdict}


@router.post("/swarm/evaluate/{node_id}")
async def trigger_swarm_evaluation(node_id: int):
    """Manually trigger a swarm evaluation for a specific decision node."""
    from .swarm_consensus import evaluate_node

    result = await evaluate_node(node_id)
    if result is None:
        raise HTTPException(
            status_code=500,
            detail="Swarm evaluation failed — check Groq API key or logs.",
        )
    return {
        "node_id": node_id,
        "verdict": result.get("verdict", "HOLD"),
        "confidence": result.get("confidence", 0),
        "primary_ticker": result.get("primary_ticker"),
    }


@router.post("/swarm/run-cycle")
async def trigger_swarm_cycle():
    """Manually trigger a full swarm consensus cycle."""
    from .swarm_consensus import swarm_consensus_cycle

    count = await swarm_consensus_cycle()
    return {"status": "ok", "nodes_evaluated": count}


@router.get("/swarm/status")
def get_swarm_status():
    """Get swarm consensus status (enabled, stats, etc)."""
    from .db import SwarmVerdict

    db = get_session()
    try:
        total = db.query(SwarmVerdict).count()
        active = (
            db.query(SwarmVerdict)
            .filter(SwarmVerdict.superseded_at.is_(None))
            .count()
        )

        # Count verdicts by type
        verdicts_by_type = {}
        if active > 0:
            rows = (
                db.query(SwarmVerdict.verdict, func.count())
                .filter(SwarmVerdict.superseded_at.is_(None))
                .group_by(SwarmVerdict.verdict)
                .all()
            )
            verdicts_by_type = {v: c for v, c in rows}

        return {
            "enabled": config.swarm_enabled,
            "groq_model": config.groq_model,
            "groq_configured": bool(config.groq_api_key),
            "openrouter_configured": bool(config.openrouter_api_key),
            "interval_minutes": config.swarm_interval_minutes,
            "total_verdicts": total,
            "active_verdicts": active,
            "verdicts_by_type": verdicts_by_type,
        }
    finally:
        db.close()


@router.put("/swarm/config")
def update_swarm_config(
    groq_api_key: Optional[str] = None,
    openrouter_api_key: Optional[str] = None,
    interval_minutes: Optional[int] = None,
):
    """Update Groq/OpenRouter API keys or swarm interval."""
    if groq_api_key is not None:
        config.groq_api_key = groq_api_key
    if openrouter_api_key is not None:
        config.openrouter_api_key = openrouter_api_key
    if interval_minutes is not None and interval_minutes >= 10:
        config.swarm_interval_minutes = interval_minutes
    return {
        "enabled": config.swarm_enabled,
        "groq_configured": bool(config.groq_api_key),
        "openrouter_configured": bool(config.openrouter_api_key),
        "interval_minutes": config.swarm_interval_minutes,
    }


# ===========================================================================
# Price Data & Market Indicators
# ===========================================================================


@router.get("/price/{ticker}")
def get_price(ticker: str):
    """Get current price + 24h change for a stock/ETF ticker."""
    from .price_fetcher import get_price_fetcher

    fetcher = get_price_fetcher()
    return fetcher.get_quote(ticker.upper())


@router.get("/price/{ticker}/chart")
def get_price_chart(
    ticker: str,
    period: str = Query("3mo", description="1mo, 3mo, 6mo, 1y, 2y"),
):
    """Get OHLCV candlestick data for charting."""
    allowed = {"1mo", "3mo", "6mo", "1y", "2y"}
    if period not in allowed:
        period = "3mo"
    from .price_fetcher import get_price_fetcher

    fetcher = get_price_fetcher()
    return fetcher.get_chart_data(ticker.upper(), period)


@router.get("/indicators")
def get_market_indicators():
    """Get BTC + Gold + VIX market indicators."""
    from .price_fetcher import get_price_fetcher

    fetcher = get_price_fetcher()
    return fetcher.get_market_indicators()


# ===========================================================================
# Dashboard: Opportunities Board
# ===========================================================================


@router.get("/dashboard/opportunities")
def dashboard_opportunities(
    min_edge: float = Query(5.0, ge=0, le=100),
    db: Session = Depends(_get_db),
):
    """Return opportunities where our probability diverges from Polymarket.

    Computes edge = (our_prob - market_prob) for each run-up that has
    a PolymarketMatch, filters by |edge| >= min_edge.
    """
    # All run-ups that have decision nodes (active first, then expired)
    active_runups = (
        db.query(RunUp)
        .filter(RunUp.merged_into_id.is_(None))
        .order_by(
            case((RunUp.status == "active", 0), else_=1),
            desc(RunUp.current_score),
        )
        .all()
    )

    opportunities = []
    for ru in active_runups:
        # Get root node
        root_node = (
            db.query(DecisionNode)
            .filter(DecisionNode.run_up_id == ru.id, DecisionNode.depth == 0)
            .first()
        )
        if not root_node:
            continue

        # Skip resolved nodes — the event already happened
        if root_node.status in ("confirmed_yes", "confirmed_no"):
            continue

        our_prob = round((root_node.yes_probability or 0.5) * 100, 1)

        # Best Polymarket match
        best_pm = (
            db.query(PolymarketMatch)
            .filter(PolymarketMatch.run_up_id == ru.id)
            .order_by(PolymarketMatch.match_score.desc())
            .first()
        )
        if not best_pm or best_pm.outcome_yes_price is None:
            continue

        market_prob = round(best_pm.outcome_yes_price * 100, 1)
        edge = round(our_prob - market_prob, 1)

        if abs(edge) < min_edge:
            continue

        # Latest swarm verdict for root node (skip failed evaluations)
        sv = (
            db.query(SwarmVerdict)
            .filter(
                SwarmVerdict.decision_node_id == root_node.id,
                SwarmVerdict.superseded_at.is_(None),
                SwarmVerdict.confidence > 0,
            )
            .order_by(SwarmVerdict.created_at.desc())
            .first()
        )
        # Also skip verdicts whose entry_reasoning indicates synthesis failure
        if sv and sv.entry_reasoning and "Synthesis failed" in sv.entry_reasoning:
            sv = None

        opportunities.append({
            "run_up_id": ru.id,
            "question": root_node.question or ru.narrative_name,
            "our_probability": our_prob,
            "market_probability": market_prob,
            "edge": edge,
            "edge_direction": "long" if edge > 0 else "short",
            "polymarket_question": best_pm.polymarket_question,
            "polymarket_url": best_pm.polymarket_url,
            "volume": best_pm.volume,
            "match_score": best_pm.match_score,
            "narrative_name": ru.narrative_name,
            "article_count": ru.article_count_total,
            "days_active": (date.today() - ru.start_date).days if ru.start_date else 0,
            "swarm_verdict": sv.verdict if sv else None,
            "swarm_confidence": round(sv.confidence * 100) if sv else None,
            "swarm_ticker": sv.primary_ticker if sv else None,
            "swarm_ticker_direction": sv.ticker_direction if sv else None,
        })

    # Sort by abs(edge) descending
    opportunities.sort(key=lambda x: abs(x["edge"]), reverse=True)
    return opportunities


# ===========================================================================
# Serialisation helpers
# ===========================================================================

def _parse_kw(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    try:
        kws = json.loads(raw)
        return [str(k).lower().strip() for k in kws] if isinstance(kws, list) else []
    except Exception:
        return []


def _brief_to_dict(b: ArticleBrief) -> Dict:
    article = b.article
    return {
        "id": b.id,
        "article_id": b.article_id,
        "title": article.title if article else None,
        "source": article.source if article else None,
        "link": article.link if article else None,
        "pub_date": article.pub_date.isoformat() if article and article.pub_date else None,
        "region": b.region,
        "entities": _safe_json(b.entities_json),
        "keywords": _safe_json(b.keywords_json),
        "sentiment": b.sentiment,
        "intensity": b.intensity,
        "summary": b.summary,
        "topic_cluster_id": b.topic_cluster_id,
        "processed_at": b.processed_at.isoformat() if b.processed_at else None,
    }


def _timeline_to_dict(t: NarrativeTimeline) -> Dict:
    return {
        "id": t.id,
        "narrative_name": t.narrative_name,
        "topic_cluster_id": t.topic_cluster_id,
        "date": t.date.isoformat() if t.date else None,
        "article_count": t.article_count,
        "sources_count": t.sources_count,
        "unique_regions": t.unique_regions,
        "avg_sentiment": t.avg_sentiment,
        "intensity_score": t.intensity_score,
        "trend": t.trend,
    }


def _runup_to_dict(r: RunUp) -> Dict:
    return {
        "id": r.id,
        "narrative_name": r.narrative_name,
        "detected_at": r.detected_at.isoformat() if r.detected_at else None,
        "start_date": r.start_date.isoformat() if r.start_date else None,
        "current_score": r.current_score,
        "acceleration_rate": r.acceleration_rate,
        "article_count_total": r.article_count_total,
        "status": r.status,
        "is_focused": bool(getattr(r, "is_focused", False)),
    }


def _node_to_dict(n: DecisionNode) -> Dict:
    return {
        "id": n.id,
        "run_up_id": n.run_up_id,
        "parent_node_id": n.parent_node_id,
        "branch": n.branch,
        "question": n.question,
        "yes_probability": n.yes_probability,
        "no_probability": n.no_probability,
        "yes_keywords": _safe_json(n.yes_keywords_json),
        "no_keywords": _safe_json(n.no_keywords_json),
        "depth": n.depth,
        "timeline_estimate": n.timeline_estimate,
        "status": n.status,
        "confirmed_at": n.confirmed_at.isoformat() if n.confirmed_at else None,
        "evidence": n.evidence,
        "created_at": n.created_at.isoformat() if n.created_at else None,
        "updated_at": n.updated_at.isoformat() if n.updated_at else None,
    }


def _consequence_to_dict(c: Consequence) -> Dict:
    return {
        "id": c.id,
        "decision_node_id": c.decision_node_id,
        "branch": c.branch,
        "order": c.order,
        "description": c.description,
        "probability": c.probability,
        "impact_economic": c.impact_economic,
        "impact_geopolitical": c.impact_geopolitical,
        "impact_social": c.impact_social,
        "keywords": _safe_json(c.keywords_json),
        "status": c.status,
        "confirmed_at": c.confirmed_at.isoformat() if c.confirmed_at else None,
        "evidence": c.evidence,
    }


def _prob_update_to_dict(u: ProbabilityUpdate) -> Dict:
    return {
        "id": u.id,
        "target_type": u.target_type,
        "target_id": u.target_id,
        "prior": u.prior,
        "posterior": u.posterior,
        "shift": round(u.posterior - u.prior, 4),
        "evidence_count": u.evidence_count,
        "evidence_briefs": _safe_json(u.evidence_briefs_json),
        "evidence_summary": u.evidence_summary,
        "updated_at": u.updated_at.isoformat() if u.updated_at else None,
    }


def _prediction_to_dict(p: Prediction) -> Dict:
    return {
        "id": p.id,
        "run_up_id": p.run_up_id,
        "decision_node_id": p.decision_node_id,
        "consequence_id": p.consequence_id,
        "prediction_text": p.prediction_text,
        "confidence": p.confidence,
        "branch": p.branch,
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "deadline": p.deadline.isoformat() if p.deadline else None,
        "outcome": p.outcome,
        "verified_at": p.verified_at.isoformat() if p.verified_at else None,
        "evidence": p.evidence,
    }


def _stock_impact_to_dict(si: StockImpact) -> Dict:
    from .bunq_stocks import is_available_on_bunq
    return {
        "id": si.id,
        "consequence_id": si.consequence_id,
        "ticker": si.ticker,
        "name": si.name,
        "asset_type": si.asset_type,
        "direction": si.direction,
        "magnitude": si.magnitude,
        "reasoning": si.reasoning,
        "available_on_bunq": is_available_on_bunq(si.ticker),
    }


def _polymarket_to_dict(m: PolymarketMatch) -> Dict:
    return {
        "id": m.id,
        "run_up_id": m.run_up_id,
        "decision_node_id": m.decision_node_id,
        "polymarket_id": m.polymarket_id,
        "polymarket_slug": m.polymarket_slug,
        "polymarket_question": m.polymarket_question,
        "polymarket_url": m.polymarket_url,
        "outcome_yes_price": m.outcome_yes_price,
        "outcome_no_price": m.outcome_no_price,
        "volume": m.volume,
        "liquidity": m.liquidity,
        "end_date": m.end_date.isoformat() if m.end_date else None,
        "match_score": m.match_score,
        "match_method": m.match_method,
        "calibrated_probability": m.calibrated_probability,
        "fetched_at": m.fetched_at.isoformat() if m.fetched_at else None,
        "updated_at": m.updated_at.isoformat() if m.updated_at else None,
    }


def _safe_json(raw: Optional[str]) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


# ---------------------------------------------------------------------------
# Daily Investment Advisory
# ---------------------------------------------------------------------------

@router.get("/advisory/latest")
def advisory_latest(db: Session = Depends(_get_db)):
    """Return the most recent daily advisory with portfolio context."""
    report = (
        db.query(AnalysisReport)
        .filter(AnalysisReport.report_type == "daily_advisory")
        .order_by(AnalysisReport.created_at.desc())
        .first()
    )
    if not report:
        return {"advisory": None, "message": "No advisory generated yet."}

    try:
        data = json.loads(report.report_json)
    except Exception:
        data = {}

    performance = None
    if report.performance_json:
        try:
            performance = json.loads(report.performance_json)
        except Exception:
            pass

    # Inject live portfolio holdings for UI
    from .db import EngineSettings
    holdings = []
    s = db.query(EngineSettings).get("portfolio_holdings")
    if s and s.value:
        try:
            holdings = json.loads(s.value)
        except Exception:
            pass

    return {
        "advisory": data,
        "report_id": report.id,
        "created_at": report.created_at.isoformat() if report.created_at else None,
        "performance": performance,
        "portfolio_holdings": holdings,
    }


@router.get("/advisory/history")
def advisory_history(
    limit: int = Query(30, ge=1, le=90),
    db: Session = Depends(_get_db),
):
    """Return historical advisories with performance data."""
    reports = (
        db.query(AnalysisReport)
        .filter(AnalysisReport.report_type == "daily_advisory")
        .order_by(AnalysisReport.created_at.desc())
        .limit(limit)
        .all()
    )

    history = []
    for r in reports:
        try:
            data = json.loads(r.report_json)
        except Exception:
            data = {}

        performance = None
        if r.performance_json:
            try:
                performance = json.loads(r.performance_json)
            except Exception:
                pass

        # Summary stats from outcomes
        outcomes = data.get("outcomes", {})
        total_evals = 0
        total_correct = 0
        avg_return = 0.0
        returns = []
        for ticker, horizons in outcomes.items():
            for h_key, result in horizons.items():
                total_evals += 1
                if result.get("correct"):
                    total_correct += 1
                ret = result.get("return_pct", 0)
                returns.append(ret)

        history.append({
            "report_id": r.id,
            "date": r.period_start.isoformat() if r.period_start else None,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "market_stance": data.get("market_stance"),
            "buy_count": len(data.get("buy_recommendations", [])),
            "sell_count": len(data.get("sell_recommendations", [])),
            "buy_tickers": [
                rec.get("ticker") for rec in data.get("buy_recommendations", [])
            ],
            "sell_tickers": [
                rec.get("ticker") for rec in data.get("sell_recommendations", [])
            ],
            "outcomes_evaluated": total_evals,
            "outcomes_correct": total_correct,
            "accuracy": round(total_correct / total_evals, 4) if total_evals > 0 else None,
            "avg_return_pct": round(sum(returns) / len(returns), 2) if returns else None,
            "performance": performance,
        })

    # Also fetch overall learning stats
    from .db import EngineSettings
    stats = {}
    for key in ("advisory_accuracy", "advisory_total_checks", "advisory_total_hits",
                "advisory_brier_scores", "advisory_weights", "advisory_component_emas"):
        s = db.query(EngineSettings).get(key)
        if s and s.value:
            try:
                stats[key] = json.loads(s.value)
            except (json.JSONDecodeError, ValueError):
                stats[key] = s.value

    return {
        "history": history,
        "total_advisories": len(reports),
        "learning_stats": stats,
    }


@router.post("/advisory/generate")
def advisory_generate():
    """Manually trigger daily advisory generation."""
    try:
        from .daily_advisory import generate_daily_advisory
        report = generate_daily_advisory()
        if not report:
            return {"success": False, "message": "Advisory generation failed — check logs."}

        data = json.loads(report.report_json) if report.report_json else {}
        return {
            "success": True,
            "report_id": report.id,
            "market_stance": data.get("market_stance"),
            "buy_count": len(data.get("buy_recommendations", [])),
            "sell_count": len(data.get("sell_recommendations", [])),
            "advisory": data,
        }
    except Exception as e:
        logger.exception("Manual advisory generation failed.")
        raise HTTPException(status_code=500, detail=str(e))


# ── Portfolio Holdings ──────────────────────────────────────────────────


@router.get("/portfolio/holdings")
def portfolio_holdings(db: Session = Depends(_get_db)):
    """Return user's current portfolio holdings."""
    from .db import EngineSettings
    s = db.query(EngineSettings).get("portfolio_holdings")
    if not s or not s.value:
        return {"holdings": [], "total_value": 0}
    try:
        holdings = json.loads(s.value)
    except (json.JSONDecodeError, ValueError):
        holdings = []

    total = sum(h.get("value_eur", 0) for h in holdings)
    total_pnl = sum(h.get("pnl_eur", 0) for h in holdings)
    return {"holdings": holdings, "total_value": round(total, 2), "total_pnl": round(total_pnl, 2)}


@router.put("/portfolio/holdings")
def portfolio_holdings_update(
    payload: Dict[str, Any] = Body(...),
    db: Session = Depends(_get_db),
):
    """Update user's current portfolio holdings.

    Body: { "holdings": [ {"ticker": "XOM", "name": "Exxon Mobil", "value_eur": 10.89}, ... ] }
    """
    holdings = payload.get("holdings", [])

    # Validate
    cleaned = []
    for h in holdings:
        ticker = (h.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        cleaned.append({
            "ticker": ticker,
            "name": h.get("name", ticker),
            "value_eur": round(float(h.get("value_eur", 0)), 2),
            "pnl_eur": round(float(h.get("pnl_eur", 0)), 2),
            "updated_at": datetime.utcnow().isoformat(),
        })

    from .db import EngineSettings
    s = db.query(EngineSettings).get("portfolio_holdings")
    if s:
        s.value = json.dumps(cleaned)
    else:
        s = EngineSettings(key="portfolio_holdings", value=json.dumps(cleaned))
        db.add(s)
    db.commit()

    total = sum(c["value_eur"] for c in cleaned)
    return {"success": True, "holdings": cleaned, "total_value": round(total, 2)}


@router.get("/portfolio/alignment")
def portfolio_alignment(db: Session = Depends(_get_db)):
    """Compare current portfolio against latest advisory and return alignment analysis."""
    from .db import EngineSettings

    # 1. Get holdings
    s = db.query(EngineSettings).get("portfolio_holdings")
    holdings = []
    if s and s.value:
        try:
            holdings = json.loads(s.value)
        except Exception:
            pass

    if not holdings:
        return {"alignment": [], "message": "No portfolio holdings configured."}

    # 2. Get latest advisory
    report = (
        db.query(AnalysisReport)
        .filter(AnalysisReport.report_type == "daily_advisory")
        .order_by(AnalysisReport.created_at.desc())
        .first()
    )
    if not report:
        return {"alignment": [], "message": "No advisory generated yet."}

    try:
        advisory = json.loads(report.report_json)
    except Exception:
        return {"alignment": [], "message": "Failed to parse advisory."}

    buy_map = {
        r["ticker"]: r for r in advisory.get("buy_recommendations", [])
    }
    sell_map = {
        r["ticker"]: r for r in advisory.get("sell_recommendations", [])
    }

    # 3. Analyse each holding
    alignment = []
    for h in holdings:
        t = h["ticker"]
        entry = {
            "ticker": t,
            "name": h.get("name", t),
            "value_eur": h.get("value_eur", 0),
            "pnl_eur": h.get("pnl_eur", 0),
        }

        if t in buy_map:
            rec = buy_map[t]
            entry["signal"] = "HOLD_ADD"
            entry["label"] = "✅ Aanhouden + bijkopen"
            entry["reasoning"] = rec.get("reasoning", "In BUY aanbevelingen")
            entry["composite_score"] = rec.get("composite_score")
            entry["rank"] = rec.get("rank")
        elif t in sell_map:
            rec = sell_map[t]
            entry["signal"] = "REDUCE"
            entry["label"] = "⚠️ Afbouwen / verkopen"
            entry["reasoning"] = rec.get("reasoning", "In SELL aanbevelingen")
            entry["composite_score"] = rec.get("composite_score")
            entry["rank"] = rec.get("rank")
        else:
            # Check sector alignment
            sector_outlook = advisory.get("sectors_outlook", [])
            relevant_sectors = _match_holding_sectors(t, h.get("name", ""), sector_outlook)
            if relevant_sectors:
                best = relevant_sectors[0]
                direction = best.get("direction", "neutral")
                if direction == "bullish":
                    entry["signal"] = "HOLD"
                    entry["label"] = f"🟢 Aanhouden — {best['sector']} bullish"
                elif direction == "bearish":
                    entry["signal"] = "WATCH"
                    entry["label"] = f"🟡 Monitoren — {best['sector']} bearish"
                else:
                    entry["signal"] = "NEUTRAL"
                    entry["label"] = "⚪ Geen signaal"
                entry["reasoning"] = best.get("reasoning", "")
            else:
                # Detect sector from ticker/name even without outlook match
                _sector = _detect_sector(t, h.get("name", ""))
                if _sector:
                    entry["signal"] = "HOLD"
                    entry["label"] = f"🟢 Aanhouden — {_sector}"
                    entry["reasoning"] = "Geen actief signaal; positie behouden."
                else:
                    entry["signal"] = "NEUTRAL"
                    entry["label"] = "⚪ Geen signaal"
                    entry["reasoning"] = "Niet in huidige advisory — geen actie nodig"

        alignment.append(entry)

    # 4. Missed opportunities: BUY recs not in portfolio
    held_tickers = {h["ticker"] for h in holdings}
    missed = [
        {
            "ticker": r["ticker"],
            "name": r["name"],
            "action": "BUY",
            "composite_score": r["composite_score"],
            "reasoning": r.get("reasoning", ""),
            "current_price": r.get("current_price"),
        }
        for r in advisory.get("buy_recommendations", [])
        if r["ticker"] not in held_tickers
    ]

    return {
        "alignment": alignment,
        "missed_opportunities": missed,
        "market_stance": advisory.get("market_stance"),
        "advisory_date": advisory.get("generated_at"),
    }


def _match_holding_sectors(
    ticker: str, name: str, sectors: list
) -> list:
    """Match a holding to relevant sector outlooks."""
    lower_name = name.lower()
    matches = []

    SECTOR_KEYWORDS = {
        "Energy": ["oil", "gas", "energy", "petroleum", "crude", "fuel",
                    "xle", "uso", "xop", "is0d", "exploration", "exxon", "xom"],
        "Precious Metals": ["gold", "silver", "precious", "mining", "metal", "copper",
                            "gld", "slv", "gdx", "ring", "pick", "is0e",
                            "wmin", "vaneck", "producer", "miner"],
        "Defense & Aerospace": ["defense", "defence", "aerospace", "military",
                                "lmt", "rtx", "noc", "ita"],
        "Emerging Markets": ["emerging", "eem", "iema", "developing"],
        "Credit": ["bond", "yield", "credit", "hyg", "tlt", "high yield",
                    "dividend", "ispa", "select dividend", "stoxx"],
        "Technology": ["tech", "software", "semiconductor", "xlk", "qqq"],
    }

    ticker_lower = ticker.lower()
    for sector_info in sectors:
        sector_name = sector_info.get("sector", "")
        # Match flexibly — Claude may return "Energy (XOM, XLE)" not just "Energy"
        keywords = []
        for key, kwlist in SECTOR_KEYWORDS.items():
            if sector_name.lower().startswith(key.lower()):
                keywords = kwlist
                break
        if any(kw in lower_name or kw in ticker_lower for kw in keywords):
            matches.append(sector_info)

    return matches


def _detect_sector(ticker: str, name: str):
    """Detect sector label from ticker/name, independent of sectors_outlook."""
    lower = (name + " " + ticker).lower()
    SECTOR_MAP = [
        ("Energy / Oil & Gas", ["oil", "gas", "energy", "petroleum", "crude",
                                 "is0d", "xle", "exxon", "xom"]),
        ("Mining & Grondstoffen", ["mining", "miner", "mineral", "copper",
                                    "wmin", "vaneck", "glen", "bhp", "rio",
                                    "teck", "fcx", "scco"]),
        ("Goud & Edelmetalen", ["gold", "silver", "precious", "producer",
                                 "is0e", "gdx"]),
        ("Dividend", ["dividend", "select dividend", "stoxx", "ispa", "vhyl",
                       "yield", "income"]),
        ("Defensie", ["defense", "defence", "aerospace", "military"]),
        ("Technologie", ["tech", "software", "semiconductor"]),
    ]
    for sector_label, keywords in SECTOR_MAP:
        if any(kw in lower for kw in keywords):
            return sector_label
    return None


# ── Telegram Notifications ────────────────────────────────────────────


@router.get("/telegram/status")
def telegram_status(db: Session = Depends(_get_db)):
    """Check if Telegram notifications are configured."""
    from .telegram_notifier import is_telegram_configured
    from .db import EngineSettings

    configured = is_telegram_configured()

    # Get stored values (masked)
    token_set = False
    chat_id_set = False
    s = db.query(EngineSettings).get("telegram_bot_token")
    if s and s.value:
        token_set = True
    s = db.query(EngineSettings).get("telegram_chat_id")
    if s and s.value:
        chat_id_set = True

    return {
        "configured": configured,
        "token_set": token_set,
        "chat_id_set": chat_id_set,
    }


@router.put("/telegram/configure")
def telegram_configure(
    payload: Dict[str, Any] = Body(...),
    db: Session = Depends(_get_db),
):
    """Configure Telegram bot token and chat ID.

    Body: { "bot_token": "...", "chat_id": "..." }
    """
    from .db import EngineSettings

    token = (payload.get("bot_token") or "").strip()
    chat_id = (payload.get("chat_id") or "").strip()

    if not token or not chat_id:
        raise HTTPException(status_code=400, detail="Both bot_token and chat_id are required.")

    for key, val in [("telegram_bot_token", token), ("telegram_chat_id", chat_id)]:
        s = db.query(EngineSettings).get(key)
        if s:
            s.value = val
        else:
            db.add(EngineSettings(key=key, value=val))
    db.commit()

    return {"success": True, "message": "Telegram configured. Use /api/telegram/test to verify."}


@router.post("/telegram/test")
def telegram_test():
    """Send a test message via Telegram to verify configuration."""
    from .telegram_notifier import send_test_message, is_telegram_configured
    if not is_telegram_configured():
        return {"success": False, "message": "Telegram not configured. Set bot_token and chat_id first."}

    sent = send_test_message()
    return {
        "success": sent,
        "message": "Test message sent!" if sent else "Failed to send — check token and chat_id.",
    }


@router.post("/telegram/send-advisory")
def telegram_send_advisory(db: Session = Depends(_get_db)):
    """Manually send the latest advisory via Telegram."""
    from .telegram_notifier import send_advisory_notification, is_telegram_configured

    if not is_telegram_configured():
        return {"success": False, "message": "Telegram not configured."}

    report = (
        db.query(AnalysisReport)
        .filter(AnalysisReport.report_type == "daily_advisory")
        .order_by(AnalysisReport.created_at.desc())
        .first()
    )
    if not report:
        return {"success": False, "message": "No advisory available to send."}

    try:
        data = json.loads(report.report_json)
    except Exception:
        return {"success": False, "message": "Failed to parse advisory data."}

    sent = send_advisory_notification(data)
    return {
        "success": sent,
        "message": "Advisory sent via Telegram!" if sent else "Failed to send.",
    }


# ---------------------------------------------------------------------------
# ML endpoints (V2.0)
# ---------------------------------------------------------------------------

@router.get("/ml/status")
def ml_status():
    """Get ML model status, metrics, and feature importance."""
    try:
        from .ml.inference import get_model_status
        return get_model_status()
    except ImportError:
        return {"status": "not_installed", "message": "ML module not available."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/ml/predictions")
def ml_predictions():
    """Get ML predictions for all active run-ups."""
    try:
        from .ml.inference import predict_signal
        session = get_session()
        try:
            from .db import RunUp
            active = session.query(RunUp).filter(
                RunUp.status == "active",
                RunUp.merged_into_id.is_(None),
            ).all()

            predictions = []
            for ru in active:
                score = predict_signal(ru.id)
                predictions.append({
                    "run_up_id": ru.id,
                    "narrative_name": ru.narrative_name,
                    "ml_score": round(score, 4),
                    "signal": "bullish" if score > 0.6 else "bearish" if score < 0.4 else "neutral",
                })
            return {"predictions": predictions, "count": len(predictions)}
        finally:
            session.close()
    except ImportError:
        return {"predictions": [], "count": 0, "message": "ML module not available."}


@router.post("/ml/retrain")
def ml_retrain():
    """Force ML model retrain on current data."""
    try:
        from .ml.prepare import extract_all
        from .ml.train import train

        features, labels = extract_all()
        if features.empty:
            return {"success": False, "message": "No features available."}

        metadata = train(force=True)
        if metadata:
            return {
                "success": True,
                "metrics": metadata.get("metrics", {}),
                "n_samples": metadata.get("n_samples", 0),
                "training_seconds": metadata.get("training_seconds", 0),
            }
        return {"success": False, "message": "Training produced no model."}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.get("/ml/experiments")
def ml_experiments():
    """Get experiment history from results.tsv."""
    import csv
    from pathlib import Path
    results_path = Path(__file__).parent / "ml" / "results.tsv"
    if not results_path.exists():
        return {"experiments": [], "count": 0}

    experiments = []
    with open(results_path) as f:
        reader = csv.DictReader(f, delimiter="\t",
                                fieldnames=["commit", "sharpe", "hit_rate", "brier", "status", "description"])
        for row in reader:
            experiments.append(row)
    return {"experiments": experiments, "count": len(experiments)}
