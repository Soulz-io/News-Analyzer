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

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, desc
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
)
from .narrative_tracker import update_narratives, detect_runups, consolidate_runups
from .probability_engine import get_significant_shifts
from .tree_generator import generate_tree, generate_trees_for_new_runups

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

    node.status = f"{payload.branch}_confirmed"
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

    # Active run-ups — exclude merged ones (only show consolidated topics)
    active_runup_rows = (
        db.query(RunUp)
        .filter(RunUp.status == "active", RunUp.merged_into_id.is_(None))
        .order_by(desc(RunUp.current_score))
        .all()
    )

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

    return {
        "runups": runups_out,
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
            # Attach stock impacts
            impacts = (
                db.query(StockImpact)
                .filter(StockImpact.consequence_id == c.id)
                .all()
            )
            cd["stock_impacts"] = [_stock_impact_to_dict(si) for si in impacts]
            cons_dicts.append(cd)

        tree_nodes.append({
            **_node_to_dict(n),
            "consequences": cons_dicts,
            "children_ids": [ch.id for ch in n.children] if n.children else [],
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
