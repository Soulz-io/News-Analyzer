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
    ProbabilityUpdate,
    Prediction,
)
from .probability_engine import get_significant_shifts

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


# ===========================================================================
# DASHBOARD ENDPOINTS (read-only)
# ===========================================================================

@router.get("/dashboard/overview")
def dashboard_overview(db: Session = Depends(_get_db)):
    """High-level stats for the dashboard landing page."""
    total_articles = db.query(func.count(Article.id)).scalar() or 0
    total_briefs = db.query(func.count(ArticleBrief.id)).scalar() or 0
    active_runups = (
        db.query(func.count(RunUp.id)).filter(RunUp.status == "active").scalar() or 0
    )
    open_nodes = (
        db.query(func.count(DecisionNode.id))
        .filter(DecisionNode.status == "open")
        .scalar()
        or 0
    )
    pending_predictions = (
        db.query(func.count(Prediction.id))
        .filter(Prediction.outcome == "pending")
        .scalar()
        or 0
    )

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
        "total_articles": total_articles,
        "total_briefs": total_briefs,
        "active_runups": active_runups,
        "open_decision_nodes": open_nodes,
        "pending_predictions": pending_predictions,
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
        tree_nodes.append({
            **_node_to_dict(n),
            "consequences": [_consequence_to_dict(c) for c in consequences],
            "children_ids": [ch.id for ch in n.children] if n.children else [],
        })

    return {
        "run_up": _runup_to_dict(ru),
        "tree": tree_nodes,
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
        "engine_port": config.engine_port,
        "database": config.database_uri,
        "feeds_configured": len(config.feeds),
        "total_articles": article_count,
        "total_briefs": brief_count,
        "fetch_interval_minutes": config.fetch_interval_minutes,
    }


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


def _safe_json(raw: Optional[str]) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw
