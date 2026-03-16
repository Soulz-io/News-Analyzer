"""SQLAlchemy models and database utilities for the News Analyzer engine.

All models use SQLite as the backing store.  The database file lives inside
the DATA_DIR configured in ``config.py``.
"""

import json
import logging
from datetime import datetime, date
from typing import Optional, List, Any

from sqlalchemy import (
    create_engine,
    Boolean,
    Column,
    Index,
    Integer,
    Float,
    String,
    Text,
    DateTime,
    Date,
    ForeignKey,
    UniqueConstraint,
    event,
)
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    relationship,
    Session,
)

from .config import config

logger = logging.getLogger(__name__)

Base = declarative_base()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_loads(raw: Optional[str]) -> Any:
    """Safely deserialise a JSON text column."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


def _json_dumps(obj: Any) -> Optional[str]:
    """Serialise a Python object to a JSON string for storage."""
    if obj is None:
        return None
    return json.dumps(obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Article(Base):
    """A single news article fetched from an RSS feed."""

    __tablename__ = "articles"
    __table_args__ = (
        Index("idx_article_pubdate", "pub_date"),
        Index("idx_article_source", "source"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    title: str = Column(String(512), nullable=False)
    description: Optional[str] = Column(Text, nullable=True)
    link: str = Column(String(1024), nullable=False, unique=True)
    source: str = Column(String(256), nullable=False)
    pub_date: Optional[datetime] = Column(DateTime, nullable=True)
    fetched_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    original_lang: Optional[str] = Column(String(16), nullable=True)

    # Relationship to brief
    brief = relationship("ArticleBrief", back_populates="article", uselist=False)

    def __repr__(self) -> str:
        return f"<Article id={self.id} source={self.source!r} title={self.title[:50]!r}>"


class UserFeed(Base):
    """A user-added custom RSS feed."""

    __tablename__ = "user_feeds"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    name: str = Column(String, nullable=False)
    url: str = Column(String, nullable=False, unique=True)
    region: str = Column(String, default="global")
    lang: str = Column(String, default="en")
    enabled: bool = Column(Boolean, default=True)
    added_at: datetime = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<UserFeed id={self.id} name={self.name!r} url={self.url!r} enabled={self.enabled}>"


class ArticleBrief(Base):
    """NLP-enriched summary of an article."""

    __tablename__ = "article_briefs"
    __table_args__ = (
        Index("idx_brief_region", "region"),
        Index("idx_brief_processed_at", "processed_at"),
        Index("idx_brief_cluster", "topic_cluster_id"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    article_id: int = Column(Integer, ForeignKey("articles.id"), nullable=False, unique=True)
    region: str = Column(String(64), nullable=False, default="global")
    entities_json: Optional[str] = Column(Text, nullable=True)
    keywords_json: Optional[str] = Column(Text, nullable=True)
    sentiment: float = Column(Float, nullable=False, default=0.0)
    intensity: str = Column(String(32), nullable=False, default="low")
    summary: Optional[str] = Column(Text, nullable=True)
    topic_cluster_id: Optional[int] = Column(Integer, nullable=True)
    processed_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Extended analysis fields (v2)
    urgency_score: float = Column(Float, nullable=False, default=0.0)
    source_credibility: float = Column(Float, nullable=False, default=0.6)
    key_actors_json: Optional[str] = Column(Text, nullable=True)
    event_type: Optional[str] = Column(String(32), nullable=True)

    # Relationship back to article
    article = relationship("Article", back_populates="brief")

    # ------------------------------------------------------------------
    # Convenience properties for JSON fields
    # ------------------------------------------------------------------
    @property
    def entities(self) -> Any:
        return _json_loads(self.entities_json)

    @entities.setter
    def entities(self, value: Any) -> None:
        self.entities_json = _json_dumps(value)

    @property
    def keywords(self) -> Any:
        return _json_loads(self.keywords_json)

    @keywords.setter
    def keywords(self, value: Any) -> None:
        self.keywords_json = _json_dumps(value)

    def __repr__(self) -> str:
        return (
            f"<ArticleBrief id={self.id} article_id={self.article_id} "
            f"region={self.region!r} intensity={self.intensity!r}>"
        )


class NarrativeTimeline(Base):
    """Daily snapshot of a narrative's progression."""

    __tablename__ = "narrative_timeline"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    narrative_name: str = Column(String(256), nullable=False)
    topic_cluster_id: Optional[int] = Column(Integer, nullable=True)
    date: date = Column(Date, nullable=False)
    article_count: int = Column(Integer, nullable=False, default=0)
    sources_count: int = Column(Integer, nullable=False, default=0)
    unique_regions: int = Column(Integer, nullable=False, default=0)
    avg_sentiment: float = Column(Float, nullable=False, default=0.0)
    intensity_score: float = Column(Float, nullable=False, default=0.0)
    trend: str = Column(String(32), nullable=False, default="stable")

    __table_args__ = (
        UniqueConstraint("narrative_name", "date", name="uq_narrative_date"),
        Index("idx_nt_name_date", "narrative_name", "date"),
    )

    def __repr__(self) -> str:
        return (
            f"<NarrativeTimeline narrative={self.narrative_name!r} "
            f"date={self.date} articles={self.article_count}>"
        )


class RunUp(Base):
    """A detected escalation / run-up in a narrative."""

    __tablename__ = "run_ups"
    __table_args__ = (
        Index("idx_runup_status", "status"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    narrative_name: str = Column(String(256), nullable=False)
    detected_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    start_date: date = Column(Date, nullable=False)
    current_score: float = Column(Float, nullable=False, default=0.0)
    acceleration_rate: float = Column(Float, nullable=False, default=0.0)
    article_count_total: int = Column(Integer, nullable=False, default=0)
    status: str = Column(String(32), nullable=False, default="active")
    # When merged into another run-up, points to the primary
    merged_into_id: Optional[int] = Column(
        Integer, ForeignKey("run_ups.id"), nullable=True
    )
    # Focus Mode: concentrated analysis on this narrative
    is_focused: bool = Column(Boolean, nullable=False, default=False)

    # Relationships
    decision_nodes = relationship("DecisionNode", back_populates="run_up")
    predictions = relationship("Prediction", back_populates="run_up")
    polymarket_matches = relationship("PolymarketMatch", back_populates="run_up")
    merged_children = relationship(
        "RunUp",
        backref="merged_into",
        remote_side="RunUp.id",
        foreign_keys="RunUp.merged_into_id",
    )

    def __repr__(self) -> str:
        return (
            f"<RunUp id={self.id} narrative={self.narrative_name!r} "
            f"score={self.current_score:.1f} status={self.status!r}>"
        )


class DecisionNode(Base):
    """A node in a decision tree attached to a RunUp."""

    __tablename__ = "decision_nodes"
    __table_args__ = (
        Index("idx_dn_runup_status", "run_up_id", "status"),
        Index("idx_dn_parent", "parent_node_id"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    run_up_id: int = Column(Integer, ForeignKey("run_ups.id"), nullable=False)
    parent_node_id: Optional[int] = Column(
        Integer, ForeignKey("decision_nodes.id"), nullable=True
    )
    branch: str = Column(String(16), nullable=False, default="root")
    question: str = Column(Text, nullable=False)
    yes_probability: float = Column(Float, nullable=False, default=0.5)
    no_probability: float = Column(Float, nullable=False, default=0.5)
    yes_keywords_json: Optional[str] = Column(Text, nullable=True)
    no_keywords_json: Optional[str] = Column(Text, nullable=True)
    depth: int = Column(Integer, nullable=False, default=0)
    timeline_estimate: Optional[str] = Column(String(128), nullable=True)
    status: str = Column(String(32), nullable=False, default="open")
    confirmed_at: Optional[datetime] = Column(DateTime, nullable=True)
    evidence: Optional[str] = Column(Text, nullable=True)
    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: datetime = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Relationships
    run_up = relationship("RunUp", back_populates="decision_nodes")
    parent = relationship("DecisionNode", remote_side="DecisionNode.id", backref="children")
    consequences = relationship("Consequence", back_populates="decision_node")
    predictions = relationship("Prediction", back_populates="decision_node")

    # ------------------------------------------------------------------
    @property
    def yes_keywords(self) -> Any:
        return _json_loads(self.yes_keywords_json)

    @yes_keywords.setter
    def yes_keywords(self, value: Any) -> None:
        self.yes_keywords_json = _json_dumps(value)

    @property
    def no_keywords(self) -> Any:
        return _json_loads(self.no_keywords_json)

    @no_keywords.setter
    def no_keywords(self, value: Any) -> None:
        self.no_keywords_json = _json_dumps(value)

    def __repr__(self) -> str:
        return (
            f"<DecisionNode id={self.id} run_up={self.run_up_id} "
            f"branch={self.branch!r} status={self.status!r}>"
        )


class Consequence(Base):
    """A predicted consequence linked to a DecisionNode branch."""

    __tablename__ = "consequences"
    __table_args__ = (
        Index("idx_cons_node", "decision_node_id"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    decision_node_id: int = Column(
        Integer, ForeignKey("decision_nodes.id"), nullable=False
    )
    branch: str = Column(String(16), nullable=False)
    order: int = Column(Integer, nullable=False)
    description: str = Column(Text, nullable=False)
    probability: float = Column(Float, nullable=False, default=0.5)
    impact_economic: Optional[str] = Column(Text, nullable=True)
    impact_geopolitical: Optional[str] = Column(Text, nullable=True)
    impact_social: Optional[str] = Column(Text, nullable=True)
    keywords_json: Optional[str] = Column(Text, nullable=True)
    status: str = Column(String(32), nullable=False, default="predicted")
    confirmed_at: Optional[datetime] = Column(DateTime, nullable=True)
    evidence: Optional[str] = Column(Text, nullable=True)
    # Proximity tracking
    price_thresholds_json: Optional[str] = Column(Text, nullable=True)
    proximity_pct: Optional[float] = Column(Float, nullable=True)

    # Relationships
    decision_node = relationship("DecisionNode", back_populates="consequences")
    predictions = relationship("Prediction", back_populates="consequence")
    stock_impacts = relationship("StockImpact", back_populates="consequence")

    @property
    def keywords(self) -> Any:
        return _json_loads(self.keywords_json)

    @keywords.setter
    def keywords(self, value: Any) -> None:
        self.keywords_json = _json_dumps(value)

    @property
    def price_thresholds(self) -> Any:
        return _json_loads(self.price_thresholds_json)

    @price_thresholds.setter
    def price_thresholds(self, value: Any) -> None:
        self.price_thresholds_json = _json_dumps(value)

    def __repr__(self) -> str:
        return (
            f"<Consequence id={self.id} node={self.decision_node_id} "
            f"branch={self.branch!r} p={self.probability:.2f}>"
        )


class StockImpact(Base):
    """A stock or ETF affected by a specific consequence."""

    __tablename__ = "stock_impacts"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    consequence_id: int = Column(
        Integer, ForeignKey("consequences.id"), nullable=False
    )
    ticker: str = Column(String(16), nullable=False)
    name: str = Column(String(256), nullable=False)
    asset_type: str = Column(String(32), nullable=False, default="stock")
    direction: str = Column(String(16), nullable=False)
    magnitude: str = Column(String(16), nullable=False, default="moderate")
    reasoning: str = Column(Text, nullable=False, default="")
    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    consequence = relationship("Consequence", back_populates="stock_impacts")

    def __repr__(self) -> str:
        return (
            f"<StockImpact id={self.id} ticker={self.ticker!r} "
            f"dir={self.direction!r} mag={self.magnitude!r}>"
        )


class PolymarketMatch(Base):
    """A Polymarket prediction market matched to a run-up question."""

    __tablename__ = "polymarket_matches"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    run_up_id: int = Column(Integer, ForeignKey("run_ups.id"), nullable=False)
    decision_node_id: Optional[int] = Column(
        Integer, ForeignKey("decision_nodes.id"), nullable=True
    )
    polymarket_id: str = Column(String(64), nullable=False)
    polymarket_slug: Optional[str] = Column(String(256), nullable=True)
    polymarket_question: str = Column(Text, nullable=False)
    polymarket_url: Optional[str] = Column(String(512), nullable=True)
    outcome_yes_price: float = Column(Float, nullable=False, default=0.5)
    outcome_no_price: float = Column(Float, nullable=False, default=0.5)
    volume: Optional[float] = Column(Float, nullable=True)
    liquidity: Optional[float] = Column(Float, nullable=True)
    end_date: Optional[datetime] = Column(DateTime, nullable=True)
    match_score: float = Column(Float, nullable=False, default=0.0)
    match_method: str = Column(String(32), nullable=False, default="keyword")
    calibrated_probability: Optional[float] = Column(Float, nullable=True)
    fetched_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: datetime = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("run_up_id", "polymarket_id", name="uq_runup_polymarket"),
    )

    run_up = relationship("RunUp", back_populates="polymarket_matches")
    decision_node = relationship("DecisionNode", backref="polymarket_matches")

    def __repr__(self) -> str:
        return (
            f"<PolymarketMatch id={self.id} run_up={self.run_up_id} "
            f"yes={self.outcome_yes_price:.2f} score={self.match_score:.1f}>"
        )


class ProbabilityUpdate(Base):
    """Audit log for every Bayesian probability update."""

    __tablename__ = "probability_updates"
    __table_args__ = (
        Index("idx_pu_target", "target_type", "target_id"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    target_type: str = Column(String(32), nullable=False)  # "node" or "consequence"
    target_id: int = Column(Integer, nullable=False)
    prior: float = Column(Float, nullable=False)
    posterior: float = Column(Float, nullable=False)
    evidence_count: int = Column(Integer, nullable=False, default=0)
    evidence_briefs_json: Optional[str] = Column(Text, nullable=True)
    evidence_summary: str = Column(Text, nullable=False, default="")
    updated_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    @property
    def evidence_briefs(self) -> Any:
        return _json_loads(self.evidence_briefs_json)

    @evidence_briefs.setter
    def evidence_briefs(self, value: Any) -> None:
        self.evidence_briefs_json = _json_dumps(value)

    def __repr__(self) -> str:
        return (
            f"<ProbabilityUpdate {self.target_type}:{self.target_id} "
            f"{self.prior:.2f}->{self.posterior:.2f}>"
        )


class Prediction(Base):
    """A concrete prediction made by an agent against a run-up/node/consequence."""

    __tablename__ = "predictions"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    run_up_id: int = Column(Integer, ForeignKey("run_ups.id"), nullable=False)
    decision_node_id: Optional[int] = Column(
        Integer, ForeignKey("decision_nodes.id"), nullable=True
    )
    consequence_id: Optional[int] = Column(
        Integer, ForeignKey("consequences.id"), nullable=True
    )
    prediction_text: str = Column(Text, nullable=False)
    confidence: float = Column(Float, nullable=False, default=0.5)
    branch: str = Column(String(16), nullable=False, default="yes")
    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    deadline: Optional[datetime] = Column(DateTime, nullable=True)
    outcome: str = Column(String(32), nullable=False, default="pending")
    verified_at: Optional[datetime] = Column(DateTime, nullable=True)
    evidence: Optional[str] = Column(Text, nullable=True)

    # Relationships
    run_up = relationship("RunUp", back_populates="predictions")
    decision_node = relationship("DecisionNode", back_populates="predictions")
    consequence = relationship("Consequence", back_populates="predictions")

    def __repr__(self) -> str:
        return (
            f"<Prediction id={self.id} run_up={self.run_up_id} "
            f"outcome={self.outcome!r} confidence={self.confidence:.2f}>"
        )


class TokenUsage(Base):
    """Log of every Claude API call and its cost."""

    __tablename__ = "token_usage"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    model: str = Column(String(64), nullable=False)
    input_tokens: int = Column(Integer, nullable=False, default=0)
    output_tokens: int = Column(Integer, nullable=False, default=0)
    cost_eur: float = Column(Float, nullable=False, default=0.0)
    purpose: str = Column(String(128), nullable=False, default="tree_generation")
    run_up_id: Optional[int] = Column(Integer, ForeignKey("run_ups.id"), nullable=True)

    def __repr__(self) -> str:
        return (
            f"<TokenUsage model={self.model!r} "
            f"in={self.input_tokens} out={self.output_tokens} "
            f"cost=\u20ac{self.cost_eur:.4f}>"
        )


class TradingSignal(Base):
    """A generated trading signal when confidence crosses a threshold.

    Combines run-up momentum, X/Twitter OSINT, Polymarket drift,
    news acceleration, and source convergence into a composite confidence
    score.  Signal levels: WATCH (>=0.40), ALERT (>=0.60), BUY (>=0.75),
    STRONG_BUY (>=0.85).
    """

    __tablename__ = "trading_signals"
    __table_args__ = (
        Index("idx_ts_level", "signal_level"),
        Index("idx_ts_created", "created_at"),
        Index("idx_ts_runup", "run_up_id"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    run_up_id: int = Column(Integer, ForeignKey("run_ups.id"), nullable=False)
    narrative_name: str = Column(String(256), nullable=False)
    ticker: Optional[str] = Column(String(16), nullable=True)
    direction: Optional[str] = Column(String(16), nullable=True)  # bullish/bearish
    confidence: float = Column(Float, nullable=False)
    signal_level: str = Column(String(16), nullable=False)  # WATCH/ALERT/BUY/STRONG_BUY

    # Component scores (transparency)
    runup_score_component: float = Column(Float, nullable=False, default=0.0)
    x_signal_component: float = Column(Float, nullable=False, default=0.0)
    polymarket_drift_component: float = Column(Float, nullable=False, default=0.0)
    news_acceleration_component: float = Column(Float, nullable=False, default=0.0)
    source_convergence_component: float = Column(Float, nullable=False, default=0.0)
    ml_prediction_component: float = Column(Float, nullable=False, default=0.0)

    # Context
    x_signal_count: int = Column(Integer, nullable=False, default=0)
    news_count: int = Column(Integer, nullable=False, default=0)
    polymarket_prob: Optional[float] = Column(Float, nullable=True)
    reasoning: Optional[str] = Column(Text, nullable=True)

    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at: Optional[datetime] = Column(DateTime, nullable=True)
    superseded_by_id: Optional[int] = Column(
        Integer, ForeignKey("trading_signals.id"), nullable=True
    )

    run_up = relationship("RunUp", backref="trading_signals")

    def __repr__(self) -> str:
        return (
            f"<TradingSignal [{self.signal_level}] "
            f"{self.narrative_name} conf={self.confidence:.2f} "
            f"ticker={self.ticker}>"
        )


class PolymarketPriceHistory(Base):
    """Hourly price snapshots for Polymarket drift detection."""

    __tablename__ = "polymarket_price_history"
    __table_args__ = (
        Index("idx_pmph_id_time", "polymarket_id", "recorded_at"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    polymarket_id: str = Column(String(64), nullable=False)
    question: str = Column(Text, nullable=False)
    yes_price: float = Column(Float, nullable=False)
    volume: Optional[float] = Column(Float, nullable=True)
    recorded_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<PolymarketPriceHistory {self.polymarket_id} "
            f"yes={self.yes_price:.2f} at={self.recorded_at}>"
        )


class PriceSnapshot(Base):
    """Periodic price snapshots for tracked tickers — enables news→price correlation.

    Stored every 4 hours for tickers in active StockImpacts + key macro assets.
    Retention: 30 days (cleaned by snapshot job).
    """

    __tablename__ = "price_snapshots"
    __table_args__ = (
        Index("idx_ps_ticker_time", "ticker", "recorded_at"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    ticker: str = Column(String(16), nullable=False)
    price: float = Column(Float, nullable=False)
    recorded_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"<PriceSnapshot {self.ticker} price={self.price:.2f} at={self.recorded_at}>"


def cleanup_old_price_snapshots(max_age_days: int = 30) -> int:
    """Delete PriceSnapshot records older than max_age_days. Returns count deleted."""
    session = get_session()
    try:
        from datetime import timedelta as _td
        cutoff = datetime.utcnow() - _td(days=max_age_days)
        count = session.query(PriceSnapshot).filter(PriceSnapshot.recorded_at < cutoff).delete()
        session.commit()
        return count
    finally:
        session.close()


class EngineSettings(Base):
    """Key-value settings store (e.g. daily budget)."""

    __tablename__ = "engine_settings"

    key: str = Column(String(128), primary_key=True)
    value: str = Column(Text, nullable=False, default="")
    updated_at: datetime = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<EngineSettings {self.key!r}={self.value!r}>"


class AnalysisReport(Base):
    """Deep analysis report generated 2x daily from database patterns."""

    __tablename__ = "analysis_reports"
    __table_args__ = (
        Index("idx_report_type_date", "report_type", "period_end"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    report_type: str = Column(String(64), nullable=False)  # "daily_briefing" | "weekly_briefing"
    period_start: date = Column(Date, nullable=False)
    period_end: date = Column(Date, nullable=False)
    report_json: str = Column(Text, nullable=False)
    performance_json: str = Column(Text, nullable=True)
    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<AnalysisReport type={self.report_type!r} "
            f"period={self.period_start}..{self.period_end}>"
        )


class SwarmVerdict(Base):
    """A swarm consensus verdict for a decision node.

    Produced by the expert panel (7 agents × 3 debate rounds).
    New verdicts supersede old ones for the same node.
    """

    __tablename__ = "swarm_verdicts"
    __table_args__ = (
        Index("idx_sv_node", "decision_node_id"),
        Index("idx_sv_runup", "run_up_id"),
        Index("idx_sv_created", "created_at"),
    )

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    decision_node_id: int = Column(
        Integer, ForeignKey("decision_nodes.id"), nullable=False
    )
    run_up_id: int = Column(Integer, ForeignKey("run_ups.id"), nullable=False)

    # Verdict
    verdict: str = Column(String(16), nullable=False)  # STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL
    confidence: float = Column(Float, nullable=False, default=0.0)  # 0.0-1.0
    yes_probability: float = Column(Float, nullable=False, default=0.5)  # 0.0-1.0
    consensus_strength: float = Column(Float, nullable=False, default=0.0)  # 0.0-1.0

    # Primary trading signal
    primary_ticker: Optional[str] = Column(String(16), nullable=True)
    ticker_direction: Optional[str] = Column(String(8), nullable=True)  # long/short

    # Reasoning
    entry_reasoning: str = Column(Text, nullable=False, default="")
    exit_trigger: str = Column(Text, nullable=False, default="")
    risk_note: str = Column(Text, nullable=False, default="")
    dissent_note: str = Column(Text, nullable=False, default="")

    # Full ticker signals JSON: [{"ticker":"XOM","direction":"long","votes":5}]
    all_ticker_signals_json: Optional[str] = Column(Text, nullable=True)

    # Debate transcript (for detail panel)
    round1_json: Optional[str] = Column(Text, nullable=True)
    round2_json: Optional[str] = Column(Text, nullable=True)

    # Meta
    model_used: str = Column(String(64), nullable=False, default="llama-3.3-70b-versatile")
    context_hash: Optional[str] = Column(String(64), nullable=True)  # SHA-256 of input context
    created_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    superseded_at: Optional[datetime] = Column(DateTime, nullable=True)

    # Relationships
    decision_node = relationship("DecisionNode", backref="swarm_verdicts")
    run_up = relationship("RunUp", backref="swarm_verdicts")

    def __repr__(self) -> str:
        return (
            f"<SwarmVerdict id={self.id} node={self.decision_node_id} "
            f"verdict={self.verdict!r} conf={self.confidence:.0%}>"
        )


# ---------------------------------------------------------------------------
# Engine / Session factory
# ---------------------------------------------------------------------------

_engine = None
_SessionLocal = None


def _get_engine():
    """Create or return the singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            config.database_uri,
            echo=False,
            future=True,
            connect_args={"check_same_thread": False},  # required for SQLite
        )
        # Enable WAL mode for better concurrent read performance with SQLite
        @event.listens_for(_engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        logger.info("Database engine created: %s", config.database_uri)
    return _engine


def get_session_factory():
    """Return the session factory, creating it if necessary."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=_get_engine(), expire_on_commit=False)
    return _SessionLocal


def get_session() -> Session:
    """Open a new database session.

    The caller is responsible for closing it (ideally via a context manager
    or FastAPI dependency injection).
    """
    factory = get_session_factory()
    return factory()


def create_all() -> None:
    """Create all tables if they do not exist, and run lightweight migrations."""
    engine = _get_engine()
    Base.metadata.create_all(bind=engine)

    # Lightweight column migrations for SQLite (ALTER TABLE ADD COLUMN).
    # SQLAlchemy's create_all() creates new tables but won't add columns
    # to existing ones.  We handle that here.
    _migrate_columns(engine)

    logger.info("Database tables created / verified.")


def _migrate_columns(engine) -> None:
    """Add missing columns to existing tables (SQLite ALTER TABLE ADD COLUMN)."""
    import sqlite3

    # Only applies to SQLite
    if "sqlite" not in str(engine.url):
        return

    db_path = str(engine.url).replace("sqlite:///", "")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check article_briefs for new v2 columns
        cursor.execute("PRAGMA table_info(article_briefs)")
        existing_cols = {row[1] for row in cursor.fetchall()}

        migrations = {
            "urgency_score": "FLOAT DEFAULT 0.0",
            "source_credibility": "FLOAT DEFAULT 0.6",
            "key_actors_json": "TEXT DEFAULT NULL",
            "event_type": "VARCHAR(32) DEFAULT NULL",
        }

        for col_name, col_type in migrations.items():
            if col_name not in existing_cols:
                cursor.execute(
                    f"ALTER TABLE article_briefs ADD COLUMN {col_name} {col_type}"
                )
                logger.info("Migration: added column article_briefs.%s", col_name)

        # Check consequences for proximity tracking columns
        cursor.execute("PRAGMA table_info(consequences)")
        cons_cols = {row[1] for row in cursor.fetchall()}

        cons_migrations = {
            "price_thresholds_json": "TEXT DEFAULT NULL",
            "proximity_pct": "FLOAT DEFAULT NULL",
        }
        for col_name, col_type in cons_migrations.items():
            if col_name not in cons_cols:
                cursor.execute(
                    f"ALTER TABLE consequences ADD COLUMN {col_name} {col_type}"
                )
                logger.info("Migration: added column consequences.%s", col_name)

        # Check run_ups for focus column
        cursor.execute("PRAGMA table_info(run_ups)")
        runup_cols = {row[1] for row in cursor.fetchall()}
        if "is_focused" not in runup_cols:
            cursor.execute(
                "ALTER TABLE run_ups ADD COLUMN is_focused BOOLEAN DEFAULT 0"
            )
            logger.info("Migration: added column run_ups.is_focused")

        # Check analysis_reports for performance_json column
        cursor.execute("PRAGMA table_info(analysis_reports)")
        report_cols = {row[1] for row in cursor.fetchall()}
        if "performance_json" not in report_cols:
            cursor.execute(
                "ALTER TABLE analysis_reports ADD COLUMN performance_json TEXT DEFAULT NULL"
            )
            logger.info("Migration: added column analysis_reports.performance_json")

        # Check swarm_verdicts for context_hash column
        cursor.execute("PRAGMA table_info(swarm_verdicts)")
        sv_cols = {row[1] for row in cursor.fetchall()}
        if "context_hash" not in sv_cols:
            cursor.execute(
                "ALTER TABLE swarm_verdicts ADD COLUMN context_hash VARCHAR(64) DEFAULT NULL"
            )
            logger.info("Migration: added column swarm_verdicts.context_hash")

        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Column migration failed (non-fatal).")
