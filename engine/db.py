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
    Column,
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


class ArticleBrief(Base):
    """NLP-enriched summary of an article."""

    __tablename__ = "article_briefs"

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
    )

    def __repr__(self) -> str:
        return (
            f"<NarrativeTimeline narrative={self.narrative_name!r} "
            f"date={self.date} articles={self.article_count}>"
        )


class RunUp(Base):
    """A detected escalation / run-up in a narrative."""

    __tablename__ = "run_ups"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    narrative_name: str = Column(String(256), nullable=False)
    detected_at: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    start_date: date = Column(Date, nullable=False)
    current_score: float = Column(Float, nullable=False, default=0.0)
    acceleration_rate: float = Column(Float, nullable=False, default=0.0)
    article_count_total: int = Column(Integer, nullable=False, default=0)
    status: str = Column(String(32), nullable=False, default="active")

    # Relationships
    decision_nodes = relationship("DecisionNode", back_populates="run_up")
    predictions = relationship("Prediction", back_populates="run_up")

    def __repr__(self) -> str:
        return (
            f"<RunUp id={self.id} narrative={self.narrative_name!r} "
            f"score={self.current_score:.1f} status={self.status!r}>"
        )


class DecisionNode(Base):
    """A node in a decision tree attached to a RunUp."""

    __tablename__ = "decision_nodes"

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

    # Relationship
    decision_node = relationship("DecisionNode", back_populates="consequences")
    predictions = relationship("Prediction", back_populates="consequence")

    @property
    def keywords(self) -> Any:
        return _json_loads(self.keywords_json)

    @keywords.setter
    def keywords(self, value: Any) -> None:
        self.keywords_json = _json_dumps(value)

    def __repr__(self) -> str:
        return (
            f"<Consequence id={self.id} node={self.decision_node_id} "
            f"branch={self.branch!r} p={self.probability:.2f}>"
        )


class ProbabilityUpdate(Base):
    """Audit log for every Bayesian probability update."""

    __tablename__ = "probability_updates"

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
    """Create all tables if they do not exist."""
    engine = _get_engine()
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created / verified.")
