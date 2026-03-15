"""Runtime ML inference for the confidence scorer integration.

Loads the trained XGBoost model and produces predictions for active run-ups.
Falls back to 0.5 (neutral) on any error, so the ML component never crashes
the confidence scoring pipeline.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional

import numpy as np

logger = logging.getLogger(__name__)

_ML_DIR = Path(__file__).parent
_MODEL_PATH = _ML_DIR / "models" / "signal_predictor.json"
_METADATA_PATH = _ML_DIR / "models" / "metadata.json"

# In-memory cache
_cached_model = None
_cached_metadata = None
_model_mtime = 0.0


def _load_model():
    """Load or reload model from disk (with file mtime caching)."""
    global _cached_model, _cached_metadata, _model_mtime

    if not _MODEL_PATH.exists():
        return None, None

    current_mtime = _MODEL_PATH.stat().st_mtime
    if _cached_model is not None and current_mtime == _model_mtime:
        return _cached_model, _cached_metadata

    try:
        import xgboost as xgb
        model = xgb.XGBClassifier()
        model.load_model(str(_MODEL_PATH))
        _cached_model = model
        _model_mtime = current_mtime

        if _METADATA_PATH.exists():
            with open(_METADATA_PATH) as f:
                _cached_metadata = json.load(f)
        else:
            _cached_metadata = {}

        logger.info("ML model loaded: %s (trained %s)",
                     _MODEL_PATH.name,
                     _cached_metadata.get("trained_at", "unknown"))
        return _cached_model, _cached_metadata

    except Exception:
        logger.exception("Failed to load ML model")
        return None, None


def _extract_features_for_runup(run_up_id: int) -> Optional[np.ndarray]:
    """Extract features for a single run-up at inference time.

    Uses the same feature extraction as prepare.py but for a single sample.
    """
    try:
        from ..ml.prepare import _connect
    except ImportError:
        from .prepare import _connect

    conn = _connect()
    try:
        import pandas as pd

        # Load metadata to know which features the model expects
        _, metadata = _load_model()
        if not metadata or "feature_columns" not in metadata:
            return None

        expected_cols = metadata["feature_columns"]

        # Extract features using a focused query for this single run-up
        query = """
        SELECT
            r.id AS run_up_id,
            r.current_score AS runup_score,
            r.acceleration_rate AS runup_acceleration,
            r.article_count_total,
            -- Node aggregates
            (SELECT COUNT(*) FROM decision_nodes dn WHERE dn.run_up_id = r.id) AS node_count,
            (SELECT COUNT(*) FROM decision_nodes dn WHERE dn.run_up_id = r.id AND dn.status = 'open') AS open_node_count,
            (SELECT AVG(dn.yes_probability) FROM decision_nodes dn WHERE dn.run_up_id = r.id) AS avg_yes_prob,
            -- Consequence aggregates
            (SELECT COUNT(*) FROM consequences c
             JOIN decision_nodes dn ON c.decision_node_id = dn.id
             WHERE dn.run_up_id = r.id) AS consequence_count,
            (SELECT AVG(c.probability) FROM consequences c
             JOIN decision_nodes dn ON c.decision_node_id = dn.id
             WHERE dn.run_up_id = r.id) AS avg_consequence_prob,
            -- Stock impact aggregates
            (SELECT COUNT(*) FROM stock_impacts si
             JOIN consequences c ON si.consequence_id = c.id
             JOIN decision_nodes dn ON c.decision_node_id = dn.id
             WHERE dn.run_up_id = r.id) AS impact_count,
            (SELECT CAST(SUM(CASE WHEN si.direction = 'bullish' THEN 1 ELSE 0 END) AS FLOAT)
                   / MAX(COUNT(*), 1)
             FROM stock_impacts si
             JOIN consequences c ON si.consequence_id = c.id
             JOIN decision_nodes dn ON c.decision_node_id = dn.id
             WHERE dn.run_up_id = r.id) AS bullish_ratio,
            (SELECT COUNT(DISTINCT si.ticker) FROM stock_impacts si
             JOIN consequences c ON si.consequence_id = c.id
             JOIN decision_nodes dn ON c.decision_node_id = dn.id
             WHERE dn.run_up_id = r.id) AS unique_tickers,
            -- Article features (7d window)
            (SELECT COUNT(*) FROM articles a
             WHERE a.pub_date >= datetime('now', '-1 day')) AS article_count_24h,
            (SELECT COUNT(*) FROM articles a
             WHERE a.pub_date >= datetime('now', '-7 days')) AS article_count_7d,
            (SELECT COUNT(DISTINCT a.source) FROM articles a
             WHERE a.pub_date >= datetime('now', '-7 days')) AS source_diversity,
            -- Narrative features
            (SELECT nt.intensity_score FROM narrative_timeline nt
             WHERE nt.narrative_name = r.narrative_name
             ORDER BY nt.date DESC LIMIT 1) AS latest_intensity,
            (SELECT AVG(nt.intensity_score) FROM narrative_timeline nt
             WHERE nt.narrative_name = r.narrative_name
             AND nt.date >= date('now', '-7 days')) AS intensity_7d_avg,
            (SELECT COUNT(*) FROM narrative_timeline nt
             WHERE nt.narrative_name = r.narrative_name) AS timeline_entries,
            -- Swarm features
            (SELECT sv.confidence FROM swarm_verdicts sv
             WHERE sv.run_up_id = r.id AND sv.superseded_at IS NULL
             ORDER BY sv.created_at DESC LIMIT 1) AS swarm_confidence,
            (SELECT sv.consensus_strength FROM swarm_verdicts sv
             WHERE sv.run_up_id = r.id AND sv.superseded_at IS NULL
             ORDER BY sv.created_at DESC LIMIT 1) AS swarm_consensus,
            -- Signal history
            (SELECT COUNT(*) FROM trading_signals ts
             WHERE ts.run_up_id = r.id AND ts.superseded_by_id IS NOT NULL) AS prev_signal_count,
            (SELECT MAX(ts.confidence) FROM trading_signals ts
             WHERE ts.run_up_id = r.id) AS max_prev_confidence,
            -- Narrative age
            CAST((julianday('now') - julianday(r.detected_at)) AS FLOAT) AS narrative_age_days
        FROM run_ups r
        WHERE r.id = ?
        """

        df = pd.read_sql_query(query, conn, params=[run_up_id])
        if df.empty:
            return None

        # Build feature vector matching expected columns
        feature_dict = df.iloc[0].to_dict()
        feature_vector = []
        for col in expected_cols:
            val = feature_dict.get(col, 0.0)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                val = 0.0
            feature_vector.append(float(val))

        return np.array([feature_vector])

    except Exception:
        logger.exception("Feature extraction failed for run_up %d", run_up_id)
        return None
    finally:
        conn.close()


def predict_signal(run_up_id: int) -> float:
    """Return ML confidence score (0-1) for a run-up.

    Returns 0.5 (neutral) if:
    - Model not trained yet
    - Not enough training data
    - Feature extraction fails
    - Any other error

    A score of 0.5 means neutral — no impact on composite confidence.
    Scores > 0.5 boost confidence, < 0.5 reduce it.
    """
    try:
        model, metadata = _load_model()
        if model is None:
            return 0.5

        features = _extract_features_for_runup(run_up_id)
        if features is None:
            return 0.5

        # Get probability of profitable trade
        prob = model.predict_proba(features)[0, 1]

        # Clamp to reasonable range
        return float(max(0.05, min(0.95, prob)))

    except Exception:
        logger.exception("ML prediction failed for run_up %d", run_up_id)
        return 0.5


def get_model_status() -> Dict[str, Any]:
    """Return current model status for API endpoint."""
    model, metadata = _load_model()

    if model is None:
        return {
            "status": "not_trained",
            "model_path": str(_MODEL_PATH),
            "message": "No trained model found. Run 'python -m engine.ml.train' first.",
        }

    return {
        "status": "active",
        "model_path": str(_MODEL_PATH),
        "trained_at": metadata.get("trained_at"),
        "n_samples": metadata.get("n_samples"),
        "n_features": metadata.get("n_features"),
        "target": metadata.get("target"),
        "metrics": metadata.get("metrics", {}),
        "top_features": metadata.get("top_features", {}),
        "training_seconds": metadata.get("training_seconds"),
    }
