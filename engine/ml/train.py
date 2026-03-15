"""OpenClaw Signal Predictor — XGBoost model training.

THIS FILE IS MODIFIED by the autoresearch loop.
Claude Code iteratively improves features, hyperparameters, preprocessing,
and ensemble methods to maximize the primary metric (sharpe_ratio).

Usage:
    python -m engine.ml.train              # Train + evaluate
    python -m engine.ml.train --force      # Force retrain even with few samples

Autoresearch rules:
    - Output metrics via print() for grep parsing
    - Save best model to engine/ml/models/signal_predictor.json
    - Keep training time under 30 seconds
    - ALWAYS use TimeSeriesSplit (never random split)
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

# Relative imports for both standalone and module usage
try:
    from .prepare import load_features, load_labels, extract_all
    from .evaluate import calculate_metrics, format_metrics
    from . import MIN_TRAINING_SAMPLES
except ImportError:
    from prepare import load_features, load_labels, extract_all
    from evaluate import calculate_metrics, format_metrics
    MIN_TRAINING_SAMPLES = 30

logger = logging.getLogger(__name__)

_ML_DIR = Path(__file__).parent
_MODEL_PATH = _ML_DIR / "models" / "signal_predictor.json"
_METADATA_PATH = _ML_DIR / "models" / "metadata.json"

# ══════════════════════════════════════════════════════════════════════
# EXPERIMENTABLE SECTION — modify these during autoresearch loop
# ══════════════════════════════════════════════════════════════════════

# Feature selection: which columns from prepare.py to use
# Set to None to use all available numeric features
FEATURE_COLUMNS = None

# Target variable
TARGET = "profitable_7d"

# XGBoost hyperparameters
PARAMS = {
    "max_depth": 4,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "min_child_weight": 3,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "tree_method": "hist",  # CPU-optimized
    "random_state": 42,
}

# Cross-validation splits
N_SPLITS = 3

# Prediction threshold (probability above this = "trade")
PRED_THRESHOLD = 0.55

# ══════════════════════════════════════════════════════════════════════
# END EXPERIMENTABLE SECTION
# ══════════════════════════════════════════════════════════════════════


def _select_features(features_df, labels_df):
    """Merge features with labels and prepare train/test arrays."""
    import pandas as pd

    # Merge on run_up_id
    merged = features_df.merge(
        labels_df[["run_up_id", TARGET]].dropna(subset=[TARGET]),
        on="run_up_id",
        how="inner",
    )

    if merged.empty:
        return None, None, None

    y = merged[TARGET].values.astype(float)

    # Select feature columns
    drop_cols = {"run_up_id", TARGET, "primary_ticker", "primary_direction",
                 "signal_created_at", "signal_confidence",
                 "price_change_1d", "price_change_3d", "price_change_7d",
                 "adjusted_return_1d", "adjusted_return_3d", "adjusted_return_7d",
                 "profitable_1d", "profitable_3d", "profitable_7d"}

    if FEATURE_COLUMNS is not None:
        feature_cols = [c for c in FEATURE_COLUMNS if c in merged.columns]
    else:
        feature_cols = [c for c in merged.columns
                        if c not in drop_cols
                        and merged[c].dtype in [np.float64, np.int64, float, int]]

    X = merged[feature_cols].values.astype(float)

    # Handle any remaining NaN/inf
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    return X, y, feature_cols


def train(force: bool = False) -> dict:
    """Train the signal predictor model.

    Returns:
        Dict with metrics and model info.
    """
    start_time = time.time()

    # Load data
    features_df = load_features()
    labels_df = load_labels()

    if features_df.empty or labels_df.empty:
        # Try fresh extraction
        features_df, labels_df = extract_all()

    if features_df.empty:
        print("ERROR: No features available. Run prepare.py first.")
        return {}

    if labels_df.empty:
        print("ERROR: No labeled data available. Need price outcome data.")
        return {}

    X, y, feature_cols = _select_features(features_df, labels_df)

    if X is None or len(X) == 0:
        print(f"ERROR: No valid training samples after merge.")
        return {}

    n_samples = len(X)
    n_positive = int(np.sum(y == 1))
    n_features = X.shape[1]

    print(f"data_samples: {n_samples}")
    print(f"data_positive: {n_positive}")
    print(f"data_features: {n_features}")

    if n_samples < MIN_TRAINING_SAMPLES and not force:
        print(f"WARNING: Only {n_samples} samples (need {MIN_TRAINING_SAMPLES}). "
              f"Use --force to train anyway.")
        print("sharpe_ratio: 0.000000")
        print("hit_rate: 0.0000")
        print("brier_score: 1.000000")
        return {}

    # TimeSeriesSplit cross-validation
    n_splits = min(N_SPLITS, max(2, n_samples // 10))
    tscv = TimeSeriesSplit(n_splits=n_splits)

    all_y_true = []
    all_y_pred = []
    all_y_prob = []
    fold_metrics = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        # Handle class imbalance via scale_pos_weight
        n_neg = np.sum(y_train == 0)
        n_pos = np.sum(y_train == 1)
        scale_pos = n_neg / n_pos if n_pos > 0 else 1.0

        model = xgb.XGBClassifier(
            **PARAMS,
            scale_pos_weight=scale_pos,
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= PRED_THRESHOLD).astype(int)

        all_y_true.extend(y_test)
        all_y_pred.extend(y_pred)
        all_y_prob.extend(y_prob)

        fold_m = calculate_metrics(y_test, y_pred, y_prob)
        fold_metrics.append(fold_m)

    # Aggregate metrics across folds
    all_y_true = np.array(all_y_true)
    all_y_pred = np.array(all_y_pred)
    all_y_prob = np.array(all_y_prob)

    metrics = calculate_metrics(all_y_true, all_y_pred, all_y_prob)

    # Train final model on ALL data
    n_neg = np.sum(y == 0)
    n_pos = np.sum(y == 1)
    scale_pos = n_neg / n_pos if n_pos > 0 else 1.0

    final_model = xgb.XGBClassifier(
        **PARAMS,
        scale_pos_weight=scale_pos,
    )
    final_model.fit(X, y, verbose=False)

    # Feature importance
    importance = dict(zip(feature_cols, final_model.feature_importances_))
    top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]

    # Save model
    _MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_model.save_model(str(_MODEL_PATH))

    # Save metadata
    metadata = {
        "version": "1.0",
        "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "n_samples": n_samples,
        "n_features": n_features,
        "feature_columns": feature_cols,
        "target": TARGET,
        "params": PARAMS,
        "pred_threshold": PRED_THRESHOLD,
        "metrics": metrics,
        "top_features": {k: round(float(v), 4) for k, v in top_features},
        "training_seconds": round(time.time() - start_time, 2),
    }
    with open(_METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)

    # Print metrics (for autoresearch loop grep)
    print(format_metrics(metrics))
    print(f"training_seconds: {metadata['training_seconds']}")
    print(f"\nTop features:")
    for fname, fimp in top_features:
        print(f"  {fname}: {fimp:.4f}")
    print(f"\nModel saved: {_MODEL_PATH}")

    return metadata


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    force = "--force" in sys.argv
    train(force=force)
