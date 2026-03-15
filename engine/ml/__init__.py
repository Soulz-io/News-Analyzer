"""OpenClaw ML module — autoresearch-inspired signal prediction.

Applies Karpathy's autoresearch methodology (autonomous AI experiment loop)
to iteratively improve trading signal predictions using XGBoost on OpenClaw's
own historical data.

Components:
  prepare.py   — Feature extraction from SQLite (FIXED — not AI-modified)
  train.py     — Model training (AI-MODIFIED via autoresearch loop)
  evaluate.py  — Backtesting framework (FIXED)
  inference.py — Runtime predictions for confidence_scorer integration
  program.md   — Autoresearch instructions for Claude Code
"""

ML_VERSION = "2.0.0"
MIN_TRAINING_SAMPLES = 30  # Minimum labeled samples before ML activates
