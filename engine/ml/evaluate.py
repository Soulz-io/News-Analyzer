"""Backtesting and evaluation framework for ML models.

THIS FILE IS FIXED — not modified by the autoresearch loop.
Provides standardized metrics that the autoresearch loop uses
to evaluate model improvements.

Metrics:
  - sharpe_ratio: (mean return / std return) * sqrt(252)
  - hit_rate: fraction of trades with positive adjusted return
  - brier_score: calibration of probability estimates
  - profit_factor: total gains / total losses
  - max_drawdown: worst peak-to-trough decline
"""

import numpy as np
from typing import Dict, Any


def calculate_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_prob: np.ndarray | None = None,
    returns: np.ndarray | None = None,
) -> Dict[str, float]:
    """Calculate all evaluation metrics.

    Args:
        y_true: Binary labels (1 = profitable, 0 = not)
        y_pred: Binary predictions (1 = trade, 0 = skip)
        y_prob: Predicted probabilities (for Brier score). Optional.
        returns: Actual percentage returns per sample. Optional.

    Returns:
        Dict with sharpe_ratio, hit_rate, brier_score, profit_factor,
        max_drawdown, precision, recall, f1, n_samples, n_trades.
    """
    n = len(y_true)
    if n == 0:
        return _empty_metrics()

    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    # Basic classification metrics
    tp = np.sum((y_pred == 1) & (y_true == 1))
    fp = np.sum((y_pred == 1) & (y_true == 0))
    fn = np.sum((y_pred == 0) & (y_true == 1))
    n_trades = int(np.sum(y_pred == 1))

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    # Hit rate: among predictions where we trade, what fraction is profitable?
    if n_trades > 0:
        trade_mask = y_pred == 1
        hit_rate = float(np.mean(y_true[trade_mask]))
    else:
        hit_rate = 0.0

    # Brier score (lower is better, 0 is perfect)
    if y_prob is not None:
        y_prob = np.asarray(y_prob, dtype=float)
        brier = float(np.mean((y_prob - y_true) ** 2))
    else:
        brier = float(np.mean((y_pred - y_true) ** 2))

    # Sharpe ratio on returns
    sharpe = 0.0
    profit_factor = 0.0
    max_dd = 0.0

    if returns is not None:
        returns = np.asarray(returns, dtype=float)
        # Only count returns where we predicted "trade"
        if n_trades > 0:
            trade_returns = returns[y_pred == 1]
            sharpe = _sharpe_ratio(trade_returns)
            profit_factor = _profit_factor(trade_returns)
            max_dd = _max_drawdown(trade_returns)
        else:
            sharpe = 0.0
    elif n_trades > 0:
        # Estimate returns from binary outcomes
        # Assume +2% for profitable trades, -1% for unprofitable (asymmetric payoff)
        trade_mask = y_pred == 1
        est_returns = np.where(y_true[trade_mask] == 1, 2.0, -1.0)
        sharpe = _sharpe_ratio(est_returns)
        profit_factor = _profit_factor(est_returns)
        max_dd = _max_drawdown(est_returns)

    return {
        "sharpe_ratio": round(sharpe, 6),
        "hit_rate": round(hit_rate, 4),
        "brier_score": round(brier, 6),
        "profit_factor": round(profit_factor, 4),
        "max_drawdown": round(max_dd, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "n_samples": n,
        "n_trades": n_trades,
    }


def _sharpe_ratio(returns: np.ndarray, annualize: bool = True) -> float:
    """Calculate Sharpe ratio from a series of returns."""
    if len(returns) < 2:
        return 0.0
    mean_r = np.mean(returns)
    std_r = np.std(returns, ddof=1)
    if std_r < 1e-8:
        return 0.0 if mean_r <= 0 else 10.0  # Cap at 10 for zero-vol
    ratio = mean_r / std_r
    if annualize:
        ratio *= np.sqrt(252)  # Annualize (trading days)
    return float(ratio)


def _profit_factor(returns: np.ndarray) -> float:
    """Calculate profit factor: sum of gains / abs(sum of losses)."""
    gains = np.sum(returns[returns > 0])
    losses = abs(np.sum(returns[returns < 0]))
    if losses < 1e-8:
        return 10.0 if gains > 0 else 0.0
    return float(gains / losses)


def _max_drawdown(returns: np.ndarray) -> float:
    """Calculate maximum drawdown from a return series."""
    if len(returns) == 0:
        return 0.0
    cumulative = np.cumsum(returns)
    peak = np.maximum.accumulate(cumulative)
    drawdown = peak - cumulative
    return float(np.max(drawdown)) if len(drawdown) > 0 else 0.0


def _empty_metrics() -> Dict[str, float]:
    """Return zero-valued metrics dict."""
    return {
        "sharpe_ratio": 0.0,
        "hit_rate": 0.0,
        "brier_score": 1.0,
        "profit_factor": 0.0,
        "max_drawdown": 0.0,
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
        "n_samples": 0,
        "n_trades": 0,
    }


def format_metrics(metrics: Dict[str, Any]) -> str:
    """Format metrics as human-readable string (for autoresearch loop parsing)."""
    return (
        f"sharpe_ratio: {metrics['sharpe_ratio']:.6f}\n"
        f"hit_rate: {metrics['hit_rate']:.4f}\n"
        f"brier_score: {metrics['brier_score']:.6f}\n"
        f"profit_factor: {metrics['profit_factor']:.4f}\n"
        f"max_drawdown: {metrics['max_drawdown']:.4f}\n"
        f"precision: {metrics['precision']:.4f}\n"
        f"recall: {metrics['recall']:.4f}\n"
        f"f1: {metrics['f1']:.4f}\n"
        f"n_samples: {metrics['n_samples']}\n"
        f"n_trades: {metrics['n_trades']}"
    )
