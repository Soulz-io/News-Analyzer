"""Feature extraction from SQLite for ML training.

THIS FILE IS FIXED — not modified by the autoresearch loop.
It extracts raw features from the OpenClaw database and produces
a feature matrix + labels for model training.

Usage:
    python -m engine.ml.prepare          # Extract features → parquet
    python -m engine.ml.prepare --stats  # Print data statistics
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Paths
_ML_DIR = Path(__file__).parent
_DATA_DIR = _ML_DIR / "data"
_DB_PATH = _ML_DIR.parent / "data" / "news_analyzer.db"

# Feature output files
FEATURES_PATH = _DATA_DIR / "features.parquet"
LABELS_PATH = _DATA_DIR / "labels.parquet"


def _get_db_path() -> str:
    """Resolve database path, respecting DATABASE_URI env var."""
    env_uri = os.environ.get("DATABASE_URI", "")
    if env_uri.startswith("sqlite:///"):
        return env_uri.replace("sqlite:///", "")
    return str(_DB_PATH)


def _connect():
    """Open a read-only SQLite connection."""
    import sqlite3
    db_path = _get_db_path()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ──────────────────────────────────────────────────────────────────────
# Feature extractors
# ──────────────────────────────────────────────────────────────────────

def _extract_runup_features(conn) -> pd.DataFrame:
    """Extract features from run_ups + decision_nodes + consequences + stock_impacts."""
    query = """
    SELECT
        r.id AS run_up_id,
        r.narrative_name,
        r.current_score AS runup_score,
        r.acceleration_rate AS runup_acceleration,
        r.article_count_total,
        r.status AS runup_status,
        r.detected_at,
        r.start_date,
        -- Decision node aggregates
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
         WHERE dn.run_up_id = r.id) AS unique_tickers
    FROM run_ups r
    WHERE r.merged_into_id IS NULL
    ORDER BY r.detected_at
    """
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df["detected_at"] = pd.to_datetime(df["detected_at"], utc=True)
        df["start_date"] = pd.to_datetime(df["start_date"], utc=True)
        now = pd.Timestamp.now(tz="UTC")
        df["narrative_age_days"] = (
            now - df["detected_at"]
        ).dt.total_seconds() / 86400
    return df


def _extract_article_features(conn) -> pd.DataFrame:
    """Extract article-level aggregates per run-up narrative."""
    query = """
    SELECT
        r.id AS run_up_id,
        r.narrative_name,
        -- 24h article count
        (SELECT COUNT(*) FROM articles a
         JOIN article_briefs ab ON ab.article_id = a.id
         WHERE a.pub_date >= datetime('now', '-1 day')
         AND (a.title LIKE '%' || REPLACE(r.narrative_name, '-', '%') || '%'
              OR ab.keywords_json LIKE '%' || r.narrative_name || '%')
        ) AS article_count_24h,
        -- 7d article count
        (SELECT COUNT(*) FROM articles a
         JOIN article_briefs ab ON ab.article_id = a.id
         WHERE a.pub_date >= datetime('now', '-7 days')
         AND (a.title LIKE '%' || REPLACE(r.narrative_name, '-', '%') || '%'
              OR ab.keywords_json LIKE '%' || r.narrative_name || '%')
        ) AS article_count_7d,
        -- Sentiment stats from matching briefs (last 7 days)
        (SELECT AVG(ab.sentiment) FROM article_briefs ab
         JOIN articles a ON ab.article_id = a.id
         WHERE a.pub_date >= datetime('now', '-7 days')
         AND ab.keywords_json LIKE '%' || r.narrative_name || '%'
        ) AS sentiment_mean,
        -- Source diversity (unique sources, last 7 days)
        (SELECT COUNT(DISTINCT a.source) FROM articles a
         JOIN article_briefs ab ON ab.article_id = a.id
         WHERE a.pub_date >= datetime('now', '-7 days')
         AND (a.title LIKE '%' || REPLACE(r.narrative_name, '-', '%') || '%'
              OR ab.keywords_json LIKE '%' || r.narrative_name || '%')
        ) AS source_diversity,
        -- Avg credibility
        (SELECT AVG(ab.source_credibility) FROM article_briefs ab
         JOIN articles a ON ab.article_id = a.id
         WHERE a.pub_date >= datetime('now', '-7 days')
         AND ab.keywords_json LIKE '%' || r.narrative_name || '%'
        ) AS source_credibility_avg,
        -- Entity count (approx: avg entities per brief)
        (SELECT AVG(LENGTH(ab.entities_json) - LENGTH(REPLACE(ab.entities_json, ',', '')))
         FROM article_briefs ab
         JOIN articles a ON ab.article_id = a.id
         WHERE a.pub_date >= datetime('now', '-7 days')
         AND ab.keywords_json LIKE '%' || r.narrative_name || '%'
        ) AS avg_entity_count
    FROM run_ups r
    WHERE r.merged_into_id IS NULL
    ORDER BY r.detected_at
    """
    return pd.read_sql_query(query, conn)


def _extract_narrative_features(conn) -> pd.DataFrame:
    """Extract narrative timeline features per run-up."""
    query = """
    SELECT
        r.id AS run_up_id,
        -- Latest timeline intensity
        (SELECT nt.intensity_score FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name
         ORDER BY nt.date DESC LIMIT 1) AS latest_intensity,
        -- Trend
        (SELECT nt.trend FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name
         ORDER BY nt.date DESC LIMIT 1) AS latest_trend,
        -- 7-day avg intensity
        (SELECT AVG(nt.intensity_score) FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name
         AND nt.date >= date('now', '-7 days')) AS intensity_7d_avg,
        -- Intensity acceleration (last 3d avg - prev 3d avg)
        (SELECT AVG(nt.intensity_score) FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name
         AND nt.date >= date('now', '-3 days')
        ) - (SELECT AVG(nt.intensity_score) FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name
         AND nt.date BETWEEN date('now', '-6 days') AND date('now', '-3 days')
        ) AS intensity_acceleration,
        -- Timeline entries count
        (SELECT COUNT(*) FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name) AS timeline_entries,
        -- Region diversity
        (SELECT MAX(nt.unique_regions) FROM narrative_timeline nt
         WHERE nt.narrative_name = r.narrative_name
         AND nt.date >= date('now', '-7 days')) AS max_unique_regions
    FROM run_ups r
    WHERE r.merged_into_id IS NULL
    ORDER BY r.detected_at
    """
    return pd.read_sql_query(query, conn)


def _extract_swarm_features(conn) -> pd.DataFrame:
    """Extract latest swarm verdict features per run-up."""
    query = """
    SELECT
        sv.run_up_id,
        sv.confidence AS swarm_confidence,
        sv.consensus_strength AS swarm_consensus,
        sv.yes_probability AS swarm_yes_prob,
        sv.verdict AS swarm_verdict,
        sv.ticker_direction AS swarm_direction
    FROM swarm_verdicts sv
    INNER JOIN (
        SELECT run_up_id, MAX(created_at) AS max_created
        FROM swarm_verdicts
        WHERE superseded_at IS NULL
        GROUP BY run_up_id
    ) latest ON sv.run_up_id = latest.run_up_id
                AND sv.created_at = latest.max_created
    """
    df = pd.read_sql_query(query, conn)
    # Encode verdict as numeric
    verdict_map = {"strong_yes": 2, "yes": 1, "lean_yes": 0.5,
                   "neutral": 0, "lean_no": -0.5, "no": -1, "strong_no": -2}
    if not df.empty:
        df["swarm_verdict_encoded"] = df["swarm_verdict"].map(verdict_map).fillna(0)
        df["swarm_direction_encoded"] = df["swarm_direction"].map(
            {"bullish": 1, "bearish": -1}).fillna(0)
    return df


def _extract_polymarket_features(conn) -> pd.DataFrame:
    """Extract Polymarket features per run-up."""
    query = """
    SELECT
        pm.run_up_id,
        pm.outcome_yes_price AS poly_yes_price,
        pm.volume AS poly_volume,
        pm.liquidity AS poly_liquidity,
        pm.match_score AS poly_match_score,
        -- 24h drift from price history
        (SELECT ph1.yes_price - ph2.yes_price
         FROM polymarket_price_history ph1, polymarket_price_history ph2
         WHERE ph1.polymarket_id = pm.polymarket_id
         AND ph2.polymarket_id = pm.polymarket_id
         AND ph1.recorded_at = (SELECT MAX(recorded_at) FROM polymarket_price_history WHERE polymarket_id = pm.polymarket_id)
         AND ph2.recorded_at = (SELECT MAX(recorded_at) FROM polymarket_price_history WHERE polymarket_id = pm.polymarket_id AND recorded_at <= datetime('now', '-1 day'))
        ) AS poly_drift_24h
    FROM polymarket_matches pm
    INNER JOIN (
        SELECT run_up_id, MAX(updated_at) AS max_updated
        FROM polymarket_matches
        GROUP BY run_up_id
    ) latest ON pm.run_up_id = latest.run_up_id
                AND pm.updated_at = latest.max_updated
    """
    return pd.read_sql_query(query, conn)


def _extract_signal_history(conn) -> pd.DataFrame:
    """Extract previous signal history per run-up (for trend features)."""
    query = """
    SELECT
        ts.run_up_id,
        COUNT(*) AS prev_signal_count,
        MAX(ts.confidence) AS max_prev_confidence,
        AVG(ts.confidence) AS avg_prev_confidence,
        -- How many times did signal level upgrade?
        SUM(CASE WHEN ts.signal_level IN ('BUY', 'STRONG_BUY') THEN 1 ELSE 0 END) AS buy_signal_count
    FROM trading_signals ts
    WHERE ts.superseded_by_id IS NOT NULL
    GROUP BY ts.run_up_id
    """
    return pd.read_sql_query(query, conn)


# ──────────────────────────────────────────────────────────────────────
# Labels (targets for supervised learning)
# ──────────────────────────────────────────────────────────────────────

def _extract_labels(conn) -> pd.DataFrame:
    """Extract outcome labels for each run-up.

    Strategy:
    1. Try price_snapshots for actual T+1d/3d/7d price changes (best)
    2. Fallback: use swarm verdict + stock impact consensus as proxy label
       (useful during cold-start when price data is sparse)
    """
    query = """
    SELECT
        r.id AS run_up_id,
        -- Find the most impactful ticker
        (SELECT si.ticker FROM stock_impacts si
         JOIN consequences c ON si.consequence_id = c.id
         JOIN decision_nodes dn ON c.decision_node_id = dn.id
         WHERE dn.run_up_id = r.id
         GROUP BY si.ticker
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS primary_ticker,
        -- Primary direction
        (SELECT si.direction FROM stock_impacts si
         JOIN consequences c ON si.consequence_id = c.id
         JOIN decision_nodes dn ON c.decision_node_id = dn.id
         WHERE dn.run_up_id = r.id
         GROUP BY si.direction
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS primary_direction,
        -- Latest signal creation time
        (SELECT MAX(ts.created_at) FROM trading_signals ts
         WHERE ts.run_up_id = r.id) AS signal_created_at,
        -- Signal confidence at that time
        (SELECT ts.confidence FROM trading_signals ts
         WHERE ts.run_up_id = r.id
         ORDER BY ts.created_at DESC LIMIT 1) AS signal_confidence,
        -- Swarm verdict (proxy label)
        (SELECT sv.confidence FROM swarm_verdicts sv
         WHERE sv.run_up_id = r.id AND sv.superseded_at IS NULL
         ORDER BY sv.created_at DESC LIMIT 1) AS swarm_confidence,
        (SELECT sv.verdict FROM swarm_verdicts sv
         WHERE sv.run_up_id = r.id AND sv.superseded_at IS NULL
         ORDER BY sv.created_at DESC LIMIT 1) AS swarm_verdict,
        -- Bullish ratio from stock impacts (proxy for signal quality)
        (SELECT CAST(SUM(CASE WHEN si.direction = 'bullish' THEN 1 ELSE 0 END) AS FLOAT)
               / MAX(COUNT(*), 1)
         FROM stock_impacts si
         JOIN consequences c ON si.consequence_id = c.id
         JOIN decision_nodes dn ON c.decision_node_id = dn.id
         WHERE dn.run_up_id = r.id) AS bullish_ratio,
        -- Run-up score (higher = more significant)
        r.current_score AS runup_score
    FROM run_ups r
    WHERE r.merged_into_id IS NULL
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df

    rows = []
    for _, row in df.iterrows():
        row = row.copy()
        ticker = row["primary_ticker"]
        signal_time = row["signal_created_at"]
        has_price_labels = False

        # Try price-based labels first
        if ticker and signal_time:
            signal_dt = pd.to_datetime(signal_time)
            price_query = """
            SELECT price, recorded_at FROM price_snapshots
            WHERE ticker = ? AND recorded_at >= ? AND recorded_at <= ?
            ORDER BY recorded_at
            """
            for horizon_name, horizon_days in [("1d", 1), ("3d", 3), ("7d", 7)]:
                start_str = signal_dt.strftime("%Y-%m-%d %H:%M:%S")
                end_str = (signal_dt + timedelta(days=horizon_days + 1)).strftime("%Y-%m-%d %H:%M:%S")
                prices = pd.read_sql_query(price_query, conn, params=[ticker, start_str, end_str])
                if len(prices) >= 2:
                    p0 = prices.iloc[0]["price"]
                    p_end = prices.iloc[-1]["price"]
                    if p0 > 0:
                        pct_change = (p_end - p0) / p0 * 100
                        direction_mult = -1 if row["primary_direction"] == "bearish" else 1
                        adjusted_return = pct_change * direction_mult
                        row[f"price_change_{horizon_name}"] = pct_change
                        row[f"adjusted_return_{horizon_name}"] = adjusted_return
                        row[f"profitable_{horizon_name}"] = 1 if adjusted_return > 0.5 else 0
                        has_price_labels = True

        # Fallback: proxy labels from signal confidence + run-up score
        if not has_price_labels:
            # Composite proxy: high confidence signals on strong run-ups
            # are considered "profitable" as a training bootstrap
            conf = row.get("signal_confidence", 0) or 0
            score = row.get("runup_score", 0) or 0
            bullish = row.get("bullish_ratio", 0.5) or 0.5

            # Proxy: score > 50 AND confidence > 0.5 AND bullish_ratio > 0.5
            proxy_signal = 1 if (score > 50 and conf > 0.5 and bullish > 0.5) else 0

            for h in ["1d", "3d", "7d"]:
                row[f"profitable_{h}"] = proxy_signal
                row[f"price_change_{h}"] = 0.0
                row[f"adjusted_return_{h}"] = 0.0

        rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    # Ensure label columns exist
    for h in ["1d", "3d", "7d"]:
        for col in [f"profitable_{h}", f"price_change_{h}", f"adjusted_return_{h}"]:
            if col not in result.columns:
                result[col] = 0.0

    return result


# ──────────────────────────────────────────────────────────────────────
# Main: build combined feature matrix
# ──────────────────────────────────────────────────────────────────────

def load_features() -> pd.DataFrame:
    """Load features from parquet cache, or extract fresh if missing."""
    if FEATURES_PATH.exists():
        return pd.read_parquet(FEATURES_PATH)
    return extract_features()


def load_labels() -> pd.DataFrame:
    """Load labels from parquet cache, or extract fresh if missing."""
    if LABELS_PATH.exists():
        return pd.read_parquet(LABELS_PATH)
    return extract_labels()


def extract_features() -> pd.DataFrame:
    """Extract all features and save to parquet."""
    conn = _connect()
    try:
        # Extract all feature groups
        runup_df = _extract_runup_features(conn)
        if runup_df.empty:
            logger.warning("No run-ups found — cannot extract features")
            return pd.DataFrame()

        article_df = _extract_article_features(conn)
        narrative_df = _extract_narrative_features(conn)
        swarm_df = _extract_swarm_features(conn)
        poly_df = _extract_polymarket_features(conn)
        signal_df = _extract_signal_history(conn)

        # Merge all on run_up_id
        features = runup_df.copy()
        for df in [article_df, narrative_df, swarm_df, poly_df, signal_df]:
            if not df.empty and "run_up_id" in df.columns:
                # Drop duplicate columns before merge
                overlap = set(features.columns) & set(df.columns) - {"run_up_id"}
                df = df.drop(columns=list(overlap), errors="ignore")
                features = features.merge(df, on="run_up_id", how="left")

        # Encode categorical features
        trend_map = {"rising": 2, "accelerating": 3, "stable": 0,
                     "declining": -1, "fading": -2}
        if "latest_trend" in features.columns:
            features["trend_encoded"] = features["latest_trend"].map(trend_map).fillna(0)

        # Fill NaN with 0 for numeric columns
        numeric_cols = features.select_dtypes(include=[np.number]).columns
        features[numeric_cols] = features[numeric_cols].fillna(0)

        # Drop non-feature columns
        drop_cols = ["narrative_name", "runup_status", "detected_at", "start_date",
                     "latest_trend", "swarm_verdict", "swarm_direction"]
        features = features.drop(columns=[c for c in drop_cols if c in features.columns])

        # Save
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        features.to_parquet(FEATURES_PATH, index=False)
        logger.info("Features extracted: %d samples, %d features → %s",
                     len(features), len(features.columns), FEATURES_PATH)
        return features

    finally:
        conn.close()


def extract_labels() -> pd.DataFrame:
    """Extract outcome labels and save to parquet."""
    conn = _connect()
    try:
        labels = _extract_labels(conn)
        if labels.empty:
            logger.warning("No labeled data found")
            return pd.DataFrame()

        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        labels.to_parquet(LABELS_PATH, index=False)
        logger.info("Labels extracted: %d samples → %s", len(labels), LABELS_PATH)
        return labels
    finally:
        conn.close()


def extract_all() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract features and labels together."""
    features = extract_features()
    labels = extract_labels()
    return features, labels


def print_stats():
    """Print dataset statistics."""
    conn = _connect()
    try:
        import sqlite3
        c = conn.cursor()
        tables = {
            "articles": "SELECT COUNT(*) FROM articles",
            "article_briefs": "SELECT COUNT(*) FROM article_briefs",
            "run_ups": "SELECT COUNT(*) FROM run_ups WHERE merged_into_id IS NULL",
            "decision_nodes": "SELECT COUNT(*) FROM decision_nodes",
            "consequences": "SELECT COUNT(*) FROM consequences",
            "stock_impacts": "SELECT COUNT(*) FROM stock_impacts",
            "swarm_verdicts": "SELECT COUNT(*) FROM swarm_verdicts",
            "trading_signals": "SELECT COUNT(*) FROM trading_signals",
            "price_snapshots": "SELECT COUNT(*) FROM price_snapshots",
            "narrative_timeline": "SELECT COUNT(*) FROM narrative_timeline",
        }
        print("=" * 50)
        print("OpenClaw ML Data Statistics")
        print("=" * 50)
        for name, query in tables.items():
            c.execute(query)
            count = c.fetchone()[0]
            print(f"  {name:25s}: {count:>6d}")

        # Labeled data estimate
        c.execute("""
            SELECT COUNT(DISTINCT r.id) FROM run_ups r
            JOIN trading_signals ts ON ts.run_up_id = r.id
            JOIN stock_impacts si ON si.consequence_id IN (
                SELECT c.id FROM consequences c
                JOIN decision_nodes dn ON c.decision_node_id = dn.id
                WHERE dn.run_up_id = r.id
            )
            WHERE r.merged_into_id IS NULL
        """)
        labeled = c.fetchone()[0]
        print(f"\n  Labeled run-ups (signal + ticker): {labeled}")
        print(f"  Min for ML activation: 30")
        print(f"  Status: {'READY' if labeled >= 30 else f'Need {30 - labeled} more'}")
        print("=" * 50)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if "--stats" in sys.argv:
        print_stats()
    else:
        features, labels = extract_all()
        print(f"Features: {features.shape}")
        print(f"Labels: {labels.shape}")
        if not features.empty:
            print(f"\nFeature columns: {list(features.columns)}")
        print_stats()
