"""Automatic prediction scoring via Polymarket resolved markets.

Checks PolymarketMatch records for resolved markets (yes_price >= 0.95 or <= 0.05)
and updates the corresponding DecisionNode and Consequence statuses. Calculates
overall prediction accuracy for the scoreboard.
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import or_

from .db import (
    get_session,
    DecisionNode,
    Consequence,
    PolymarketMatch,
    EngineSettings,
)

logger = logging.getLogger(__name__)


def _save_setting(session, key: str, value: str) -> None:
    """Upsert an EngineSettings value."""
    setting = session.query(EngineSettings).get(key)
    if setting:
        setting.value = value
    else:
        setting = EngineSettings(key=key, value=value)
        session.add(setting)


def _get_setting(session, key: str, default: str = "") -> str:
    """Read an EngineSettings value."""
    setting = session.query(EngineSettings).get(key)
    return setting.value if setting else default


def score_predictions() -> int:
    """Auto-resolve predictions using Polymarket resolved markets.

    A market is considered resolved when its yes price is >= 0.95 (yes won)
    or <= 0.05 (no won). When a matched DecisionNode is resolved:
      - The node status changes to "confirmed_yes" or "confirmed_no"
      - Consequences on the winning branch become "tracking"
      - Consequences on the losing branch become "denied"

    Also computes overall accuracy metrics and stores them in EngineSettings.

    Returns
    -------
    int
        Number of nodes resolved this cycle.
    """
    session = get_session()
    resolved_count = 0

    try:
        # --- 1. Find resolved Polymarket matches ---
        resolved_matches = (
            session.query(PolymarketMatch)
            .filter(
                or_(
                    PolymarketMatch.outcome_yes_price >= 0.95,
                    PolymarketMatch.outcome_yes_price <= 0.05,
                )
            )
            .all()
        )

        for pm in resolved_matches:
            # Get linked decision node
            if not pm.decision_node_id:
                continue

            node = session.query(DecisionNode).get(pm.decision_node_id)
            if not node or node.status != "open":
                continue

            resolved_yes = pm.outcome_yes_price >= 0.95

            # Resolve the node
            node.status = "confirmed_yes" if resolved_yes else "confirmed_no"
            node.confirmed_at = datetime.utcnow()
            node.evidence = (
                f"Polymarket resolved: {pm.polymarket_question} "
                f"→ {'Yes' if resolved_yes else 'No'} "
                f"(price: {pm.outcome_yes_price:.2f})"
            )

            # Score consequences
            winning_branch = "yes" if resolved_yes else "no"
            for cons in node.consequences:
                if cons.status not in ("predicted", "tracking"):
                    continue
                if cons.branch == winning_branch:
                    cons.status = "tracking"
                else:
                    cons.status = "denied"
                    cons.confirmed_at = datetime.utcnow()

            resolved_count += 1
            logger.info(
                "Node %d resolved as %s (Polymarket: %s)",
                node.id,
                node.status,
                pm.polymarket_question[:60],
            )

        # --- 2. Calculate overall accuracy ---
        resolved_nodes = (
            session.query(DecisionNode)
            .filter(DecisionNode.status.in_(["confirmed_yes", "confirmed_no"]))
            .all()
        )

        total_resolved = len(resolved_nodes)
        correct = 0
        for node in resolved_nodes:
            was_yes = node.status == "confirmed_yes"
            # We predicted correctly if our yes_probability > 0.5 and outcome was yes,
            # or yes_probability <= 0.5 and outcome was no
            # Skip neutral predictions (no meaningful signal)
            if abs((node.yes_probability or 0.5) - 0.5) < 0.01:
                total_resolved -= 1  # Don't count neutral predictions
                continue
            if (node.yes_probability > 0.5 and was_yes) or (
                node.yes_probability < 0.5 and not was_yes
            ):
                correct += 1

        accuracy = correct / total_resolved if total_resolved > 0 else 0.0

        # Store metrics in engine_settings
        _save_setting(session, "prediction_accuracy", str(round(accuracy, 4)))
        _save_setting(session, "predictions_total", str(total_resolved))
        _save_setting(session, "predictions_correct", str(correct))

        session.commit()
        logger.info(
            "Prediction scoring: %d newly resolved, %d total (%d correct, %.0f%% accuracy).",
            resolved_count,
            total_resolved,
            correct,
            accuracy * 100,
        )

    except Exception:
        logger.exception("Prediction scoring failed.")
        session.rollback()
    finally:
        session.close()

    return resolved_count
