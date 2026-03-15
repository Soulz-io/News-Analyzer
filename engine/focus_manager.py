"""Focus Mode manager -- concentrated analysis on 1-3 narratives.

Persists focus state in the EngineSettings key-value table.
Provides a 60-second in-memory cache to avoid hitting the DB on every
scheduler tick.
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from .db import get_session, EngineSettings, RunUp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_focus_cache: Optional[Dict] = None
_cache_ts: float = 0.0
_CACHE_TTL = 60  # seconds


def _invalidate_cache() -> None:
    global _focus_cache, _cache_ts
    _focus_cache = None
    _cache_ts = 0.0


def _load_focus_state() -> Dict:
    """Load focus state from EngineSettings (with caching)."""
    global _focus_cache, _cache_ts
    now = time.time()
    if _focus_cache is not None and (now - _cache_ts) < _CACHE_TTL:
        return _focus_cache

    session = get_session()
    try:
        ids_setting = session.query(EngineSettings).get("focus_runup_ids")
        ids = json.loads(ids_setting.value) if ids_setting and ids_setting.value else []

        links_setting = session.query(EngineSettings).get("focus_polymarket_links")
        links = json.loads(links_setting.value) if links_setting and links_setting.value else {}

        _focus_cache = {"ids": ids, "polymarket_links": links}
        _cache_ts = now
        return _focus_cache
    except Exception:
        logger.exception("Failed to load focus state")
        return {"ids": [], "polymarket_links": {}}
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_focused_runup_ids() -> List[int]:
    """Return the list of focused run-up IDs."""
    return _load_focus_state()["ids"]


def is_focused(run_up_id: int) -> bool:
    """Check if a run-up is in focus mode."""
    return run_up_id in get_focused_runup_ids()


def set_focus(runup_ids: List[int]) -> List[int]:
    """Set the focused run-up IDs (max 3).

    Updates EngineSettings + syncs RunUp.is_focused column.
    Returns the accepted IDs.
    """
    runup_ids = list(set(runup_ids))[:3]  # dedupe + hard cap

    session = get_session()
    try:
        # Persist IDs
        _upsert_setting(session, "focus_runup_ids", json.dumps(runup_ids))
        _upsert_setting(session, "focus_updated_at", datetime.utcnow().isoformat())

        # Sync is_focused column: clear all, then set selected
        session.query(RunUp).filter(RunUp.is_focused == True).update(  # noqa: E712
            {"is_focused": False}, synchronize_session="fetch"
        )
        if runup_ids:
            session.query(RunUp).filter(RunUp.id.in_(runup_ids)).update(
                {"is_focused": True}, synchronize_session="fetch"
            )

        session.commit()
        _invalidate_cache()
        logger.info("Focus mode set to run-ups: %s", runup_ids)
        return runup_ids
    except Exception:
        session.rollback()
        logger.exception("Failed to set focus")
        return []
    finally:
        session.close()


def clear_focus() -> None:
    """Remove all focus selections."""
    set_focus([])


# ---------------------------------------------------------------------------
# Polymarket manual links
# ---------------------------------------------------------------------------

def get_focus_polymarket_links() -> Dict:
    """Return manual Polymarket links: {str(runup_id): [link_objects]}."""
    return _load_focus_state()["polymarket_links"]


def add_polymarket_link(
    run_up_id: int, url: str, question: str, polymarket_id: str = ""
) -> bool:
    """Add a manual Polymarket link for a run-up."""
    session = get_session()
    try:
        links = dict(get_focus_polymarket_links())  # copy
        key = str(run_up_id)
        if key not in links:
            links[key] = []

        # Avoid duplicates
        existing_urls = {l["url"] for l in links[key]}
        if url in existing_urls:
            return True

        links[key].append({
            "url": url,
            "question": question,
            "polymarket_id": polymarket_id,
        })
        _upsert_setting(session, "focus_polymarket_links", json.dumps(links))
        session.commit()
        _invalidate_cache()
        logger.info("Added Polymarket link for run-up %d: %s", run_up_id, url)
        return True
    except Exception:
        session.rollback()
        logger.exception("Failed to add Polymarket link")
        return False
    finally:
        session.close()


def remove_polymarket_link(run_up_id: int, polymarket_id: str) -> bool:
    """Remove a manual Polymarket link by polymarket_id."""
    session = get_session()
    try:
        links = dict(get_focus_polymarket_links())
        key = str(run_up_id)
        if key not in links:
            return False

        links[key] = [l for l in links[key] if l.get("polymarket_id") != polymarket_id]
        if not links[key]:
            del links[key]

        _upsert_setting(session, "focus_polymarket_links", json.dumps(links))
        session.commit()
        _invalidate_cache()
        return True
    except Exception:
        session.rollback()
        logger.exception("Failed to remove Polymarket link")
        return False
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _upsert_setting(session, key: str, value: str) -> None:
    """Insert or update an EngineSettings row."""
    setting = session.query(EngineSettings).get(key)
    if setting:
        setting.value = value
    else:
        session.add(EngineSettings(key=key, value=value))
