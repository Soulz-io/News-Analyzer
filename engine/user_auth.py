"""User authentication: registration, login, session management.

Uses bcrypt for password hashing, server-side sessions with httponly cookies.
Portfolio is stored per-user in the User table.
"""

import secrets
import time
import re
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import bcrypt

from .db import get_session, User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SESSION_MAX_AGE = 86400 * 7  # 7 days
SESSION_COOKIE_NAME = "oc_session"
MAX_FAILED_LOGINS = 5
LOCKOUT_MINUTES = 15
USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")
EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# ---------------------------------------------------------------------------
# Session store (in-memory with expiry)
# ---------------------------------------------------------------------------
_sessions: Dict[str, Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(12)).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_signup(username: str, email: str, password: str,
                    first_name: str, last_name: str) -> Optional[str]:
    """Return error message or None if valid."""
    if not username or not USERNAME_RE.match(username):
        return "Username must be 3-20 characters (letters, numbers, underscore)"
    if not email or not EMAIL_RE.match(email):
        return "Valid email address required"
    if not first_name or len(first_name.strip()) < 1:
        return "First name is required"
    if not last_name or len(last_name.strip()) < 1:
        return "Last name is required"
    if len(password) < 8:
        return "Password must be at least 8 characters"
    if password.lower() == password:
        return "Password must contain at least one uppercase letter"
    if not any(c.isdigit() for c in password):
        return "Password must contain at least one number"
    return None


# ---------------------------------------------------------------------------
# User CRUD
# ---------------------------------------------------------------------------
def create_user(username: str, email: str, password: str,
                first_name: str, last_name: str) -> User:
    """Create a new user. Raises ValueError on validation/duplicate errors."""
    username = username.strip().lower()
    email = email.strip().lower()
    first_name = first_name.strip()
    last_name = last_name.strip()

    err = validate_signup(username, email, password, first_name, last_name)
    if err:
        raise ValueError(err)

    session = get_session()
    try:
        if session.query(User).filter_by(username=username).first():
            raise ValueError("Username already taken")
        if session.query(User).filter_by(email=email).first():
            raise ValueError("Email already registered")

        user = User(
            username=username,
            email=email,
            first_name=first_name,
            last_name=last_name,
            password_hash=hash_password(password),
            portfolio_json="[]",
            onboarded=False,
        )
        session.add(user)
        session.commit()
        session.refresh(user)
        logger.info("User created: %s (%s)", username, email)
        return user
    except ValueError:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def authenticate(login: str, password: str) -> Optional[User]:
    """Authenticate by username or email. Returns User or None.
    Handles account lockout after too many failed attempts.
    """
    login = login.strip().lower()
    session = get_session()
    try:
        user = (
            session.query(User)
            .filter((User.username == login) | (User.email == login))
            .first()
        )
        if not user:
            return None

        # Check lockout
        if user.locked_until and user.locked_until > datetime.utcnow():
            remaining = int((user.locked_until - datetime.utcnow()).total_seconds() / 60) + 1
            logger.warning("Account %s locked for %d more minutes", user.username, remaining)
            return None

        if not verify_password(password, user.password_hash):
            user.failed_logins = (user.failed_logins or 0) + 1
            if user.failed_logins >= MAX_FAILED_LOGINS:
                user.locked_until = datetime.utcnow() + timedelta(minutes=LOCKOUT_MINUTES)
                logger.warning("Account %s locked after %d failed attempts", user.username, user.failed_logins)
            session.commit()
            return None

        # Success — reset counters
        user.failed_logins = 0
        user.locked_until = None
        user.last_login = datetime.utcnow()
        session.commit()
        session.refresh(user)
        return user
    except Exception:
        session.rollback()
        logger.exception("Authentication error")
        return None
    finally:
        session.close()


def get_user_by_id(user_id: int) -> Optional[User]:
    session = get_session()
    try:
        return session.query(User).get(user_id)
    finally:
        session.close()


def get_user_portfolio(user_id: int) -> List[Dict]:
    """Get user's portfolio as list of dicts."""
    import json
    session = get_session()
    try:
        user = session.query(User).get(user_id)
        if not user:
            return []
        return json.loads(user.portfolio_json or "[]")
    except Exception:
        return []
    finally:
        session.close()


def set_user_portfolio(user_id: int, holdings: List[Dict]) -> bool:
    """Save user's portfolio."""
    import json
    session = get_session()
    try:
        user = session.query(User).get(user_id)
        if not user:
            return False
        user.portfolio_json = json.dumps(holdings)
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False
    finally:
        session.close()


def set_user_onboarded(user_id: int, session_token: str = None) -> None:
    """Mark user as having completed onboarding. Also updates in-memory session."""
    session = get_session()
    try:
        user = session.query(User).get(user_id)
        if user:
            user.onboarded = True
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------
def create_session(user: User) -> str:
    """Create session, return token."""
    _cleanup_expired_sessions()
    token = secrets.token_urlsafe(48)
    _sessions[token] = {
        "user_id": user.id,
        "username": user.username,
        "email": user.email,
        "display_name": f"{user.first_name} {user.last_name}",
        "onboarded": user.onboarded,
        "is_admin": getattr(user, "is_admin", False),
        "created_at": time.time(),
        "expires_at": time.time() + SESSION_MAX_AGE,
    }
    return token


def get_session_data(token: str) -> Optional[Dict[str, Any]]:
    """Get full session data or None."""
    if not token:
        return None
    data = _sessions.get(token)
    if not data:
        return None
    if time.time() > data["expires_at"]:
        del _sessions[token]
        return None
    return data


def get_session_user(token: str) -> Optional[str]:
    """Get username for session (backward compat with middleware)."""
    data = get_session_data(token)
    return data["username"] if data else None


def update_session(token: str, **kwargs) -> None:
    """Update fields in an existing session."""
    data = _sessions.get(token)
    if data:
        data.update(kwargs)


def destroy_session(token: str) -> None:
    _sessions.pop(token, None)


def _cleanup_expired_sessions() -> None:
    now = time.time()
    expired = [k for k, v in _sessions.items() if now > v["expires_at"]]
    for k in expired:
        del _sessions[k]
