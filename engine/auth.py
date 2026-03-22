"""Bearer token authentication middleware for the News Analyzer engine.

Provides a FastAPI dependency that validates the Authorization header
against the configured API_BEARER_TOKEN.  If no token is configured,
all requests are allowed (development mode) with a startup warning.
"""

import logging
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import config

logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)

# Public endpoints that never require authentication
PUBLIC_PATHS: set = {
    "/api/status",
    "/docs",
    "/openapi.json",
    "/redoc",
}


def _is_public(path: str) -> bool:
    """Check if the request path is always public."""
    return path in PUBLIC_PATHS


def _is_static_or_ui(path: str) -> bool:
    """Check if the request is for static UI files."""
    return (
        path == "/"
        or path.startswith("/plugins/")
        or path.startswith("/static/")
    )


async def verify_bearer_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> Optional[str]:
    """FastAPI dependency that validates the Bearer token.

    If API_BEARER_TOKEN is not configured, all requests pass (dev mode).
    If configured, all non-public endpoints require a valid token.

    Returns the token string if valid, or None for public/dev requests.
    """
    path = request.url.path
    configured_token = config.api_bearer_token

    # Static files and UI always pass
    if _is_static_or_ui(path):
        return None

    # Public endpoints always pass
    if _is_public(path):
        return None

    # If no token is configured, allow all (dev mode)
    if not configured_token:
        return None

    # Token is configured -- require valid credentials
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header. Use: Authorization: Bearer <token>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not secrets.compare_digest(credentials.credentials, configured_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return credentials.credentials


def generate_token(length: int = 48) -> str:
    """Generate a cryptographically secure bearer token."""
    return secrets.token_urlsafe(length)


def log_auth_warning_if_needed() -> None:
    """Log a warning at startup if no bearer token is configured."""
    if not config.api_bearer_token:
        logger.warning(
            "WARNING: API_BEARER_TOKEN is not set. All endpoints are unauthenticated. "
            "Set API_BEARER_TOKEN in .env for production use."
        )
    else:
        logger.info("Bearer token authentication is ENABLED.")
