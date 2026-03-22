"""Session authentication middleware.

Protects ALL routes except public paths.
Redirects unauthenticated users to /login.
Redirects authenticated-but-not-onboarded users to /onboarding.
Attaches full session data to request.state for downstream use.
"""

import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

from .user_auth import get_session_data, get_session_user, SESSION_COOKIE_NAME

logger = logging.getLogger(__name__)

# Paths that don't require authentication
PUBLIC_PATHS = frozenset({
    "/auth/login",
    "/auth/signup",
    "/auth/logout",
    "/auth/me",
    "/login",
    "/api/status",
})

PUBLIC_PREFIXES = (
    "/auth/",
)

# Paths accessible during onboarding (before portfolio setup)
ONBOARDING_PATHS = frozenset({
    "/onboarding",
    "/auth/complete-onboarding",
    "/api/portfolio/search",
})


class SessionAuthMiddleware(BaseHTTPMiddleware):
    """Require valid session cookie for all protected routes."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Always try to attach session data if cookie exists
        token = request.cookies.get(SESSION_COOKIE_NAME)
        session_data = get_session_data(token)

        if session_data:
            request.state.user = session_data.get("username")
            request.state.user_id = session_data.get("user_id")
            request.state.session = session_data

        # Allow public paths
        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)

        # Not authenticated → redirect or 401
        if not session_data:
            accept = request.headers.get("accept", "")
            if "application/json" in accept or path.startswith("/api/"):
                return JSONResponse({"detail": "Not authenticated"}, status_code=401)
            return RedirectResponse(url="/login", status_code=302)

        # Authenticated but not onboarded → redirect to onboarding
        # (except for onboarding-related paths and API calls)
        if not session_data.get("onboarded") and path not in ONBOARDING_PATHS:
            if not path.startswith("/api/"):
                return RedirectResponse(url="/onboarding", status_code=302)

        return await call_next(request)
