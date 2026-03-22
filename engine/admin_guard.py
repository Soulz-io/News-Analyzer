"""Server-side admin authorization guard.

Usage:
    from .admin_guard import require_admin

    @router.post("/admin-only-endpoint")
    def my_endpoint(request: Request, _admin=Depends(require_admin)):
        ...
"""

from fastapi import Request, HTTPException, Depends

from .user_auth import get_session_data, SESSION_COOKIE_NAME


def require_admin(request: Request) -> dict:
    """FastAPI dependency that enforces admin-only access.

    Returns the session data if user is admin.
    Raises 403 Forbidden otherwise.
    """
    token = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = get_session_data(token)

    if not session_data:
        raise HTTPException(status_code=401, detail="Not authenticated")

    if not session_data.get("is_admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")

    return session_data
