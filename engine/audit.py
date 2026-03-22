"""Audit logging helper for security-relevant operations.

Usage::

    from .audit import audit_log

    @router.put("/settings/api-key")
    def update_api_key(request: Request, payload: ApiKeyUpdate):
        ...
        audit_log(request, "api_key.update", "api_key", "anthropic",
                  detail={"masked": masked})
"""

import ipaddress
import json
import logging
from typing import Any, Optional

from starlette.requests import Request

logger = logging.getLogger(__name__)


def audit_log(
    request: Request,
    action: str,
    resource_type: str = "",
    resource_id: Optional[str] = None,
    detail: Optional[Any] = None,
    status: str = "success",
) -> None:
    """Write an audit log entry.  Non-blocking, never raises."""
    try:
        from .db import get_session, AuditLog

        ip = _get_client_ip(request)
        ua = (request.headers.get("user-agent") or "")[:512]

        detail_str = None
        if detail is not None:
            if isinstance(detail, str):
                detail_str = detail
            else:
                detail_str = json.dumps(detail, ensure_ascii=False)

        session = get_session()
        try:
            entry = AuditLog(
                action=action,
                resource_type=resource_type,
                resource_id=str(resource_id) if resource_id is not None else None,
                ip_address=ip,
                user_agent=ua,
                detail=detail_str,
                status=status,
            )
            session.add(entry)
            session.commit()
        finally:
            session.close()

        logger.info(
            "AUDIT: action=%s resource=%s:%s ip=%s status=%s",
            action,
            resource_type,
            resource_id,
            ip,
            status,
        )
    except Exception:
        # Audit logging must never break the application
        logger.exception("Failed to write audit log entry.")


def _get_client_ip(request: Request) -> str:
    """Extract client IP, respecting X-Forwarded-For behind proxies."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            ip = request.client.host if request.client else "unknown"
        return ip
    if request.client:
        return request.client.host
    return "unknown"
