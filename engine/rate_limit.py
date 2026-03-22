"""Rate limiting configuration using slowapi.

Limits:
  - Global default: 120 requests/minute per IP
  - Write endpoints (POST/PUT/DELETE): 30 requests/minute per IP
  - Sensitive endpoints (API key updates, tree generation): 5 requests/minute per IP
"""

import logging

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["120/minute"],
    storage_uri="memory://",
)

# Specific rate limits for sensitive operations
SENSITIVE_LIMIT = "5/minute"
WRITE_LIMIT = "30/minute"
READ_LIMIT = "120/minute"


async def rate_limit_exceeded_handler(
    request: Request, exc: RateLimitExceeded
) -> JSONResponse:
    """Custom handler for rate limit exceeded errors."""
    logger.warning(
        "Rate limit exceeded: %s %s from %s",
        request.method,
        request.url.path,
        get_remote_address(request),
    )
    return JSONResponse(
        status_code=429,
        content={
            "detail": "Rate limit exceeded. Please slow down.",
            "retry_after": str(exc.detail),
        },
    )
