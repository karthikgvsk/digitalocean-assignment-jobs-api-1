"""Shared network resilience settings for PostgreSQL and Redis/Valkey."""
from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Callable
from typing import TypeVar

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

logger = logging.getLogger(__name__)
T = TypeVar("T")

RETRY_ATTEMPTS = max(1, int(os.getenv("EXTERNAL_RETRY_ATTEMPTS", "3")))
RETRY_BASE_DELAY_SEC = max(0.01, float(os.getenv("EXTERNAL_RETRY_BASE_DELAY_SEC", "0.1")))
RETRY_MAX_DELAY_SEC = max(
    RETRY_BASE_DELAY_SEC, float(os.getenv("EXTERNAL_RETRY_MAX_DELAY_SEC", "1.0"))
)
REDIS_SOCKET_CONNECT_TIMEOUT_SEC = max(
    0.1, float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT_SEC", "2"))
)
REDIS_SOCKET_TIMEOUT_SEC = max(0.1, float(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", "3")))


def is_retryable_database_error(exc: BaseException) -> bool:
    """Retry only connection/transport failures, not integrity or application errors."""
    return isinstance(exc, (OperationalError, InterfaceError, TimeoutError)) or (
        isinstance(exc, DBAPIError) and exc.connection_invalidated
    )


def retry_transient(
    operation: Callable[[], T],
    *,
    should_retry: Callable[[BaseException], bool] = is_retryable_database_error,
    operation_name: str = "external_operation",
    attempts: int = RETRY_ATTEMPTS,
) -> T:
    """Retry transient failures with capped exponential backoff and full jitter."""
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 - predicate below limits retries to transient errors
            if attempt >= attempts or not should_retry(exc):
                raise
            ceiling = min(RETRY_MAX_DELAY_SEC, RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)))
            delay = random.uniform(0, ceiling)
            logger.warning(
                "%s_transient_failure attempt=%s max_attempts=%s retry_in_sec=%.3f error=%s",
                operation_name,
                attempt,
                attempts,
                delay,
                type(exc).__name__,
            )
            time.sleep(delay)
    raise RuntimeError("unreachable retry state")


def create_redis_client(url: str | None = None) -> redis.Redis:
    """Build a Redis client with bounded network timeouts and transport retries."""
    client_url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    retry = Retry(
        ExponentialBackoff(base=RETRY_BASE_DELAY_SEC, cap=RETRY_MAX_DELAY_SEC),
        retries=max(0, RETRY_ATTEMPTS - 1),
    )
    return redis.Redis.from_url(
        client_url,
        decode_responses=True,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_SEC,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SEC,
        retry=retry,
        retry_on_error=[redis.ConnectionError, redis.TimeoutError],
        health_check_interval=30,
    )
