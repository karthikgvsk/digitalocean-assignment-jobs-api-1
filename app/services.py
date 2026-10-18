"""Business logic layer: enforce job lifecycle rules independently of HTTP."""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any
from uuid import UUID

import redis

from app import repositories
from app.metrics import JOBS_CANCELLED, JOBS_SUBMITTED
from app.resilience import create_redis_client

logger = logging.getLogger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class JobNotFoundError(Exception):
    pass


class InvalidJobStateError(Exception):
    pass


class ConfirmationRequiredError(Exception):
    pass


class QueueUnavailableError(Exception):
    pass


def submit_job(
    *,
    task_type: str,
    payload: dict,
    priority: str,
    max_retries: int,
    timeout_sec: int,
    run_at: datetime | None,
    idempotency_key: str | None,
):
    job, replayed = repositories.create_job(
        task_type=task_type,
        payload=payload,
        priority=priority,
        max_retries=max_retries,
        timeout_sec=timeout_sec,
        run_at=run_at,
        idempotency_key=idempotency_key,
    )
    if not replayed:
        JOBS_SUBMITTED.inc()
        logger.info("job_submitted id=%s priority=%s", job.id, job.priority)
    return job, replayed


def list_jobs(status: str | None, limit: int):
    return repositories.list_jobs(status=status, limit=limit)


def get_job(job_id: UUID):
    job = repositories.get_job(job_id)
    if not job:
        raise JobNotFoundError(f"Job {job_id} was not found")
    return job


def cancel_job(job_id: UUID):
    job, cancelled = repositories.cancel_job(job_id)
    if not job:
        raise JobNotFoundError(f"Job {job_id} was not found")
    if not cancelled:
        raise InvalidJobStateError("Only queued jobs can be cancelled")
    JOBS_CANCELLED.inc()
    return job


def queue_depth() -> dict[str, int]:
    return repositories.queue_depth()


def drain_queue(confirm: bool) -> dict[str, Any]:
    if not confirm:
        raise ConfirmationRequiredError("Set confirm=true to acknowledge queue drain")
    client = create_redis_client(REDIS_URL)
    try:
        client.set("jobs:drain", "1")
    except redis.RedisError as exc:
        logger.exception("queue_drain_failed")
        raise QueueUnavailableError("Queue control store unavailable") from exc
    finally:
        client.close()
    return {
        "accepted": True,
        "state": "draining",
        "message": "Workers will stop claiming new jobs",
    }


def retry_job(job_id: UUID):
    job, retried = repositories.retry_job(job_id)
    if not job:
        raise JobNotFoundError(f"Job {job_id} was not found")
    if not retried:
        raise InvalidJobStateError("Only failed or dead-lettered jobs can be retried")
    return job
