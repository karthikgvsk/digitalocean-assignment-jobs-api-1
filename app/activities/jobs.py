"""Activity layer: HTTP routes, request validation, and response mapping."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from app import services
from app.db import Job
from app.services import (
    ConfirmationRequiredError,
    InvalidJobStateError,
    JobNotFoundError,
    QueueUnavailableError,
)

router = APIRouter()


class Priority(str, Enum):
    high = "high"
    normal = "normal"
    low = "low"


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    dead_lettered = "dead_lettered"
    cancelled = "cancelled"


class JobCreate(BaseModel):
    task_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.normal
    max_retries: int = Field(default=3, ge=0, le=20)
    timeout_sec: int = Field(default=30, ge=1, le=3600)
    run_at: datetime | None = None


class JobView(BaseModel):
    id: UUID
    task_type: str
    payload: dict[str, Any]
    priority: Priority
    status: JobStatus
    attempts: int
    max_retries: int
    timeout_sec: int
    run_at: datetime | None
    created_at: datetime
    updated_at: datetime
    error: str | None = None


class DrainRequest(BaseModel):
    confirm: bool = False


def as_view(job: Job) -> JobView:
    return JobView(
        id=job.id,
        task_type=job.task_type,
        payload=job.payload,
        priority=job.priority,
        status=job.status,
        attempts=job.attempts,
        max_retries=job.max_retries,
        timeout_sec=job.timeout_sec,
        run_at=job.run_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
        error=job.error,
    )


def _raise_http_error(exc: Exception) -> None:
    if isinstance(exc, JobNotFoundError):
        raise HTTPException(status_code=404, detail="Job not found") from exc
    if isinstance(exc, InvalidJobStateError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, ConfirmationRequiredError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, QueueUnavailableError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    raise exc


@router.post("/jobs", response_model=JobView, status_code=status.HTTP_202_ACCEPTED)
def submit_job(
    request: JobCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JobView:
    try:
        job, replayed = services.submit_job(
            task_type=request.task_type,
            payload=request.payload,
            priority=request.priority.value,
            max_retries=request.max_retries,
            timeout_sec=request.timeout_sec,
            run_at=request.run_at,
            idempotency_key=idempotency_key,
        )
    except Exception as exc:
        _raise_http_error(exc)
    if replayed:
        response.headers["Idempotency-Replayed"] = "true"
    return as_view(job)


@router.get("/jobs", response_model=list[JobView])
def list_jobs(
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[JobView]:
    try:
        return [as_view(job) for job in services.list_jobs(
            status=job_status.value if job_status else None, limit=limit
        )]
    except Exception as exc:
        _raise_http_error(exc)


@router.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: UUID) -> JobView:
    try:
        return as_view(services.get_job(job_id))
    except Exception as exc:
        _raise_http_error(exc)


@router.delete("/jobs/{job_id}", response_model=JobView)
def cancel_job(job_id: UUID) -> JobView:
    try:
        return as_view(services.cancel_job(job_id))
    except Exception as exc:
        _raise_http_error(exc)


@router.get("/queue/depth")
def queue_depth() -> dict[str, int]:
    try:
        return services.queue_depth()
    except Exception as exc:
        _raise_http_error(exc)


@router.post("/queue/drain")
def drain_queue(request: DrainRequest) -> dict[str, Any]:
    try:
        return services.drain_queue(request.confirm)
    except Exception as exc:
        _raise_http_error(exc)


@router.post("/jobs/{job_id}/retry", response_model=JobView)
def retry_job(job_id: UUID) -> JobView:
    try:
        return as_view(services.retry_job(job_id))
    except Exception as exc:
        _raise_http_error(exc)
