"""HTTP API scaffold; durable persistence and stream dispatch are next implementation steps."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
JOBS_SUBMITTED = Counter("jobs_submitted_total", "Number of accepted job submissions")
JOBS_CANCELLED = Counter("jobs_cancelled_total", "Number of cancelled jobs")


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


# In-memory scaffold only. Replace with PostgreSQL + transactional outbox before deployment.
_jobs: dict[UUID, JobView] = {}
_idempotency: dict[str, UUID] = {}
app = FastAPI(title="DigitalOcean Async Jobs API", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/jobs", response_model=JobView, status_code=status.HTTP_202_ACCEPTED)
def submit_job(
    request: JobCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JobView:
    if idempotency_key and idempotency_key in _idempotency:
        existing = _jobs.get(_idempotency[idempotency_key])
        if existing:
            response.headers["Idempotency-Replayed"] = "true"
            return existing
    now = datetime.now(timezone.utc)
    job = JobView(
        id=uuid4(), task_type=request.task_type, payload=request.payload,
        priority=request.priority, status=JobStatus.queued, attempts=0,
        max_retries=request.max_retries, timeout_sec=request.timeout_sec,
        run_at=request.run_at, created_at=now, updated_at=now,
    )
    _jobs[job.id] = job
    if idempotency_key:
        _idempotency[idempotency_key] = job.id
    JOBS_SUBMITTED.inc()
    logger.info("job_submitted id=%s priority=%s", job.id, job.priority.value)
    return job


@app.get("/jobs", response_model=list[JobView])
def list_jobs(
    job_status: JobStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[JobView]:
    jobs = sorted(_jobs.values(), key=lambda job: job.created_at, reverse=True)
    if job_status:
        jobs = [job for job in jobs if job.status == job_status]
    return jobs[:limit]


@app.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: UUID) -> JobView:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.delete("/jobs/{job_id}", response_model=JobView)
def cancel_job(job_id: UUID) -> JobView:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.queued:
        raise HTTPException(status_code=409, detail="Only queued jobs can be cancelled")
    updated = job.model_copy(update={"status": JobStatus.cancelled, "updated_at": datetime.now(timezone.utc)})
    _jobs[job_id] = updated
    JOBS_CANCELLED.inc()
    return updated


@app.get("/queue/depth")
def queue_depth() -> dict[str, int]:
    return {
        p.value: sum(1 for job in _jobs.values() if job.priority == p and job.status == JobStatus.queued)
        for p in Priority
    }


class DrainRequest(BaseModel):
    confirm: bool = False


@app.post("/queue/drain")
def drain_queue(request: DrainRequest) -> dict[str, Any]:
    if not request.confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to acknowledge queue drain")
    return {"accepted": True, "message": "Drain coordination is not yet persisted"}


@app.post("/jobs/{job_id}/retry", response_model=JobView)
def retry_job(job_id: UUID) -> JobView:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.failed, JobStatus.dead_lettered):
        raise HTTPException(status_code=409, detail="Only failed or dead-lettered jobs can be retried")
    updated = job.model_copy(update={"status": JobStatus.queued, "attempts": 0, "error": None, "updated_at": datetime.now(timezone.utc)})
    _jobs[job_id] = updated
    return updated
