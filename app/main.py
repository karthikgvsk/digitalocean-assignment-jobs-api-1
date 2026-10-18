"""HTTP API for asynchronous job submission and status inspection."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

import redis
from fastapi import FastAPI, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from sqlalchemy import select

from app.db import Base, Job, Outbox, SessionLocal, engine, utcnow

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
JOBS_SUBMITTED = Counter("jobs_submitted_total", "Number of accepted job submissions")
JOBS_CANCELLED = Counter("jobs_cancelled_total", "Number of cancelled jobs")
OUTBOX_PENDING = Gauge("outbox_pending_records", "Unpublished transactional outbox records")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAMS = {"high": "jobs:high", "normal": "jobs:normal", "low": "jobs:low"}


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


def publish_outbox_once() -> int:
    """Publish committed outbox records; duplicate publication is safe for idempotent workers."""
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    published = 0
    try:
        with SessionLocal() as session:
            rows = session.scalars(
                select(Outbox).where(Outbox.published_at.is_(None)).order_by(Outbox.created_at).limit(100)
            ).all()
            for row in rows:
                client.xadd(row.stream, {"job_id": str(row.job_id), "payload": __import__("json").dumps(row.payload)})
                row.published_at = utcnow()
                published += 1
            session.commit()
            OUTBOX_PENDING.set(session.scalar(select(__import__("sqlalchemy").func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))) or 0)
    finally:
        client.close()
    return published


async def outbox_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(publish_outbox_once)
        except Exception:
            logger.exception("outbox_publish_failed")
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(engine)
    task = asyncio.create_task(outbox_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="DigitalOcean Async Jobs API", version="0.1.0", lifespan=lifespan)


def as_view(job: Job) -> JobView:
    return JobView(
        id=job.id, task_type=job.task_type, payload=job.payload, priority=job.priority,
        status=job.status, attempts=job.attempts, max_retries=job.max_retries,
        timeout_sec=job.timeout_sec, run_at=job.run_at, created_at=job.created_at,
        updated_at=job.updated_at, error=job.error,
    )


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
    with SessionLocal() as session:
        if idempotency_key:
            existing = session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
            if existing:
                response.headers["Idempotency-Replayed"] = "true"
                return as_view(existing)
        now = utcnow()
        job = Job(
            task_type=request.task_type, payload=request.payload, priority=request.priority.value,
            status="queued", attempts=0, max_retries=request.max_retries,
            timeout_sec=request.timeout_sec, run_at=request.run_at, created_at=now,
            updated_at=now, idempotency_key=idempotency_key,
        )
        session.add(job)
        session.flush()
        session.add(Outbox(
            job_id=job.id, stream=STREAMS[request.priority.value],
            payload={"job_id": str(job.id), "task_type": request.task_type, "payload": request.payload,
                     "priority": request.priority.value, "timeout_sec": request.timeout_sec},
        ))
        try:
            session.commit()
        except Exception:
            session.rollback()
            if idempotency_key:
                existing = session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
                if existing:
                    response.headers["Idempotency-Replayed"] = "true"
                    return as_view(existing)
            raise
        session.refresh(job)
        JOBS_SUBMITTED.inc()
        logger.info("job_submitted id=%s priority=%s", job.id, job.priority)
        return as_view(job)


@app.get("/jobs", response_model=list[JobView])
def list_jobs(
    job_status: JobStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[JobView]:
    with SessionLocal() as session:
        query = select(Job).order_by(Job.created_at.desc()).limit(limit)
        if job_status:
            query = select(Job).where(Job.status == job_status.value).order_by(Job.created_at.desc()).limit(limit)
        return [as_view(job) for job in session.scalars(query).all()]


@app.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: UUID) -> JobView:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return as_view(job)


@app.delete("/jobs/{job_id}", response_model=JobView)
def cancel_job(job_id: UUID) -> JobView:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status != "queued":
            raise HTTPException(status_code=409, detail="Only queued jobs can be cancelled")
        job.status = "cancelled"
        job.updated_at = utcnow()
        session.commit()
        session.refresh(job)
        JOBS_CANCELLED.inc()
        return as_view(job)


@app.get("/queue/depth")
def queue_depth() -> dict[str, int]:
    from sqlalchemy import func
    with SessionLocal() as session:
        return {
            priority: int(session.scalar(select(func.count()).select_from(Job).where(
                Job.priority == priority, Job.status == "queued"
            )) or 0)
            for priority in STREAMS
        }


@app.post("/queue/drain")
def drain_queue(request: DrainRequest) -> dict[str, Any]:
    if not request.confirm:
        raise HTTPException(status_code=400, detail="Set confirm=true to acknowledge queue drain")
    raise HTTPException(status_code=501, detail="Persistent global drain coordination is not implemented yet")


@app.post("/jobs/{job_id}/retry", response_model=JobView)
def retry_job(job_id: UUID) -> JobView:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status not in ("failed", "dead_lettered"):
            raise HTTPException(status_code=409, detail="Only failed or dead-lettered jobs can be retried")
        job.status, job.attempts, job.error, job.updated_at = "queued", 0, None, utcnow()
        session.add(Outbox(
            job_id=job.id, stream=STREAMS[job.priority],
            payload={"job_id": str(job.id), "task_type": job.task_type, "payload": job.payload,
                     "priority": job.priority, "timeout_sec": job.timeout_sec},
        ))
        session.commit()
        session.refresh(job)
        return as_view(job)
