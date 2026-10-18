"""Model/data-access layer: all PostgreSQL reads and transactions live here."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.db import Job, Outbox, SessionLocal, utcnow
from app.resilience import retry_transient

STREAMS = {"high": "jobs:high", "normal": "jobs:normal", "low": "jobs:low"}


def _run_db(operation, name: str):
    return retry_transient(operation, operation_name=f"db_{name}")


def create_job(
    *,
    task_type: str,
    payload: dict,
    priority: str,
    max_retries: int,
    timeout_sec: int,
    run_at: datetime | None,
    idempotency_key: str | None,
) -> tuple[Job, bool]:
    # Reuse the same primary key across transient retries. If the first COMMIT succeeded
    # but its response was lost, a retry finds the already-created row instead of duplicating it.
    job_id = uuid4()

    def operation() -> tuple[Job, bool]:
        with SessionLocal() as session:
            if idempotency_key:
                existing = session.scalar(
                    select(Job).where(Job.idempotency_key == idempotency_key)
                )
                if existing:
                    return existing, True
            existing_by_id = session.get(Job, job_id)
            if existing_by_id:
                return existing_by_id, bool(idempotency_key)
            now = utcnow()
            job = Job(
                id=job_id,
                task_type=task_type,
                payload=payload,
                priority=priority,
                status="queued",
                attempts=0,
                max_retries=max_retries,
                timeout_sec=timeout_sec,
                run_at=run_at,
                created_at=now,
                updated_at=now,
                idempotency_key=idempotency_key,
            )
            session.add(job)
            session.add(
                Outbox(
                    job_id=job.id,
                    stream=STREAMS[priority],
                    payload={
                        "job_id": str(job.id),
                        "task_type": task_type,
                        "payload": payload,
                        "priority": priority,
                        "timeout_sec": timeout_sec,
                    },
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                if idempotency_key:
                    existing = session.scalar(
                        select(Job).where(Job.idempotency_key == idempotency_key)
                    )
                    if existing:
                        return existing, True
                raise
            session.refresh(job)
            return job, False

    return _run_db(operation, "create_job")


def list_jobs(*, status: str | None, limit: int) -> list[Job]:
    def operation() -> list[Job]:
        with SessionLocal() as session:
            query = select(Job).order_by(Job.created_at.desc()).limit(limit)
            if status:
                query = (
                    select(Job)
                    .where(Job.status == status)
                    .order_by(Job.created_at.desc())
                    .limit(limit)
                )
            return list(session.scalars(query).all())

    return _run_db(operation, "list_jobs")


def get_job(job_id: UUID) -> Job | None:
    def operation() -> Job | None:
        with SessionLocal() as session:
            return session.get(Job, job_id)

    return _run_db(operation, "get_job")


def cancel_job(job_id: UUID) -> tuple[Job | None, bool]:
    def operation() -> tuple[Job | None, bool]:
        with SessionLocal() as session:
            job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
            if not job:
                return None, False
            if job.status != "queued":
                return job, False
            job.status = "cancelled"
            job.updated_at = utcnow()
            session.commit()
            session.refresh(job)
            return job, True

    return _run_db(operation, "cancel_job")


def queue_depth() -> dict[str, int]:
    def operation() -> dict[str, int]:
        with SessionLocal() as session:
            return {
                priority: int(
                    session.scalar(
                        select(func.count())
                        .select_from(Job)
                        .where(Job.priority == priority, Job.status == "queued")
                    )
                    or 0
                )
                for priority in STREAMS
            }

    return _run_db(operation, "queue_depth")


def retry_job(job_id: UUID) -> tuple[Job | None, bool]:
    # A stable outbox ID makes retrying an uncertain COMMIT safe for this operation.
    retry_outbox_id = uuid4()

    def operation() -> tuple[Job | None, bool]:
        with SessionLocal() as session:
            job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
            if not job:
                return None, False
            if job.status == "queued" and session.get(Outbox, retry_outbox_id):
                return job, True
            if job.status not in ("failed", "dead_lettered"):
                return job, False
            job.status, job.attempts, job.error, job.updated_at = (
                "queued",
                0,
                None,
                utcnow(),
            )
            session.add(
                Outbox(
                    id=retry_outbox_id,
                    job_id=job.id,
                    stream=STREAMS[job.priority],
                    payload={
                        "job_id": str(job.id),
                        "task_type": job.task_type,
                        "payload": job.payload,
                        "priority": job.priority,
                        "timeout_sec": job.timeout_sec,
                    },
                )
            )
            session.commit()
            session.refresh(job)
            return job, True

    return _run_db(operation, "retry_job")
