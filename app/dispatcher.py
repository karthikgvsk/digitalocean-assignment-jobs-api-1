"""Standalone transactional outbox dispatcher process."""
from __future__ import annotations

import logging
import os
import signal
import time

import redis
from sqlalchemy import func, or_, select

from app.db import Job, Outbox, SessionLocal, Base, engine, utcnow

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POLL_INTERVAL_SEC = max(0.1, float(os.getenv("DISPATCHER_POLL_INTERVAL_SEC", "1")))
BATCH_SIZE = max(1, int(os.getenv("DISPATCHER_BATCH_SIZE", "100")))
STREAMS = {"high": "jobs:high", "normal": "jobs:normal", "low": "jobs:low"}
stopping = False


def stop(signum: int, frame: object) -> None:
    global stopping
    stopping = True
    logger.info("dispatcher_shutdown_requested signal=%s", signum)


def publish_outbox_once() -> int:
    """Publish due outbox records; duplicates are possible across the DB/Redis boundary."""
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    published = 0
    try:
        with SessionLocal() as session:
            rows = session.scalars(
                select(Outbox)
                .join(Job, Job.id == Outbox.job_id)
                .where(
                    Outbox.published_at.is_(None),
                    or_(Job.run_at.is_(None), Job.run_at <= utcnow()),
                )
                .order_by(Outbox.created_at)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)
            ).all()
            for row in rows:
                client.xadd(
                    row.stream,
                    {"job_id": str(row.job_id), "payload": __import__("json").dumps(row.payload)},
                )
                row.published_at = utcnow()
                published += 1
            session.commit()
            pending = session.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
            ) or 0
            logger.info("outbox_batch_published count=%s pending=%s", published, pending)
    finally:
        client.close()
    return published


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    Base.metadata.create_all(engine)
    logger.info(
        "dispatcher_started poll_interval_sec=%s batch_size=%s",
        POLL_INTERVAL_SEC,
        BATCH_SIZE,
    )
    while not stopping:
        try:
            publish_outbox_once()
        except Exception:
            logger.exception("outbox_publish_failed")
        # Wait interruptibly so SIGTERM can stop the process promptly.
        deadline = time.monotonic() + POLL_INTERVAL_SEC
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(0.2, deadline - time.monotonic()))
    logger.info("dispatcher_stopped")


if __name__ == "__main__":
    main()
