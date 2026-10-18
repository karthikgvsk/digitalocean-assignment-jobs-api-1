"""Redis Streams worker with consumer-group delivery and bounded execution."""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pipe, Process
from uuid import UUID

import redis
from sqlalchemy import select

from app.db import Job, Outbox, SessionLocal, utcnow

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CONCURRENCY = max(1, int(os.getenv("WORKER_CONCURRENCY", "4")))
STREAMS = ["jobs:high", "jobs:normal", "jobs:low"]
GROUP = "job-workers"
CONSUMER = f"{socket.gethostname()}-{os.getpid()}"
stopping = False


def stop(signum: int, frame: object) -> None:
    global stopping
    stopping = True
    logger.info("worker_shutdown_requested signal=%s", signum)


def execute_task(task_type: str, payload: dict) -> dict:
    """Small, explicit handler registry for the assessment's sample tasks."""
    if task_type == "echo":
        return payload
    if task_type == "sleep":
        time.sleep(max(0, min(float(payload.get("seconds", 0)), 3600)))
        return {"slept": payload.get("seconds", 0)}
    if task_type == "fail":
        raise RuntimeError(str(payload.get("message", "Requested test failure")))
    raise ValueError(f"Unsupported task_type: {task_type}")


def _run_in_child(connection: object, task_type: str, payload: dict) -> None:
    try:
        connection.send((True, execute_task(task_type, payload)))
    except BaseException as exc:
        connection.send((False, f"{type(exc).__name__}: {exc}"))
    finally:
        connection.close()


def execute_with_timeout(task_type: str, payload: dict, timeout_sec: int) -> dict:
    parent, child = Pipe(duplex=False)
    process = Process(target=_run_in_child, args=(child, task_type, payload), daemon=True)
    process.start()
    child.close()
    process.join(timeout=timeout_sec)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()
        raise TimeoutError(f"Task exceeded timeout of {timeout_sec} seconds")
    if parent.poll():
        succeeded, value = parent.recv()
        parent.close()
        if succeeded:
            return value
        raise RuntimeError(value)
    parent.close()
    raise RuntimeError(f"Task process exited unexpectedly (exit code {process.exitcode})")


def ensure_groups(client: redis.Redis) -> None:
    for stream in STREAMS:
        try:
            client.xgroup_create(stream, GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise


def process(client: redis.Redis, stream: str, message_id: str, fields: dict[str, str]) -> None:
    job_id = fields.get("job_id")
    if not job_id:
        client.xack(stream, GROUP, message_id)
        return
    with SessionLocal() as session:
        try:
            parsed_job_id = UUID(job_id)
        except ValueError:
            client.xack(stream, GROUP, message_id)
            return
        job = session.scalar(select(Job).where(Job.id == parsed_job_id).with_for_update())
        if not job or job.status in ("succeeded", "cancelled", "dead_lettered", "running"):
            client.xack(stream, GROUP, message_id)
            return
        if job.status != "queued":
            client.xack(stream, GROUP, message_id)
            return
        job.status = "running"
        job.attempts += 1
        job.updated_at = utcnow()
        session.commit()
        task_type, payload, timeout_sec = job.task_type, job.payload, job.timeout_sec

    try:
        result = execute_with_timeout(task_type, payload, timeout_sec)
        with SessionLocal() as session:
            job = session.get(Job, parsed_job_id)
            if job:
                job.status, job.error, job.updated_at = "succeeded", None, utcnow()
                session.commit()
        logger.info("job_succeeded id=%s result=%s", job_id, json.dumps(result, default=str))
        client.xack(stream, GROUP, message_id)
    except (Exception, TimeoutError) as exc:
        with SessionLocal() as session:
            job = session.get(Job, job_id)
            if job:
                job.error = str(exc)[:4000]
                job.updated_at = utcnow()
                if job.attempts <= job.max_retries:
                    job.status = "queued"
                    session.add(Outbox(
                        job_id=job.id, stream=stream,
                        payload={"job_id": str(job.id), "task_type": job.task_type, "payload": job.payload,
                                 "priority": job.priority, "timeout_sec": job.timeout_sec},
                    ))
                else:
                    job.status = "dead_lettered"
                session.commit()
        logger.exception("job_attempt_failed id=%s", job_id)
        # A retry has been durably written to outbox; terminal state is durable before ACK.
        client.xack(stream, GROUP, message_id)


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    ensure_groups(client)
    logger.info("worker_started consumer=%s concurrency=%s", CONSUMER, CONCURRENCY)
    pool = ThreadPoolExecutor(max_workers=CONCURRENCY)
    try:
        while not stopping:
            if client.get("jobs:drain") == "1":
                time.sleep(0.5)
                continue
            records = client.xreadgroup(
                GROUP, CONSUMER, {stream: ">" for stream in STREAMS},
                count=CONCURRENCY, block=1000,
            )
            for stream, messages in records:
                for message_id, fields in messages:
                    if stopping or client.get("jobs:drain") == "1":
                        break
                    pool.submit(process, client, stream, message_id, fields)
    finally:
        # Let already-claimed jobs finish before the process exits on SIGTERM.
        pool.shutdown(wait=True, cancel_futures=False)
        client.close()
    logger.info("worker_stopped")


if __name__ == "__main__":
    main()
