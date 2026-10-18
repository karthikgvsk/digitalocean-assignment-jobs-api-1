from uuid import UUID

import redis
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from app.db import Base, Job, Outbox, SessionLocal, engine
from app.main import REDIS_URL, app

client = TestClient(app)


def setup_function() -> None:
    Base.metadata.create_all(engine)
    with SessionLocal() as session:
        session.execute(text("TRUNCATE TABLE outbox, jobs CASCADE"))
        session.commit()
    with redis.Redis.from_url(REDIS_URL, decode_responses=True) as redis_client:
        redis_client.flushdb()


def test_submit_and_fetch_job() -> None:
    response = client.post("/jobs", json={"task_type": "echo", "payload": {"value": 7}})
    assert response.status_code == 202
    job = response.json()
    assert job["status"] == "queued"
    fetched = client.get(f"/jobs/{job['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == job["id"]


def test_idempotency_key_returns_same_job_without_duplicate_outbox() -> None:
    first = client.post(
        "/jobs",
        headers={"Idempotency-Key": "request-123"},
        json={"task_type": "echo"},
    )
    second = client.post(
        "/jobs",
        headers={"Idempotency-Key": "request-123"},
        json={"task_type": "echo"},
    )
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert second.headers["Idempotency-Replayed"] == "true"

    with SessionLocal() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1
        assert session.scalar(select(func.count()).select_from(Outbox)) == 1


def test_cancel_queued_job() -> None:
    job = client.post("/jobs", json={"task_type": "echo"}).json()
    response = client.delete(f"/jobs/{job['id']}")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


def test_cannot_cancel_non_queued_job() -> None:
    job = client.post("/jobs", json={"task_type": "echo"}).json()
    # The test database is isolated, so transition the record to running directly.
    with SessionLocal() as session:
        stored_job = session.get(Job, UUID(job["id"]))
        stored_job.status = "running"
        session.commit()

    response = client.delete(f"/jobs/{job['id']}")
    assert response.status_code == 409


def test_drain_requires_confirmation() -> None:
    assert client.post("/queue/drain", json={"confirm": False}).status_code == 400
    response = client.post("/queue/drain", json={"confirm": True})
    assert response.status_code == 200
    assert response.json()["state"] == "draining"


def test_invalid_job_request_returns_validation_error() -> None:
    response = client.post("/jobs", json={"task_type": "echo", "timeout_sec": 0})
    assert response.status_code == 422


def test_list_jobs_can_filter_by_status() -> None:
    queued = client.post("/jobs", json={"task_type": "echo"}).json()
    cancelled = client.post("/jobs", json={"task_type": "echo"}).json()
    assert client.delete(f"/jobs/{cancelled['id']}").json()["status"] == "cancelled"

    response = client.get("/jobs", params={"status": "queued"})
    assert response.status_code == 200
    assert [job["id"] for job in response.json()] == [queued["id"]]
