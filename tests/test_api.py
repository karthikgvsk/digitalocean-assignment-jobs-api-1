from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db import Base, SessionLocal, engine
from app.main import app

client = TestClient(app)


def setup_function() -> None:
    Base.metadata.create_all(engine)
    with SessionLocal() as session:
        session.execute(text("TRUNCATE TABLE outbox, jobs CASCADE"))
        session.commit()


def test_submit_and_fetch_job() -> None:
    response = client.post("/jobs", json={"task_type": "echo", "payload": {"value": 7}})
    assert response.status_code == 202
    job = response.json()
    assert job["status"] == "queued"
    fetched = client.get(f"/jobs/{job['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == job["id"]


def test_idempotency_key_returns_same_job() -> None:
    first = client.post("/jobs", headers={"Idempotency-Key": "request-123"}, json={"task_type": "echo"})
    second = client.post("/jobs", headers={"Idempotency-Key": "request-123"}, json={"task_type": "echo"})
    assert first.json()["id"] == second.json()["id"]
    assert second.headers["Idempotency-Replayed"] == "true"


def test_cancel_queued_job() -> None:
    job = client.post("/jobs", json={"task_type": "echo"}).json()
    response = client.delete(f"/jobs/{job['id']}")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


def test_drain_requires_confirmation() -> None:
    assert client.post("/queue/drain", json={"confirm": False}).status_code == 400
    response = client.post("/queue/drain", json={"confirm": True})
    assert response.status_code == 200
    assert response.json()["state"] == "draining"
