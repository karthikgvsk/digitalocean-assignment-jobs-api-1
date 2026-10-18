"""Integration tests for PostgreSQL, the transactional outbox, Valkey, and worker."""

import pytest
import redis
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.db import Base, Outbox, SessionLocal, engine
from app.dispatcher import publish_outbox_once
from app.main import REDIS_URL, app
from app.worker import CONSUMER, GROUP, STREAMS, ensure_groups, process

client = TestClient(app)


def setup_function() -> None:
    """Use the dedicated CI PostgreSQL/Redis services and isolate each test."""
    Base.metadata.create_all(engine)
    with SessionLocal() as session:
        session.execute(text("TRUNCATE TABLE outbox, jobs CASCADE"))
        session.commit()
    with redis.Redis.from_url(REDIS_URL, decode_responses=True) as redis_client:
        redis_client.flushdb()


def read_one_message(
    redis_client: redis.Redis, stream: str
) -> tuple[str, dict[str, str]]:
    records = redis_client.xreadgroup(
        GROUP, CONSUMER, {stream: ">"}, count=1, block=1000
    )
    assert records, f"Expected a message in {stream}"
    messages = records[0][1]
    assert messages
    return messages[0]


@pytest.mark.parametrize(
    ("priority", "expected_stream"),
    [
        ("high", "jobs:high"),
        ("normal", "jobs:normal"),
        ("low", "jobs:low"),
    ],
)
def test_outbox_dispatches_to_priority_stream(
    priority: str, expected_stream: str
) -> None:
    response = client.post(
        "/jobs",
        json={"task_type": "echo", "payload": {"priority_test": True}, "priority": priority},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]

    assert publish_outbox_once() == 1

    with redis.Redis.from_url(REDIS_URL, decode_responses=True) as redis_client:
        assert len(redis_client.xrange(expected_stream)) == 1
        for stream_name in STREAMS:
            if stream_name != expected_stream:
                assert redis_client.xlen(stream_name) == 0
        fields = redis_client.xrange(expected_stream)[0][1]
        assert fields["job_id"] == job_id

    with SessionLocal() as session:
        outbox_rows = session.query(Outbox).filter(Outbox.job_id == job_id).all()
        assert len(outbox_rows) == 1
        assert outbox_rows[0].published_at is not None


def test_worker_executes_echo_job_and_updates_postgres() -> None:
    response = client.post(
        "/jobs",
        json={"task_type": "echo", "payload": {"message": "integration-test"}},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]
    assert publish_outbox_once() == 1

    with redis.Redis.from_url(REDIS_URL, decode_responses=True) as redis_client:
        ensure_groups(redis_client)
        message_id, fields = read_one_message(redis_client, "jobs:normal")
        process(redis_client, "jobs:normal", message_id, fields)

    result = client.get(f"/jobs/{job_id}")
    assert result.status_code == 200
    assert result.json()["status"] == "succeeded"
    assert result.json()["attempts"] == 1
    assert result.json()["error"] is None


def test_worker_retries_then_dead_letters_after_retries_are_exhausted() -> None:
    response = client.post(
        "/jobs",
        json={
            "task_type": "fail",
            "payload": {"message": "intentional integration-test failure"},
            "max_retries": 1,
        },
    )
    assert response.status_code == 202
    job_id = response.json()["id"]
    assert publish_outbox_once() == 1

    with redis.Redis.from_url(REDIS_URL, decode_responses=True) as redis_client:
        ensure_groups(redis_client)
        message_id, fields = read_one_message(redis_client, "jobs:normal")
        process(redis_client, "jobs:normal", message_id, fields)

        first_attempt = client.get(f"/jobs/{job_id}").json()
        assert first_attempt["status"] == "queued"
        assert first_attempt["attempts"] == 1
        assert first_attempt["error"]

        # The failed attempt wrote a retry record to the transactional outbox.
        assert publish_outbox_once() == 1
        retry_message_id, retry_fields = read_one_message(redis_client, "jobs:normal")
        process(redis_client, "jobs:normal", retry_message_id, retry_fields)

    final_job = client.get(f"/jobs/{job_id}").json()
    assert final_job["status"] == "dead_lettered"
    assert final_job["attempts"] == 2
    assert final_job["error"]


def test_cancelled_job_is_not_executed_by_worker() -> None:
    response = client.post("/jobs", json={"task_type": "echo", "payload": {"should": "not run"}})
    assert response.status_code == 202
    job_id = response.json()["id"]
    assert publish_outbox_once() == 1

    cancelled = client.delete(f"/jobs/{job_id}")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    with redis.Redis.from_url(REDIS_URL, decode_responses=True) as redis_client:
        ensure_groups(redis_client)
        message_id, fields = read_one_message(redis_client, "jobs:normal")
        process(redis_client, "jobs:normal", message_id, fields)
        pending = redis_client.xpending("jobs:normal", GROUP)
        assert pending["pending"] == 0

    final_job = client.get(f"/jobs/{job_id}").json()
    assert final_job["status"] == "cancelled"
    assert final_job["attempts"] == 0
