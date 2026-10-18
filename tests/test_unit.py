"""Fast unit tests for worker task execution and request validation."""

import pytest
from pydantic import ValidationError

from app.main import JobCreate
from app.worker import execute_task, execute_with_timeout


def test_echo_task_returns_payload() -> None:
    payload = {"message": "hello", "count": 3}
    assert execute_task("echo", payload) == payload


def test_sleep_task_returns_requested_duration() -> None:
    assert execute_task("sleep", {"seconds": 0}) == {"slept": 0}


def test_fail_task_raises_requested_error() -> None:
    with pytest.raises(RuntimeError, match="requested failure"):
        execute_task("fail", {"message": "requested failure"})


def test_unknown_task_type_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported task_type"):
        execute_task("unknown", {})


def test_execute_with_timeout_returns_successful_result() -> None:
    assert execute_with_timeout("echo", {"message": "ok"}, timeout_sec=2) == {
        "message": "ok"
    }


def test_execute_with_timeout_propagates_task_failure() -> None:
    with pytest.raises(RuntimeError, match="Requested test failure"):
        execute_with_timeout("fail", {"message": "Requested test failure"}, timeout_sec=2)


def test_execute_with_timeout_stops_long_running_task() -> None:
    with pytest.raises(TimeoutError, match="exceeded timeout"):
        execute_with_timeout("sleep", {"seconds": 2}, timeout_sec=0.1)


def test_job_request_defaults_and_priority() -> None:
    request = JobCreate(task_type="echo")
    assert request.priority.value == "normal"
    assert request.max_retries == 3
    assert request.timeout_sec == 30
    assert request.payload == {}


@pytest.mark.parametrize(
    "payload",
    [
        {"task_type": ""},
        {"task_type": "echo", "priority": "urgent"},
        {"task_type": "echo", "timeout_sec": 0},
        {"task_type": "echo", "max_retries": -1},
    ],
)
def test_invalid_job_request_is_rejected(payload: dict) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate(payload)
