"""Unit tests for bounded transient-failure retry behavior."""
import pytest
from sqlalchemy.exc import OperationalError

from app import resilience


def test_retry_transient_retries_database_transport_errors(monkeypatch) -> None:
    calls = 0
    sleeps: list[float] = []
    monkeypatch.setattr(resilience.time, "sleep", sleeps.append)
    monkeypatch.setattr(resilience.random, "uniform", lambda low, high: high)

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OperationalError("SELECT 1", {}, RuntimeError("connection reset"))
        return "ok"

    assert resilience.retry_transient(operation, operation_name="test_db") == "ok"
    assert calls == 3
    assert sleeps == [0.1, 0.2]


def test_retry_transient_does_not_retry_non_transient_errors(monkeypatch) -> None:
    calls = 0
    monkeypatch.setattr(resilience.time, "sleep", lambda _: pytest.fail("must not sleep"))

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("invalid input")

    with pytest.raises(ValueError, match="invalid input"):
        resilience.retry_transient(operation)

    assert calls == 1
