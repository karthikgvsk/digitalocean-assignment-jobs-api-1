"""SQLAlchemy models and database helpers."""
import os
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, Integer, String, Text, create_engine
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://jobs:jobs@localhost:5432/jobs")
DB_CONNECT_TIMEOUT_SEC = max(1, int(os.getenv("DB_CONNECT_TIMEOUT_SEC", "5")))
DB_POOL_TIMEOUT_SEC = max(1, int(os.getenv("DB_POOL_TIMEOUT_SEC", "5")))
DB_STATEMENT_TIMEOUT_MS = max(100, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "5000")))

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_timeout=DB_POOL_TIMEOUT_SEC,
    pool_recycle=1800,
    connect_args={
        "connect_timeout": DB_CONNECT_TIMEOUT_SEC,
        "options": f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}",
    },
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[object] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[str] = mapped_column(String(10), nullable=False, default="normal", index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued", index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True, index=True)


class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[object] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id: Mapped[object] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    stream: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
