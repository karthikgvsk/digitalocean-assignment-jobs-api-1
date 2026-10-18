"""FastAPI application bootstrap and cross-cutting endpoints."""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.activities.jobs import router as jobs_router
from app.db import Base, engine

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Schema creation is retained for this assignment; use versioned migrations in production.
    Base.metadata.create_all(engine)
    yield


app = FastAPI(title="DigitalOcean Async Jobs API", version="0.2.0", lifespan=lifespan)
app.include_router(jobs_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
