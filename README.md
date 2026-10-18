# DigitalOcean Assignment — Async Jobs API

FastAPI service for asynchronous job submission, backed by PostgreSQL and Redis Streams.

## Architecture
- API validates requests and persists jobs.
- Transactional outbox records enqueue intent in the same database transaction as the job; a dispatcher publishes it to Redis Streams.
- Worker replicas consume via consumer groups and update job state in PostgreSQL.
- Delivery is at-least-once: handlers must be idempotent. Dead-letter state is persisted before acknowledging the stream message.
- `max_retries` means retries after the initial attempt. One-time `run_at` scheduling is in scope; cron can be added later.

## Local development
Requires Docker Compose:
```bash
docker compose up --build
```
API docs: http://localhost:8000/docs · Health: http://localhost:8000/health · Metrics: http://localhost:8000/metrics

## API
- `POST /jobs` — submit (202 Accepted); supports `Idempotency-Key`
- `GET /jobs/{job_id}` and `GET /jobs?status=queued`
- `GET /queue/depth`
- `DELETE /jobs/{job_id}` — cancel queued job
- `POST /jobs/{job_id}/retry`
- `POST /queue/drain` — requires `{"confirm": true}`

## Tests
```bash
pip install -e '.[dev]'
pytest
ruff check .
```

## Production/scaling notes
Scale API and workers independently. Increase worker concurrency gradually while watching queue lag, DB connections, retries, and execution latency. Use managed PostgreSQL/Redis, private networking, TLS, and platform-managed secrets. Alert on oldest pending-job age, dead-letter rate, outbox lag, and worker health. Never commit credentials.
