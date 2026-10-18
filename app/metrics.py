"""Application metrics shared by activity and business layers."""
from prometheus_client import Counter

JOBS_SUBMITTED = Counter("jobs_submitted_total", "Number of accepted job submissions")
JOBS_CANCELLED = Counter("jobs_cancelled_total", "Number of cancelled jobs")
