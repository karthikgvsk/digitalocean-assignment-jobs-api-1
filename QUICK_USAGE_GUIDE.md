# Quick Usage Guide

Use the deployed API to submit a background job and check its status.

**Live API:** https://sea-lion-app-kn7w6.ondigitalocean.app  
**Interactive API docs:** https://sea-lion-app-kn7w6.ondigitalocean.app/docs

## 1. Submit a job

Run this command to submit a sample `echo` job:

```bash
curl -X POST "https://sea-lion-app-kn7w6.ondigitalocean.app/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "echo",
    "payload": {
      "message": "Hello DigitalOcean"
    },
    "priority": "normal",
    "max_retries": 3,
    "timeout_sec": 30
  }'
```

The API returns the created job details, including its `id` and initial `status`. Copy the returned `id` for the next request.

## 2. Check job status

Replace `YOUR_JOB_ID` with the actual `id` from the POST response:

```bash
curl "https://sea-lion-app-kn7w6.ondigitalocean.app/jobs/YOUR_JOB_ID"
```

Check the `status` field. A job will typically move from `queued` to `running` and then `succeeded`; a failing job may be retried or become `dead_lettered`.

## 3. Check API health

```bash
curl "https://sea-lion-app-kn7w6.ondigitalocean.app/health"
```

This is a basic process-health endpoint and does not verify that the database and queue are reachable.

## 4. Try the API in a browser

Open the [Swagger UI](https://sea-lion-app-kn7w6.ondigitalocean.app/docs) to inspect and invoke the available endpoints interactively.

**Troubleshooting:** If a submitted job remains `queued`, check that the DigitalOcean dispatcher and worker components are running. The API accepts jobs separately from their background execution.
