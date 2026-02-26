# DOT Architecture

## Core Pattern: Push-Not-Poll

DOT uses an event-driven, push-based architecture. When a long-running job completes, the result is **pushed** via message to the next step in the pipeline. We do **not** poll for job status from Cloud Functions.

### Why Push-Not-Poll

- **Cloud Functions have execution time limits** — polling for long-running job status keeps the function alive unnecessarily, consuming resources and risking timeouts.
- **Cloud Workflows handle long-running orchestration** — Workflows can sleep, poll, and branch without the same cost/timeout constraints as Cloud Functions.
- **Pub/Sub + Eventarc decouples producers from consumers** — the webhook function returns immediately after publishing a message; it doesn't need to know what happens next.
- **Consistent infrastructure** — all orchestration uses the same Pub/Sub + Eventarc + Cloud Workflow stack. No mixing in Cloud Tasks, Cloud Scheduler polling, or other async patterns.

### The Standard Flow

```
External event (webhook, schedule, etc.)
  → Cloud Function (validate, classify, publish to Pub/Sub)
  → return 200 immediately

Pub/Sub topic
  → Eventarc trigger
  → Cloud Workflow (orchestration: API calls, retries, monitoring)
```

**Cloud Functions** are thin and fast: validate input, make a routing decision, publish a message, return. They should never make long-running API calls or poll for status.

**Cloud Workflows** handle all orchestration: API calls to external services, polling for job completion, conditional branching, retries with delays, error handling.

## Current Workflows

### dbt Job Trigger (Pub/Sub → dbt Cloud)

```
Pub/Sub topic: cloud-run-job-completed
  → Eventarc → Cloud Workflow (cloud-run-job-dbt)
    → POST to dbt-trigger Cloud Function
    → dbt-trigger calls dbt Cloud API to trigger job
```

Publishers: okta-sync, woo-sync, process-geography, google-sheets-trigger

### Fivetran → dbt (Pub/Sub → dbt Cloud)

```
Pub/Sub topic: fivetran-events
  → Eventarc → Cloud Workflow (fivetran-dbt)
    → decode message, map connector_id → dbt job_id
    → POST to dbt-trigger Cloud Function
```

### dbt → Fabric (Webhook → Pub/Sub → Fabric API)

```
dbt Cloud job completes successfully (status_code=10)
  → POST to dbt-webhook Cloud Function
    → verify signature, parse payload
    → route by status: success → fabric topic, failure → retry topic
    → map job_id → Fabric config
    → publish to Pub/Sub topic: fabric-job-events
    → return 200 immediately

Pub/Sub topic: fabric-job-events
  → Eventarc → Cloud Workflow (fabric-job-workflow)
    → get Azure credentials from Secret Manager
    → trigger Fabric job (POST, expect 202 Accepted)
    → wait 1 hour, then check job status
    → if completed: optionally trigger Power BI refresh
    → if failed: log for manual review (dormant retry logic available)
```

### dbt Job Failure Retry (Webhook → Pub/Sub → dbt Cloud)

```
dbt Cloud job fails (status_code=20)
  → POST to dbt-webhook Cloud Function
    → verify signature, parse payload
    → detect failure status (status_code=20 or run_status="Error")
    → publish to Pub/Sub topic: dbt-retry-events
      (includes job_id, run_id, job_name, attempt_number=0)
    → return 200 immediately

Pub/Sub topic: dbt-retry-events
  → Eventarc → Cloud Workflow (dbt-retry-workflow)
    → decode message, extract retry context
    → check attempt_number < max_retries (default: 1)
    → if max retries exceeded: log DBT_JOB_RETRY_EXHAUSTED alert, stop
    → fetch run_results.json from dbt Cloud API (classify failure)
    → wait 5 minutes (base_delay_seconds)
    → POST to dbt-trigger Cloud Function with:
        job_id: original job_id
        cause: "Auto-retry (attempt N): transient failure in run {run_id}"
    → log retry success
```

**Key design decisions:**

- **Max 1 retry** — prevents runaway retry loops. If a job fails twice, it needs human attention.
- **5-minute delay** — gives transient issues (network, API rate limits) time to resolve without being so long that alerts are delayed.
- **Cause tracking** — the retry workflow passes a descriptive `cause` to dbt-trigger so retried runs are clearly identified in the dbt Cloud UI. The `cause` field also enables future loop detection (check if previous run was already a retry).
- **run_results.json fetch** — the workflow fetches artifacts to classify failures. If artifacts aren't available (setup/compile failure), the job is still retried since those are often transient.
- **Uses dbt-trigger SA** — the retry workflow runs as the dbt-trigger service account, which already has permissions to trigger dbt jobs and access the DBT_TOKEN secret.

## Anti-Patterns

### Do NOT use Cloud Tasks for delayed retries

Cloud Tasks is a separate infrastructure type that adds complexity without benefit in this architecture. The same delay-and-retry behavior is achieved with Cloud Workflows using `sys.sleep` and step branching.

**Wrong approach (Cloud Tasks):**
```
Cloud Function detects failure
  → enqueues Cloud Tasks with delay
  → Cloud Tasks calls another function after delay
  → that function polls dbt Cloud API for status
```

**Correct approach (Pub/Sub + Workflow):**
```
Cloud Function detects failure
  → publishes to Pub/Sub retry topic
  → returns 200 immediately

Pub/Sub → Eventarc → Cloud Workflow
  → Workflow sleeps for delay period
  → Workflow calls dbt Cloud API directly
  → Workflow classifies failure and decides to retry or stop
```

### Do NOT make API calls from webhook Cloud Functions

The webhook function should validate, classify, and publish — then return immediately. All API calls to external services (dbt Cloud, Fabric, Power BI) belong in Cloud Workflows.

### Do NOT poll from Cloud Functions

If you need to wait for a job to complete, use a Cloud Workflow with `sys.sleep` and status check steps. Cloud Functions should be stateless and short-lived.

See [Testing Guide](TESTING.md) for POC deployment and workflow verification steps.

## Adding a New Workflow

1. **Create the Pub/Sub topic** in Terraform (if new)
2. **Create the workflow YAML** file (see `fabric_job_workflow.yaml` or `dbt_retry_workflow.yaml` as reference)
3. **Register the workflow** in `workflow.tf` using `google_workflows_workflow`
4. **Wire Eventarc** in `event-triggers.tf` using the `eventarc_standard/workflow` module
5. **Update the Cloud Function** to publish to the new topic
6. **Grant permissions** to the workflow's service account in `permissions.tf`

## Infrastructure Reference

| Component | Location |
|-----------|----------|
| Cloud Functions | `dot/` repo (this repo), one folder per function |
| Terraform (prod) | `cru-terraform/applications/data-warehouse/dot/prod/` |
| Terraform (POC) | `dot/poc-terraform/` |
| Workflow definitions | `cru-terraform/.../dot/prod/*.yaml` + `workflow.tf` |
| Eventarc triggers | `cru-terraform/.../dot/prod/event-triggers.tf` |
| Permissions | `cru-terraform/.../dot/prod/permissions.tf` |
| Secrets | `cru-terraform/.../dot/prod/secrets.tf` |

## Pub/Sub Topics

| Topic | Publisher | Consumer Workflow | Purpose |
|-------|-----------|-------------------|---------|
| `cloud-run-job-completed` | okta-sync, woo-sync, process-geography | cloud-run-job-dbt | Trigger dbt job after CloudRun job completes |
| `fivetran-events` | fivetran-webhook | fivetran-dbt | Trigger dbt job after Fivetran sync completes |
| `fabric-job-events` | dbt-webhook (on success) | fabric-job-workflow | Trigger Fabric job after dbt job succeeds |
| `dbt-retry-events` | dbt-webhook (on failure) | dbt-retry-workflow | Retry transient dbt Cloud job failures |
