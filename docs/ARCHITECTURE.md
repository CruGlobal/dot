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

### dbt → Hightouch → MPDX (Webhook → Pub/Sub → Hightouch API → Webhook)

```
dbt Cloud job completes successfully (NetSuite jobs: 1032903 prod, 1032904 beta-prod)
  → POST to dbt-webhook Cloud Function
    → verify signature, parse payload
    → publish to Pub/Sub topic: dbt-job-completed
    → return 200 immediately

Pub/Sub topic: dbt-job-completed
  → Eventarc → Cloud Workflow (hightouch-workflow)
    → check job_id matches [1032903, 1032904] — exit if no match
    → resolve config: prod job → prod sequence, beta-prod job → stage sequence
    → fetch Hightouch API key from Secret Manager
    → trigger Hightouch sync sequence (POST to Hightouch API)
    → poll for completion with exponential backoff (30s → 300s, max 60 polls)
    → publish completion to Pub/Sub topic: hightouch-completed

Pub/Sub topic: hightouch-completed
  → Eventarc → Cloud Workflow (mpdx-webhook-workflow)
    → resolve environment (prod/stage) from payload
    → fetch webhook URL from Secret Manager
    → call MPDX webhook (GET request)
```

**Note:** The dbt-webhook CF publishes ALL successful completions to `dbt-job-completed`. The hightouch-workflow filters by job_id in its first step and exits early for non-matching jobs.

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
3. **Register the workflow** in `workflow.tf` using `google_workflows_workflow` with `templatefile()`
4. **Wire Eventarc** in `event-triggers.tf` using the `eventarc_standard/workflow` module
5. **Update the Cloud Function** to publish to the new topic
6. **Grant permissions** to the workflow's service account in `permissions.tf`

### Terraform Gotchas

**`workflow_id` must be a static string for new workflows.** The `eventarc_standard/workflow` module uses `count = length(var.workflow_id) > 0 ? 1 : 0`. If `workflow_id` references a resource that doesn't exist yet (e.g., `google_workflows_workflow.my_workflow.id`), Terraform can't resolve the count at plan time and the plan fails with `Invalid count argument`.

Use a hardcoded path string instead:
```hcl
# WRONG — fails on first plan because the workflow doesn't exist yet
workflow_id = google_workflows_workflow.my_workflow.id

# CORRECT — static string that Terraform can evaluate at plan time
workflow_id = "projects/${module.project.project_id}/locations/us-central1/workflows/my-workflow-name"
```

Once the workflow exists in state (after first apply), either form works. But since the first `atlantis apply` creates the workflow and the eventarc trigger together, the static string is required.

**Workflow YAML uses `$${...}` for Cloud Workflows expressions.** Because `templatefile()` interprets `${...}` as Terraform interpolation, all Cloud Workflows expressions in YAML files must use the double-dollar escape: `$${variable_name}`. Terraform variables passed to the template use the normal single-dollar `${var_name}`.

```yaml
# Terraform variable (resolved by templatefile):
url: "https://${region}-${project_id}.cloudfunctions.net/${function_name}"

# Cloud Workflows expression (passed through literally):
payload: $${json.decode(base64.decode(event.data.message.data))}
```

When testing workflow YAML directly via `gcloud workflows deploy` (not through Terraform), use single-dollar `${...}` — see [TESTING.md](TESTING.md) for details.

### API Gateway Gotchas

**The gateway hostname does NOT match the Terraform output pattern.** The `dbt_gateway_url` Terraform output uses the pattern `<gateway-id>-<region>.gateway.dev` (e.g., `dbt-webhook-handler-gateway-us-central1.gateway.dev`). But the actual deployed hostname includes a random suffix: `dbt-webhook-handler-gateway-6sk89xvx.uc.gateway.dev`. Always get the real hostname from:

```bash
gcloud api-gateway gateways describe dbt-webhook-handler-gateway \
  --location=us-central1 --project=cru-data-orchestration-prod \
  --format='value(defaultHostname)'
```

Using the Terraform output pattern instead of the actual hostname will return 404. This applies to any URL configured externally (dbt Cloud webhooks, documentation, manual trigger scripts).

**Current gateway hostnames:**
- dbt-webhook: `dbt-webhook-handler-gateway-6sk89xvx.uc.gateway.dev`
- fivetran-webhook: `fivetran-webhook-handler-gateway-6sk89xvx.uc.gateway.dev`

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
| `dbt-job-completed` | dbt-webhook (on success) | hightouch-workflow | Generic fan-out for all post-dbt orchestration |
| `fabric-job-events` | dbt-webhook (on success, legacy for job 163545) | fabric-job-workflow | Trigger Fabric job after US Donations dbt job succeeds |
| `hightouch-completed` | hightouch-workflow | mpdx-webhook-workflow | Trigger MPDX webhook after Hightouch sync completes |
| `dbt-retry-events` | dbt-webhook (on failure) | dbt-retry-workflow | Retry transient dbt Cloud job failures |

## Webhook Authentication

### How dbt Cloud Webhook Auth Works (and Why We Don't Validate the Bearer Token)

dbt Cloud webhooks use HMAC-SHA256 for authentication. When dbt Cloud sends a webhook, it computes `HMAC-SHA256(signing_key, request_body)` and sends the hex digest in the `Authorization` header (no `Bearer` prefix). The [dbt Cloud docs](https://docs.getdbt.com/docs/deploy/webhooks#validate-a-webhook) show this validation pattern:

```python
auth_header = request.headers.get('authorization', None)
app_secret = os.environ['MY_DBT_CLOUD_AUTH_TOKEN'].encode('utf-8')
signature = hmac.new(app_secret, request_body, hashlib.sha256).hexdigest()
return signature == auth_header
```

However, our Cloud Function sits behind a Google API Gateway (ESPv2). The gateway intercepts the `Authorization` header, replaces the original HMAC value with its own JWT (prefixed with `Bearer`), and forwards the rewritten request to the Cloud Function. The function never sees the original HMAC signature.

**The result:**
- The `Authorization` header the function receives starts with `Bearer eyJ...` (a gateway JWT)
- This is NOT the dbt Cloud signing key and NOT the HMAC signature
- The original HMAC value is gone — the gateway consumed it

**What this means for the code (`webhook_utils.py`):**
- Bearer tokens are accepted without validation because the value is the gateway JWT, not a dbt Cloud credential
- The HMAC validation path exists for direct calls that bypass the gateway (e.g., manual `curl` testing without the `Bearer` prefix)
- **DO NOT** add Bearer token validation — it will always fail and will break all downstream pipelines

**The signing key still matters:**
- The signing key configured in dbt Cloud must match `dbt-webhook_DBT_WEBHOOK_SECRET` in Secret Manager
- dbt Cloud uses this key to compute the HMAC it sends — if they don't match, dbt Cloud's own endpoint test fails
- The key is stored in [1Password](https://start.1password.com/open/i?a=JYIIWWYNKNGGFKU535Y2OR2DOE&v=dhvopdqasf4myknupv5egnktui&i=iwbonr5ku3c5x5ktn2bxuiuu2m&h=cru-data-team.1password.com)

**What protects us:**
- The API Gateway URL is not publicly discoverable
- The gateway requires proper routing from dbt Cloud's webhook infrastructure
- The HMAC path validates direct calls

### Fivetran Webhook Auth (Different Pattern)

Fivetran uses `X-Fivetran-Signature-256` instead of `Authorization`, so the API Gateway does not intercept it. The `fivetran-webhook` function validates the HMAC directly. This is not affected by the gateway rewrite issue.

## Secrets

All secrets are stored in GCP Secret Manager in project `cru-data-orchestration-prod`. Terraform creates the secret resources; values are added manually via the GCP Console or `gcloud secrets versions add`.

| Secret Name | Purpose | 1Password |
|-------------|---------|-----------|
| `dbt-webhook_DBT_WEBHOOK_SECRET` | dbt Cloud webhook signing key — must match the key configured in dbt Cloud webhooks | [1Password](https://start.1password.com/open/i?a=JYIIWWYNKNGGFKU535Y2OR2DOE&v=dhvopdqasf4myknupv5egnktui&i=iwbonr5ku3c5x5ktn2bxuiuu2m&h=cru-data-team.1password.com) |
| `hightouch-workflow_API_KEY` | Hightouch API Bearer token for triggering sync sequences | [1Password](https://start.1password.com/open/i?a=JYIIWWYNKNGGFKU535Y2OR2DOE&v=dhvopdqasf4myknupv5egnktui&i=wnn3jvjm5cjblksxog3xjhlb5q&h=cru-data-team.1password.com) |
| `mpdx-webhook_URL_PROD` | MPDX production webhook URL (called after Hightouch sync completes) | [1Password](https://start.1password.com/open/i?a=JYIIWWYNKNGGFKU535Y2OR2DOE&v=dhvopdqasf4myknupv5egnktui&i=lav6dc7lszmaccvmfhd22otqpq&h=cru-data-team.1password.com) |
| `mpdx-webhook_URL_STAGE` | MPDX stage webhook URL | [1Password](https://start.1password.com/open/i?a=JYIIWWYNKNGGFKU535Y2OR2DOE&v=dhvopdqasf4myknupv5egnktui&i=5fujvlyilt6ucudxdc2qvohvoa&h=cru-data-team.1password.com) |
| `fabric-workflow_AZURE_CLIENT_ID` | Azure service principal client ID for Fabric API | Secret Manager only |
| `fabric-workflow_AZURE_CLIENT_SECRET` | Azure service principal client secret for Fabric API | Secret Manager only |
| `fabric-workflow_AZURE_TENANT_ID` | Azure tenant ID for Fabric API | Secret Manager only |

**Important:** When rotating a secret, add a new version in Secret Manager, then **redeploy the Cloud Function** (push any change to the function's folder on `main`) to pick up the new value. Running instances cache the secret at startup.

## Manual Trigger Runbook

Sometimes you need to trigger a downstream workflow without running the full upstream dbt job (e.g., recovering from an outage, testing after secret rotation).

### Trigger via dbt-webhook (simulates a dbt Cloud completion)

This sends a POST to the dbt-webhook Cloud Function as if dbt Cloud sent it. The function validates the signing key, publishes to the appropriate Pub/Sub topics, and all downstream workflows fire normally.

**Prerequisites:**
- The `DBT_WEBHOOK_SECRET` from 1Password or Secret Manager
- The webhook endpoint URL: `https://dbt-webhook-handler-gateway-6sk89xvx.uc.gateway.dev/dbt-webhook`

**Trigger Fabric (US Donations, job 163545):**
```bash
curl -s -X POST "https://dbt-webhook-handler-gateway-6sk89xvx.uc.gateway.dev/dbt-webhook" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <DBT_WEBHOOK_SECRET>" \
  -d '{
    "eventType": "job.run.completed",
    "accountId": "10206",
    "data": {
      "jobId": "163545",
      "jobName": "US Donations",
      "runId": "0",
      "runStatus": "Success",
      "runStatusCode": 10,
      "runStatusMessage": "Success",
      "environmentId": "0"
    }
  }'
```

**Trigger Hightouch → MPDX (NetSuite prod, job 1032903):**
```bash
curl -s -X POST "https://dbt-webhook-handler-gateway-6sk89xvx.uc.gateway.dev/dbt-webhook" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <DBT_WEBHOOK_SECRET>" \
  -d '{
    "eventType": "job.run.completed",
    "accountId": "10206",
    "data": {
      "jobId": "1032903",
      "jobName": "NetSuite for MPDX",
      "runId": "0",
      "runStatus": "Success",
      "runStatusCode": 10,
      "runStatusMessage": "Success",
      "environmentId": "0"
    }
  }'
```

**Trigger Hightouch → MPDX (NetSuite beta-prod, job 1032904):**
```bash
# Same as above but with jobId "1032904" — triggers stage Hightouch sequence
```

Replace `<DBT_WEBHOOK_SECRET>` with the signing key from 1Password.

### After Secret Rotation

When you update a secret in Secret Manager, the running Cloud Function instances still hold the old value in memory. To pick up the new secret:

1. Push any change to the function's folder on `main` in the dot repo (triggers GHA auto-deploy)
2. Or ask someone with `run.services.update` permission to restart the Cloud Run service
3. Or wait for all instances to scale to zero (happens after a period of no traffic)

### Verifying the Trigger Worked

1. **Cloud Logging** in `cru-data-orchestration-prod` — filter: `resource.type="cloud_run_revision" resource.labels.service_name="dbt-webhook"`. Look for `200` responses.
2. **Cloud Workflows** — check the relevant workflow's Executions tab for a new execution.
3. A `200` response from the curl means the webhook was accepted and published to Pub/Sub. Downstream workflow execution is asynchronous.
