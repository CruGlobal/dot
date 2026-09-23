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
    → decode message; skip unless sync status == SUCCESSFUL   (DT-511)
    → map connector_id → dbt job_id
    → per-job build-window gate: skip if this window already has a gate-started build   (DT-736)
    → POST to dbt-trigger Cloud Function
```

### dbt → Fabric (Webhook → Pub/Sub → Fabric API)

```
dbt Cloud job completes successfully (status_code=10)
  → POST to dbt-webhook Cloud Function
    → verify signature, parse payload
    → route by status: success → dbt-job-completed (all successes), failure → dbt-retry-events
    → also publish to fabric-job-events if the job has a Fabric mapping (legacy)
    → return 200 immediately

Pub/Sub topic: fabric-job-events
  → Eventarc → Cloud Workflow (fabric-job-workflow)
    → get Azure credentials from Secret Manager
    → trigger Fabric job (POST .../items/{item_id}/jobs/instances?jobType=..., expect 202 Accepted)
      · jobType=RunNotebook sends the mapping's execution_data as the executionData body
      · jobType=Execute (CopyJob) sends an empty JSON object (`{}`)
    → wait 1 hour, then check job status
    → if completed: trigger Power BI refresh only when refresh_workspace_id + lakehouse_dataset_id are set
    → if failed: log for manual review (dormant retry logic available)
```

### dbt Job Failure Retry (Webhook → Pub/Sub → Workflow → dbt-classify → dbt Cloud)

```
dbt Cloud job fails (status_code=20)
  → POST to dbt-webhook Cloud Function
    → verify signature, parse payload
    → detect failure status (status_code=20 or run_status="Error")
    → publish to Pub/Sub topic: dbt-retry-events (job_id, run_id, job_name, account_id)
    → return 200 immediately

Pub/Sub topic: dbt-retry-events
  → Eventarc → Cloud Workflow (dbt-retry-workflow) -- stays thin
    → POST to the dbt-classify Cloud Function (OIDC) with {account_id, run_id}
        dbt-classify (Python) fetches the run metadata (include_related=
        ["trigger","run_steps"]) + run_results.json, applies the loop guard +
        transient allowlist, and returns a small verdict:
          { reason, is_retryable, prior_is_retry, failed_count, run_created_at, … }
    → branch on the verdict:
        · prior_is_retry              → DBT_JOB_RETRY_EXHAUSTED, stop
        · reason metadata_unavailable → DBT_JOB_RETRY_GUARD_UNCERTAIN, stop (fail-closed)
        · not is_retryable            → DBT_JOB_NOT_RETRYABLE, stop
        · is_retryable                → wait 5 min (base_delay_seconds)
                                       → dedup: skip (superseded) if a newer run exists
                                       → POST to dbt-trigger with
                                         cause: "Auto-retry for transient failure in run {run_id}"
```

**Why classification is a function, not all in the workflow:** a Cloud Workflow cannot hold a large `run_results.json` in a variable — it exceeds the Workflows per-variable memory limit (a 354-node job failed exactly this way). Parsing it in Python (`dbt-classify`) sidesteps that, and makes the classification logic unit-testable rather than untestable YAML. This is the one place a DOT function deliberately makes external API calls: the "no API calls from functions" rule exists to keep *webhooks* fast to return; `dbt-classify` is invoked by a workflow (not a webhook) and must read the artifact, so the rule does not apply.

**Key design decisions:**

- **Transient-only, default-deny classification** (in `dbt-classify`) — retries only infrastructure/transient errors (e.g. the BigQuery `409 Already Exists: Job` collision, rate/quota limits, 5xx, deadline/connection errors) identified from `run_results.json`. Test failures, missing tables/columns, broken joins, and other invalid SQL are never retried. Anything that cannot be positively classified as transient is left for a human.
- **Max 1 retry, enforced via `cause`** — `dbt-classify` reads the failed run's `trigger.cause` (`include_related=["trigger"]`): a run whose cause already starts with "Auto-retry" returns `prior_is_retry` and is not retried again. There is no `attempt_number` counter. The `fivetran-dbt` build-window gate also matches this exact text (`Auto-retry for transient failure in run <id>`) to count a retry of its own run toward a window, so don't reword it.
- **Fail-closed** — if `dbt-classify` cannot read the run metadata it returns `metadata_unavailable`; the workflow then cannot confirm the run was not already a retry, so it does not retry. A missed retry of a genuine transient is cheap (re-run by hand); an uncapped retry loop is not.
- **Multi-step cross-check** — `run_steps` is read to detect step-level errors; if a step errored but `run_results.json` explains no failed node, the failure is command-level/uncovered and is not retried.
- **Dedup stays in the workflow** — the small list-runs check (skip as superseded if a newer run for the job already exists) is cheap and remains in the workflow; only the large-artifact read+classify moved to the function.
- **Classification is unit-tested** — the logic lives in `dbt-classify/classifier.py` with a unit-test suite (`main_test.py`), covering each rule (transient match, test-failure, unknown error, mixed, no-results, uncovered-step, loop guard, fail-closed). It used to be untestable Cloud Workflows YAML.
- **5-minute delay** — gives transient issues (network, API rate limits) time to clear without delaying escalation much.
- **Alerting** — `DBT_JOB_NOT_RETRYABLE`, `DBT_JOB_RETRY_EXHAUSTED`, and `DBT_JOB_RETRY_GUARD_UNCERTAIN` are emitted as ERROR-severity logs and surfaced by a Datadog monitor, so failures the workflow will not auto-fix escalate to a human.
- **Service account + logging** — the workflow runs as the dbt-trigger service account, which has dbt-job trigger and DBT_TOKEN secret access **and** `roles/logging.logWriter`. The logWriter grant is required because a Cloud Workflow's `sys.log` calls the Logging API as the workflow's own service account; without it the workflow fails on its first log step.

### dbt → Hightouch → MPDX (Webhook → Pub/Sub → Hightouch API → Webhook)

```
dbt Cloud job completes successfully (NetSuite jobs: 1032903 prod, 1032904 beta-prod)
  → POST to dbt-webhook Cloud Function
    → verify signature, parse payload
    → publish to Pub/Sub topic: dbt-job-completed
    → return 200 immediately

Pub/Sub topic: dbt-job-completed
  → Eventarc → Cloud Workflow (hightouch-workflow)
    → look up job_id in the dbt_job_to_hightouch config map — exit if not present
    → resolve {chain, sequence_id, webhook_secret_name} from the map entry
    → fetch Hightouch API key from Secret Manager
    → trigger Hightouch sync sequence (POST to Hightouch API)
    → poll for completion with exponential backoff (30s → 300s, max 60 polls)
    → publish completion to Pub/Sub topic: hightouch-completed (carries chain + webhook_secret_name)

Pub/Sub topic: hightouch-completed
  → Eventarc → Cloud Workflow (webhook-notify-workflow)
    → read webhook_secret_name from the payload
    → fetch that webhook URL from Secret Manager
    → call the downstream webhook (GET request)
```

**Note:** The dbt-webhook CF publishes ALL successful completions to `dbt-job-completed`. The hightouch-workflow looks the job_id up in its `dbt_job_to_hightouch` config map (a `local` in `cru-terraform/.../dot/prod/workflow.tf`, injected via `jsonencode`) and exits for jobs not in the map. **Adding a new "dbt job → Hightouch → webhook" chain is a config change, not workflow code:** add one map entry (`{ chain, sequence_id, webhook_secret_name }`) — reusing an existing webhook secret needs nothing more; a new webhook target also needs its Secret Manager secret + an accessor grant for the webhook-notify SA. The `webhook-notify-workflow` calls whatever secret the payload's `webhook_secret_name` names, so it is generic across chains (not MPDX-specific). Logs and the completion payload carry a `chain` label (the pipeline name), which replaced the earlier `environment` field — ambiguous once a chain's dbt environment (e.g. beta-prod) differed from its downstream target (stage).

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

**Workflow YAML uses `$${...}` for Cloud Workflows expressions.** Terraform interprets `${...}` as interpolation both in `templatefile()` YAML files and in inline `<<EOF` heredocs (the `fivetran-dbt` source is one), so Cloud Workflows expressions there must use the double-dollar escape: `$${variable_name}`. Terraform variables use the normal single-dollar `${var_name}`. The one exception is a file inlined with `file()`, which Terraform does not template: `fivetran_dbt_window_start.yaml` (the `window_start` subworkflow, inlined into `fivetran-dbt`) uses plain `${...}`. Copying expressions between the two kinds of file breaks them silently.

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

## Adding a New Fivetran-Triggered dbt Job

Use this runbook when you want a Fivetran sync completion to trigger a dbt Cloud job. The `fivetran-dbt` workflow already exists — you don't create new workflows or Pub/Sub topics; you just wire a new connector → job mapping and (if needed) a new Cloud Scheduler entry.

### Architecture (what already exists)

```
Cloud Scheduler (in functions.tf)
  → fivetran-trigger CF → starts Fivetran sync
  → Fivetran completes → fivetran-webhook CF → fivetran-events topic
  → fivetran-dbt workflow → success filter + per-job build-window gate (DT-736)
  → looks up connector_id in connector_to_dbt_mapping
  → dbt-trigger CF → runs dbt Cloud job
```

### Steps

1. **Define the dbt Cloud job** in [`dse-dbt-jobs-as-code/jobs.yml`](https://github.com/CruGlobal/dse-dbt-jobs-as-code).
   - Use `<<: [*<env_anchor>, *triggered_job_defaults]` to inherit settings — `triggered_job_defaults` sets `triggers.schedule: false`, no completion trigger, `job_type: other`.
   - No `schedule` block, no `job_completion_trigger_condition` — the workflow fires the job, not dbt Cloud itself.
   - Keep `description` under 255 chars (dbt-jobs-as-code schema limit).
   - Merge the PR. The GHA `sync` step creates the job in dbt Cloud and assigns it a real job ID.

2. **Look up the new job ID** in dbt Cloud (Deploy → Jobs → find by name → URL contains `/jobs/<ID>/`).

3. **Set the Fivetran connector to manual schedule** when DOT schedules its syncs (step 4). Without this, both Fivetran's native scheduler and Cloud Scheduler fire syncs, causing double dbt runs. A connector left on its native Fivetran schedule skips steps 3 and 4 and relies on a build window (step 6) to limit builds; `el_ert` (`crossing_accidental`) works this way until DT-680 moves it to DOT-scheduled syncs.

   ```bash
   # Direct API call — the ~/bin/fivetran wrapper supports pause/resume but NOT schedule_type
   curl -s -u "${FIVETRAN_API_KEY}:${FIVETRAN_API_SECRET}" \
     -H "Content-Type: application/json" \
     -X PATCH \
     -d '{"schedule_type": "manual"}' \
     "https://api.fivetran.com/v1/connectors/<connector_id>"
   ```

   Note: `schedule_type: "manual"` ≠ `paused: true`. `paused: true` blocks API-triggered syncs too — wrong for this pattern. `manual` only disables the native schedule.

4. **Add a Cloud Scheduler entry** in `cru-terraform/applications/data-warehouse/dot/prod/functions.tf` inside `module "fivetran_trigger".schedule`:

   ```hcl
   el_<schema>_<env> = {
     # Runs Daily <time + zone>
     cron = "<UTC cron expression>"
     argument = {
       "connector_id" = "<connector_id>"
     }
   },
   ```

   This is what tells `fivetran-trigger` CF when to start the sync.

5. **Add the connector → job mapping** in `cru-terraform/applications/data-warehouse/dot/prod/workflow.tf` inside the `connector_to_dbt_mapping` block:

   ```yaml
   <connector_id>: ["<dbt_job_id>"]            # el_<schema>_<env> → <dbt_job_name>
   ```

   The value is a list — a single connector can fan out to multiple dbt jobs (see `supervision_narrowly` for an example).

6. **(Optional) Limit when dbt builds** with a build window in `dbt_job_build_windows` in the same `workflow.tf`. List the job's **anchors**: UTC hours, optionally limited to weekdays (0 = Sunday … 6 = Saturday). A window runs from one anchor to the next, and the job builds once per window, on the first successful sync that finishes at or after the anchor. **Quote the job-id key**: an unquoted number parses as an int, never matches, and silently leaves the job building on every sync.

   ```yaml
   # Once a day, after the 05:00 UTC sync:
   "<dbt_job_id>": [{hour: 5}]
   # Or, twice a day: after 11:00 UTC daily and after 17:00 UTC on weekdays:
   "<dbt_job_id>": [{hour: 11}, {hour: 17, weekdays: [1, 2, 3, 4, 5]}]
   ```

   Use this when the connector syncs more often than you want dbt to build: two Oracle syncs a day for redo-log retention, an hourly native schedule, or a DT-561 valve force-sync. Omit the job to build on every successful sync (the default).

   - **Each anchor must be an hour a sync starts** (the Cloud Scheduler cron from step 4, or the connector's own Fivetran schedule), and no sync may start before an anchor and finish after it, or that sync takes the new window's build. If you later move the cron, move the anchor with it.
   - **For one build a day off a multi-sync connector, anchor on the sync whose data people need.** Rows from the other sync wait for the next window (with syncs at 06:00 and 18:00 and an anchor of 6, the 18:00 rows are built after the next day's 06:00 sync).
   - **A wrong but valid hour does not page.** With syncs at 06:00 and 18:00, an anchor of 7 makes the job build after the 18:00 sync instead, and if the 06:00 sync sometimes runs past 07:00 the build moves between slots day to day. `DBT_TRIGGER_GATE_MISCONFIG` only catches malformed anchors.
   - **After apply, confirm the first sync writes a `Build-window gate` log line for the job** (see the on-call notes below). No line means the key didn't match.

7. **PR, Atlantis plan, apply.** Expected plan: 1 add (the Cloud Scheduler; 0 if the connector already has one) + 1 in-place update (the workflow's `source_contents`). If you see destroys, stop and investigate — your branch is probably behind master.

### Trigger gate: success filter + build windows (DT-511, DT-736)

The `fivetran-dbt` workflow triggers a dbt job **only when the Fivetran sync succeeded**: it reads `data.status` from the `sync_end` event and proceeds only on `SUCCESSFUL` (a missing or malformed status fails safe to no trigger).

A job listed in `dbt_job_build_windows` (step 6 above) builds **once per window**. On each successful `sync_end`, the gate finds the window the event falls in (the most recent active anchor at or before the event's `created` time, computed by the `window_start` subworkflow in `fivetran_dbt_window_start.yaml`), lists the job's last 10 dbt Cloud runs, and:

- **counts only runs the gate started**, marked by the cause prefix `fivetran-dbt window: `, plus DT-568 auto-retries of those runs (`Auto-retry for transient failure in run <id>`). Manual runs and anything else are ignored, so a manual rerun never takes a window's build or moves the build to another sync slot;
- **skips** if a counted run in this window succeeded or is still running;
- **stops re-triggering** after 2 failed gate-started builds in the window (an hourly connector would otherwise retry a broken build every hour);
- otherwise **triggers**, stamping the gate's cause so the run counts.

**Events** (`events_prod`, 1013020) is built this way, off `el_ert`'s native hourly sync, with anchors `[{hour: 11}, {hour: 17, weekdays: [1, 2, 3, 4, 5]}]`; it has no `dbt_trigger` cron.

**Every gate error fails open** (the job triggers), in one of two ways:

- **The build counts toward the window** (the gate cause is stamped): the runs list can't be fetched or evaluated, runs come back without trigger data (`cause_filter_blind`), or the 10 runs don't reach back to the window start (`run_history_page_too_short`).
- **The build does not count** (the default cause is used), so each later sync in the window builds again: the dbt token is unavailable (the gate is off for that execution), the window can't be computed, the anchors are invalid (`DBT_TRIGGER_GATE_MISCONFIG`), or the cause can't be built (this last case logs nothing).

A persistent fail-open alerts through the Datadog monitor "DSE dbt trigger gate failing open (build-window gate not evaluating)"; invalid anchors through "DSE dbt trigger gate misconfigured (invalid build-window anchors)". (Not fully closed: two Pub/Sub deliveries seconds apart, before the first run shows in the dbt API, can both trigger.)

**On call: "why did or didn't a job build?"**

- Read the `fivetran-dbt` workflow logs in `cru-data-orchestration-prod`, filtered by the job id (e.g. `163545` for `us_donations_prod`):
  ```bash
  gcloud logging read 'resource.type="workflows.googleapis.com/Workflow" AND resource.labels.workflow_id="fivetran-dbt" AND jsonPayload.job_id="163545"' \
    --project=cru-data-orchestration-prod --limit=10 --format="value(timestamp,jsonPayload.decision,jsonPayload.reason,jsonPayload.alert_type,jsonPayload.deciding_run_id)"
  ```
- Normal lines carry `decision` (`build` / `skip`) and `reason`: `window_not_built`, `window_build_failed_retrying`, `window_already_built`, `window_attempts_exhausted`. Fail-open lines carry `alert_type: DBT_TRIGGER_GATE_FAILOPEN` or `DBT_TRIGGER_GATE_MISCONFIG` instead of `decision`; the token-unavailable line has no `job_id`.
- **No gate line at all** for a sync means the gate never evaluated the job: the sync wasn't `SUCCESSFUL` (visible only in the execution's return output), the connector isn't in `connector_to_dbt_mapping`, or the job-id key in `dbt_job_build_windows` didn't match.
- **`window_already_built` because of a stuck run:** `deciding_run_id` names it. Cancel it in dbt Cloud and the next sync in the window builds.
- A manual rerun needs no timing: it can't knock out the next scheduled build. After a **failed** window build, a manual fix does not count, so the next sync in that window builds again, along with anything that hangs off the job (for `us_donations_prod`, the extract jobs and the Fabric chain).

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
| `fabric-job-events` | dbt-webhook (on success, legacy for job 163545) | fabric-job-workflow | Run the Fabric Notebook (jobType `RunNotebook`) after the US Donations dbt job succeeds |
| `hightouch-completed` | hightouch-workflow | webhook-notify-workflow | Call the downstream webhook (named by the payload's `webhook_secret_name`) after a Hightouch sync completes |
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

**Trigger Fabric Notebook (US Donations, job 163545):**

> **This runs the production notebook against prod data. It is not a smoke test.** The curl returns
> 200 in milliseconds; the workflow then starts notebook `84bf60cb-…` in prod workspace `c2bafcfd-…`
> (a full reload of every table, ~35-40 min) and reports the outcome about 1 hour later. Check it with
> `gcloud workflows executions list fabric-job-workflow --location=us-central1 --project=cru-data-orchestration-prod`.
> There is no way to test the Azure token without also starting the notebook. Publishing with
> `enable_monitoring: false` in the payload still runs the notebook; it only skips the 1-hour status check.

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
