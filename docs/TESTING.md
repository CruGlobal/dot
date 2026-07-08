# DOT Testing Guide

## Environments

| Environment | GCP Project | Deploy Trigger | Terraform |
|-------------|-------------|----------------|-----------|
| POC | `cru-data-orchestration-poc` | Push to `poc` branch | Local (`poc-terraform/`, local state) |
| Stage | `cru-data-orchestration-stage` | Push to `staging` branch | Atlantis (`cru-terraform`) |
| Prod | `cru-data-orchestration-prod` | Push to `main` branch | Atlantis (`cru-terraform`) |

### POC Environment

POC is a sandbox for testing Cloud Function code changes. It has:
- **dbt-trigger** — deployed via terraform (`poc-terraform/`)
- **dbt-webhook** — deployed via GitHub Actions on push to `poc` branch
- **woo-sync** — Cloud Run job

POC does **not** have the full event pipeline (Pub/Sub topics, Eventarc triggers, Cloud Workflows). Those only exist in prod and are managed by `cru-terraform`. To test workflows in POC, create temporary resources via gcloud (see below).

## Unit Tests

Each Cloud Function has its own test suite.

```bash
# dbt-trigger
cd dbt-trigger && pip install -r requirements.txt -r requirements-test.txt && pytest

# dbt-webhook
cd dbt-webhook && pip install -r requirements.txt -r requirements-test.txt && pytest
```

## Testing Cloud Workflows in POC

Cloud Workflows can be deployed and tested directly via gcloud. This is useful for validating workflow YAML before deploying to prod via Terraform.

### Prerequisites

- `gcloud` CLI authenticated with access to `cru-data-orchestration-poc`
- A service account in POC (e.g., `dbt-webhook-sa@cru-data-orchestration-poc.iam.gserviceaccount.com`)

### Deploy a Workflow

```bash
gcloud workflows deploy <workflow-name> \
  --project=cru-data-orchestration-poc \
  --location=us-central1 \
  --description='TEST: <description>' \
  --service-account=<sa-email> \
  --source=<path-to-yaml>
```

**Important**: Production workflow YAML files use `$${...}` for Terraform escaping. When deploying directly via gcloud (not through Terraform), use `${...}` instead.

For YAML values containing string expressions with double quotes, wrap the entire value in single quotes:
```yaml
# Won't parse:
cause: ${"Auto-retry (attempt " + string(n) + ")"}

# Correct:
cause: '${"Auto-retry (attempt " + string(n) + ")"}'
```

### Run a Workflow with Test Data

Workflows expect Pub/Sub event format. Create a base64-encoded JSON payload:

```bash
# Create the message payload
echo -n '{"job_id":"12345","run_id":"67890","job_name":"test-job","run_status":"Error","attempt_number":0}' | base64 -w0
# Output: eyJqb2Jf...

# Run the workflow
gcloud workflows run <workflow-name> \
  --project=cru-data-orchestration-poc \
  --location=us-central1 \
  --data='{"data":{"message":{"data":"<base64-encoded-payload>"}}}'
```

### Simplified Test Workflows

For testing workflow logic without external dependencies (secrets, API calls), create a simplified version that:
- Keeps message decoding and field extraction
- Keeps conditional branching logic
- Replaces external API calls with log statements
- Skips `sys.sleep` delays

This validates the workflow's core logic without needing secrets or endpoints deployed in POC.

### Full Integration Test Workflows

For testing the complete path including Secret Manager and external APIs:

1. **Create the secret** in POC:
   ```bash
   gcloud secrets create <secret-name> --project=cru-data-orchestration-poc --replication-policy=automatic
   echo -n "<secret-value>" | gcloud secrets versions add <secret-name> --project=cru-data-orchestration-poc --data-file=-
   ```

2. **Grant access** to the workflow's service account:
   ```bash
   gcloud secrets add-iam-policy-binding <secret-name> \
     --project=cru-data-orchestration-poc \
     --member="serviceAccount:<sa-email>" \
     --role="roles/secretmanager.secretAccessor"
   ```

3. **Deploy the workflow** with real Secret Manager paths pointing to POC project

4. **Run with real data** (e.g., a real failed dbt Cloud run_id)

5. **Check logs**:
   ```bash
   gcloud logging read "resource.type=workflows.googleapis.com/Workflow AND resource.labels.workflow_id=<workflow-name>" \
     --project=cru-data-orchestration-poc \
     --limit=10 \
     --format="table(timestamp,jsonPayload.message,severity)" \
     --freshness=10m
   ```

### Cleanup

Always delete test resources after testing:

```bash
gcloud workflows delete <workflow-name> --project=cru-data-orchestration-poc --location=us-central1 --quiet
gcloud pubsub topics delete <topic-name> --project=cru-data-orchestration-poc --quiet
gcloud pubsub subscriptions delete <sub-name> --project=cru-data-orchestration-poc --quiet
gcloud secrets delete <secret-name> --project=cru-data-orchestration-poc --quiet
```

## Testing dbt-webhook Locally

The webhook function can be tested locally with Flask's test client:

```bash
cd dbt-webhook
pip install -r requirements.txt -r requirements-test.txt
pytest -v
```

The test suite covers:
- Success routing (publishes to `fabric-job-events` topic)
- Failure routing (publishes to `dbt-retry-events` topic)
- Cancelled/non-completion events (ignored)
- Signature verification
- Invalid payloads

## Deploying Code to POC

Push to the `poc` branch to trigger GitHub Actions deployment:

```bash
# Cherry-pick specific commits
git checkout poc
git cherry-pick <commit-hash>
git push origin poc

# Or merge a feature branch
git checkout poc
git merge <feature-branch>
git push origin poc
```

**Note**: The dbt-trigger GHA deployment may fail in POC because the function was originally created by terraform with a schedule trigger, not HTTP trigger. The webhook deployment works correctly.

## Fivetran slot valve (DT-561)

The slot valve has two halves:

- **Cloud Function** (`fivetran-slot-valve/main.py`) — covered by **unit tests** (`fivetran-slot-valve/main_test.py`, run with `pytest`). This is the fast, CI-able layer.
- **Cloud Workflow** (`fivetran_slot_valve_workflow.yaml`, in cru-terraform) — the drain state machine. There is **no offline Cloud Workflows runtime**, so it can only be validated by deploying it. The repeatable integration test is the script `fivetran-slot-valve/poc_test.sh`.

### Safe test target

Use the **`grip_oblivious`** connector (`el_fivetran_logs_stage` — the *stage* Fivetran-logs connector). Only DSE consumes it, so force-syncing / pausing / resuming it during tests has **no external blast radius**. (Recorded in the orchestration program doc as the designated sync-side test target.)

### Decision table (what each branch should do)

The workflow GETs the connector and branches on its state:

| Connector state | `setup_state` / `sync_state` / `paused` | Expected workflow result |
|---|---|---|
| Not connected (broken/incomplete) | `setup_state != "connected"` | `connector_broken` — stops, emits `alert_type: FIVETRAN_SLOT_VALVE_CONNECTOR_BROKEN` (ERROR) for DevOps |
| Already syncing | `sync_state == "syncing"` | `already_syncing` — no-op (no stacked sync) |
| Paused | `paused == true` (and connected) | resume (`paused:false`) then `sync_forced` |
| Healthy | connected, idle | `sync_forced` |

### Run it

```bash
cd fivetran-slot-valve

# While the workflow still lives on the cru-terraform mechanism feature branch,
# point the script at that worktree (after merge, the default master path works):
export WORKFLOW_YAML=$HOME/gitRepos/cru-terraform-worktrees/pmh_06-22-2026_dot_fivetran_slot_valve_mechanism/applications/data-warehouse/dot/prod/fivetran_slot_valve_workflow.yaml

./poc_test.sh setup     # copy Fivetran creds prod->POC, render + deploy the workflow
./poc_test.sh healthy   # connected+idle  -> expect "sync_forced"
./poc_test.sh syncing   # mid-sync        -> expect "already_syncing"
./poc_test.sh paused    # paused          -> expect resume + "sync_forced"
./poc_test.sh cleanup   # unpause connector, delete POC workflow + secrets
```

The `not-connected` branch can't be induced safely on a healthy connector, so it is covered by the unit-level logic + review rather than a live POC run.

Check logs with the `gcloud logging read` command above, filtering `resource.labels.workflow_id=fivetran-slot-valve`. **Always run `cleanup` when done** — it deletes the POC workflow + the copied secrets and leaves `grip_oblivious` unpaused.
