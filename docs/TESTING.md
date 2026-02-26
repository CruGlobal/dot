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

## Test Results: dbt-retry-workflow (2026-02-26)

### Simplified Logic Test
- Message decoding: PASS
- Field extraction (job_id, run_id, attempt_number, etc.): PASS
- Retry limit branching (attempt < max → retry, attempt >= max → stop): PASS
- Cause string construction: PASS

### Full Integration Test (real secrets + real dbt Cloud API)
- Secret Manager access (`dbt-trigger_DBT_TOKEN`): PASS
- Token decode + BOM strip: PASS
- dbt Cloud API call (fetch run_results.json for run 465417728): PASS
- Failure classification (found 1/28 failed nodes): PASS
- Retry decision logic: PASS
- Max retries exceeded path: PASS (tested separately)
