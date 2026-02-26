# DOT - Data Orchestration Triggers

Cloud Functions that orchestrate data pipeline operations (dbt runs, Fivetran triggers, Google Sheets sync, data processing).

## Environments & Deployment

| Environment | Branch | GCP Project ID |
|-------------|--------|----------------|
| **Staging** | `staging` | `cru-data-orchestration-stage` |
| **Production** | `main` | `cru-data-orchestration-prod` |
| **POC** | `poc` | `cru-data-orchestration-poc` |

- **Staging** is the testing/beta environment. Use it for all pre-production testing.
- **Production** is the live environment. Deploy by merging PRs to `main`.
- **POC** is an experimental sandbox requiring local Terraform. Not needed for regular testing.

To test a feature, merge your branch into `staging` and push. GitHub Actions deploys automatically.
Then use Cloud Scheduler "Force Run" in the GCP Console to trigger the function manually.

## Two-Phase Deployment

Deployment uses two repos:
1. **cru-terraform repo** -- Creates the Cloud Function (with placeholder code), Cloud Scheduler,
   service account, secrets, and IAM bindings. Must be applied FIRST.
   - Staging: `cru-terraform/applications/data-warehouse/dot/stage/functions.tf`
   - Production: `cru-terraform/applications/data-warehouse/dot/prod/functions.tf`
2. **dot repo (this repo)** -- GitHub Actions deploys the real code to the existing function.

If GHA fails with "trigger required", it means Terraform hasn't been applied yet for that
function in the target environment. The function must exist in GCP before GHA can update it.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the push-not-poll pattern, workflow diagrams, and anti-patterns.

**Key principle**: Cloud Functions validate and publish to Pub/Sub, then return immediately. All orchestration (API calls, delays, retries, status polling) happens in Cloud Workflows. Do NOT poll from Cloud Functions or use Cloud Tasks.

## Functions

| Directory | Purpose |
|-----------|---------|
| `dbt-trigger/` | Triggers dbt Cloud jobs via API. Accepts optional `cause` for tracking. |
| `dbt-webhook/` | Receives dbt Cloud webhooks. Routes success → Fabric, failure → retry. |
| `fivetran-trigger/` | Triggers Fivetran sync jobs |
| `fivetran-webhook/` | Receives Fivetran webhook callbacks |
| `gsheets-trigger/` | Syncs data from Google Sheets |
| `process-geography/` | Processes geography data in BigQuery |
| `woo-sync/` | Syncs WooCommerce data |

## Google Drive API

- The Drive API must be enabled on the GCP project you're authenticating against.
- Files in shared drives require `supportsAllDrives=True` in API calls.
- To test locally against real Google Sheets, the sheets must be shared with your authenticated account (or the service account).

## Testing

See [docs/TESTING.md](docs/TESTING.md) for the full testing guide including POC workflow testing.

### Unit tests (no GCP credentials needed)
```bash
cd <function-dir>
pip install -r requirements.txt -r requirements-test.txt
pytest -v
```

### Integration test against real Google Sheets
```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
cd gsheets-trigger
python -c "from sheets_client import SheetsClient; c = SheetsClient(); print(c.get_modified_time('SHEET_ID'))"
```

## Infrastructure (Terraform)

Cloud Function infrastructure is managed in `cru-terraform`, not this repo:
- **Prod**: `cru-terraform/applications/data-warehouse/dot/prod/`
- **Stage**: `cru-terraform/applications/data-warehouse/dot/stage/`
- **POC**: `poc-terraform/` (this repo, local state)

Key files in cru-terraform:
- `functions.tf` — Cloud Function definitions, schedules, secrets
- `workflow.tf` — Cloud Workflow definitions
- `event-triggers.tf` — Eventarc triggers (Pub/Sub → Workflow)
- `permissions.tf` — IAM bindings for service accounts
- `*.yaml` — Workflow YAML files (referenced by `workflow.tf` via `templatefile()`)
