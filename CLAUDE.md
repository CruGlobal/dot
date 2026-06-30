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

### Phase 2 is per-function — each function needs its OWN deploy workflow

GitHub Actions does NOT deploy functions generically. Each function deploys via a dedicated
workflow file: `.github/workflows/build-deploy-cloudrun-<function-name>.yml`. It is path-filtered
to `<function-name>/**`, so merging to `main` (prod) / `staging` (stage) / `poc` redeploys only
the functions whose source changed; it also exposes `workflow_dispatch` for a manual run.

**If you add a new function and forget this file, the code NEVER deploys** — merging to `main`
runs only the test suite, the function keeps serving the Terraform placeholder, and nothing errors
or warns. (This is exactly how `dbt-classify` reached `main` un-deployed.)

Each workflow is a thin caller of the shared reusable workflow
`CruGlobal/.github/.github/workflows/build-deploy-cloudrun-function.yml@v1`. Copy an existing one
(e.g. `build-deploy-cloudrun-dbt-trigger.yml`) and change `name`, `function_name`, `entry_point`
(the `@functions_framework.http` function in `main.py`), `runtime`, and the `paths:` filter.

That reusable workflow deploys **source-only**: `gcloud functions deploy --source --entry-point
--runtime --build-service-account`. It does NOT pass `--set-secrets`, `--run-service-account`,
`--timeout`, or `--memory`, so on an existing gen2 function that is a partial update and the
Terraform-set secret bindings, runtime SA, timeout, and memory are PRESERVED across deploys. Set
those in Terraform (phase 1), never here.

### Checklist: adding a new Cloud Function

1. `<function-name>/` folder — `main.py` (with an `@functions_framework.http` entry point),
   `requirements.txt`, `requirements-test.txt`, and tests.
2. Terraform module in cru-terraform `functions.tf` (+ secrets, SA, IAM, and any workflow/eventarc
   wiring), applied FIRST. The Terraform `name` MUST equal the folder name.
3. **`.github/workflows/build-deploy-cloudrun-<function-name>.yml`** — copy a sibling and set
   `name`, `function_name`, `entry_point`, `runtime`, and the `paths:` filter. Without this the
   function never deploys.
4. Add the function to the `## Functions` table below.
5. Populate any secret values (see README "Secrets").

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the push-not-poll pattern, workflow diagrams, and anti-patterns.

**Key principle**: Cloud Functions validate and publish to Pub/Sub, then return immediately. All orchestration (API calls, delays, retries, status polling) happens in Cloud Workflows. Do NOT poll from Cloud Functions or use Cloud Tasks.

## Functions

| Directory | Purpose |
|-----------|---------|
| `dbt-classify/` | Classifies a failed dbt Cloud run as transient (retryable) or not, for the dbt-retry-workflow. Reads run metadata + `run_results.json` (kept out of the Cloud Workflow, which can't hold the large artifact in a variable). |
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
