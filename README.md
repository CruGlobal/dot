# DOT - Data Orchestration Tool

This repo contains a collection of GCP Cloud Run functions to 'glue' together
various pieces of our ELT stack.

## Organization
Each function has its own folder.
The folder name exactly matches the 'name' component of several kinds of GCP resources,
but most importantly it matches the GCR function name.
Each function also has its own deploy workflow at
`.github/workflows/build-deploy-cloudrun-<folder>.yml` (see "Automated deploys" below) — a new
function does not deploy until that file exists.

## Local setup

Initial Setup
 * Clone the repo
 * `python3 -m venv .venv`
 * `source .venv/bin/activate`
 * `PATH=$PATH:$PWD/.venv/bin`
 * `cd` into the function directory of interest
 * `pip3 install -r requirements.txt`

Run locally with [functions-framework-python](https://github.com/GoogleCloudPlatform/functions-framework-python)
 * read .env.example
 * set up required env vars, eg `export API_KEY=my_key`
 * run `functions-framework-python --target hello_http --debug` (hello_http is an example)
 * in another shell, run `http http://localhost:8080/`

Run unit tests with pytest:
 * `pip3 install -r requirements-test.txt`
 * `pytest`


## Secrets

Secrets are managed in two steps:

**Step 1: Create the secret resource (Terraform)**

Secret resources are created in [cru-terraform](https://github.com/CruGlobal/cru-terraform/tree/master/applications/data-warehouse/dot) — each function or workflow defines its secrets in its Terraform module. The naming convention is `{function-or-workflow-name}_{ENV_VAR_NAME}` (e.g., `dbt-trigger_DBT_TOKEN`, `hightouch-workflow_HT_KEY`).

Terraform creates the empty secret resource — it does NOT contain the actual value.

**Step 2: Add the secret value (manual, by developer)**

After Terraform creates the secret, add the value via the GCP Console or CLI:

- **GCP Console**: [Secret Manager](https://console.cloud.google.com/security/secret-manager?project=cru-data-orchestration-prod) → find the secret → Add New Version → paste the value
- **CLI**:
  ```bash
  gcloud config set project cru-data-orchestration-prod
  echo -n "the-actual-secret-value" | gcloud secrets versions add {secret-name} --data-file=-
  ```

Devs in the `dps-gcp-role-data-engineers@cru.org` group have access to write secret values but not read them (except in POC).

**Important:** Creating a new secret version does not automatically take effect. You'll need to trigger a new deployment (push to `main` for prod, `staging` for stage).

## Automated deploys (prod & stage)

Prod and stage deploy automatically via GitHub Actions — there is nothing to run by hand:

- Merge/push to `main` → deploys to **prod** (`cru-data-orchestration-prod`).
- Push to `staging` → deploys to **stage**.

Each function has its **own** deploy workflow: `.github/workflows/build-deploy-cloudrun-<function-name>.yml`,
path-filtered to `<function-name>/**` so merging only redeploys the functions whose source changed
(plus a manual `workflow_dispatch` trigger). Each is a thin caller of the shared reusable workflow
`CruGlobal/.github/.github/workflows/build-deploy-cloudrun-function.yml@v1`.

**Adding a new function?** You MUST add its `build-deploy-cloudrun-<name>.yml` — copy an existing one
and change `name`, `function_name`, `entry_point`, `runtime`, and the `paths:` filter. Without it,
merging to `main` runs only the tests; the function never deploys and silently keeps serving the
Terraform placeholder, with no error to tell you.

The reusable workflow deploys source-only (`gcloud functions deploy --source --entry-point --runtime
--build-service-account`) — it does not pass `--set-secrets`, `--run-service-account`, `--timeout`, or
`--memory`, so the Terraform-managed runtime config (secrets, SA, timeout, memory) is preserved across
deploys. Configure those in Terraform (see the cru-terraform `dot` modules), not here.

## Deploy new code manually:
You probably should only be doing this in the POC env.

```bash
gcloud functions deploy fivetran-trigger --source=. --entry-point=hello_http --runtime=python312 --gen2 --region=us-central1
```


## Environments

| Environment | GCP Project | Code Branch | Purpose |
|-------------|------------|-------------|---------|
| **prod** | `cru-data-orchestration-prod` | `main` | All production AND staging data pipelines. This is where all real orchestration runs. |
| **stage** | `cru-data-orchestration-stage` | `staging` | Infrastructure health check only. Not used for data pipelines. |
| **poc** | `cru-data-orchestration-poc` | `poc` | Developer sandbox for testing new functions and workflows. |

All data pipelines — including those that process staging source data — are orchestrated from the
**prod** project. The prod environment routes to the correct dbt Cloud jobs and environments based on
job IDs and configuration. The stage project does not run data pipelines.

Terraform for prod and stage is managed via Atlantis in the
[cru-terraform repo](https://github.com/CruGlobal/cru-terraform/tree/master/applications/data-warehouse/dot).

## POC environment infrastructure

The POC environment is contained within the [cru-data-orchestration-poc](https://console.cloud.google.com/welcome?project=cru-data-orchestration-poc) GCP project.
The project and its integrations with Datadog and Github are managed by Terraform and Atlantis in the [cru-terraform repo](https://github.com/CruGlobal/cru-terraform/tree/master/applications/data-warehouse/dot/poc).
However, the functions and related infrastructure are not managed that way. Instead, devs can 'spin up' the functions by using terraform locally, using local tf state. They can then use the web console or gcloud cli to experiment and learn.

To spin up the POC infrastructure:
 * install terraform and gcloud
 * authenticate with gcloud with ADC: `gcloud auth application-default set-quota-project cru-data-orchestration-poc`
 * cd into `poc-terraform`
 * coordinate with team; only one person can do this at time
 * `terraform init`
 * `terraform apply`
 * set up secrets with gcloud (see above)
 * deploy code (see above, or use GHA)

To clean up when you're done:
 * `terraform destroy`

Infrastructure learnings here can be applied to the terraform config for the beta and production environments.
