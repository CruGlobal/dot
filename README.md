# DOT - Data Orchestration Tool

This repo contains a collection of GCP Cloud Run functions to 'glue' together
various pieces of our ELT stack.

## Organization
Each function has its own folder.
The folder name exactly matches the 'name' component of several kinds of GCP resources,
but most importantly it matches the GCR function name.

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


## Deploy new secrets manually:
The env.example file describes the env vars.
They are all deployed as GCP secrets.
(It probably makes sense to use pure ENV vars for non-secret config,
but we don't support that yet.)

All secrets are deployed manually by developers.
Devs have access to write secrets, but don't have access to read them
(except possibly in the POC environment).

First, set up the appropriate environment's GCP project, for example:
```bash
gcloud config set project cru-data-orchestration-poc
```

The secret name is the function name concatenated with an underscore and then with
the env variable name. For example:
```bash
echo -n "123shhhhh456" | gcloud secrets versions add fivetran-trigger_API_SECRET --data-file=-
```
Instead of `echo`, you may want to use a cli for your password manager.
Or on a mac, you can use `pbpaste` to paste the contents of your clipboard,
after you've clicked a 'copy' button in your password manager.

Creating a new secret version does not automatically take effect.
You'll need to trigger a new deployment.
See below, except ignore the advice about "only doing this in the POC env".

## Deploy new code manually:
You probably should only be doing this in the POC env.

```bash
gcloud functions deploy fivetran-trigger --source=. --entry-point=hello_http --runtime=python312 --gen2 --region=us-central1
```


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


## Service Accounts

Service accounts for this project are managed in a separate Terraform repository:
- **Location**: `cru-terraform/applications/data-warehouse/dot/prod/permissions.tf`
- **Pattern**: One service account per Cloud Run function/job
- **Naming convention**: `{function-name}@{project-id}.iam.gserviceaccount.com`

### Adding a New Service Account

1. Add the service account resource to `permissions.tf` in the cru-terraform repo
2. Add required IAM bindings (Pub/Sub publisher, BigQuery access, etc.)
3. If the function needs access to Google Sheets, share those files with the service account email


## Pub/Sub Topics

### cloud-run-job-completed
This topic triggers dbt jobs after a Cloud Run job completes.

**Publishers**: okta-sync, woo-sync, process-geography, google-sheets-trigger

**Subscriber**: A Cloud Function (not in this repo) that calls dbt-trigger

To trigger a dbt job from your function:
```python
from google.cloud import pubsub_v1
import json
import os

def publish_pubsub_message(data: dict, topic_name: str) -> None:
    google_cloud_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(google_cloud_project_id, topic_name)
    data_encoded = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data_encoded)
    future.result()

# Trigger dbt job:
publish_pubsub_message({"job_id": "YOUR_DBT_JOB_ID"}, "cloud-run-job-completed")
```


## google-sheets-trigger

A reusable Cloud Function that checks Google Sheets for changes and triggers dbt jobs.

### Usage

Configure schedules in Terraform (`poc-terraform/functions.tf`). Each schedule specifies:
- Which sheets to monitor (by ID and name)
- Which dbt job to trigger
- When to run (cron schedule)
- Whether to include weekends in change detection

Example Terraform configuration:
```hcl
module "google-sheets-trigger" {
  source = "..."
  schedule = {
    my_sheets: {
      cron: "0 17 * * 1-5",  # M-F 5pm
      argument = {
        "sheets" = [
          { "id" = "your-sheet-id", "name" = "My Sheet" }
        ],
        "dbt_job_id" = "123456",
        "include_weekends" = false
      }
    }
  }
}
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sheets` | array | Yes | List of sheets to monitor, each with `id` and `name` |
| `dbt_job_id` | string | Yes | The dbt job ID to trigger when changes are detected |
| `include_weekends` | boolean | No | If `true`, always looks back 24 hours. If `false` (default), looks back 72 hours on Monday to cover the weekend. |

### Permissions

Share all monitored Google Sheets with the service account:
- `google-sheets-trigger@{project-id}.iam.gserviceaccount.com`
- Grant "Viewer" access (read-only is sufficient)
