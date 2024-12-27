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
