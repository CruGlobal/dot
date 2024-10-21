# DOT - Data Orchestration Tool

This repo contains a collection of GCP Cloud Run functions to 'glue' together
various pieces of our ELT stack.

## Organization
Each function has its own folder.

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


Deploy new secrets:

`echo -n "123shhhhh456" | gcloud secrets versions add fivetran-trigger_API_SECRET --data-file=-`


