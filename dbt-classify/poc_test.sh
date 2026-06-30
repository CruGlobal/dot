#!/usr/bin/env bash
# POC integration test for the dbt auto-retry pipeline (DT-568): the thin
# dbt-retry-workflow calling the dbt-classify Cloud Function.
#
# Deploys both to cru-data-orchestration-poc and drives three REAL dbt runs
# end-to-end through the workflow. It is read-only / side-effect-free:
#   - the retry trigger points at an ABSENT function (never reached in these cases), and
#   - the transient case is an OLD run, so the dedup step marks it "superseded" --
#     so NOTHING is actually re-run.
#
#   transient (354 nodes)  run 492507687  -> classify transient -> retry path -> superseded
#   non-transient          run 490901715  -> not_retryable
#   fail-closed (bogus)    run 999999999  -> guard_uncertain
#
# The classifier's logic is unit-tested (main_test.py) and validated against real data
# locally; this script proves the workflow -> function OIDC call + branching.
#
# Usage: ./poc_test.sh {setup|transient|nontransient|failclosed|render|cleanup|all}
#  - gcloud here runs from the Windows SDK under WSL, so calls are slow; the gen2
#    function deploy (Cloud Build) takes a few minutes.
set -euo pipefail

POC=cru-data-orchestration-poc
REGION=us-central1
FN=dbt-classify
WF=dbt-retry-workflow
SECRET=dbt-classify_DBT_TOKEN
SRC="$(cd "$(dirname "$0")" && pwd)"  # the dbt-classify function directory
WORKFLOW_YAML="${WORKFLOW_YAML:-$HOME/gitRepos/cru-terraform/applications/data-warehouse/dot/prod/dbt_retry_workflow.yaml}"
RENDERED="$HOME/dse-scratch/tmp/${WF}.poc.yaml"

poc_sa() {
  local num
  num=$(gcloud projects describe "$POC" --format="value(projectNumber)")
  echo "${num}-compute@developer.gserviceaccount.com"
}

dbt_token() {
  # the dbt service token used for the read-only dbt Cloud API calls
  (
    set -a
    # shellcheck disable=SC1090
    source "$HOME/.dbt/dbt-jobs.env"
    set +a
    printf "%s" "${DBT_API_KEY:-}"
  )
}

render() {
  mkdir -p "$(dirname "$RENDERED")"
  # ${...} terraform vars -> POC values; base_delay 300 -> 10 (fast tests); $${...} -> ${...}
  sed -e "s/\${project_id}/$POC/g" \
      -e "s/\${region}/$REGION/g" \
      -e "s/\${dbt_classify_function_name}/$FN/g" \
      -e "s/\${dbt_trigger_function_name}/dbt-trigger-poc-absent/g" \
      -e "s/base_delay_seconds: 300/base_delay_seconds: 10/g" \
      -e 's/\$\$/\$/g' \
      "$WORKFLOW_YAML" >"$RENDERED"
  echo "rendered: $RENDERED  (from $WORKFLOW_YAML)"
}

setup() {
  local sa
  sa=$(poc_sa)
  echo "== Fivetran/dbt secret in POC =="
  gcloud secrets describe "$SECRET" --project="$POC" >/dev/null 2>&1 \
    || gcloud secrets create "$SECRET" --project="$POC" --replication-policy=automatic
  dbt_token | gcloud secrets versions add "$SECRET" --project="$POC" --data-file=-
  gcloud secrets add-iam-policy-binding "$SECRET" --project="$POC" \
    --member="serviceAccount:$sa" --role=roles/secretmanager.secretAccessor >/dev/null
  echo "== deploy dbt-classify (gen2) =="
  gcloud functions deploy "$FN" --gen2 --region="$REGION" --project="$POC" \
    --runtime=python312 --source="$SRC" --entry-point=classify_run \
    --trigger-http --no-allow-unauthenticated \
    --service-account="$sa" \
    --set-secrets="DBT_TOKEN=${SECRET}:latest"
  echo "== grant the workflow SA run.invoker on dbt-classify =="
  gcloud run services add-iam-policy-binding "$FN" --region="$REGION" --project="$POC" \
    --member="serviceAccount:$sa" --role=roles/run.invoker >/dev/null
  echo "== deploy the thin workflow =="
  render
  gcloud workflows deploy "$WF" --project="$POC" --location="$REGION" \
    --description="TEST: DT-568 dbt auto-retry (classifier)" \
    --service-account="$sa" --source="$RENDERED"
}

deploy_workflow() {  # re-deploy just the workflow (skips the slow function build)
  render
  gcloud workflows deploy "$WF" --project="$POC" --location="$REGION" \
    --description="TEST: DT-568 dbt auto-retry (classifier)" \
    --service-account="$(poc_sa)" --source="$RENDERED"
}

run_case() {  # $1=label $2=job_id $3=run_id
  echo "== $1 (job=$2 run=$3) =="
  local b64
  b64=$(printf '{"job_id":"%s","run_id":"%s","account_id":"10206"}' "$2" "$3" | base64 -w0)
  gcloud workflows run "$WF" --project="$POC" --location="$REGION" \
    --data="{\"data\":{\"message\":{\"data\":\"$b64\"}}}" \
    --format="value(result)"
}

cleanup() {
  gcloud workflows delete "$WF" --project="$POC" --location="$REGION" --quiet 2>/dev/null || true
  gcloud functions delete "$FN" --gen2 --region="$REGION" --project="$POC" --quiet 2>/dev/null || true
  gcloud secrets delete "$SECRET" --project="$POC" --quiet 2>/dev/null || true
  echo "cleanup done"
}

case "${1:-}" in
  setup) setup ;;
  deploy-workflow) deploy_workflow ;;
  transient) run_case "transient (354-node)" 54170 492507687 ;;
  nontransient) run_case "non-transient" 852483 490901715 ;;
  failclosed) run_case "fail-closed" 999999 999999999 ;;
  render) render ;;
  cleanup) cleanup ;;
  all)
    setup
    run_case "transient (354-node)" 54170 492507687
    run_case "non-transient" 852483 490901715
    run_case "fail-closed" 999999 999999999
    cleanup
    ;;
  *) echo "usage: $0 {setup|transient|nontransient|failclosed|render|cleanup|all}"; exit 1 ;;
esac
