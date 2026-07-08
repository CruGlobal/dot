#!/usr/bin/env bash
# POC integration test for the fivetran-slot-valve Cloud Workflow (DT-561).
#
# The Cloud Workflow YAML cannot be unit-tested locally (there is no offline
# Cloud Workflows runtime), so this script is the repeatable integration test:
# it deploys the workflow to the POC project and exercises its state-machine
# branches against a SAFE test connector.
#
#   Safe target: grip_oblivious (el_fivetran_logs_stage) -- the *stage*
#   Fivetran-logs connector. Only DSE consumes it, so force-syncing / pausing /
#   resuming it has no external blast radius.
#
# The Cloud Function half (main.py) is covered by main_test.py (unit tests); this
# script covers the workflow. See docs/TESTING.md "Fivetran slot valve".
#
# Usage:
#   ./poc_test.sh setup     # copy Fivetran creds prod->POC, render + deploy workflow
#   ./poc_test.sh healthy   # connected + idle      -> expect status "sync_forced"
#   ./poc_test.sh paused    # paused                -> expect resume + "sync_forced"
#   ./poc_test.sh syncing   # already syncing       -> expect "already_syncing" (no-op)
#   ./poc_test.sh status    # print the connector's current Fivetran state
#   ./poc_test.sh render    # render the workflow YAML for POC (no cloud calls)
#   ./poc_test.sh cleanup   # unpause connector, delete workflow + POC secrets
#   ./poc_test.sh all       # setup -> healthy -> syncing -> paused -> cleanup
#
# Notes:
#  - gcloud here runs from the Windows SDK under WSL, so each call is slow (10-30s).
#  - POC is one-person-at-a-time; coordinate before running.
#  - The force/resume branches fire REAL syncs on grip_oblivious (safe, but real).
set -euo pipefail

POC=cru-data-orchestration-poc
PROD=cru-data-orchestration-prod
REGION=us-central1
WF=fivetran-slot-valve
CONNECTOR=grip_oblivious
INSTANCE=fivetran-logs-stage-poc

# Canonical workflow YAML. Defaults to the post-merge master path; override with
# WORKFLOW_YAML=<path> while it still lives on the mechanism feature branch.
SRC="${WORKFLOW_YAML:-$HOME/gitRepos/cru-terraform/applications/data-warehouse/dot/prod/fivetran_slot_valve_workflow.yaml}"
RENDERED="$HOME/dse-scratch/tmp/${WF}.poc.yaml"

poc_sa() {
  local num
  num=$(gcloud projects describe "$POC" --format='value(projectNumber)')
  echo "${num}-compute@developer.gserviceaccount.com"
}

# Load the Fivetran API creds the ~/bin/fivetran wrapper uses. They live in
# ~/.dse-credentials.env (set up by dse-dev-setup/install.sh); interactive shells
# source it via ~/.bashrc, but this non-interactive script must source it itself.
# The file assigns WITHOUT "export", so we export them for child processes (the
# fivetran CLI). NOTE: the prod Secret Manager copy is intentionally NOT used --
# reading those secret values needs the data-engineers identity, not a dev account.
load_fivetran_creds() {
  if [ -z "${FIVETRAN_API_KEY:-}" ] || [ -z "${FIVETRAN_API_SECRET:-}" ]; then
    # shellcheck disable=SC1090
    [ -f "$HOME/.dse-credentials.env" ] && source "$HOME/.dse-credentials.env"
  fi
  if [ -z "${FIVETRAN_API_KEY:-}" ] || [ -z "${FIVETRAN_API_SECRET:-}" ]; then
    echo "ERROR: FIVETRAN_API_KEY/SECRET not set (expected in ~/.dse-credentials.env)." >&2
    exit 1
  fi
  export FIVETRAN_API_KEY FIVETRAN_API_SECRET
}

event_data() {
  local b64
  b64=$(printf '{"connector_id":"%s","instance_id":"%s"}' "$CONNECTOR" "$INSTANCE" | base64 -w0)
  printf '{"data":{"message":{"data":"%s"}}}' "$b64"
}

render() {
  mkdir -p "$(dirname "$RENDERED")"
  # ${project_id} (terraform var) -> POC project; then $${...} -> ${...} for gcloud.
  sed -e 's/\${project_id}/'"$POC"'/g' -e 's/\$\$/\$/g' "$SRC" >"$RENDERED"
  echo "rendered: $RENDERED  (from $SRC)"
}

run_workflow() {
  echo "== running workflow (branch: ${1:-?}) =="
  gcloud workflows run "$WF" --project="$POC" --location="$REGION" \
    --data="$(event_data)" \
    --format='value(result)'
}

connector_status() {
  load_fivetran_creds
  "$HOME/bin/fivetran" status "$CONNECTOR"
}

# $1 = secret name; the secret VALUE is read from stdin.
put_secret() {
  local s="$1"
  gcloud secrets describe "$s" --project="$POC" >/dev/null 2>&1 \
    || gcloud secrets create "$s" --project="$POC" --replication-policy=automatic
  gcloud secrets versions add "$s" --project="$POC" --data-file=-
  gcloud secrets add-iam-policy-binding "$s" --project="$POC" \
    --member="serviceAccount:$(poc_sa)" --role=roles/secretmanager.secretAccessor >/dev/null
  echo "  secret ready in POC: $s"
}

setup() {
  load_fivetran_creds
  echo "== populate Fivetran creds in POC (from ~/.dse-credentials.env) =="
  printf '%s' "$FIVETRAN_API_KEY"    | put_secret fivetran-trigger_API_KEY
  printf '%s' "$FIVETRAN_API_SECRET" | put_secret fivetran-trigger_API_SECRET
  render
  echo "== deploy workflow to POC =="
  gcloud workflows deploy "$WF" --project="$POC" --location="$REGION" \
    --description='TEST: DT-561 fivetran slot valve' \
    --service-account="$(poc_sa)" --source="$RENDERED"
}

healthy() {
  echo "Precondition: connector connected + not syncing."; connector_status
  run_workflow healthy
  echo "Expect: status \"sync_forced\"."
}

paused() {
  echo "Precondition: pausing connector first."; load_fivetran_creds
  "$HOME/bin/fivetran" pause "$CONNECTOR"
  run_workflow paused
  echo "Expect: status \"sync_forced\"; connector now resumed."
  connector_status
}

syncing() {
  echo "Precondition: kicking a sync so one is in flight, then immediately re-firing."
  load_fivetran_creds
  "$HOME/bin/fivetran" sync "$CONNECTOR"
  run_workflow syncing
  echo "Expect: status \"already_syncing\" (no second sync stacked)."
}

cleanup() {
  echo "== ensure connector is unpaused =="
  load_fivetran_creds
  "$HOME/bin/fivetran" resume "$CONNECTOR" 2>/dev/null || true
  echo "== delete POC workflow + secrets =="
  gcloud workflows delete "$WF" --project="$POC" --location="$REGION" --quiet 2>/dev/null || true
  for s in fivetran-trigger_API_KEY fivetran-trigger_API_SECRET; do
    gcloud secrets delete "$s" --project="$POC" --quiet 2>/dev/null || true
  done
  echo "cleanup done."
}

case "${1:-}" in
  setup) setup ;;
  healthy) healthy ;;
  paused) paused ;;
  syncing) syncing ;;
  status) connector_status ;;
  render) render ;;
  cleanup) cleanup ;;
  all) setup; healthy; syncing; paused; cleanup ;;
  *) echo "usage: $0 {setup|healthy|paused|syncing|status|cleanup|all}"; exit 1 ;;
esac
