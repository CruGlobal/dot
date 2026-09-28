#!/usr/bin/env bash
# dot's workloads, containerized for the gcp/cloudrun/app platform.
#
# ONE image, NINE entry points, NO Dockerfile: the image is built with Google's
# Cloud Native Buildpacks (the same machinery Cloud Functions source deploy
# used), so the handlers keep the functions-framework programming model and the
# repo keeps handler + requirements.txt as its whole contract.
#
# Which workload a container runs is chosen at RUNTIME:
#
# HTTP functions -- the buildpack image's `web` process runs functions-framework
# (see Procfile), which reads the two env vars it already understands:
#
#   FUNCTION_SOURCE=<dir>/main.py   which module to import (relative to /workspace)
#   FUNCTION_TARGET=<handler>       which decorated function to serve
#
#   directory              FUNCTION_SOURCE                 FUNCTION_TARGET
#   ---------------------  ------------------------------  -----------------
#   dbt-classify           dbt-classify/main.py            classify_run
#   dbt-trigger            dbt-trigger/main.py             trigger_dbt_job
#   dbt-webhook            dbt-webhook/main.py             webhook_handler
#   fivetran-slot-valve    fivetran-slot-valve/main.py     valve_handler
#   fivetran-trigger       fivetran-trigger/main.py        trigger_sync
#   fivetran-webhook       fivetran-webhook/main.py        webhook_handler
#
# functions-framework appends the source file's directory to sys.path, so each
# function's sibling imports (`from dbt_client import ...`) resolve exactly as
# they do today. There is deliberately no default target: a container started
# without FUNCTION_TARGET exits with functions-framework's own error rather
# than silently serving whichever function happened to be listed first.
#
# Batch jobs -- each gcp/cloudrun/app job overrides the container command. The
# command MUST go through the buildpack launcher: a CNB image only sets up its
# runtime env (python on PATH, pip layer on sys.path) via the launcher
# entrypoint, so a bare ["python", ...] command would not find python.
#
#   woo-sync               command = ["launcher"], args = ["python", "woo-sync/main.py"]
#   okta-sync              command = ["launcher"], args = ["python", "okta-sync/main.py"]
#   process-geography      command = ["launcher"], args = ["python", "process-geography/main.py"]
#
# (The per-directory `dockerfile`s the jobs use today stay until cutover.)
#
# Pipeline-v2's build-cloudrun workflow invokes this script with buildx-shaped
# $DOCKER_ARGS; only `--tag <ref>` and `--push` apply to pack, the buildx
# builder/cache flags are ignored (pack keeps its own cache in docker volumes).
#
# Local:
#   ./build.sh                 # builds dot:local
#   ./build.sh mytag:dev
#   docker run --rm -p 8080:8080 \
#     -e FUNCTION_SOURCE=fivetran-trigger/main.py \
#     -e FUNCTION_TARGET=trigger_sync dot:local
set -euo pipefail

PACK_VERSION="v0.40.8"
BUILDER="gcr.io/buildpacks/builder:google-22"

IMAGE="dot:local"
PUBLISH=""
prev=""
for arg in ${DOCKER_ARGS:-}; do
  [[ "$prev" == "--tag" ]] && IMAGE="$arg"
  [[ "$arg" == "--push" ]] && PUBLISH="--publish"
  prev="$arg"
done
[[ $# -ge 1 ]] && IMAGE="$1"

PACK="pack"
if ! command -v pack >/dev/null 2>&1; then
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"; [[ "$arch" == "x86_64" ]] && arch="" || arch="-${arch/aarch64/arm64}"
  bindir="$(mktemp -d)"
  curl -sSL "https://github.com/buildpacks/pack/releases/download/${PACK_VERSION}/pack-${PACK_VERSION}-${os}${arch}.tgz" \
    | tar -xz -C "$bindir"
  PACK="$bindir/pack"
fi

"$PACK" build "$IMAGE" \
  --builder "$BUILDER" \
  --trust-builder \
  $PUBLISH
