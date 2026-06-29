"""Pure classification logic for the dbt auto-retry pipeline (DT-568).

No I/O lives here -- `decide()` takes already-fetched dbt Cloud data and returns a
verdict the retry workflow branches on. Keeping it pure is the whole point of the
function: this logic used to live in the Cloud Workflow YAML (untestable, and it
blew the Workflows memory limit when it held a large run_results.json). Here it is
unit-tested.

Verdict reasons:
  already_retried       -> the run was itself an auto-retry (loop guard); stop
  metadata_unavailable  -> could not read run metadata; fail-closed (guard uncertain)
  results_unavailable   -> could not read run_results.json; cannot classify; do not retry
  no_results            -> run_results.json had no results; cannot classify; do not retry
  uncovered_step        -> a job step errored but no failed node explains it; do not retry
  nontransient          -> at least one failed node is not a transient/infra error; do not retry
  transient             -> every failed node is a transient/infra error; retry once
"""

import re

# Transient / infrastructure error allowlist (case-insensitive). ONLY failures whose
# run_results message matches this are retried. Everything else -- test failures,
# missing table/column, broken joins, other invalid SQL, compile errors -- is left
# for a human. Default-deny: no match => not retryable. Ported verbatim from the
# POC-validated workflow (dbt_retry_workflow.poc.yaml).
TRANSIENT_PATTERN = re.compile(
    r"already exists: job|ratelimitexceeded|exceeded rate limits|jobratelimitexceeded"
    r"|quota exceeded|quotaexceeded|resources exceeded|resourcesexceeded"
    r"|service unavailable|backend error|backenderror|internal error"
    r"|deadline exceeded|deadlineexceeded|could not serialize access"
    r"|connection reset|connection aborted|connection broken|try again later",
    re.IGNORECASE,
)

# dbt Cloud run-step status: 20 == Error.
STEP_STATUS_ERROR = 20


def is_already_retried(run_data: dict) -> bool:
    """True if this run's trigger cause shows it was itself an auto-retry.

    The retry trigger sets a cause containing "Auto-retry"; matching it here is the
    one-retry cap (fail-closed: if we can't read the cause, treat as not-retried only
    because metadata_unavailable is handled upstream).
    """
    cause = (run_data.get("trigger") or {}).get("cause") or ""
    return bool(re.search(r"auto-retry", cause, re.IGNORECASE))


def count_failed_steps(run_data: dict) -> int:
    """Number of run steps that errored (status 20)."""
    steps = run_data.get("run_steps") or []
    return sum(1 for s in steps if (s.get("status") == STEP_STATUS_ERROR))


def classify_results(results: list) -> dict:
    """Scan run_results.json results[] and tally failed vs non-transient nodes.

    - status "fail"  -> a test/assertion failure: a data problem, never transient.
    - status "error" + message matches the transient allowlist -> retryable.
    - status "error" otherwise -> non-transient (missing relation, bad SQL, ...).
    """
    failed_nodes, nontransient_nodes = [], []
    for r in results:
        status = r.get("status")
        unique_id = r.get("unique_id")
        if status == "fail":
            failed_nodes.append(unique_id)
            nontransient_nodes.append(unique_id)
        elif status == "error":
            failed_nodes.append(unique_id)
            if not TRANSIENT_PATTERN.search(r.get("message") or ""):
                nontransient_nodes.append(unique_id)
    return {
        "failed_count": len(failed_nodes),
        "nontransient_count": len(nontransient_nodes),
        "failed_nodes": failed_nodes,
        "nontransient_nodes": nontransient_nodes,
    }


def _verdict(reason, is_retryable=False, prior_is_retry=False, run_data=None, scan=None,
             failed_step_count=0):
    scan = scan or {}
    return {
        "reason": reason,
        "is_retryable": is_retryable,
        "prior_is_retry": prior_is_retry,
        "failed_count": scan.get("failed_count", 0),
        "nontransient_count": scan.get("nontransient_count", 0),
        "failed_nodes": scan.get("failed_nodes", []),
        "nontransient_nodes": scan.get("nontransient_nodes", []),
        "failed_step_count": failed_step_count,
        "run_created_at": (run_data or {}).get("created_at", ""),
    }


def decide(run_data, results, results_fetch_failed=False) -> dict:
    """Produce the retry verdict.

    Args:
        run_data: parsed `/runs/{id}/?include_related=["trigger","run_steps"]` data,
                  or None if that fetch failed.
        results: the `run_results.json` results[] list, or None if unavailable.
        results_fetch_failed: True if the run_results fetch itself errored.
    """
    if run_data is None:
        # Cannot confirm this run was not already a retry -> fail-closed.
        return _verdict("metadata_unavailable")

    if is_already_retried(run_data):
        return _verdict("already_retried", prior_is_retry=True, run_data=run_data)

    failed_step_count = count_failed_steps(run_data)

    if results_fetch_failed:
        return _verdict("results_unavailable", run_data=run_data,
                        failed_step_count=failed_step_count)

    results = results or []
    if len(results) == 0:
        return _verdict("no_results", run_data=run_data, failed_step_count=failed_step_count)

    scan = classify_results(results)

    # Multi-step cross-check: a step errored but run_results explains no failed node
    # (command-level / compile / connection-abort failure this artifact doesn't cover).
    # We cannot classify it -> do not retry.
    if failed_step_count > 0 and scan["failed_count"] == 0:
        return _verdict("uncovered_step", run_data=run_data, scan=scan,
                        failed_step_count=failed_step_count)

    is_retryable = scan["failed_count"] > 0 and scan["nontransient_count"] == 0
    return _verdict("transient" if is_retryable else "nontransient",
                    is_retryable=is_retryable, run_data=run_data, scan=scan,
                    failed_step_count=failed_step_count)
